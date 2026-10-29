"""Nested types through emberserde: reading and writing vectors.

`DuckDBMapped` (`mapping.mojo`) covers the scalar types. Everything else
(`Optional`, `List`, `Dict`, `Tuple`, `Variant`, structs, and whole rows as
structs) goes through emberserde with the two formats here:

- `_Cursor`, an emberserde `Deserializer`, reads one value at one row of a
  vector tree (`_ReadTree`). The tree is built once per chunk.
- `_Writer`, an emberserde `Serializer`, writes one value at one row of a
  writable vector tree (`_WriteTree`), owned by a `ChunkWriter`.

Both check for a DuckDB scalar type before handing a type to emberserde, so
scalars are read and written in their storage layout and never reach
emberserde's own impls.

Mapping:
- STRUCT values and whole rows map to struct fields by name
  (case-insensitive; `Rename` and `Alias` apply). `Tuple` maps to STRUCT
  fields (or row columns) by position.
- LIST and ARRAY map to sequences (`List`, ...), MAP to `Dict` or to a
  sequence of key/value tuples.
- UNION maps to `Variant`. Members match arms by name when every member
  name is an arm's tag (`Rename` on the arm types); otherwise by type, when
  exactly one arm can hold the member's type.
- NULL maps to `None` of an `Optional`.
"""

from std.builtin.rebind import downcast, rebind_var
from std.reflection import reflect
from std.utils import Variant
from emberserde import (
    DerErrorKind,
    DeserializationError,
    Deserializer,
    EnumDerState,
    EnumSerState,
    MapDerState,
    MapSerState,
    SeqDerState,
    SeqSerState,
    SerErrorKind,
    SerializationError,
    Serializer,
    StructDerState,
    StructSerState,
    TupleDerState,
    TupleSerState,
    deserialize,
    serialize,
)
from emberserde.field_meta import is_skipped, name_matches, static_wire_name
from emberserde.struct_modifiers import arm_tag
from emberserde.utils import Base
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb.v2._runtime import api_error, builtin_factory, check, lib
from duckdb.v2.error import DuckDBError, ErrorCode
from duckdb.v2.mapping import (
    Blob,
    ColumnMeta,
    MutRawData,
    RawData,
    is_mapped,
    mapped_accepts,
    mapped_from_value,
    mapped_logical_type,
    mapped_read,
    mapped_to_value,
    mapped_write,
)
from duckdb.v2.types import LogicalType, TypeId


# ===--------------------------------------------------------------------===#
# Logical types of Mojo types
# ===--------------------------------------------------------------------===#


trait _Typed:
    """A generic container whose DuckDB type follows from its parameters."""

    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        ...

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        """Whether a value of this type id can be read as this type."""
        ...


__extension Optional(_Typed):
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        return logical_type_of[Self.T]()

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return _accepts_kind[Self.T](kind)


__extension List(_Typed):
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        return LogicalType.list(logical_type_of[Self.T]())

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return kind == TypeId.LIST or kind == TypeId.ARRAY


__extension Array(_Typed):
    # A list literal passed to a generic parameter becomes an `Array`; like
    # a SQL list literal it is a LIST. It reads from LIST and ARRAY values.
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        return LogicalType.list(logical_type_of[Self.T]())

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return kind == TypeId.LIST or kind == TypeId.ARRAY


__extension Dict(_Typed):
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        return LogicalType.map(logical_type_of[Self.K](), logical_type_of[Self.V]())

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return kind == TypeId.MAP


__extension Tuple(_Typed):
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        var types = List[LogicalType]()
        comptime for i in range(Self.__len__()):
            comptime ET = Self.Ts[i]
            types.append(logical_type_of[ET]())
        return LogicalType.struct_(List[String](), types)

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return kind == TypeId.STRUCT or kind == TypeId.LIST or kind == TypeId.ARRAY


__extension Variant(_Typed):
    @staticmethod
    def _duckdb_type() raises DuckDBError -> LogicalType:
        var names = List[String]()
        var types = List[LogicalType]()
        comptime for i in range(Self.Ts.length):
            comptime AT = Self.Ts[i]
            names.append(arm_tag[AT]())
            types.append(logical_type_of[AT]())
        return LogicalType.union_(names, types)

    @staticmethod
    def _accepts(kind: TypeId) -> Bool:
        return kind == TypeId.UNION


trait _VariantArms:
    """The arms of a `Variant`, for matching them to UNION members."""

    @staticmethod
    def _arm_count() -> Int:
        ...

    @staticmethod
    def _arm_name(i: Int) -> String:
        ...

    @staticmethod
    def _arm_accepts(i: Int, kind: TypeId) -> Bool:
        ...


__extension Variant(_VariantArms):
    @staticmethod
    def _arm_count() -> Int:
        return Self.Ts.length

    @staticmethod
    def _arm_name(i: Int) -> String:
        comptime for a in range(Self.Ts.length):
            if a == i:
                return arm_tag[Self.Ts[a]]()
        return String()

    @staticmethod
    def _arm_accepts(i: Int, kind: TypeId) -> Bool:
        comptime for a in range(Self.Ts.length):
            if a == i:
                return _accepts_kind[Self.Ts[a]](kind)
        return False


def _accepts_kind[T: AnyType](kind: TypeId) -> Bool:
    """Whether a value of type id `kind` can be read as `T`."""
    comptime if is_mapped[T]():
        return mapped_accepts[T](kind)
    elif conforms_to(T, _Typed):
        return downcast[T, _Typed]._accepts(kind)
    else:
        return kind == TypeId.STRUCT


def _match_union[T: AnyType](lower_names: List[String], kinds: List[TypeId]) -> List[Int]:
    """UNION member -> `Variant` arm: by name when every member names an
    arm, else by type. -1: no arm fits; -2: several do."""
    comptime V = downcast[T, _VariantArms]
    var arms = V._arm_count()
    var out = List[Int]()
    var by_name = True
    for m in range(len(kinds)):
        var found = -1
        for a in range(arms):
            if found == -1 and V._arm_name(a).lower() == lower_names[m]:
                found = a
        by_name = by_name and found != -1
        out.append(found)
    if by_name:
        return out^
    out = List[Int]()
    for m in range(len(kinds)):
        var found = -1
        for a in range(arms):
            if V._arm_accepts(a, kinds[m]):
                found = a if found == -1 else -2
        out.append(found)
    return out^


@no_inline
def _union_mismatch(member: String, kind: TypeId, arm: Int) -> String:
    var why = "no Variant arm can hold it" if arm == -1 else "several Variant arms can hold it"
    return String(
        "UNION member ", member, " (", kind, "): ", why, " (name the arms after the members with Rename to match by name)"
    )


def logical_type_of[T: AnyType]() raises DuckDBError -> LogicalType:
    """The DuckDB type a value of `T` is written as.

    Scalars map to their own type, `Optional[T]` to the type of `T`,
    `List` to LIST, `Dict` to MAP, `Tuple` to an unnamed STRUCT, `Variant` to
    UNION, `Array` (what a list literal becomes) to LIST, and any other
    struct to a STRUCT of its fields (by wire name).
    """
    comptime if is_mapped[T]():
        return mapped_logical_type[T]()
    elif conforms_to(T, _Typed):
        return downcast[T, _Typed]._duckdb_type()
    else:
        comptime r = reflect[T]
        comptime assert r.is_struct(), "this type has no DuckDB mapping"
        var names = List[String]()
        var types = List[LogicalType]()
        comptime for i in range(r.field_count()):
            comptime if not is_skipped[T, i]():
                names.append(String(static_wire_name[T, i]()))
                comptime FT = r.field_types()[i]
                types.append(logical_type_of[FT]())
        return LogicalType.struct_(names, types)


# ===--------------------------------------------------------------------===#
# Errors (out of line: only reached on failure)
# ===--------------------------------------------------------------------===#


@no_inline
def _mismatch(expected: StaticString, kind: TypeId) -> DeserializationError:
    return DeserializationError(String("expected ", expected, ", got ", kind), DerErrorKind.TypeMismatch)


@no_inline
def _mismatch_type[T: AnyType](kind: TypeId) -> DeserializationError:
    return DeserializationError(
        String("a ", kind, " value cannot be read as ", reflect[T].name()), DerErrorKind.TypeMismatch
    )


@no_inline
def _unexpected_null() -> DeserializationError:
    return DeserializationError(
        String("unexpected NULL (read it as an Optional to allow NULLs)"), DerErrorKind.InvalidValue
    )


@no_inline
def _ser_error(e: DuckDBError) -> SerializationError:
    return SerializationError(String(e.message), SerErrorKind.Custom)


@no_inline
def _ser_invalid(var message: String) -> SerializationError:
    return SerializationError(message^, SerErrorKind.InvalidValue)


@no_inline
def _from_der(e: DeserializationError, var context: String) -> DuckDBError:
    var code = ErrorCode.INPUT_INVALID
    if e.kind != DerErrorKind.InvalidValue and e.kind != DerErrorKind.Custom:
        code = ErrorCode.TYPE_MISMATCH
    if context:
        context += ": "
    return DuckDBError(code, context + String(e))


@no_inline
def _from_ser(e: SerializationError, var context: String) -> DuckDBError:
    if context:
        context += ": "
    return DuckDBError(ErrorCode.INPUT_INVALID, context + e.message)


def _is_nested(kind: TypeId) -> Bool:
    return (
        kind == TypeId.LIST
        or kind == TypeId.MAP
        or kind == TypeId.ARRAY
        or kind == TypeId.STRUCT
        or kind == TypeId.UNION
    )


def _child_vector(vec: duckdb_v2_vector_handle, i: Int) raises DuckDBError -> duckdb_v2_vector_handle:
    var child = null[duckdb_v2_vector_handle]()
    var err = null_error_info()
    check(lib().duckdb_v2_vector_get_child(vec, idx_t(i), Pointer(to=child), Pointer(to=err)), err)
    return child


def _entries_type(map_type: LogicalType) raises DuckDBError -> LogicalType:
    """The STRUCT(key, value) a MAP vector stores its entries as."""
    var kv = map_type.child_types()
    return LogicalType.struct_(["key", "value"], [kv[0][1].copy(), kv[1][1].copy()])


# ===--------------------------------------------------------------------===#
# Reading: the vector tree
# ===--------------------------------------------------------------------===#


struct _RNode(Copyable, Movable):
    var meta: ColumnMeta
    var data: RawData
    var validity: Pointer[UInt64, ImmUntrackedOrigin]
    var sel: Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]
    var array_size: Int
    var constant: Bool  # a CONSTANT vector: every row reads index 0
    # LIST/ARRAY: [elements]; MAP: [entries]; STRUCT and the root: fields;
    # UNION: [tag, members...].
    var children: List[Int]
    # STRUCT and root: child names, as given and lowercased.
    var names: List[String]
    var lower_names: List[String]
    # Child -> field index (-1: no field) for the struct type last read from
    # this node; rebuilt when another type is read.
    var map_key: String
    var field_map: List[Int]

    def __init__(out self, var meta: ColumnMeta, data: RawData, validity: Pointer[UInt64, ImmUntrackedOrigin], sel: Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]):
        self.meta = meta^
        self.data = data
        self.validity = validity
        self.sel = sel
        self.array_size = 0
        self.constant = False
        self.children = List[Int]()
        self.names = List[String]()
        self.lower_names = List[String]()
        self.map_key = String()
        self.field_map = List[Int]()


struct _ReadTree(Movable):
    """Views of a chunk's vectors, nested ones included. Node 0 is the row:
    a virtual STRUCT whose fields are the columns."""

    var nodes: List[_RNode]

    def __init__(out self, vectors: List[duckdb_v2_vector_handle], names: List[String], types: List[LogicalType]) raises DuckDBError:
        self.nodes = List[_RNode]()
        self.nodes.append(
            _RNode(
                ColumnMeta(kind=TypeId.STRUCT),
                null[RawData](),
                null[Pointer[UInt64, ImmUntrackedOrigin]](),
                null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
            )
        )
        for i in range(len(vectors)):
            var child = self._add(vectors[i], types[i])
            self.nodes[0].children.append(child)
            self.nodes[0].lower_names.append(names[i].lower())
            self.nodes[0].names.append(names[i].copy())

    def is_valid(self, node: Int, row: Int) -> Bool:
        """Whether `row` of `node` is not NULL (selection applied)."""
        ref n = self.nodes[node]
        var idx = Int(n.sel[unsafe_offset=row]) if Int(n.sel) != 0 else row
        if Int(n.validity) == 0:
            return True
        return (n.validity[unsafe_offset=idx >> 6] >> UInt64(idx & 63)) & 1 == 1

    def _add(mut self, vec: duckdb_v2_vector_handle, type: LogicalType) raises DuckDBError -> Int:
        var err = null_error_info()
        var meta = ColumnMeta(type)
        var kind = meta.kind
        var nested = _is_nested(kind)
        var vtype: DUCKDB_V2_VECTOR_TYPE = DUCKDB_V2_VECTOR_TYPE_OTHER
        check(lib().duckdb_v2_vector_get_vector_type(vec, Pointer(to=vtype), Pointer(to=err)), err)
        # Children of nested vectors line up with the parent's storage rows
        # only when the parent is flat.
        if vtype == DUCKDB_V2_VECTOR_TYPE_OTHER or (nested and vtype != DUCKDB_V2_VECTOR_TYPE_FLAT):
            check(lib().duckdb_v2_vector_flatten(vec, Pointer(to=err)), err)
        var view = duckdb_v2_vector_view(
            null[RawData](),
            null[Pointer[UInt64, ImmUntrackedOrigin]](),
            null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
            0,
        )
        check(lib().duckdb_v2_vector_get_view(vec, Pointer(to=view), Pointer(to=err)), err)
        var idx = len(self.nodes)
        self.nodes.append(_RNode(meta^, view.data, view.validity, view.sel))
        self.nodes[idx].constant = vtype == DUCKDB_V2_VECTOR_TYPE_CONSTANT
        if not nested:
            return idx
        if kind == TypeId.MAP:
            var entries = self._add(_child_vector(vec, 0), _entries_type(type))
            self.nodes[idx].children.append(entries)
            return idx
        var children = type.child_types()
        if kind == TypeId.LIST or kind == TypeId.ARRAY:
            if kind == TypeId.ARRAY:
                self.nodes[idx].array_size = type.array_size()
            var elements = self._add(_child_vector(vec, 0), children[0][1])
            self.nodes[idx].children.append(elements)
            return idx
        var first = 0
        if kind == TypeId.UNION:
            var tag = self._add(_child_vector(vec, 0), LogicalType(TypeId.UTINYINT))
            self.nodes[idx].children.append(tag)
            first = 1
        for i in range(len(children)):
            var child = self._add(_child_vector(vec, i + first), children[i][1])
            self.nodes[idx].children.append(child)
            self.nodes[idx].lower_names.append(children[i][0].lower())
            self.nodes[idx].names.append(children[i][0].copy())
        return idx


def _vector_type(vec: duckdb_v2_vector_handle) raises DuckDBError -> LogicalType:
    var t = null[duckdb_v2_logical_type_handle]()
    var err = null_error_info()
    check(lib().duckdb_v2_vector_get_logical_type(vec, Pointer(to=t), Pointer(to=err)), err)
    return LogicalType(_owned=t)


def read_tree_of(vec: duckdb_v2_vector_handle) raises DuckDBError -> _ReadTree:
    """A read tree over one vector (its node is 1), typed by the vector itself."""
    return _ReadTree([vec], ["v"], [_vector_type(vec)])


def read_value[T: AnyType](mut tree: _ReadTree, node: Int, row: Int) raises DuckDBError -> T:
    """Read `T` at `row` of a tree node (node 0: the whole row)."""
    var c = _Cursor(Pointer(to=tree).unsafe_origin_cast[MutUntrackedOrigin](), node, row)
    try:
        return _read[T](c)
    except e:
        raise _from_der(e, String("row ", row))


def _is_row_type[T: AnyType]() -> Bool:
    """Whether `T` maps to a whole row (a struct or a `Tuple`) rather than to
    one column (scalars, `Optional`, lists, maps, `Variant`)."""
    comptime if is_mapped[T]():
        return False
    elif conforms_to(T, _Typed):
        return reflect[T].base_name() == "Tuple"
    else:
        return True


def read_row[T: AnyType](mut tree: _ReadTree, row: Int) raises DuckDBError -> T:
    """Read a whole row as `T`: a struct (columns by name), a `Tuple` (by
    position), or any other type when there is exactly one column."""
    comptime if not _is_row_type[T]():
        if len(tree.nodes[0].children) != 1:
            raise api_error(
                String("a row with ", len(tree.nodes[0].children), " columns cannot be read as one value; use a struct or a Tuple")
            )
        return read_value[T](tree, tree.nodes[0].children[0], row)
    else:
        return read_value[T](tree, 0, row)


# ===--------------------------------------------------------------------===#
# Reading: the format
# ===--------------------------------------------------------------------===#


@always_inline
def _read[T: AnyType](mut c: _Cursor) raises DeserializationError -> T:
    """Read `T` at the cursor: in DuckDB's layout for scalars, else through emberserde."""
    comptime if is_mapped[T]():
        ref node = c.tree[].nodes[c.node]
        if not mapped_accepts[T](node.meta.kind):
            raise _mismatch_type[T](node.meta.kind)
        var idx = c._phys()
        if not c._valid(idx):
            raise _unexpected_null()
        return mapped_read[T](node.meta, node.data, idx)
    else:
        return deserialize[T](c)


comptime _RTreePtr = Pointer[_ReadTree, MutUntrackedOrigin]


struct _Cursor(Deserializer):
    """A position (node, row) in a read tree."""

    var tree: _RTreePtr
    var node: Int
    var row: Int

    comptime SeqType = _SeqDe
    comptime StructType = _StructDe
    comptime MapType = _MapDe
    comptime TupleType = _TupleDe
    comptime EnumType = _EnumDe

    def __init__(out self, tree: _RTreePtr, node: Int, row: Int):
        self.tree = tree
        self.node = node
        self.row = row

    @always_inline
    def _phys(self) -> Int:
        ref sel = self.tree[].nodes[self.node].sel
        return Int(sel[unsafe_offset=self.row]) if Int(sel) != 0 else self.row

    @always_inline
    def _valid(self, idx: Int) -> Bool:
        ref v = self.tree[].nodes[self.node].validity
        if Int(v) == 0:
            return True
        return (v[unsafe_offset=idx >> 6] >> UInt64(idx & 63)) & 1 == 1

    def _valid_index(self) raises DeserializationError -> Int:
        var idx = self._phys()
        if not self._valid(idx):
            raise _unexpected_null()
        return idx

    def expect_bool(mut self) raises DeserializationError -> Bool:
        return _read[Bool](self)

    def expect_number[DT: DType](mut self) raises DeserializationError -> Scalar[DT]:
        return _read[Scalar[DT]](self)

    def expect_string(mut self) raises DeserializationError -> String:
        return _read[String](self)

    def expect_bytes(mut self) raises DeserializationError -> List[Byte]:
        var b = _read[Blob](self)
        return b.data.copy()

    def expect_optional[T: Base](mut self) raises DeserializationError -> Optional[T]:
        if not self._valid(self._phys()):
            return None
        return Optional[T](_read[T](self))

    def begin_seq(mut self) raises DeserializationError -> Self.SeqType:
        ref node = self.tree[].nodes[self.node]
        var kind = node.meta.kind
        if kind == TypeId.LIST or kind == TypeId.MAP:
            var idx = self._valid_index()
            ref entry = node.data.unsafe_bitcast[duckdb_v2_list_entry]()[unsafe_offset=idx]
            return _SeqDe(self.tree, node.children[0], Int(entry.offset), Int(entry.offset + entry.length))
        if kind == TypeId.ARRAY:
            var idx = self._valid_index()
            var n = node.array_size
            return _SeqDe(self.tree, node.children[0], idx * n, idx * n + n)
        raise _mismatch("a LIST, ARRAY or MAP", kind)

    def begin_map(mut self) raises DeserializationError -> Self.MapType:
        ref node = self.tree[].nodes[self.node]
        if node.meta.kind != TypeId.MAP:
            raise _mismatch("a MAP", node.meta.kind)
        var idx = self._valid_index()
        ref entry = node.data.unsafe_bitcast[duckdb_v2_list_entry]()[unsafe_offset=idx]
        ref entries = self.tree[].nodes[node.children[0]]
        return _MapDe(
            self.tree, entries.children[0], entries.children[1], Int(entry.offset), Int(entry.offset + entry.length)
        )

    def begin_struct[T: AnyType](mut self) raises DeserializationError -> Self.StructType:
        var kind = self.tree[].nodes[self.node].meta.kind
        if kind != TypeId.STRUCT:
            raise _mismatch("a STRUCT", kind)
        var idx = self._valid_index()
        _map_fields[T](self.tree[].nodes[self.node])
        return _StructDe(self.tree, self.node, idx, 0)

    def begin_tuple[field_count: Int](mut self) raises DeserializationError -> Self.TupleType:
        ref node = self.tree[].nodes[self.node]
        var kind = node.meta.kind
        if kind == TypeId.STRUCT:
            var idx = self._valid_index()
            if len(node.children) != field_count:
                raise DeserializationError(
                    String("a STRUCT with ", len(node.children), " fields cannot be read as a tuple of ", field_count),
                    DerErrorKind.TypeMismatch,
                )
            return _TupleDe(self.tree, self.node, -1, idx)
        var seq = self.begin_seq()
        if seq.end_pos - seq.pos != field_count:
            raise DeserializationError(
                String("a list of ", seq.end_pos - seq.pos, " elements cannot be read as a tuple of ", field_count),
                DerErrorKind.TypeMismatch,
            )
        return _TupleDe(self.tree, self.node, seq.child, seq.pos)

    def begin_enum[T: AnyType, arm_names: List[String]](mut self) raises DeserializationError -> Self.EnumType:
        var kind = self.tree[].nodes[self.node].meta.kind
        if kind != TypeId.UNION:
            raise _mismatch("a UNION", kind)
        var idx = self._valid_index()
        var tag_cursor = _Cursor(self.tree, self.tree[].nodes[self.node].children[0], idx)
        var tag = Int(_read[UInt8](tag_cursor))
        ref node = self.tree[].nodes[self.node]
        var arm = tag
        comptime if conforms_to(T, _VariantArms):
            comptime key = reflect[T].name()
            if node.map_key != key:
                var kinds = List[TypeId]()
                for m in range(1, len(node.children)):
                    kinds.append(self.tree[].nodes[node.children[m]].meta.kind)
                node.field_map = _match_union[T](node.lower_names, kinds)
                node.map_key = String(key)
            arm = node.field_map[tag]
            if arm < 0:
                raise DeserializationError(
                    _union_mismatch(node.names[tag], self.tree[].nodes[node.children[tag + 1]].meta.kind, arm),
                    DerErrorKind.UnknownVariant,
                )
        else:
            comptime arms = len(arm_names)
            if tag >= arms:
                raise DeserializationError(
                    String("UNION member ", tag, " has no arm (", arms, " arms)"), DerErrorKind.UnknownVariant
                )
        return _EnumDe(self.tree, node.children[tag + 1], idx, arm)


def _map_fields[T: AnyType](mut node: _RNode):
    """Map the node's children to `T`'s fields, once per struct type.

    Exact name or `Alias` first (through emberserde), then case-insensitive
    against the wire name, because DuckDB identifiers are case-insensitive.
    """
    comptime key = reflect[T].name()
    if node.map_key == key:
        return
    node.field_map = List[Int]()
    for k in range(len(node.names)):
        var found = -1
        comptime for i in range(reflect[T].field_count()):
            if found == -1 and name_matches[T, i](node.names[k]):
                found = i
        comptime for i in range(reflect[T].field_count()):
            comptime if not is_skipped[T, i]():
                if found == -1 and String(static_wire_name[T, i]()).lower() == node.lower_names[k]:
                    found = i
        node.field_map.append(found)
    node.map_key = String(key)


@fieldwise_init
struct _SeqDe(SeqDerState):
    var tree: _RTreePtr
    var child: Int
    var pos: Int
    var end_pos: Int

    def has_next(mut self) raises DeserializationError -> Bool:
        return self.pos < self.end_pos

    def expect_element[T: AnyType](mut self) raises DeserializationError -> T:
        var c = _Cursor(self.tree, self.child, self.pos)
        self.pos += 1
        return _read[T](c)

    def end(mut self) raises DeserializationError:
        pass


@fieldwise_init
struct _MapDe(MapDerState):
    var tree: _RTreePtr
    var keys: Int
    var values: Int
    var pos: Int
    var end_pos: Int

    def has_next(mut self) raises DeserializationError -> Bool:
        return self.pos < self.end_pos

    def expect_key[T: AnyType](mut self) raises DeserializationError -> T:
        var c = _Cursor(self.tree, self.keys, self.pos)
        return _read[T](c)

    def expect_value[T: AnyType](mut self) raises DeserializationError -> T:
        var c = _Cursor(self.tree, self.values, self.pos)
        self.pos += 1
        return _read[T](c)

    def end(mut self) raises DeserializationError:
        pass


@fieldwise_init
struct _StructDe(StructDerState):
    var tree: _RTreePtr
    var node: Int
    # Storage row of the struct; its children line up with it.
    var row: Int
    var next_child: Int

    def expect_field_index[T: AnyType](mut self) raises DeserializationError -> Optional[Int]:
        ref node = self.tree[].nodes[self.node]
        if self.next_child >= len(node.children):
            return None
        var field = node.field_map[self.next_child]
        self.next_child += 1
        return field

    def expect_field_value[T: AnyType](mut self) raises DeserializationError -> T:
        var c = _Cursor(self.tree, self.tree[].nodes[self.node].children[self.next_child - 1], self.row)
        return _read[T](c)

    def skip_value(mut self) raises DeserializationError:
        pass

    def end(mut self) raises DeserializationError:
        pass


@fieldwise_init
struct _TupleDe(TupleDerState):
    var tree: _RTreePtr
    var node: Int
    # -1: STRUCT fields by position at `pos`; else the element node of a
    # list, with the elements from `pos` on.
    var seq_child: Int
    var pos: Int
    var next: Int

    def __init__(out self, tree: _RTreePtr, node: Int, seq_child: Int, pos: Int):
        self.tree = tree
        self.node = node
        self.seq_child = seq_child
        self.pos = pos
        self.next = 0

    def expect_element[T: AnyType](mut self) raises DeserializationError -> T:
        var i = self.next
        self.next += 1
        if self.seq_child >= 0:
            var c = _Cursor(self.tree, self.seq_child, self.pos + i)
            return _read[T](c)
        var c = _Cursor(self.tree, self.tree[].nodes[self.node].children[i], self.pos)
        return _read[T](c)

    def end(mut self) raises DeserializationError:
        pass


@fieldwise_init
struct _EnumDe(EnumDerState):
    var tree: _RTreePtr
    var member: Int
    var row: Int
    var tag: Int

    def variant_index(mut self) raises DeserializationError -> Int:
        return self.tag

    def expect_payload[T: AnyType](mut self) raises DeserializationError -> T:
        var c = _Cursor(self.tree, self.member, self.row)
        return _read[T](c)

    def end(mut self) raises DeserializationError:
        pass


# ===--------------------------------------------------------------------===#
# Writing: the vector tree
# ===--------------------------------------------------------------------===#


struct _WNode(Copyable, Movable):
    var vec: duckdb_v2_vector_handle  # NULL for the root
    var meta: ColumnMeta
    # Mutable data, fetched on first use. Reset when the vector (or an
    # ancestor) is resized, which may move it.
    var data: MutRawData
    var array_size: Int
    var children: List[Int]
    var lower_names: List[String]
    # Elements in use and the vector's current size (element vectors of
    # LIST and MAP nodes, which grow as values are written).
    var used: Int
    var reserved: Int
    # Field index -> child position (-1: no such child) for the struct type
    # last written to this node.
    var map_key: String
    var field_map: List[Int]

    def __init__(out self, vec: duckdb_v2_vector_handle, var meta: ColumnMeta):
        self.vec = vec
        self.meta = meta^
        self.data = null[MutRawData]()
        self.array_size = 0
        self.children = List[Int]()
        self.lower_names = List[String]()
        self.used = 0
        self.reserved = 0
        self.map_key = String()
        self.field_map = List[Int]()


struct _WriteTree(Movable):
    var nodes: List[_WNode]

    def __init__(out self):
        self.nodes = List[_WNode]()

    def _add(mut self, vec: duckdb_v2_vector_handle, type: LogicalType) raises DuckDBError -> Int:
        var meta = ColumnMeta(type)
        var kind = meta.kind
        var idx = len(self.nodes)
        self.nodes.append(_WNode(vec, meta^))
        if not _is_nested(kind):
            return idx
        if kind == TypeId.MAP:
            var entries = self._add(_child_vector(vec, 0), _entries_type(type))
            self.nodes[idx].children.append(entries)
            return idx
        var children = type.child_types()
        if kind == TypeId.LIST or kind == TypeId.ARRAY:
            if kind == TypeId.ARRAY:
                self.nodes[idx].array_size = type.array_size()
            var elements = self._add(_child_vector(vec, 0), children[0][1])
            self.nodes[idx].children.append(elements)
            return idx
        var first = 0
        if kind == TypeId.UNION:
            var tag = self._add(_child_vector(vec, 0), LogicalType(TypeId.UTINYINT))
            self.nodes[idx].children.append(tag)
            first = 1
        for i in range(len(children)):
            var child = self._add(_child_vector(vec, i + first), children[i][1])
            self.nodes[idx].children.append(child)
            self.nodes[idx].lower_names.append(children[i][0].lower())
        return idx

    def data(mut self, node: Int) raises DuckDBError -> MutRawData:
        ref n = self.nodes[node]
        if Int(n.data) == 0:
            var err = null_error_info()
            check(lib().duckdb_v2_vector_get_data_mutable(n.vec, Pointer(to=n.data), Pointer(to=err)), err)
        return n.data

    def forget_data(mut self, node: Int):
        """Drop cached data pointers of a node and everything below it."""
        var pending = List[Int]()
        pending.append(node)
        while len(pending) > 0:
            var n = pending.pop()
            self.nodes[n].data = null[MutRawData]()
            pending.extend(self.nodes[n].children.copy())

    def take(mut self, node: Int, n: Int) raises DuckDBError -> Int:
        """Take `n` more elements of an element vector; returns the first one."""
        ref c = self.nodes[node]
        var start = c.used
        c.used += n
        if c.used > c.reserved:
            var size = max(c.used, 2 * c.reserved, 64)
            var err = null_error_info()
            check(lib().duckdb_v2_vector_set_size(c.vec, idx_t(size), Pointer(to=err)), err)
            c.reserved = size
            self.forget_data(node)
        return start

    def set_null(self, node: Int, row: Int) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_vector_set_null(self.nodes[node].vec, idx_t(row), Pointer(to=err)), err)

    def set_valid(mut self, node: Int, row: Int) raises DuckDBError:
        """Mark a row valid again, with STRUCT/UNION/ARRAY children (after a failed write)."""
        ref n = self.nodes[node]
        if Int(n.vec) != 0:
            var mask = null[Pointer[UInt64, MutUntrackedOrigin]]()
            var err = null_error_info()
            check(lib().duckdb_v2_vector_flat_get_validity_mutable(n.vec, Pointer(to=mask), Pointer(to=err)), err)
            if Int(mask) != 0:
                mask[unsafe_offset=row >> 6] |= UInt64(1) << UInt64(row & 63)
        var kind = n.meta.kind
        if kind == TypeId.STRUCT or kind == TypeId.UNION:
            for i in range(len(self.nodes[node].children)):
                self.set_valid(self.nodes[node].children[i], row)
        elif kind == TypeId.ARRAY:
            var size = self.nodes[node].array_size
            for i in range(size):
                self.set_valid(self.nodes[node].children[0], row * size + i)

    def finish(self, node: Int) raises DuckDBError:
        """Size the element vectors below `node` to what is in use."""
        ref n = self.nodes[node]
        var kind = n.meta.kind
        if kind == TypeId.LIST or kind == TypeId.MAP:
            ref c = self.nodes[n.children[0]]
            var err = null_error_info()
            check(lib().duckdb_v2_vector_set_size(c.vec, idx_t(c.used), Pointer(to=err)), err)
        for i in range(len(n.children)):
            self.finish(n.children[i])


struct ChunkWriter(Movable, Sized):
    """A data chunk filled row by row through the type mapping.

    Values are written in DuckDB's storage layout where the Mojo type matches
    the column type, and else as a value DuckDB casts (an `Int` into an
    INTEGER column, a `String` into an ENUM column, ...).
    """

    var _handle: duckdb_v2_data_chunk_handle
    var _types: List[LogicalType]
    var _names: List[String]
    var _tree: _WriteTree
    var _capacity: Int
    var _rows: Int
    var _visible: Int  # columns of a row; the others are written by index only
    var _columns: List[Int]  # tree node of each column

    def __init__(
        out self,
        var types: List[LogicalType],
        var names: List[String],
        factory: duckdb_v2_factory_handle,
        visible: Optional[Int] = None,
    ) raises DuckDBError:
        """A chunk with the given column types. With `visible`, only the
        first `visible` columns make up a row for `write_row`; the rest are
        extra columns written with `write_cell`."""
        self._visible = visible.value() if visible else len(types)
        self._columns = List[Int]()
        self._types = types^
        self._names = names^
        self._handle = null[duckdb_v2_data_chunk_handle]()
        self._tree = _WriteTree()
        self._capacity = 0
        self._rows = 0
        var handles = List[duckdb_v2_logical_type_handle]()
        for i in range(len(self._types)):
            handles.append(self._types[i].handle())
        var err = null_error_info()
        check(
            lib().duckdb_v2_data_chunk_create(
                factory, handles.unsafe_ptr().as_imm(), idx_t(len(handles)), Pointer(to=self._handle), Pointer(to=err)
            ),
            err,
        )
        _ = handles^
        var cap: idx_t = 0
        check(lib().duckdb_v2_data_chunk_get_capacity(self._handle, Pointer(to=cap), Pointer(to=err)), err)
        self._capacity = Int(cap)
        self._build()

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle
        self._types = move._types^
        self._names = move._names^
        self._tree = move._tree^
        self._capacity = move._capacity
        self._rows = move._rows
        self._visible = move._visible
        self._columns = move._columns^

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_data_chunk_destroy(Pointer(to=self._handle))

    def _build(mut self) raises DuckDBError:
        """Build the write tree; size the columns to the capacity (NULL writes check it)."""
        self._tree = _WriteTree()
        self._columns = List[Int]()
        self._tree.nodes.append(_WNode(null[duckdb_v2_vector_handle](), ColumnMeta(kind=TypeId.STRUCT)))
        var err = null_error_info()
        for i in range(len(self._types)):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_data_chunk_get_vector(self._handle, idx_t(i), Pointer(to=vec), Pointer(to=err)), err)
            check(lib().duckdb_v2_vector_set_size(vec, idx_t(self._capacity), Pointer(to=err)), err)
            var node = self._tree._add(vec, self._types[i])
            self._columns.append(node)
            if i < self._visible:
                self._tree.nodes[0].children.append(node)
                self._tree.nodes[0].lower_names.append(self._names[i].lower())

    def handle(self) -> duckdb_v2_data_chunk_handle:
        """The chunk (borrowed). Call `finish` before handing it to DuckDB."""
        return self._handle

    def __len__(self) -> Int:
        """Complete rows written."""
        return self._rows

    def column_count(self) -> Int:
        return len(self._types)

    def is_full(self) -> Bool:
        return self._rows >= self._capacity

    def _begin(mut self) raises DuckDBError:
        if self._rows >= self._capacity:
            raise api_error("the chunk is full")

    def _recover(mut self, node: Int):
        """Undo NULLs a failed write left at the current row of `node`, so
        the row can be written again."""
        try:
            self._tree.set_valid(node, self._rows)
        except:
            pass

    def write_row[T: AnyType](mut self, value: T) raises DuckDBError:
        """Write a whole row: a struct (fields to columns by name; columns
        without a field become NULL) or a `Tuple` (by position)."""
        self._begin()
        var w = _Writer(Pointer(to=self._tree).unsafe_origin_cast[MutUntrackedOrigin](), 0, self._rows)
        try:
            _write(w, value)
        except e:
            self._recover(0)
            raise _from_ser(e, String("row ", self._rows))
        self._rows += 1

    def write_cell[T: AnyType](mut self, col: Int, value: T) raises DuckDBError:
        """Write one value of the current row (complete it with `end_row`)."""
        self._begin()
        if col < 0 or col >= len(self._types):
            raise api_error(String("column index ", col, " out of range (", len(self._types), " columns)"))
        var node = self._columns[col]
        var w = _Writer(Pointer(to=self._tree).unsafe_origin_cast[MutUntrackedOrigin](), node, self._rows)
        try:
            _write(w, value)
        except e:
            self._recover(node)
            raise _from_ser(e, String("row ", self._rows, ", column ", self._names[col]))

    def store_int32(mut self, col: Int, value: Int32) raises DuckDBError:
        """Write an INTEGER cell of the current row directly (no type checks;
        the column must be INTEGER)."""
        self._tree.data(self._columns[col]).unsafe_bitcast[Int32]()[unsafe_offset=self._rows] = value

    def write_null(mut self, col: Int) raises DuckDBError:
        self._begin()
        if col < 0 or col >= len(self._types):
            raise api_error(String("column index ", col, " out of range (", len(self._types), " columns)"))
        self._tree.set_null(self._columns[col], self._rows)

    def end_row(mut self):
        self._rows += 1

    def finish(mut self) raises DuckDBError:
        """Size every vector to the rows written, for handing the chunk to DuckDB."""
        var err = null_error_info()
        for i in range(len(self._columns)):
            var node = self._columns[i]
            check(lib().duckdb_v2_vector_set_size(self._tree.nodes[node].vec, idx_t(self._rows), Pointer(to=err)), err)
            self._tree.finish(node)

    def reset(mut self) raises DuckDBError:
        """Empty the chunk for the next batch of rows."""
        var err = null_error_info()
        check(lib().duckdb_v2_data_chunk_clear(self._handle, Pointer(to=err)), err)
        self._rows = 0
        self._build()

    def vector(self, col: Int) -> duckdb_v2_vector_handle:
        return self._tree.nodes[self._columns[col]].vec


struct VectorWriter(Movable):
    """Writes the values of one vector DuckDB hands out to be filled (the
    result of a scalar function), `size` rows of it.

    Sizes the vector first: DuckDB creates it with size 0, and NULL writes
    check the row against the size.
    """

    var _tree: _WriteTree
    var _size: Int
    var _vec: duckdb_v2_vector_handle
    var _data: MutRawData  # NULL for nested results
    var _meta: Pointer[ColumnMeta, MutUntrackedOrigin]  # into the tree, which is not resized after init

    def __init__(out self, vec: duckdb_v2_vector_handle, size: Int) raises DuckDBError:
        self._size = size
        self._vec = vec
        var err = null_error_info()
        check(lib().duckdb_v2_vector_set_size(vec, idx_t(size), Pointer(to=err)), err)
        self._tree = _WriteTree()
        self._tree.nodes.append(_WNode(null[duckdb_v2_vector_handle](), ColumnMeta(kind=TypeId.STRUCT)))
        var node = self._tree._add(vec, _vector_type(vec))
        self._tree.nodes[0].children.append(node)
        self._data = null[MutRawData]()
        if not _is_nested(self._tree.nodes[1].meta.kind):
            self._data = self._tree.data(1)
        self._meta = Pointer(to=self._tree.nodes[1].meta).unsafe_origin_cast[MutUntrackedOrigin]()

    def __len__(self) -> Int:
        return self._size

    def write[T: AnyType](mut self, row: Int, value: T) raises DuckDBError:
        """Write `value` at `row` (an `Optional` without a value writes NULL)."""
        comptime if is_mapped[T]():
            if Int(self._data) != 0:
                mapped_write[T](value, self._meta[], self._vec, self._data, row)
                return
        var w = _Writer(Pointer(to=self._tree).unsafe_origin_cast[MutUntrackedOrigin](), 1, row)
        try:
            _write(w, value)
        except e:
            raise _from_ser(e, String("row ", row))

    def write_null(mut self, row: Int) raises DuckDBError:
        self._tree.set_null(1, row)

    def data[T: TrivialRegisterPassable](mut self) raises DuckDBError -> Pointer[T, MutUntrackedOrigin]:
        """The vector's data as an array of `T`, for writing fixed-width values directly."""
        return self._tree.data(1).unsafe_bitcast[T]()

    def finish(self) raises DuckDBError:
        """Size the element vectors of LIST/MAP results to what was written."""
        self._tree.finish(1)


# ===--------------------------------------------------------------------===#
# Writing: the format
# ===--------------------------------------------------------------------===#


comptime _TreePtr = Pointer[_WriteTree, MutUntrackedOrigin]


@always_inline
def _write[T: AnyType](mut w: _Writer, value: T) raises SerializationError:
    """Write `value` at the writer: in DuckDB's layout for scalars, else through emberserde."""
    comptime if is_mapped[T]():
        if w.node == 0:
            raise _ser_invalid("a row needs a struct or a Tuple, not a single value")
        try:
            ref node = w.tree[].nodes[w.node]
            var data = null[MutRawData]()
            if not _is_nested(node.meta.kind):
                data = w.tree[].data(w.node)
            mapped_write[T](value, node.meta, node.vec, data, w.row)
        except e:
            raise _ser_error(e)
    else:
        serialize(value, w)


struct _Writer(Serializer):
    """A position (node, row) in a write tree."""

    var tree: _TreePtr
    var node: Int
    var row: Int

    comptime SeqType = _SeqSer
    comptime MapType = _MapSer
    comptime StructType = _StructSer
    comptime TupleType = _TupleSer
    comptime EnumType = _EnumSer

    def __init__(out self, tree: _TreePtr, node: Int, row: Int):
        self.tree = tree
        self.node = node
        self.row = row

    def _kind(self) -> TypeId:
        return self.tree[].nodes[self.node].meta.kind

    def serialize_bool(mut self, v: Bool) raises SerializationError:
        _write(self, v)

    def serialize_number[dt: DType, //](mut self, v: Scalar[dt]) raises SerializationError:
        _write(self, v)

    def serialize_string(mut self, v: StringSlice) raises SerializationError:
        _write(self, String(v))

    def serialize_bytes(mut self, v: Span[Byte, _]) raises SerializationError:
        var data = List[UInt8](capacity=len(v))
        for b in v:
            data.append(b)
        _write(self, Blob(data^))

    def serialize_none(mut self) raises SerializationError:
        if self.node == 0:
            raise _ser_invalid("a row cannot be NULL")
        try:
            self.tree[].set_null(self.node, self.row)
        except e:
            raise _ser_error(e)

    def serialize_some(mut self, v: Some[AnyType]) raises SerializationError:
        _write(self, v)

    def _list_range(mut self, size_hint: Optional[Int]) raises SerializationError -> Tuple[Int, Int]:
        """(element node, first element) for a list of `size_hint` elements at this row."""
        var kind = self._kind()
        if kind != TypeId.LIST and kind != TypeId.ARRAY and kind != TypeId.MAP:
            raise _ser_invalid(String("a sequence cannot be written to a ", kind, " column"))
        if not size_hint:
            raise _ser_invalid("sequences need a known length")
        var n = size_hint.value()
        var child = self.tree[].nodes[self.node].children[0]
        if kind == TypeId.ARRAY:
            var size = self.tree[].nodes[self.node].array_size
            if n != size:
                raise _ser_invalid(String("a sequence of ", n, " elements cannot be written to an ARRAY of ", size))
            return (child, self.row * size)
        try:
            var start = self.tree[].take(child, n)
            var data = self.tree[].data(self.node)
            data.unsafe_bitcast[duckdb_v2_list_entry]()[unsafe_offset=self.row] = duckdb_v2_list_entry(
                UInt64(start), UInt64(n)
            )
            return (child, start)
        except e:
            raise _ser_error(e)

    def begin_seq(mut self, size_hint: Optional[Int] = None) raises SerializationError -> Self.SeqType:
        var r = self._list_range(size_hint)
        return _SeqSer(self.tree, r[0], r[1])

    def begin_map(mut self, size_hint: Optional[Int] = None) raises SerializationError -> Self.MapType:
        if self._kind() != TypeId.MAP:
            raise _ser_invalid(String("a map cannot be written to a ", self._kind(), " column"))
        var r = self._list_range(size_hint)
        ref entries = self.tree[].nodes[r[0]]
        return _MapSer(self.tree, entries.children[0], entries.children[1], r[1])

    def begin_struct[T: AnyType](mut self, field_count: Int) raises SerializationError -> Self.StructType:
        if self._kind() != TypeId.STRUCT:
            raise _ser_invalid(String("a struct cannot be written to a ", self._kind(), " column"))
        _map_write_fields[T](self.tree[].nodes[self.node])
        return _StructSer(self.tree, self.node, self.row, List[Bool](length=len(self.tree[].nodes[self.node].children), fill=False))

    def begin_tuple[field_count: Int](mut self) raises SerializationError -> Self.TupleType:
        if self._kind() == TypeId.STRUCT:
            var n = len(self.tree[].nodes[self.node].children)
            if n != field_count:
                raise _ser_invalid(String("a tuple of ", field_count, " cannot be written to ", n, " fields"))
            return _TupleSer(self.tree, self.node, -1, self.row)
        var r = self._list_range(field_count)
        return _TupleSer(self.tree, self.node, r[0], r[1])

    def begin_enum[T: AnyType, variant: String](mut self, idx: UInt32) raises SerializationError -> Self.EnumType:
        if self._kind() != TypeId.UNION:
            raise _ser_invalid(String("a Variant cannot be written to a ", self._kind(), " column"))
        ref node = self.tree[].nodes[self.node]
        var members = len(node.children) - 1
        var arm = Int(idx)
        var member = arm
        comptime if conforms_to(T, _VariantArms):
            comptime key = reflect[T].name()
            if node.map_key != key:
                var kinds = List[TypeId]()
                for m in range(1, len(node.children)):
                    kinds.append(self.tree[].nodes[node.children[m]].meta.kind)
                node.field_map = _match_union[T](node.lower_names, kinds)
                node.map_key = String(key)
            member = -1
            for m in range(members):
                if node.field_map[m] == arm:
                    member = m if member == -1 else -2
            if member < 0:
                raise _ser_invalid(
                    String(
                        "Variant arm ", arm, " (", variant, ") matches ", "no" if member == -1 else "several", " UNION members"
                    )
                )
        else:
            if arm >= members:
                raise _ser_invalid(String("Variant arm ", arm, " has no UNION member (", members, " members)"))
        var tag = _Writer(self.tree, node.children[0], self.row)
        _write(tag, UInt8(member))
        try:
            for m in range(members):
                if m != member:
                    self.tree[].set_null(node.children[m + 1], self.row)
        except e:
            raise _ser_error(e)
        return _EnumSer(self.tree, node.children[member + 1], self.row)


def _map_write_fields[T: AnyType](mut node: _WNode):
    """Map `T`'s fields to the node's children by wire name, case-insensitively."""
    comptime key = reflect[T].name()
    if node.map_key == key:
        return
    node.field_map = List[Int]()
    comptime for i in range(reflect[T].field_count()):
        var found = -1
        comptime if not is_skipped[T, i]():
            var name = String(static_wire_name[T, i]()).lower()
            for k in range(len(node.lower_names)):
                if found == -1 and node.lower_names[k] == name:
                    found = k
        node.field_map.append(found)
    node.map_key = String(key)


@fieldwise_init
struct _SeqSer(SeqSerState):
    var tree: _TreePtr
    var child: Int
    var pos: Int

    def serialize_element(mut self, v: Some[AnyType]) raises SerializationError:
        var w = _Writer(self.tree, self.child, self.pos)
        self.pos += 1
        _write(w, v)

    def end(mut self) raises SerializationError:
        pass


@fieldwise_init
struct _MapSer(MapSerState):
    var tree: _TreePtr
    var keys: Int
    var values: Int
    var pos: Int

    def serialize_key(mut self, k: Some[AnyType]) raises SerializationError:
        var w = _Writer(self.tree, self.keys, self.pos)
        _write(w, k)

    def serialize_value(mut self, v: Some[AnyType]) raises SerializationError:
        var w = _Writer(self.tree, self.values, self.pos)
        self.pos += 1
        _write(w, v)

    def end(mut self) raises SerializationError:
        pass


@fieldwise_init
struct _StructSer(StructSerState):
    var tree: _TreePtr
    var node: Int
    var row: Int
    var written: List[Bool]

    def serialize_field[T: AnyType, idx: Int](mut self, field_name: StringSlice, v: Some[AnyType]) raises SerializationError:
        var k = self.tree[].nodes[self.node].field_map[idx]
        if k == -1:
            raise _ser_invalid(String("no column or STRUCT field named ", field_name))
        self.written[k] = True
        var w = _Writer(self.tree, self.tree[].nodes[self.node].children[k], self.row)
        try:
            _write(w, v)
        except e:
            raise SerializationError(String(".", field_name, ": ", e.message), e.kind)

    def end(mut self) raises SerializationError:
        # Fields the struct does not have are NULL.
        try:
            for k in range(len(self.written)):
                if not self.written[k]:
                    self.tree[].set_null(self.tree[].nodes[self.node].children[k], self.row)
        except e:
            raise _ser_error(e)


@fieldwise_init
struct _TupleSer(TupleSerState):
    var tree: _TreePtr
    var node: Int
    # -1: STRUCT fields by position at `pos`; else the element node of a
    # list, with the elements from `pos` on.
    var seq_child: Int
    var pos: Int
    var next: Int

    def __init__(out self, tree: _TreePtr, node: Int, seq_child: Int, pos: Int):
        self.tree = tree
        self.node = node
        self.seq_child = seq_child
        self.pos = pos
        self.next = 0

    def serialize_element(mut self, v: Some[AnyType]) raises SerializationError:
        var i = self.next
        self.next += 1
        if self.seq_child >= 0:
            var w = _Writer(self.tree, self.seq_child, self.pos + i)
            _write(w, v)
        else:
            var w = _Writer(self.tree, self.tree[].nodes[self.node].children[i], self.pos)
            _write(w, v)

    def end(mut self) raises SerializationError:
        pass


@fieldwise_init
struct _EnumSer(EnumSerState):
    var tree: _TreePtr
    var member: Int
    var row: Int

    def serialize_payload(mut self, v: Some[AnyType]) raises SerializationError:
        var w = _Writer(self.tree, self.member, self.row)
        _write(w, v)

    def end(mut self) raises SerializationError:
        pass


# ===--------------------------------------------------------------------===#
# Values of any mapped type
# ===--------------------------------------------------------------------===#


def to_value[T: AnyType](value: T, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
    """An owned DuckDB value from any mapped Mojo type, nested ones included."""
    comptime if is_mapped[T]():
        return mapped_to_value(value, factory)
    else:
        # Write it into a one-row chunk and take the cell as a value.
        var w = ChunkWriter([logical_type_of[T]()], ["v"], factory)
        w.write_cell(0, value)
        w.end_row()
        w.finish()
        var out = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_vector_get_value(w.vector(0), 0, Pointer(to=out), Pointer(to=err)), err)
        _ = w^
        return out


def from_value[T: Movable & Deinitable](value: duckdb_v2_value_handle) raises DuckDBError -> T:
    """Read a (non-NULL) DuckDB value as any mapped Mojo type, nested ones included."""
    comptime if is_mapped[T]():
        return mapped_from_value[T](value)
    else:
        var t = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_logical_type(value, Pointer(to=t), Pointer(to=err)), err)
        var type = LogicalType(_owned=t)
        var w = ChunkWriter([type.copy()], ["v"], builtin_factory())
        check(lib().duckdb_v2_vector_set_value(w.vector(0), 0, value, Pointer(to=err)), err)
        w.end_row()
        w.finish()
        var tree = _ReadTree([w.vector(0)], ["v"], [type^])
        var out = read_value[T](tree, 1, 0)
        _ = tree^
        _ = w^
        return out^
