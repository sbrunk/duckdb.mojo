"""Proof of concept: reading DuckDB V2 result chunks through emberserde.

Evaluates whether emberserde can carry the struct and container side of the
type mapping (docs/design/v2-high-level-api.md, "Substrate: emberserde").
Read-only. Not part of the public API.

A chunk is turned into a tree of vector views once (`_Tree`): one node per
column, plus children for LIST and STRUCT columns. Scalar columns are read
through their view as they are (flat, constant or dictionary); LIST and STRUCT
columns are flattened first, so their children line up with the parent row.
The root node is a virtual STRUCT whose children are the columns, so a whole
row reads like a STRUCT value.

`DuckDBCursor` is the emberserde format: a node plus a row. DuckDB types with
their own storage layout (here: `Date`) are read natively through
`DuckDBNative`, intercepted before emberserde's own dispatch.
"""

from std.builtin.rebind import downcast, rebind_var
from std.reflection import reflect
from emberserde import (
    DeserializationError,
    DerErrorKind,
    Deserializer,
    SeqDerState,
    StructDerState,
    Unsupported,
    deserialize,
)
from emberserde.field_meta import is_skipped, name_matches, static_wire_name
from emberserde.utils import Base
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import DuckDBV2, StrArg, check, null, null_error_info, str_to_string
from duckdb.duckdb_type import Date


# ===--------------------------------------------------------------------===#
# Native DuckDB types
# ===--------------------------------------------------------------------===#


trait DuckDBNative(Base):
    """A Mojo type read straight from a DuckDB storage layout.

    The cursor checks for this before handing a type to emberserde, so these
    types never go through the generic data model.
    """

    @staticmethod
    def read_duckdb(
        kind: DUCKDB_V2_LOGICAL_TYPE_ID, data: Pointer[NoneType, ImmUntrackedOrigin], idx: Int
    ) raises DeserializationError -> Self:
        ...


__extension Date(DuckDBNative):
    @staticmethod
    def read_duckdb(
        kind: DUCKDB_V2_LOGICAL_TYPE_ID, data: Pointer[NoneType, ImmUntrackedOrigin], idx: Int
    ) raises DeserializationError -> Self:
        if kind != DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
            raise _type_mismatch("DATE", kind)
        return Date(data.unsafe_bitcast[Int32]()[unsafe_offset=idx])


# ===--------------------------------------------------------------------===#
# Errors (out of line: only reached on failure)
# ===--------------------------------------------------------------------===#


@no_inline
def _type_mismatch(expected: StaticString, kind: DUCKDB_V2_LOGICAL_TYPE_ID) -> DeserializationError:
    return DeserializationError(
        String("expected a ", expected, " column, got type id ", kind), DerErrorKind.TypeMismatch
    )


@no_inline
def _unexpected_null() -> DeserializationError:
    return DeserializationError(
        String("unexpected NULL (read the column as Optional[...] to allow NULLs)"),
        DerErrorKind.InvalidValue,
    )


@no_inline
def _api_error(e: Error) -> DeserializationError:
    return DeserializationError(String(e), DerErrorKind.Custom)


# ===--------------------------------------------------------------------===#
# Vector tree
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct _Node(Copyable, Movable):
    var kind: DUCKDB_V2_LOGICAL_TYPE_ID
    # Vector representation before any flattening (diagnostics for the POC).
    var vector_type: DUCKDB_V2_VECTOR_TYPE
    var data: Pointer[NoneType, ImmUntrackedOrigin]
    var validity: Pointer[UInt64, ImmUntrackedOrigin]
    var sel: Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]
    var children: List[Int]
    # STRUCT (and root) only: child names, as given and lowercased.
    var names: List[String]
    var lower_names: List[String]
    # Child index -> field index of the struct type last read from this node
    # (-1: no field). Recomputed when a different type is read.
    var map_key: String
    var field_map: List[Int]


struct _Tree(Movable):
    var nodes: List[_Node]
    var size: Int

    def __init__(out self):
        self.nodes = List[_Node]()
        self.size = 0


def _type_id(t: duckdb_v2_logical_type_handle) raises -> DUCKDB_V2_LOGICAL_TYPE_ID:
    ref lib = DuckDBV2().libduckdb()
    var err = null_error_info()
    var id: DUCKDB_V2_LOGICAL_TYPE_ID = 0
    check(lib.duckdb_v2_logical_type_get_id(t, Pointer(to=id), Pointer(to=err)), err)
    return id


def _add_node(
    mut tree: _Tree, vec: duckdb_v2_vector_handle, ltype: duckdb_v2_logical_type_handle
) raises -> Int:
    ref lib = DuckDBV2().libduckdb()
    var err = null_error_info()
    var kind = _type_id(ltype)
    var vtype: DUCKDB_V2_VECTOR_TYPE = DUCKDB_V2_VECTOR_TYPE_OTHER
    check(lib.duckdb_v2_vector_get_vector_type(vec, Pointer(to=vtype), Pointer(to=err)), err)
    var nested = kind == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT or kind == DUCKDB_V2_LOGICAL_TYPE_ID_LIST
    if vtype == DUCKDB_V2_VECTOR_TYPE_OTHER or (nested and vtype != DUCKDB_V2_VECTOR_TYPE_FLAT):
        check(lib.duckdb_v2_vector_flatten(vec, Pointer(to=err)), err)
    var view = duckdb_v2_vector_view(
        null[Pointer[NoneType, ImmUntrackedOrigin]](),
        null[Pointer[UInt64, ImmUntrackedOrigin]](),
        null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
        0,
    )
    check(lib.duckdb_v2_vector_get_view(vec, Pointer(to=view), Pointer(to=err)), err)
    var idx = len(tree.nodes)
    tree.nodes.append(
        _Node(kind, vtype, view.data, view.validity, view.sel, [], [], [], "", [])
    )
    if not nested:
        return idx

    var count: idx_t = 0
    check(lib.duckdb_v2_logical_type_get_param_count(ltype, Pointer(to=count), Pointer(to=err)), err)
    for i in range(Int(count)):
        var name = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var value = null[duckdb_v2_value_handle]()
        check(
            lib.duckdb_v2_logical_type_get_param(ltype, idx_t(i), Pointer(to=name), Pointer(to=value), Pointer(to=err)),
            err,
        )
        var child_type = null[duckdb_v2_logical_type_handle]()
        check(lib.duckdb_v2_value_get_type(value, Pointer(to=child_type), Pointer(to=err)), err)
        var child_vec = null[duckdb_v2_vector_handle]()
        check(lib.duckdb_v2_vector_get_child(vec, idx_t(i), Pointer(to=child_vec), Pointer(to=err)), err)
        var child = _add_node(tree, child_vec, child_type)
        tree.nodes[idx].children.append(child)
        if kind == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
            var n = str_to_string(name)
            tree.nodes[idx].lower_names.append(n.lower())
            tree.nodes[idx].names.append(n^)
        _ = lib.duckdb_v2_logical_type_destroy(Pointer(to=child_type))
        _ = lib.duckdb_v2_value_destroy(Pointer(to=value))
    return idx


# ===--------------------------------------------------------------------===#
# The format
# ===--------------------------------------------------------------------===#


@always_inline
def _read[
    T: AnyType, o: MutOrigin
](mut c: DuckDBCursor[o]) raises DeserializationError -> T:
    """Read `T` at the cursor: natively for DuckDB types, else through emberserde."""
    comptime if conforms_to(T, DuckDBNative):
        comptime N = downcast[T, DuckDBNative]
        ref node = c.tree[].nodes[c.node]
        var idx = c._phys()
        if not c._valid(idx):
            raise _unexpected_null()
        return rebind_var[T](N.read_duckdb(node.kind, node.data, idx))
    else:
        return deserialize[T](c)


struct DuckDBCursor[o: MutOrigin](Deserializer):
    """An emberserde deserializer positioned at one row of one tree node."""

    var tree: Pointer[_Tree, Self.o]
    var node: Int
    var row: Int

    comptime SeqType = DuckDBListDe[Self.o]
    comptime StructType = DuckDBStructDe[Self.o]
    comptime MapType = Unsupported["maps (POC)"]
    comptime TupleType = Unsupported["tuples (POC)"]
    comptime EnumType = Unsupported["unions (POC)"]

    def __init__(out self, tree: Pointer[_Tree, Self.o], node: Int, row: Int):
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

    @always_inline
    def _value_index(self, expected: DUCKDB_V2_LOGICAL_TYPE_ID, name: StaticString) raises DeserializationError -> Int:
        var kind = self.tree[].nodes[self.node].kind
        if kind != expected:
            raise _type_mismatch(name, kind)
        var idx = self._phys()
        if not self._valid(idx):
            raise _unexpected_null()
        return idx

    def expect_bool(mut self) raises DeserializationError -> Bool:
        var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, "BOOLEAN")
        return self.tree[].nodes[self.node].data.unsafe_bitcast[Bool]()[unsafe_offset=idx]

    def expect_number[DT: DType](mut self) raises DeserializationError -> Scalar[DT]:
        comptime if DT == DType.int32:
            var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, "INTEGER")
            return self.tree[].nodes[self.node].data.unsafe_bitcast[Scalar[DT]]()[unsafe_offset=idx]
        elif DT == DType.int64 or DT == DType.int:
            var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, "BIGINT")
            return self.tree[].nodes[self.node].data.unsafe_bitcast[Int64]()[unsafe_offset=idx].cast[DT]()
        elif DT == DType.float64:
            var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, "DOUBLE")
            return self.tree[].nodes[self.node].data.unsafe_bitcast[Scalar[DT]]()[unsafe_offset=idx]
        else:
            comptime assert False, "POC: number type not supported"
            return Scalar[DT]()

    def expect_string(mut self) raises DeserializationError -> String:
        var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, "VARCHAR")
        var base = self.tree[].nodes[self.node].data.unsafe_bitcast[UInt8]().unsafe_offset(idx * 16)
        var length = Int(base.unsafe_bitcast[UInt32]()[])
        var bytes: Pointer[UInt8, ImmUntrackedOrigin]
        if length <= Int(DUCKDB_V2_BYTES_INLINE_LENGTH):
            bytes = base.unsafe_offset(4)
        else:
            bytes = base.unsafe_offset(8).unsafe_bitcast[Pointer[UInt8, ImmUntrackedOrigin]]()[]
        return String(from_utf8_lossy=Span[UInt8, ImmUntrackedOrigin](unsafe_ptr=bytes, length=length))

    def expect_optional[T: Base](mut self) raises DeserializationError -> Optional[T]:
        if not self._valid(self._phys()):
            return None
        return Optional[T](_read[T](self))

    def begin_seq(mut self) raises DeserializationError -> Self.SeqType:
        var idx = self._value_index(DUCKDB_V2_LOGICAL_TYPE_ID_LIST, "LIST")
        ref node = self.tree[].nodes[self.node]
        ref entry = node.data.unsafe_bitcast[duckdb_v2_list_entry]()[unsafe_offset=idx]
        return DuckDBListDe[Self.o](self.tree, node.children[0], Int(entry.offset), Int(entry.offset + entry.length))

    def begin_map(mut self) raises DeserializationError -> Self.MapType:
        return Self.MapType()

    def begin_struct[T: AnyType](mut self) raises DeserializationError -> Self.StructType:
        # The root (node 0) is the row; it has no validity of its own.
        var kind = self.tree[].nodes[self.node].kind
        if kind != DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
            raise _type_mismatch("STRUCT", kind)
        if not self._valid(self._phys()):
            raise _unexpected_null()
        _map_fields[T](self.tree[].nodes[self.node])
        return DuckDBStructDe[Self.o](self.tree, self.node, self._phys(), 0)

    def begin_tuple[field_count: Int](mut self) raises DeserializationError -> Self.TupleType:
        return Self.TupleType()

    def begin_enum[T: AnyType, arm_names: List[String]](mut self) raises DeserializationError -> Self.EnumType:
        return Self.EnumType()


def _map_fields[T: AnyType](mut node: _Node):
    """Map the node's children to `T`'s fields, once per struct type.

    Exact name (or `Alias`) first, through emberserde, then case-insensitive
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
struct DuckDBListDe[o: MutOrigin](SeqDerState):
    var tree: Pointer[_Tree, Self.o]
    var child: Int
    var pos: Int
    var end_pos: Int

    def has_next(mut self) raises DeserializationError -> Bool:
        return self.pos < self.end_pos

    def expect_element[T: AnyType](mut self) raises DeserializationError -> T:
        var c = DuckDBCursor[Self.o](self.tree, self.child, self.pos)
        self.pos += 1
        return _read[T](c)

    def end(mut self) raises DeserializationError:
        pass


@fieldwise_init
struct DuckDBStructDe[o: MutOrigin](StructDerState):
    var tree: Pointer[_Tree, Self.o]
    var node: Int
    # Physical row of the struct; children of a flat STRUCT (and the root's
    # columns) are addressed with it.
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
        var child = self.tree[].nodes[self.node].children[self.next_child - 1]
        var c = DuckDBCursor[Self.o](self.tree, child, self.row)
        return _read[T](c)

    def skip_value(mut self) raises DeserializationError:
        pass

    def end(mut self) raises DeserializationError:
        pass


# ===--------------------------------------------------------------------===#
# Query plumbing (just enough for the POC)
# ===--------------------------------------------------------------------===#


struct PocDatabase(Movable):
    """An environment, an in-memory instance and one connection."""

    var env: duckdb_v2_environment_handle
    var instance: duckdb_v2_instance_handle
    var conn: duckdb_v2_connection_handle

    def __init__(out self) raises:
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        self.env = null[duckdb_v2_environment_handle]()
        self.instance = null[duckdb_v2_instance_handle]()
        self.conn = null[duckdb_v2_connection_handle]()
        check(lib.duckdb_v2_environment_create(Pointer(to=self.env), Pointer(to=err)), err)
        check(lib.duckdb_v2_instance_create(self.env, Pointer(to=self.instance), Pointer(to=err)), err)
        var name = StrArg("memory")
        var path = StrArg(":memory:")
        check(
            lib.duckdb_v2_instance_attach(
                self.instance, name.ptr(), path.ptr(), null[duckdb_v2_attach_options_handle](), True, Pointer(to=err)
            ),
            err,
        )
        check(lib.duckdb_v2_connection_create(self.instance, Pointer(to=self.conn), Pointer(to=err)), err)

    def __deinit__(deinit self):
        ref lib = DuckDBV2().libduckdb()
        _ = lib.duckdb_v2_connection_destroy(Pointer(to=self.conn))
        _ = lib.duckdb_v2_instance_destroy(Pointer(to=self.instance))
        _ = lib.duckdb_v2_environment_destroy(Pointer(to=self.env))

    def query(mut self, sql: String) raises -> PocResult:
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        var it = null[duckdb_v2_statement_iterator_handle]()
        var text = sql.copy()
        check(lib.duckdb_v2_parse_sql(self.conn, text.as_c_string_span().ptr(), Pointer(to=it), Pointer(to=err)), err)
        _ = text^
        var stmt = null[duckdb_v2_sql_statement_handle]()
        var result = null[duckdb_v2_result_handle]()
        try:
            check(lib.duckdb_v2_statement_iterator_next(it, Pointer(to=stmt), Pointer(to=err)), err)
            check(
                lib.duckdb_v2_statement_execute(
                    self.conn,
                    stmt,
                    null[Pointer[duckdb_v2_identifier_t, ImmUntrackedOrigin]](),
                    null[Pointer[duckdb_v2_value_handle, ImmUntrackedOrigin]](),
                    0,
                    Pointer(to=result),
                    Pointer(to=err),
                ),
                err,
            )
        finally:
            _ = lib.duckdb_v2_sql_statement_destroy(Pointer(to=stmt))
            _ = lib.duckdb_v2_statement_iterator_destroy(Pointer(to=it))
        return PocResult(result)

    def run(mut self, sql: String) raises:
        """Run a statement and drain it."""
        var r = self.query(sql)
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        var changed: idx_t = 0
        check(lib.duckdb_v2_result_drain(r.result, Pointer(to=changed), Pointer(to=err)), err)
        _ = r^  # Mojo destroys values after their last use: keep the result alive past drain


struct PocResult(Movable):
    var result: duckdb_v2_result_handle
    var schema: duckdb_v2_schema_handle

    def __init__(out self, result: duckdb_v2_result_handle) raises:
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        self.result = result
        self.schema = null[duckdb_v2_schema_handle]()
        check(lib.duckdb_v2_result_get_schema(self.result, Pointer(to=self.schema), Pointer(to=err)), err)

    def __deinit__(deinit self):
        ref lib = DuckDBV2().libduckdb()
        _ = lib.duckdb_v2_schema_destroy(Pointer(to=self.schema))
        _ = lib.duckdb_v2_result_destroy(Pointer(to=self.result))

    def fetch_chunk(ref self) raises -> Optional[PocChunk[ImmOrigin(origin_of(self))]]:
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        var chunk = null[duckdb_v2_data_chunk_handle]()
        check(lib.duckdb_v2_result_fetch_chunk(self.result, Pointer(to=chunk), Pointer(to=err)), err)
        if Int(chunk) == 0:
            return None
        return PocChunk[ImmOrigin(origin_of(self))](chunk, Pointer(to=self))

    def rows[T: Base](var self) raises -> List[T]:
        """All remaining rows, each read as `T`."""
        var out = List[T]()
        while True:
            var chunk = self.fetch_chunk()
            if not chunk:
                break
            chunk.value().append_rows[T](out)
        return out^


struct PocChunk[origin: ImmOrigin](Movable):
    var chunk: duckdb_v2_data_chunk_handle
    # The result owns the schema the chunk's column types come from, so the
    # chunk keeps it alive.
    var result: Pointer[PocResult, Self.origin]

    def __init__(out self, chunk: duckdb_v2_data_chunk_handle, result: Pointer[PocResult, Self.origin]):
        self.chunk = chunk
        self.result = result

    def __deinit__(deinit self):
        ref lib = DuckDBV2().libduckdb()
        _ = lib.duckdb_v2_data_chunk_destroy(Pointer(to=self.chunk))

    def _tree(self) raises -> _Tree:
        ref lib = DuckDBV2().libduckdb()
        var err = null_error_info()
        var tree = _Tree()
        var size: idx_t = 0
        check(lib.duckdb_v2_data_chunk_get_size(self.chunk, Pointer(to=size), Pointer(to=err)), err)
        tree.size = Int(size)
        var count: idx_t = 0
        check(lib.duckdb_v2_schema_get_count(self.result[].schema, Pointer(to=count), Pointer(to=err)), err)
        tree.nodes.append(
            _Node(
                DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
                DUCKDB_V2_VECTOR_TYPE_FLAT,
                null[Pointer[NoneType, ImmUntrackedOrigin]](),
                null[Pointer[UInt64, ImmUntrackedOrigin]](),
                null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
                [],
                [],
                [],
                "",
                [],
            )
        )
        for i in range(Int(count)):
            var name = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
            var ltype = null[duckdb_v2_logical_type_handle]()  # borrowed from the schema
            check(
                lib.duckdb_v2_schema_get_field(self.result[].schema, idx_t(i), Pointer(to=name), Pointer(to=ltype), Pointer(to=err)),
                err,
            )
            var vec = null[duckdb_v2_vector_handle]()
            check(lib.duckdb_v2_data_chunk_get_vector(self.chunk, idx_t(i), Pointer(to=vec), Pointer(to=err)), err)
            var node = _add_node(tree, vec, ltype)
            tree.nodes[0].children.append(node)
            var n = str_to_string(name)
            tree.nodes[0].lower_names.append(n.lower())
            tree.nodes[0].names.append(n^)
        return tree^

    def append_rows[T: Base](self, mut out: List[T]) raises:
        """Read every row of the chunk as `T` (a struct over the columns)."""
        var tree = self._tree()
        var tp = Pointer(to=tree)
        for r in range(tree.size):
            var c = DuckDBCursor(tp, 0, r)
            try:
                out.append(_read[T](c))
            except e:
                raise Error(String("row ", r, ": ", e))

    def column_vector_types(self) raises -> List[DUCKDB_V2_VECTOR_TYPE]:
        """The original vector representation of each column (POC diagnostics)."""
        var tree = self._tree()
        var out = List[DUCKDB_V2_VECTOR_TYPE]()
        for c in tree.nodes[0].children:
            out.append(tree.nodes[c].vector_type)
        return out^

    def column[T: Base](self, col: Int) raises -> List[T]:
        """Read one column as `T` per row."""
        var tree = self._tree()
        var tp = Pointer(to=tree)
        var node = tree.nodes[0].children[col]
        var out = List[T](capacity=tree.size)
        for r in range(tree.size):
            var c = DuckDBCursor(tp, node, r)
            try:
                out.append(_read[T](c))
            except e:
                raise Error(String("row ", r, ": ", e))
        return out^
