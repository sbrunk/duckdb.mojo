"""Arrow interop through the Arrow C data interface.

`con.execute_arrow(sql)` gives Arrow arrays, or an `ArrowArrayStream` for any
consumer, and `ArrowResult.fetch_batch()` reads a batch back into Mojo values.
`con.arrow_scan(stream)` is a relation over a stream the caller supplies, and DuckDB
reads its buffers without copying. A stream hands over each batch once, and the
next read of it returns an empty array. The stream must stay alive until the
query over it has been read.

```mojo
var exported = other.execute_arrow("SELECT * FROM range(10) t(i)").into_stream()
var back = con.arrow_scan(exported).fetchall()
```

An Arrow result holds its connection until its batches are read, so a round trip
needs two connections.

The structs follow the Arrow specification and work with other Arrow libraries.
`unsafe_ptr()` gives the C struct. `release_to(dst)` moves ownership into an
empty struct the consumer allocated, and that consumer calls `release` once. An
owner releases its struct on destruction, a moved-from owner stays empty.
"""

from std.builtin.rebind import downcast, rebind_var
from std.memory.alloc import unsafe_alloc
from std.sys import size_of
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb._runtime import api_error, check, lib
from duckdb.chunk import _read_schema
from duckdb.error import DuckDBError, ErrorCode
from duckdb.functions import BindArgs
from duckdb.mapping import Blob, is_mapped, mapped_accepts
from duckdb.scalar_types import Date, Decimal, Interval, Time
from duckdb.types import TypeId
from duckdb.table import ScanShared, TableColumns, TableFunctionImpl, TableOutput


def _zeroed[T: AnyType]() -> Pointer[T, MutUntrackedOrigin]:
    """A zeroed heap `T`: a released, empty Arrow struct."""
    var p = unsafe_alloc[T](1)
    var bytes = p.unsafe_bitcast[UInt8]()
    for i in range(size_of[T]()):
        bytes[unsafe_offset=i] = 0
    return p


@always_inline
def _schema_live(p: Pointer[ArrowSchema, MutUntrackedOrigin]) -> Bool:
    return Pointer(to=p[].release).unsafe_bitcast[Int64]()[] != 0


@always_inline
def _array_live(p: Pointer[ArrowArray, MutUntrackedOrigin]) -> Bool:
    return Pointer(to=p[].release).unsafe_bitcast[Int64]()[] != 0


@always_inline
def _stream_live(p: Pointer[ArrowArrayStream, MutUntrackedOrigin]) -> Bool:
    return Pointer(to=p[].release).unsafe_bitcast[Int64]()[] != 0


@always_inline
def _move_into[T: AnyType](src: Pointer[T, MutUntrackedOrigin], dst: Pointer[T, MutUntrackedOrigin]):
    """The Arrow spec's move: copy `T` from `src` to `dst` and zero `src`, which must
    be left empty."""
    var s = src.unsafe_bitcast[UInt8]()
    var d = dst.unsafe_bitcast[UInt8]()
    for i in range(size_of[T]()):
        d[unsafe_offset=i] = s[unsafe_offset=i]
        s[unsafe_offset=i] = 0


struct OwnedArrowSchema(Movable):
    """The C `ArrowSchema` owned by `OwnedArrowSchema`."""

    var _ptr: Pointer[ArrowSchema, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowSchema]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _schema_live(self._ptr):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
        return self._ptr

    def release_to(mut self, dst: Pointer[ArrowSchema, MutUntrackedOrigin]):
        """Move the schema into `dst`, which must be empty."""
        _move_into(self._ptr, dst)

    def is_released(self) -> Bool:
        return not _schema_live(self._ptr)

    def n_children(self) -> Int:
        return Int(self._ptr[].n_children)


struct OwnedArrowArray(Movable, Sized):
    """The C `ArrowArray` owned by `OwnedArrowArray`."""

    var _ptr: Pointer[ArrowArray, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowArray]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _array_live(self._ptr):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowArray, MutUntrackedOrigin]:
        return self._ptr

    def release_to(mut self, dst: Pointer[ArrowArray, MutUntrackedOrigin]):
        """Move the array into `dst`, which must be empty."""
        _move_into(self._ptr, dst)

    def is_released(self) -> Bool:
        return not _array_live(self._ptr)

    def __len__(self) -> Int:
        """The number of rows."""
        return Int(self._ptr[].length)


struct OwnedArrowStream(Movable):
    """The C `ArrowArrayStream` owned by `OwnedArrowStream`."""

    var _ptr: Pointer[ArrowArrayStream, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowArrayStream]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _stream_live(self._ptr):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowArrayStream, MutUntrackedOrigin]:
        return self._ptr

    def release_to(mut self, dst: Pointer[ArrowArrayStream, MutUntrackedOrigin]):
        """Move the stream into `dst`, which must be empty."""
        _move_into(self._ptr, dst)

    def is_released(self) -> Bool:
        """True when the stream was moved to another struct or read to the end."""
        return not _stream_live(self._ptr)


struct ArrowResult(Movable):
    """A query result that produces Arrow arrays (`Connection.execute_arrow`)."""

    var _handle: duckdb_v2_arrow_result_handle

    def __init__(out self, *, var _owned: duckdb_v2_arrow_result_handle):
        self._handle = _owned

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_arrow_result_destroy(Pointer(to=self._handle))

    def schema(self) raises DuckDBError -> OwnedArrowSchema:
        """The result's schema: a struct with one child per column."""
        var s = OwnedArrowSchema()
        var err = null_error_info()
        check(lib().duckdb_v2_arrow_result_get_schema(self._handle, s.unsafe_ptr(), Pointer(to=err)), err)
        return s^

    def fetch_array(mut self) raises DuckDBError -> Optional[OwnedArrowArray]:
        """The next batch as a struct array, or `None` at the end."""
        var a = self._next_array()
        if a.is_released():
            return None
        return a^

    def fetch_batch(mut self) raises DuckDBError -> Optional[ArrowBatch]:
        """The next batch as an `ArrowBatch`, or `None` at the end. Its schema is
        fetched for this call only."""
        var a = self._next_array()
        if a.is_released():
            return None
        var s = self.schema()
        var b = ArrowBatch(a^, s^)
        return b^

    def _next_array(mut self) raises DuckDBError -> OwnedArrowArray:
        var a = OwnedArrowArray()
        var err = null_error_info()
        check(lib().duckdb_v2_arrow_result_fetch_array(self._handle, a.unsafe_ptr(), Pointer(to=err)), err)
        return a^

    def into_stream(var self) raises DuckDBError -> OwnedArrowStream:
        """The rest of the result as an `ArrowArrayStream`. `into_stream` uses up
        the result."""
        var s = OwnedArrowStream()
        var err = null_error_info()
        var handle = self._handle
        self._handle = null[duckdb_v2_arrow_result_handle]()
        check(lib().duckdb_v2_arrow_result_to_arrow_c_stream(Pointer(to=handle), s.unsafe_ptr(), Pointer(to=err)), err)
        return s^


# ===--------------------------------------------------------------------===#
# Reading an exported batch
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct _Lay(ImplicitlyCopyable):
    """The layout an Arrow format string describes.

    `kind`: the DuckDB type its values map to (`TypeId.INVALID`: no layout for the
    format).
    `width`: bytes per value in the data buffer (0: not fixed width, `b` is a
    bitmap). `offs`: bytes per offset (4/8, -1 for the 16-byte string view, 0:
    none). `size`: element count of `w:N`/`+w:N`. `conv`: temporal unit factor
    (multiply the stored integer by it, or divide by `-conv`).
    """

    var kind: TypeId
    var width: Int
    var offs: Int
    var size: Int
    var precision: Int
    var scale: Int
    var conv: Int

    @staticmethod
    def _fixed(kind: TypeId, width: Int, conv: Int = 0) -> Self:
        return Self(kind, width, 0, 0, 0, 0, conv)

    @staticmethod
    def _offset(kind: TypeId, offs: Int) -> Self:
        return Self(kind, 0, offs, 0, 0, 0, 0)


def _numbers(f: String) raises DuckDBError -> List[Int]:
    """The comma-separated integers after a format's ':' (`d:11,3` -> 11, 3)."""
    var out = List[Int]()
    var parts = f.split(":")
    if len(parts) < 2:
        raise api_error(String("the Arrow format '", f, "' is missing its parameters"))
    for part in parts[1].split(","):
        if not part:
            continue
        try:
            out.append(Int(part))
        except e:
            raise api_error(String("the Arrow format '", f, "' has a bad parameter '", part, "': ", e))
    return out^


def _layout(f: String) raises DuckDBError -> _Lay:
    """The layout of an Arrow format string: DuckDB 2.0.1's exports plus large and
    view variants, `time32`, the other timestamp units, `w:N`, 32/64-bit
    decimals. A format outside that list gets `TypeId.INVALID`."""
    match f:
        case "b":
            return _Lay._fixed(TypeId.BOOLEAN, 0)
        case "c":
            return _Lay._fixed(TypeId.TINYINT, 1)
        case "C":
            return _Lay._fixed(TypeId.UTINYINT, 1)
        case "s":
            return _Lay._fixed(TypeId.SMALLINT, 2)
        case "S":
            return _Lay._fixed(TypeId.USMALLINT, 2)
        case "i":
            return _Lay._fixed(TypeId.INTEGER, 4)
        case "I":
            return _Lay._fixed(TypeId.UINTEGER, 4)
        case "l":
            return _Lay._fixed(TypeId.BIGINT, 8)
        case "L":
            return _Lay._fixed(TypeId.UBIGINT, 8)
        case "f":
            return _Lay._fixed(TypeId.FLOAT, 4)
        case "g":
            return _Lay._fixed(TypeId.DOUBLE, 8)
        case "u":
            return _Lay._offset(TypeId.VARCHAR, 4)
        case "U":
            return _Lay._offset(TypeId.VARCHAR, 8)
        case "vu":
            return _Lay._offset(TypeId.VARCHAR, -1)
        case "z":
            return _Lay._offset(TypeId.BLOB, 4)
        case "Z":
            return _Lay._offset(TypeId.BLOB, 8)
        case "vz":
            return _Lay._offset(TypeId.BLOB, -1)
        case "+l":
            return _Lay._offset(TypeId.LIST, 4)
        case "+L":
            return _Lay._offset(TypeId.LIST, 8)
        case "tdD":
            return _Lay._fixed(TypeId.DATE, 4)
        case "tdm":
            return _Lay._fixed(TypeId.DATE, 8, -86_400_000)  # date64: milliseconds
        case "ttu":
            return _Lay._fixed(TypeId.TIME, 8)
        case "ttn":
            return _Lay._fixed(TypeId.TIME_NS, 8)
        case "tts":
            return _Lay._fixed(TypeId.TIME, 4, 1_000_000)
        case "ttm":
            return _Lay._fixed(TypeId.TIME, 4, 1_000)
        case "tin":
            return _Lay._fixed(TypeId.INTERVAL, 16)
        case "tiM" | "tiD":
            return _Lay._fixed(TypeId.INTERVAL, 4)
        case p if p.startswith("ts"):  # tss/tsm/tsu/tsn, then ':' and the zone name
            var tz = f.byte_length() > 4
            match p:
                case q if q.startswith("tsu"):
                    return _Lay._fixed(TypeId.TIMESTAMP_TZ if tz else TypeId.TIMESTAMP, 8)
                case q if q.startswith("tsn"):
                    return _Lay._fixed(TypeId.TIMESTAMP_TZ_NS if tz else TypeId.TIMESTAMP_NS, 8)
                case q if q.startswith("tsm"):
                    return _Lay._fixed(TypeId.TIMESTAMP_MS, 8)
                case q if q.startswith("tss"):
                    return _Lay._fixed(TypeId.TIMESTAMP_SEC, 8)
        case p if p.startswith("d:"):  # decimal: precision, scale, [bit width]
            var nums = _numbers(p)
            var bits = 128 if len(nums) < 3 else nums[2]
            if len(nums) < 2 or bits % 8 != 0 or bits < 8 or bits > 128:
                return _Lay(TypeId.INVALID, 0, 0, 0, 0, 0, 0)
            return _Lay(TypeId.DECIMAL, Int(bits // 8), 0, 0, nums[0], nums[1], 0)
        case p if p.startswith("w:"):
            return _Lay(TypeId.BLOB, _size(p), 0, 0, 0, 0, 0) if _size(p) > 0 else _Lay(
                TypeId.INVALID, 0, 0, 0, 0, 0, 0
            )
        case p if p.startswith("+w:"):
            return _Lay(TypeId.ARRAY, 0, 0, _size(p), 0, 0, 0) if _size(p) > 0 else _Lay(
                TypeId.INVALID, 0, 0, 0, 0, 0, 0
            )
    return _Lay(TypeId.INVALID, 0, 0, 0, 0, 0, 0)


def _size(f: String) -> Int:
    """The `N` of `w:N`/`+w:N` (0 if it is not a number)."""
    try:
        return Int(f.split(":")[1])
    except e:
        _ = e
        return 0


def _unsupported(what: String, f: String) -> DuckDBError:
    var why = "this format"
    match f:
        case "+s":
            why = "a STRUCT column, use arrow_scan"
        case "+m":
            why = "a MAP column, use arrow_scan"
        case p if p.startswith("+r"):
            why = "a run-end encoded column"
        case p if p.startswith("+v"):
            why = "a list-view column"
        case p if p.startswith("+c") or p.startswith("+u") or p.startswith("+x"):
            why = "a union column"
        case p if p.startswith("+d") or p.startswith("+t"):
            why = "a union column"
        case p if p.startswith("tD"):
            why = "a duration column"
        case "e":
            why = "a float16 column"
        case p if p.startswith("d:"):
            why = "a decimal wider than 128 bits"
    return DuckDBError(ErrorCode.TYPE_MISMATCH, String(what, " is '", f, "' and cannot be read: ", why))


@always_inline
def _same[T: AnyType, U: AnyType]() -> Bool:
    return reflect[T].name() == reflect[U].name()


@always_inline
def _is_list[T: AnyType]() -> Bool:
    return conforms_to(T, _ListValue)


trait _ListValue:
    """A `List` that `column[T]` can build."""

    @staticmethod
    def _read_value(nodes: Pointer[_Node, _], at: Int, idx: Int, what: String) raises DuckDBError -> Self:
        """The list at logical index `idx` of node `at`."""
        ...


__extension List(_ListValue):
    @staticmethod
    def _read_value(nodes: Pointer[_Node, _], at: Int, idx: Int, what: String) raises DuckDBError -> Self:
        # `List`'s own parameter is unbounded, so `_read_value` checks the element's
        # bounds.
        comptime if conforms_to(Self.T, Movable):
            comptime if conforms_to(Self.T, Deinitable):
                return _read_elements[Self.T](nodes, at, idx, what)
            else:
                raise api_error(String(what, " reads a list of a type that is not deinitable"))
        else:
            raise api_error(String(what, " reads a list of a type that is not movable"))


@always_inline
def _is_scalar_number[T: AnyType]() -> Bool:
    return (
        _same[T, Int8]()
        or _same[T, Int16]()
        or _same[T, Int32]()
        or _same[T, Int64]()
        or _same[T, Int]()
        or _same[T, Int128]()
        or _same[T, UInt8]()
        or _same[T, UInt16]()
        or _same[T, UInt32]()
        or _same[T, UInt64]()
        or _same[T, UInt]()
        or _same[T, UInt128]()
        or _same[T, Float32]()
        or _same[T, Float64]()
    )


@always_inline
def _scalar_form[T: AnyType]() -> Int:
    """How `column[T]` treats a non-`List` `T` (comptime): 0 a scalar it reads, 1 a
    multi-element SIMD value, 2 a type with no scalar mapping (a nested column
    needs `arrow_scan`)."""
    comptime if not is_mapped[T]():
        return 2
    elif reflect[T].base_name() == "SIMD" and not _is_scalar_number[T]():
        return 1
    else:
        return 0


@always_inline
def _cname(p: Pointer[c_char, ImmUntrackedOrigin]) -> String:
    """A schema node's C string field (NULL gives an empty string)."""
    if Int(p) == 0:
        return ""
    return String(unsafe_from_utf8_ptr=p)


@always_inline
def _buffer(a: Pointer[ArrowArray, MutUntrackedOrigin], i: Int) -> Pointer[NoneType, ImmUntrackedOrigin]:
    """Buffer `i` of an array, or null when the array has no buffer `i` (`n_buffers`
    is not reliable)."""
    if i < 0 or i >= Int(a[].n_buffers):
        return null[Pointer[NoneType, ImmUntrackedOrigin]]()
    return a[].buffers[unsafe_offset=i]


@always_inline
def _bit(p: Pointer[NoneType, ImmUntrackedOrigin], idx: Int) -> Bool:
    """Bit `idx` of an Arrow bitmap (validity, and a `b` column's values)."""
    return (p.unsafe_bitcast[UInt8]()[unsafe_offset=idx >> 3] >> UInt8(idx & 7)) & 1 == 1


@always_inline
def _load[T: ImplicitlyCopyable](data: Pointer[NoneType, ImmUntrackedOrigin], idx: Int) -> T:
    """`T`'s own storage at index `idx`, for a layout the format guarantees."""
    return data.unsafe_bitcast[T]()[unsafe_offset=idx]


@always_inline
def _rescale(v: Int128, conv: Int) -> Int128:
    """A temporal value from the format's unit to the Mojo type's (`_Lay.conv`)."""
    if conv > 0:
        return v * Int128(conv)
    if conv < 0:
        return v // Int128(-conv)
    return v


@always_inline
def _load_as[T: Movable](
    data: Pointer[NoneType, ImmUntrackedOrigin], phys: Int, what: String, format: String
) raises DuckDBError -> T:
    """`T`'s own storage at `phys`, for a fixed-width scalar the formats describe. A
    wider SIMD value or a nested type is an error."""
    comptime if conforms_to(T, TrivialRegisterPassable):
        return rebind_var[T](_load[T](data, phys))
    else:
        raise _unsupported(what, format)


struct _Node(Movable):
    """A column of an exported batch. The batch owns its values array, so a
    `_Node`'s pointers stay valid while the batch does. `child` and `dict` index
    other `_Nodes.items` nodes: a list's elements, and a dictionary column's values.
    """

    var name: String
    var format: String
    var lay: _Lay
    var index_kind: TypeId  # a dictionary column's index type, kept when `lay.kind` becomes the value type
    var array: Pointer[ArrowArray, MutUntrackedOrigin]
    var child: Int
    var dict: Int

    def __init__(out self):
        self.name = ""
        self.format = ""
        self.lay = _Lay(TypeId.INVALID, 0, 0, 0, 0, 0, 0)
        self.index_kind = TypeId.INVALID
        self.array = null[Pointer[ArrowArray, MutUntrackedOrigin]]()
        self.child = -1
        self.dict = -1

    def _valid(self, phys: Int) -> Bool:
        """True when physical index `phys` is not NULL. A column with no validity
        buffer is not NULL at any index."""
        var v = _buffer(self.array, 0)
        if Int(v) == 0:
            return True
        return _bit(v, phys)

    def _phys(self, idx: Int) -> Int:
        """Logical `idx` as a physical index (a sliced array starts at `offset`)."""
        return idx + Int(self.array[].offset)

    def _index(self, phys: Int) raises DuckDBError -> Int:
        """The dictionary index at `phys`, at the index type's width and sign."""
        var d = self._data(1)
        match self.index_kind:
            case TypeId.TINYINT:
                return Int(_load[Int8](d, phys))
            case TypeId.SMALLINT:
                return Int(_load[Int16](d, phys))
            case TypeId.INTEGER:
                return Int(_load[Int32](d, phys))
            case TypeId.BIGINT:
                return Int(_load[Int64](d, phys))
            case TypeId.USMALLINT:
                return Int(_load[UInt16](d, phys))
            case TypeId.UINTEGER:
                return Int(_load[UInt32](d, phys))
            case TypeId.UBIGINT:
                return Int(_load[UInt64](d, phys))
            case _:
                return Int(_load[UInt8](d, phys))

    def _native(self) -> Bool:
        """`_native()` is true when the data buffer already stores values in the Mojo
        layout of the column's type. `span()` borrows such a buffer. Dictionary
        indices, decimals and intervals need a conversion first."""
        return (
            self.lay.width > 0
            and self.lay.conv == 0
            and Int(self.array[].dictionary) == 0
            and self.lay.kind != TypeId.DECIMAL
            and self.lay.kind != TypeId.INTERVAL
        )

    def _offset(self, i: Int) -> Int:
        """Offsets buffer element `i` (list, string and blob columns, -1: missing)."""
        var b = _buffer(self.array, 1)
        if Int(b) == 0:
            return -1
        return Int(_load[Int32](b, i)) if self.lay.offs == 4 else Int(_load[Int64](b, i))

    def _data(self, i: Int) raises DuckDBError -> Pointer[NoneType, ImmUntrackedOrigin]:
        """Buffer `i`. The array must carry one."""
        var b = _buffer(self.array, i)
        if Int(b) == 0:
            raise api_error(String("'", self.format, "' has no data buffer"))
        return b


struct _Nodes(Movable):
    """Every node of a batch (a column, a list child, a dictionary's values) in a
    flat list linked by index."""

    var items: List[_Node]
    var columns: List[Int]  # the node index of each top-level column

    def __init__(out self):
        self.items = List[_Node]()
        self.columns = List[Int]()

    def _plan(
        mut self,
        schema: Pointer[ArrowSchema, MutUntrackedOrigin],
        array: Pointer[ArrowArray, MutUntrackedOrigin],
        what: String,
    ) raises DuckDBError -> Int:
        """Plans a schema node with its dictionary and child and returns its index.
        `_plan` uses `what` as the column name in the errors it raises."""
        var node = _Node()
        node.name = _cname(schema[].name)
        node.format = _cname(schema[].format)
        node.array = array
        node.lay = _layout(node.format)
        node.index_kind = node.lay.kind
        if Int(array[].length) < 0 or Int(array[].offset) < 0:
            raise api_error(String(what, " has a negative length or offset (Arrow format '", node.format, "')"))
        if Int(schema[].dictionary) != 0:
            if Int(array[].dictionary) == 0:
                raise api_error(String(what, " has a dictionary in its schema but not in its array"))
            node.dict = self._plan(schema[].dictionary, array[].dictionary, what)
            # The column reads as its dictionary's type: a DuckDB ENUM as VARCHAR.
            node.lay.kind = self.items[node.dict].lay.kind
        if node.lay.kind == TypeId.LIST or node.lay.kind == TypeId.ARRAY:
            if Int(schema[].n_children) != 1 or Int(array[].n_children) != 1:
                raise api_error(String(what, " needs one child (Arrow format '", node.format, "')"))
            node.child = self._plan(schema[].children[unsafe_offset=0], array[].children[unsafe_offset=0], what)
        var at = len(self.items)
        self.items.append(node^)
        return at


@always_inline
def _check[T: Movable & Deinitable](nodes: Pointer[_Node, _], at: Int, what: String) raises DuckDBError:
    """`_check[T]` raises when `T` does not match node `at`'s format."""
    ref n = nodes.unsafe_offset(at)[]
    if n.lay.kind == TypeId.INVALID:
        raise _unsupported(what, n.format)
    comptime if _is_list[T]():
        if n.lay.kind != TypeId.LIST and n.lay.kind != TypeId.ARRAY:
            raise DuckDBError(
                ErrorCode.TYPE_MISMATCH, String(what, " is '", n.format, "' and cannot be read as ", reflect[T].name())
            )
        if n.child < 0:
            raise api_error(String(what, " has no list child (Arrow format '", n.format, "')"))
    else:
        comptime match _scalar_form[T]():
            case 2:
                raise DuckDBError(
                    ErrorCode.TYPE_MISMATCH,
                    String(
                        what,
                        " is '",
                        n.format,
                        "' and cannot be read as ",
                        reflect[T].name(),
                        ": column[T] reads scalars and lists, nested columns need arrow_scan",
                    ),
                )
            case 1:
                raise DuckDBError(
                    ErrorCode.TYPE_MISMATCH,
                    String(
                        what,
                        " is '",
                        n.format,
                        "' and cannot be read as ",
                        reflect[T].name(),
                        ": read the column as its element type",
                    ),
                )
            case _:
                var ok = mapped_accepts[T](n.lay.kind)
                # A DECIMAL with scale 0 is the integer DuckDB stored in it.
                var raw_int = _same[T, Int128]() and n.lay.kind == TypeId.DECIMAL and n.lay.scale == 0
                if not ok and not raw_int:
                    raise DuckDBError(
                        ErrorCode.TYPE_MISMATCH,
                        String(what, " is '", n.format, "' and cannot be read as ", reflect[T].name()),
                    )


def _read[T: Movable & Deinitable](nodes: Pointer[_Node, _], at: Int, idx: Int, what: String) raises DuckDBError -> T:
    """The value at logical index `idx` of node `at`. A NULL at `idx` raises."""
    ref n = nodes.unsafe_offset(at)[]
    var phys = n._phys(idx)
    if not n._valid(phys):
        raise api_error(
            String(
                what,
                " has a NULL value at row ",
                idx,
                " (read it with get(), or with arrow_scan for a list with NULL elements)",
            )
        )
    if n.dict >= 0:
        var dict = n.dict
        var entry = n._index(phys)
        if entry < 0 or entry >= Int(nodes.unsafe_offset(dict)[].array[].length):
            raise api_error(String(what, " has dictionary index ", entry, " out of range (Arrow format '", n.format, "')"))
        return _read[T](nodes, dict, entry, what)
    comptime if _is_list[T]():
        return rebind_var[T](downcast[T, _ListValue]._read_value(nodes, at, idx, what))
    else:
        return _read_scalar[T](nodes, at, phys, what)


def _read_elements[U: Movable & Deinitable](
    nodes: Pointer[_Node, _], at: Int, idx: Int, what: String
) raises DuckDBError -> List[U]:
    """The list at logical `idx` of node `at`. Its offsets address the child's
    logical indices, and the child applies its own `offset`."""
    ref n = nodes.unsafe_offset(at)[]
    if n.child < 0:
        raise api_error(String(what, " has no list child (Arrow format '", n.format, "')"))
    _check[U](nodes, n.child, what)
    var phys = n._phys(idx)
    var child = n.child
    var start = idx * n.lay.size
    var count = n.lay.size
    if n.lay.kind == TypeId.LIST:
        start = n._offset(phys)
        count = n._offset(phys + 1) - start
    if start < 0 or count < 0 or start + count > Int(nodes.unsafe_offset(child)[].array[].length):
        raise api_error(String(what, " has offsets out of range (Arrow format '", n.format, "')"))
    var out = List[U](capacity=count)
    for k in range(start, start + count):
        out.append(_read[U](nodes, child, k, what))
    return out^


@always_inline
def _read_scalar[T: Movable & Deinitable](
    nodes: Pointer[_Node, _], at: Int, phys: Int, what: String
) raises DuckDBError -> T:
    """The scalar at physical `phys` of node `at`, for a `T` `_check` accepted."""
    ref n = nodes.unsafe_offset(at)[]
    var data = n._data(1)
    comptime match T:
        case == Date:
            if n.lay.width == 4:  # `tdD`, while `tdm` (date64) stores milliseconds
                return rebind_var[T](Date(Int32(_rescale(Int128(_load[Int32](data, phys)), n.lay.conv))))
            return rebind_var[T](Date(Int32(_rescale(Int128(_load[Int64](data, phys)), n.lay.conv))))
        case == Time:
            if n.lay.width == 4:  # `tts`/`ttm`: time32
                return rebind_var[T](Time(Int64(_rescale(Int128(_load[Int32](data, phys)), n.lay.conv))))
            return rebind_var[T](Time(Int64(_rescale(Int128(_load[Int64](data, phys)), n.lay.conv))))
        case == Bool:
            return rebind_var[T](Bool(_bit(data, phys)))
        case == String:
            return rebind_var[T](String(from_utf8_lossy=_element_bytes(nodes, at, phys, what)))
        case == Blob:
            return rebind_var[T](Blob(_bytes_of(nodes, at, phys, what)))
        case == Decimal:
            return rebind_var[T](Decimal(UInt8(n.lay.precision), UInt8(n.lay.scale), _decimal(nodes, at, phys, what)))
        case == Int128:
            return rebind_var[T](_decimal(nodes, at, phys, what))
        case == Interval:
            return rebind_var[T](_interval(n, phys))
        case _:
            return _load_as[T](data, phys, what, n.format)


@always_inline
def _element_bytes(
    nodes: Pointer[_Node, _], at: Int, phys: Int, what: String
) raises DuckDBError -> Span[UInt8, ImmUntrackedOrigin]:
    """The bytes of the string or blob element at `phys`."""
    ref n = nodes.unsafe_offset(at)[]
    if n.lay.offs == 0:  # `w:N`: N bytes per value
        if n.lay.kind != TypeId.BLOB or n.lay.width <= 0:
            raise api_error(String(what, " has no offsets and is not fixed-size binary (Arrow format '", n.format, "')"))
        return Span[UInt8, ImmUntrackedOrigin](
            unsafe_ptr=n._data(1).unsafe_bitcast[UInt8]().unsafe_offset(phys * n.lay.width), length=n.lay.width
        )
    if n.lay.offs < 0:  # `vu`/`vz`: a 16-byte view (length, prefix, buffer index, offset)
        var view = n._data(1).unsafe_bitcast[UInt32]().unsafe_offset(phys * 4)
        var length = Int(Int32(view[]))
        if length < 0:
            raise api_error(String(what, " has a '", n.format, "' view of length ", length))
        if length <= 12:
            return Span[UInt8, ImmUntrackedOrigin](unsafe_ptr=view.unsafe_offset(1).unsafe_bitcast[UInt8](), length=length)
        var buffer = 2 + Int(Int32(view.unsafe_offset(2).unsafe_bitcast[Int32]()[]))
        var at_offset = Int(Int32(view.unsafe_offset(3).unsafe_bitcast[Int32]()[]))
        if at_offset < 0 or buffer < 2 or buffer >= Int(n.array[].n_buffers):
            raise api_error(String(what, " has a '", n.format, "' view into buffer ", buffer, ", offset ", at_offset))
        # The view layout's sizes buffer gives the element's bound.
        if 3 < Int(n.array[].n_buffers):
            var sizes = _buffer(n.array, 3)
            var cap = Int(_load[Int64](sizes, buffer - 2)) if Int(sizes) != 0 else -1
            if cap >= 0 and at_offset + length > cap:
                raise api_error(
                    String(what, " has a '", n.format, "' view of ", length, " bytes at ", at_offset, " in a ", cap, "-byte buffer")
                )
        return Span[UInt8, ImmUntrackedOrigin](
            unsafe_ptr=n._data(buffer).unsafe_bitcast[UInt8]().unsafe_offset(at_offset), length=length
        )
    var start = n._offset(phys)
    var end = n._offset(phys + 1)
    if start < 0 or end < start:
        raise api_error(String(what, " has offsets out of range (Arrow format '", n.format, "')"))
    return Span[UInt8, ImmUntrackedOrigin](
        unsafe_ptr=n._data(2).unsafe_bitcast[UInt8]().unsafe_offset(start), length=end - start
    )


@always_inline
def _bytes_of(nodes: Pointer[_Node, _], at: Int, phys: Int, what: String) raises DuckDBError -> List[UInt8]:
    """The bytes of a blob element, owned."""
    var view = _element_bytes(nodes, at, phys, what)
    var out = List[UInt8](capacity=len(view))
    for b in view:
        out.append(b)
    return out^


@always_inline
def _decimal(nodes: Pointer[_Node, _], at: Int, phys: Int, what: String) raises DuckDBError -> Int128:
    """The unscaled decimal at `phys`: two's complement little endian of the bit
    width the format declares (DuckDB 128 bits, another producer 32 or 64)."""
    ref n = nodes.unsafe_offset(at)[]
    var bytes = n.lay.width
    if bytes != 4 and bytes != 8 and bytes != 16:
        raise api_error(String(what, " has a ", bytes * 8, "-bit decimal (Arrow format '", n.format, "')"))
    var p = n._data(1).unsafe_bitcast[UInt8]().unsafe_offset(phys * bytes)
    var v = Int128(0)
    for k in range(bytes):
        v = v | (Int128(p.unsafe_offset(k)[]) << Int128(8 * k))
    if bytes < 16 and p.unsafe_offset(bytes - 1)[] & 0x80 != 0:  # sign extend
        v = v | (-Int128(1) << Int128(8 * bytes))
    return v


@always_inline
def _interval(ref n: _Node, phys: Int) raises DuckDBError -> Interval:
    """The interval at `phys`: Arrow stores {months, days, nanos} (`tin`) or only
    months or days (`tiM`, `tiD`), `Interval` holds micros."""
    var p = n._data(1).unsafe_bitcast[UInt8]().unsafe_offset(phys * n.lay.width)
    var months = Int32(0)
    var days = Int32(0)
    var micros = Int64(0)
    if n.lay.width == 16:
        months = p.unsafe_bitcast[Int32]()[unsafe_offset=0]
        days = p.unsafe_bitcast[Int32]()[unsafe_offset=1]
        var nanos = p.unsafe_bitcast[Int64]()[unsafe_offset=1]
        micros = Int64(nanos / 1000)  # toward zero, as DuckDB's importer does
        if nanos % 1000 != 0 and nanos < 0:
            micros += 1
    elif n.format == "tiM":
        months = p.unsafe_bitcast[Int32]()[]
    else:
        days = p.unsafe_bitcast[Int32]()[]
    return Interval(months, days, micros)


struct ArrowBatch(Movable, Sized):
    """A `fetch_batch()` result: an Arrow struct array and the columns planned from
    its schema.

    The batch owns its array, so its column readers stay valid while the batch
    does. `ArrowBatch` reads its schema while planning and releases it when the plan
    is built. STRUCT and MAP columns need `arrow_scan`.
    """

    var _array: OwnedArrowArray
    var _nodes: _Nodes
    var _rows: Int

    def __init__(out self, var array: OwnedArrowArray, var schema: OwnedArrowSchema) raises DuckDBError:
        """Takes ownership of `array` (one child per column) and of `schema`."""
        var sp = schema.unsafe_ptr()
        var ap = array.unsafe_ptr()
        if not _schema_live(sp) or not _array_live(ap):
            raise api_error("an Arrow batch needs a schema and an array that have not been released")
        if _cname(sp[].format) != "+s":
            raise api_error(String("an Arrow batch needs a struct array, not '", _cname(sp[].format), "'"))
        if Int(sp[].n_children) != Int(ap[].n_children):
            raise api_error(
                String("the Arrow array has ", ap[].n_children, " children, its schema ", sp[].n_children)
            )
        var rows = Int(ap[].length)
        if rows < 0:
            raise api_error(String("an Arrow batch cannot have ", rows, " rows"))
        if Int(ap[].offset) != 0:
            raise api_error(String("an Arrow batch cannot be sliced (offset ", ap[].offset, ")"))
        self._nodes = _Nodes()
        for i in range(Int(ap[].n_children)):
            var at = self._nodes._plan(
                sp[].children[unsafe_offset=i], ap[].children[unsafe_offset=i], String("column ", i)
            )
            self._nodes.columns.append(at)
            ref col = self._nodes.items.unsafe_ptr().unsafe_offset(at)[]
            if Int(col.array[].length) + Int(col.array[].offset) < rows:
                raise api_error(
                    String("column '", col.name, "' has ", col.array[].length, " rows, the batch has ", rows)
                )
        self._rows = rows
        self._array = array^
        _ = schema^  # the nodes copied what they need from the schema, so it can go now

    def __len__(self) -> Int:
        """The number of rows."""
        return self._rows

    def column_count(self) -> Int:
        """The number of columns."""
        return len(self._nodes.columns)

    def column_name(self, col: Int) raises DuckDBError -> String:
        """The column's Arrow field name."""
        self._check_col(col)
        return self._nodes.items[self._nodes.columns[col]].name.copy()

    def column_format(self, col: Int) raises DuckDBError -> String:
        """The column's Arrow format string, e.g. `d:11,3,128`."""
        self._check_col(col)
        return self._nodes.items[self._nodes.columns[col]].format.copy()

    def _check_col(self, col: Int) raises DuckDBError:
        if col < 0 or col >= self.column_count():
            raise api_error(String("column index ", col, " out of range (", self.column_count(), " columns)"))

    def column[T: Movable & Deinitable](ref self, col: Int) raises DuckDBError -> ArrowColumnReader[T, origin_of(self)]:
        """An `ArrowColumnReader[T]` over column `col`.

        `T` is a scalar type of `duckdb.mapping`, or a `List` for a `+l`, `+L` or
        `+w:N` column. A dictionary column such as a DuckDB ENUM reads as its value
        type. `T` must match the column's format, or `column[T]` raises
        `ErrorCode.TYPE_MISMATCH`.
        """
        self._check_col(col)
        var at = self._nodes.columns[col]
        var nodes = self._nodes.items.unsafe_ptr()
        var what = String("column ", col, " (", nodes.unsafe_offset(at)[].name, ")")
        _check[T](nodes, at, what)
        return ArrowColumnReader[T, origin_of(self)](
            nodes.unsafe_origin_cast[origin_of(self)](), at, what, self._rows
        )

    def get[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> T:
        """The value at `row` of column `col`. A NULL row raises."""
        return self.column[T](col)[row]

    def get_optional[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> Optional[T]:
        """The value at `row` of column `col`, or `None` at a NULL."""
        return self.column[T](col).get(row)

    def get[T: Movable & Deinitable](self, *, col: Int) raises DuckDBError -> List[T]:
        """Every value in the column. The first NULL raises."""
        return self.column[T](col).to_list_of_values()

    def get_optional[T: Movable & Deinitable](self, *, col: Int) raises DuckDBError -> List[Optional[T]]:
        """Every value of one column, NULLs as `None`."""
        return self.column[T](col).to_list()


struct ArrowColumnReader[T: Movable & Deinitable, origin: ImmOrigin](Sized):
    """An `ArrowBatch` column read as `T`.

    The reader borrows the batch. It checks the validity buffer before it reads a
    row, and a row's physical index is `row + array[].offset`.
    """

    var _nodes: Pointer[_Node, Self.origin]
    var _at: Int
    var _what: String
    var _size: Int

    def __init__(out self, var nodes: Pointer[_Node, Self.origin], at: Int, var what: String, size: Int):
        self._nodes = nodes
        self._at = at
        self._what = what^
        self._size = size

    def __len__(self) -> Int:
        return self._size

    def is_valid(self, row: Int) -> Bool:
        """Row `row` holds a value."""
        ref n = self._nodes.unsafe_offset(self._at)[]
        return n._valid(n._phys(row))

    def has_nulls(self) -> Bool:
        """True when the column has a NULL row. The check walks the validity buffer,
        because Arrow's `null_count` is optional."""
        ref n = self._nodes.unsafe_offset(self._at)[]
        if Int(_buffer(n.array, 0)) == 0:
            return False
        for i in range(self._size):
            if not n._valid(n._phys(i)):
                return True
        return False

    def span(self) -> Optional[Span[Self.T, Self.origin]]:
        """The borrowed values when the column is fixed-width, NULL-free and stores
        `T`'s own layout. Otherwise `None`."""
        comptime assert conforms_to(Self.T, TrivialRegisterPassable), "span() needs a fixed-width type"
        ref n = self._nodes.unsafe_offset(self._at)[]
        if not n._native() or size_of[Self.T]() != n.lay.width or self.has_nulls():
            return None
        var raw = _buffer(n.array, 1)
        if Int(raw) == 0:
            return None
        var data = raw.unsafe_bitcast[Self.T]().unsafe_offset(n._phys(0))
        return Span[Self.T, Self.origin](unsafe_ptr=data.unsafe_origin_cast[Self.origin](), length=self._size)

    def __getitem__(self, var row: Int) raises DuckDBError -> Self.T:
        """The value at `row`. A NULL row and an out-of-range row both raise. `get`
        answers a NULL row with `None`."""
        _check_row(row, self._size)
        if not self.is_valid(row):
            raise api_error(String("row ", row, " is NULL (use get() to read NULLs as None)"))
        return _read[Self.T](self._nodes, self._at, row, self._what)

    def get(self, var row: Int) raises DuckDBError -> Optional[Self.T]:
        """The value at `row`, or `None` for NULL."""
        _check_row(row, self._size)
        if not self.is_valid(row):
            return None
        return Optional[Self.T](_read[Self.T](self._nodes, self._at, row, self._what))

    def to_list(self) raises DuckDBError -> List[Optional[Self.T]]:
        """Every value, NULLs as `None`."""
        var out = List[Optional[Self.T]](capacity=self._size)
        for i in range(self._size):
            out.append(self.get(i))
        return out^

    def to_list_of_values(self) raises DuckDBError -> List[Self.T]:
        """Every value. The first NULL raises."""
        var out = List[Self.T](capacity=self._size)
        for i in range(self._size):
            out.append(self[i])
        return out^


def _check_row(row: Int, size: Int) raises DuckDBError:
    if row < 0 or row >= size:
        raise api_error(String("row ", row, " out of range (", size, " rows)"))

# ===--------------------------------------------------------------------===#
# Scanning an Arrow stream
# ===--------------------------------------------------------------------===#


struct _ArrowScanState(Movable):
    var chunk: duckdb_v2_data_chunk_handle  # the chunk the last scan referenced
    var done: Bool

    def __init__(out self):
        self.chunk = null[duckdb_v2_data_chunk_handle]()
        self.done = False

    def __init__(out self, *, deinit move: Self):
        self.chunk = move.chunk
        self.done = move.done

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_data_chunk_destroy(Pointer(to=self.chunk))


struct ArrowScan(TableFunctionImpl):
    """`__mojo_arrow_scan(address)`: rows of the `ArrowArrayStream` at `address`."""

    comptime State = _ArrowScanState
    var _stream: Pointer[ArrowArrayStream, MutUntrackedOrigin]
    var _importer: duckdb_v2_arrow_importer_handle
    var _columns: Int

    def __init__(out self, stream: Pointer[ArrowArrayStream, MutUntrackedOrigin], importer: duckdb_v2_arrow_importer_handle, columns: Int):
        self._stream = stream
        self._importer = importer
        self._columns = columns

    def __init__(out self, *, deinit move: Self):
        self._stream = move._stream
        self._importer = move._importer
        self._columns = move._columns

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_arrow_importer_destroy(Pointer(to=self._importer))

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        var address = args.constant[Int64](0)
        if not address:
            raise Error("__mojo_arrow_scan needs the address of an ArrowArrayStream")
        var stream = Pointer[ArrowArrayStream, MutUntrackedOrigin](unsafe_from_address=Int(address.value()))
        if not _stream_live(stream):
            raise Error("the Arrow stream was already read or released")
        var schema = OwnedArrowSchema()
        if stream[].get_schema(stream, schema.unsafe_ptr()) != 0:
            raise Error(String("the Arrow stream has no schema: ", _stream_error(stream)))
        var importer = null[duckdb_v2_arrow_importer_handle]()
        var err = null_error_info()
        var code = lib().duckdb_v2_arrow_importer_create(args._context, schema.unsafe_ptr(), 2048, Pointer(to=importer), Pointer(to=err))
        _ = schema^  # keep the schema alive until the importer has read it
        check(code, err)
        var duck_schema = null[duckdb_v2_schema_handle]()
        check(lib().duckdb_v2_arrow_importer_get_schema(importer, Pointer(to=duck_schema), Pointer(to=err)), err)
        var s = _read_schema(duck_schema)
        _ = lib().duckdb_v2_schema_destroy(Pointer(to=duck_schema))
        for i in range(len(s.names)):
            columns.add(s.names[i], s.types[i])
        return Self(stream, importer, len(s.names))

    def init(ref self, shared: ScanShared) raises -> _ArrowScanState:
        return _ArrowScanState()

    def scan(ref self, mut state: _ArrowScanState, mut out: TableOutput) raises -> Int:
        var err = null_error_info()
        _ = lib().duckdb_v2_data_chunk_destroy(Pointer(to=state.chunk))
        while True:
            check(lib().duckdb_v2_arrow_importer_next_chunk(self._importer, Pointer(to=state.chunk), Pointer(to=err)), err)
            if Int(state.chunk) != 0:
                break
            if state.done:
                return 0
            var array = OwnedArrowArray()
            if self._stream[].get_next(self._stream, array.unsafe_ptr()) != 0:
                raise Error(String("reading the Arrow stream failed: ", _stream_error(self._stream)))
            if array.is_released():
                state.done = True
                continue
            # The importer takes the array over and keeps its buffers alive.
            var code = lib().duckdb_v2_arrow_importer_append(self._importer, array.unsafe_ptr(), True, True, Pointer(to=err))
            _ = array^  # keep the struct alive until the importer has taken it over
            check(code, err)
        var size: idx_t = 0
        check(lib().duckdb_v2_data_chunk_get_size(state.chunk, Pointer(to=size), Pointer(to=err)), err)
        for c in range(self._columns):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_data_chunk_get_vector(state.chunk, idx_t(c), Pointer(to=vec), Pointer(to=err)), err)
            out.reference(c, vec)
        return Int(size)


def _stream_error(stream: Pointer[ArrowArrayStream, MutUntrackedOrigin]) -> String:
    var msg = stream[].get_last_error(stream)
    if Int(msg) == 0:
        return "unknown error"
    return String(unsafe_from_utf8_ptr=msg)
