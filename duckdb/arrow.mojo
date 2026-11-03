"""Arrow interop through the Arrow C data interface.

Export: `con.execute_arrow(sql)` runs a query whose result comes as Arrow
arrays, or as a standard `ArrowArrayStream` for any Arrow consumer.

Import: `con.arrow_scan(stream)` is a relation over an `ArrowArrayStream`
someone else produced. DuckDB reads the Arrow buffers without copying them.
A stream can be read once (reading it again gives no rows), and must stay
alive until the query over it has been read.

```mojo
var exported = other.execute_arrow("SELECT * FROM range(10) t(i)").into_stream()
var back = con.arrow_scan(exported).fetchall()
```

An Arrow result streams like any result: until it has been read, its
connection runs no other statement. Reading a stream exported by a
connection needs another connection (`other` above).

The structs follow the Arrow specification, so they can be handed to other
Arrow libraries: `unsafe_ptr()` gives the C struct, `release_to(dst)` moves
its ownership into a struct the consumer allocated.
"""

from std.memory.alloc import unsafe_alloc
from std.sys import size_of
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb._runtime import api_error, check, lib
from duckdb.chunk import _read_schema
from duckdb.error import DuckDBError, ErrorCode
from duckdb.functions import BindArgs
from duckdb.table import ScanShared, TableColumns, TableFunctionImpl, TableOutput


def _zeroed[T: AnyType]() -> Pointer[T, MutUntrackedOrigin]:
    """A heap allocation of `T` with every byte 0 (a released, empty Arrow struct)."""
    var p = unsafe_alloc[T](1)
    var bytes = p.unsafe_bitcast[UInt8]()
    for i in range(size_of[T]()):
        bytes[unsafe_offset=i] = 0
    return p


@always_inline
def _live[T: AnyType](p: Pointer[T, MutUntrackedOrigin], release_offset: Int) -> Bool:
    """Whether the Arrow struct at `p` is live (its `release` is not NULL)."""
    return p.unsafe_bitcast[UInt8]().unsafe_offset(release_offset).unsafe_bitcast[Int]()[] != 0


comptime _SCHEMA_RELEASE = 7 * 8
comptime _ARRAY_RELEASE = 8 * 8
comptime _STREAM_RELEASE = 3 * 8


struct OwnedArrowSchema(Movable):
    """An `ArrowSchema` this program owns (released on destruction)."""

    var _ptr: Pointer[ArrowSchema, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowSchema]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _live(self._ptr, _SCHEMA_RELEASE):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
        return self._ptr

    def n_children(self) -> Int:
        return Int(self._ptr[].n_children)


struct OwnedArrowArray(Movable, Sized):
    """An `ArrowArray` this program owns (released on destruction)."""

    var _ptr: Pointer[ArrowArray, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowArray]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _live(self._ptr, _ARRAY_RELEASE):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowArray, MutUntrackedOrigin]:
        return self._ptr

    def is_released(self) -> Bool:
        return not _live(self._ptr, _ARRAY_RELEASE)

    def __len__(self) -> Int:
        """The number of rows."""
        return Int(self._ptr[].length)


struct OwnedArrowStream(Movable):
    """An `ArrowArrayStream` this program owns (released on destruction)."""

    var _ptr: Pointer[ArrowArrayStream, MutUntrackedOrigin]

    def __init__(out self):
        self._ptr = _zeroed[ArrowArrayStream]()

    def __init__(out self, *, deinit move: Self):
        self._ptr = move._ptr

    def __deinit__(deinit self):
        if _live(self._ptr, _STREAM_RELEASE):
            self._ptr[].release(self._ptr)
        self._ptr.unsafe_free()

    def unsafe_ptr(self) -> Pointer[ArrowArrayStream, MutUntrackedOrigin]:
        return self._ptr

    def release_to(mut self, dst: Pointer[ArrowArrayStream, MutUntrackedOrigin]):
        """Move the stream into `dst` (a struct the consumer owns); this one is then empty."""
        dst.unsafe_bitcast[UInt8]()
        for i in range(size_of[ArrowArrayStream]()):
            dst.unsafe_bitcast[UInt8]()[unsafe_offset=i] = self._ptr.unsafe_bitcast[UInt8]()[unsafe_offset=i]
            self._ptr.unsafe_bitcast[UInt8]()[unsafe_offset=i] = 0


struct ArrowResult(Movable):
    """A query result that produces Arrow arrays (from `Connection.execute_arrow`)."""

    var _handle: duckdb_v2_arrow_result_handle

    def __init__(out self, *, var _owned: duckdb_v2_arrow_result_handle):
        self._handle = _owned

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_arrow_result_destroy(Pointer(to=self._handle))

    def schema(self) raises DuckDBError -> OwnedArrowSchema:
        """The Arrow schema of the arrays (a struct with one child per column)."""
        var s = OwnedArrowSchema()
        var err = null_error_info()
        check(lib().duckdb_v2_arrow_result_get_schema(self._handle, s.unsafe_ptr(), Pointer(to=err)), err)
        return s^

    def fetch_array(mut self) raises DuckDBError -> Optional[OwnedArrowArray]:
        """The next batch as a struct array (one child per column), or `None` at the end."""
        var a = OwnedArrowArray()
        var err = null_error_info()
        check(lib().duckdb_v2_arrow_result_fetch_array(self._handle, a.unsafe_ptr(), Pointer(to=err)), err)
        if a.is_released():
            return None
        return a^

    def into_stream(var self) raises DuckDBError -> OwnedArrowStream:
        """The rest of the result as an `ArrowArrayStream` (consumes the result)."""
        var s = OwnedArrowStream()
        var err = null_error_info()
        var handle = self._handle
        self._handle = null[duckdb_v2_arrow_result_handle]()
        check(lib().duckdb_v2_arrow_result_to_arrow_c_stream(Pointer(to=handle), s.unsafe_ptr(), Pointer(to=err)), err)
        return s^


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
        if not _live(stream, _STREAM_RELEASE):
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
