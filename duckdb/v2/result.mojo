"""Query results: streaming `Result` and `MaterializedResult`."""

from std.memory import ArcPointer
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.v2._runtime import api_error, check, lib
from duckdb.v2.chunk import Chunk, _Schema, _read_schema
from duckdb.v2.serde import _ReadTree, read_row
from duckdb.v2.error import DuckDBError
from duckdb.v2.types import LogicalType


@fieldwise_init
struct Column(Copyable, Movable, Writable):
    """A result column: its name and full type (parameters included)."""

    var name: String
    var type: LogicalType

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.name, " ", self.type)


@fieldwise_init
struct StatementType(Equatable, ImplicitlyCopyable, Writable):
    """The kind of statement a result came from (V2 `STATEMENT_TYPE`)."""

    var value: DUCKDB_V2_STATEMENT_TYPE

    comptime INVALID = Self(DUCKDB_V2_STATEMENT_TYPE_INVALID)
    comptime SELECT = Self(DUCKDB_V2_STATEMENT_TYPE_SELECT)
    comptime INSERT = Self(DUCKDB_V2_STATEMENT_TYPE_INSERT)
    comptime UPDATE = Self(DUCKDB_V2_STATEMENT_TYPE_UPDATE)
    comptime DELETE = Self(DUCKDB_V2_STATEMENT_TYPE_DELETE)
    comptime CREATE = Self(DUCKDB_V2_STATEMENT_TYPE_CREATE)
    comptime DROP = Self(DUCKDB_V2_STATEMENT_TYPE_DROP)
    comptime ALTER = Self(DUCKDB_V2_STATEMENT_TYPE_ALTER)
    comptime TRANSACTION = Self(DUCKDB_V2_STATEMENT_TYPE_TRANSACTION)
    comptime COPY = Self(DUCKDB_V2_STATEMENT_TYPE_COPY)
    comptime EXPLAIN = Self(DUCKDB_V2_STATEMENT_TYPE_EXPLAIN)
    comptime PRAGMA = Self(DUCKDB_V2_STATEMENT_TYPE_PRAGMA)
    comptime SET = Self(DUCKDB_V2_STATEMENT_TYPE_SET)
    comptime LOAD = Self(DUCKDB_V2_STATEMENT_TYPE_LOAD)
    comptime CALL = Self(DUCKDB_V2_STATEMENT_TYPE_CALL)
    comptime ATTACH = Self(DUCKDB_V2_STATEMENT_TYPE_ATTACH)
    comptime DETACH = Self(DUCKDB_V2_STATEMENT_TYPE_DETACH)
    comptime MERGE_INTO = Self(DUCKDB_V2_STATEMENT_TYPE_MERGE_INTO)
    comptime MULTI = Self(DUCKDB_V2_STATEMENT_TYPE_MULTI)

    def write_to(self, mut writer: Some[Writer]):
        writer.write("StatementType(", self.value, ")")


@fieldwise_init
struct ResultType(Equatable, ImplicitlyCopyable, Writable):
    """What a result holds: rows, a changed-row count, or nothing."""

    var value: DUCKDB_V2_RESULT_TYPE

    comptime ROWS = Self(DUCKDB_V2_RESULT_TYPE_QUERY_RESULT)
    comptime CHANGED_ROWS = Self(DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS)
    comptime NOTHING = Self(DUCKDB_V2_RESULT_TYPE_NOTHING)

    def write_to(self, mut writer: Some[Writer]):
        if self == Self.ROWS:
            writer.write("ROWS")
        elif self == Self.CHANGED_ROWS:
            writer.write("CHANGED_ROWS")
        elif self == Self.NOTHING:
            writer.write("NOTHING")
        else:
            writer.write("ResultType(", self.value, ")")


def _columns(schema: ArcPointer[_Schema]) -> List[Column]:
    var out = List[Column]()
    for i in range(len(schema[].names)):
        out.append(Column(schema[].names[i].copy(), schema[].types[i].copy()))
    return out^


def _text_sink(
    text: Pointer[duckdb_v2_str, ImmUntrackedOrigin],
    user_data: Pointer[NoneType, MutUntrackedOrigin],
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    """Collect rendered text into the `String` that `user_data` points to."""
    user_data.unsafe_bitcast[String]()[] += str_to_string(text[])


struct Result(Movable):
    """The result of one statement.

    Statements that return rows stream: nothing runs until rows are read, and
    the connection refuses other statements until the result is read to the
    end or destroyed. Mojo destroys a result right after its last use, so an
    abandoned result frees the connection on its own. Statements without rows
    (INSERT, CREATE, ...) have already run to completion when `execute` returns.

    ```mojo
    for chunk in con.execute("SELECT * FROM t").chunks():
        ...
    var rows = con.execute("SELECT * FROM t").fetchall()
    var n = con.execute("DELETE FROM t WHERE x < 0").rows_changed()
    ```
    """

    var _handle: duckdb_v2_result_handle  # NULL once finished
    var _schema: ArcPointer[_Schema]
    var _statement_type: StatementType
    var _result_type: ResultType
    var _rows_changed: Int
    # The chunk `fetchone`/`fetchmany` are reading, with its views.
    var _buffer: Optional[Chunk]
    var _buffer_tree: Optional[_ReadTree]
    var _buffer_row: Int

    def __init__(out self, *, var _owned: duckdb_v2_result_handle) raises DuckDBError:
        """Wrap a fresh V2 result. Statements without rows are drained here."""
        self._handle = _owned
        var err = null_error_info()
        var stype: DUCKDB_V2_STATEMENT_TYPE = 0
        var rtype: DUCKDB_V2_RESULT_TYPE = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT
        var schema = null[duckdb_v2_schema_handle]()
        self._statement_type = StatementType.INVALID
        self._result_type = ResultType.ROWS
        self._rows_changed = 0
        self._schema = ArcPointer(_Schema([], []))
        self._buffer = None
        self._buffer_tree = None
        self._buffer_row = 0
        try:
            check(lib().duckdb_v2_result_get_statement_type(self._handle, Pointer(to=stype), Pointer(to=err)), err)
            self._statement_type = StatementType(stype)
            var code = lib().duckdb_v2_result_get_result_type(self._handle, Pointer(to=rtype), Pointer(to=err))
            # Expanding statements (PIVOT, ...) only know their metadata once
            # they run; they produce rows, so they stream like a query.
            if code == DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED:
                _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))  # resets the slot
            else:
                check(code, err)
                self._result_type = ResultType(rtype)
                check(lib().duckdb_v2_result_get_schema(self._handle, Pointer(to=schema), Pointer(to=err)), err)
                self._schema = ArcPointer(_read_schema(schema))
                _ = lib().duckdb_v2_schema_destroy(Pointer(to=schema))
            if self._result_type != ResultType.ROWS:
                self._finish()
        except e:
            _ = lib().duckdb_v2_result_destroy(Pointer(to=self._handle))
            raise e^

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle
        self._schema = move._schema^
        self._statement_type = move._statement_type
        self._result_type = move._result_type
        self._rows_changed = move._rows_changed
        self._buffer = move._buffer^
        self._buffer_tree = move._buffer_tree^
        self._buffer_row = move._buffer_row

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_result_destroy(Pointer(to=self._handle))

    def _finish(mut self) raises DuckDBError:
        """Run the statement to the end (applying its side effects) and free the connection."""
        if Int(self._handle) == 0:
            return
        var err = null_error_info()
        var changed: idx_t = 0
        var code = lib().duckdb_v2_result_drain(self._handle, Pointer(to=changed), Pointer(to=err))
        _ = lib().duckdb_v2_result_destroy(Pointer(to=self._handle))
        check(code, err)
        self._rows_changed = Int(changed)

    # ── Metadata ──────────────────────────────────────────────────

    def statement_type(self) -> StatementType:
        return self._statement_type

    def result_type(self) -> ResultType:
        return self._result_type

    def rows_changed(self) -> Int:
        """Rows inserted, updated or deleted (0 for statements that return rows)."""
        return self._rows_changed

    def columns(self) -> List[Column]:
        return _columns(self._schema)

    def column_count(self) -> Int:
        return len(self._schema[].names)

    def column_names(self) -> List[String]:
        return self._schema[].names.copy()

    # ── Reading ───────────────────────────────────────────────────

    def fetch_chunk(mut self) raises DuckDBError -> Optional[Chunk]:
        """The next chunk of rows, or `None` at the end."""
        if self._buffer:
            if self._buffer_row < len(self._buffer.value()):
                raise api_error("fetchone/fetchmany left rows of a chunk unread; read the rest with them")
            self._buffer_tree = None
            self._buffer = None
        if Int(self._handle) == 0:
            return None
        var err = null_error_info()
        var chunk = null[duckdb_v2_data_chunk_handle]()
        check(lib().duckdb_v2_result_fetch_chunk(self._handle, Pointer(to=chunk), Pointer(to=err)), err)
        if Int(chunk) == 0:
            _ = lib().duckdb_v2_result_destroy(Pointer(to=self._handle))
            return None
        return Chunk(_owned=chunk, schema=self._schema)

    def fetchone[T: Movable & Deinitable](mut self) raises DuckDBError -> Optional[T]:
        """The next row as `T`, or `None` at the end.

        `T` is a struct (columns matched to fields by name,
        case-insensitively), a `Tuple` (by position), or a scalar type when
        the result has one column.

        ```mojo
        var r = con.execute("SELECT name, age FROM users")
        while True:
            var user = r.fetchone[User]()
            if not user:
                break
            print(user.value().name)
        ```
        """
        while True:
            if self._buffer and self._buffer_row < len(self._buffer.value()):
                var row = self._buffer_row
                self._buffer_row += 1
                return Optional[T](read_row[T](self._buffer_tree.value(), row))
            var chunk = self.fetch_chunk()
            if not chunk:
                return None
            self._buffer_tree = chunk.value()._read_tree()
            self._buffer = chunk^
            self._buffer_row = 0

    def fetchmany[T: Movable & Deinitable](mut self, n: Int) raises DuckDBError -> List[T]:
        """Up to `n` next rows as `T` (see `fetchone`)."""
        var out = List[T](capacity=n)
        while len(out) < n:
            var row = self.fetchone[T]()
            if not row:
                break
            out.append(row.take())
        return out^

    def get[T: Movable & Deinitable](var self) raises DuckDBError -> List[T]:
        """All remaining rows as `T` (see `fetchone`; consumes the result)."""
        var out = List[T]()
        while True:
            var row = self.fetchone[T]()
            if not row:
                break
            out.append(row.take())
        return out^

    def chunks(var self) -> ChunkIter:
        """Iterate over the remaining chunks (consumes the result).

        A DuckDB error while reading is raised out of the `for` loop.
        """
        return ChunkIter(self^)

    def try_chunks(var self) -> TryChunkIter:
        """Iterate over the remaining chunks as `ChunkResult`s (consumes the result).

        A standard `Iterator`, so it works with adapters like `enumerate`.
        Unwrap each element with `.get()`, which raises a reading error.
        """
        return TryChunkIter(self^)

    def fetchall(var self) raises DuckDBError -> MaterializedResult:
        """Read all remaining rows into memory (consumes the result)."""
        var chunks = List[Chunk]()
        while True:
            var chunk = self.fetch_chunk()
            if not chunk:
                break
            chunks.append(chunk.take())
        return MaterializedResult(chunks^, self._schema, self._statement_type)

    def render(var self, max_rows: Int = 20, max_width: Int = 0, max_col_width: Int = 0) raises DuckDBError -> String:
        """The remaining rows as the box table the DuckDB CLI prints (consumes the result).

        `max_rows` bounds what is shown, not what is read: the whole remaining
        result is read. 0 for `max_width`/`max_col_width` picks the defaults.
        """
        if Int(self._handle) == 0:
            raise api_error("the result has no rows to render")
        var out = String()
        var null_text = StrArg("NULL")
        var err = null_error_info()
        var code = lib().duckdb_v2_result_render_box(
            Pointer(to=self._handle),
            idx_t(max_rows),
            idx_t(max_width),
            idx_t(max_col_width),
            null_text.ptr(),
            0,
            0,
            _text_sink,
            Pointer(to=out).unsafe_bitcast[NoneType](),
            Pointer(to=err),
        )
        check(code, err)
        return out^

    def show(var self, max_rows: Int = 20, max_width: Int = 0, max_col_width: Int = 0) raises DuckDBError:
        """Print the remaining rows as a box table (consumes the result)."""
        print(self^.render(max_rows, max_width, max_col_width), end="")


struct ChunkIter(Copyable):
    """Iterates over the chunks of a `Result`; `for` loops raise DuckDB errors.

    A streaming result can fail while it is read (a runtime error such as a
    division by zero). Mojo's `Iterator` protocol only lets `__next__` raise
    `StopIteration`, which a `for` loop swallows. This type uses the other
    protocol the compiler accepts: `__has_next__()` fetches the next chunk and
    raises `DuckDBError` on failure, which propagates out of the loop, and a
    non-raising `__next__()` hands the chunk out.

    That protocol is no longer part of the documented `Iterator` trait (Mojo
    0.26.1), so this type does not conform to `Iterator` and does not work
    with adapters such as `enumerate`; use `Result.try_chunks()` for those.
    `test_chunks_for_loop_raises` pins the behavior.
    """

    # Shared, because a `for` loop takes the iterable by reference and needs
    # an iterator value from it.
    var _result: ArcPointer[Result]
    var _next: ArcPointer[Optional[Chunk]]

    def __init__(out self, var result: Result):
        self._result = ArcPointer(result^)
        self._next = ArcPointer(Optional[Chunk]())

    def __iter__(ref self) -> Self:
        return self.copy()

    def __has_next__(mut self) raises DuckDBError -> Bool:
        var chunk = self._result[].fetch_chunk()
        if not chunk:
            return False
        self._next[] = chunk^
        return True

    def __next__(mut self) -> Chunk:
        return self._next[].take()


struct ChunkResult(Movable):
    """One step of `Result.try_chunks()`: a chunk, or the error that ended the result."""

    var _chunk: Optional[Chunk]
    var _error: Optional[DuckDBError]

    def __init__(out self, var chunk: Chunk):
        self._chunk = chunk^
        self._error = None

    def __init__(out self, var error: DuckDBError):
        self._chunk = None
        self._error = error^

    def is_error(self) -> Bool:
        return Bool(self._error)

    def error(self) -> Optional[DuckDBError]:
        return self._error.copy()

    def get(ref self) raises DuckDBError -> ref[self._chunk.value()] Chunk:
        """The chunk (borrowed), or raise the error."""
        if self._error:
            raise self._error.value().copy()
        if not self._chunk:
            raise api_error("the chunk was already taken")
        return self._chunk.value()

    def take(mut self) raises DuckDBError -> Chunk:
        """Move the chunk out (use with `for var item in ...`), or raise the error."""
        if self._error:
            raise self._error.value().copy()
        if not self._chunk:
            raise api_error("the chunk was already taken")
        return self._chunk.take()


struct TryChunkIter(Copyable, Iterable, Iterator):
    """Iterates over the chunks of a `Result` as `ChunkResult`s (a standard `Iterator`).

    An error ends the iteration after it has been handed out as the last
    element, so `chunk.get()` in the loop body raises it:

    ```mojo
    for item in con.execute(q).try_chunks():
        var chunk = item.get()  # raises DuckDBError if reading failed
    ```
    """

    comptime Element = ChunkResult
    comptime IteratorType[o: Origin] = Self

    var _result: ArcPointer[Result]
    var _failed: ArcPointer[Bool]

    def __init__(out self, var result: Result):
        self._result = ArcPointer(result^)
        self._failed = ArcPointer(False)

    def __iter__(ref self) -> Self:
        return self.copy()

    def __next__(mut self) raises StopIteration -> ChunkResult:
        if self._failed[]:
            raise StopIteration()
        var chunk: Optional[Chunk]
        try:
            chunk = self._result[].fetch_chunk()
        except e:
            self._failed[] = True
            return ChunkResult(e^)
        if not chunk:
            raise StopIteration()
        return ChunkResult(chunk.take())


struct MaterializedResult(Movable, Sized):
    """All rows of a result, in memory.

    ```mojo
    var r = con.execute("SELECT id, name FROM users").fetchall()
    for i in range(len(r)):
        print(r.get[Int64](col=0, row=i), r.get[String](col=1, row=i))
    var names = r.get[String](col=1)
    ```
    """

    var _chunks: List[Chunk]
    var _starts: List[Int]  # first row of each chunk
    var _size: Int
    var _schema: ArcPointer[_Schema]
    var _statement_type: StatementType

    def __init__(out self, var chunks: List[Chunk], schema: ArcPointer[_Schema], statement_type: StatementType):
        self._starts = List[Int](capacity=len(chunks))
        self._size = 0
        for i in range(len(chunks)):
            self._starts.append(self._size)
            self._size += len(chunks[i])
        self._chunks = chunks^
        self._schema = schema
        self._statement_type = statement_type

    def __len__(self) -> Int:
        return self._size

    def column_count(self) -> Int:
        return len(self._schema[].names)

    def columns(self) -> List[Column]:
        return _columns(self._schema)

    def column_names(self) -> List[String]:
        return self._schema[].names.copy()

    def statement_type(self) -> StatementType:
        return self._statement_type

    def chunk_count(self) -> Int:
        return len(self._chunks)

    def chunk(ref self, i: Int) -> ref[self._chunks[0]] Chunk:
        return self._chunks[i]

    def _locate(self, row: Int) raises DuckDBError -> Tuple[Int, Int]:
        """(chunk index, row in chunk) by binary search over chunk starts."""
        if row < 0 or row >= self._size:
            raise api_error(String("row ", row, " out of range (", self._size, " rows)"))
        var lo = 0
        var hi = len(self._starts) - 1
        while lo < hi:
            var mid = (lo + hi + 1) // 2
            if self._starts[mid] <= row:
                lo = mid
            else:
                hi = mid - 1
        return (lo, row - self._starts[lo])

    def column_index(self, name: StringSlice) raises DuckDBError -> Int:
        """The index of the column called `name` (case-insensitive)."""
        var lower = String(name).lower()
        for i in range(len(self._schema[].names)):
            if self._schema[].names[i].lower() == lower:
                return i
        raise api_error(String("no column named ", name))

    def get[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> T:
        """One value. Raises on NULL (use `get_optional`)."""
        var at = self._locate(row)
        return self._chunks[at[0]].get[T](col=col, row=at[1])

    def get_optional[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> Optional[T]:
        """One value, or `None` for NULL."""
        var at = self._locate(row)
        return self._chunks[at[0]].get_optional[T](col=col, row=at[1])

    def get[T: Movable & Deinitable](self, *, row: Int) raises DuckDBError -> T:
        """Row `row` as `T`: a struct (columns by name), a `Tuple` (by
        position), or a scalar when the result has one column."""
        var at = self._locate(row)
        return self._chunks[at[0]].get[T](row=at[1])

    def get[T: Movable & Deinitable](self) raises DuckDBError -> List[T]:
        """All rows as `T` (see `get[T](row=)`).

        ```mojo
        var users = con.execute("SELECT * FROM users").fetchall().get[User]()
        ```
        """
        var out = List[T](capacity=self._size)
        for c in range(len(self._chunks)):
            self._chunks[c]._append_rows[T](out)
        return out^

    def get[T: Movable & Deinitable](self, *, col: Int) raises DuckDBError -> List[T]:
        """A whole column. Raises if it holds a NULL (use `get_optional`)."""
        var out = List[T](capacity=self._size)
        for c in range(len(self._chunks)):
            var reader = self._chunks[c].column[T](col)
            for i in range(len(reader)):
                out.append(reader[i])
        return out^

    def get_optional[T: Movable & Deinitable](self, *, col: Int) raises DuckDBError -> List[Optional[T]]:
        """A whole column, NULLs as `None`."""
        var out = List[Optional[T]](capacity=self._size)
        for c in range(len(self._chunks)):
            var reader = self._chunks[c].column[T](col)
            for i in range(len(reader)):
                out.append(reader.get(i))
        return out^

    def row(ref self, i: Int) -> Row[origin_of(self)]:
        return Row[origin_of(self)](Pointer(to=self), i)

    def rows(ref self) -> RowIter[origin_of(self)]:
        """Iterate over the rows; each `Row` borrows this result."""
        return RowIter[origin_of(self)](Pointer(to=self), 0)


struct Row[origin: ImmOrigin](Copyable, Movable):
    """One row of a `MaterializedResult`. It borrows the result."""

    var _result: Pointer[MaterializedResult, Self.origin]
    var _row: Int

    def __init__(out self, result: Pointer[MaterializedResult, Self.origin], row: Int):
        self._result = result
        self._row = row

    def index(self) -> Int:
        return self._row

    def get[T: Movable & Deinitable](self, col: Int) raises DuckDBError -> T:
        return self._result[].get[T](col=col, row=self._row)

    def get[T: Movable & Deinitable](self) raises DuckDBError -> T:
        """The whole row as `T` (a struct, a `Tuple`, or a scalar for one column)."""
        return self._result[].get[T](row=self._row)

    def get_optional[T: Movable & Deinitable](self, col: Int) raises DuckDBError -> Optional[T]:
        return self._result[].get_optional[T](col=col, row=self._row)


struct RowIter[origin: ImmOrigin](Copyable, Iterable, Iterator):
    comptime Element = Row[Self.origin]
    comptime IteratorType[o: Origin] = Self

    var _result: Pointer[MaterializedResult, Self.origin]
    var _next: Int

    def __init__(out self, result: Pointer[MaterializedResult, Self.origin], start: Int):
        self._result = result
        self._next = start

    def __iter__(ref self) -> Self:
        return self.copy()

    def __next__(mut self) raises StopIteration -> Row[Self.origin]:
        if self._next >= len(self._result[]):
            raise StopIteration()
        self._next += 1
        return Row[Self.origin](self._result, self._next - 1)
