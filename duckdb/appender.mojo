"""`Appender`: bulk loading rows into a table."""

from duckdb._libduckdb_v2 import *
from duckdb._sql_util import _quote_ident, _quote_qualified
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb._runtime import api_error, check, lib
from duckdb.database import Connection, _Statements
from duckdb.error import DuckDBError
from duckdb.result import Result
from duckdb.serde import ChunkWriter
from duckdb.statement import _Arguments
from duckdb.types import LogicalType, TypeId
from duckdb.value import Value

comptime _DATA = "__mojo_appender_rows"
comptime _PATTERN = "__mojo_default_pattern"


@fieldwise_init
struct OnConflict(Equatable, ImplicitlyCopyable):
    """What an `Appender` does with rows that violate a primary key or
    unique constraint."""

    var _value: Int

    comptime ERROR = Self(0)
    """Fail the flush (the default)."""
    comptime IGNORE = Self(1)
    """Skip the conflicting rows (`INSERT OR IGNORE`)."""
    comptime REPLACE = Self(2)
    """Replace the existing rows (`INSERT OR REPLACE`)."""


struct Appender[origin: ImmOrigin](Movable):
    """Loads rows into a table in batches.

    Works like DuckDB's own appender: rows are written into data chunks in
    DuckDB's storage layout and collected in memory (DuckDB can spill the
    collection to disk), then inserted with one `INSERT ... SELECT` over the
    collection. That happens every `flush_rows` rows (204,800 by default, as
    in DuckDB), on `flush()` and `close()`, and on destruction.

    ```mojo
    var appender = Appender(con, "users")
    appender.append_row(User(1, "Mark"))
    appender.append_value(Int64(2))
    appender.append_value(String("Hannes"))
    appender.end_row()
    appender.close()
    ```

    Structs map to columns by name (case-insensitive) and tuples by
    position. Columns without a field become NULL. Every mapped type works,
    including lists, maps and nested structs. Values whose type differs
    from the column's are cast by DuckDB (an `Int` into an INTEGER column).

    Errors:
    - A value that cannot be written raises from `append_*`, and the row
      can be written again.
    - Constraint violations show up when rows are inserted, at a flush. The
      rows of a failed flush are dropped, and the error says how many.
      `on_conflict` skips or replaces rows with duplicate keys instead.
    - Destruction flushes too, but it cannot raise: it prints the error.
      Call `close()` to see it.

    It borrows its connection, which stays alive as long as the appender.
    """

    var _conn: Pointer[Connection, Self.origin]
    var _target: String
    var _insert: String  # "INSERT [OR ...] INTO <table>"
    var _names: List[String]  # quoted column names
    var _types: List[LogicalType]
    var _writer: ChunkWriter
    var _collection: duckdb_v2_column_data_collection_handle
    var _append_state: duckdb_v2_column_data_collection_append_state_handle
    var _flush_rows: Int
    var _pending: Int  # rows in the collection
    var _col: Int  # next column of the current row for append_value
    # append_default: which columns of the current row take their default,
    # and each distinct set of defaulted columns in the batch (index 0: none).
    var _defaulted: List[Bool]
    var _patterns: List[List[Bool]]

    def __init__(
        out self,
        ref[Self.origin] con: Connection,
        table: String,
        columns: Optional[List[String]] = None,
        on_conflict: OnConflict = OnConflict.ERROR,
        flush_rows: Int = 204800,
    ) raises DuckDBError:
        """An appender for `table` (optionally `schema.table`).

        With `columns`, rows hold only those columns and the others take
        their defaults.
        """
        self._conn = Pointer(to=con)
        self._target = _quote_qualified(table)
        var insert = String("INSERT")
        if on_conflict == OnConflict.IGNORE:
            insert += " OR IGNORE"
        elif on_conflict == OnConflict.REPLACE:
            insert += " OR REPLACE"
        self._insert = String(insert, " INTO ", self._target)
        var select_list = String("*")
        if columns:
            var quoted = List[String]()
            for c in columns.value():
                quoted.append(_quote_ident(c))
            select_list = String(", ").join(quoted)
        var description = con.describe(String("SELECT ", select_list, " FROM ", self._target))
        self._types = List[LogicalType]()
        self._names = List[String]()
        var names = List[String]()
        for c in description.columns:
            self._types.append(c.type.copy())
            self._names.append(_quote_ident(c.name))
            names.append(c.name.copy())
        var n = len(self._types)
        # One extra column: the set of defaulted columns of each row.
        var chunk_types = self._types.copy()
        chunk_types.append(LogicalType(TypeId.INTEGER))
        names.append(_PATTERN)
        self._writer = ChunkWriter(chunk_types.copy(), names^, con.factory(), visible=n)
        self._collection = null[duckdb_v2_column_data_collection_handle]()
        self._append_state = null[duckdb_v2_column_data_collection_append_state_handle]()
        self._flush_rows = max(flush_rows, 1)
        self._pending = 0
        self._col = 0
        self._defaulted = List[Bool](length=n, fill=False)
        self._patterns = [List[Bool](length=n, fill=False)]
        var handles = List[duckdb_v2_logical_type_handle]()
        for i in range(len(chunk_types)):
            handles.append(chunk_types[i].handle())
        var err = null_error_info()
        check(
            lib().duckdb_v2_column_data_collection_create(
                con.factory(), handles.unsafe_ptr().as_imm(), idx_t(len(handles)), Pointer(to=self._collection), Pointer(to=err)
            ),
            err,
        )
        _ = handles^
        _ = chunk_types^
        self._new_append_state()

    def __init__(out self, *, deinit move: Self):
        self._conn = move._conn
        self._target = move._target^
        self._insert = move._insert^
        self._names = move._names^
        self._types = move._types^
        self._writer = move._writer^
        self._collection = move._collection
        self._append_state = move._append_state
        self._flush_rows = move._flush_rows
        self._pending = move._pending
        self._col = move._col
        self._defaulted = move._defaulted^
        self._patterns = move._patterns^

    def __deinit__(deinit self):
        try:
            _ = self.flush()
        except e:
            print("Appender: flush on destruction failed:", e)
        # The C destroy functions NULL the slots they are given, so destroying
        # a handle an earlier path already destroyed is a no-op.
        _ = lib().duckdb_v2_column_data_collection_append_state_destroy(Pointer(to=self._append_state))
        _ = lib().duckdb_v2_column_data_collection_destroy(Pointer(to=self._collection))

    def _new_append_state(mut self) raises DuckDBError:
        _ = lib().duckdb_v2_column_data_collection_append_state_destroy(Pointer(to=self._append_state))
        var err = null_error_info()
        check(
            lib().duckdb_v2_column_data_collection_append_state_create(
                self._collection, Pointer(to=self._append_state), Pointer(to=err)
            ),
            err,
        )

    def column_count(self) -> Int:
        return len(self._types)

    # ── Appending ─────────────────────────────────────────────────

    def _check_room(self) raises DuckDBError:
        if self._col >= len(self._types):
            raise api_error(String("the row already has all ", len(self._types), " values; call end_row()"))

    def append_value[T: AnyType](mut self, value: T) raises DuckDBError:
        """Append a value to the current row (finish the row with `end_row`)."""
        self._check_room()
        self._writer.write_cell(self._col, value)
        self._col += 1

    def append_null(mut self) raises DuckDBError:
        """Append a NULL to the current row."""
        self._check_room()
        self._writer.write_null(self._col)
        self._col += 1

    def append_default(mut self) raises DuckDBError:
        """Let the current column of the row take its default (NULL when it
        has none). Defaults are evaluated per row, so `nextval(...)` works."""
        self._check_room()
        self._writer.write_null(self._col)
        self._defaulted[self._col] = True
        self._col += 1

    def end_row(mut self) raises DuckDBError:
        """Finish the row started with `append_value`/`append_null`/`append_default`."""
        if self._col != len(self._types):
            raise api_error(String("the row has ", self._col, " values, but the table has ", len(self._types), " columns"))
        var pattern = self._pattern_id()
        self._writer.store_int32(len(self._types), Int32(pattern))
        self._writer.end_row()
        if pattern == len(self._patterns):
            self._patterns.append(self._defaulted.copy())
        self._col = 0
        for i in range(len(self._defaulted)):
            self._defaulted[i] = False
        self._after_row()

    def _pattern_id(self) raises DuckDBError -> Int:
        for p in range(len(self._patterns)):
            if self._patterns[p] == self._defaulted:
                return p
        var all_default = True
        for d in self._defaulted:
            all_default = all_default and d
        if all_default:
            raise api_error("a row needs at least one value that is not a default")
        return len(self._patterns)  # a new one, recorded by end_row

    def append_row[T: AnyType](mut self, row: T) raises DuckDBError:
        """Append a whole row: a struct (fields to columns by name) or a `Tuple`."""
        if self._col != 0:
            raise api_error("a row is half written with append_value; call end_row() first")
        self._writer.store_int32(len(self._types), 0)
        self._writer.write_row(row)
        self._after_row()

    def append_rows[T: Copyable](mut self, rows: List[T]) raises DuckDBError:
        """Append each element of `rows` as a row."""
        for i in range(len(rows)):
            self.append_row(rows[i])

    def _after_row(mut self) raises DuckDBError:
        if self._writer.is_full():
            self._spill()
            if self._pending >= self._flush_rows:
                _ = self.flush()

    def _spill(mut self) raises DuckDBError:
        """Move the chunk's rows into the collection."""
        self._writer.finish()
        var err = null_error_info()
        check(
            lib().duckdb_v2_column_data_collection_append(
                self._collection, self._append_state, self._writer.handle(), Pointer(to=err)
            ),
            err,
        )
        self._pending += len(self._writer)
        self._writer.reset()

    # ── Flushing ──────────────────────────────────────────────────

    def flush(mut self) raises DuckDBError -> Int:
        """Insert the rows appended so far. Returns the number of rows inserted.

        If the insert fails, its rows are dropped and the error says how many.
        """
        if self._col != 0:
            raise api_error("a row is half written with append_value; call end_row() first")
        if len(self._writer) > 0:
            self._spill()
        if self._pending == 0:
            return 0
        var rows = self._pending
        var changed = 0
        try:
            if len(self._patterns) == 1:
                changed = self._insert_pattern(-1)
            else:
                # One INSERT per set of defaulted columns, which it leaves out.
                for p in range(len(self._patterns)):
                    changed += self._insert_pattern(p)
        except e:
            self._reset_batch()
            raise DuckDBError(e.code, String(e.message, " (", rows, " appended rows were dropped)"))
        self._reset_batch()
        return changed

    def _insert_pattern(self, pattern: Int) raises DuckDBError -> Int:
        """Insert the rows of one default pattern (-1: all rows, no defaults)."""
        var columns = List[String]()
        var selected = List[String]()
        for i in range(len(self._names)):
            if pattern <= 0 or not self._patterns[pattern][i]:
                columns.append(self._names[i])
                selected.append(String("c", i))
        var sql = String(
            self._insert, " (", String(", ").join(columns), ") SELECT ", String(", ").join(selected), " FROM ", _DATA
        )
        if pattern >= 0:
            sql += String(" WHERE ", _PATTERN, " = ", pattern)
        var statements = _Statements(self._conn[].handle(), sql)
        var stmt = statements.next()
        var err = null_error_info()
        try:
            var name = StrArg(_DATA)
            var column_names = List[StrArg]()
            for i in range(len(self._names)):
                column_names.append(StrArg(String("c", i)))
            column_names.append(StrArg(_PATTERN))
            var views = List[duckdb_v2_identifier_t]()
            for i in range(len(column_names)):
                views.append(column_names[i].ptr()[].copy())
            check(
                lib().duckdb_v2_statement_add_collection(
                    stmt, name.ptr(), self._collection, views.unsafe_ptr().as_imm(), idx_t(len(views)), Pointer(to=err)
                ),
                err,
            )
            _ = views^
            _ = column_names^
            var arguments = _Arguments(List[Value](), List[String]())
            return Result(_owned=self._conn[]._execute_statement(stmt, arguments)).rows_changed()
        finally:
            _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=stmt))
            _ = statements^

    def _reset_batch(mut self) raises DuckDBError:
        var err = null_error_info()
        if lib().duckdb_v2_column_data_collection_clear(self._collection, Pointer(to=err)) != DUCKDB_V2_ERROR_NONE:
            _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))
        self._pending = 0
        self._patterns = [List[Bool](length=len(self._types), fill=False)]
        self._new_append_state()

    def close(mut self) raises DuckDBError:
        """Flush the remaining rows (the appender stays usable)."""
        _ = self.flush()
