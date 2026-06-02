from duckdb._libduckdb import *
from duckdb.chunk import Chunk, Row
from duckdb.duckdb_type import DuckDBType
from duckdb.typed_api import mojo_type_to_duckdb_type, deserialize_from_vector
from std.collections import Optional
from std.builtin.error import StackTrace
from std.iter import Iterator, Iterable, StopIteration


@fieldwise_init
struct ResultType(
    Comparable,
    ImplicitlyCopyable,
    Writable,
):
    """Represents DuckDB result types.
    
    Defines the available result types that indicate what kind of result
    a query produced.
    """

    var _value: Int32

    comptime INVALID = Self(DUCKDB_RESULT_TYPE_INVALID)
    """Invalid result type."""

    comptime CHANGED_ROWS = Self(DUCKDB_RESULT_TYPE_CHANGED_ROWS)
    """Result represents changed rows (INSERT/UPDATE/DELETE)."""

    comptime NOTHING = Self(DUCKDB_RESULT_TYPE_NOTHING)
    """Result represents nothing (e.g., CREATE TABLE)."""

    comptime QUERY_RESULT = Self(DUCKDB_RESULT_TYPE_QUERY_RESULT)
    """Result represents a query result with data."""

    @always_inline
    def __eq__(self, other: Self) -> Bool:
        """Returns True if this result type equals the other result type.

        Args:
            other: The result type to compare with.

        Returns:
            Bool: True if the result types are equal, False otherwise.
        """
        return self._value == other._value

    def __lt__(self, other: Self) -> Bool:
        """Returns True if this result type is less than the other result type.

        Args:
            other: The result type to compare with.

        Returns:
            Bool: True if this result type is less than the other result type,
                False otherwise.
        """
        return self._value < other._value

    def write_to[W: Writer](self, mut writer: W):
        """Writes the string representation of this result type to a writer.

        Parameters:
            W: The writer type.

        Args:
            writer: The writer to write to.
        """
        if self == Self.INVALID:
            writer.write("INVALID")
        elif self == Self.CHANGED_ROWS:
            writer.write("CHANGED_ROWS")
        elif self == Self.NOTHING:
            writer.write("NOTHING")
        elif self == Self.QUERY_RESULT:
            writer.write("QUERY_RESULT")

    @no_inline
    def __str__(self) -> String:
        """Returns the string representation of this result type.

        Returns:
            String: A human-readable string representation of the result type
                (e.g., "INVALID", "QUERY_RESULT").
        """
        return String.write(self)

    @no_inline
    def __repr__(self) -> String:
        """Returns the detailed string representation of this result type.

        Returns:
            String: A string representation including the type name and value
                (e.g., "ResultType.QUERY_RESULT").
        """
        return String("ResultType.", self)


@fieldwise_init
struct ErrorType(
    Comparable,
    ImplicitlyCopyable,
    Writable,
):
    """Represents DuckDB error types.
    
    Defines the available error types that classify different kinds of errors
    that can occur during query execution.
    """

    var _value: Int32

    comptime INVALID = Self(DUCKDB_ERROR_INVALID)
    """No error / invalid error type."""
    
    comptime OUT_OF_RANGE = Self(DUCKDB_ERROR_OUT_OF_RANGE)
    """Value out of range error."""
    
    comptime CONVERSION = Self(DUCKDB_ERROR_CONVERSION)
    """Conversion/casting error."""
    
    comptime UNKNOWN_TYPE = Self(DUCKDB_ERROR_UNKNOWN_TYPE)
    """Unknown type error."""
    
    comptime DECIMAL = Self(DUCKDB_ERROR_DECIMAL)
    """Decimal related error."""
    
    comptime MISMATCH_TYPE = Self(DUCKDB_ERROR_MISMATCH_TYPE)
    """Type mismatch error."""
    
    comptime DIVIDE_BY_ZERO = Self(DUCKDB_ERROR_DIVIDE_BY_ZERO)
    """Division by zero error."""
    
    comptime OBJECT_SIZE = Self(DUCKDB_ERROR_OBJECT_SIZE)
    """Object size error."""
    
    comptime INVALID_TYPE = Self(DUCKDB_ERROR_INVALID_TYPE)
    """Invalid type error."""
    
    comptime SERIALIZATION = Self(DUCKDB_ERROR_SERIALIZATION)
    """Serialization error."""
    
    comptime TRANSACTION = Self(DUCKDB_ERROR_TRANSACTION)
    """Transaction error."""
    
    comptime NOT_IMPLEMENTED = Self(DUCKDB_ERROR_NOT_IMPLEMENTED)
    """Feature not implemented error."""
    
    comptime EXPRESSION = Self(DUCKDB_ERROR_EXPRESSION)
    """Expression error."""
    
    comptime CATALOG = Self(DUCKDB_ERROR_CATALOG)
    """Catalog error."""
    
    comptime PARSER = Self(DUCKDB_ERROR_PARSER)
    """Parser error."""
    
    comptime PLANNER = Self(DUCKDB_ERROR_PLANNER)
    """Planner error."""
    
    comptime SCHEDULER = Self(DUCKDB_ERROR_SCHEDULER)
    """Scheduler error."""
    
    comptime EXECUTOR = Self(DUCKDB_ERROR_EXECUTOR)
    """Executor error."""
    
    comptime CONSTRAINT = Self(DUCKDB_ERROR_CONSTRAINT)
    """Constraint violation error."""
    
    comptime INDEX = Self(DUCKDB_ERROR_INDEX)
    """Index error."""
    
    comptime STAT = Self(DUCKDB_ERROR_STAT)
    """Statistics error."""
    
    comptime CONNECTION = Self(DUCKDB_ERROR_CONNECTION)
    """Connection error."""
    
    comptime SYNTAX = Self(DUCKDB_ERROR_SYNTAX)
    """Syntax error."""
    
    comptime SETTINGS = Self(DUCKDB_ERROR_SETTINGS)
    """Settings error."""
    
    comptime BINDER = Self(DUCKDB_ERROR_BINDER)
    """Binder error."""
    
    comptime NETWORK = Self(DUCKDB_ERROR_NETWORK)
    """Network error."""
    
    comptime OPTIMIZER = Self(DUCKDB_ERROR_OPTIMIZER)
    """Optimizer error."""
    
    comptime NULL_POINTER = Self(DUCKDB_ERROR_NULL_POINTER)
    """Null pointer error."""
    
    comptime IO = Self(DUCKDB_ERROR_IO)
    """IO error."""
    
    comptime INTERRUPT = Self(DUCKDB_ERROR_INTERRUPT)
    """Interrupt error."""
    
    comptime FATAL = Self(DUCKDB_ERROR_FATAL)
    """Fatal error."""
    
    comptime INTERNAL = Self(DUCKDB_ERROR_INTERNAL)
    """Internal error."""
    
    comptime INVALID_INPUT = Self(DUCKDB_ERROR_INVALID_INPUT)
    """Invalid input error."""
    
    comptime OUT_OF_MEMORY = Self(DUCKDB_ERROR_OUT_OF_MEMORY)
    """Out of memory error."""
    
    comptime PERMISSION = Self(DUCKDB_ERROR_PERMISSION)
    """Permission error."""
    
    comptime PARAMETER_NOT_RESOLVED = Self(DUCKDB_ERROR_PARAMETER_NOT_RESOLVED)
    """Parameter not resolved error."""
    
    comptime PARAMETER_NOT_ALLOWED = Self(DUCKDB_ERROR_PARAMETER_NOT_ALLOWED)
    """Parameter not allowed error."""
    
    comptime DEPENDENCY = Self(DUCKDB_ERROR_DEPENDENCY)
    """Dependency error."""
    
    comptime HTTP = Self(DUCKDB_ERROR_HTTP)
    """HTTP error."""
    
    comptime MISSING_EXTENSION = Self(DUCKDB_ERROR_MISSING_EXTENSION)
    """Missing extension error."""
    
    comptime AUTOLOAD = Self(DUCKDB_ERROR_AUTOLOAD)
    """Autoload error."""
    
    comptime SEQUENCE = Self(DUCKDB_ERROR_SEQUENCE)
    """Sequence error."""
    
    comptime INVALID_CONFIGURATION = Self(DUCKDB_INVALID_CONFIGURATION)
    """Invalid configuration error."""

    @always_inline
    def __eq__(self, other: Self) -> Bool:
        """Returns True if this error type equals the other error type.

        Args:
            other: The error type to compare with.

        Returns:
            Bool: True if the error types are equal, False otherwise.
        """
        return self._value == other._value

    def __lt__(self, other: Self) -> Bool:
        """Returns True if this error type is less than the other error type.

        Args:
            other: The error type to compare with.

        Returns:
            Bool: True if this error type is less than the other error type,
                False otherwise.
        """
        return self._value < other._value

    def write_to[W: Writer](self, mut writer: W):
        """Writes the string representation of this error type to a writer.

        Parameters:
            W: The writer type.

        Args:
            writer: The object to write to.
        """
        if self == Self.INVALID:
            writer.write("INVALID")
        elif self == Self.OUT_OF_RANGE:
            writer.write("OUT_OF_RANGE")
        elif self == Self.CONVERSION:
            writer.write("CONVERSION")
        elif self == Self.UNKNOWN_TYPE:
            writer.write("UNKNOWN_TYPE")
        elif self == Self.DECIMAL:
            writer.write("DECIMAL")
        elif self == Self.MISMATCH_TYPE:
            writer.write("MISMATCH_TYPE")
        elif self == Self.DIVIDE_BY_ZERO:
            writer.write("DIVIDE_BY_ZERO")
        elif self == Self.OBJECT_SIZE:
            writer.write("OBJECT_SIZE")
        elif self == Self.INVALID_TYPE:
            writer.write("INVALID_TYPE")
        elif self == Self.SERIALIZATION:
            writer.write("SERIALIZATION")
        elif self == Self.TRANSACTION:
            writer.write("TRANSACTION")
        elif self == Self.NOT_IMPLEMENTED:
            writer.write("NOT_IMPLEMENTED")
        elif self == Self.EXPRESSION:
            writer.write("EXPRESSION")
        elif self == Self.CATALOG:
            writer.write("CATALOG")
        elif self == Self.PARSER:
            writer.write("PARSER")
        elif self == Self.PLANNER:
            writer.write("PLANNER")
        elif self == Self.SCHEDULER:
            writer.write("SCHEDULER")
        elif self == Self.EXECUTOR:
            writer.write("EXECUTOR")
        elif self == Self.CONSTRAINT:
            writer.write("CONSTRAINT")
        elif self == Self.INDEX:
            writer.write("INDEX")
        elif self == Self.STAT:
            writer.write("STAT")
        elif self == Self.CONNECTION:
            writer.write("CONNECTION")
        elif self == Self.SYNTAX:
            writer.write("SYNTAX")
        elif self == Self.SETTINGS:
            writer.write("SETTINGS")
        elif self == Self.BINDER:
            writer.write("BINDER")
        elif self == Self.NETWORK:
            writer.write("NETWORK")
        elif self == Self.OPTIMIZER:
            writer.write("OPTIMIZER")
        elif self == Self.NULL_POINTER:
            writer.write("NULL_POINTER")
        elif self == Self.IO:
            writer.write("IO")
        elif self == Self.INTERRUPT:
            writer.write("INTERRUPT")
        elif self == Self.FATAL:
            writer.write("FATAL")
        elif self == Self.INTERNAL:
            writer.write("INTERNAL")
        elif self == Self.INVALID_INPUT:
            writer.write("INVALID_INPUT")
        elif self == Self.OUT_OF_MEMORY:
            writer.write("OUT_OF_MEMORY")
        elif self == Self.PERMISSION:
            writer.write("PERMISSION")
        elif self == Self.PARAMETER_NOT_RESOLVED:
            writer.write("PARAMETER_NOT_RESOLVED")
        elif self == Self.PARAMETER_NOT_ALLOWED:
            writer.write("PARAMETER_NOT_ALLOWED")
        elif self == Self.DEPENDENCY:
            writer.write("DEPENDENCY")
        elif self == Self.HTTP:
            writer.write("HTTP")
        elif self == Self.MISSING_EXTENSION:
            writer.write("MISSING_EXTENSION")
        elif self == Self.AUTOLOAD:
            writer.write("AUTOLOAD")
        elif self == Self.SEQUENCE:
            writer.write("SEQUENCE")
        elif self == Self.INVALID_CONFIGURATION:
            writer.write("INVALID_CONFIGURATION")

    @no_inline
    def __str__(self) -> String:
        """Returns the string representation of this error type.

        Returns:
            String: A human-readable string representation of the error type
                (e.g., "PARSER", "CONSTRAINT").
        """
        return String.write(self)

    @no_inline
    def __repr__(self) -> String:
        """Returns the detailed string representation of this error type.

        Returns:
            String: A string representation including the type name and value
                (e.g., "ErrorType.PARSER").
        """
        return String("ErrorType.", self)


@fieldwise_init
struct StatementType(
    Comparable,
    ImplicitlyCopyable,
    Writable,
):
    """Represents DuckDB statement types.
    
    Defines the available statement types that indicate what kind of SQL
    statement was executed.
    """

    var _value: Int32

    comptime INVALID = Self(DUCKDB_STATEMENT_TYPE_INVALID)
    """Invalid statement type."""

    comptime SELECT = Self(DUCKDB_STATEMENT_TYPE_SELECT)
    """SELECT statement."""

    comptime INSERT = Self(DUCKDB_STATEMENT_TYPE_INSERT)
    """INSERT statement."""

    comptime UPDATE = Self(DUCKDB_STATEMENT_TYPE_UPDATE)
    """UPDATE statement."""

    comptime EXPLAIN = Self(DUCKDB_STATEMENT_TYPE_EXPLAIN)
    """EXPLAIN statement."""

    comptime DELETE = Self(DUCKDB_STATEMENT_TYPE_DELETE)
    """DELETE statement."""

    comptime PREPARE = Self(DUCKDB_STATEMENT_TYPE_PREPARE)
    """PREPARE statement."""

    comptime CREATE = Self(DUCKDB_STATEMENT_TYPE_CREATE)
    """CREATE statement."""

    comptime EXECUTE = Self(DUCKDB_STATEMENT_TYPE_EXECUTE)
    """EXECUTE statement."""

    comptime ALTER = Self(DUCKDB_STATEMENT_TYPE_ALTER)
    """ALTER statement."""

    comptime TRANSACTION = Self(DUCKDB_STATEMENT_TYPE_TRANSACTION)
    """Transaction statement (BEGIN/COMMIT/ROLLBACK)."""

    comptime COPY = Self(DUCKDB_STATEMENT_TYPE_COPY)
    """COPY statement."""

    comptime ANALYZE = Self(DUCKDB_STATEMENT_TYPE_ANALYZE)
    """ANALYZE statement."""

    comptime VARIABLE_SET = Self(DUCKDB_STATEMENT_TYPE_VARIABLE_SET)
    """Variable SET statement."""

    comptime CREATE_FUNC = Self(DUCKDB_STATEMENT_TYPE_CREATE_FUNC)
    """CREATE FUNCTION statement."""

    comptime DROP = Self(DUCKDB_STATEMENT_TYPE_DROP)
    """DROP statement."""

    comptime EXPORT = Self(DUCKDB_STATEMENT_TYPE_EXPORT)
    """EXPORT statement."""

    comptime PRAGMA = Self(DUCKDB_STATEMENT_TYPE_PRAGMA)
    """PRAGMA statement."""

    comptime VACUUM = Self(DUCKDB_STATEMENT_TYPE_VACUUM)
    """VACUUM statement."""

    comptime CALL = Self(DUCKDB_STATEMENT_TYPE_CALL)
    """CALL statement."""

    @always_inline
    def __eq__(self, other: Self) -> Bool:
        """Returns True if this statement type equals the other statement type.

        Args:
            other: The statement type to compare with.

        Returns:
            Bool: True if the statement types are equal, False otherwise.
        """
        return self._value == other._value

    def __lt__(self, other: Self) -> Bool:
        """Returns True if this statement type is less than the other statement type.

        Args:
            other: The statement type to compare with.

        Returns:
            Bool: True if this statement type is less than the other statement type,
                False otherwise.
        """
        return self._value < other._value

    def write_to[W: Writer](self, mut writer: W):
        """Writes the string representation of this statement type to a writer.

        Parameters:
            W: The writer type.

        Args:
            writer: The writer to write to.
        """
        if self == Self.INVALID:
            writer.write("INVALID")
        elif self == Self.SELECT:
            writer.write("SELECT")
        elif self == Self.INSERT:
            writer.write("INSERT")
        elif self == Self.UPDATE:
            writer.write("UPDATE")
        elif self == Self.EXPLAIN:
            writer.write("EXPLAIN")
        elif self == Self.DELETE:
            writer.write("DELETE")
        elif self == Self.PREPARE:
            writer.write("PREPARE")
        elif self == Self.CREATE:
            writer.write("CREATE")
        elif self == Self.EXECUTE:
            writer.write("EXECUTE")
        elif self == Self.ALTER:
            writer.write("ALTER")
        elif self == Self.TRANSACTION:
            writer.write("TRANSACTION")
        elif self == Self.COPY:
            writer.write("COPY")
        elif self == Self.ANALYZE:
            writer.write("ANALYZE")
        elif self == Self.VARIABLE_SET:
            writer.write("VARIABLE_SET")
        elif self == Self.CREATE_FUNC:
            writer.write("CREATE_FUNC")
        elif self == Self.DROP:
            writer.write("DROP")
        elif self == Self.EXPORT:
            writer.write("EXPORT")
        elif self == Self.PRAGMA:
            writer.write("PRAGMA")
        elif self == Self.VACUUM:
            writer.write("VACUUM")
        elif self == Self.CALL:
            writer.write("CALL")

    @no_inline
    def __str__(self) -> String:
        """Returns the string representation of this statement type.

        Returns:
            String: A human-readable string representation of the statement type
                (e.g., "SELECT", "INSERT").
        """
        return String.write(self)

    @no_inline
    def __repr__(self) -> String:
        """Returns the detailed string representation of this statement type.

        Returns:
            String: A string representation including the type name and value
                (e.g., "StatementType.SELECT").
        """
        return String("StatementType.", self)


@fieldwise_init
struct Column(Movable & Copyable & Writable):
    var index: Int
    var name: String
    var type: LogicalType[is_owned=True, origin=MutExternalOrigin]

    def write_to[W: Writer](self, mut writer: W):
        writer.write(
            "Column(", self.index, ", ", self.name, ": ", self.type, ")"
        )

    def __str__(self) -> String:
        return String(self.type)



struct Result(Writable, Iterable, Movable):
    """A streaming query result.

    Iterating a ``Result`` yields ``Row`` proxies — the most ergonomic
    way to consume query output:

        for row in conn.execute("SELECT name, age FROM users"):
            print(row.get[String](col=0), row.get[Int64](col=1))

    Use ``.chunks()`` when you need batch / columnar access.
    """

    comptime Element = Row
    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = RowIter[ImmutOrigin(iterable_origin)]

    var _result: duckdb_result
    var _columns: List[Column]
    # Streaming cursor for fetchone/fetchmany (shared, DBAPI-style).
    var _cur_chunk: Optional[Chunk[is_owned=True]]
    var _cur_row: Int

    def __init__(out self, result: duckdb_result):
        self._result = result
        self._cur_chunk = None
        self._cur_row = 0
        self._columns = List[Column]()
        for i in range(self.column_count()):
            var borrowed_type = self.column_type(i)
            var col = Column(
                index=i, name=self.column_name(i), type=LogicalType[is_owned=True, origin=MutExternalOrigin](borrowed_type.get_type_id())
            )
            self._columns.append(col^)

    def column_count(self) -> Int:
        ref libduckdb = DuckDB().libduckdb()
        return Int(
            libduckdb.duckdb_column_count(UnsafePointer(to=self._result))
        )

    def column_name(self, col: Int) -> String:
        ref libduckdb = DuckDB().libduckdb()
        var c_str = libduckdb.duckdb_column_name(
            UnsafePointer(to=self._result), UInt64(col)
        )
        return String(unsafe_from_utf8_ptr=c_str)

    def column_types(self) -> List[LogicalType[is_owned=True, origin=MutExternalOrigin]]:
        var types = List[LogicalType[is_owned=True, origin=MutExternalOrigin]]()
        for i in range(self.column_count()):
            # Copy the borrowed type to create an owned version
            var borrowed_type = self.column_type(i)
            types.append(LogicalType[is_owned=True, origin=MutExternalOrigin](borrowed_type.get_type_id()))
        return types^

    def column_type(ref [_]self: Self, col: Int) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](
            libduckdb.duckdb_column_logical_type(
                UnsafePointer(to=self._result), UInt64(col)
            )
        )

    def statement_type(self) -> StatementType:
        """Returns the statement type of the statement that was executed.
        
        Returns:
            StatementType: The type of statement that was executed.
        """
        ref libduckdb = DuckDB().libduckdb()
        return StatementType(libduckdb.duckdb_result_statement_type(self._result))

    def rows_changed(self) -> Int:
        """Returns the number of rows changed by the query stored in the result.
        
        This is relevant only for INSERT/UPDATE/DELETE queries. For other queries the rows_changed will be 0.
        
        * returns: The number of rows changed.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_rows_changed(UnsafePointer(to=self._result)))

    def write_to[W: Writer](self, mut writer: W):
        for col in self._columns:
            writer.write(col, ", ")

    def __str__(self) -> String:
        return String.write(self)

    # ── Column metadata (Python-style) ────────────────────────────

    def columns(self) -> List[String]:
        """Column names, in order (Python ``rel.columns``)."""
        var names = List[String](capacity=len(self._columns))
        for col in self._columns:
            names.append(col.name.copy())
        return names^

    def types(self) -> List[LogicalType[is_owned=True, origin=MutExternalOrigin]]:
        """Column logical types, in order (Python ``rel.dtypes``).

        Alias for `column_types`.
        """
        return self.column_types()

    def description(self) -> List[Column]:
        """Per-column metadata as `Column` structs (index, name, type).

        The Python analog is ``con.description``, a list of 7-tuples; duckdb.mojo
        provides the meaningful subset (name + type). Display size, precision,
        scale and null_ok are not reported.
        """
        return self._columns.copy()

    def __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        """Iterate over rows in this result.

        Example:
            ```mojo
            for row in conn.execute("SELECT name, age FROM users"):
                print(row.get[String](col=0), row.get[Int64](col=1))
            ```
        """
        return RowIter(Pointer(to=self))

    def fetch_chunk(self) raises -> Chunk[is_owned=True]:
        """Fetch the next data chunk from the streaming result.

        Raises when no more chunks remain.  The returned chunk is owned
        and destroyed automatically when it goes out of scope.

        Returns:
            An owned Chunk.
        """
        ref libduckdb = DuckDB().libduckdb()
        var raw: Optional[duckdb_data_chunk] = libduckdb.duckdb_fetch_chunk(
            self._result
        )
        if raw is None:
            raise Error("No more chunks available")
        return Chunk[is_owned=True](raw.value())

    def chunks(ref self) -> ChunkIter[ImmutOrigin(origin_of(self))]:
        """Iterate over data chunks in this result.

        Returns:
            A ``ChunkIter`` that yields owned ``Chunk`` objects.

        Example:
            ```mojo
            for chunk in result.chunks():
                var users = chunk.get[User]()
            ```
        """
        return ChunkIter(Pointer(to=self))

    def rows(ref self) -> RowIter[ImmutOrigin(origin_of(self))]:
        """Iterate over rows — explicit spelling of ``__iter__``.

        Equivalent to ``for row in result``, provided for
        discoverability.

        Returns:
            A ``RowIter`` that yields ``Row`` proxies.

        Example:
            ```mojo
            for row in result.rows():
                var name = row.get[String](col=0)
                var age = row.get[Int64](col=1)
            ```
        """
        return RowIter(Pointer(to=self))

    def fetchall(var self) raises -> MaterializedResult:
        """Fetch all chunks into memory and return a MaterializedResult.

        Consumes this Result (streaming is exhausted).

        Returns:
            A MaterializedResult holding all rows.
        """
        return MaterializedResult(self^)

    def fetchone[
        *Ts: Copyable & Movable
    ](mut self) raises -> Optional[Tuple[*Ts]]:
        """Fetch the next row as an owned tuple, or ``None`` if exhausted.

        Column types are supplied as parameters (Mojo is statically typed,
        unlike Python's untyped ``fetchone``):

            var row = result.fetchone[String, Int64]()
            if row:
                print(row.value()[0], row.value()[1])

        Advances an internal cursor.  ``fetchone``/``fetchmany`` consume the
        same cursor and must not be mixed with ``__iter__``/``rows``/``chunks``/
        ``fetchall`` on the same ``Result``.

        Parameters:
            Ts: The Mojo type of each column, in order.
        """
        while True:
            if self._cur_chunk is None:
                try:
                    self._cur_chunk = self.fetch_chunk()
                    self._cur_row = 0
                except:
                    return None
            if self._cur_row >= len(self._cur_chunk.value()):
                self._cur_chunk = None
                continue
            var t = self._cur_chunk.value().get_tuple[*Ts](row=self._cur_row)
            self._cur_row += 1
            return t^

    def fetchmany[
        *Ts: Copyable & Movable
    ](mut self, size: Int = 1) raises -> List[Tuple[*Ts]]:
        """Fetch up to ``size`` rows as owned tuples, advancing the cursor.

        Returns fewer than ``size`` rows (possibly empty) when the result is
        exhausted.  See `fetchone` for cursor semantics.

        Parameters:
            Ts: The Mojo type of each column, in order.

        Args:
            size: Maximum number of rows to fetch.
        """
        var out = List[Tuple[*Ts]]()
        var n = 0
        while n < size:
            if self._cur_chunk is None:
                try:
                    self._cur_chunk = self.fetch_chunk()
                    self._cur_row = 0
                except:
                    break
            if self._cur_row >= len(self._cur_chunk.value()):
                self._cur_chunk = None
                continue
            out.append(self._cur_chunk.value().get_tuple[*Ts](row=self._cur_row))
            self._cur_row += 1
            n += 1
        return out^

    def show(var self, *, max_rows: Int = 40, max_col_width: Int = 32) raises:
        """Print the result as a formatted table (Python ``rel.show()``).

        Materializes the result, so it consumes ``self``.

        Args:
            max_rows: Maximum number of data rows to display.
            max_col_width: Maximum width of any single column.
        """
        self^.fetchall().show(max_rows=max_rows, max_col_width=max_col_width)

    def __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_result(UnsafePointer(to=self._result))

    def __init__(out self, *, deinit take: Self):
        self._result = take._result^
        self._columns = take._columns^
        self._cur_chunk = take._cur_chunk^
        self._cur_row = take._cur_row


@fieldwise_init
struct ResultError(Writable):
    var message: String
    var type: ErrorType

    var _stack_trace: Optional[StackTrace]
    
    """The stack trace of the error, if collected.

    By default, stack trace is collected for errors created from string
    literals. Stack trace collection can be controlled via the
    `MOJO_ENABLE_STACK_TRACE_ON_ERROR` environment variable.
    """

    # ===-------------------------------------------------------------------===#
    # Life cycle methods
    # ===-------------------------------------------------------------------===#

    @always_inline
    def __init__(
        out self, 
        var value: String, 
        type: ErrorType = ErrorType.INVALID,
        *, 
        depth: Int = -1
    ):
        """Construct a ResultError object with a given String.

        Args:
            value: The error message.
            type: The error type classification (defaults to INVALID).
            depth: The depth of the stack trace to collect. When negative,
                no stack trace is collected.
        """
        self.message = value^
        self.type = type
        self._stack_trace = StackTrace.collect_if_enabled(depth)

    @always_inline
    @implicit
    def __init__(out self, value: StringLiteral):
        """Construct a ResultError object with a given string literal.

        Args:
            value: The error message.
        """
        self.message = String(value)
        self.type = ErrorType.INVALID
        self._stack_trace = StackTrace.collect_if_enabled(0)

    @no_inline
    @implicit
    def __init__(out self, value: Some[Writable]):
        """Construct a ResultError object from a Writable argument.

        Args:
            value: The Writable argument to store in the error message.
        """
        self.message = String(value)
        self.type = ErrorType.INVALID
        self._stack_trace = StackTrace.collect_if_enabled(0)

    @no_inline
    def __init__[*Ts: Writable](out self, *args: *Ts):
        """Construct a ResultError by concatenating a sequence of Writable arguments.

        Args:
            args: A sequence of Writable arguments.

        Parameters:
            Ts: The types of the arguments to format. Each type must satisfy
                `Writable`.
        """
        self = ResultError(String(*args), depth=0)

    # ===-------------------------------------------------------------------===#
    # Trait implementations
    # ===-------------------------------------------------------------------===#

    @no_inline
    def __str__(self) -> String:
        """Converts the ResultError to string representation.

        Returns:
            A String of the error message.
        """
        return String(self.message)

    @no_inline
    def write_to[W: Writer](self, mut writer: W):
        """
        Formats this error to the provided Writer.

        Parameters:
            W: The writer type.

        Args:
            writer: The object to write to.
        """
        self.message.write_to(writer)

    @no_inline
    def __repr__(self) -> String:
        """Converts the ResultError to printable representation.

        Returns:
            A printable representation of the error message.
        """
        return String("ResultError('", self.message, "', error=", self.type, ")")

struct MaterializedResult(Sized, Movable):
    """A result with all rows fetched into memory.
    
    Stores owned chunks that are automatically destroyed when MaterializedResult is destroyed.
    """

    var result: Result
    var chunks: List[UnsafePointer[Chunk[is_owned=True], MutAnyOrigin]]
    var size: Int

    def __init__(out self, var result: Result) raises:
        self.result = result^
        self.chunks = List[UnsafePointer[Chunk[is_owned=True], MutAnyOrigin]]()
        self.size = 0
        while True:
            try:
                var chunk = self.result.fetch_chunk()
                self.size += len(chunk)
                var chunk_ptr = alloc[Chunk[is_owned=True]](1)
                chunk_ptr.init_pointee_move(chunk^)
                self.chunks.append(chunk_ptr)
            except StopIteration:
                break

    def column_count(self) -> Int:
        return self.result.column_count()

    def column_name(self, col: Int) -> String:
        return self.result.column_name(col)

    def column_types(self) -> List[LogicalType[is_owned=True, origin=MutExternalOrigin]]:
        return self.result.column_types()

    def column_type(ref [_]self: Self, col: Int) -> LogicalType[is_owned=False, origin=origin_of(self.result)]:
        return self.result.column_type(col)

    def columns(self) -> List[String]:
        """Column names, in order (Python ``rel.columns``)."""
        return self.result.columns()

    def types(self) -> List[LogicalType[is_owned=True, origin=MutExternalOrigin]]:
        """Column logical types, in order (Python ``rel.dtypes``)."""
        return self.result.column_types()

    def description(self) -> List[Column]:
        """Per-column metadata as `Column` structs (index, name, type)."""
        return self.result.description()

    def __len__(self) -> Int:
        return self.size

    def get[
        T: Copyable & Movable
    ](self, *, col: Int) raises -> List[T]:
        """Get all typed values from a column.

        When T is a plain type, raises if any value is NULL.
        When T is Optional[X], NULL entries become None.

        Parameters:
            T: The Mojo type to deserialize. Use Optional[T] for nullable values.

        Args:
            col: Column index.

        Returns:
            List[T] containing all values.

        Example:
            ```mojo
            var result = con.execute("SELECT * FROM table").fetchall()
            var int_values = result.get[Int64](col=0)
            var nullable = result.get[Optional[String]](col=1)
            ```
        """
        ref libduckdb = DuckDB().libduckdb()
        var result = List[T](
            capacity=len(self.chunks) * Int(libduckdb.duckdb_vector_size())
        )
        for chunk_ptr in self.chunks:
            result.extend(chunk_ptr[].get[T](col=col))
        return result^

    def _locate(self, row: Int) raises -> Tuple[Int, Int]:
        """Map a global row index to a ``(chunk_index, offset_in_chunk)`` pair.

        Walks chunks by their actual sizes — chunks are not assumed to all be
        ``vector_size`` rows (e.g. ``UNION ALL`` can yield small chunks).
        """
        if row < 0 or row >= self.size:
            raise Error("Row index out of bounds")
        var remaining = row
        for i in range(len(self.chunks)):
            var clen = len(self.chunks[i][])
            if remaining < clen:
                return (i, remaining)
            remaining -= clen
        raise Error("Row index out of bounds")

    def get[
        T: Copyable & Movable
    ](self, *, col: Int, row: Int) raises -> T:
        """Get a single typed value.

        When T is a plain type, raises on NULL.
        When T is Optional[X], returns None for NULL.

        Parameters:
            T: The Mojo type to deserialize. Use Optional[T] for nullable values.

        Args:
            col: Column index.
            row: Row index.

        Returns:
            The deserialized value.

        Example:
            ```mojo
            var result = con.execute("SELECT * FROM table").fetchall()
            var value = result.get[Int64](col=0, row=5)
            ```
        """
        var loc = self._locate(row)
        return self.chunks[loc[0]][].get[T](col=col, row=loc[1])

    def get[
        T: Copyable & Movable
    ](self, *, row: Int) raises -> T:
        """Deserialize a table row into a Mojo struct.

        Maps each column to a field in T by position.
        Non-Optional fields raise on NULL; Optional fields become None.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            row: Row index (global, across all chunks).

        Returns:
            The deserialized struct.

        Example:
            ```mojo
            var result = con.execute("SELECT * FROM users").fetchall()
            var user = result.get[User](row=0)
            ```
        """
        var loc = self._locate(row)
        return self.chunks[loc[0]][].get[T](row=loc[1])

    def get[
        T: Copyable & Movable
    ](self) raises -> List[T]:
        """Deserialize all rows into a list of Mojo structs.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Returns:
            List[T] — one struct per row across all chunks.

        Example:
            ```mojo
            var result = con.execute("SELECT * FROM users").fetchall()
            var users = result.get[User]()
            ```
        """
        var result = List[T](capacity=self.size)
        for chunk_ptr in self.chunks:
            result.extend(chunk_ptr[].get[T]())
        return result^

    # ── Pretty printing ───────────────────────────────────────────

    def _cell_str(self, col: Int, row: Int) raises -> String:
        """Stringify a single cell, dispatching on the column's runtime type.

        Best-effort: common scalar types are rendered exactly, NULL as the
        literal ``NULL``, and unsupported/nested types as a ``<type>``
        placeholder (Mojo's typed ``get`` can't render arbitrary runtime types
        generically).
        """
        var tid = self.result._columns[col].type.get_type_id()
        if tid == DuckDBType.boolean:
            var v = self.get[Optional[Bool]](col=col, row=row)
            if not v:
                return String("NULL")
            return String("true") if v.value() else String("false")
        elif tid == DuckDBType.tinyint:
            var v = self.get[Optional[Int8]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.smallint:
            var v = self.get[Optional[Int16]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.integer:
            var v = self.get[Optional[Int32]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.bigint:
            var v = self.get[Optional[Int64]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.utinyint:
            var v = self.get[Optional[UInt8]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.usmallint:
            var v = self.get[Optional[UInt16]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.uinteger:
            var v = self.get[Optional[UInt32]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.ubigint:
            var v = self.get[Optional[UInt64]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.hugeint:
            var v = self.get[Optional[Int128]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.uhugeint:
            var v = self.get[Optional[UInt128]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.float:
            var v = self.get[Optional[Float32]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.double:
            var v = self.get[Optional[Float64]](col=col, row=row)
            return String(v.value()) if v else String("NULL")
        elif tid == DuckDBType.varchar:
            var v = self.get[Optional[String]](col=col, row=row)
            return v.value().copy() if v else String("NULL")
        else:
            return String("<", Self._type_name(tid), ">")

    @staticmethod
    def _type_name(tid: DuckDBType) -> String:
        """The DuckDB-CLI type label for a type id (e.g. INTEGER -> ``int32``)."""
        if tid == DuckDBType.tinyint:
            return String("int8")
        elif tid == DuckDBType.smallint:
            return String("int16")
        elif tid == DuckDBType.integer:
            return String("int32")
        elif tid == DuckDBType.bigint:
            return String("int64")
        elif tid == DuckDBType.hugeint:
            return String("int128")
        elif tid == DuckDBType.utinyint:
            return String("uint8")
        elif tid == DuckDBType.usmallint:
            return String("uint16")
        elif tid == DuckDBType.uinteger:
            return String("uint32")
        elif tid == DuckDBType.ubigint:
            return String("uint64")
        elif tid == DuckDBType.uhugeint:
            return String("uint128")
        else:
            return String(tid)

    @staticmethod
    def _is_right_aligned(tid: DuckDBType) -> Bool:
        """Numeric types are right-aligned, like the DuckDB CLI."""
        return (
            tid == DuckDBType.tinyint
            or tid == DuckDBType.smallint
            or tid == DuckDBType.integer
            or tid == DuckDBType.bigint
            or tid == DuckDBType.hugeint
            or tid == DuckDBType.utinyint
            or tid == DuckDBType.usmallint
            or tid == DuckDBType.uinteger
            or tid == DuckDBType.ubigint
            or tid == DuckDBType.uhugeint
            or tid == DuckDBType.decimal
            or tid == DuckDBType.float
            or tid == DuckDBType.double
        )

    @staticmethod
    def _spaces(n: Int) -> String:
        var s = String("")
        for _ in range(n if n > 0 else 0):
            s += " "
        return s^

    @staticmethod
    def _truncate(value: String, max_width: Int) -> String:
        if value.count_codepoints() <= max_width:
            return value.copy()
        var out = String("")
        var count = 0
        for cp in value.codepoint_slices():
            if count >= max_width - 1:
                break
            out += String(cp)
            count += 1
        out += "…"
        return out^

    @staticmethod
    def _field(content: String, w: Int, right_aligned: Bool) -> String:
        """A value cell: 1 space of padding each side, aligned within ``w``."""
        var extra = w - content.count_codepoints()
        if extra < 0:
            extra = 0
        if right_aligned:
            return String(" ", Self._spaces(extra), content, " ")
        return String(" ", content, Self._spaces(extra), " ")

    @staticmethod
    def _center_field(content: String, w: Int) -> String:
        """A header/separator cell: content centered within ``w`` (extra on right)."""
        var extra = w - content.count_codepoints()
        if extra < 0:
            extra = 0
        var left = extra // 2
        return String(" ", Self._spaces(left), content, Self._spaces(extra - left), " ")

    @staticmethod
    def _center_line(text: String, width: Int) -> String:
        var extra = width - text.count_codepoints()
        if extra <= 0:
            return text.copy()
        return Self._spaces(extra // 2) + text

    @staticmethod
    def _border(
        widths: List[Int], left: String, mid: String, right: String
    ) -> String:
        var line = left.copy()
        var ncols = len(widths)
        for c in range(ncols):
            for _ in range(widths[c] + 2):
                line += "─"
            line += right if c == ncols - 1 else mid
        return line^

    def _render_table(self, *, max_rows: Int, max_col_width: Int) raises -> String:
        var ncols = self.column_count()
        var names = self.result.columns()

        # Per-column type label and alignment.
        var type_names = List[String](capacity=ncols)
        var right = List[Bool](capacity=ncols)
        for c in range(ncols):
            var tid = self.result._columns[c].type.get_type_id()
            type_names.append(Self._type_name(tid))
            right.append(Self._is_right_aligned(tid))

        # Truncation: show everything when the result barely exceeds max_rows
        # (DuckDB CLI rule), else top + bottom halves with a `·` separator.
        var truncated = self.size > max_rows + 3
        var top: Int
        var bottom: Int
        if not truncated:
            top = self.size
            bottom = 0
        else:
            top = max_rows // 2 + (1 if max_rows % 2 != 0 else 0)
            bottom = max_rows - top

        var row_indices = List[Int]()
        for r in range(top):
            row_indices.append(r)
        for r in range(self.size - bottom, self.size):
            row_indices.append(r)

        # Column widths, seeded by header name and type label.
        var widths = List[Int](capacity=ncols)
        for c in range(ncols):
            var w = names[c].count_codepoints()
            var tw = type_names[c].count_codepoints()
            if tw > w:
                w = tw
            widths.append(w if w < max_col_width else max_col_width)

        # Materialize displayed cells, growing widths as needed.
        var cells = List[List[String]]()
        for ri in row_indices:
            var row_cells = List[String]()
            for c in range(ncols):
                var s = Self._truncate(self._cell_str(c, ri), max_col_width)
                var cw = s.count_codepoints()
                if cw > widths[c]:
                    widths[c] = cw
                row_cells.append(s^)
            cells.append(row_cells^)

        var table = String("")
        table += Self._border(widths, "┌", "┬", "┐") + "\n"
        # Header: column names, then type labels (both centered).
        table += "│"
        for c in range(ncols):
            table += Self._center_field(
                Self._truncate(names[c], max_col_width), widths[c]
            ) + "│"
        table += "\n│"
        for c in range(ncols):
            table += Self._center_field(
                Self._truncate(type_names[c], max_col_width), widths[c]
            ) + "│"
        table += "\n" + Self._border(widths, "├", "┼", "┤") + "\n"
        # Data rows, with a 3-row `·` separator between top and bottom halves.
        for i in range(len(cells)):
            if truncated and i == top:
                for _d in range(3):
                    table += "│"
                    for c in range(ncols):
                        table += Self._center_field("·", widths[c]) + "│"
                    table += "\n"
            table += "│"
            for c in range(ncols):
                table += Self._field(cells[i][c], widths[c], right[c]) + "│"
            table += "\n"
        table += Self._border(widths, "└", "┴", "┘")
        if truncated:
            var total_w = ncols + 1  # vertical borders
            for c in range(ncols):
                total_w += widths[c] + 2
            table += "\n" + Self._center_line(String(self.size, " rows"), total_w)
            table += "\n" + Self._center_line(
                String("(", top + bottom, " shown)"), total_w
            )
        return table^

    def show(self, *, max_rows: Int = 40, max_col_width: Int = 32) raises:
        """Print the result as a formatted table, à la the DuckDB CLI.

        The header carries the column name and its type (e.g. ``int32``).
        Numeric columns are right-aligned, others left-aligned.  Large results
        are truncated to the first and last rows with a ``·`` separator and a
        row-count footer.  Rendering is best-effort for common scalar types;
        nested/unsupported types appear as ``<type>`` placeholders.

        Args:
            max_rows: Maximum number of data rows to display before truncating.
            max_col_width: Maximum display width of any single column.
        """
        print(self._render_table(max_rows=max_rows, max_col_width=max_col_width))

    def __del__(deinit self):
        for chunk_ptr in self.chunks:
            chunk_ptr.destroy_pointee()
            chunk_ptr.free()


# ──────────────────────────────────────────────────────────────────
# Chunk-level iterator — streams chunks from a Result
# ──────────────────────────────────────────────────────────────────


@fieldwise_init
struct ChunkIter[
    origin: ImmutOrigin
](ImplicitlyCopyable, Iterable, Iterator):
    """Streams data chunks from a Result.

    Created by ``Result.chunks()``.  Each ``__next__`` call fetches and
    returns the next ``Chunk`` from DuckDB's streaming result.

    Example:
        ```mojo
        for chunk in result.chunks():
            var users = chunk.get[User]()
        ```
    """

    comptime Element = Chunk[is_owned=True]
    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = Self

    var _result: Pointer[Result, Self.origin]

    def __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        return self.copy()

    def __next__(mut self) raises StopIteration -> Chunk[is_owned=True]:
        ref libduckdb = DuckDB().libduckdb()
        var raw: Optional[duckdb_data_chunk] = libduckdb.duckdb_fetch_chunk(
            self._result[]._result
        )
        if raw is None:
            raise StopIteration()
        return Chunk[is_owned=True](raw.value())


# ──────────────────────────────────────────────────────────────────
# Cross-chunk row iterator — streams Row proxies across chunks
# ──────────────────────────────────────────────────────────────────


struct RowIter[
    origin: ImmutOrigin
](ImplicitlyCopyable, Iterable, Iterator):
    """Streams Row proxies from a Result, transparently fetching chunks.

    Returned by ``Result.__iter__`` (i.e. ``for row in result``) and
    by ``Result.rows()``.  Internally fetches chunks one at a time and
    yields one ``Row`` per call to ``__next__``.

    Example:
        ```mojo
        for row in result:
            var name = row.get[String](col=0)
            var age = row.get[Int64](col=1)
        ```
    """

    comptime Element = Row
    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = Self

    var _result: Pointer[Result, Self.origin]
    var _raw_chunk: Optional[duckdb_data_chunk]
    var _num_cols: Int
    var _chunk_row: Int
    var _chunk_size: Int
    var _exhausted: Bool

    def __init__(out self, result_ptr: Pointer[Result, Self.origin]):
        self._result = result_ptr
        self._raw_chunk = None
        self._num_cols = 0
        self._chunk_row = 0
        self._chunk_size = 0
        self._exhausted = False

    def __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        return self.copy()

    def _destroy_current_chunk(mut self):
        """Destroy the current chunk if it exists."""
        if self._raw_chunk is not None:
            ref libduckdb = DuckDB().libduckdb()
            var chunk = self._raw_chunk.value()
            libduckdb.duckdb_destroy_data_chunk(UnsafePointer(to=chunk))
            self._raw_chunk = None

    def __next__(mut self) raises StopIteration -> Row:
        # Advance to next chunk if current one is exhausted
        while self._chunk_row >= self._chunk_size:
            if self._exhausted:
                raise StopIteration()
            self._destroy_current_chunk()
            ref libduckdb = DuckDB().libduckdb()
            var raw: Optional[duckdb_data_chunk] = (
                libduckdb.duckdb_fetch_chunk(self._result[]._result)
            )
            if raw is None:
                self._exhausted = True
                raise StopIteration()
            var chunk = raw.value()
            self._raw_chunk = chunk
            self._num_cols = Int(
                libduckdb.duckdb_data_chunk_get_column_count(chunk)
            )
            self._chunk_row = 0
            self._chunk_size = Int(
                libduckdb.duckdb_data_chunk_get_size(chunk)
            )

        var row = Row(
            self._raw_chunk.value(),
            self._chunk_row,
            self._num_cols,
        )
        self._chunk_row += 1
        return row^
