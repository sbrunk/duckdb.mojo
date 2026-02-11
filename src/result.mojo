from duckdb._libduckdb import *
from duckdb.chunk import Chunk, _ChunkIter
from collections import Optional
from std.builtin.error import StackTrace


@fieldwise_init
struct ResultType(
    Comparable,
    ImplicitlyCopyable,
    Stringable,
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
    fn __eq__(self, other: Self) -> Bool:
        """Returns True if this result type equals the other result type.

        Args:
            other: The result type to compare with.

        Returns:
            Bool: True if the result types are equal, False otherwise.
        """
        return self._value == other._value

    fn __lt__(self, other: Self) -> Bool:
        """Returns True if this result type is less than the other result type.

        Args:
            other: The result type to compare with.

        Returns:
            Bool: True if this result type is less than the other result type,
                False otherwise.
        """
        return self._value < other._value

    fn write_to[W: Writer](self, mut writer: W):
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
    fn __str__(self) -> String:
        """Returns the string representation of this result type.

        Returns:
            String: A human-readable string representation of the result type
                (e.g., "INVALID", "QUERY_RESULT").
        """
        return String.write(self)

    @no_inline
    fn __repr__(self) -> String:
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
    Stringable,
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
    fn __eq__(self, other: Self) -> Bool:
        """Returns True if this error type equals the other error type.

        Args:
            other: The error type to compare with.

        Returns:
            Bool: True if the error types are equal, False otherwise.
        """
        return self._value == other._value

    fn __lt__(self, other: Self) -> Bool:
        """Returns True if this error type is less than the other error type.

        Args:
            other: The error type to compare with.

        Returns:
            Bool: True if this error type is less than the other error type,
                False otherwise.
        """
        return self._value < other._value

    fn write_to[W: Writer](self, mut writer: W):
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
    fn __str__(self) -> String:
        """Returns the string representation of this error type.

        Returns:
            String: A human-readable string representation of the error type
                (e.g., "PARSER", "CONSTRAINT").
        """
        return String.write(self)

    @no_inline
    fn __repr__(self) -> String:
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
    Stringable,
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
    fn __eq__(self, other: Self) -> Bool:
        """Returns True if this statement type equals the other statement type.

        Args:
            other: The statement type to compare with.

        Returns:
            Bool: True if the statement types are equal, False otherwise.
        """
        return self._value == other._value

    fn __lt__(self, other: Self) -> Bool:
        """Returns True if this statement type is less than the other statement type.

        Args:
            other: The statement type to compare with.

        Returns:
            Bool: True if this statement type is less than the other statement type,
                False otherwise.
        """
        return self._value < other._value

    fn write_to[W: Writer](self, mut writer: W):
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
    fn __str__(self) -> String:
        """Returns the string representation of this statement type.

        Returns:
            String: A human-readable string representation of the statement type
                (e.g., "SELECT", "INSERT").
        """
        return String.write(self)

    @no_inline
    fn __repr__(self) -> String:
        """Returns the detailed string representation of this statement type.

        Returns:
            String: A string representation including the type name and value
                (e.g., "StatementType.SELECT").
        """
        return String("StatementType.", self)


@fieldwise_init
struct Column(Movable & Copyable & Stringable & Writable):
    var index: Int
    var name: String
    var type: LogicalType

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(
            "Column(", self.index, ", ", self.name, ": ", self.type, ")"
        )

    fn __str__(self) -> String:
        return String(self.type)



struct Result(Writable, Stringable):
    var _result: duckdb_result
    var columns: List[Column]

    fn __init__(out self, result: duckdb_result):
        self._result = result
        self.columns = List[Column]()
        for i in range(self.column_count()):
            var col = Column(
                index=i, name=self.column_name(i), type=self.column_type(i)
            )
            self.columns.append(col^)

    fn column_count(self) -> Int:
        ref libduckdb = DuckDB().libduckdb()
        return Int(
            libduckdb.duckdb_column_count(UnsafePointer(to=self._result))
        )

    fn column_name(self, col: Int) -> String:
        ref libduckdb = DuckDB().libduckdb()
        var c_str = libduckdb.duckdb_column_name(
            UnsafePointer(to=self._result), UInt64(col)
        )
        return String(unsafe_from_utf8_ptr=c_str)

    fn column_types(self) -> List[LogicalType]:
        var types = List[LogicalType]()
        for i in range(self.column_count()):
            types.append(self.column_type(i))
        return types^

    fn column_type(self, col: Int) -> LogicalType:
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType(
            libduckdb.duckdb_column_logical_type(
                UnsafePointer(to=self._result), UInt64(col)
            )
        )

    fn statement_type(self) -> StatementType:
        """Returns the statement type of the statement that was executed.
        
        Returns:
            StatementType: The type of statement that was executed.
        """
        ref libduckdb = DuckDB().libduckdb()
        return StatementType(libduckdb.duckdb_result_statement_type(self._result))

    fn rows_changed(self) -> Int:
        """Returns the number of rows changed by the query stored in the result.
        
        This is relevant only for INSERT/UPDATE/DELETE queries. For other queries the rows_changed will be 0.
        
        * returns: The number of rows changed.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_rows_changed(UnsafePointer(to=self._result)))

    fn write_to[W: Writer](self, mut writer: W):
        for col in self.columns:
            writer.write(col, ", ")

    fn __str__(self) -> String:
        return String.write(self)

    # fn __iter__(self) -> ResultIterator:
    #     return ResultIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        ref libduckdb = DuckDB().libduckdb()
        return Chunk(libduckdb.duckdb_fetch_chunk(self._result))

    fn chunk_iterator(self) raises -> _ChunkIter[origin_of(self)]:
        return _ChunkIter(self)

    fn fetch_all(var self) raises -> MaterializedResult:
        return MaterializedResult(self^)

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_result(UnsafePointer(to=self._result))

    fn __moveinit__(out self, deinit existing: Self):
        self._result = existing._result^
        self.columns = existing.columns^


@fieldwise_init
struct ResultError(Stringable, Writable):
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
    fn __init__(
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
    fn __init__(out self, value: StringLiteral):
        """Construct a ResultError object with a given string literal.

        Args:
            value: The error message.
        """
        self.message = String(value)
        self.type = ErrorType.INVALID
        self._stack_trace = StackTrace.collect_if_enabled(0)

    @no_inline
    @implicit
    fn __init__(out self, value: Some[Writable]):
        """Construct a ResultError object from a Writable argument.

        Args:
            value: The Writable argument to store in the error message.
        """
        self.message = String(value)
        self.type = ErrorType.INVALID
        self._stack_trace = StackTrace.collect_if_enabled(0)

    @no_inline
    fn __init__[*Ts: Writable](out self, *args: *Ts):
        """Construct a ResultError by concatenating a sequence of Writable arguments.

        Args:
            args: A sequence of Writable arguments.

        Parameters:
            Ts: The types of the arguments to format. Each type must satisfy
                `Writable`.
        """
        self = ResultError(String(args), depth=0)

    # ===-------------------------------------------------------------------===#
    # Trait implementations
    # ===-------------------------------------------------------------------===#

    @no_inline
    fn __str__(self) -> String:
        """Converts the ResultError to string representation.

        Returns:
            A String of the error message.
        """
        return String(self.message)

    @no_inline
    fn write_to[W: Writer](self, mut writer: W):
        """
        Formats this error to the provided Writer.

        Parameters:
            W: The writer type.

        Args:
            writer: The object to write to.
        """
        self.message.write_to(writer)

    @no_inline
    fn __repr__(self) -> String:
        """Converts the ResultError to printable representation.

        Returns:
            A printable representation of the error message.
        """
        return String("ResultError('", self.message, "', error=", self.type, ")")

struct MaterializedResult(Sized, Movable):
    """A result with all rows fetched into memory."""

    var result: Result
    var chunks: List[UnsafePointer[Chunk, MutAnyOrigin]]
    var size: Int

    fn __init__(out self, var result: Result) raises:
        self.result = result^
        self.chunks = List[UnsafePointer[Chunk, MutAnyOrigin]]()
        self.size = 0
        var iter = self.result.chunk_iterator()
        while iter.__has_next__():
            var chunk = iter.__next__()
            self.size += len(chunk)
            var chunk_ptr = alloc[Chunk](1)
            chunk_ptr.init_pointee_move(chunk^)
            self.chunks.append(chunk_ptr)

    fn column_count(self) -> Int:
        return self.result.column_count()

    fn column_name(self, col: Int) -> String:
        return self.result.column_name(col)

    fn column_types(self) -> List[LogicalType]:
        return self.result.column_types()

    fn column_type(self, col: Int) -> LogicalType:
        return self.result.column_type(col)

    fn columns(self) -> List[Column]:
        return self.result.columns.copy()

    fn __len__(self) -> Int:
        return self.size

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], col: Int) raises -> List[Optional[T]]:
        ref libduckdb = DuckDB().libduckdb()
        var result = List[Optional[T]](
            capacity=len(self.chunks) * Int(libduckdb.duckdb_vector_size())
        )
        for chunk_ptr in self.chunks:
            result.extend(chunk_ptr[].get(type, col))
        return result^

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], col: Int, row: Int) raises -> Optional[T]:
        ref libduckdb = DuckDB().libduckdb()
        if row < 0 or row >= self.size:
            raise Error("Row index out of bounds")
        var chunk_idx = Int(UInt64(row) // libduckdb.duckdb_vector_size())
        var chunk_offset = Int(UInt64(row) % libduckdb.duckdb_vector_size())
        return self.chunks[chunk_idx][].get(type, col=col, row=chunk_offset)

    fn __del__(deinit self):
        for chunk_ptr in self.chunks:
            chunk_ptr.destroy_pointee()
            chunk_ptr.free()
