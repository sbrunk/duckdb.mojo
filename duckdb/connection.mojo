from std.collections import List, Dict
from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.api_level import ApiLevel
from duckdb.config import Config
from duckdb.database import Database
from duckdb.result import Result, ResultError, ResultType, ErrorType
from duckdb.prepared_statement import PreparedStatement
from duckdb.value import DuckDBValue
from duckdb._sql_util import _sql_quote


struct Connection[api_level: ApiLevel = ApiLevel.CLIENT](Movable):
    """A connection to a DuckDB database.

    Connection borrows the Database handle during construction — it does **not**
    take ownership. The caller is responsible for keeping the Database alive for
    the lifetime of all its connections (same contract as the C API).

    The convenience constructor ``Connection(path)`` (and ``DuckDB.connect()``)
    creates *and owns* an internal Database so the connection is self-contained.

    The ``api_level`` parameter controls compile-time access to unstable API
    functions.  The default (``ApiLevel.CLIENT``) gives full access.  When
    running as an extension, ``Extension.run`` creates a connection with the
    appropriate level (``EXT_STABLE`` or ``EXT_UNSTABLE``).

    Parameters:
        api_level: The API surface available at compile time.  Defaults to
            ``ApiLevel.CLIENT`` (full access).

    Example:
    ```mojo
    from duckdb import DuckDB, Config
    # Self-contained (owns its own database):
    var con = DuckDB.connect(":memory:")

    # With startup config:
    var config = Config()
    config.set("threads", "2")
    var con2 = DuckDB.connect(":memory:", config^)

    # Shared database, multiple connections:
    var db = Database(":memory:")
    var con3 = Connection(db)
    var con4 = Connection(db)
    ```
    """

    var _db: Database
    var _conn: duckdb_connection

    def __init__(out self, path: String) raises:
        """Create a connection with a new database."""
        self._db = Database(path)
        # Placeholder handle — duckdb_connect populates it via out-param.
        self._conn = UnsafePointer[
            duckdb_connection.type, MutExternalOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    def __init__(out self, path: String, config: Config) raises:
        """Create a connection with a new database and startup configuration.

        Args:
            path: Database path (e.g. ``":memory:"`` or a file path).
            config: Startup configuration.
        """
        self._db = Database(path, config)
        # Placeholder handle — duckdb_connect populates it via out-param.
        self._conn = UnsafePointer[
            duckdb_connection.type, MutExternalOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    def __init__(out self, path: String, *, read_only: Bool) raises:
        """Create a connection, optionally in read-only mode.

        Args:
            path: Database path (e.g. ``":memory:"`` or a file path).
            read_only: If True, set ``access_mode=READ_ONLY`` on startup.
        """
        if read_only:
            var cfg = Config()
            cfg.set("access_mode", "READ_ONLY")
            self = Connection[Self.api_level](path, cfg)
        else:
            self = Connection[Self.api_level](path)

    def __init__(out self, db: Database) raises:
        """Create a connection from an existing database.

        Args:
            db: An existing database handle.
        """
        self._db = Database(_handle=db._db)
        # Placeholder handle — duckdb_connect populates it via out-param.
        self._conn = UnsafePointer[
            duckdb_connection.type, MutExternalOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    def __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_disconnect(UnsafePointer(to=self._conn))

    def execute(self, query: String) raises ResultError -> Result:
        var result = duckdb_result()
        var result_ptr = UnsafePointer(to=result)
        var _query = query.copy()
        ref libduckdb = DuckDB().libduckdb()
        var state = libduckdb.duckdb_query(self._conn, _query.as_c_string_slice().unsafe_ptr(), result_ptr)
        if state == DuckDBError:
            var error_msg = String(unsafe_from_utf8_ptr=libduckdb.duckdb_result_error(result_ptr))
            var error_type_value = libduckdb.duckdb_result_error_type(result_ptr)
            libduckdb.duckdb_destroy_result(result_ptr)
            raise ResultError(error_msg, ErrorType(error_type_value))
        return Result(result)

    def sql(self, query: String) raises ResultError -> Result:
        """Run ``query`` and return a `Result`.

        Alias for `execute`.  DuckDB's Python client distinguishes ``sql``
        (lazy relation) from ``execute``; duckdb.mojo has no Relation API, so
        the two are equivalent here.
        """
        return self.execute(query)

    # ── Prepared statements / parameter binding ───────────────────

    def prepare(self, query: String) raises ResultError -> PreparedStatement:
        """Prepare ``query`` for repeated execution with bound parameters."""
        return PreparedStatement(self._conn, query)

    def execute[
        *Ts: Copyable & Movable
    ](self, query: String, *args: *Ts) raises ResultError -> Result:
        """Execute ``query`` with positional parameters (``?`` or ``$1``).

        Parameters are bound left-to-right starting at index 1.  Plain Mojo
        scalars are accepted directly; `Optional[T]` binds NULL for `None`.

        Parameters:
            Ts: The types of the positional arguments.

        Args:
            query: The SQL query with ``?``/``$N`` placeholders.
            args: The values to bind, in order.

        Example:
            ```mojo
            var r = con.execute("SELECT ? + ?", Int32(40), Int32(2))
            ```
        """
        var stmt = PreparedStatement(self._conn, query)
        comptime T = Tuple[*Ts]
        comptime n = T.__len__()
        comptime for idx in range(n):
            stmt.bind(idx + 1, args[idx])
        return stmt.execute()

    def execute(
        self, query: String, params: List[DuckDBValue]
    ) raises ResultError -> Result:
        """Execute ``query`` binding the given pre-built positional values."""
        var stmt = PreparedStatement(self._conn, query)
        for i in range(len(params)):
            stmt.bind_value(i + 1, params[i])
        return stmt.execute()

    def execute_named[
        T: Copyable & Movable & ImplicitlyDestructible
    ](self, query: String, params: Dict[String, T]) raises ResultError -> Result:
        """Execute ``query`` binding named parameters (``$name``).

        All values must share the type ``T``.  For heterogeneous named
        parameters, use `prepare` and bind manually.

        Parameters:
            T: The (shared) type of all parameter values.

        Args:
            query: The SQL query with ``$name`` placeholders.
            params: Mapping of parameter name to value.
        """
        var stmt = PreparedStatement(self._conn, query)
        for entry in params.items():
            var idx = stmt.parameter_index(entry.key)
            stmt.bind(idx, entry.value)
        return stmt.execute()

    def executemany[
        *Ts: Copyable & Movable
    ](self, query: String, rows: List[Tuple[*Ts]]) raises ResultError:
        """Execute ``query`` once per row of positional parameters.

        Prepares the statement a single time and re-binds for each row.  Useful
        for bulk INSERTs.

        Parameters:
            Ts: The types of the per-row tuple elements.

        Args:
            query: The SQL query with ``?``/``$N`` placeholders.
            rows: One parameter tuple per execution.
        """
        var stmt = PreparedStatement(self._conn, query)
        comptime T = Tuple[*Ts]
        comptime n = T.__len__()
        for i in range(len(rows)):
            stmt.clear_bindings()
            ref row = rows[i]
            comptime for idx in range(n):
                stmt.bind(idx + 1, row[idx])
            _ = stmt.execute()

    # ── File readers ──────────────────────────────────────────────

    def read_csv(self, path: String) raises ResultError -> Result:
        """Read a CSV file and return the rows as a `Result`.

        Equivalent to ``SELECT * FROM read_csv('path')``.  Only the path is
        configurable for now; reader options (``header``, ``delim``, ...) would
        be added as keyword arguments appended inside ``read_csv(...)``.
        """
        return self.execute(
            String("SELECT * FROM read_csv(", _sql_quote(path), ")")
        )

    def read_parquet(self, path: String) raises ResultError -> Result:
        """Read a Parquet file and return the rows as a `Result`.

        Equivalent to ``SELECT * FROM read_parquet('path')``.
        """
        return self.execute(
            String("SELECT * FROM read_parquet(", _sql_quote(path), ")")
        )

    def read_json(self, path: String) raises ResultError -> Result:
        """Read a JSON file and return the rows as a `Result`.

        Equivalent to ``SELECT * FROM read_json('path')``.
        """
        return self.execute(
            String("SELECT * FROM read_json(", _sql_quote(path), ")")
        )
