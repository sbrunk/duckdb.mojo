from std.collections import List, Dict
from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.api_level import ApiLevel
from duckdb.config import Config
from duckdb.database import Database
from duckdb.result import Result, ResultError, ResultType, ErrorType
from duckdb.prepared_statement import PreparedStatement
from duckdb.value import DuckDBValue
from duckdb.relation import Relation
from duckdb._sql_util import _sql_quote, _quote_ident, _quote_literal, _quote_qualified


def _reader_call(
    fn_name: String, path: String, options: Dict[String, String]
) -> String:
    """Build a ``read_*('path', key=value, ...)`` table-function call.

    The path is quoted as a literal. Option values are inserted verbatim.
    """
    var out = String(fn_name, "(", _quote_literal(path))
    for entry in options.items():
        out += String(", ", entry.key, "=", entry.value)
    out += ")"
    return out^


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
        self._conn = Pointer[
            duckdb_connection.type, MutUntrackedOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, Pointer(to=self._conn))
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
        self._conn = Pointer[
            duckdb_connection.type, MutUntrackedOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, Pointer(to=self._conn))
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
        self._conn = Pointer[
            duckdb_connection.type, MutUntrackedOrigin
        ].unsafe_dangling()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, Pointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    def __deinit__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_disconnect(Pointer(to=self._conn))

    def execute(self, query: String) raises ResultError -> Result:
        var result = duckdb_result()
        var result_ptr = Pointer(to=result)
        var _query = query.copy()
        ref libduckdb = DuckDB().libduckdb()
        var state = libduckdb.duckdb_query(self._conn, _query.as_c_string_slice().unsafe_ptr(), result_ptr)
        if state == DuckDBError:
            var error_msg = String(unsafe_from_utf8_ptr=libduckdb.duckdb_result_error(result_ptr))
            var error_type_value = libduckdb.duckdb_result_error_type(result_ptr)
            libduckdb.duckdb_destroy_result(result_ptr)
            raise ResultError(error_msg, ErrorType(error_type_value))
        return Result(result)

    def sql(ref self, query: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Build a lazy `Relation` from ``query``.

        Unlike `execute` (which runs immediately and returns a `Result`), `sql`
        returns a composable relation that executes only at a terminal
        (`fetchall`/`show`/`get`/...):

        ```mojo
        con.sql("FROM t").filter("x > 0").order("x").show()
        ```

        The relation borrows this connection so we need to keep the connection alive while
        the relation is in use.
        """
        return Relation[ImmOrigin(origin_of(self._conn))](Pointer(to=self._conn), query)

    def query(ref self, query: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Alias for `sql` (Python ``con.query``)."""
        return Relation[ImmOrigin(origin_of(self._conn))](Pointer(to=self._conn), query)

    def from_query(ref self, query: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Alias for `sql` (Python ``con.from_query``)."""
        return Relation[ImmOrigin(origin_of(self._conn))](Pointer(to=self._conn), query)

    def table(ref self, name: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """A relation over an existing table (Python ``con.table``)."""
        return Relation[ImmOrigin(origin_of(self._conn))](
            Pointer(to=self._conn), String("SELECT * FROM ", _quote_qualified(name))
        )

    def view(ref self, name: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """A relation over an existing view (Python ``con.view``)."""
        return Relation[ImmOrigin(origin_of(self._conn))](
            Pointer(to=self._conn), String("SELECT * FROM ", _quote_qualified(name))
        )

    # ── Transactions ──────────────────────────────────────────────

    def begin(self) raises:
        """Begin a transaction (Python ``con.begin``)."""
        _ = self.execute("BEGIN TRANSACTION")

    def commit(self) raises:
        """Commit the current transaction (Python ``con.commit``)."""
        _ = self.execute("COMMIT")

    def rollback(self) raises:
        """Roll back the current transaction (Python ``con.rollback``)."""
        _ = self.execute("ROLLBACK")

    def checkpoint(self) raises:
        """Flush the WAL to disk (Python ``con.checkpoint``)."""
        _ = self.execute("CHECKPOINT")

    # ── Lifecycle ─────────────────────────────────────────────────

    def close(mut self):
        """Disconnect now instead of waiting for destruction.

        Idempotent: ``duckdb_disconnect`` nulls the handle, so the destructor's
        later disconnect is a safe no-op. Using the connection after `close` is
        an error (queries will fail).
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_disconnect(Pointer(to=self._conn))

    def cursor(self) raises -> Connection[Self.api_level]:
        """Open a second connection to the same database.

        The returned connection shares this connection's database and must not
        outlive it.
        """
        return Connection[Self.api_level](self._db)

    def duplicate(self) raises -> Connection[Self.api_level]:
        """Alias for `cursor` (Python ``duplicate``)."""
        return Connection[Self.api_level](self._db)

    def __enter__(var self) -> Self:
        """Enter a ``with`` block; the connection is destroyed (disconnected)
        when the block exits."""
        return self^

    # ── Extensions ────────────────────────────────────────────────

    def install_extension(self, name: String, *, force: Bool = False) raises:
        """Install an extension by name (or path/URL) (Python ``install_extension``).

        DuckDB exposes no C API for this, so it runs ``INSTALL`` / ``FORCE
        INSTALL``. The name is passed as a quoted literal.
        """
        var verb = String("FORCE INSTALL ") if force else String("INSTALL ")
        _ = self.execute(String(verb, _quote_literal(name)))

    def load_extension(self, name: String) raises:
        """Load an installed extension (Python ``load_extension``)."""
        _ = self.execute(String("LOAD ", _quote_literal(name)))

    def remove_function(self, name: String) raises:
        """Drop a user-defined function by name (Python ``remove_function``).

        Register scalar/aggregate/table functions with
        ``ScalarFunction.from_function[...]().register(con)`` and friends.
        """
        _ = self.execute(String("DROP FUNCTION IF EXISTS ", _quote_ident(name)))

    # ── Introspection / control ───────────────────────────────────

    def interrupt(self):
        """Interrupt the currently running query (Python ``interrupt``)."""
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_interrupt(self._conn)

    def query_progress(self) -> Float64:
        """Progress of the running query as a percentage in ``[0, 100]``.

        Returns a negative value when no query is running or progress is
        unavailable.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_query_progress(self._conn).percentage

    # ── Prepared statements / parameter binding ───────────────────

    def prepare(self, query: String) raises ResultError -> PreparedStatement:
        """Prepare ``query`` for repeated execution with bound parameters."""
        return PreparedStatement(self._conn, query)

    def execute[
        *Ts: Copyable & Movable & Deinitable
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
        T: Copyable & Movable & Deinitable
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
        *Ts: Copyable & Movable & Deinitable
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

    def read_csv(ref self, path: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a CSV file as a lazy `Relation`.

        ``con.read_csv('f.csv').filter(...).show()`` composes like any relation
        and executes only at a terminal.
        """
        return self.sql(String("SELECT * FROM ", _reader_call("read_csv", path, Dict[String, String]())))

    def read_csv(
        ref self, path: String, options: Dict[String, String]
    ) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a CSV file with reader options appended as ``key=value``.

        Option *values* are inserted verbatim, so quote string values with
        `lit` (e.g. ``{"header": "true", "delim": lit(",")}``).
        """
        return self.sql(String("SELECT * FROM ", _reader_call("read_csv", path, options)))

    def read_parquet(ref self, path: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a Parquet file as a lazy `Relation` (Python ``con.read_parquet``)."""
        return self.sql(String("SELECT * FROM ", _reader_call("read_parquet", path, Dict[String, String]())))

    def read_parquet(
        ref self, path: String, options: Dict[String, String]
    ) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a Parquet file with reader options appended as ``key=value``."""
        return self.sql(String("SELECT * FROM ", _reader_call("read_parquet", path, options)))

    def read_json(ref self, path: String) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a JSON file as a lazy `Relation` (Python ``con.read_json``)."""
        return self.sql(String("SELECT * FROM ", _reader_call("read_json", path, Dict[String, String]())))

    def read_json(
        ref self, path: String, options: Dict[String, String]
    ) -> Relation[ImmOrigin(origin_of(self._conn))]:
        """Read a JSON file with reader options appended as ``key=value``."""
        return self.sql(String("SELECT * FROM ", _reader_call("read_json", path, options)))
