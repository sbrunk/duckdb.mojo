"""`Database` and `Connection` on the V2 API."""

from std.memory import ArcPointer
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.v2._runtime import api_error, check, environment, lib
from duckdb.v2.chunk import _read_schema
from duckdb.v2.result import Result, _columns
from duckdb.v2.relation import Relation, _reader_call
from duckdb._sql_util import _quote_qualified
from duckdb.v2.statement import Description, Params, PreparedStatement, _Arguments, _named, _positional
from duckdb.v2.error import DuckDBError
from duckdb.v2.functions import RegistrationTarget, Registrar


struct _Instance(Movable):
    """Owns the V2 instance handle; shared by a `Database` and its connections."""

    var handle: duckdb_v2_instance_handle

    def __init__(out self, handle: duckdb_v2_instance_handle):
        self.handle = handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_instance_destroy(Pointer(to=self.handle))


struct Database(Copyable, Movable):
    """A DuckDB database: one instance with one database attached as its default.

    Copies share the same database, and so do the connections made from it.
    It is closed when the last copy and the last connection are gone.

    ```mojo
    var db = Database()                                  # in-memory
    var file = Database("my.db", config={"threads": "4"})
    var con = db.connect()
    ```

    Opening a file that is already open in this process raises
    (`ErrorCode.RESOURCE_IN_USE`): every database of the process shares one
    instance cache.
    """

    var _instance: ArcPointer[_Instance]

    def __init__(
        out self,
        path: String = ":memory:",
        *,
        read_only: Bool = False,
        config: Dict[String, String] = {},
    ) raises DuckDBError:
        """Open (or create) a database.

        Args:
            path: A file path, or `:memory:` for an in-memory database.
            read_only: Open the database file read-only.
            config: Startup options, e.g. `{"threads": "4"}`, as DuckDB's `SET` text.
        """
        var err = null_error_info()
        var names = List[StrArg]()
        var values = List[StrArg]()
        for entry in config.items():
            names.append(StrArg(entry.key))
            values.append(StrArg(entry.value))
        var name_views = List[duckdb_v2_str]()
        var value_views = List[duckdb_v2_str]()
        for i in range(len(names)):
            name_views.append(names[i].ptr()[].copy())
            value_views.append(values[i].ptr()[].copy())
        var handle = null[duckdb_v2_instance_handle]()
        check(
            lib().duckdb_v2_instance_create_with_options(
                environment(),
                name_views.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin](),
                value_views.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin](),
                idx_t(len(name_views)),
                Pointer(to=handle),
                Pointer(to=err),
            ),
            err,
        )
        # Mojo frees values after their last use, which for the view arrays was
        # `unsafe_ptr()` above: keep them, and the strings they point into,
        # alive until the call has returned.
        _ = name_views^
        _ = value_views^
        _ = names^
        _ = values^
        self._instance = ArcPointer(_Instance(handle))

        var options = null[duckdb_v2_attach_options_handle]()
        if read_only:
            check(lib().duckdb_v2_attach_options_create(handle, Pointer(to=options), Pointer(to=err)), err)
            var key = StrArg("READ_ONLY")
            var setting = StrArg("true")
            try:
                check(lib().duckdb_v2_attach_options_set(options, key.ptr(), setting.ptr(), Pointer(to=err)), err)
            except e:
                _ = lib().duckdb_v2_attach_options_destroy(Pointer(to=options))
                raise e^
        var name = StrArg("")  # derived from the path
        var path_arg = StrArg(path)
        var code = lib().duckdb_v2_instance_attach(handle, name.ptr(), path_arg.ptr(), options, True, Pointer(to=err))
        _ = lib().duckdb_v2_attach_options_destroy(Pointer(to=options))
        check(code, err)

    def connect(self) raises DuckDBError -> Connection:
        """A new connection to this database."""
        return Connection(self)

    def factory(self) raises DuckDBError -> duckdb_v2_factory_handle:
        """The instance's factory (built-in types and values only; borrowed)."""
        var f = null[duckdb_v2_factory_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_instance_get_factory(self._instance[].handle, Pointer(to=f), Pointer(to=err)), err)
        return f


struct Connection(Movable, Registrar):
    """A session on a `Database`, with its own settings and transaction.

    It keeps its database alive. Use one connection per thread.
    """

    var _db: Database
    var _handle: duckdb_v2_connection_handle

    def __init__(out self, db: Database) raises DuckDBError:
        self._db = db.copy()
        self._handle = null[duckdb_v2_connection_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_connection_create(db._instance[].handle, Pointer(to=self._handle), Pointer(to=err)), err)

    def __init__(out self, *, deinit move: Self):
        self._db = move._db^
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_connection_destroy(Pointer(to=self._handle))

    def handle(self) -> duckdb_v2_connection_handle:
        """The underlying V2 handle (borrowed; valid while `self` lives)."""
        return self._handle

    def database(self) -> Database:
        return self._db.copy()

    def factory(self) raises DuckDBError -> duckdb_v2_factory_handle:
        """The connection's factory: resolves catalog types in its transaction (borrowed)."""
        var f = null[duckdb_v2_factory_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_connection_get_factory(self._handle, Pointer(to=f), Pointer(to=err)), err)
        return f

    def _register_scalar_handle(self, function: duckdb_v2_scalar_function_handle) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_connection_register_scalar_function(self._handle, function, Pointer(to=err)), err)

    def _register_aggregate_handle(self, function: duckdb_v2_aggregate_function_handle) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_connection_register_aggregate_function(self._handle, function, Pointer(to=err)), err)

    def _register_table_handle(self, function: duckdb_v2_table_function_handle) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_connection_register_table_function(self._handle, function, Pointer(to=err)), err)

    def _target(self) -> RegistrationTarget:
        return RegistrationTarget(self._handle, null[duckdb_v2_extension_handle]())

    def _run_sql(self, sql: String) raises DuckDBError:
        _ = self.execute(sql)

    def interrupt(self) raises DuckDBError:
        """Interrupt the statement running on this connection (safe from another thread)."""
        var err = null_error_info()
        check(lib().duckdb_v2_connection_interrupt(self._handle, Pointer(to=err)), err)

    # ── Execution ─────────────────────────────────────────────────

    def execute[*Ts: Movable & Deinitable](self, sql: StringSlice, *args: *Ts) raises DuckDBError -> Result:
        """Run SQL, binding positional parameters (`?` or `$1`, `$2`, ...).

        ```mojo
        _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
        _ = con.execute("INSERT INTO t VALUES (?, ?)", Int32(1), String("Mark"))
        var rows = con.execute("SELECT * FROM t WHERE id = ?", Int32(1)).fetchall()
        ```

        `sql` may hold several statements separated by `;`. All but the last
        run to completion; the result is the last one's. Parameters need a
        single statement.
        """
        var arguments = _positional(self.factory(), *args)
        return self._execute(sql, arguments)

    def execute(self, sql: StringSlice, *, var params: Params) raises DuckDBError -> Result:
        """Run SQL, binding named parameters (`$name`)."""
        var arguments = _named(params^)
        return self._execute(sql, arguments)

    def execute[T: AnyType](self, sql: StringSlice, *, params: T) raises DuckDBError -> Result:
        """Run SQL, binding named parameters (`$name`) from the fields of a struct.

        ```mojo
        @fieldwise_init
        struct Filter(Copyable, Movable):
            var min_age: Int32
            var city: String

        var r = con.execute("SELECT * FROM users WHERE age >= $min_age AND city = $city", params=Filter(30, "Amsterdam"))
        ```
        """
        var arguments = _named(Params.of(params))
        return self._execute(sql, arguments)

    def _execute(self, sql: StringSlice, ref arguments: _Arguments) raises DuckDBError -> Result:
        var statements = _Statements(self._handle, sql)
        var current = statements.next()
        if Int(current) == 0:
            raise api_error("no statement to execute")
        while True:
            var result: Result
            try:
                result = Result(_owned=self._execute_statement(current, arguments))
            finally:
                _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=current))
            # Statements without rows have already run (see `Result`). A query
            # cannot change the grammar, so the next statement can be parsed
            # before the query's rows are read.
            var following = statements.next()
            if Int(following) == 0:
                return result^
            if arguments.count() > 0:
                _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=following))
                raise api_error("parameters need a single statement, but the SQL holds several")
            result._finish()
            current = following

    def _execute_statement(
        self, stmt: duckdb_v2_sql_statement_handle, ref arguments: _Arguments
    ) raises DuckDBError -> duckdb_v2_result_handle:
        var result = null[duckdb_v2_result_handle]()
        var err = null_error_info()
        check(
            lib().duckdb_v2_statement_execute(
                self._handle,
                stmt,
                arguments.names_ptr(),
                arguments.values_ptr(),
                arguments.count(),
                Pointer(to=result),
                Pointer(to=err),
            ),
            err,
        )
        return result

    def prepare(self, sql: StringSlice) raises DuckDBError -> PreparedStatement:
        """Plan a single statement once, to execute it many times."""
        var statements = _Statements(self._handle, sql)
        var stmt = statements.next()
        if Int(stmt) == 0:
            raise api_error("no statement to prepare")
        var prepared = null[duckdb_v2_prepared_statement_handle]()
        var err = null_error_info()
        var code = lib().duckdb_v2_prepared_statement_create(self._handle, stmt, False, Pointer(to=prepared), Pointer(to=err))
        _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=stmt))
        check(code, err)
        var extra = statements.next()
        if Int(extra) != 0:
            _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=extra))
            _ = lib().duckdb_v2_prepared_statement_destroy(Pointer(to=prepared))
            raise api_error("prepare takes a single statement, but the SQL holds several")
        return PreparedStatement(_owned=prepared, factory=self.factory())

    def describe(self, sql: StringSlice) raises DuckDBError -> Description:
        """The result columns and parameter types of a statement, without running it."""
        var statements = _Statements(self._handle, sql)
        var stmt = statements.next()
        if Int(stmt) == 0:
            raise api_error("no statement to describe")
        var out_schema = null[duckdb_v2_schema_handle]()
        var param_schema = null[duckdb_v2_schema_handle]()
        var err = null_error_info()
        var code = lib().duckdb_v2_statement_bind(self._handle, stmt, Pointer(to=out_schema), Pointer(to=param_schema), Pointer(to=err))
        _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=stmt))
        check(code, err)
        try:
            var columns = _columns(ArcPointer(_read_schema(out_schema)))
            var parameters = _columns(ArcPointer(_read_schema(param_schema)))
            return Description(columns^, parameters^)
        finally:
            _ = lib().duckdb_v2_schema_destroy(Pointer(to=out_schema))
            _ = lib().duckdb_v2_schema_destroy(Pointer(to=param_schema))

    # ── Relations ─────────────────────────────────────────────────

    def sql(self, query: String) -> Relation[ImmOrigin(origin_of(self))]:
        """A lazy relation over `query`; nothing runs until a terminal method."""
        return Relation[ImmOrigin(origin_of(self))](Pointer(to=self), query)

    def table(self, name: String) -> Relation[ImmOrigin(origin_of(self))]:
        """A relation over a table or view."""
        return self.sql(String("SELECT * FROM ", _quote_qualified(name)))

    def read_csv(self, path: String, options: Dict[String, String] = {}) -> Relation[ImmOrigin(origin_of(self))]:
        """A CSV file as a relation. Option values are verbatim SQL (quote strings with `lit`)."""
        return self.sql(String("SELECT * FROM ", _reader_call("read_csv", path, options)))

    def read_parquet(self, path: String, options: Dict[String, String] = {}) -> Relation[ImmOrigin(origin_of(self))]:
        return self.sql(String("SELECT * FROM ", _reader_call("read_parquet", path, options)))

    def read_json(self, path: String, options: Dict[String, String] = {}) -> Relation[ImmOrigin(origin_of(self))]:
        return self.sql(String("SELECT * FROM ", _reader_call("read_json", path, options)))

    # ── Transactions ──────────────────────────────────────────────

    def begin(self) raises DuckDBError:
        _ = self.execute("BEGIN TRANSACTION")

    def commit(self) raises DuckDBError:
        _ = self.execute("COMMIT")

    def rollback(self) raises DuckDBError:
        _ = self.execute("ROLLBACK")


struct _Statements(Movable):
    """Parses SQL one statement at a time (owns the V2 statement iterator).

    Parsing is lazy, so statements that change the grammar (`LOAD`) take
    effect for the statements after them.
    """

    var _handle: duckdb_v2_statement_iterator_handle

    def __init__(out self, conn: duckdb_v2_connection_handle, sql: StringSlice) raises DuckDBError:
        self._handle = null[duckdb_v2_statement_iterator_handle]()
        var text = String(sql)  # parse_sql needs a NUL-terminated string and copies it
        var err = null_error_info()
        var code = lib().duckdb_v2_parse_sql(conn, text.as_c_string_span().ptr(), Pointer(to=self._handle), Pointer(to=err))
        _ = text^
        check(code, err)

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_statement_iterator_destroy(Pointer(to=self._handle))

    def next(mut self) raises DuckDBError -> duckdb_v2_sql_statement_handle:
        """The next statement (owned by the caller), or NULL at the end."""
        var stmt = null[duckdb_v2_sql_statement_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_statement_iterator_next(self._handle, Pointer(to=stmt), Pointer(to=err)), err)
        return stmt
