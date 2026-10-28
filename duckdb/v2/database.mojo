"""`Database` and `Connection` on the V2 API."""

from std.memory import ArcPointer
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.v2._runtime import api_error, check, environment, lib
from duckdb.v2.chunk import Chunk, _Schema
from duckdb.v2.error import DuckDBError
from duckdb.v2.types import LogicalType


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


struct Connection(Movable):
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

    def interrupt(self) raises DuckDBError:
        """Interrupt the statement running on this connection (safe from another thread)."""
        var err = null_error_info()
        check(lib().duckdb_v2_connection_interrupt(self._handle, Pointer(to=err)), err)

    # ── Execution (minimal; results come in the next phase) ───────

    def _execute_one(mut self, sql: StringSlice) raises DuckDBError -> duckdb_v2_result_handle:
        """Parse and execute the first statement of `sql`; the caller owns the result."""
        var err = null_error_info()
        var it = null[duckdb_v2_statement_iterator_handle]()
        var text = String(sql)
        check(lib().duckdb_v2_parse_sql(self._handle, text.as_c_string_span().ptr(), Pointer(to=it), Pointer(to=err)), err)
        _ = text^  # parse_sql copies the text
        var stmt = null[duckdb_v2_sql_statement_handle]()
        var result = null[duckdb_v2_result_handle]()
        try:
            check(lib().duckdb_v2_statement_iterator_next(it, Pointer(to=stmt), Pointer(to=err)), err)
            if Int(stmt) == 0:
                raise api_error("no statement to execute")
            check(
                lib().duckdb_v2_statement_execute(
                    self._handle,
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
            _ = lib().duckdb_v2_sql_statement_destroy(Pointer(to=stmt))
            _ = lib().duckdb_v2_statement_iterator_destroy(Pointer(to=it))
        return result

    def _run(mut self, sql: StringSlice) raises DuckDBError:
        """Execute one statement to completion, discarding rows."""
        var result = self._execute_one(sql)
        var err = null_error_info()
        var changed: idx_t = 0
        var code = lib().duckdb_v2_result_drain(result, Pointer(to=changed), Pointer(to=err))
        _ = lib().duckdb_v2_result_destroy(Pointer(to=result))
        check(code, err)

    def _query_chunks(mut self, sql: StringSlice) raises DuckDBError -> List[Chunk]:
        """Execute one statement and collect all of its chunks.

        Chunks are owned by the caller and stay valid after the result is gone.
        """
        var result = self._execute_one(sql)
        var err = null_error_info()
        var chunks = List[Chunk]()
        try:
            var schema = null[duckdb_v2_schema_handle]()
            check(lib().duckdb_v2_result_get_schema(result, Pointer(to=schema), Pointer(to=err)), err)
            var types = List[LogicalType]()
            var names = List[String]()
            try:
                var n: idx_t = 0
                check(lib().duckdb_v2_schema_get_count(schema, Pointer(to=n), Pointer(to=err)), err)
                for i in range(Int(n)):
                    var name = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
                    var t = null[duckdb_v2_logical_type_handle]()  # borrowed from the schema
                    check(lib().duckdb_v2_schema_get_field(schema, idx_t(i), Pointer(to=name), Pointer(to=t), Pointer(to=err)), err)
                    var owned = null[duckdb_v2_logical_type_handle]()
                    check(lib().duckdb_v2_logical_type_copy(t, Pointer(to=owned), Pointer(to=err)), err)
                    types.append(LogicalType(_owned=owned))
                    names.append(str_to_string(name))
            finally:
                _ = lib().duckdb_v2_schema_destroy(Pointer(to=schema))
            var shared = ArcPointer(_Schema(names^, types^))
            while True:
                var chunk = null[duckdb_v2_data_chunk_handle]()
                check(lib().duckdb_v2_result_fetch_chunk(result, Pointer(to=chunk), Pointer(to=err)), err)
                if Int(chunk) == 0:
                    break
                chunks.append(Chunk(_owned=chunk, schema=shared))
        finally:
            _ = lib().duckdb_v2_result_destroy(Pointer(to=result))
        return chunks^

