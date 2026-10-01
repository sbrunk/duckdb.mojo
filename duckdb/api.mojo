from std.ffi import _Global, external_call
from std.os import abort
from duckdb._libduckdb import LibDuckDB, duckdb_ext_api_v1
from duckdb.config import Config
from duckdb.connection import Connection

comptime _DUCKDB_GLOBAL = _Global["DuckDB", _init_duckdb_global]


# ===--------------------------------------------------------------------===#
# Extension API pointer (set before first DuckDB() access in extension mode)
# ===--------------------------------------------------------------------===#


def _kgen_insert_global(name: StringSlice, ptr: Pointer):
    external_call["KGEN_CompilerRT_InsertGlobal", NoneType](
        name, ptr.unsafe_bitcast[NoneType](),
    )


def _kgen_get_global(name: StringSlice) -> Pointer[NoneType, MutUntrackedOrigin]:
    return external_call[
        "KGEN_CompilerRT_GetGlobalOrNull",
        Pointer[NoneType, MutUntrackedOrigin],
    ](name.unsafe_ptr(), name.byte_length())


def _set_ext_api_ptr(ptr: Pointer[duckdb_ext_api_v1, ImmUntrackedOrigin]):
    """Store the extension API pointer for later use by
    _init_duckdb_global.

    Must be called before the first DuckDB() access so that LibDuckDB is
    constructed from the API struct instead of via dlopen/dlsym.
    """
    _kgen_insert_global("DuckDB_ExtApiPtr", ptr)


def _get_ext_api_ptr() -> Optional[
    Pointer[duckdb_ext_api_v1, ImmUntrackedOrigin]
]:
    """Retrieve the previously stored extension API pointer, or `None`."""
    var raw = _kgen_get_global("DuckDB_ExtApiPtr")
    var addr = Int(raw)
    if addr == 0:
        return None
    return Pointer[duckdb_ext_api_v1, ImmUntrackedOrigin](
        unsafe_from_address=addr
    )


# ===--------------------------------------------------------------------===#
# Global singleton
# ===--------------------------------------------------------------------===#

def _init_duckdb_global() -> _DuckDBGlobal:
    var api_ptr = _get_ext_api_ptr()
    if api_ptr is not None:
        return _DuckDBGlobal(api_ptr.value())
    return _DuckDBGlobal()


struct _DuckDBGlobal(Defaultable, Movable):
    var libduckdb: LibDuckDB

    def __init__(out self):
        """Standalone mode: load all functions via dlopen/dlsym."""
        self.libduckdb = LibDuckDB()

    def __init__(
        out self, api: Pointer[duckdb_ext_api_v1, ImmUntrackedOrigin]
    ):
        """Extension mode: construct LibDuckDB from the extension API struct."""
        self.libduckdb = LibDuckDB(api)


def _get_duckdb_interface() raises -> Pointer[LibDuckDB, ImmStaticOrigin]:
    """Returns an immutable static pointer to the LibDuckDB global.

    The returned pointer is immutable to prevent invalid shared mutation of
    this global variable. Once it is initialized, it may not be mutated.
    """

    var ptr = _DUCKDB_GLOBAL.get_or_create_ptr()
    var ptr2 = Pointer(to=ptr[].libduckdb).as_imm().unsafe_origin_cast[ImmStaticOrigin
    ]()
    return Pointer(to=ptr2[])


struct DuckDB(ImplicitlyCopyable):
    var _impl: Pointer[LibDuckDB, ImmStaticOrigin]

    def __init__(out self):
        try:
            self._impl = _get_duckdb_interface()
        except e:
            abort(String("Failed to load libduckdb", e))

    @always_inline
    def libduckdb(self) -> ref [ImmStaticOrigin] LibDuckDB:
        return self._impl[]

    @staticmethod
    def connect(db_path: String) raises -> Connection:
        return Connection(db_path)

    @staticmethod
    def connect(db_path: String, config: Config) raises -> Connection:
        """Open a connection with startup configuration.

        Args:
            db_path: Database path (e.g. ``:memory:``).
            config: Startup configuration (borrowed; DuckDB copies it internally).
        """
        return Connection(db_path, config)

    @staticmethod
    def connect(
        db_path: String, *, config: Dict[String, String]
    ) raises -> Connection:
        """Open a connection with configuration from a dictionary.

        Args:
            db_path: Database path (e.g. ``":memory:"``)..
            config: Dictionary mapping option names to values.
        """
        var cfg = Config(config)
        return Connection(db_path, cfg)

    @staticmethod
    def connect(
        db_path: String, *, read_only: Bool
    ) raises -> Connection:
        """Open a connection, optionally in read-only mode.

        Args:
            db_path: Database path (e.g. ``":memory:"`` or a file path).
            read_only: If True, open the database with ``access_mode=READ_ONLY``.
        """
        return Connection(db_path, read_only=read_only)


# ===--------------------------------------------------------------------===#
# Default (process-wide) connection, like Python's ``:default:`` connection
# ===--------------------------------------------------------------------===#

comptime _DEFAULT_CONN_GLOBAL = _Global["DuckDBDefaultConn", _init_default_conn]


def _init_default_conn() -> _DefaultConnGlobal:
    return _DefaultConnGlobal()


struct _DefaultConnGlobal(Defaultable, Movable):
    var conn: Connection

    def __init__(out self):
        try:
            self.conn = Connection(":memory:")
        except e:
            abort(String("Failed to create default DuckDB connection: ", e))


def _get_default_connection() raises -> Pointer[
    Connection, ImmStaticOrigin
]:
    """Return a static pointer to the lazily-created default connection.

    The default connection is a single, process-wide in-memory connection,
    created on first use and living until process exit (its destructor is never
    run, same as the ``LibDuckDB`` global). It is not safe to use from
    several threads at once. Open an explicit `connect()` for that.
    """
    var ptr = _DEFAULT_CONN_GLOBAL.get_or_create_ptr()
    var conn_ptr = Pointer(to=ptr[].conn).as_imm().unsafe_origin_cast[
        ImmStaticOrigin
    ]()
    return Pointer(to=conn_ptr[])
