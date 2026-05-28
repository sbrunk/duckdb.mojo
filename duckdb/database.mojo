from duckdb._libduckdb import *
from duckdb.api import DuckDB

struct Database(Movable):
    var _db: duckdb_database
    var _is_owned: Bool

    def __init__(out self, path: Optional[String] = None) raises:
        ref libduckdb = DuckDB().libduckdb()
        # NULL handle — duckdb_open_ext populates it via out-param. If
        # construction raises after this, __del__'s duckdb_close becomes a
        # safe no-op on NULL.
        self._db = UnsafePointer[duckdb_database.type, MutExternalOrigin](
            unsafe_from_address=0
        )
        self._is_owned = True
        var db_addr = UnsafePointer(to=self._db)
        var resolved_path = path.value() if path else ":memory:"
        var path_ptr = resolved_path.as_c_string_slice().unsafe_ptr()
        var out_error = alloc[UnsafePointer[c_char, MutAnyOrigin]](1)
        # config=NULL signals "use default config" to DuckDB.
        if (
            libduckdb.duckdb_open_ext(path_ptr, db_addr, config=duckdb_config(unsafe_from_address=0), out_error=out_error)
        ) == DuckDBError:
            var error_ptr = out_error[]
            var error_msg = String(unsafe_from_utf8_ptr=error_ptr)
            # the String constructor copies the data so this is safe
            libduckdb.duckdb_free(error_ptr.bitcast[NoneType]())
            raise Error(error_msg)

    def __init__(out self, path: Optional[String], config: Config) raises:
        """Create a database with startup configuration options.

        Args:
            path: Database file path, or None / ":memory:" for in-memory.
            config: Startup configuration.
        """
        ref libduckdb = DuckDB().libduckdb()
        # NULL handle — duckdb_open_ext populates it via out-param.
        self._db = UnsafePointer[duckdb_database.type, MutExternalOrigin](
            unsafe_from_address=0
        )
        self._is_owned = True
        var db_addr = UnsafePointer(to=self._db)
        var resolved_path = path.value() if path else ":memory:"
        var path_ptr = resolved_path.as_c_string_slice().unsafe_ptr()
        var out_error = alloc[UnsafePointer[c_char, MutAnyOrigin]](1)
        if (
            libduckdb.duckdb_open_ext(path_ptr, db_addr, config=config._handle(), out_error=out_error)
        ) == DuckDBError:
            var error_ptr = out_error[]
            var error_msg = String(unsafe_from_utf8_ptr=error_ptr)
            libduckdb.duckdb_free(error_ptr.bitcast[NoneType]())
            raise Error(error_msg)

    def __init__(out self, *, _handle: duckdb_database):
        """Wrap an existing database handle without taking ownership.

        The caller retains ownership — the handle will not be closed
        when this Database is destroyed.

        Args:
            _handle: An existing database handle (not owned).
        """
        self._db = _handle
        self._is_owned = False

    def __del__(deinit self):
        if not self._is_owned:
            return
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_close(UnsafePointer(to=self._db))