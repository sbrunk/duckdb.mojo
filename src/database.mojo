from duckdb._libduckdb import *
from duckdb.api import DuckDB

struct Database(Movable):
    var _db: duckdb_database

    fn __init__(out self, path: Optional[String] = None) raises:
        ref libduckdb = DuckDB().libduckdb()
        self._db = UnsafePointer[duckdb_database.type, MutExternalOrigin]()
        var db_addr = UnsafePointer(to=self._db)
        var resolved_path = path.value() if path else ":memory:"
        var path_ptr = resolved_path.as_c_string_slice().unsafe_ptr()
        var out_error = alloc[UnsafePointer[c_char, MutAnyOrigin]](1)
        if (
            libduckdb.duckdb_open_ext(path_ptr, db_addr, config=duckdb_config(), out_error=out_error)
        ) == DuckDBError:
            var error_ptr = out_error[]
            var error_msg = String(error_ptr)
            # the String constructor copies the data so this is safe
            libduckdb.duckdb_free(error_ptr.bitcast[NoneType]())
            raise Error(error_msg)

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_close(UnsafePointer(to=self._db))