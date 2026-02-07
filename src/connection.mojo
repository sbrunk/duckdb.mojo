from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.database import Database


struct Connection(Movable):
    """A connection to a DuckDB database.

    Example:
    ```mojo
    from duckdb import DuckDB
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT lst, lst || 'duckdb' FROM range(10) tbl(lst)")
    ```
    """

    var database: Database
    var _conn: duckdb_connection

    fn __init__(out self, path: String) raises:
        ref libduckdb = DuckDB().libduckdb()
        self.database = Database(path)
        self._conn = UnsafePointer[
            duckdb_connection.type, ImmutExternalOrigin
        ]()
        if (
            libduckdb.duckdb_connect(
                self.database._db, UnsafePointer(to=self._conn)
            )
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_disconnect(UnsafePointer(to=self._conn))

    fn execute(self, query: String) raises -> Result:
        var result = duckdb_result()
        var result_ptr = UnsafePointer(to=result)
        var _query = query.copy()
        ref libduckdb = DuckDB().libduckdb()
        var state =libduckdb.duckdb_query(self._conn, _query.as_c_string_slice().unsafe_ptr(), result_ptr)
        if state == DuckDBError:
            var error = String(unsafe_from_utf8_ptr=libduckdb.duckdb_result_error(result_ptr))
            libduckdb.duckdb_destroy_result(result_ptr)
            raise Error(error)
        return Result(result)
