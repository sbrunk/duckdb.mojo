from duckdb._c_api.c_api import *
from duckdb._c_api.libduckdb import _impl


# TODO separate opening and connecting but add convenient functions to keep it simple
struct Connection:
    """A connection to a DuckDB database.

    Example:
    ```mojo
    from duckdb import DuckDB
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT lst, lst || 'duckdb' FROM range(10) tbl(lst)")
    ```
    """

    var _db: duckdb_database
    var __conn: duckdb_connection

    fn __init__(inout self, db_path: String) raises:
        self._db = UnsafePointer[duckdb_database.type]()
        var db_addr = UnsafePointer.address_of(self._db)
        if (
            _impl().duckdb_open(db_path.unsafe_cstr_ptr(), db_addr)
        ) == DuckDBError:
            raise Error(
                "Could not open database"
            )  ## TODO use duckdb_open_ext and return error message
        self.__conn = UnsafePointer[duckdb_connection.type]()
        if (
            _impl().duckdb_connect(
                self._db, UnsafePointer.address_of(self.__conn)
            )
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(owned self):
        _impl().duckdb_disconnect(UnsafePointer.address_of(self.__conn))
        _impl().duckdb_close(UnsafePointer.address_of(self._db))

    fn execute(self, query: String) raises -> Result:
        var result = duckdb_result()
        var result_ptr = UnsafePointer.address_of(result)
        if (
            _impl().duckdb_query(
                self.__conn, query.unsafe_cstr_ptr(), result_ptr
            )
            == DuckDBError
        ):
            raise Error(_impl().duckdb_result_error(result_ptr))
        return Result(result)
