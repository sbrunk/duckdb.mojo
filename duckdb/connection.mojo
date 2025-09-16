from duckdb._libduckdb import *


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

    fn __init__(out self, db_path: String) raises:
        self._db = UnsafePointer[duckdb_database.type]()
        var db_addr = UnsafePointer(to=self._db)
        var path = db_path.copy()
        if (
            duckdb_open(path.unsafe_cstr_ptr(), db_addr)
        ) == DuckDBError:
            raise Error(
                "Could not open database"
            )  ## TODO use duckdb_open_ext and return error message
        self.__conn = UnsafePointer[duckdb_connection.type]()
        if (
            duckdb_connect(
                self._db, UnsafePointer(to=self.__conn)
            )
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(owned self):
        duckdb_disconnect(UnsafePointer(to=self.__conn))
        duckdb_close(UnsafePointer(to=self._db))

    fn execute(self, query: String) raises -> Result:
        var result = duckdb_result()
        var result_ptr = UnsafePointer(to=result)
        var _query = query.copy()
        if (
            duckdb_query(
                self.__conn, _query.unsafe_cstr_ptr(), result_ptr
            )
            == DuckDBError
        ):
            raise Error(duckdb_result_error(result_ptr))
        return Result(result)
