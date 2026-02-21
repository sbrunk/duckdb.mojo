from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.api_level import ApiLevel
from duckdb.database import Database
from duckdb.result import ResultError, ResultType, ErrorType


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
    from duckdb import DuckDB
    # Self-contained (owns its own database):
    var con = DuckDB.connect(":memory:")

    # Shared database, multiple connections:
    var db = Database(":memory:")
    var con1 = Connection(db)
    var con2 = Connection(db)
    ```
    """

    var _db: Database
    var _conn: duckdb_connection

    fn __init__(out self, path: String) raises:
        """Create a connection with a new database."""
        self._db = Database(path)
        self._conn = UnsafePointer[
            duckdb_connection.type, MutExternalOrigin
        ]()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __init__(out self, db: Database) raises:
        """Create a connection from an existing database.

        Args:
            db: An existing database handle.
        """
        self._db = Database(_handle=db._db)
        self._conn = UnsafePointer[
            duckdb_connection.type, MutExternalOrigin
        ]()
        ref libduckdb = DuckDB().libduckdb()
        if (
            libduckdb.duckdb_connect(self._db._db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_disconnect(UnsafePointer(to=self._conn))

    fn execute(self, query: String) raises ResultError -> Result:
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
