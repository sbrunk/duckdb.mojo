"""Module-level functions on a process-wide default connection, like
Python's `duckdb.sql(...)`.

```mojo
from duckdb.v2 import default

default.sql("SELECT 42 AS answer").show()
var n = default.execute("SELECT count(*) FROM range(10)").get[Int64]()[0]
var con = default.connect("my.db", read_only=True)
```

The default connection is an in-memory database, created on first use and
kept until the process ends. Use it from one thread at a time; open your own
connection with `connect()` for anything else.
"""

from std.ffi import _Global
from std.os import abort
from duckdb.v2.database import Connection, Database
from duckdb.v2.error import DuckDBError
from duckdb.v2.relation import Relation
from duckdb.v2.result import Result
from duckdb.v2.statement import Params


comptime _DEFAULT = _Global["DuckDBV2DefaultConnection", _init_default]


def _init_default() -> _Default:
    return _Default()


struct _Default(Defaultable, Movable):
    var conn: Connection

    def __init__(out self):
        try:
            self.conn = Database().connect()
        except e:
            abort(String("Failed to create the default DuckDB connection: ", e))


def default_connection() -> ref[ImmStaticOrigin] Connection:
    """The process-wide default connection (in-memory, created on first use)."""
    try:
        return _DEFAULT.get_or_create_ptr()[].conn
    except e:
        abort(String(e))


def connect(
    path: String = ":memory:", *, read_only: Bool = False, config: Dict[String, String] = {}
) raises DuckDBError -> Connection:
    """A connection to a new database (or a file already opened in this process)."""
    return Database(path, read_only=read_only, config=config).connect()


def execute[*Ts: Movable & Deinitable](sql: StringSlice, *args: *Ts) raises DuckDBError -> Result:
    """Run SQL on the default connection (see `Connection.execute`)."""
    return default_connection().execute(sql, *args)


def execute(sql: StringSlice, *, var params: Params) raises DuckDBError -> Result:
    return default_connection().execute(sql, params=params^)


def sql(query: String) -> Relation[ImmStaticOrigin]:
    """A lazy relation over `query` on the default connection."""
    return default_connection().sql(query)


def table(name: String) -> Relation[ImmStaticOrigin]:
    return default_connection().table(name)


def read_csv(path: String, options: Dict[String, String] = {}) -> Relation[ImmStaticOrigin]:
    return default_connection().read_csv(path, options)


def read_parquet(path: String, options: Dict[String, String] = {}) -> Relation[ImmStaticOrigin]:
    return default_connection().read_parquet(path, options)


def read_json(path: String, options: Dict[String, String] = {}) -> Relation[ImmStaticOrigin]:
    return default_connection().read_json(path, options)
