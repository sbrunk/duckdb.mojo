"""Module-level convenience functions mirroring DuckDB's Python top-level API.

These run against a single, lazily-created, process-wide in-memory default
connection (like Python's ``duckdb.sql(...)`` / ``duckdb.execute(...)``), or
return a fresh `Connection` from `connect`.

```mojo
import duckdb

var con = duckdb.connect()                  # in-memory
var con2 = duckdb.connect("my.db", read_only=True)
duckdb.sql("SELECT 42").show()              # default connection
```
"""

from std.collections import Dict
from duckdb.api import _get_default_connection
from duckdb.api_level import ApiLevel
from duckdb.config import Config
from duckdb.connection import Connection
from duckdb.result import Result, ResultError


def connect(
    database: String = ":memory:", *, read_only: Bool = False
) raises -> Connection[ApiLevel.CLIENT]:
    """Open a new connection to ``database`` (default in-memory).

    Args:
        database: Database path (``":memory:"`` or a file path).
        read_only: Open with ``access_mode=READ_ONLY`` when True.
    """
    if read_only:
        return Connection(database, read_only=True)
    return Connection(database)


def connect(
    database: String, var config: Config, *, read_only: Bool = False
) raises -> Connection[ApiLevel.CLIENT]:
    """Open a new connection with an explicit `Config`."""
    if read_only:
        config.set("access_mode", "READ_ONLY")
    return Connection(database, config)


def connect(
    database: String, *, config: Dict[String, String], read_only: Bool = False
) raises -> Connection[ApiLevel.CLIENT]:
    """Open a new connection with configuration from a dictionary."""
    var cfg = Config(config)
    if read_only:
        cfg.set("access_mode", "READ_ONLY")
    return Connection(database, cfg)


def sql(query: String) raises ResultError -> Result:
    """Run ``query`` against the default connection and return a `Result`."""
    return _get_default_connection()[].execute(query)


def execute(query: String) raises ResultError -> Result:
    """Run ``query`` against the default connection and return a `Result`."""
    return _get_default_connection()[].execute(query)


def read_csv(path: String) raises ResultError -> Result:
    """Read a CSV file via the default connection."""
    return _get_default_connection()[].read_csv(path)


def read_parquet(path: String) raises ResultError -> Result:
    """Read a Parquet file via the default connection."""
    return _get_default_connection()[].read_parquet(path)


def read_json(path: String) raises ResultError -> Result:
    """Read a JSON file via the default connection."""
    return _get_default_connection()[].read_json(path)
