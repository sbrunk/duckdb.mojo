# duckdb.mojo

Mojo Bindings for DuckDB

Status:
- Early proof of concept, many parts of the API are still missing (feel free to submit PRs).
- Only working for macOS for now but we'll add support for Linux soon.

## Example

```mojo
from duckdb import *

var con = DuckDB.connect(":memory:")

_ = con.execute(
    """
CREATE TABLE train_services AS
FROM 's3://duckdb-blobs/train_services.parquet';
"""
)

var result = con.execute(
    """
-- Get the top-3 busiest train stations
SELECT station_name, count(*) AS num_services
FROM train_services
GROUP BY ALL
ORDER BY num_services DESC
LIMIT 3;
"""
)

for i in range(result.column_count()):
    print(result.column_name(i), end=" ")
print()

for chunk in result.chunk_iterator():
    for i in range(len(chunk)):
        print(
            chunk.get_string(col=0, row=i).value(),
            " ",
            chunk.get_int64(col=1, row=i).value(),
        )
```

## Installation

1. Download the [DuckDB C/C++ library](https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-osx-universal.zip) from the [installation](https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=macos) page.
2. Extract `libduckdb.dylib` to the project directory.
3. Set library path:
```shell
export DYLD_FALLBACK_LIBRARY_PATH=$(realpath .)
```
4. Run
``` shell
mojo example.mojo
```