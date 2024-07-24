# duckdb.mojo

[Mojo](https://www.modular.com/mojo) bindings for [DuckDB](https://duckdb.org/).

Status:
- Work in progress, many parts of the API are still missing (PRs welcome).

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
            chunk.get(string, col=0, row=i).value(),
            " ",
            chunk.get(int64, col=1, row=i).value(),
        )
```

## Installation

1. [Install Mojo](https://docs.modular.com/mojo/manual/get-started#1-install-mojo). Currently nightly >= `2024.7.1105` is required, so install or update the nightly version: `modular install nightly/mojo`
2. Download the DuckDB C/C++ library from the [installation](https://duckdb.org/docs/installation/?version=stable&environment=cplusplus) page.
3. Extract `libduckdb.so` (Linux) or `libduckdb.dylib` (macOS) to the project directory.
4. Set library path:
```shell
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(realpath .) # Linux
export DYLD_FALLBACK_LIBRARY_PATH=$(realpath .) # macOS
```
5. Run
``` shell
mojo example.mojo
```

### Run Tests

```shell
mojo test -I .
```