# duckdb.mojo

[Mojo](https://www.modular.com/mojo) bindings for [DuckDB](https://duckdb.org/).

Status:
- Work in progress, many parts of the API are still missing (PRs welcome).

## Example

```mojo
from duckdb import *

var con = DuckDB.connect(":memory:")

_ = con.execute("""
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

CREATE TABLE train_services AS
FROM 's3://duckdb-blobs/train_services.parquet';
"""
)

var result = con.execute("""
-- Get the top-3 busiest train stations
SELECT station_name, count(*) AS num_services
FROM train_services
GROUP BY ALL
ORDER BY num_services DESC
LIMIT 3;
"""
).fetch_all()

for col in result.columns():
    print(col[])

print()

for row in range(len(result)):
    print(
        result.get(varchar, col=0, row=row).value(),
        " ",
        result.get(bigint, col=1, row=row).value(),
    )
```

## Installation

Currently, you'll need to checkout the source. We'll publish a Conda package soon to make it easier to use from another Mojo project.

1. [Install the Magic package manager for Mojo](https://docs.modular.com/mojo/manual/get-started#1-install-mojo).
2. Checkout this repo
3. Run `magic shell`
4. Run `mojo example.mojo`

### Run Tests

```shell
magic run test
```