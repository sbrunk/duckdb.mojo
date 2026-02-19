# duckdb.mojo

[Mojo](https://www.modular.com/mojo) bindings for [DuckDB](https://duckdb.org/).

## 10 minute presentation at the MAX & Mojo community meeting

<div align="center">
  <a href="https://www.youtube.com/watch?v=6huytcgQgk8&t=788"><img src="https://img.youtube.com/vi/6huytcgQgk8/0.jpg" alt="10 minute DuckDB.mojo presentation at the MAX & Mojo community meeting"></a>
</div>

Status:
- Work in progress, many parts of the API are still missing (PRs welcome).

## Example

```mojo
from duckdb import *

def main():
    var con = DuckDB.connect(":memory:")

    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """
    )

    var result = con.execute(
        """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name", count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """
    ).fetch_all()

    for col in result.columns():
        print(col)

    print()
    print("Length: " + String(len(result)))
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

1. [Install Pixi](https://pixi.sh/latest/installation/).
2. Checkout this repo
3. Run `pixi shell`
4. Run `mojo example.mojo`

### Run Tests

```shell
pixi run test
```

### Build a conda package

```shell
pixi build
```

## Scalar Functions

Register Mojo functions as DuckDB scalar functions (UDFs) that operate on table
columns. There are several convenience levels:

### Stdlib math functions (zero boilerplate)

Pass Mojo stdlib math functions directly — types and SIMD vectorization are
handled automatically:

```mojo
import math
from duckdb import *
from duckdb.scalar_function import ScalarFunction

var conn = DuckDB.connect(":memory:")

# Register stdlib math functions as SQL scalar functions — one line each
ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, math.sqrt](conn)
ScalarFunction.from_simd_function["mojo_sin",  DType.float64, math.sin](conn)
ScalarFunction.from_simd_function["mojo_cos",  DType.float64, math.cos](conn)
ScalarFunction.from_simd_function["mojo_exp",  DType.float64, math.exp](conn)
ScalarFunction.from_simd_function["mojo_log",  DType.float64, math.log](conn)

# Binary stdlib functions work too
ScalarFunction.from_simd_function["mojo_atan2", DType.float64, math.atan2](conn)

# Now use them in SQL
var result = conn.execute("SELECT mojo_sqrt(x), mojo_sin(x) FROM my_table")
```

### Custom SIMD functions

Write your own SIMD-vectorized kernels for fused computations:

```mojo
fn sin_plus_cos[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return math.sin(x) + math.cos(x)

# Register — processes data in hardware-optimal SIMD batches automatically
ScalarFunction.from_simd_function[
    "mojo_sin_plus_cos", DType.float64, DType.float64, sin_plus_cos
](conn)
```

### Row-at-a-time functions

For simple per-row logic without manual SIMD:

```mojo
fn add_one(x: Int32) -> Int32:
    return x + 1

ScalarFunction.from_function["add_one", DType.int32, DType.int32, add_one](conn)
```

### Math Benchmark

A benchmark comparing Mojo SIMD scalar functions against DuckDB builtins is
available in `benchmark/math_benchmark.mojo`. It covers unary functions
(sqrt, sin, cos, exp, log, abs), fused computations (sin+cos, hypot, Gaussian),
and binary functions (hypot, atan2). Change the `F` constant to switch between
`DType.float32` and `DType.float64`.

```shell
pixi run mojo run benchmark/math_benchmark.mojo
```