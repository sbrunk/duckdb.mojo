# duckdb.mojo

[Mojo](https://www.modular.com/mojo) bindings for [DuckDB](https://duckdb.org/).

duckdb.mojo can be used in two ways:

1. **Client API** — Query DuckDB from Mojo, register scalar/aggregate/table functions (UDFs), and process results with SIMD vectorization.
2. **Extension development** *(experimental)* — Build DuckDB [extensions](https://duckdb.org/docs/stable/extensions/overview) written in Mojo that can be loaded with `LOAD`. See the [demo extension](demo-extension/README.md) for a working example.

## 10 minute presentation at the MAX & Mojo community meeting

<div align="center">
  <a href="https://www.youtube.com/watch?v=6huytcgQgk8&t=788"><img src="https://img.youtube.com/vi/6huytcgQgk8/0.jpg" alt="10 minute DuckDB.mojo presentation at the MAX & Mojo community meeting"></a>
</div>

## Examples

### Client API

```mojo
from duckdb import *

def main():
    var con = DuckDB.connect(":memory:")

    _ = con.execute("""
    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """
    )

    var result = con.execute("""
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

    print("\nLength:", len(result), "\n")

    for row in range(len(result)):
        print(
            result.get(varchar, col=0, row=row).value(),
            " ",
            result.get(bigint, col=1, row=row).value(),
        )
```

### Extension

Build DuckDB extensions as shared libraries in Mojo. Write an init function
that receives a `Connection` and registers your functions, then pass it to
`Extension.run`:

```mojo
from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction

fn add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

fn init(conn: Connection) raises:
    ScalarFunction.from_function[
        "mojo_add_numbers", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)

@export("my_ext_init_c_api", ABI="C")
fn my_ext_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    return Extension.run[init](info, access)
```

```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
```

```sql
LOAD 'my_ext.duckdb_extension';
SELECT mojo_add_numbers(40, 2);  -- 42
```

See the [demo extension](demo-extension/) for a full working example.

## Status
- The [FFI bindings](src/_libduckdb.mojo) should be complete as they are auto-generated but the high-level Mojo API is still work in progress.
- We need to build a small C shim library until https://github.com/modular/modular/issues/5846 is fixed.


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

### (Re-)generate the C API bindings

The low-level bindings in `src/_libduckdb.mojo` are auto-generated from DuckDB's
declarative JSON schemata (the same source used to generate `duckdb.h`).
To regenerate them (e.g. after bumping the DuckDB version in `pixi.toml`):

```shell
pixi run generate-api
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

## Table Functions

Register Mojo functions as DuckDB table functions that generate rows.
A table function needs three callbacks: **bind** (declare output columns and
store parameters), **init** (optional per-scan setup), and the **main function**
(produce output batches).

```mojo
from duckdb import *
from duckdb.table_function import TableFunction, TableFunctionInfo, TableBindInfo, TableInitInfo
from duckdb._libduckdb import *
from memory.unsafe_pointer import alloc

@fieldwise_init
struct CounterBindData(Copyable, Movable):
    var limit: Int
    var current_row: Int

fn destroy_bind_data(data: UnsafePointer[NoneType, MutAnyOrigin]):
    data.bitcast[CounterBindData]().destroy_pointee()

fn counter_bind(info: TableBindInfo):
    info.add_result_column("i", LogicalType(DuckDBType.integer))
    var limit = Int(info.get_parameter(0).as_int32())
    var bind_data = alloc[CounterBindData](1)
    bind_data.init_pointee_move(CounterBindData(limit=limit, current_row=0))
    info.set_bind_data(bind_data.bitcast[NoneType](), destroy_bind_data)

fn counter_init(info: TableInitInfo):
    pass

fn counter_function(info: TableFunctionInfo, mut output: Chunk):
    var bind_data = info.get_bind_data().bitcast[CounterBindData]()
    var current = bind_data[].current_row
    var remaining = bind_data[].limit - current
    if remaining <= 0:
        output.set_size(0)
        return
    var batch = min(remaining, 2048)
    var out = output.get_vector(0).get_data().bitcast[Int32]()
    for i in range(batch):
        out[i] = Int32(current + i)
    bind_data[].current_row = current + batch
    output.set_size(batch)

fn main() raises:
    var conn = DuckDB.connect(":memory:")
    var tf = TableFunction()
    tf.set_name("generate_ints")
    tf.add_parameter(LogicalType(DuckDBType.bigint))
    tf.set_function[counter_bind, counter_init, counter_function]()
    tf.register(conn)

    var result = conn.execute("SELECT sum(i) FROM generate_ints(100)")
```

## Aggregate Functions

Register Mojo functions as DuckDB aggregate functions that reduce many rows
into a single value (per group). There are two API levels: high-level
convenience methods and a low-level callback API.

### High-level: reduction-based aggregates

Use `from_sum`, `from_max`, `from_min`, `from_product`, and `from_mean` to
register common aggregates in one line:

```mojo
from duckdb import *
from duckdb.aggregate_function import AggregateFunction

var conn = DuckDB.connect(":memory:")

AggregateFunction.from_sum["mojo_sum", DType.float64](conn)
AggregateFunction.from_max["mojo_max", DType.float64](conn)
AggregateFunction.from_min["mojo_min", DType.float64](conn)
AggregateFunction.from_mean["mojo_avg", DType.float64](conn)
AggregateFunction.from_product["mojo_product", DType.float64](conn)

var result = conn.execute("SELECT mojo_sum(x), mojo_max(x) FROM my_table")
```

### Custom reductions with `from_reduce`

Define your own binary SIMD reduce function and identity element:

```mojo
fn my_add[w: Int](a: SIMD[DType.float64, w], b: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return a + b

fn zero() -> Scalar[DType.float64]:
    return 0.0

AggregateFunction.from_reduce["custom_sum", DType.float64, my_add, zero](conn)
```

A separate-type overload allows accumulating into a wider type (e.g. Int32 input
→ Int64 output):

```mojo
fn add[w: Int](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    return a + b

fn zero() -> Scalar[DType.int64]:
    return 0

AggregateFunction.from_reduce["wide_sum", DType.int32, DType.int64, add, zero](conn)
```

### Low-level API

For full control, implement the five aggregate callbacks manually
(state_size, state_init, update, combine, finalize) plus an optional destructor:

```mojo
from sys.info import size_of
from duckdb import *
from duckdb.aggregate_function import *
from duckdb._libduckdb import *

fn my_state_size(info: AggregateFunctionInfo) -> idx_t:
    return idx_t(size_of[Int64]())

fn my_state_init(info: AggregateFunctionInfo, state: AggregateState):
    state.get_data().bitcast[Int64]().init_pointee_move(0)

fn my_update(info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray):
    var data = input.get_vector(0).get_data().bitcast[Int32]()
    for i in range(len(input)):
        var s = states.get_state(i).get_data().bitcast[Int64]()
        s[] += Int64(data[i])

fn my_combine(info: AggregateFunctionInfo, source: AggregateStateArray,
              target: AggregateStateArray, count: Int):
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[Int64]()
        var t = target.get_state(i).get_data().bitcast[Int64]()
        t[] += s[]

fn my_finalize(info: AggregateFunctionInfo, source: AggregateStateArray,
               result: Vector, count: Int, offset: Int):
    var out = result.get_data().bitcast[Int64]()
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[Int64]()
        out[offset + i] = s[]

fn main() raises:
    var conn = DuckDB.connect(":memory:")
    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[my_state_size, my_state_init, my_update, my_combine, my_finalize]()
    func.register(conn)
```

### Reduction Benchmark

A benchmark comparing Mojo aggregate functions against DuckDB builtins is
available in `benchmark/reduction_benchmark.mojo`. It covers ungrouped and
grouped aggregates (sum, max, min, avg) on 10M rows.

```shell
pixi run mojo run benchmark/reduction_benchmark.mojo
```

### Note on SIMD utilization and the DuckDB C API

Mojo's `algorithm.reduction` module provides highly optimized SIMD-vectorized
and parallelized reduction functions (`sum`, `max`, `min`, `mean`, etc.) that
operate on contiguous `Span` data. However, these cannot be used directly in
DuckDB aggregate callbacks because the C API `update` function receives one
state pointer **per row** (`duckdb_aggregate_state *states`), where each pointer
may reference a different group's state — there is no contiguous buffer-to-single-accumulator path.

DuckDB's internal aggregates use a separate `simple_update` callback for
ungrouped aggregates that passes the entire vector plus a single state pointer,
which would be a natural fit for stdlib reduction. However, the C API does not
expose this — `simple_update` is hardcoded to `nullptr` for all C API aggregate
functions.

Exposing a `duckdb_aggregate_function_set_simple_update(fn(info, vector, state, count))`
callback in the C API would allow Mojo bindings to call
`algorithm.reduction.sum(Span(vector_data, count))` directly on the input
vector, leveraging full SIMD vectorization and parallel execution for ungrouped
aggregates instead of the current scalar per-row accumulation loop.