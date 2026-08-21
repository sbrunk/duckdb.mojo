# duckdb.mojo

[![CodeQL](https://github.com/sbrunk/duckdb.mojo/actions/workflows/codeql.yml/badge.svg)](https://github.com/sbrunk/duckdb.mojo/actions/workflows/codeql.yml)

[Mojo](https://www.modular.com/mojo) bindings for [DuckDB](https://duckdb.org/).

duckdb.mojo can be used in two ways:

1. **Client API**: query DuckDB from Mojo, register scalar/aggregate/table functions (UDFs), and process results with SIMD vectorization.
2. **Extension development**: build DuckDB [extensions](https://duckdb.org/docs/stable/extensions/overview) written in Mojo that can be loaded with `LOAD`. See the [demo extension](extensions/demo-extension/README.md) for a working example.
3. **Accelerate DuckDB**: drop-in Mojo kernels for existing queries. The CPU/SIMD built-in overrides ([mojo-kernel-overrides](extensions/mojo-kernel-overrides/README.md), dependency-free) and the GPU offload ([mojo-gpu-operator](extensions/mojo-gpu-operator/README.md)) speed up aggregates, math, and vector search. See [Accelerating DuckDB](#accelerating-duckdb).

## 10 minute presentation at the MAX & Mojo community meeting

<div align="center">
  <a href="https://www.youtube.com/watch?v=6huytcgQgk8&t=788"><img src="https://img.youtube.com/vi/6huytcgQgk8/0.jpg" alt="10 minute DuckDB.mojo presentation at the MAX & Mojo community meeting"></a>
</div>

## Examples

### Client API

```mojo
from duckdb import *

# Struct fields map to query columns by position.
@fieldwise_init
struct StationCount(Writable, Copyable, Movable):
    var station: String
    var num_services: Int64

def main():
    var con = DuckDB.connect(":memory:")
    _ = con.execute("""
    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """)

    var query = """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name", count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """

    # Iterate over rows directly
    for row in con.execute(query):
        print(row.get[String](col=0), " ", row.get[Int64](col=1))

    # Iterate over chunks, then rows within each chunk
    for chunk in con.execute(query).chunks():
        for row in chunk:
            print(row.get[String](col=0), " ", row.get[Int64](col=1))

    # Decode directly into tuples
    for row in con.execute(query):
        var t = row.get_tuple[String, Int64]()
        print(t[0], ": ", t[1])

    # Typed struct access
    var result = con.execute(query).fetchall()
    var stations: List[StationCount] = result.get[StationCount]()
    for i in range(len(stations)):
        print(stations[i])
```

`Result.show()` renders every DuckDB type, including `DECIMAL`, the date/time
family, `UUID`, `INTERVAL`, `BIT`, `BLOB`, `ENUM`, and nested
`LIST`/`STRUCT`/`MAP`.

### Relation API (lazy, composable)

`con.sql(...)`, `con.table(...)`/`con.view(...)`, and the `read_*` readers return
a lazy `Relation`: a query you build with chainable transforms that runs only at
a terminal (`show`/`fetchall`/`get[T]`/`to_table`/...). It mirrors DuckDB's
Python relational API and reuses the typed decoding shown above.

```mojo
from duckdb import *

var con = DuckDB.connect(":memory:")
_ = con.execute("CREATE TABLE trips AS SELECT * FROM 'trips.parquet'")

# Build lazily, execute at a terminal.
con.sql("FROM trips") \
   .filter("fare > 0") \
   .aggregate("payment, sum(fare) AS total", group="payment") \
   .order("total DESC") \
   .limit(10) \
   .show()

# Readers are relations, so they chain directly.
con.read_csv("more_trips.csv").filter("fare > 0").count().show()

# Terminals reuse typed decoding. Print renders the table.
var payments = con.table("trips").select("payment").distinct().get[String]()
print(con.sql("SELECT 1 AS a, 'hi' AS b"))

# lit() quotes values, col() quotes identifiers (injection-safe by default).
con.table("people").filter(col("name") + " = " + lit("O'Brien")).show()

# Set operations, joins, and persistence.
var ab = con.sql("SELECT 1 AS x").union(con.sql("SELECT 2 AS x"))
con.table("orders").set_alias("o") \
   .join(con.table("items").set_alias("i"), on="o.id = i.order_id") \
   .to_table("orders_items")
```

### Connection lifecycle, transactions, and errors

```mojo
from duckdb import *

with DuckDB.connect("my.db") as con:          # disconnects at block exit
    con.begin()
    _ = con.execute("INSERT INTO t VALUES (1)")
    con.commit()                              # or con.rollback()

    con.install_extension("httpfs")
    con.load_extension("httpfs")

    var cur = con.cursor()                    # second connection, same database

    try:
        _ = con.execute("SELECT * FROM missing")
    except e:
        # Coarse error categories for branching on the kind of failure.
        if e.type.is_programming_error():
            print("bad SQL:", e)              # CATALOG/PARSER/BINDER/...
```

Build nested types with `list_type`, `map_type`, `array_type`, `struct_type`,
`decimal_type`, and `enum_type`.

### Parameterized queries

Bind parameters positionally (`?` / `$1`) or by name (`$name`). Plain Mojo
scalars are bound directly; `Optional[T]` binds SQL `NULL` for `None`:

```mojo
from duckdb import *

var con = DuckDB.connect(":memory:")
_ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")

# Positional parameters
_ = con.execute("INSERT INTO t VALUES (?, ?)", 1, String("Mark"))

# Bulk insert: prepares once, re-binds per row
var rows: List[Tuple[Int32, String]] = [
    (Int32(2), String("Hannes")),
    (Int32(3), String("Pedro")),
]
con.executemany("INSERT INTO t VALUES (?, ?)", rows)

# Named parameters
var r = con.execute_named(
    "SELECT name FROM t WHERE id = $id", {"id": 1}
).fetchall()

# Or prepare explicitly and reuse
var stmt = con.prepare("SELECT $1 + $2")
stmt.bind(1, Int32(40))
stmt.bind(2, Int32(2))
var sum = stmt.execute().fetchall()
```

### Module-level API and result helpers

A Python-style top-level API runs against a lazily-created, process-wide
in-memory default connection:

```mojo
import duckdb

# Default connection
duckdb.sql("CREATE TABLE t AS SELECT * FROM range(5) r(i)")
duckdb.sql("SELECT * FROM t").show()        # formatted table

var con = duckdb.connect("my.db", read_only=True)

# Read files directly
var csv = con.read_csv("data.csv").fetchall()

# Typed fetching (column types given as parameters)
var result = con.execute("SELECT i FROM t")
var first = result.fetchone[Int64]()        # Optional[Tuple[Int64]]
var batch = result.fetchmany[Int64](size=2)

# Column metadata
print(result.columns())                     # List[String] of names
print(result.description())                 # List[Column] (index/name/type)
```

### Extensions

Build DuckDB extensions as shared libraries in Mojo. Write an init function
that receives a `Connection` and registers your functions, then pass it to
`Extension.run`:

```mojo
from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.api_level import ApiLevel
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction

fn add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

fn init(conn: Connection[ApiLevel.EXT_STABLE]) raises:
    ScalarFunction.from_function[
        "mojo_add_numbers", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)

@export("my_ext_init_c_api")
fn my_ext_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) abi("C") -> Bool:
    return Extension.run[init](info, access)
```

DuckDB's [Extension C API](https://github.com/duckdb/duckdb/blob/v1.5.5/src/include/duckdb/main/capi/header_generation/README.md)
provides extensions with a [struct of function pointers](https://github.com/duckdb/duckdb/blob/v1.5.5/src/include/duckdb_extension.h)
instead of relying on dynamic symbol lookup. The struct is split into a
**stable** and an **unstable** part (see [duckdb/duckdb#14992](https://github.com/duckdb/duckdb/pull/14992)
for the full design):

- **Stable** (`Extension.run`): uses only functions stabilized since DuckDB
  v1.2.0.  Because the stable struct is append-only and never modified, the
  compiled extension binary is forward-compatible with all future DuckDB
  releases that share the same API major version.
- **Unstable** (`Extension.run_unstable`): additionally exposes recently added
  functions that are candidates for future stabilization.  Unstable extensions
  are tied to the exact DuckDB version they were compiled against, since
  unstable entries may be reordered or removed between releases.

`Extension.run` resolves functions from `duckdb_ext_api_v1` (stable part).
The `Connection` is parameterized with an `ApiLevel` that gates access to
unstable functions at **compile time**, so calling an unstable method from a
stable-only extension is a compile error, not a runtime crash.

If you need access to unstable C API functions, use `Extension.run_unstable` instead:

```mojo
fn init_unstable(conn: Connection[ApiLevel.EXT_UNSTABLE]) raises:
    # Unstable methods like ScalarFunction.set_bind() are available here
    ...

@export("my_ext_init_c_api")
fn my_ext_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) abi("C") -> Bool:
    return Extension.run_unstable[init_unstable](info, access)
```

```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
```

```sql
LOAD 'my_ext.duckdb_extension';
SELECT mojo_add_numbers(40, 2);  -- 42
```

See the [demo extension](extensions/demo-extension/) for a full working example.

### CPP-ABI extensions (advanced)

The C API above is enough for scalar/aggregate/table UDFs, but it cannot reach
DuckDB internals such as mutating catalog entries, adding an `OptimizerExtension`,
or registering a custom logical operator. For those you need DuckDB's **CPP ABI**.
This is how the [mojo-kernel-overrides](extensions/mojo-kernel-overrides/README.md)
and [mojo-gpu-operator](extensions/mojo-gpu-operator/README.md) extensions are built.

A CPP-ABI extension here is really a **C++ extension that calls Mojo-compiled
kernels over a C ABI**. No DuckDB C++ type crosses into Mojo:

- **Mojo** exports kernels over raw pointers with `@export(...) ... abi("C")`.
- **C++** declares those symbols in an `extern "C"` block and calls them, and does
  all the DuckDB-internal work (catalog, optimizer, operators) against the internal
  C++ headers.

The C++ side provides the entry points DuckDB's loader looks up by extension name:

```cpp
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

extern "C" {                                  // Mojo kernels, linked into this .so
void myext_scale_f64(const double *in, double *out, int64_t n, double k);
}

namespace duckdb {
void RegisterMyExt(DatabaseInstance &db) {
    // internal C++ API: mutate the catalog / add an OptimizerExtension / register
    // a TableFunction, calling myext_scale_f64(...) on raw FLAT column buffers.
}
} // namespace duckdb

extern "C" {
// LOAD entry point: DuckDB calls <extension_name>_duckdb_cpp_init.
__attribute__((visibility("default")))
void myext_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::RegisterMyExt(loader.GetDatabaseInstance());
}
__attribute__((visibility("default")))
const char *myext_version() { return duckdb::DuckDB::LibraryVersion(); }

// Optional: let an embedder that already holds a connection install it directly
// (no LOAD, so no footer/version check; the caller must match the DuckDB version).
__attribute__((visibility("default")))
void register_myext(duckdb_connection connection) {
    auto con = reinterpret_cast<duckdb::Connection *>(connection);
    duckdb::RegisterMyExt(*con->context->db);
}
}
```

Build it in two steps: compile the Mojo kernels, then link them into a C++ shared
object and append the `CPP` metadata footer.

```sh
# 1. Mojo kernels. --emit object gives a plain .o with NO Mojo runtime deps (CPU/SIMD
#    only), so the final .so is self-contained (links only libm). If the kernels need
#    the Mojo GPU/AsyncRT runtime, use --emit shared-lib instead and link + rpath the
#    resulting companion dylib (see extensions/mojo-gpu-operator/build.sh).
mojo build --emit object kernels.mojo -o kernels.o

# 2. C++ extension. DuckDB symbols are left unresolved and bound at load time against
#    the host libduckdb (-undefined dynamic_lookup on macOS; -Wl,--allow-shlib-undefined
#    on Linux). Internal headers come from conda libduckdb-devel.
clang++ -std=c++17 -O2 -fPIC -shared -undefined dynamic_lookup \
    myext.cpp kernels.o -I "$CONDA_PREFIX/include" -lm \
    -o myext.duckdb_extension

# 3. Append the footer. CPP is version-locked, so the version field is the DuckDB
#    version (not the C API version).
python3 scripts/append_extension_metadata.py myext.duckdb_extension \
    --abi-type CPP --duckdb-version v1.5.5
```

Caveats specific to the CPP ABI:

- **Version-locked.** The footer carries the exact DuckDB version and `LOAD` rejects
  any mismatch. Rebuild per DuckDB version. (The stable C API, by contrast, is
  forward-compatible.)
- **Needs the internal C++ headers** and an ABI-matched libduckdb, from conda
  `libduckdb-devel`, not the stable C extension API.
- **Unsigned**, so load with `-unsigned` / `allow_unsigned_extensions`.
- A host that statically links DuckDB and `dlopen`s a CPP extension must link with
  `-rdynamic` so DuckDB's symbols resolve in the loaded extension.


## Installation

### Use in your own project (conda package)

`duckdb-mojo` is going to be available on the
[modular-community](https://prefix.dev/channels/modular-community) channel soon.
Once it's published, you can install it as follows:

Add the channels to your project's `pixi.toml` and install it.

```toml title="pixi.toml"
[workspace]
channels = [
  "https://conda.modular.com/max",
  "https://repo.prefix.dev/modular-community",
  "conda-forge",
]
```

```shell
pixi add duckdb-mojo
```

The `libduckdb` runtime library is pulled in automatically as a dependency.
Import it in Mojo as usual:

```mojo
from duckdb import *
```

### Develop from source

1. [Install Pixi](https://pixi.sh/latest/installation/).
2. Checkout this repo
3. Run `pixi shell`
4. Run `mojo example.mojo`

### Run Tests

```shell
pixi run test
```

### Build a conda package

There are two ways to build a conda package of the bindings, for two different
purposes:

**`pixi build`** uses the `[package]` block in `pixi.toml` (the
`pixi-build-mojo` backend, which infers the build steps from the project
layout). Produces a local `.conda` and lets other Pixi workspaces depend on
duckdb.mojo as a source dependency. No recipe needed:

```shell
pixi build
```

**`rattler-build`** builds from the explicit recipe in `conda.recipe/`. This
is the path used to publish to the
[modular-community](https://prefix.dev/channels/modular-community) channel,
whose CI runs `rattler-build` on the submitted `conda.recipe/recipe.yaml`. To
verify the recipe locally, build the `recipe.local.yaml` variant (it builds
from the working tree instead of a pushed git SHA):

```shell
rattler-build build \
  --recipe conda.recipe/recipe.local.yaml \
  -c conda-forge \
  -c https://conda.modular.com/max \
  -c https://repo.prefix.dev/modular-community
```

A successful build runs the in-package smoke test and writes the `.conda` under
`output/<platform>/`. `conda.recipe/recipe.yaml` is the file submitted to
modular-community; bump its `mojo-compiler` pin together with `pixi.toml` on
every compiler update.

### (Re-)generate the C API bindings

The low-level bindings in `duckdb/_libduckdb.mojo` are auto-generated from DuckDB's
declarative JSON schemata (the same source used to generate `duckdb.h`).
To regenerate them (e.g. after bumping the DuckDB version in `pixi.toml`):

```shell
pixi run generate-api
```

## Scalar Functions

Register Mojo functions as DuckDB scalar functions (UDFs) that operate on table
columns. There are several convenience levels:

### Stdlib math functions (zero boilerplate)

Pass Mojo stdlib math functions directly. Types and SIMD vectorization are
handled automatically:

```mojo
import math
from duckdb import *
from duckdb.scalar_function import ScalarFunction

var conn = DuckDB.connect(":memory:")

# Register stdlib math functions as SQL scalar functions, one line each
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

# Register. Data is processed in hardware-optimal SIMD batches automatically
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

### Accelerating DuckDB

Three ways to run Mojo compute inside DuckDB: named SIMD UDFs (part of this package),
the CPU/SIMD built-in overrides extension, and the GPU offload extension.

**1. Named UDFs (part of this package).** `duckdb.kernels.register_simd_math(conn)`
registers `mojo_sqrt`, `mojo_sin`, ... as scalar functions you call by name. The
kernels and this helper are part of the `duckdb` package itself: the conda
`duckdb-mojo` package precompiles all of `duckdb/` (including `duckdb/kernels`),
so there is nothing extra to build, ship, or `LOAD`. Install the package, import,
call:

```mojo
from duckdb.kernels import register_simd_math
register_simd_math(conn)
_ = conn.execute("SELECT mojo_sqrt(x) FROM t")
```

You can also use the kernels (`duckdb.kernels.simd`) directly in your own UDFs.

**2. Built-in overrides (CPU/SIMD, a separate dependency-free extension).** To
speed up existing queries without renaming functions, the
[mojo-kernel-overrides](extensions/mojo-kernel-overrides/README.md) extension rewrites
selected built-ins in place, without forking DuckDB:

- scalar `sqrt`/`sin`/`cos`/`ln`/`exp`/`log10`;
- aggregates `sum`/`avg` (DOUBLE plus INT128-backed HUGEINT/DECIMAL) and `min`/`max`;
- vector distance: `array_distance`, `array_cosine_distance`,
  `array_cosine_similarity`, `array_inner_product`, `array_negative_inner_product`;
- nullable columns (validity-masked, not just all-valid), and a transparent optimizer
  rewrite of `sum/avg(sqrt|exp|ln|…)` into a fused one-pass kernel.

It also adds `mojo_knn(...)`, a batch (multi-query) brute-force top-k table function
for vector search. The kernels are linked straight in, so the `.so` is self-contained
(only libm), with no Mojo runtime dependency. It is **not** part of the conda package;
build with `pixi run overrides-build`, then `LOAD` it (allow unsigned extensions):

```mojo
from duckdb.config import Config
var config = Config()
config.set("allow_unsigned_extensions", "true")
var conn = DuckDB.connect(":memory:", config)
_ = conn.execute("LOAD 'extensions/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension'")
```

**3. GPU offload (a separate extension).** The
[mojo-gpu-operator](extensions/mojo-gpu-operator/README.md) extension transparently
offloads supported query plans to the GPU via an `OptimizerExtension`, with the
compute kernels written in Mojo. It is general-purpose: it handles a class of
aggregation-over-filter/join plans, plus vector-search top-k (`gpu_cosine_topk` and
`gpu_cosine_topk_batch`). Matching queries route to the GPU with no syntax change.
Anything it can't translate, or any runtime GPU error, falls back to stock DuckDB,
with decimal-exact results. Runs on NVIDIA and Apple GPUs. Build with `pixi run gpu-op-build`.
(Unlike the CPU overrides it links the Mojo GPU runtime, so it is not a single
self-contained `.so`.)

### Benchmarks

A consolidated harness lives in [benchmark/](benchmark/README.md):

```shell
pixi run bench-build                                  # build DuckDB's benchmark_runner (once)
pixi run bench-sql <group> --engines=stock,cpu,gpu    # warm stock vs CPU-SIMD vs GPU compare
pixi run bench-knn                                    # vector-search: single + batch cosine top-k
```

`bench-knn` compares stock / CPU-SIMD / vss-HNSW / GPU with latency and recall. The
standalone POC microbenchmarks above (`benchmark/math_benchmark.mojo`,
`benchmark/reduction_benchmark.mojo`, `pixi run overrides-bench`) remain for quick
kernel-level checks.

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
may reference a different group's state, so there is no contiguous buffer-to-single-accumulator path.

DuckDB's internal aggregates use a separate `simple_update` callback for
ungrouped aggregates that passes the entire vector plus a single state pointer,
which would be a natural fit for stdlib reduction. However, the C API does not
expose this. `simple_update` is hardcoded to `nullptr` for all C API aggregate
functions.

Exposing a `duckdb_aggregate_function_set_simple_update(fn(info, vector, state, count))`
callback in the C API would allow Mojo bindings to call
`algorithm.reduction.sum(Span(vector_data, count))` directly on the input
vector, leveraging full SIMD vectorization and parallel execution for ungrouped
aggregates instead of the current scalar per-row accumulation loop.