<p align="center">
  <img src="docs/assets/logo.svg" width="160" alt="duckdb.mojo logo: the DuckDB duck inside the Mojo flame">
</p>

# duckdb.mojo

[![Run tests](https://github.com/sbrunk/duckdb.mojo/actions/workflows/test.yml/badge.svg)](https://github.com/sbrunk/duckdb.mojo/actions/workflows/test.yml)
[![CodeQL](https://github.com/sbrunk/duckdb.mojo/actions/workflows/codeql.yml/badge.svg)](https://github.com/sbrunk/duckdb.mojo/actions/workflows/codeql.yml)

[Mojo](https://mojolang.org/) bindings for [DuckDB](https://duckdb.org/).

You can use duckdb.mojo to:

1. Query DuckDB from Mojo, decode results into Mojo types, and register
   scalar, aggregate and table functions written in Mojo.
2. Write DuckDB extensions in Mojo that load with `LOAD`.
3. Speed up DuckDB with Mojo kernels (experimental): SIMD versions of
   built-in functions, and GPU execution of aggregations and vector search,
   without changing the SQL.

Requirements: Mojo 1.2.0 and DuckDB 2.0.0, on Linux (x86_64, aarch64) and macOS
(Apple silicon). Writing extensions and the acceleration extensions are
experimental.

<div align="center">
  <a href="https://www.youtube.com/watch?v=6huytcgQgk8&t=788"><img src="https://img.youtube.com/vi/6huytcgQgk8/0.jpg" alt="10 minute duckdb.mojo presentation at the MAX & Mojo community meeting"></a>
  <br>10 minute presentation at the MAX & Mojo community meeting
</div>

## Quick start

duckdb.mojo is not published as a package yet (see [Installation](#installation)).
To try it from source, [install Pixi](https://pixi.sh/latest/installation/), then:

```shell
git clone https://github.com/sbrunk/duckdb.mojo
cd duckdb.mojo
pixi run mojo run examples/example.mojo
```

[`examples/example.mojo`](examples/example.mojo) loads a CSV file over HTTP and
queries it:

```mojo
import duckdb


# Rows map to struct fields by column name.
@fieldwise_init
struct StationCount(Copyable, Defaultable, Movable, Writable):
    var station: String
    var num_services: Int64

    def __init__(out self):
        self.station = ""
        self.num_services = 0


def main() raises:
    var con = duckdb.connect()
    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """)

    var query = """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name" AS station, count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """

    # Print the result as a table
    con.execute(query).show()

    # Read all rows, then access cells by column and row
    var result = con.execute(query).fetchall()
    for row in result.rows():
        print(row.get[String](0), " ", row.get[Int64](1))

    # Stream chunks and read columns
    for chunk in con.execute(query).chunks():
        var names = chunk.column[String](0)
        for i in range(len(chunk)):
            print(names.get(i).value())

    # Rows as tuples
    for t in con.execute(query).get[Tuple[String, Int64]]():
        print(t[0], ": ", t[1])

    # Rows as structs
    for s in con.execute(query).get[StationCount]():
        print(s)
```

More examples are in [`examples/`](examples/).

## Client API

The bindings are built on DuckDB's V2 C API. One type mapping is used in both
directions: for results, parameters, values, the appender and function
signatures. It covers Mojo scalars, `String`, `Optional` (NULL), `List`,
`Dict`, `Tuple`, `Variant`, structs and the value types `Date`, `Time`,
`Timestamp`, `Interval`, `Decimal`, `UUID` and `Bit`, including nested
`LIST`, `ARRAY`, `STRUCT`, `MAP` and `UNION` columns. Structs map to columns
and `STRUCT` fields by name (case-insensitive), tuples by position. A struct
read from DuckDB must be `Defaultable` when it has fields such as `String` or
`List`.

`execute` returns a streaming `Result`. Read it with `get[T]()` (all rows),
`fetchone[T]()`, `fetchmany[T](n)`, `chunks()`, `fetchall()` (all chunks in
memory) or `show()`. Errors are `DuckDBError`s with a `code` (`ErrorCode`)
and a message.

### Relation API (lazy, composable)

`con.sql(...)`, `con.table(...)` and the `read_*` readers return a lazy
`Relation`: a query you build with chainable transforms that runs only at a
terminal (`show`/`fetchall`/`get[T]`/`create`/...). It follows DuckDB's
Python relational API.

```mojo
import duckdb
from duckdb import col, lit

var con = duckdb.connect()
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

# Terminals decode into Mojo types.
var payments = con.table("trips").select("payment").distinct().get[String]()

# lit() quotes values, col() quotes identifiers.
con.table("people").filter(col("name") + " = " + lit("O'Brien")).show()

# Set operations, joins, and persistence.
var ab = con.sql("SELECT 1 AS x").union(con.sql("SELECT 2 AS x"))
con.table("orders").set_alias("o") \
   .join(con.table("items").set_alias("i"), on="o.id = i.order_id") \
   .create("orders_items")
```

### Transactions and errors

```mojo
import duckdb
from duckdb import ErrorCode

var con = duckdb.connect("my.db")
con.begin()
_ = con.execute("INSERT INTO t VALUES (1)")
con.commit()                                  # or con.rollback()

try:
    _ = con.execute("SELECT * FROM missing")
except e:
    if e.code == ErrorCode.CATALOG:           # or e.code.is_query(), is_io(), ...
        print("no such table:", e)
```

### Parameters and prepared statements

Bind parameters by position (`?`, `$1`) or by name (`$name`). Any mapped
type works as a parameter, including lists and structs; `None` binds NULL:

```mojo
import duckdb
from duckdb import Params

var con = duckdb.connect()
_ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR, tags VARCHAR[])")

# Positional parameters
_ = con.execute("INSERT INTO t VALUES (?, ?, ?)", Int32(1), String("Mark"), [String("a"), String("b")])

# Named parameters, from a builder or from a struct's fields
var p = Params()
p.set("id", Int32(1))
var names = con.execute("SELECT name FROM t WHERE id = $id", params=p^).get[String]()

# Prepare once, run many times
var stmt = con.prepare("SELECT $1 + $2")
var sum = stmt.execute(Int32(40), Int32(2)).get[Int32]()[0]
```

### Module-level API

A Python-style top-level API runs on a process-wide in-memory connection,
created on first use:

```mojo
import duckdb

_ = duckdb.execute("CREATE TABLE t AS SELECT * FROM range(5) r(i)")
duckdb.sql("SELECT * FROM t").show()
var csv = duckdb.read_csv("data.csv").fetchall()

var con = duckdb.connect("my.db", read_only=True)   # a separate database
```

### Appender

The Appender loads rows faster than `INSERT` statements. Rows can be structs
(by field name) or tuples (by position)
([`examples/appender.mojo`](examples/appender.mojo)):

```mojo
var appender = Appender(con, "people")
appender.append_row(Person(1, "Mark"))
appender.append_row((Int32(2), String("Hannes")))
appender.close()
```

It flushes every 204,800 rows and on `close()`. `on_conflict=` turns the
inserts into `INSERT OR IGNORE` / `OR REPLACE`, and `append_default` uses a
column's default value.

### More

`Connection` also gives access to settings (`con.settings()`), the file
system (`con.file_system()`), profiling (`con.enable_profiling()`,
`con.query_metrics()`), logging, Arrow import and export
(`con.execute_arrow`, `con.arrow_scan`) and non-blocking execution
(`result.step()`).

## User-defined functions

Register Mojo functions as DuckDB scalar, aggregate and table functions with
`con.register(...)`. The same definitions work inside extensions.

### Scalar functions

Pass Mojo stdlib math functions directly, write your own SIMD kernel, or write
a function that handles one row at a time. SIMD functions get the input in
batches of the hardware SIMD width. Row-at-a-time functions take and return
any mapped type, and their SQL signature is inferred from the Mojo one
([`examples/scalar_function.mojo`](examples/scalar_function.mojo)):

```mojo
import std.math as math
import duckdb
from duckdb import ScalarFunction

def sin_plus_cos[w: SIMDLength](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return math.sin(x) + math.cos(x)

def greet(name: String) -> String:
    return String("hello ", name)

# An Optional argument sees NULLs; otherwise NULL rows skip the call.
def or_zero(x: Optional[Int64]) -> Int64:
    return x.value() if x else 0

def main() raises:
    var con = duckdb.connect()

    # Stdlib math functions
    con.register(ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, math.sqrt]())
    # A custom SIMD kernel
    con.register(ScalarFunction.from_simd_function["mojo_sin_plus_cos", DType.float64, DType.float64, sin_plus_cos]())
    # One row at a time
    con.register(ScalarFunction.from_function["greet", greet]())
    con.register(ScalarFunction.from_function["or_zero", or_zero]())

    con.execute("SELECT mojo_sqrt(2.0), mojo_sin_plus_cos(1.0), greet('duck'), or_zero(NULL)").show()
```

`ScalarFunction.from_chunk_function` processes whole chunks with explicit
types (for example `DECIMAL`), with optional bind data and per-thread state.
Functions can have named parameters with defaults (`.named(...)`,
`.default(...)`), variadic arguments and `ANY` types.

### Aggregate functions

Common reductions are one line of code. `from_reduce` builds an aggregate from
a SIMD combine function and its identity value. For anything else, write a
state struct: `update` takes one row's arguments, `combine` merges states
computed in parallel, and `finalize` returns the result
([`examples/aggregate_function.mojo`](examples/aggregate_function.mojo)):

```mojo
con.register(AggregateFunction.from_sum["mojo_sum", DType.float64]())  # also from_min/max/mean/product

struct Avg(AggregateState):
    comptime Result = Float64
    var sum: Float64
    var n: Int

    def __init__(out self):
        self.sum = 0
        self.n = 0

    def update(mut self, x: Float64):
        self.sum += x
        self.n += 1

    def combine(mut self, other: Self):
        self.sum += other.sum
        self.n += other.n

    def finalize(self) -> Float64:
        return self.sum / Float64(self.n)

# The SQL signature is inferred from Avg.update.
con.register(AggregateFunction.from_state["my_avg", Avg.update]())
```

### Table functions

A table function is a struct: `bind` reads the arguments and declares the
output columns, `init` creates the state of one scan, and `scan` fills an
output chunk and returns its row count (0 ends the scan). Columns the query
doesn't use are skipped (`out.wants(col)`), scans can run in parallel, and
filters can be pushed down. See
[`examples/table_function.mojo`](examples/table_function.mojo), which
registers `squares(n)`:

```mojo
con.register(TableFunction.from_impl[Squares]("squares", param_types[Int64]()))
con.execute("SELECT sum(square) FROM squares(100)").show()
```

Casts, replacement scans, macros, pragmas, `COPY` formats, custom types,
options and log sinks can be registered the same way.

## Writing extensions

Build DuckDB extensions as shared libraries in Mojo. Write an init function
that registers your functions, then pass it to `Extension.run`:

```mojo
from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb import Extension, ScalarFunction

def add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

def init(ext: Extension) raises:
    ext.register(ScalarFunction.from_function["mojo_add_numbers", add_numbers]())

@export("my_ext_init_c_api_v2")
def my_ext_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    Extension.run[init](input)
```

Build it and append the metadata footer that DuckDB checks on `LOAD`:

```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
python3 scripts/append_extension_metadata.py my_ext.duckdb_extension --abi-type C_STRUCT_UNSTABLE
```

```sql
LOAD 'my_ext.duckdb_extension';  -- needs allow_unsigned_extensions
SELECT mojo_add_numbers(40, 2);  -- 42
```

Extensions use DuckDB's V2 C API. Every V2 function is still unstable in
DuckDB 2.0.0, so an extension loads only into the exact DuckDB version it was
built for, like DuckDB's C++ extensions, and needs a rebuild for every
release. The [demo extension](extensions/demo-extension/) is a complete
example.

### Raw C API bindings

The generated bindings are part of the package: `duckdb._libduckdb_v2`
(`LibDuckDBV2`, DuckDB's V2 C API, `api_spec/v2`) and `duckdb._libduckdb`
(the V1 C API). `duckdb.extension_v2.ExtensionV2` is the extension entrypoint
on the raw V2 API; see
[`test_v2_extension.mojo`](extensions/test-extension/src/test_v2_extension.mojo).

Extensions that need DuckDB internals, such as replacing built-in functions or
hooking the optimizer, have to use the C++ API instead; see
[CPP-ABI extensions](docs/cpp-abi-extensions.md).

## Accelerating DuckDB

There are multiple ways to run Mojo kernels inside DuckDB.

### 1. Named SIMD functions (part of the package)

`duckdb.register_simd_math(con)` registers `mojo_sqrt`, `mojo_sin`,
`mojo_cos`, `mojo_ln`, `mojo_exp` and `mojo_log10` as scalar functions. They are
part of the `duckdb` package, so there is nothing extra to build or `LOAD`:

```mojo
from duckdb import register_simd_math

register_simd_math(con)
_ = con.execute("SELECT mojo_sqrt(x) FROM t")
```

The kernels in `duckdb.kernels.simd` can also be used in your own functions.

### 2. Built-in overrides (CPU, SIMD)

The [mojo-kernel-overrides](extensions/mojo-kernel-overrides/README.md)
extension replaces built-in functions in place, so existing queries get faster
without changes:

- `sqrt`, `sin`, `cos`, `ln`, `exp`, `log10`
- `sum` and `avg` on `DOUBLE`, `HUGEINT` and `DECIMAL(19..38)`, and `min`/`max`,
  including columns with NULLs. The `HUGEINT`/`DECIMAL` sum is about 7.5× faster
  than stock DuckDB single-threaded.
- `array_distance`, `array_cosine_distance`, `array_cosine_similarity`,
  `array_inner_product`, `array_negative_inner_product`
- `sum`/`avg` of `sqrt`, `exp`, `ln` and so on, rewritten into one fused pass
- `mojo_knn(...)`, a table function for batched brute-force top-k vector search

Anything it doesn't handle falls back to the stock implementation. The
extension is a single self-contained `.so` that is built with
`pixi run overrides-build` and loaded with `LOAD`.

### 3. GPU execution

The [mojo-gpu-operator](extensions/mojo-gpu-operator/README.md) extension
recognizes supported query plans and runs them on the GPU with Mojo kernels:
aggregations over filters and joins , and exact
vector search (`array_cosine_distance` top-k, plus the `gpu_cosine_topk`
table functions). SQL doesn't change, and anything the GPU can't run falls back
to stock DuckDB. Results match stock DuckDB exactly for decimals. On an RTX 4090,
TPC-H sf1 runs up to about 11× faster than stock DuckDB (Q14); see the
extension README for all numbers. It runs on NVIDIA and Apple GPUs. Build it
with `pixi run -e gpu gpu-op-build`.

[`examples/gpu_knn.mojo`](examples/gpu_knn.mojo) shows the idea in about 170
lines without the extension: it keeps a table of embeddings on the GPU and runs
exact k-nearest-neighbor queries against it. Per query it is about 75× faster
than DuckDB's `array_cosine_distance` on an RTX 4090 and about 9× faster on
Apple silicon, with the same results:

```shell
pixi run -e gpu mojo run examples/gpu_knn.mojo
```

## Benchmarks

The [benchmark harness](benchmark/README.md) compares stock DuckDB with the
CPU and GPU extensions:

```shell
pixi run bench-build                                  # build DuckDB's benchmark_runner (once)
pixi run bench-sql <group> --engines=stock,cpu,gpu    # for example tpch/sf1/q06 or mojo_simd
pixi run bench-knn                                    # vector search: latency and recall
```

`benchmark/math_benchmark.mojo`, `benchmark/reduction_benchmark.mojo` and
`benchmark/udf_bench.mojo` compare Mojo scalar and aggregate functions with
DuckDB built-ins; `benchmark/serde_bench.mojo` measures reading rows into Mojo
values and appending them:

```shell
pixi run mojo run benchmark/math_benchmark.mojo
```

## Installation

duckdb.mojo will be published as `duckdb-mojo` on the
[modular-community](https://prefix.dev/channels/modular-community) channel.
Once it is, add the channels to your project's `pixi.toml` and install it:

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

The `libduckdb` runtime library comes with it as a dependency.

The package has no GPU code and doesn't depend on MAX. To write GPU code
alongside it, such as [`examples/gpu_knn.mojo`](examples/gpu_knn.mojo), also
run `pixi add max-core`. The matching version is selected automatically,
because both packages pin the same Mojo compiler. Note that MAX has different
license terms than Mojo: Mojo is licensed under Apache 2.0 with LLVM
exceptions, while most of MAX is under the
[Modular MAX Community License](https://www.modular.com/legal/community).

## Development

```shell
pixi run test                 # library and extension tests
pixi run compile-check        # compile the benchmarks and examples
pixi run overrides-test       # build and test mojo-kernel-overrides
pixi run -e gpu gpu-op-test   # build and test mojo-gpu-operator (needs a GPU)
```

The low-level bindings in `duckdb/_libduckdb.mojo` and
`duckdb/_libduckdb_v2.mojo` are generated from DuckDB's API spec in the
`third_party/duckdb` submodule. Regenerate them with
`pixi run generate-api` after updating DuckDB.

`pixi build` builds a conda package of the bindings, and
`conda.recipe/recipe.yaml` is the recipe submitted to modular-community.
