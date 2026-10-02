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

Requirements: Mojo 1.1.0 and DuckDB 1.5.6, on Linux (x86_64, aarch64) and macOS
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
from duckdb import *

# Define a struct matching the query columns. Fields map to columns by position.
@fieldwise_init
struct StationCount(Writable, Copyable, Movable):
    var station: String
    var num_services: Int64

def main() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

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

More examples are in [`examples/`](examples/).

## Client API

Results decode into Mojo scalars, `String`, `Optional`, `List`, `Dict`,
`Tuple`, `Variant` and structs, including nested `LIST`, `STRUCT` and `MAP`
columns. `Result.show()` prints any result as a table, for every DuckDB type.
Build nested types with `list_type`, `map_type`, `array_type`, `struct_type`,
`decimal_type`, and `enum_type`.

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

### Appender

The Appender loads rows faster than `INSERT` statements. Rows can be structs
or tuples ([`examples/appender.mojo`](examples/appender.mojo)):

```mojo
var appender = Appender(con, "people")
appender.append_row(Person(1, "Mark"))
appender.append_rows([Person(2, "Hannes"), Person(3, "Pedro")])
appender.close()
```

## User-defined functions

Register Mojo functions as DuckDB scalar, aggregate and table functions. They
work in client code and inside extensions.

### Scalar functions

Pass Mojo stdlib math functions directly, write your own SIMD kernel, or write
a function that handles one row at a time. SIMD functions get the input in
batches of the hardware SIMD width
([`examples/scalar_function.mojo`](examples/scalar_function.mojo)):

```mojo
import std.math as math
from duckdb import *
from duckdb.scalar_function import ScalarFunction

def sin_plus_cos[w: SIMDLength](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return math.sin(x) + math.cos(x)

def add_one(x: Int32) -> Int32:
    return x + 1

def main() raises:
    var conn = DuckDB.connect(":memory:")

    # Stdlib math functions
    ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, math.sqrt](conn)
    ScalarFunction.from_simd_function["mojo_atan2", DType.float64, math.atan2](conn)

    # A custom SIMD kernel
    ScalarFunction.from_simd_function[
        "mojo_sin_plus_cos", DType.float64, DType.float64, sin_plus_cos
    ](conn)

    # One row at a time
    ScalarFunction.from_function["add_one", DType.int32, DType.int32, add_one](conn)

    conn.execute("SELECT mojo_sqrt(2.0), mojo_sin_plus_cos(1.0), add_one(41)").show()
```

### Aggregate functions

Common reductions are one line of code. `from_reduce` builds an aggregate from a SIMD
combine function and its identity value, optionally accumulating into a wider
type:

```mojo
AggregateFunction.from_sum["mojo_sum", DType.float64](conn)
AggregateFunction.from_max["mojo_max", DType.float64](conn)  # also from_min, from_mean, from_product

def add[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    return a + b

def zero() -> Scalar[DType.int64]:
    return 0

# INTEGER input, BIGINT result
AggregateFunction.from_reduce["wide_sum", DType.int32, DType.int64, add, zero](conn)
```

For full control, you can implement the callbacks yourself (state size, init, update,
combine, finalize, and an optional destructor) and register them with
`AggregateFunction.set_functions`. See
[`examples/aggregate_function.mojo`](examples/aggregate_function.mojo).

### Table functions

A table function has three callbacks: bind declares the output columns and
reads the parameters, init sets up a scan, and the main function fills output
chunks until it returns an empty one. See
[`examples/table_function.mojo`](examples/table_function.mojo), which registers
`generate_ints(n)`:

```mojo
var tf = TableFunction()
tf.set_name("generate_ints")
tf.add_parameter(LogicalType(DuckDBType.integer))
tf.set_bind[counter_bind]()
tf.set_init[counter_init]()
tf.set_function[counter_function]()
tf.register(conn)

conn.execute("SELECT sum(i) FROM generate_ints(100)").show()
```

## Writing extensions

Build DuckDB extensions as shared libraries in Mojo. Write an init function
that receives a `Connection` and registers your functions, then pass it to
`Extension.run`:

```mojo
from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction

def add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

def init(conn: Connection) raises:
    ScalarFunction.from_function[
        "mojo_add_numbers", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)

@export("my_ext_init_c_api")
def my_ext_init_c_api(
    info: duckdb_extension_info,
    access: Pointer[duckdb_extension_access, MutUntrackedOrigin],
) abi("C") -> Bool:
    return Extension.run[init](info, access)
```

Build it and append the metadata footer that DuckDB checks on `LOAD`:

```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
python3 scripts/append_extension_metadata.py my_ext.duckdb_extension
```

```sql
LOAD 'my_ext.duckdb_extension';  -- needs allow_unsigned_extensions
SELECT mojo_add_numbers(40, 2);  -- 42
```

The extension uses DuckDB's
[C extension API](https://github.com/duckdb/duckdb/tree/v1.5.6/api_spec/v1),
which hands the extension a
[struct of function pointers](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb_extension.h)
for a requested API version. `Extension.run` requests v1.5.6, which covers the
whole C API, so extensions need DuckDB 1.5.6 or newer. The struct only grows
between releases, so a compiled extension keeps working with later DuckDB 1.x
releases without a rebuild.

See the [demo extension](extensions/demo-extension/) for a complete example.
Extensions that need DuckDB internals, such as replacing built-in functions or
hooking the optimizer, have to use the C++ API instead; see
[CPP-ABI extensions](docs/cpp-abi-extensions.md).

## Accelerating DuckDB

There are multiple ways to run Mojo kernels inside DuckDB.

### 1. Named SIMD functions (part of the package)

`duckdb.kernels.register_simd_math(conn)` registers `mojo_sqrt`, `mojo_sin`,
`mojo_cos`, `mojo_ln`, `mojo_exp` and `mojo_log10` as scalar functions. They are
part of the `duckdb` package, so there is nothing extra to build or `LOAD`:

```mojo
from duckdb.kernels import register_simd_math

register_simd_math(conn)
_ = conn.execute("SELECT mojo_sqrt(x) FROM t")
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

`benchmark/math_benchmark.mojo` and `benchmark/reduction_benchmark.mojo`
compare Mojo scalar and aggregate functions with DuckDB built-ins:

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

## Development

```shell
pixi run test                 # library and extension tests
pixi run compile-check        # compile the benchmarks and examples
pixi run overrides-test       # build and test mojo-kernel-overrides
pixi run -e gpu gpu-op-test   # build and test mojo-gpu-operator (needs a GPU)
```

The low-level bindings in `duckdb/_libduckdb.mojo` are generated from DuckDB's
API spec in the `third_party/duckdb` submodule. Regenerate them with
`pixi run generate-api` after updating DuckDB.

`pixi build` builds a conda package of the bindings, and
`conda.recipe/recipe.yaml` is the recipe submitted to modular-community.
