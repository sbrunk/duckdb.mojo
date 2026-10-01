# mojo-kernel-overrides

A DuckDB extension that routes selected built-in scalar and aggregate functions
to Mojo SIMD kernels, without patching DuckDB and without changes to your SQL. You can load
it into a normal libduckdb and the rewrites apply for the rest of the session.

## What it overrides

| function(s) | type | mechanism |
|---|---|---|
| `sqrt sin cos ln exp log10` | scalar `DOUBLE→DOUBLE` | swap `function` |
| `sum` `avg` | aggregate `DOUBLE` | swap `simple_update` (ungrouped) |
| `sum` `avg` | aggregate `HUGEINT` + `DECIMAL(19..38)` (INT128-backed) | direct swap (`HUGEINT`) / wrap `bind` (`DECIMAL`), swap `simple_update` (ungrouped) |
| `min` `max` | aggregate `DOUBLE` + `FLOAT` | wrap the `bind` callback, swap resolved `simple_update` |
| `array_distance` `array_inner_product` `array_negative_inner_product` `array_cosine_similarity` `array_cosine_distance` | scalar `FLOAT[]`/`DOUBLE[]` fold | swap `function` |

In stock DuckDB the array distance and similarity functions are serial loops with
a single accumulator (`result += x*y`). The compiler can't auto-vectorize them
because they are floating-point reductions. The Mojo kernels use `W`-wide SIMD
accumulators and unroll by 2 to break the dependency chain. That makes the raw
loop about 7 to 12× faster (768-dim f32: stock reaches about 4.8 GB/s, limited by
latency, while the kernel reaches about 52 GB/s, limited by memory bandwidth). End
to end, a brute-force `ORDER BY array_cosine_distance … LIMIT k` scan is about 1.5
to 1.7× faster, because the array scan and the top-k sort now take most of the
time. Three kernels (`array_dot`, `array_l2dist`, `array_cosine_sim`) cover all
five functions: the caller computes `negative_inner_product` as −x and
`cosine_distance` as 1−x. See [`CPU_SIMD_BACKLOG.md`](CPU_SIMD_BACKLOG.md).

The INT128 sum/avg is the one decimal aggregate with real room for speedup.
DuckDB's `HugeintSumOperation` adds each element with the overflow-checked,
non-inlined `Hugeint::Add`, which is a function call per element (about 5 ns per
element). The Mojo kernel inlines the add and uses several accumulators (about
7.5× faster single-threaded on 50M rows). The int64-backed `DECIMAL` / `BIGINT`
sum is not overridden: it already runs at about 1 cycle per element (limited by
memory), so there is nothing to gain.

Everything else, and any non-FLAT or grouped input, falls back to the original
built-in (its function pointer is saved at load), so results are unchanged.

Nullable columns (FLAT with NULLs) are handled by the kernels instead of falling
back. The aggregate kernels reduce only the valid lanes using a SIMD validity mask
(`select(mask, x, identity)`) and count the valid values (the A1 mask-multiply
model). `min`/`max` leave the state unset when a chunk is all NULL, and `avg`
divides by the valid count. Scalar functions compute over all rows and copy the
input validity to the result. Nullable `sum`/`avg` run about 1.75× faster than
stock. Grouped aggregates still fall back to stock.

## How it works

At `LOAD`, the extension's init changes the built-in catalog function entries in place:

- `ScalarFunctionCatalogEntry::functions` and `AggregateFunctionCatalogEntry::functions`
  are public and mutable, and the binder copies the function by value at bind time, so a
  change made at load is picked up by every later query.
- It saves each original function pointer for the slow-path fallback (so no
  DuckDB-internal operator types are needed). For FLAT, all-valid input it calls a Mojo
  SIMD kernel (linked into the extension) over the raw column buffer.
- `sum`/`avg` have concrete per-type overloads, so the extension overrides `simple_update`
  directly. `min`/`max` are registered as `ANY→ANY` with a bind callback that produces the
  concrete per-type function at bind time. The extension wraps that bind callback: it runs
  the original, then swaps the resolved `f64`/`f32` `simple_update`.
- Aggregate state structs (`SumState`/`AvgState`/`MinMaxState`) are mirrored and guarded by a
  runtime `state_size` check before swapping.

## Fused `sum`/`avg` of transcendentals

An OptimizerExtension rewrites ungrouped `sum(f(col))` / `avg(f(col))` (for `f`
in `sqrt sin cos ln exp log10`, `DOUBLE`) into a custom aggregate that applies `f`
and reduces in one pass, without an intermediate vector. The gain is small: about
1.13× over the already-SIMD two-pass `f` plus `sum`, and about 2.25× over stock.
On CPU the intermediate vector stays in L1 cache and computing the function takes
most of the time, so fusion helps mostly on GPU. Nullable columns are handled with
a mask. Domain errors behave as on the scalar fast path (NaN instead of an error).
Disable with `MOJO_OVERRIDES_NO_FUSE=1`. The same rewrite code is the basis for a
cost-based router that picks the CPU or GPU backend (see the GPU operator).

## `mojo_knn`: blocked multi-query brute-force kNN

DuckDB has no batch kNN function, so a nearest-neighbour search for many queries
written in SQL (a cross join plus a window over M·N distance rows) is very slow.
`mojo_knn` is a table function that does it in one register-tiled CPU SIMD pass.
Each embedding row is loaded once and reused for a batch of queries. Otherwise the
per-pair dot product is limited by L1/L2 reads, and M separate scans load the same
data M times.

```sql
LOAD 'mojo_overrides.duckdb_extension';
SELECT query_rowid, rowid, dist
FROM mojo_knn('emb', 'v', 'queries', 'qv', 10, metric := 'cosine');
```

- Arguments: `emb_table, emb_col, query_table, query_col, k`, plus an optional
  `metric :=`. The `*_col` columns are `FLOAT[K]` ARRAY columns of the same
  dimension. `metric` is `cosine` (default, same as `array_cosine_distance`), `l2`
  (`array_distance`) or `ip` (`array_negative_inner_product`).
- Returns `(query_rowid, rowid, dist)`: 0-based row positions in the query and
  embedding tables, top `k` per query.
- The ranking is exact (recall@k = 1.0 compared to stock). It runs on CPU SIMD with
  no extra dependencies. It is about 89× faster than the cross-join plus window SQL
  (M=64, N=100k, D=128, 1 thread), and the kernel alone is about 2.8× (AVX-512)
  faster than separate optimized scans. Caveat: the `l2` and `cosine` distances use
  the `‖a‖²+‖b‖²−2·dot` identity, so the distance of a vector to itself or to a
  duplicate can come out as about 1e-3 instead of exactly 0. This rounding error
  does not affect the ranking.

## Build and run (pixi)

```bash
pixi run overrides-build                 # build the self-contained mojo_overrides.duckdb_extension
pixi run overrides-bench                 # build, load, print stock-vs-Mojo table (50M rows, 1 thread)
pixi run overrides-bench -- --threads=8 --rows=20000000
pixi run overrides-clean
```

Artifacts land in `build/`:
- `mojo_overrides.duckdb_extension`: the loadable extension (CPP ABI), self-contained:
  the Mojo SIMD kernels are emitted as an object (`capi.o`) and linked straight in, so
  there is no separate kernel lib and no runtime `dlopen` of one.
- `libmojo_simd.dylib|.so`: the same kernels as a standalone C-ABI library, for
  calling the kernels directly or for the source-patch path. The extension does not need it.

Builds against the default pixi env's libduckdb (`$CONDA_PREFIX/include` + `/lib`). Override
with `DUCKDB_INCLUDE` / `DUCKDB_LIB` / `DUCKDB_VERSION` to target another DuckDB tree.

## Run DuckDB's own benchmark suite

You can run the extension through [DuckDB's benchmark suite](https://duckdb.org/docs/current/dev/benchmark)
(`benchmark_runner`) with the shared harness in [`benchmark/`](../../benchmark/).
The `mojo_simd` micro benchmark group in
[`benchmark/sql/mojo_simd/`](../../benchmark/sql/mojo_simd/) covers exactly the overridden functions.

```bash
pixi run bench-build                            # build benchmark_runner (once)
pixi run bench-sql mojo_simd --engines=stock,cpu --threads=1   # the mojo micro group
pixi run bench-sql tpch/sf1/q0[16] --engines=stock,cpu          # official TPC-H
```

The unified driver runs the runner once per engine (stock = no extension, `cpu` =
this extension via `DUCKDB_BENCH_EXTENSION`) and prints a per-benchmark comparison.
The runner build applies a small hook
([`benchmark/drivers/runner_load_extension.patch`](../../benchmark/drivers/runner_load_extension.patch))
to `interpreted_benchmark.cpp` that allows unsigned extensions and `LOAD`s
`$DUCKDB_BENCH_EXTENSION` at init. See [`benchmark/README.md`](../../benchmark/README.md).

## Use from the DuckDB CLI / any libduckdb

The extension is unsigned and uses the CPP ABI, so it must be loaded with unsigned
extensions allowed, into a libduckdb of the exact same version it was built against.
It is self-contained (kernels linked in), so nothing else needs to be on the path:

```bash
duckdb -unsigned -c "
  LOAD '$PWD/extensions/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension';
  SELECT min(i::DOUBLE), sum(i::DOUBLE) FROM range(50000000) t(i);"
```

## Use from the Mojo client

Build the extension first (`pixi run overrides-build`), then load it like any
other extension: allow unsigned extensions when connecting and run `LOAD`:

```mojo
from duckdb import DuckDB
from duckdb.config import Config

var config = Config()
config.set("allow_unsigned_extensions", "true")
var conn = DuckDB.connect(":memory:", config)
_ = conn.execute("LOAD 'extensions/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension'")
```

`LOAD` validates the CPP metadata footer, so a `.so` built for a different DuckDB
version is rejected with a clear error rather than crashing.

### Advanced: install without `LOAD`

The `.so` also exports a plain C entry point
`register_mojo_overrides(duckdb_connection)`. A host that holds a connection but cannot
issue `LOAD` (for example an embedding application) can `dlopen` the `.so` and call it directly. This skips
the loader, so there is no signature or version-footer check; the caller is responsible
for matching the DuckDB version. The library must stay loaded for the process, since the
catalog holds function pointers into it.

## Caveats

- Version and ABI locked: a CPP-ABI extension only loads into the exact DuckDB version it was
  built against (the footer is `--duckdb-version`, default `v1.5.6`). Rebuild for each version.
- Needs the C++ internal headers and an ABI-matched libduckdb, not the stable C extension API.
- State layout: the copied aggregate state structs must match DuckDB's. The `state_size`
  runtime check catches size mismatches but not a change in field order.
- Requires `allow_unsigned_extensions` (set in the benchmark driver; use `-unsigned` in the CLI).
- Mutates shared system-catalog functions for the whole DB instance; applied once at load.
- Correctness: SIMD reductions add and compare in a different order, so floating-point results
  can differ from stock in about the last ULP (verified within 1e-6 relative). The fast path
  skips domain checks (for example `sqrt`/`ln` of negative numbers return NaN instead of an error).
- INT128 overflow: the int128 sum/avg kernel detects signed overflow on each add and falls back
  to the stock (throwing) path on any overflow, so normal results and the overflow exception are
  identical to stock. Integer addition is exact, so a non-overflowing
  reduce returns the same total regardless of accumulator order, but if the data overflows int128
  only on an intermediate prefix (not in the final total), stock throws while the kernel returns
  the mathematically correct total. This can only happen near the ±1.7e38 int128 limit.

## Files

The SIMD kernels themselves live in [`duckdb/kernels`](../../duckdb/kernels) and are
shared with the scalar UDF helpers; this package only adds the C++ override glue.

- `src/capi_shim.mojo`: `@export` C-ABI wrappers over `duckdb.kernels.simd`, emitted as an object and linked into the extension
- `src/mojo_overrides.cpp`: the C++ extension (catalog changes and wrappers, two entry points)
- `bench/benchmark.cpp`: standalone stock-vs-Mojo timing driver
- `build.sh` / `run-benchmark.sh`: invoked by the pixi tasks
