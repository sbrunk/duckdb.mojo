# mojo-kernel-overrides

A DuckDB extension that transparently dispatches selected built-in scalar and
aggregate functions to Mojo SIMD kernels without patching to DuckDB. You can load
it into a normal libduckdb and the rewrites apply for the rest of the session.

## What it overrides

| function(s) | type | mechanism |
|---|---|---|
| `sqrt sin cos ln exp log10` | scalar `DOUBLE→DOUBLE` | swap `function` |
| `sum` `avg` | aggregate `DOUBLE` | swap `simple_update` (ungrouped) |
| `sum` `avg` | aggregate `HUGEINT` + `DECIMAL(19..38)` (INT128-backed) | direct swap (`HUGEINT`) / wrap `bind` (`DECIMAL`), swap `simple_update` (ungrouped) |
| `min` `max` | aggregate `DOUBLE` + `FLOAT` | wrap the `bind` callback, swap resolved `simple_update` |
| `array_distance` `array_inner_product` `array_negative_inner_product` `array_cosine_similarity` `array_cosine_distance` | scalar `FLOAT[]`/`DOUBLE[]` fold | swap `function` |

The array distance/similarity folds are stock serial single-accumulator loops
(`result += x*y`) that can't auto-vectorize (FP reduction). The Mojo kernels use
`W`-wide SIMD accumulators with a 2× unroll to break the dependency chain:
**~7–12× on the raw fold** (768-dim f32: stock ~4.8 GB/s latency-bound → kernel
~52 GB/s memory-bandwidth-bound). End-to-end a `ORDER BY array_cosine_distance …
LIMIT k` brute-force scan is ~1.5–1.7× (the array scan + top-k sort dominate; the
distance is no longer the bottleneck). The three kernels `array_dot` /
`array_l2dist` / `array_cosine_sim` back all five functions — the caller derives
`negative_inner_product` (−x) and `cosine_distance` (1−x). See
[`CPU_SIMD_BACKLOG.md`](CPU_SIMD_BACKLOG.md).

The INT128 sum/avg is the one *decimal* aggregate with real headroom: DuckDB's
`HugeintSumOperation` adds each element via the overflow-checked, non-inlined
`Hugeint::Add` (a function call per element, ~5 ns/elem). The Mojo kernel inlines
the add and uses multiple accumulators (~7.5× single-threaded on 50M rows). The
int64-backed `DECIMAL` / `BIGINT` sum is **not** overridden — it already runs at
~1 cycle/elem (memory-bound), so there is nothing to win.

Everything else, and any non-FLAT / grouped input, falls back to the
original built-in (the original pointer is captured at load), so results are unchanged.

**Nullable columns** (FLAT with NULLs) are handled in place rather than falling
back: the aggregate kernels reduce only the valid lanes via a SIMD validity mask
(`select(mask, x, identity)`) and track the valid count (the A1 mask-multiply
model — `min/max` leave the state unset on an all-NULL chunk, `avg` divides by the
count); scalar functions compute over all rows and copy the input validity to the
result. Nullable `sum`/`avg` run ~1.75× over stock and are now covered instead of
declined. Grouped aggregates still fall back to stock.

## How it works

At `LOAD`, the extension's init mutates the built-in **catalog function entries in place**:

- `ScalarFunctionCatalogEntry::functions` / `AggregateFunctionCatalogEntry::functions`
  are public & mutable, and the binder copies the function **by value** at bind time so a
  mutation done at load is picked up by every later query.
- It captures each original function pointer for the slow-path fallback (so no
  DuckDB-internal operator types are needed) and, for FLAT/all-valid input, calls a Mojo
  SIMD kernel (linked into the extension) over the raw column buffer.
- `sum`/`avg` have concrete per-type overloads → override `simple_update` directly.
  `min`/`max` are registered `ANY→ANY` with a **bind callback** (the concrete per-type
  function is produced at bind time) → the extension **wraps the bind**, runs the original,
  then swaps the resolved `f64`/`f32` `simple_update`.
- Aggregate state structs (`SumState`/`AvgState`/`MinMaxState`) are mirrored and guarded by a
  runtime `state_size` check before swapping.

## Fused `sum`/`avg` of transcendentals

An OptimizerExtension transparently rewrites ungrouped `sum(f(col))` / `avg(f(col))`
(for `f` in `sqrt sin cos ln exp log10`, `DOUBLE`) into a custom aggregate that
applies `f` and reduces in **one pass** — no intermediate vector. The win is
modest (~1.13× over the already-SIMD two-pass `f` + `sum`; ~2.25× over stock
scalar), since on CPU the intermediate stays L1-resident and the transcendental
compute dominates — fusion is mostly a GPU advantage. Nullable columns are handled
(masked); domain errors follow the scalar fast-path caveat (NaN, not a throw).
Disable with `MOJO_OVERRIDES_NO_FUSE=1`. The rewrite infra is the foundation for a
cost-based CPU/GPU backend router (see the GPU operator).

## `mojo_knn` — blocked multi-query brute-force kNN

DuckDB has no batch-kNN primitive, so multi-query nearest-neighbour written in SQL
(cross-join + window over M·N distance rows) is pathological. `mojo_knn` is a table
function that does it in one register-tiled CPU SIMD pass — each embedding row's
bytes are reused across a batch of queries (the per-pair dot is otherwise L1/L2
read-bound), so naive M separate scans waste loads.

```sql
LOAD 'mojo_overrides.duckdb_extension';
SELECT query_rowid, rowid, dist
FROM mojo_knn('emb', 'v', 'queries', 'qv', 10, metric := 'cosine');
```

- Args: `emb_table, emb_col, query_table, query_col, k` (+ `metric :=`).
  `*_col` are `FLOAT[K]` ARRAY columns of equal dim. `metric`: `cosine` (default,
  = `array_cosine_distance`), `l2` (`array_distance`), `ip`
  (`array_negative_inner_product`).
- Returns `(query_rowid, rowid, dist)` — 0-based positional indices into the query
  / embedding tables, top-`k` per query.
- **Exact ranking** (recall@k = 1.0 vs stock), CPU SIMD, dependency-free. ~89×
  over the cross-join+window SQL (M=64, N=100k, D=128, 1 thread); the kernel itself
  is ~2.8× (AVX-512) over naive optimized scans. Caveat: `l2`/`cosine` distances
  use the `‖a‖²+‖b‖²−2·dot` identity, so a self/duplicate pair can read ~1e-3
  instead of exactly 0 (benign cancellation; ranking unaffected).

## Build & run (pixi)

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
- `libmojo_simd.dylib|.so`: the same kernels as a standalone C-ABI lib,
  built for direct kernel use / the source-patch path; not needed by the extension.

Builds against the default pixi env's libduckdb (`$CONDA_PREFIX/include` + `/lib`). Override
with `DUCKDB_INCLUDE` / `DUCKDB_LIB` / `DUCKDB_VERSION` to target another DuckDB tree.

## Run DuckDB's own benchmark suite

You can run the extension through [DuckDB's benchmark suite](https://duckdb.org/docs/current/dev/benchmark)
(`benchmark_runner`) via the consolidated harness in [`benchmark/`](../../benchmark/) —
the committed `mojo_simd` micro group in
[`benchmark/sql/mojo_simd/`](../../benchmark/sql/mojo_simd/) targets the exact overridden ops.

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
extensions allowed, into a libduckdb of the **exact same version** it was built against.
It is self-contained (kernels linked in), so nothing else needs to be on the path:

```bash
duckdb -unsigned -c "
  LOAD '$PWD/extensions/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension';
  SELECT min(i::DOUBLE), sum(i::DOUBLE) FROM range(50000000) t(i);"
```

## Use from the Mojo client

You need to build the extension first (`pixi run overrides-build`),
then load it like any other
extension by allowing unsigned extensions at connect and issuing `LOAD`:

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
issue `LOAD` (e.g. an embedder) can `dlopen` the `.so` and call it directly. This skips
the loader, so there is no signature or version-footer check; the caller is responsible
for matching the DuckDB version. The library must stay loaded for the process, since the
catalog holds function pointers into it.

## Caveats

- **Version/ABI locked**: a CPP-ABI extension only loads into the exact DuckDB version it was
  built against (footer is `--duckdb-version`, default `v1.5.4`). Rebuild per version.
- Needs the C++ internal headers + an ABI-matched libduckdb, not the stable C extension API.
- **State-layout coupling**: the mirrored aggregate state structs must match DuckDB's; the
  `state_size` runtime check guards gross mismatches but not silent field-order changes.
- Requires `allow_unsigned_extensions` (set in the benchmark driver; use `-unsigned` in the CLI).
- Mutates shared system-catalog functions for the whole DB instance; applied once at load.
- **Correctness**: SIMD reductions sum/compare in a different order, so floating-point results
  can differ from stock in the last ~1 ULP (verified within 1e-6 relative). Domain checks on
  the fast path are skipped (e.g. `sqrt`/`ln` of negatives return NaN instead of throwing).
- **INT128 overflow**: the int128 sum/avg kernel detects per-add signed overflow and falls back
  to the stock (throwing) path on any overflow, so normal results and the overflow exception are
  identical to stock. Integer addition is exact, so a non-overflowing
  reduce returns the same total regardless of accumulator order, but if the data overflows int128
  only on an intermediate prefix (not in the final total), stock throws while the kernel returns
  the (mathematically correct) total. Only reachable near the ±1.7e38 int128 limit.

## Files

The SIMD kernels themselves live in [`duckdb/kernels`](../../duckdb/kernels) and are
shared with the scalar UDF helpers; this package only adds the C++ override glue.

- `src/capi_shim.mojo`: `@export` C-ABI wrappers over `duckdb.kernels.simd`, emitted as an object and linked into the extension
- `src/mojo_overrides.cpp`: the C++ extension (catalog mutation + wrappers, two entry points)
- `bench/benchmark.cpp`: standalone stock-vs-Mojo timing driver
- `build.sh` / `run-benchmark.sh`: invoked by the pixi tasks
