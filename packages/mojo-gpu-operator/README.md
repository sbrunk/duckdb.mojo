# mojo-gpu-operator

A DuckDB extension that **transparently offloads supported SQL to the GPU**, with the
compute kernels written in **Mojo**. An `OptimizerExtension` matches eligible plan
shapes and rewrites them to custom GPU `PhysicalOperator`s (translate-or-fallback:
anything that doesn't match, or any runtime GPU error, runs on stock DuckDB CPU).
Results are **decimal-exact** vs stock. It extends the hybrid C++/Mojo build model of
[`../mojo-kernel-overrides`](../mojo-kernel-overrides) from *function* overrides up to
*plan/operator* rewrites.

> **Scope / caveats.** CPP-ABI extension linking DuckDB's internal C++ headers — **locked
> to the exact DuckDB build** it was compiled against (currently `v1.5.3`); not part of the
> conda package. **macOS / Apple GPU only** as written. Unsigned, so load with
> `-unsigned` / `allow_unsigned_extensions`.

## Build

```bash
pixi run gpu-op-build      # -> build/mojo_gpu_operator.duckdb_extension (+ companion kernel dylib)
pixi run gpu-op-clean      # remove build artifacts
```

The Mojo kernels are built as a shared library (`mojo build --emit shared-lib`) so the Mojo
GPU/AsyncRT runtime is linked in, and the C++ extension links that companion dylib with
rpaths — so unlike the SIMD-only `mojo-kernel-overrides`, GPU support isn't a single
self-contained `.so`.

## Use

Load the extension and run **ordinary SQL** — matching queries are auto-routed to the GPU
(no syntax change). `EXPLAIN` shows the GPU operator (e.g. `GPU_Q6`) replacing the matched
subtree; non-matching queries run on CPU unchanged.

```sql
LOAD 'packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension';

-- transparently offloaded:
SELECT id, array_cosine_distance(v, [...]::FLOAT[K]) AS d FROM emb ORDER BY d LIMIT 10;
SELECT sum(l_extendedprice * l_discount) FROM lineitem
 WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
   AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;          -- TPC-H Q6
```

### Transparently supported queries

| Workload | Transparent operator | Equivalent table function |
|---|---|---|
| `array_cosine_distance(col, <const FLOAT[K]>)` | `GPU_COSINE` | `gpu_cosine(table, column, query)` |
| TPC-H Q1 (grouped aggregation) | `GPU_Q1` | — |
| TPC-H Q3 (3-way join, top-N) | `GPU_Q3` | `gpu_q3(customer, orders, lineitem, mktsegment, o_cutoff, l_cutoff)` |
| TPC-H Q5 (6-way join, correlated) | `GPU_Q5` | `gpu_q5(customer, orders, lineitem, supplier, nation, region, region_name, o_lo, o_hi)` |
| TPC-H Q6 (filter + aggregate) | `GPU_Q6` | `gpu_q6(table, ship_lo, ship_hi, disc_lo, disc_hi, qty_hi)` |
| TPC-H Q14 (FK join + aggregate) | `GPU_Q14` | — |

The TPC-H matchers are intentionally **strict**: any deviation from the recognized shape
(extra predicates, different aggregates, etc.) falls back to CPU, so a mismatch can never
produce a wrong result. The TPC-H paths are validated to match stock DuckDB exactly.

## How it works

- **Plan rewrite** — `OptimizerExtension` (with a `pre_optimize` hook that disables
  `COMPRESSED_MATERIALIZATION` so VARCHAR group keys stay raw) matches plan subtrees and
  replaces them with `LogicalExtensionOperator`s whose `CreatePlan` emits custom
  `PhysicalOperator`s. The operators also register as table functions (`gpu_*`).
- **Mojo kernels** — exposed to C++ over a flat C-ABI; the C++ side links the kernel object.
  A single `DeviceContext` is created once at extension load and shared across all kernels.
- **Decimal exactness** — DuckDB `DECIMAL(15,2)` is int64 (scale 2), `DATE` is int32. Kernels
  do pure-integer arithmetic; per-row products and per-block `warp.sum` partials fit int64,
  and only the cross-block reduction is done in int128 on the host — so sums match DuckDB
  bit-for-bit (averages match to double rounding).
- **Joins** — foreign-key joins build a hash table on the small (dimension) side on the host,
  upload it, and **probe on the GPU** over the large fact side.
- **High-cardinality group-by** — done by sorting on the group key (in DuckDB, at load) plus a
  GPU segmented reduction (one warp per segment); Apple GPUs lack 64-bit atomics, so a hash
  aggregation into a dense accumulator isn't available.

### The pin (data load), and the warm/cold trade-off

Before running on the GPU, a query **pins** its columns: it materializes them from DuckDB and
uploads them into resident `DeviceBuffer`s (cached per table for the process lifetime, using
pre-sized pinned host staging). Consequences:

- **Warm** (data already pinned): the GPU kernel runs against resident buffers — fast.
- **Cold** (first query on a table): you pay a one-time CPU materialization + upload, which is
  much costlier than the kernel; a single cold query does **not** beat stock DuckDB.

So the win is for **warm / repeated** query workloads. Eliminating the cold cost would require a
GPU-direct scan (reading storage into device memory without the CPU round-trip), which has no
Mojo/Metal columnar decoder today.

### Why no unified-memory allocator

A tempting idea on Apple Silicon (shared CPU/GPU DRAM) is a custom `DBConfig.allocator` that
makes DuckDB's column buffers *be* GPU memory, deleting the upload. The `bench/` probes show
this isn't reachable: a scanned column *does* flow through `DBConfig.allocator`, but the Mojo
GPU API exposes no buffer that is simultaneously a stable CPU-writable pointer (for DuckDB to
fill) and a valid GPU kernel argument. So the extension uses the pin-resident route instead —
the same choice Sirius makes (it never installs a DuckDB allocator either).

## Layout

- `src/gpu_kernels.mojo` — Mojo GPU kernels + the C-ABI pin/query engines (shared `DeviceContext`,
  resident + pinned-staging buffers, int128 host reduction).
- `src/gpu_operator.cpp` — the `OptimizerExtension`, the per-query matchers, the
  `LogicalExtensionOperator`/`PhysicalOperator` subclasses, the `gpu_*` table functions, and the
  extension entry points.
- `build.sh` — `mojo --emit shared-lib` + `clang++` link with rpaths.
- `bench/` — standalone correctness/de-risk tests and micro-probes (each has a `pixi run gpu-op-*` task).

## Tasks

```
gpu-op-build / gpu-op-clean        build / clean the extension
gpu-op-bench                       pin-resident vs CPU micro-benchmark (cosine, K sweep)
gpu-op-q6test / q1test / q3groupby / q5test / q14test   standalone exact-vs-CPU kernel tests
gpu-op-probe-alloc / probe-route   the unified-memory allocator probes
```

## Status / next

Transparent, decimal-exact GPU execution for `array_cosine_distance` and TPC-H Q1/Q3/Q5/Q6/Q14
(filter, grouped aggregation, and 1-/3-/6-way joins). Open frontier: removing the cold-pin cost
(GPU-direct scan / load-time residency), a fully-GPU sort, and a general multi-join planner rather
than per-query matchers.
