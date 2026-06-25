# mojo-gpu-operator

A DuckDB extension that **transparently offloads supported SQL to the GPU**, with
the compute kernels written in **Mojo**. An `OptimizerExtension` recognizes a class
of plan subtrees (aggregate over filter / FK-join / scan) and rewrites them to a
single generic GPU operator; anything it can't translate, or any runtime GPU
error, runs on stock DuckDB CPU. Results are **decimal-exact** vs stock.

A query flows: DuckDB plans it as usual → we recognize a matching subtree → a flat
`RawPlan` is handed to a Mojo planner that makes the GPU-specific decisions →
a generic source operator runs Mojo kernels on resident GPU buffers. C++ is just
DuckDB-ABI glue; the planner and execution logic live in Mojo.

📖 **[DESIGN.md](DESIGN.md)** — how it works, why GPU planning differs from
DuckDB's, the warm/cold pin model, and performance.
🔌 **[src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md)** — the C++↔Mojo wire format.

> **Scope / caveats.** CPP-ABI extension linking DuckDB's internal C++ headers —
> **locked to the exact DuckDB build** it was compiled against (currently
> `v1.5.4`); not part of the conda package. **Validated on Apple (Metal) and
> NVIDIA (RTX 4090, Linux)** — see [DESIGN.md](DESIGN.md#hardware-portability) for
> the Linux/NixOS build notes. Unsigned — load with `-unsigned` /
> `allow_unsigned_extensions`.

## Build

```bash
pixi run gpu-op-build      # -> build/mojo_gpu_operator.duckdb_extension (+ companion kernel dylib)
pixi run gpu-op-clean      # remove build artifacts
```

The Mojo kernels are built as a shared library (`mojo build --emit shared-lib`) so
the Mojo GPU/AsyncRT runtime is linked in, and the C++ extension links that
companion dylib with rpaths — so unlike the SIMD-only `mojo-kernel-overrides`, GPU
support isn't a single self-contained `.so`.

## Use

Load the extension and run **ordinary SQL** — matching queries are auto-routed to
the GPU with no syntax change. `EXPLAIN` shows `GPU_AGG` (or `GPU_COSINE`)
replacing the matched subtree; non-matching queries run on CPU unchanged.

```sql
-- start the CLI with: duckdb -unsigned
LOAD 'packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension';

-- transparently offloaded (no syntax change):
SELECT id, array_cosine_distance(v, [...]::FLOAT[K]) AS d FROM emb ORDER BY d LIMIT 10;
SELECT sum(l_extendedprice * l_discount) FROM lineitem            -- TPC-H Q6
 WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
   AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;
```

**Toggle.** The generic GPU engine is **on by default**. `SET`-style env control:
- `GPU_OP_GENERIC=off` (or `none`) — disable GPU aggregate offload (everything runs
  stock CPU); useful as an A/B baseline.
- `GPU_OP_GENERIC="q3 q5"` — restrict offload to the named query kinds.
- `GPU_OP_SHADOW=1` — log the descriptor classification per aggregate without
  changing execution.

### What's accelerated

One generic engine (the `GPU_AGG` operator) covers a class of **filter + FK-join +
group-by/aggregate** plans; the five TPC-H queries below are instances of it, not
separate code paths. Cosine distance is a separate `GPU_COSINE` operator (also
exposed as the `gpu_cosine(table, column, query)` table function).

**Vector search (kNN) table functions.** The embedding matrix is pinned resident
on the GPU once (cached process-wide, fp16 by default) and reused across queries:

- `gpu_cosine_topk('emb','v', <FLOAT[K]>, k [, precision])` → `(rowid, dist)` —
  exact top-k cosine for a **single** query; only k rows cross PCIe.
- `gpu_cosine_topk_batch('emb','v','queries','qv', k [, precision] [, metric])` →
  `(query_rowid, rowid, dist)` — **true batched** kNN: the query set is a column
  of another table (also `FLOAT[K]`). All M queries are scored in one batched
  kernel that reads the resident N×K matrix **once per query-tile** (not once per
  query) and merges per-query top-k entirely on the GPU, so the dominant matrix-
  read bandwidth is amortized across the whole batch — per-query latency drops
  sharply with M. This is the regime where the GPU most decisively beats a per-
  query index (HNSW cannot use its index for batched queries at all). `precision`
  is `'fp16'` (default) or `'fp32'`/`'exact'`; results are bit-for-bit identical
  to M single-query `gpu_cosine_topk` calls.
  - `metric` is `'cosine'` (default), `'l2'`/`'euclidean'` (= `array_distance`,
    squared euclidean), or `'ip'` (= `array_negative_inner_product`, score
    `-dot`). **Non-cosine metrics run only on the fused tensor-core path**:
    they require `precision='fp16'`, an NVIDIA build, `GPU_OP_TENSORCORE=1`, and
    a supported `K ∈ {384,768,1024,1536}` with `k ≤ 64` — otherwise the call
    errors (the cosine scalar path has no non-cosine implementation; fall back to
    stock DuckDB `array_distance`/`array_negative_inner_product`). The fused MMA
    core is identical for all three metrics; only the per-candidate distance
    epilogue + norm-buffer semantics change. Validated against a scalar CPU
    reference: cosine, IP, and **normalized** L2 match exactly (recall@10 ≥ 0.99,
    0 genuine misses). ⚠️ **Non-normalized L2** is formed as `|q|²+|e|²−2·dot`
    with the fp16-product dot, which suffers catastrophic cancellation and can
    drop genuine neighbors — use normalized embeddings for L2 (the standard
    real-embedding case), or stock DuckDB for non-normalized L2.

| Workload | Operator |
|---|---|
| `array_cosine_distance(col, <const FLOAT[K]>)` | `GPU_COSINE` |
| TPC-H Q1 (grouped aggregation) | `GPU_AGG` |
| TPC-H Q3 (3-way join, high-card group-by) | `GPU_AGG` |
| TPC-H Q5 (6-way join, correlated condition) | `GPU_AGG` |
| TPC-H Q6 (filter + scalar aggregate) | `GPU_AGG` |
| TPC-H Q14 (FK join + aggregate, promo CASE) | `GPU_AGG` |

Acceptance is **strict**: any plan outside the supported class falls back to CPU,
so a mismatch can never produce a wrong result. The supported queries are validated
to match stock DuckDB exactly, including under repeated (warm) execution.

## Layout

- `src/gpu_operator.cpp` — the `OptimizerExtension`, the plan serializer
  (`SerializeMatchedPlan`), the generic `LogicalGpuAgg`/`PhysicalGpuAgg` source
  operator + execution shuttle, the cosine operator/table-function, and the
  extension entry points (DuckDB-ABI glue only).
- `src/descriptor.mojo` — the Mojo planner (`build_descriptor`): fact/dim, strategy,
  expression-program lowering.
- `src/expr_vm.mojo` / `src/segreduce.mojo` — the generic GPU kernels (postfix
  integer VM + segmented N-metric reduction with FK-gather), int128 host reduction.
- `src/gpu_platform.mojo`, `src/raw_plan.h`, `src/raw_plan_tags.mojo` — portability
  constants + the RawPlan tag constants (C++/Mojo in lockstep).
- `build.sh` — `mojo --emit shared-lib` + `clang++` link with rpaths (`.dylib`/macOS,
  `.so`/Linux).
- `bench/` — standalone correctness/de-risk tests and micro-probes, run directly
  with `mojo run` (see Tasks below).

## Tasks

Only build/clean are wrapped as pixi tasks:

```
gpu-op-build / gpu-op-clean        build / clean the extension
```

The `bench/` de-risk tests (end-to-end shuttle tests, kernel-algorithm oracles,
nullable/native-decode tests), the unified-memory allocator probes, and the
pin-resident micro-benchmark are run directly with `mojo run` (the former
`pixi run gpu-op-*` wrappers were removed). The kernel oracles run as-is; the
descriptor C-ABI "shuttle" tests and anything importing the operator's Mojo
modules need the package `src/` on the import path:

```bash
# kernel-algorithm oracles (q1/q3/q5/q6/q14, q3groupby) + the pin-resident bench:
pixi run mojo run packages/mojo-gpu-operator/bench/q6_kernel_test.mojo

# shuttle / nullable / native-decode tests (need -I src):
pixi run mojo run -I packages/mojo-gpu-operator/src \
  packages/mojo-gpu-operator/bench/q6_shuttle_test.mojo
```

TPC-H stock-vs-GPU benchmarking reuses the `mojo-kernel-overrides` runner — point
it at this extension:

```bash
DUCKDB_BENCH_EXTENSION=$PWD/packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension \
  pixi run overrides-bench-runner 'benchmark/tpch/sf1/q(01|03|05|06|14)\.benchmark'
```

## Status

Transparent, decimal-exact GPU execution via one descriptor-driven engine for
TPC-H Q1/Q3/Q5/Q6/Q14, correct under warm/repeated execution; validated on Apple
(Metal) and NVIDIA (RTX 4090). TPC-H sf1 warm vs stock: Apple ~1.6–2.0× on
Q1/Q5/Q14, ~parity Q6, Q3 trails; NVIDIA larger (Q14 ~11×, Q5 ~7×, Q6 ~3.8×,
Q1 ~2×, Q3 ~0.85×).

**Vector search** (`gpu_cosine_topk` / `_batch`, fp16 default): exact GPU kNN
beats CPU exact brute-force ~40–57× warm, and is competitive-to-faster than
vss/HNSW single-query up to ~1M rows while being effectively exact, zero-build,
and half the memory (fp16, recall ≥0.99 with no genuine misses). HNSW pulls
ahead only at many-millions of rows / recall-tolerant, write-once workloads.

**Open frontier:** the cold-pin cost (GPU-direct scan / load-time residency); a
faster high-cardinality group-by (Q3 — NVIDIA GPU hash-aggregate now that 64-bit
atomics are confirmed); a tensor-core GEMM for the batched kNN (the current
batch kernel amortizes the matrix read but isn't compute-optimal at large M).
See [DESIGN.md](DESIGN.md#limitations--open-frontier).
