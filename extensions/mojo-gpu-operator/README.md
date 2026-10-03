# mojo-gpu-operator

A DuckDB extension that offloads supported SQL to the GPU without any change to
the query, with the compute kernels written in Mojo. An `OptimizerExtension`
recognizes a class of plan subtrees (aggregate over filter / FK join / scan) and
rewrites them to a single generic GPU operator. Anything it can't translate, and
any runtime GPU error, runs on stock DuckDB on the CPU. Results match stock
DuckDB exactly, including decimals.

How a query flows: DuckDB plans it as usual, the extension recognizes a matching
subtree, a flat `RawPlan` is handed to a Mojo planner that makes the
GPU-specific decisions, and a generic source operator runs Mojo kernels on GPU
buffers that stay resident between queries. The C++ code only glues into the
DuckDB ABI; the planner and execution logic live in Mojo.

- [DESIGN.md](DESIGN.md): how it works, why GPU planning differs from DuckDB's,
  the warm/cold pin model, and performance.
- [src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md): the wire format between
  C++ and Mojo.

> **Scope and caveats.** This is a CPP-ABI extension that links DuckDB's
> internal C++ headers, so it only works with the exact DuckDB build it was
> compiled against (currently `v1.5.6`). It is not part of the conda package. It
> has been validated on Apple (Metal) and NVIDIA (RTX 4090, Linux). See
> [DESIGN.md](DESIGN.md#hardware-portability) for the Linux/NixOS build notes.
> You need to load the extension with `-unsigned` / `allow_unsigned_extensions`.

## Build

```bash
pixi run gpu-op-build      # -> build/mojo_gpu_operator.duckdb_extension (+ companion kernel dylib)
pixi run gpu-op-clean      # remove build artifacts
```

The Mojo kernels are built as a shared library (`mojo build --emit shared-lib`)
so that the Mojo GPU/AsyncRT runtime is linked in. The C++ extension links that
companion dylib with rpaths. So unlike the SIMD-only `mojo-kernel-overrides`,
the GPU extension is not a single self-contained `.so`.

## Use

Load the extension and run ordinary SQL. Matching queries are routed to the GPU
automatically. `EXPLAIN` shows `GPU_AGG` (or `GPU_COSINE`) in place of the
matched subtree; other queries run on the CPU unchanged.

```sql
-- start the CLI with: duckdb -unsigned
LOAD 'extensions/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension';

-- offloaded without any syntax change:
SELECT id, array_cosine_distance(v, [...]::FLOAT[K]) AS d FROM emb ORDER BY d LIMIT 10;
SELECT sum(l_extendedprice * l_discount) FROM lineitem            -- TPC-H Q6
 WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
   AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;
```

**Toggle.** The generic GPU engine is on by default. It is controlled with
environment variables:
- `GPU_OP_GENERIC=off` (or `none`): disable GPU aggregate offload, so everything
  runs on stock CPU. Useful as an A/B baseline.
- `GPU_OP_GENERIC="q3 q5"`: restrict offload to the named query kinds.
- `GPU_OP_SHADOW=1`: log the descriptor classification for each aggregate
  without changing execution.

**Tiering (GPU, then CPU SIMD, then stock).** `GPU_OP_OVERRIDES=1` also installs
the dependency-free `mojo-kernel-overrides` CPU SIMD kernels (array distance,
nullable sum/avg/min/max, fused `sum/avg(transcendental)`, `mojo_knn`), so a
shape the GPU declines still runs faster than stock. `GPU_OP_MIN_ROWS=N`
(default 50000; `0` disables) sets the GPU/CPU crossover: matched shapes with
fewer than `N` input rows go to the CPU tier, because there the GPU pin and
transfer overhead would make them slower. Set both flags to get the full
three-tier dispatch.

### What's accelerated

One generic engine (the `GPU_AGG` operator) covers a class of filter + FK join +
group-by/aggregate plans. The five TPC-H queries below are instances of it, not
separate code paths. Cosine distance is a separate `GPU_COSINE` operator (also
exposed as the `gpu_cosine(table, column, query)` table function).

**Vector search (kNN) table functions.** The embedding matrix is uploaded to the
GPU once (cached for the whole process, fp16 by default) and reused across
queries:

- `gpu_cosine_topk('emb','v', <FLOAT[K]>, k [, precision])` returns
  `(rowid, dist)`: exact top-k cosine for a single query. Only k rows cross PCIe.
- `gpu_cosine_topk_batch('emb','v','queries','qv', k [, precision] [, metric])`
  returns `(query_rowid, rowid, dist)`: batched kNN, where the query set is a
  column of another table (also `FLOAT[K]`). All M queries are scored in one
  batched kernel that reads the resident N×K matrix once per query tile (not once
  per query) and merges the per-query top-k entirely on the GPU. Reading the
  matrix is the dominant cost, and it is shared across the whole batch, so
  per-query latency drops sharply as M grows. This is where the GPU has the
  biggest advantage over a per-query index (HNSW cannot use its index for batched
  queries at all). `precision` is `'fp16'` (default) or `'fp32'`/`'exact'`;
  results are bit-for-bit identical to M single-query `gpu_cosine_topk` calls.
  - `metric` is `'cosine'` (default), `'l2'`/`'euclidean'` (same as
    `array_distance`, squared euclidean), or `'ip'` (same as
    `array_negative_inner_product`, score `-dot`). Non-cosine metrics only run
    on the fused tensor-core path: they require `precision='fp16'`, an NVIDIA
    build, `GPU_OP_TENSORCORE=1`, and a supported `K ∈ {384,768,1024,1536}` with
    `k ≤ 64`. Otherwise the call returns an error, because the scalar cosine path
    has no non-cosine implementation; use stock DuckDB
    `array_distance`/`array_negative_inner_product` instead. The fused MMA core
    is the same for all three metrics; only the per-candidate distance step and
    the meaning of the norm buffer change. Validated against a scalar CPU
    reference: cosine, IP, and normalized L2 match exactly (recall@10 ≥ 0.99,
    0 genuine misses). Non-normalized L2 is computed as `|q|²+|e|²−2·dot` with
    the fp16 dot product, which suffers catastrophic cancellation and can drop
    genuine neighbors. Use normalized embeddings for L2 (the usual case for real
    embeddings), or stock DuckDB for non-normalized L2.

| Workload | Operator |
|---|---|
| `array_cosine_distance(col, <const FLOAT[K]>)` | `GPU_COSINE` |
| TPC-H Q1 (grouped aggregation) | `GPU_AGG` |
| TPC-H Q3 (3-way join, high-card group-by) | `GPU_AGG` |
| TPC-H Q5 (6-way join, correlated condition) | `GPU_AGG` |
| TPC-H Q6 (filter + scalar aggregate) | `GPU_AGG` |
| TPC-H Q14 (FK join + aggregate, promo CASE) | `GPU_AGG` |

Matching is strict: any plan outside the supported class falls back to the CPU,
so a mismatch can never produce a wrong result. The supported queries are
validated to match stock DuckDB exactly, including under repeated (warm)
execution.

## Layout

- `src/gpu_operator.cpp`: the `OptimizerExtension`, the plan serializer
  (`SerializeMatchedPlan`), the generic `LogicalGpuAgg`/`PhysicalGpuAgg` source
  operator and execution shuttle, the cosine operator and table function, and
  the extension entry points (DuckDB ABI glue only).
- `src/descriptor.mojo`: the Mojo planner (`build_descriptor`): fact/dimension
  choice, strategy, and lowering of expressions to programs.
- `src/expr_vm.mojo` / `src/segreduce.mojo`: the generic GPU kernels (a postfix
  integer VM and a segmented N-metric reduction with FK gather) and the int128
  host reduction.
- `src/gpu_platform.mojo`, `src/raw_plan.h`, `src/raw_plan_tags.mojo`: platform
  constants and the RawPlan tag constants (C++ and Mojo must stay in sync).

## Tasks

```
gpu-op-build / gpu-op-clean        build / clean the extension
gpu-op-test [name]                 build, then run the GPU tests in bench/ (needs a GPU)
```

`pixi run -e gpu gpu-op-test` runs the tests in `bench/`: unit tests for the
descriptor, expression VM, segmented reduction, native decode and column pool;
shuttle tests that drive each query class through the C-ABI entry points
without DuckDB; SQL tests that compare the loaded extension with stock DuckDB;
and the vector-search tests. Tests that need NVIDIA (the float64 kernels and
the non-cosine tensor-core metrics) are skipped on macOS. Pass part of a test
name to run a subset, for example `pixi run -e gpu gpu-op-test q6_shuttle`.

To run a single test directly, put `src/` on the import path:

```bash
pixi run -e gpu mojo run -I extensions/mojo-gpu-operator/src \
  extensions/mojo-gpu-operator/bench/q6_shuttle_test.mojo
```

TPC-H benchmarks of stock vs GPU use the consolidated harness
([`benchmark/README.md`](../../benchmark/README.md)); its `gpu` engine loads this
extension:

```bash
pixi run bench-build                                       # once
pixi run bench-sql tpch/sf1/q0[13456] --engines=stock,cpu,gpu
pixi run bench-sql gpu_knn --by-suffix                     # vector-search top-k
pixi run bench-knn                                         # Mojo latency+recall harness
```

## Status

TPC-H Q1/Q3/Q5/Q6/Q14 run on the GPU through one descriptor-driven engine, with
no query changes and exact decimal results, and stay correct under warm/repeated
execution. Validated on Apple (Metal) and NVIDIA (RTX 4090). TPC-H sf1, warm, vs
stock: on Apple about 1.6 to 2.0× on Q1/Q5/Q14, about the same on Q6, and slower
on Q3. The NVIDIA gains are larger (Q14 ~11×, Q5 ~7×, Q6 ~3.8×, Q1 ~2×,
Q3 ~0.85×).

Vector search (`gpu_cosine_topk` / `_batch`, fp16 by default): exact GPU kNN is
about 40 to 57× faster than exact CPU brute force (warm). Compared to vss/HNSW
single-query search it is about as fast or faster up to ~1M rows, while being
effectively exact, needing no index build, and using half the memory (fp16,
recall ≥0.99 with no genuine misses). HNSW only pulls ahead at many millions of
rows, for workloads that tolerate lower recall and write once.

Open problems: the cost of the cold pin (GPU-direct scan or keeping data resident
from load time); a faster high-cardinality group-by (Q3; a GPU hash aggregate on
NVIDIA, now that 64-bit atomics are confirmed there); a tensor-core GEMM for the
batched kNN (the current batch kernel shares the matrix read across queries but
does not use compute optimally at large M). See
[DESIGN.md](DESIGN.md#limitations-and-open-problems).
