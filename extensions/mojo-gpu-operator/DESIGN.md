# Design of mojo-gpu-operator

How the GPU offload engine works, why it plans differently from DuckDB, and how
the two interact. For build and usage see [README.md](README.md); for the exact
wire format between C++ and Mojo see
[src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md).

## Mental model: DuckDB plans, we are a peephole optimizer with a GPU backend

We did not build a query planner that competes with DuckDB's. DuckDB does all
the real planning: parse, bind, cardinality estimation, join order, filter
pushdown, expression binding, type/decimal resolution. Our extension is a
narrow rewrite that runs after the optimizer and:

1. recognizes a strict, allow-listed class of already-optimized plan subtrees
   (an aggregate over a filter/FK-join/scan tree),
2. makes the handful of decisions a GPU needs that DuckDB's planner never makes,
   and
3. replaces that subtree with a single GPU source operator, leaving the rest of
   the plan (ORDER BY, LIMIT, outer projections) as ordinary DuckDB operators.

Anything outside the recognized class, and any runtime GPU error, runs on stock
DuckDB. This is translate-or-fallback, not cost-based planning. There is no
per-operator cost comparison between GPU and CPU: we accept what we can prove
correct and decline everything else, so a mismatch can never produce a wrong
result.

The C++ code is only glue to the DuckDB ABI; all planner and execution logic
lives in Mojo.

## How it hooks in and interacts with DuckDB

The extension is an `OptimizerExtension` ([src/gpu_operator.cpp](src/gpu_operator.cpp))
with two hooks plus a custom operator:

- `GpuPreOptimize` (before DuckDB's optimizer) changes exactly one setting: it
  disables `COMPRESSED_MATERIALIZATION`, so group keys stay raw `VARCHAR`
  instead of being dictionary-compressed into `__internal_compress` projections
  that a GPU source operator can't reproduce. The setting is restored right
  after, so later queries are unaffected. (This is not GPU planning; it only
  keeps the plan in a shape we can translate.)
- `OptimizeNode`/`TryRouteGeneric` (after DuckDB's optimizer) walks the
  already-optimized logical plan bottom-up. When it finds a recognized aggregate
  subtree, it serializes the subtree (`SerializeMatchedPlan` produces a flat
  `RawPlan` tape) and hands it to the Mojo planner `build_descriptor`. If the
  planner accepts it, the subtree is replaced with a `LogicalGpuAgg` (a
  `LogicalExtensionOperator`).
- `PhysicalGpuAgg` is the physical lowering of that operator. It is a source
  (`IsSource()==true`, no children) and bypasses DuckDB's physical
  scan/join/aggregate pipeline for the subtree. To get its input it issues a
  nested `Connection::Query` back into DuckDB. So DuckDB is both the thing we
  replace and the thing that feeds the GPU: its scan and pushed-down filters
  materialize the columns we upload.

```
SQL ─► DuckDB bind + optimize (join order, pushdown, cardinality, types)
        │  [pre-hook: disable COMPRESSED_MATERIALIZATION]
        ▼
   optimized logical plan ── OptimizeNode walks bottom-up
        │
        ├─ no match ─────────────────────────► stock DuckDB execution
        │ match: Aggregate→(Filter/Join/Get)
        ▼
   SerializeMatchedPlan ─► RawPlan tape ─► Mojo build_descriptor
        (GPU decisions: fact/dim, strategy, programs, residency signature)
        ▼
   LogicalGpuAgg  (replaces the subtree; parent ORDER BY / LIMIT / projection untouched)
        ▼  DuckDB physical planning
   PhysicalGpuAgg (SOURCE) ─► nested Connection::Query feeds columns
        ─► pin/upload (cold) or resident reuse (warm) ─► GPU kernel ─► result chunks
        ▼
   consumed by the stock DuckDB operators above it
```

Because the optimizer hook runs after cardinality estimation, we can read
DuckDB's `estimated_cardinality` and `table_filters` directly from the plan and
reuse them for GPU decisions.

## What we reuse from DuckDB

| Concern | Who decides | Note |
|---|---|---|
| Join order / algorithm | DuckDB | we read the resulting join tree |
| Filter pushdown | DuckDB | filters arrive on the `LogicalGet` as `table_filters` |
| Cardinality estimates | DuckDB | used to pick the fact table |
| Expression / type / decimal binding | DuckDB | we read bound exprs + `DecimalType::GetScale` |
| Input materialization | DuckDB | the source op's nested `Connection::Query` (its scan + filters feed the GPU) |
| Everything above the matched subtree | DuckDB | TOP_N / LIMIT / outer projections run stock on our output |

## Where GPU planning differs

These decisions live in the Mojo `build_descriptor`
([src/descriptor.mojo](src/descriptor.mojo)). They exist only because the target
is a SIMT device whose cost is dominated by data transfer and which (on Apple)
has no 64-bit atomics. A CPU planner needs none of them.

**1. Fact vs. dimension identification.** DuckDB's joins are pipelined and don't
single out a "big" table. A GPU kernel has to pick one streaming side (the fact
table, one thread per row) and treat the others as lookups. The fact table is the
`LogicalGet` with the largest `estimated_cardinality`. Every other GET must attach
through a resolvable FK equi-join edge, resolved iteratively so snowflake schemas
work (Q5's customer to supplier to nation).

**2. Execution strategy, chosen by GPU limits and not only by the data.** DuckDB
always uses a radix-partitioned hash aggregate. We pick one of four strategies,
based on what the GPU can and can't do (`segreduce.mojo`):
- UNGROUPED (0 keys): one global reduction.
- DENSE_GROUP (small key space: Q1 returnflag×linestatus, Q5's ≤25 nations):
  private accumulators per warp plus `warp.sum`, no atomics.
- SORT_SEGREDUCE (high-cardinality integer key: Q3 orderkey): sort first (in
  DuckDB, through an `ORDER BY` added to the materialize query), then one warp per
  contiguous segment. This strategy exists only because Apple GPUs have no 64-bit
  atomics. We can't hash-aggregate into 64-bit accumulators, so we use sort plus
  segmented reduction instead.
- HASH_GROUP: needs atomics; on Apple it is rewritten to SORT_SEGREDUCE.

This is the clearest case where the GPU needs a different plan: the same
GROUP BY maps to a different physical strategy only because of an atomics
constraint that a CPU planner never sees.

**3. Residency and materialization planning (the "pin").** DuckDB's streaming
morsel model has no equivalent. On a GPU the dominant cost is the host-to-device
transfer, so the plan decides which columns to pull once, upload to device
memory, and cache across queries (the process-global resident cache `_pin2`,
keyed by a signature). The cost model is dominated by data movement, not CPU
cycles; see "Execution and the pin" below.

**4. Lowering predicates and expressions to a form the GPU can evaluate.**
DuckDB's `ExpressionExecutor` interprets bound expression trees on vectors. We
can't ship that to the GPU, so the planner compiles the filter and aggregate
arguments into a small postfix integer program that `expr_vm` runs
([src/expr_vm.mojo](src/expr_vm.mojo)):
`LOAD_COL / PUSH_CONST / ADD / SUB / MUL / SELECT / OP_LOAD_DIM / OP_EQ`.

> **Kernels specialized at compile time (instead of runtime JIT).** The
> interpreter is the general fallback. For the recognized kinds (Q1/Q6/Q14/Q5)
> the per-row program is instead compiled into an evaluator with no stack and no
> branches that keeps its values in registers, using Mojo `@parameter`
> specialization (`seg_*_kernel_q*` + `eval_program_fast` in
> [src/segreduce.mojo](src/segreduce.mojo)). It is dispatched by `desc.kind` from
> the single warm/cold `_assemble` call site. cuDF needs a runtime `nvrtc` JIT
> (`AST_JIT`) for this; Mojo does it at compile time, portable to NVIDIA, Apple
> and AMD with no runtime compiler. Results are bit-identical to the interpreter
> (asserted in `bench/expr_comptime_probe.mojo`). Measured at the kernel level:
> 4.7× (Q1) / 1.8× (Q6) on Apple M3 Max and 17.9× (Q1) / 5.5× (Q6) on RTX 4090.
> The wide SIMT GPU pays more for the interpreter's branch divergence and its
> per-op reads of the program from global memory. Any unrecognized shape runs
> the `expr_vm` path unchanged, so a new kind of expression can never give a
> wrong result.

Two GPU-specific lowerings follow from this:
- Joins become gathers. TPC-H foreign keys point at dense surrogate primary
  keys, so a join lowers to a dense array gather on the GPU,
  `OP_LOAD_DIM(dim[fact_key[row]])`, instead of a hash probe.
- Dimension folding on the host. When a dimension joins another dimension rather
  than the fact table (Q3 customer to orders), or there is a correlated condition
  between two dimensions (Q5 `c_nationkey=s_nationkey`, handled by `OP_EQ` over
  two gathers), it can't be a single-level GPU gather. The planner folds it on
  the host into the lookup arrays keyed by the fact table before launch.

**5. Exactness planning.** To stay bit-exact for decimals without 128-bit GPU
arithmetic, per-row products and per-block/segment `warp.sum` partials stay int64
(they fit), and only the reduction across blocks is done in int128 on the host.
Sums match DuckDB bit for bit; averages match up to double rounding.

### Side-by-side comparison

| | DuckDB planner | this GPU layer |
|---|---|---|
| Scope | whole query, cost-based | recognize one aggregate sub-shape, rule-based allow-list |
| Picks join order | yes | no (reads DuckDB's) |
| Streaming vs probe side | n/a (symmetric pipeline) | must pick (fact = max cardinality) |
| Group-by physical form | always radix hash agg | 4 strategies by atomics/cardinality |
| Residency / transfer | no concept | the pin; signature-keyed device cache |
| Cost driver | CPU cycles / bandwidth | host/device transfer (cold), kernel (warm) |
| Expression eval | vectorized interpreter | compiled to a postfix GPU program |
| Unsupported input | n/a | falls back to stock DuckDB |

## The C++ / Mojo boundary (RawPlan)

C++ only does the parts that need DuckDB's C++ ABI: the optimizer hooks, walking
the plan, the operator subclasses, the nested query, and filling result chunks.
It serializes the matched subtree into a flat int64 tape plus a string blob
(`SerializeMatchedPlan`), so no DuckDB types cross over. Mojo owns the rest:
`build_descriptor` (the planner), the execution shuttle
(`materialize_sql`/`feed_column`/`pin_finalize`), the kernels, and the resident
cache. Full wire format: [src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md).

## Execution and the pin (warm/cold)

A query pins its columns: it materializes them from DuckDB and uploads them into
resident `DeviceBuffer`s, cached for the whole process under a signature that
includes the fact table, the projected columns, and the filter constants (so a
warm hit means the predicate is identical).

- Cold (first use of a table/predicate): pays a one-time materialize and upload,
  which costs much more than the kernel. A single cold query does not beat stock
  DuckDB.
- Warm (repeat): `pin_begin` reports WARM, C++ skips feeding columns, and
  `pin_finalize` re-runs the kernel on the resident device buffers with no host
  data and no new upload (`segreduce_upload` once, then `segreduce_run` per call).

So the gains come from warm, repeated workloads. The earlier implementation was
only correct on the cold (single-shot) path and returned wrong results on warm
runs. The fix is the resident `_pin2` cache plus a shared cold/warm `assemble`,
so the two paths cannot diverge.

### Bounded resident pin cache with LRU eviction

Pins are only a cache, so the resident caches can be bounded and evicted in LRU
order without affecting results. Eviction only sends the next use back onto the
cold rebuild path (materialize and upload again: slower, same answer). Sirius
closes this gap with cuCascade tiered memory and a downgrade executor. We close
it for the kNN embedding pins, which are the largest GPU residents (~3 GB at
1M×768) and the ones that used to leak VRAM until OOM when many matrices or
columns were pinned.

The kNN pin registry ([src/gpu_operator.cpp](src/gpu_operator.cpp), `ResidentPin`)
caps total resident bytes at `GPU_OP_PIN_BUDGET_MB` (default 4096 MB; `0` means
unbounded, the old behavior). Each entry tracks its approximate VRAM footprint,
a monotonic last-use tick (for LRU), and an in-use reference count. When a new
pin is added, the registry evicts the least recently used evictable entry until
the new one fits, freeing its `DeviceBuffer`s through the Mojo
`mojo_gpu_pin_free`/`_f16` entry points. If the device allocation still runs out
of memory, it evicts and retries (like Sirius's OOM retry). It only fails once
nothing more can be evicted, and then it fails with a SQL error, not a crash.

The one risk with eviction is freeing a buffer that a query is still reading. A
`gpu_cosine*` `Bind` fetches the handle while holding the registry lock but runs
the GPU query after releasing it, so a concurrent eviction could otherwise cause
a use-after-free. An RAII `PinLease` (returned by `EnsurePinned*`, which
increments the reference count under the lock) keeps the entry from being evicted
for the lifetime of the query. LRU eviction and `gpu_unpin` skip entries with a
reference count above 0 and report them as `skipped_in_use`. Explicit control is
exposed as table functions in the same style as the `gpu_cosine*` family:
`gpu_pin_status()` (resident pins, bytes, last-use age, in-use, budget),
`gpu_unpin(key)` / `gpu_unpin_all()` (free now), and
`gpu_pin_table(table, column [, precision])` (pin a column ahead of time so the
first query is warm). Covered by `bench/pin_evict_test.sh` (run directly:
`bash extensions/mojo-gpu-operator/bench/pin_evict_test.sh`). The test pins past
a small budget, checks the eviction through `gpu_pin_status`, then re-runs an
evicted kNN query and checks that the distance distribution of the cold rebuild
is identical to stock `array_cosine_distance`.

The aggregate `_pin2` cache (TPC-H column buffers) is not bounded yet. Its
entries are much smaller (KB to MB, not GB), it lives on the Mojo side
(`GpuPinned` + `SegResident`), and bounding it requires tracking bytes in Mojo
and a teardown path for `SegResident`/`GpuPinned`. It is the obvious next step,
and eviction is safe for the same reason: a missing signature makes `pin_begin`
return COLD and `_pin_finalize_*` rebuilds. Spilling to host memory (keeping a
host copy of an evicted pin and promoting it back without materializing again
from DuckDB) is deferred. It fits best on Apple unified memory, where the host
copy is nearly free, but the current kNN pin frees the host buffer right after
upload (`mojo_gpu_pin*` `h16.free()` / the pageable `enqueue_copy`). A host tier
would need new retained host storage and a promote path, which is out of scope
for now. The bound that ships (budget, LRU, OOM retry) already removes the
unbounded VRAM growth.

### Why no unified-memory allocator

An appealing idea on Apple Silicon (where CPU and GPU share DRAM) is a custom
`DBConfig.allocator` that makes DuckDB's column buffers GPU memory directly,
removing the upload. The `bench/` probes show this is not possible: a scanned
column does go through `DBConfig.allocator`, but the Mojo GPU API has no buffer
that is both a stable CPU-writable pointer (for DuckDB to fill) and a valid GPU
kernel argument. Hence the pin-resident approach, which is also what Sirius does.

## Performance characteristics

TPC-H sf1, warm, results verified against stock (benchmark_runner medians,
1 cold + 5 hot runs, `Verify()` on every hot run):

| query | RTX 4090 GPU vs stock | Apple M3 Max GPU vs stock |
|---|---|---|
| Q14 (FK join + promo CASE) | 0.64 ms vs 10.9 ms (16.9×) | 2.23 ms vs 6.92 ms (3.1×) |
| Q6  (filter + scalar sum)  | 0.37 ms vs 2.9 ms (7.9×)  | 2.02 ms vs 2.45 ms (1.2×) |
| Q5  (6-way join, grouped)  | 1.59 ms vs 11.4 ms (7.2×) | 5.23 ms vs 9.74 ms (1.9×) |
| Q1  (grouped, 8 aggregates)| 2.04 ms vs 12.6 ms (6.2×) | 5.49 ms vs 14.5 ms (2.6×) |

The GPU wins where there is real per-row or join work and a small output. On the
discrete GPU the difference between warm and cold is large: the first run on
NVIDIA, including GPU init and JIT, takes ~1.6 s for Q1 and ~0.3 to 0.5 s for the
others, after which it drops to the warm numbers above.

> **Measure warm performance with benchmark_runner, not the interactive CLI.**
> The DuckDB CLI cannot measure the warm case. Re-running an identical query hits
> DuckDB's result handling (it returns instantly without executing again), and
> changing a filter constant to avoid that produces a cold signature (the pin key
> includes filter constants). Both make the GPU look 50 to 170× slower, when they
> actually measure the cold first run plus init. benchmark_runner does proper
> cold and hot runs and verifies results, and is the source of the numbers above.
> A related false alarm: in an interactive CLI session a repeated grouped query
> can appear to return empty rows. This was confirmed to be a CLI rendering
> artifact, not an operator bug: the source operator allocates fresh state for
> each execution, and the warm finalize re-runs the kernel and rebuilds the full
> result every time. benchmark_runner `Verify()` passed on all 10 hot grouped-Q1
> executions per platform, and the CLI with piped stdin returns byte-identical
> full rows on every repeat.

Q3 is the exception and is now kept on the CPU (see the cost heuristic below).
It has light per-row work and a high-cardinality output (~1.5M groups), which
DuckDB's multithreaded join and hash aggregate handle better. Measured GPU speed
relative to stock was 0.87× at sf1 and 0.54× at sf10. The gap grows with scale,
because the single-threaded GPU source operator scans in roughly linear time
while the 16-thread stock engine has headroom. A better GPU algorithm (a hash
aggregate, which is implemented) doesn't change that; the limit comes from the
shape of the query, not the algorithm.

### Vector search (kNN): where the GPU does best
`gpu_cosine_topk` / `_batch` compute exact top-k cosine over an embedding matrix
that stays resident on the GPU (pinned once, fp16 by default). Measured on the
RTX 4090 (clustered unit-norm embeddings, warm):
- Against exact CPU brute force: about 40 to 57× faster (the fair comparison).
  The cold pin pays for itself after about 12 queries.
- Against DuckDB's vss/HNSW: about as fast or faster for single queries up to
  ~1M rows (fp16: 1M×384 ~1.3 ms, 1M×768 ~2.1 ms), while being exact, needing no
  index build, and using half the index memory. fp16 recall@10 is ≥0.99 with zero
  genuine misses (values below 1.0 come from reordering of floating-point ties at
  the boundary). HNSW only pulls ahead at many millions of rows, for workloads
  that tolerate lower recall and write once and query many times. Its query cost
  grows sublinearly, ours is an O(N) scan, and the resident pin is limited by VRAM
  (~3 GB at 1M×768).
- Single-query kNN is bandwidth-optimal (~912 GB/s, one read of the matrix); it
  was the batched path that needed work. For batched queries an optional fused
  matmul path (`GPU_OP_TENSORCORE`, off by default) replaces the scalar dot
  product per (row, query) with a tiled MMA `queries·embᵀ` combined with a
  streaming per-query top-k. The M×N similarity matrix is never materialized:
  only the embeddings are read and only M×k results are written. MMA makes the
  per-query compute very cheap, so the kernel becomes bound by reading the
  matrix, and that single read is shared by the whole batch. There are two
  backends with the same fused structure:
  - NVIDIA tensor cores ([src/tc_knn.mojo](src/tc_knn.mojo), m16n8k8 fp16 to
    fp32, `transpose_b`): up to ~60× per query at large batch sizes (M=1000 over
    1M×768), bound by HBM reads at ~69% of peak, recall@10 = 1.0 (exact ids).
  - Apple Silicon 8×8 `simdgroup_matrix`
    ([src/tc_knn_apple.mojo](src/tc_knn_apple.mojo), M1 to M4): ~3 to 7× per
    query on an M3 Max, bound by unified memory bandwidth, recall@10 = 1.0.
    (Apple's 16×16 MMA is M5-only, and `layout.tensor_core` has no Apple path, so
    this backend uses the 8×8 intrinsic directly.)
  Both backends do cosine. NVIDIA also does L2 (`array_distance`) and inner
  product (`array_inner_product`) by switching only the final distance step,
  exposed as `gpu_cosine_topk_batch(..., metric := 'l2'|'ip')`. (Non-normalized
  L2 has a documented fp16 cancellation issue in the squared norms; normalized
  L2 is exact.) The scalar kernel remains the default, and the fallback when the
  flag is off or the shape is unsupported, on every platform.

### The cost heuristic (engine on by default, no cost model)
The engine is on by default and otherwise does not consider cost. To avoid making
a query slower by offloading it, `build_descriptor` declines high-cardinality
group-by (`SORT_SEGREDUCE`/`HASH_GROUP`, the Q3 shape), which then falls back to
stock CPU. UNGROUPED and DENSE_GROUP (Q1/Q5/Q6/Q14) are unaffected.
`GPU_OP_FORCE_HIGHCARD=1` overrides this for A/B measurement. The decision lives
in `_should_decline(desc, force_highcard)`
([src/descriptor.mojo](src/descriptor.mojo)). Declining is always correct (the
query runs on stock CPU), so this check can only affect performance, never
correctness.

### The cost model (investigated; the current heuristic is close to optimal)
A natural next step (a simple version of Mordred's "holistic model") is a check
that weighs transfer cost against compute: use the cardinality DuckDB already
estimated to decline offloads whose fixed overhead won't pay off. We investigated
it and did not ship an active threshold, only a hook that is off by default, for
a structural reason:

- The signal. Each GET's `est_cardinality` is DuckDB's row estimate after
  filtering, not the base table size: the join order optimizer sets
  `get.estimated_cardinality = cardinality_after_filters`
  (`relation_statistics_helper.cpp`), which the C++ glue reads directly from the
  plan (`gpu_operator.cpp` `ge.est_cardinality = g->estimated_cardinality`). So
  the fact GET's value is the estimated GPU input size after pushdown, which is
  the right input for a transfer-vs-compute decision. (Selectivity as a ratio
  can't be recovered separately, because the base cardinality isn't sent over the
  wire, but the post-filter row count is the number that matters.)
- Why it doesn't work. The operator wins on the warm, repeated path. The cold
  path does not beat stock by design, and its cost is recovered through the
  resident pin (see "Execution and the pin"). A small `est_cardinality` is
  exactly the case that (1) loses when cold, because the fixed overhead isn't
  recovered (for example, Q1 at sf1 cold can lose to stock's ~12 ms), and (2) is
  cheap to keep resident and wins when warm if it repeats. The plan has no signal
  about repeat counts or query history, so cardinality alone cannot tell "tiny and
  loses even when warm" apart from "tiny now, but repeats and wins when warm". A
  threshold tuned to avoid cold losses would also decline queries that win when
  warm, which is the one regression we must avoid (Q1/Q5/Q6/Q14 are accepted and
  win). A threshold low enough to spare them never triggers. No cardinality cutoff
  is a clear improvement.
- What can be decided. Only "the input is too small to pay off even with one warm
  reuse". At TPC-H sf1 and above, the accepted classes are already well above any
  such floor, so an active threshold would either never trigger or cause harm.
- The hook. `_should_decline` therefore has an `est_cardinality` check that is off
  by default: `GPU_OP_COSTMODEL=1` enables it, and `GPU_OP_COSTMODEL_MINROWS=<n>`
  sets the minimum fact input size (default 4096, a placeholder that has not been
  validated). With the model off (the default) the behavior is the same as before.
  The hook exists for future measurement on hardware, not as a shipped policy.
  It has the same status as the semi-join pushdown below, which was measured and
  declined.

## Hardware portability

The generic kernels don't use atomics (`warp.sum` plus an int128 reduction on
the host), so they run on NVIDIA without a separate atomics branch. The warp
width comes from `WARP_SIZE` ([src/gpu_platform.mojo](src/gpu_platform.mojo)).
If gating on 64-bit atomics is ever needed, it has to be evaluated inside kernel
code with `is_nvidia_gpu()`/`is_amd_gpu()` (target checks), not on the host. The
fused kNN matmul path is the opposite case: the NVIDIA backend's routing and
`layout.tensor_core` instantiation are gated on `has_nvidia_gpu_accelerator()`,
and the Apple backend's routing and `simdgroup_matrix` instantiation on
`has_apple_gpu_accelerator()`. Both are compile-time queries evaluated on the
host, so each backend's MMA code is compiled only for its own target and left out
on the other, with the scalar path as the general fallback. `build.sh` produces
a `.so` with `$ORIGIN` on Linux and a `.dylib` with `@loader_path` on macOS.

Validated on both Apple (M-series, Metal) and NVIDIA (RTX 4090, Linux). On
NVIDIA all five classes match stock exactly, and the in-kernel `is_nvidia_gpu()`
branch correctly takes the native 64-bit atomics path. The difference between
warm and cold is much larger on the discrete GPU than on Apple's unified memory.
For example, Q5 at sf1 takes ~590 ms cold (materializing all tables and
uploading them over PCIe) and 1 to 2 ms warm, vs ~11 ms for stock. Cheap queries
like Q1 can lose to stock's ~12 ms, because the fixed offload overhead isn't
recovered at sf1.

Linux build notes:
- The Linux/conda toolchain has no system compiler, so `pixi.toml` lists
  `c-compiler`/`cxx-compiler` as `linux-64` dependencies. Their activation sets
  `CC`/`CXX` (conda gcc), which both Mojo's linker and `build.sh` use. macOS uses
  the system clang and is unaffected.
- On NixOS the CUDA driver library lives under a nix store path. Prepend
  `/run/opengl-driver/lib` to `LD_LIBRARY_PATH` at runtime, or `cuInit` returns
  999. The kernels compile fine; it is only a problem with the path to the
  driver's userspace library, not with the code. Standard Linux distributions
  find `libcuda` through the normal ld cache.

## Limitations and open problems

- The cold path (CPU materialize plus upload) dominates a query that touches data
  for the first time. It costs more on a discrete GPU (full PCIe upload) than on
  Apple's unified memory, and the warm speedup needs a repeated workload to make
  up for it. A GPU-direct decoder now exists
  ([src/native_decode.mojo](src/native_decode.mojo)). It reads DuckDB v1.5.4
  native column segments directly from the live buffer manager (pinned in
  process, no file re-read) and decodes them on the GPU in Mojo (portable; the
  reference implementation, Sirius, is CUDA-only). It is validated bit-exact end
  to end on both platforms: all 6,001,215 `l_shipdate` values decoded GPU-direct
  from BitPacking/FOR storage are identical to DuckDB's scan
  (`gpu_native_decode_check`; synthetic codec coverage in
  `bench/native_decode_test.mojo`).
  Covered (all fixed-width codecs): UNCOMPRESSED and the BITPACKING modes
  CONSTANT / CONSTANT_DELTA / FOR / DELTA_FOR. The mode is chosen per group of
  2048 rows and varies within a segment even when the segment is labeled "FOR",
  so `l_orderkey`/`l_linenumber` are DELTA_FOR. All six lineitem fact columns now
  decode bit-identically (6,001,215 values each, 0 mismatches, 0 deferred, both
  platforms).
  Deferred (measured, with reasons): validity masks are deferred indefinitely (no
  TPC-H column is nullable; `Has Null:false` for all of lineitem). Strings
  (Dictionary/FSST) are only needed for Q1's `l_returnflag`/`l_linestatus` group
  keys and Q14's `p_type`, so they wait until the warm-model proof of concept
  below justifies more coverage (the Q6 and Q5 fact columns are fully decodable as
  fixed-width now). RLE does not apply: it is a segment-level CompressionType, not
  a per-group BitpackingMode, and no TPC-H fixed-width column uses it.

  The decode path is a validated separate path and is not used in query execution
  yet. Measuring first showed that replacing the cold feed to speed up cold runs
  gains little. The cold path already materializes all ~6M rows (the materialize
  SQL has no WHERE; filtering happens in the kernel through a pass column
  computed on the host beforehand), so GPU-direct decode only reduces the amount
  of data transferred (compressed segments are ~3 to 8× smaller) and skips
  DuckDB's multithreaded scan. Warm runs are unchanged, and the cold cost is
  already recovered over repeated runs. The more valuable direction the decoder
  makes possible is residency that does not depend on the predicate. The resident
  columns already hold every row, so their content does not depend on the
  predicate. Only `_signature` (keyed on filter constants) and the precomputed
  pass column tie residency to a specific predicate. Removing the constants from
  the signature and moving the filter into the kernel (constants as launch
  parameters) lets the resident columns serve any filter on the same table warm.
  That turns "every query with different constants is cold" into "the first is
  cold, the rest are warm" (for example, a dashboard varying Q6's date range: the
  2nd and later queries go from ~365 ms cold to ~0.37 ms warm).

  This is implemented and verified behind the flag `GPU_OP_NATIVE_DECODE` (off by
  default) for all four queries the GPU accepts: Q6, Q1, Q5 and Q14 (Q3 runs
  better on the CPU, see below). The filter is removed from `_signature` and
  evaluated in the kernel from launch parameters for each run, over resident
  columns that don't depend on the constants, so the same resident buffers serve
  any filter constant warm. Per query: Q6/Q1/Q14 have fixed-width filters on the
  fact table (`l_shipdate` etc.) that are handled directly. Q5 filters on
  dimensions (`r_name`, `o_orderdate`) and uses "Path B": all resident buffers are
  made independent of the constants (group id = raw supplier nationkey; raw
  dimension arrays; `n_name` labels for every nationkey), and region and date are
  checked in the kernel from scalars passed for each run. (Feeding the dimensions
  again on warm runs is not possible, because the cold/warm contract feeds
  nothing on warm runs.) Eligibility is a strict check for the canonical shape.
  Any deviation (an extra filter, a non-canonical comparison, more than 64 groups)
  falls back to the signature keyed on constants. That is still correct, just not
  warm; it never produces a wrong number.
  - Q6 data path (Stage 1): the fact columns are also fed GPU-direct from the
    decoder (~2.6× less host-to-device traffic). Q1/Q5/Q14 use the existing feed
    for residency (their VARCHAR group keys and promo flag don't depend on the
    constants, so no string decoding is needed; see Deferred above).
  - Verified bit-exact on Apple M3 Max and RTX 4090: each query was run with
    several different constant sets in one session. The first run was COLD and
    the rest WARM under a single signature without constants, and every result
    matched stock (`GPU_OP_GENERIC=off`) with the same constants. The decisive Q5
    test: a region (MIDDLE EAST) that was never materialized on the cold run is
    served WARM and exact, which shows that residency really doesn't depend on
    the constants. (Q1's `avg_*` columns differ only in the last ULP of the
    double. That is the existing rounding from computing the GPU AVG as a double,
    and it also happens with the flag off.)
  Correctness comes first: with the flag off, the default keyed on constants is
  unchanged byte for byte.
- High-cardinality group-by (Q3) runs better on the CPU: light per-row work and a
  large group output. A GPU hash aggregate is implemented (NVIDIA,
  `STRAT_HASH_GROUP`) and is the right algorithm, but it still loses to
  multithreaded stock at sf1 and sf10, so the cost heuristic keeps this shape on
  the CPU. This was measured and is not an open task.
- A transfer-vs-compute cost model driven by cardinality was investigated but not
  shipped as policy. `est_cardinality` is the right (post-filter) signal, but it
  can't tell "tiny input that loses even when warm" apart from "tiny input that
  repeats and wins when warm" (the plan has no repeat-count signal). The warm
  speedup is the whole point, so any active threshold would either never trigger
  or slow down the classes that are accepted and winning. A hook that is off by
  default (`GPU_OP_COSTMODEL`) is in `_should_decline` for future tuning on
  hardware. See "The cost model (investigated)" above.
- Pushing a dynamic filter or semi-join into the fact materialize query was
  measured and declined, because the CPU does this better. The idea (Sirius
  dynamic filters, Mordred semi-join transfer) is to derive the surviving join
  keys from the filtered dimensions and push a
  `WHERE fact_fk IN (SELECT dim_key FROM dim WHERE …)` into the fact materialize
  SQL, so DuckDB prunes fact rows before they cross PCIe. The fact rows shrink a
  lot (Q5: 33×, 6.0M to 0.18M at sf1; same ratio at sf10), but the pushed query is
  1.4 to 5× slower than the plain full scan at both scales: building the dimension
  hash tables and probing the fact table costs more than the smaller output saves.
  More importantly, `EXPLAIN ANALYZE` shows that DuckDB already pushes its own
  dynamic filters from the dimension hash builds into the `lineitem` scan (227k
  rows read, not 6M). DuckDB's planner does this optimization better, and it still
  loses to a plain scan. Any gain would be limited to the first cold run on a
  discrete GPU (only Q5 among the kinds offloaded by default), and the cold path
  is already recovered by the warm pin and would be removed entirely by a
  GPU-direct scan (above). See `bench/semijoin_pushdown_probe.mojo`. Not pursued.
- The accepted class is FK join + filter + group-by/aggregate (plus the cosine kNN
  path); other shapes fall back to the CPU by design.
