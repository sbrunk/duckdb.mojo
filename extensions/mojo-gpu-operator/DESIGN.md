# Design — mojo-gpu-operator

How the GPU offload engine works, why it plans differently from DuckDB, and how
the two interact. For build/use see [README.md](README.md); for the exact
C++↔Mojo wire format see [src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md).

## Mental model: DuckDB plans, we're a peephole optimizer with a GPU backend

We did not build a query planner that competes with DuckDB's. DuckDB does all
the real planning: parse, bind, cardinality estimation, join order, filter
pushdown, expression binding, type/decimal resolution. Our extension is a
narrow post-optimizer rewrite that:

1. recognizes a strict, allow-listed **class** of already-optimized plan subtrees
   (an aggregate over a filter/FK-join/scan tree),
2. makes the handful of decisions a GPU needs that DuckDB's planner never makes,
   and
3. replaces that subtree with a single GPU **source operator** — leaving the rest
   of the plan (ORDER BY, LIMIT, outer projections) as ordinary DuckDB operators.

Anything outside the recognized class, or any runtime GPU error, runs on stock
DuckDB. This is **translate-or-fallback**, not cost-based planning: there is no
GPU-vs-CPU per-operator cost comparison — we accept what we can prove correct and
decline everything else, so a mismatch can never produce a wrong result.

`C++ is DuckDB-ABI glue; all planner + execution logic lives in Mojo.`

## How it hooks in and interacts with DuckDB

The extension is an `OptimizerExtension` ([src/gpu_operator.cpp](src/gpu_operator.cpp))
with two hooks plus a custom operator:

- **`GpuPreOptimize`** (before DuckDB's optimizer) flips exactly one setting:
  it disables `COMPRESSED_MATERIALIZATION`, so group keys stay raw `VARCHAR`
  instead of being dictionary-compressed into `__internal_compress` projections a
  GPU source op can't reproduce. Restored immediately after, so later queries are
  unaffected. (Not GPU planning — just keeping the plan in a translatable shape.)
- **`OptimizeNode`/`TryRouteGeneric`** (after DuckDB's optimizer) walks the
  **already-optimized** logical plan bottom-up. On a recognized aggregate subtree
  it serializes the subtree (`SerializeMatchedPlan` → a flat `RawPlan` tape),
  hands it to Mojo `build_descriptor` (the planner brain), and — if accepted —
  replaces the subtree with a `LogicalGpuAgg` (a `LogicalExtensionOperator`).
- **`PhysicalGpuAgg`** is the physical lowering of that operator and is a
  **source** (`IsSource()==true`, no children). It bypasses DuckDB's
  scan/join/aggregate *physical* pipeline for the subtree. To get its input it
  issues a **nested `Connection::Query` back into DuckDB** — so DuckDB is
  simultaneously the thing we replace *and* the thing we use to feed the GPU (its
  scan + pushed-down filters materialize the columns we upload).

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
DuckDB's `estimated_cardinality` and `table_filters` straight off the plan and
reuse them for GPU decisions.

## What we reuse from DuckDB

| Concern | Who decides | Note |
|---|---|---|
| Join order / algorithm | DuckDB | we read the resulting join tree |
| Filter pushdown | DuckDB | filters arrive on the `LogicalGet` as `table_filters` |
| Cardinality estimates | DuckDB | consumed for fact-table selection |
| Expression / type / decimal binding | DuckDB | we read bound exprs + `DecimalType::GetScale` |
| Input materialization | DuckDB | the source op's nested `Connection::Query` (its scan + filters feed the GPU) |
| Everything above the matched subtree | DuckDB | TOP_N / LIMIT / outer projections run stock on our output |

## Where GPU planning differs

These decisions live in Mojo's `build_descriptor` ([src/descriptor.mojo](src/descriptor.mojo))
and exist only because the target is a SIMT, transfer-bound, (on Apple)
no-64-bit-atomics device. None of them is something a CPU planner needs.

**1. Fact vs. dimension identification.** DuckDB's joins are pipelined and don't
single out a "big" table. A GPU kernel must pick one **streaming side** (the fact
table — one thread per row) and treat the others as **probe-side lookups**. Fact
= the `LogicalGet` with the largest `estimated_cardinality`; every other GET must
attach by a resolvable FK equi-edge, iteratively, to handle snowflakes (Q5's
customer→supplier→nation).

**2. Execution-strategy selection — dictated by GPU limits, not data alone.**
DuckDB always uses a radix-partitioned hash aggregate. We pick among four because
of what the GPU can/can't do (`segreduce.mojo`):
- **UNGROUPED** (0 keys) — one global reduction.
- **DENSE_GROUP** (small key space: Q1 returnflag×linestatus, Q5's ≤25 nations) —
  per-warp private accumulators + `warp.sum`, no atomics.
- **SORT_SEGREDUCE** (high-cardinality integer key: Q3 orderkey) — sort first
  (in DuckDB, via an injected `ORDER BY` at materialize) then **one warp per
  contiguous segment**. This exists *only because Apple GPUs have no 64-bit
  atomics* — we can't hash-aggregate into 64-bit accumulators, so we trade it for
  sort + segmented reduction.
- **HASH_GROUP** — would need atomics; on Apple it is rewritten to
  SORT_SEGREDUCE.

This is the clearest "must plan differently for GPU": the *same* GROUP BY maps to
a different physical strategy purely because of an atomics constraint invisible to
a CPU planner.

**3. Residency / materialization planning — the "pin".** A planning axis DuckDB's
streaming-morsel model doesn't have. For a GPU the dominant cost is **host→device
transfer**, so the plan decides *which columns to pull once, upload to device
memory, and cache across queries* (the process-global resident cache `_pin2`,
keyed by a signature). The cost model is **data-movement-dominated, not
CPU-cycle-dominated**: see "Execution & the pin" below.

**4. Lowering predicates/expressions to a GPU-evaluable form.** DuckDB's
`ExpressionExecutor` interprets bound trees on vectors; we can't ship that to the
GPU, so the planner compiles the filter + aggregate arguments into a tiny
**postfix integer program** run by `expr_vm` ([src/expr_vm.mojo](src/expr_vm.mojo)):
`LOAD_COL / PUSH_CONST / ADD / SUB / MUL / SELECT / OP_LOAD_DIM / OP_EQ`.

> **Comptime-specialized kernels (Mojo's answer to runtime JIT).** The interpreter
> is the *universal fallback*. For the recognized kinds (Q1/Q6/Q14/Q5) the per-row
> program is instead compiled into a **stackless, branch-free, register-resident**
> evaluator via Mojo `@parameter` specialization (`seg_*_kernel_q*` +
> `eval_program_fast` in [src/segreduce.mojo](src/segreduce.mojo)), dispatched by
> `desc.kind` from the single warm/cold `_assemble` site. This is what cuDF needs a
> runtime `nvrtc` JIT (`AST_JIT`) for — Mojo does it at compile time, portable to
> NVIDIA/Apple/AMD with no runtime compiler. Bit-identical to the interpreter
> (asserted in `bench/expr_comptime_probe.mojo`), measured **4.7× (Q1) / 1.8× (Q6)
> on Apple M3 Max** and **17.9× (Q1) / 5.5× (Q6) on RTX 4090** at the kernel level
> (the wide SIMT machine pays more for the interpreter's branch divergence +
> per-op global program reads). Any unrecognized shape runs the `expr_vm` path
> unchanged, so a novel expression can never be wrong.

Two GPU-specific lowerings fall out:
- **Joins become gathers.** TPC-H FKs are dense surrogate PKs, so a join lowers to
  an on-GPU dense-array gather `OP_LOAD_DIM(dim[fact_key[row]])` rather than a hash
  probe.
- **Host-side dim folding.** When a dimension joins another dimension rather than
  the fact (Q3 customer→orders), or a correlated dim↔dim condition exists (Q5
  `c_nationkey=s_nationkey`, handled by `OP_EQ` over two gathers), it can't be a
  single-level GPU gather — the planner folds it on the host into the fact-keyed
  lookup arrays before launch.

**5. Exactness planning.** To stay bit-exact in decimals without 128-bit GPU
arithmetic: per-row products and per-block/segment `warp.sum` partials stay int64
(they fit), and only the cross-block reduction is done in **int128 on the host**.
Sums match DuckDB bit-for-bit; averages match to double rounding.

### Contrast in one frame

| | DuckDB planner | this GPU layer |
|---|---|---|
| Scope | whole query, cost-based | recognize one aggregate sub-shape, rule-based allow-list |
| Picks join order | yes | no (reads DuckDB's) |
| Streaming vs probe side | n/a (symmetric pipeline) | **must pick** (fact = max cardinality) |
| Group-by physical form | always radix hash agg | **4 strategies by atomics/cardinality** |
| Residency / transfer | no concept | **the pin; signature-keyed device cache** |
| Cost driver | CPU cycles / bandwidth | **host↔device transfer (cold), kernel (warm)** |
| Expression eval | vectorized interpreter | **compiled to a postfix GPU program** |
| Unsupported input | n/a | falls back to stock DuckDB |

## The C++ ↔ Mojo boundary (RawPlan)

C++ only does what needs DuckDB's C++ ABI: the optimizer hooks, walking the plan,
the operator subclasses, the nested query, and filling result chunks. It
serializes the matched subtree into a flat **int64 tape + string blob**
(`SerializeMatchedPlan`) with no DuckDB types crossing over. Mojo owns the rest:
`build_descriptor` (the planner brain), the execution shuttle
(`materialize_sql`/`feed_column`/`pin_finalize`), the kernels, and the resident
cache. Full wire format: [src/RAW_PLAN_CONTRACT.md](src/RAW_PLAN_CONTRACT.md).

## Execution & the pin (warm/cold)

A query **pins** its columns: materialize them from DuckDB and upload into
resident `DeviceBuffer`s, cached process-wide by a signature that includes the
fact table, projected columns, and **filter constants** (so a warm hit means an
identical predicate).

- **Cold** (first touch of a table/predicate): pay a one-time materialize +
  upload, much costlier than the kernel — a single cold query does **not** beat
  stock DuckDB.
- **Warm** (repeat): `pin_begin` reports WARM, C++ **skips feeding columns**, and
  `pin_finalize` re-runs the kernel on the **resident device buffers** with no
  host data and no re-upload (`segreduce_upload` once → `segreduce_run` per call).

So the win is **warm / repeated** workloads. The earlier implementation was
correct only on the cold (single-shot) path and returned wrong results on warm
runs; the fix is the resident `_pin2` cache + a shared cold/warm `assemble` so the
two paths cannot diverge.

### Bounded, LRU-evicting resident pin cache (tiered-memory bound)

Pins are pure cache, so the resident caches can be **bounded and LRU-evicted**
without ever affecting results — eviction just forces the next touch back onto the
**cold** rebuild path (materialize + upload again, slower, identical answer). This
is the gap Sirius's cuCascade tiered memory + downgrade executor closes; we close
it for the **kNN embedding pins** — the biggest GPU residents (~3 GB at 1M×768) and
the ones that previously leaked VRAM until OOM when many matrices/columns were
pinned.

The kNN pin registry ([src/gpu_operator.cpp](src/gpu_operator.cpp), `ResidentPin`)
caps total resident bytes at `GPU_OP_PIN_BUDGET_MB` (default **4096 MB**; `0` =
unbounded/legacy). Each entry tracks its approximate VRAM footprint, a monotonic
last-use tick (LRU), and an **in-use refcount**. On a new pin it evicts the
least-recently-used *evictable* entry until the new one fits, freeing its
`DeviceBuffer`s via the Mojo `mojo_gpu_pin_free`/`_f16` entry points; if the device
allocation still OOMs, it **evicts-and-retries** (mirrors Sirius's OOM-retry) and
only fails — gracefully, as a SQL error, never a crash — once nothing more is
evictable.

The one eviction hazard is freeing a buffer a query is actively reading: a
`gpu_cosine*` `Bind` fetches the handle under the registry lock but runs the GPU
query *outside* it, so a concurrent evict could otherwise use-after-free. An RAII
**`PinLease`** (returned by `EnsurePinned*`, increments the refcount under the lock)
keeps the entry un-evictable for the query's lifetime; LRU/`gpu_unpin` skip
refcount > 0 entries and report them as `skipped_in_use`. Explicit control is
exposed as table functions consistent with the `gpu_cosine*` family:
`gpu_pin_status()` (resident pins + bytes + last-use age + in-use + budget),
`gpu_unpin(key)` / `gpu_unpin_all()` (free now), and
`gpu_pin_table(table, column [, precision])` (pre-pin a column so the first query
is warm). Covered by `bench/pin_evict_test.sh` (run directly: `bash extensions/mojo-gpu-operator/bench/pin_evict_test.sh`):
pins past a small budget, asserts eviction via `gpu_pin_status`, then re-runs an
evicted kNN query and asserts the cold rebuild's distance distribution is identical
to stock `array_cosine_distance`.

The aggregate `_pin2` cache (TPC-H column buffers) is **not yet bounded**: its
residents are far smaller (KB–MB, not GB), it lives Mojo-side (`GpuPinned` +
`SegResident`), and bounding it requires Mojo-side byte tracking + a teardown path
for `SegResident`/`GpuPinned`. It is the natural next increment; the eviction-is-
safe argument is identical (a missing signature returns COLD from `pin_begin` and
`_pin_finalize_*` rebuilds). **Host-tier spill** (keep an evicted pin's host copy
and re-promote without re-materializing from DuckDB) is deferred: it is cleanest on
Apple unified memory (the host copy is nearly free), but the current kNN pin frees
the host buffer right after upload (`mojo_gpu_pin*` `h16.free()` / the pageable
`enqueue_copy`), so a host tier would need new retained-host storage + a promote
path — out of scope for this increment. The shipped bound (budget + LRU + OOM-retry)
already removes the unbounded-VRAM failure mode.

### Why no unified-memory allocator

A tempting idea on Apple Silicon (shared CPU/GPU DRAM) is a custom
`DBConfig.allocator` that makes DuckDB's column buffers *be* GPU memory, deleting
the upload. The `bench/` probes show it's unreachable: a scanned column does flow
through `DBConfig.allocator`, but the Mojo GPU API exposes no buffer that is
simultaneously a stable CPU-writable pointer (for DuckDB to fill) and a valid GPU
kernel argument. Hence the pin-resident route — the same choice Sirius makes.

## Performance characteristics

**TPC-H sf1, warm, result-verified vs stock answers** (benchmark_runner medians,
1 cold + 5 hot runs, `Verify()` on every hot run):

| query | RTX 4090 GPU vs stock | Apple M3 Max GPU vs stock |
|---|---|---|
| Q14 (FK join + promo CASE) | 0.64 ms vs 10.9 ms — **16.9×** | 2.23 ms vs 6.92 ms — **3.1×** |
| Q6  (filter + scalar sum)  | 0.37 ms vs 2.9 ms — **7.9×**  | 2.02 ms vs 2.45 ms — **1.2×** |
| Q5  (6-way join, grouped)  | 1.59 ms vs 11.4 ms — **7.2×** | 5.23 ms vs 9.74 ms — **1.9×** |
| Q1  (grouped, 8 aggregates)| 2.04 ms vs 12.6 ms — **6.2×** | 5.49 ms vs 14.5 ms — **2.6×** |

The GPU wins where there's real per-row / join work and a small output; the
discrete GPU's warm/cold split is sharp (NVIDIA first-touch incl. GPU init/JIT:
Q1 ~1.6 s, others ~0.3–0.5 s → fully amortized to the warm numbers above).

> **Measure warm with the benchmark_runner, not the interactive CLI.** The DuckDB
> CLI cannot measure this warm regime: re-running an identical query hits DuckDB's
> result handling (returns instantly without re-executing), and varying a filter
> constant to defeat that forces a *cold* signature (the pin key includes filter
> constants) — so both look like the GPU "losing" by 50–170×, when they are really
> measuring cold first-touch + init. The benchmark_runner does proper cold+hot runs
> and verifies results, and is the source of the numbers above. (A related red
> herring: an interactive-CLI session can *appear* to return empty rows for a
> repeated grouped query — confirmed an artifact of CLI result rendering, **not** the
> operator: the source op allocates fresh per-execution state and the warm finalize
> re-runs the kernel and rebuilds the full result set every call; benchmark_runner
> `Verify()` passed on all 10 hot grouped-Q1 executions per platform, and piped-stdin
> CLI returns byte-identical full rows on every repeat.) **Q3 is the exception and is now kept on the CPU**
(see the cost heuristic below): it's a light-per-row, high-cardinality-output
(~1.5M groups) shape that DuckDB's multithreaded join+hash-aggregate does better —
measured GPU 0.87× at sf1 and **0.54× at sf10** (the gap *widens* with scale: the
single-threaded GPU source op scans ~linearly while 16-thread stock has headroom).
The right GPU algorithm (a hash-aggregate, implemented) can't change that — it's
the query shape, not the algorithm.

### Vector search (kNN) — the strongest GPU regime
`gpu_cosine_topk` / `_batch` do **exact** top-k cosine over a GPU-resident
embedding matrix (pinned once, fp16 by default). Measured on the RTX 4090
(clustered unit-norm embeddings, warm):
- **vs CPU exact brute-force: ~40–57× faster** (the fair fight). The cold pin
  amortizes in ~12 queries.
- **vs DuckDB's vss/HNSW:** competitive-to-faster single-query up to ~1M rows
  (fp16: 1M×384 ~1.3 ms, 1M×768 ~2.1 ms) while being **exact, zero-build, and
  half the index memory**. fp16 recall@10 ≥0.99 with *zero genuine misses*
  (sub-1.0 is boundary FP-tie reordering). HNSW pulls ahead only at
  many-millions-of-rows / recall-tolerant, write-once-query-forever workloads
  (its query is sublinear; ours is an O(N) scan, and the resident pin is capped by
  VRAM — ~3 GB at 1M×768).
- Single-query kNN is **bandwidth-optimal** (~912 GB/s, one matrix read; it's the
  *batched* path that needed work). For batched queries an optional **fused
  matmul path** (`GPU_OP_TENSORCORE`, default off) replaces the scalar
  per-(row, query) dot with a tiled MMA `queries·embᵀ` fused with a streaming
  per-query top-k — so the M×N similarity matrix is **never materialized** (only
  the embeddings are read, only M×k written). MMA crushes the per-query compute, so
  the kernel becomes **matrix-read-bound** and that one read amortizes across the
  batch. Two backends, same fused structure:
  - **NVIDIA tensor cores** ([src/tc_knn.mojo](src/tc_knn.mojo), m16n8k8 fp16→fp32,
    `transpose_b`): up to **~60×** per-query at large batch (M=1000 over 1M×768),
    HBM-read-bound at ~69% of peak, **recall@10 = 1.0** (exact ids).
  - **Apple-Silicon 8×8 `simdgroup_matrix`** ([src/tc_knn_apple.mojo](src/tc_knn_apple.mojo),
    M1–M4): **~3–7×** per-query on an M3 Max, unified-memory-bandwidth-bound,
    recall@10 = 1.0. (Apple's 16×16 MMA is M5-only; `layout.tensor_core` has no
    Apple path, so this backend uses the 8×8 intrinsic directly.)
  Cosine on both backends; NVIDIA also does **L2** (`array_distance`) and
  **inner-product** (`array_inner_product`) by switching only the distance epilogue
  — exposed as `gpu_cosine_topk_batch(..., metric := 'l2'|'ip')`. (Non-normalized
  L2 has an fp16 squared-norm cancellation caveat — documented; normalized is
  exact.) The scalar kernel stays the default and the flag-off / unsupported-shape
  fallback on every platform.

### The cost heuristic (default-on engine, no cost model)
The engine is default-on and otherwise cost-blind. To avoid making a query
*slower* by offloading it, `build_descriptor` **declines high-cardinality
group-by** (`SORT_SEGREDUCE`/`HASH_GROUP`, i.e. the Q3 shape) → it falls back to
stock CPU. UNGROUPED / DENSE_GROUP (Q1/Q5/Q6/Q14) are unaffected.
`GPU_OP_FORCE_HIGHCARD=1` overrides for A/B measurement. The decision lives in
`_should_decline(desc, force_highcard)` ([src/descriptor.mojo](src/descriptor.mojo)) —
declining is always correct (it just routes to stock CPU), so this gate can only
affect performance, never correctness.

### The cost model (investigated — current heuristic near-optimal)
A natural next step (Mordred's "holistic model", lite) is a transfer-vs-compute
gate: use the cardinality DuckDB already estimated to decline offloads whose
fixed overhead won't be repaid. We investigated it and **did not ship an active
threshold** — only a default-off hook — for a concrete, structural reason.

- **The signal.** Each GET's `est_cardinality` is DuckDB's *post-filter* row
  estimate, not the base-table size: the join-order optimizer sets
  `get.estimated_cardinality = cardinality_after_filters`
  (`relation_statistics_helper.cpp`), which the C++ glue reads straight off the
  plan (`gpu_operator.cpp` `ge.est_cardinality = g->estimated_cardinality`). So
  the fact GET's value is exactly the estimated GPU *input* size after pushdown —
  the right transfer-vs-compute driver. (Selectivity as a *ratio* is not
  separately recoverable — base cardinality isn't carried over the wire — but the
  post-filter row count is the number that actually matters.)
- **The tension that defeats it.** The operator's win is the **warm / repeated**
  path; the **cold** path "does not beat stock" by design and is amortized by the
  resident pin (see "Execution & the pin"). A *small* `est_cardinality` is
  precisely the case that (1) loses **cold** because the fixed overhead isn't
  amortized (cf. "Q1 sf1 cold can lose to stock's ~12 ms"), *and* (2) is cheap to
  keep resident and **wins warm** if it repeats. The plan carries **no
  repeat-count / query-history signal**, so cardinality alone cannot separate
  "tiny, loses even warm" from "tiny now, repeats and wins warm". Any threshold
  tuned to kill cold losses would also decline warm-winning queries — the one
  regression we must not cause (Q1/Q5/Q6/Q14 are accepted-and-winning). And a
  threshold set low enough to spare them never fires. There is no cardinality
  cutoff that is a clear win.
- **What's decidable.** Only "input too small to ever amortize even one warm
  reuse" — and at TPC-H sf1+ the accepted classes are already well above any such
  floor, so an active threshold would be either dead or harmful.
- **The hook.** `_should_decline` therefore carries a **default-off**
  `est_cardinality` gate: `GPU_OP_COSTMODEL=1` enables it,
  `GPU_OP_COSTMODEL_MINROWS=<n>` sets the fact-input floor (default 4096, an
  unvalidated placeholder). With the model off (the default) behavior is
  identical to before. The hook is here for future on-hardware measurement, not
  as a shipped policy — same status as the "measured and declined" semi-join
  pushdown below.

## Hardware portability

The generic kernels are **atomics-free** (warp.sum + host int128 reduce), so they
port to NVIDIA with no atomics branch; warp width comes from `WARP_SIZE`
([src/gpu_platform.mojo](src/gpu_platform.mojo)). 64-bit-atomics gating, if ever
needed, must be evaluated *inside* kernel code via `is_nvidia_gpu()`/`is_amd_gpu()`
(target checks), not host-side. The complementary case is the fused-kNN matmul
path: the NVIDIA backend's routing + `layout.tensor_core` instantiation are gated on
`has_nvidia_gpu_accelerator()`, and the Apple backend's routing + `simdgroup_matrix`
instantiation on `has_apple_gpu_accelerator()` (both **host** comptime queries) — so
each backend's MMA code is compiled only for its own target and elided on the other,
with the scalar path as the universal fallback. `build.sh` emits
`.so`+`$ORIGIN` on Linux and `.dylib`+`@loader_path` on macOS.

**Validated on both Apple (M-series, Metal) and NVIDIA (RTX 4090, Linux).** On
NVIDIA all five classes match stock exactly and the in-kernel `is_nvidia_gpu()`
branch correctly takes the **native 64-bit-atomics** path. The discrete-GPU
warm/cold split is far sharper than Apple's unified memory: e.g. Q5 sf1 cold
~590 ms (PCIe materialize+upload of all tables) → warm **1–2 ms** vs stock ~11 ms;
cheap queries like Q1 can lose to stock's ~12 ms because the offload's fixed
overhead isn't amortized at sf1.

Linux build notes:
- The Linux/conda toolchain has no system compiler, so `pixi.toml` carries
  `c-compiler`/`cxx-compiler` as `linux-64` deps; their activation sets `CC`/`CXX`
  (conda gcc) which both Mojo's linker and `build.sh` pick up. macOS uses system
  clang and is unaffected.
- On **NixOS**, the CUDA driver lib lives under a nix store path; prepend
  `/run/opengl-driver/lib` to `LD_LIBRARY_PATH` at *runtime* or `cuInit` returns
  999 (the kernels compile fine; it's purely a driver-userspace path issue, not a
  code problem). Standard Linux distros resolve `libcuda` via the normal ld cache.

## Limitations / open frontier

- **Cold path** dominates a first-touch query (CPU materialize + upload). Heavier on
  a discrete GPU (full PCIe upload) than on Apple's unified memory; the warm win
  needs a repeated workload to amortize it. **A GPU-direct decoder now exists**
  ([src/native_decode.mojo](src/native_decode.mojo)): it reads DuckDB v1.5.4 native
  column segments straight from the live buffer manager (in-process pin — no file
  re-read) and decodes them **on the GPU in Mojo** (portable; the reference, Sirius,
  is CUDA-only). Validated bit-exact end-to-end on both platforms: every one of the
  6,001,215 `l_shipdate` values decoded GPU-direct from BitPacking/FOR storage is
  identical to DuckDB's scan (`gpu_native_decode_check`; synthetic codec coverage in
  `bench/native_decode_test.mojo`). **Covered (all fixed-width codecs):** UNCOMPRESSED
  + BITPACKING modes CONSTANT / CONSTANT_DELTA / FOR / DELTA_FOR — note per-2048-row
  group modes vary *within* a segment even under a "FOR" segment label, so
  `l_orderkey`/`l_linenumber` are DELTA_FOR; all six lineitem fact columns now decode
  bit-identically (6,001,215 values each, 0 mismatches, 0 deferred, both platforms).
  **Deferred (measured, with rationale):** *validity* — DEFER indefinitely (no TPC-H
  column is nullable; `Has Null:false` for all lineitem); *strings* (Dictionary/FSST)
  — needed only for Q1's `l_returnflag`/`l_linestatus` group keys and Q14's `p_type`,
  so deferred until the warm-model PoC below justifies extending coverage (Q6/Q5-fact
  are fully fixed-width-decodable now); *RLE* — N/A (a segment-level CompressionType,
  not a per-group BitpackingMode; no TPC-H fixed-width column uses it).

  **The decode path is a validated parallel path, not yet in query execution.** A
  measure-first design (empirically grounded) found that *replacing the cold feed to
  speed up cold* is **marginal** — the cold path already materializes *all* ~6M rows
  (the materialize SQL has no WHERE; filtering is in-kernel via a host-prebaked pass
  column), so GPU-direct decode only trims transfer volume (compressed segments, ~3–8×
  smaller) and skips DuckDB's multithreaded scan; warm is unchanged and cold is already
  amortized. The genuinely valuable adjacent direction the decoder *enables* is
  **predicate-independent residency**: the resident columns already hold every row
  (content is predicate-independent), and only `_signature` (keyed on filter constants)
  + the host-prebaked pass column couple residency to a specific predicate. Dropping
  the constants from the signature and moving the filter **into** the kernel (constants
  as launch params) would let the resident columns serve *any* filter on the same
  table warm — turning "every distinct-constant query is cold" into "first cold, rest
  warm" (e.g. a dashboard varying Q6's date range: 2nd+ query ~365 ms cold → ~0.37 ms
  warm).

  **This is implemented and proven, flag-gated on `GPU_OP_NATIVE_DECODE` (default
  off), for all four GPU-accepted queries — Q6, Q1, Q5, Q14** (Q3 is CPU-favorable,
  below). The filter is dropped from `_signature` and evaluated **in-kernel** from
  per-run launch params over the constant-independent resident columns, so the same
  resident buffers serve *any* filter constant warm. Per kind: Q6/Q1/Q14 have
  fixed-width fact filters (`l_shipdate` etc.) handled directly; **Q5** (filter on
  *dimensions* — `r_name`, `o_orderdate`) uses "Path B" — all resident buffers made
  constant-independent (group id = raw supplier nationkey; raw dim arrays;
  `n_name` labels for every nationkey) with region/date gated in-kernel from
  per-run scalars (the naive "re-feed dims on warm" is impossible — the cold/warm
  contract feeds nothing on warm). Eligibility is a strict canonical-shape gate;
  any deviation (extra filter, non-canonical cmp, group cardinality > 64) falls back
  to the per-constant signature — correct, just not warm; **never a wrong number**.
  - Q6 substrate (Stage 1): the fact columns are also fed GPU-direct from the decoder
    (~2.6× less host→device traffic); Q1/Q5/Q14 use the existing feed for residency
    (their VARCHAR group keys / promo flag are constant-independent, so no string
    decode is needed — see Deferred above).
  - **Verified bit-exact on Apple M3 Max + RTX 4090**: each query run with several
    *different* constant sets in one session → first COLD, rest WARM under a single
    constant-free signature, every result identical to stock (`GPU_OP_GENERIC=off`)
    with the same constants. Decisive Q5 probe: a region (MIDDLE EAST) *never*
    materialized on the cold run is served WARM and exact, proving residency is truly
    constant-independent. (Q1's `avg_*` columns differ only in the last double ULP —
    the pre-existing GPU-AVG-as-double rounding, present with the flag off too.)
  Correctness-first: flag-off is the byte-identical, per-constant-keyed default.
- **High-cardinality group-by (Q3) is CPU-favorable** — light per-row work + large
  group output. A GPU hash-aggregate is implemented (NVIDIA, `STRAT_HASH_GROUP`)
  and is the right algorithm, but it still loses to multithreaded stock at sf1 and
  sf10, so the cost heuristic keeps this shape on the CPU. Not an open task —
  measured and settled.
- **A cardinality-driven transfer-vs-compute cost model — investigated, not
  shipped as policy.** `est_cardinality` is the right (post-filter) signal, but it
  can't separate "tiny input that loses even warm" from "tiny input that repeats
  and wins warm" (no repeat-count signal on the plan), and the warm win is the
  whole point — so any active threshold would either be dead or regress the
  accepted-and-winning classes. A default-off hook (`GPU_OP_COSTMODEL`) is in
  `_should_decline` for future on-hardware tuning. See "The cost model
  (investigated)" above.
- **Dynamic-filter / semi-join pushdown into the fact materialize is CPU-favorable
  — measured and declined.** The idea (Sirius dynamic filters / Mordred semi-join
  transfer): derive surviving join keys from the filtered dimensions and push a
  `WHERE fact_fk IN (SELECT dim_key FROM dim WHERE …)` into the fact materialize
  SQL so DuckDB prunes fact rows before they cross PCIe. The fact-row reduction is
  huge (Q5: 33×, 6.0M→0.18M at sf1; same ratio at sf10), but the pushed query is
  **1.4–5× slower** than the plain full scan at both scales: building the dim hash
  tables + probing the fact dominates the saved output volume. Decisively,
  `EXPLAIN ANALYZE` shows DuckDB **already pushes its own dynamic filters** from the
  dimension hash-builds into the `lineitem` scan (227k rows read, not 6M) — the
  optimization is something DuckDB's planner does better, and it *still* loses to a
  plain scan. The win, if any, would be discrete-GPU cold-first-touch only (Q5
  alone among default-offloaded kinds), and the cold path is amortized by the warm
  pin and removed entirely by a GPU-direct scan (above). See
  `bench/semijoin_pushdown_probe.mojo`. Not pursued.
- The accepted class is FK-join + filter + group-by/aggregate (+ the cosine kNN
  path); shapes outside it fall back to CPU by design.
