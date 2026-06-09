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
`LOAD_COL / PUSH_CONST / ADD / SUB / MUL / SELECT / OP_LOAD_DIM / OP_EQ`. Two
GPU-specific lowerings fall out:
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

### Why no unified-memory allocator

A tempting idea on Apple Silicon (shared CPU/GPU DRAM) is a custom
`DBConfig.allocator` that makes DuckDB's column buffers *be* GPU memory, deleting
the upload. The `bench/` probes show it's unreachable: a scanned column does flow
through `DBConfig.allocator`, but the Mojo GPU API exposes no buffer that is
simultaneously a stable CPU-writable pointer (for DuckDB to fill) and a valid GPU
kernel argument. Hence the pin-resident route — the same choice Sirius makes.

## Performance characteristics

At TPC-H sf1 (warm, validated vs stock answers): **Q1 ~2.0×, Q14 ~1.75×, Q5
~1.6×** over stock, **Q6 ~parity**, and **Q3 below stock** (~0.5×). Stock DuckDB
at sf1 is only ~2–13 ms (small, multithreaded), so the wins are real on the
join/group-heavy queries but Q3's sort-segreduce path is still behind. Larger
scale factors and a faster high-cardinality grouping path are where Q3 would move.

## Hardware portability

The generic kernels are **atomics-free** (warp.sum + host int128 reduce), so they
port to NVIDIA with no atomics branch; warp width comes from `WARP_SIZE`
([src/gpu_platform.mojo](src/gpu_platform.mojo)). 64-bit-atomics gating, if ever
needed, must be evaluated *inside* kernel code via `is_nvidia_gpu()`/`is_amd_gpu()`
(target checks), not host-side. `build.sh` emits `.so`+`$ORIGIN` on Linux and
`.dylib`+`@loader_path` on macOS.

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

- **Cold path** dominates a first-touch query (CPU materialize + upload); removing
  it needs a GPU-direct scan (no Mojo/Metal columnar decoder today) or persistent
  load-time residency.
- **Q3 high-cardinality group-by** (sort-segreduce) trails stock — a fully-GPU
  radix sort or a uint32-spinlock hash aggregate (within the atomics constraint)
  would help.
- **Discrete-GPU cold path** is heavier than Apple's (full PCIe upload of every
  table on first touch); the warm win needs a repeated workload to amortize it.
- The accepted class is FK-join + filter + group-by/aggregate; shapes outside it
  fall back to CPU by design.
