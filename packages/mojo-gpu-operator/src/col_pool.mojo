"""Process-global, bounded column pool (Phase 1 of RESIDENT_POOL_PLAN.md).

Deduplicates the COLD host->device upload of aggregate fact columns that are
shared across query signatures. Each distinct physical column (keyed by
`(table, column, representation, ordering, n_rows)`) is uploaded ONCE into its
own `DeviceBuffer[int64]` and inserted; subsequent queries that reference the
same column HIT the resident buffer and SKIP the H2D. The per-signature packed
`cols_d` is then assembled by a DEVICE-TO-DEVICE copy from each pooled buffer
into its slot offset (done by the caller in gpu_kernels.mojo).

CORRECTNESS: the pooled buffer holds the EXACT int64 bytes the packing loop in
`_pin_finalize_*` would have written for that column (`_col_val` widened to
int64), so the D2D-assembled `cols_d` is byte-identical to today's freshly
packed buffer. The kernels read `cols_d` unchanged => identical results.

ROW-ORDER SAFETY: `ordering` is part of the key and Phase 1 pools ONLY
`ordering == ORDERING_STORAGE` (no ORDER BY). `ensure_column` FAILS CLOSED on
any other ordering ("not poolable") so the caller uploads as today. The
STRAT_SORT_SEGREDUCE path (which appends `ORDER BY <fact gk>`, a DIFFERENT row
order) therefore never pools.

REPRESENTATION: aggregates use `REPR_INT64_PACKED`. kNN fp16/fp32 use DISTINCT
representation values and must NEVER alias these int64 buffers.

This module is PURE helpers (no `@export`); all C-ABI wrappers live in
gpu_kernels.mojo per RAW_PLAN_CONTRACT. The pool is process-global via `_Global`
(mirroring `_pin2`). Aggregate source ops are single-threaded (ParallelSource
== false), so there are no concurrent mutators; the Dict is still guarded
consistently behind the single pool-state pointer.
"""

from std.ffi import _Global
from std.collections import Dict
from std.gpu.host import DeviceContext, DeviceBuffer
from std.os import getenv


# ---------------------------------------------------------------------------
# Representation tags. Aggregates pack everything as int64 (DATE/INTEGER widened
# from int32, BIGINT/DECIMAL int64-backed already int64) -- exactly what
# `_col_val` returns. kNN paths (fp16/fp32) RESERVE distinct values so a cosine
# embedding column can never alias an int64 aggregate buffer of the same name.
# ---------------------------------------------------------------------------
comptime REPR_INT64_PACKED: Int = 0
comptime REPR_FP32: Int = 1  # reserved for kNN (never produced here)
comptime REPR_FP16: Int = 2  # reserved for kNN (never produced here)

# Ordering tokens (the second correctness axis). Phase 1 only pools STORAGE.
comptime ORDERING_STORAGE: String = "STORAGE"  # no ORDER BY -> physical order

# Key field separator: a control char that cannot appear in a SQL identifier,
# table name, representation int, or ordering token (mirrors the \x01 group-key
# joiner used elsewhere in gpu_kernels.mojo).
comptime _KEY_SEP: String = "\x1f"


def col_key(
    table: String,
    column: String,
    representation: Int,
    ordering: String,
    n_rows: Int,
) -> String:
    """Encode a ColKey as a String map key (mirrors `_pin2`'s String-keyed Dict).

    `(table, column, representation, ordering, n_rows)`. n_rows is part of the
    key: two materializations of the "same" column with different row counts are
    physically different uploads (and must never share a buffer of the wrong
    length).
    """
    return (
        table
        + _KEY_SEP
        + column
        + _KEY_SEP
        + String(representation)
        + _KEY_SEP
        + ordering
        + _KEY_SEP
        + String(n_rows)
    )


# ---------------------------------------------------------------------------
# One resident pooled column. `buf` is the int64 device buffer holding the
# widened column values (length == n_rows). `bytes` is its VRAM footprint
# (n_rows * 8), charged against the budget. `last_use`/`access_count` drive LRU
# + (future Phase 2) cost-aware eviction. `refcount` is the in-use lease count:
# an entry with refcount > 0 is NEVER evicted (an in-flight query is reading it).
# ---------------------------------------------------------------------------
# Copyable: required only to satisfy the pinned-nightly Dict's iteration trait
# bound (`for k in dict`). A copy is a cheap refcounted DeviceBuffer handle bump;
# key iteration yields String keys and never actually copies a PooledColumn, so
# the access_count/last_use/refcount bookkeeping stays single-sourced in the map.
struct PooledColumn(Copyable, Movable):
    var buf: DeviceBuffer[DType.int64]
    var n_rows: Int
    var bytes: Int
    var last_use: Int
    var access_count: Int
    var refcount: Int
    # The contract TypeTag the column was fed as (TYPE_DATE / TYPE_DECIMAL /
    # TYPE_BIGINT / ...). Set on MISS from the fed column's tag and carried so the
    # SKIP-MATERIALIZE path can recover the decimal SCALE of an OMITTED column
    # whose FedColumn (st.cols[mj]) is never filled (its type_tag would read 0 ->
    # wrong DECIMAL scale -> wrong AVG). Phase 1 never read this; harmless then.
    var type_tag: Int64

    def __init__(
        out self, var buf: DeviceBuffer[DType.int64], n_rows: Int, type_tag: Int64
    ):
        self.buf = buf^
        self.n_rows = n_rows
        self.bytes = n_rows * 8
        self.last_use = 0
        self.access_count = 0
        self.refcount = 0
        self.type_tag = type_tag


# ---------------------------------------------------------------------------
# Pool state: the keyed Dict + a monotonic LRU tick + a monotonic uploaded-bytes
# counter (H2D-on-miss bytes; the dedup measurement reads this). One struct so a
# single `_Global` owns all of it (one guarded pointer, no cross-global races).
# ---------------------------------------------------------------------------
struct ColPoolState(Movable):
    var cols: Dict[String, PooledColumn]
    var tick: Int
    var uploaded_bytes: Int64
    # Side-bookkeeping of the aggregate residency cache (`_pin2` in
    # gpu_kernels.mojo), which holds Movable-only GpuPinned values that the
    # pinned-nightly Dict cannot iterate. We mirror its live signature keys (in
    # insertion order) + the running sum of their SegResident footprints so the
    # budget can charge them and pick an LRU-ish victim WITHOUT iterating _pin2.
    # gpu_kernels keeps these in lockstep with _pin2 (only when the flag is on).
    var pin2_keys: List[String]
    var pin2_bytes: Int
    # Phase 2 (cost-aware placement) observability counters. Monotonic over the
    # process lifetime; surfaced via gpu_colpool_status(). Diagnostic only -- they
    # never feed back into a decision, so they cannot affect results. `hits` /
    # `misses` count column-level resident reuse vs cold uploads (the hot-set
    # hit-rate); `evictions` counts pooled columns dropped under budget pressure
    # by EITHER policy (the LRU-vs-keep-benefit A/B reads this).
    var hits: Int
    var misses: Int
    var evictions: Int

    def __init__(out self):
        self.cols = Dict[String, PooledColumn]()
        self.tick = 0
        self.uploaded_bytes = Int64(0)
        self.pin2_keys = []
        self.pin2_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0


def _make_col_pool() -> ColPoolState:
    return ColPoolState()


comptime _col_pool = _Global["mojo_gpu_col_pool", _make_col_pool]


def col_pool_ptr() raises -> UnsafePointer[ColPoolState, MutAnyOrigin]:
    return _col_pool.get_or_create_ptr()


# ---------------------------------------------------------------------------
# pool_bytes: total resident VRAM held by the pool (sum of per-column `bytes`).
# Used (together with the cached _pin2 GpuPinned footprints, computed on the
# caller side) to enforce GPU_OP_PIN_BUDGET_MB when GPU_OP_COLPOOL is on.
# ---------------------------------------------------------------------------
def pool_bytes() raises -> Int:
    ref st = col_pool_ptr()[]
    var total = 0
    # Iterate KEYS (not .items()): the pinned-nightly Dict allows non-Copyable
    # stored values, but its value-yielding iterators (.items()/.values())
    # require Copyable. PooledColumn owns a DeviceBuffer and is Movable-only, so
    # we index by key. (Same pattern in every pool/_pin2 scan below.)
    for k in st.cols:
        total += st.cols[k].bytes
    return total


# ---------------------------------------------------------------------------
# uploaded_bytes: monotonic count of bytes actually pushed H2D on pool misses.
# The dedup proof reads the DELTA across queries: once a shared column is
# resident, later signatures HIT and add nothing -> the delta shrinks.
# ---------------------------------------------------------------------------
def uploaded_bytes() raises -> Int64:
    return col_pool_ptr()[].uploaded_bytes


# ---------------------------------------------------------------------------
# Phase 2 observability accessors (surfaced via gpu_colpool_status). All are
# diagnostic-only reads of the monotonic counters / live pool state.
# ---------------------------------------------------------------------------
def pool_hits() raises -> Int:
    return col_pool_ptr()[].hits


def pool_misses() raises -> Int:
    return col_pool_ptr()[].misses


def pool_evictions() raises -> Int:
    return col_pool_ptr()[].evictions


# Number of columns currently resident in the pool.
def pool_resident_cols() raises -> Int:
    ref st = col_pool_ptr()[]
    var n = 0
    for k in st.cols:  # key iteration (see pool_bytes note)
        _ = k
        n += 1
    return n


# Number of resident columns that are "promoted" (access_count >= the promotion
# threshold). 0 when promotion is disabled (threshold 0). Lets the A/B confirm
# the hot set the cost-aware policy is protecting.
def pool_promoted_cols() raises -> Int:
    var threshold = _promote_threshold()
    if threshold <= 0:
        return 0
    ref st = col_pool_ptr()[]
    var n = 0
    for k in st.cols:  # key iteration (see pool_bytes note)
        if st.cols[k].access_count >= threshold:
            n += 1
    return n


# ---------------------------------------------------------------------------
# is_poolable: Phase 1 fail-closed gate. Only `ordering == ORDERING_STORAGE` and
# `representation == REPR_INT64_PACKED` (aggregate int64-packed) are poolable.
# Anything else => caller uploads as today (correct, no dedup).
# ---------------------------------------------------------------------------
def is_poolable(representation: Int, ordering: String) -> Bool:
    return representation == REPR_INT64_PACKED and ordering == ORDERING_STORAGE


# ---------------------------------------------------------------------------
# col_pool_type_tag: the contract TypeTag a RESIDENT column was uploaded with
# (TYPE_DATE / TYPE_DECIMAL / ...), or 0 if the key is not resident. The
# SKIP-MATERIALIZE finalize seeds the decimal SCALE of an OMITTED slot from this
# (its FedColumn is never filled). Reading an absent key returns 0 (the caller
# only calls this for a key it proved resident at materialize time).
# ---------------------------------------------------------------------------
def col_pool_type_tag(key: String) raises -> Int64:
    ref st = col_pool_ptr()[]
    if key not in st.cols:
        return Int64(0)
    return st.cols[key].type_tag


# ---------------------------------------------------------------------------
# col_pool_lease: SKIP-MATERIALIZE probe-time lease. Take an in-use lease
# (refcount++) on a RESIDENT column so it CANNOT be evicted between the omit
# decision (materialize_sql) and the finalize that sources it from the pool.
# Returns True on a HIT (lease taken), False if not resident (caller must NOT
# omit -> feed the column). Also refreshes the LRU tick (it is about to be used).
# Pairs with col_pool_borrow (finalize reads the buffer without a 2nd bump) +
# release_lease (drop when the owning GpuPinned is evicted, or on a backstop).
# ---------------------------------------------------------------------------
def col_pool_lease(key: String) raises -> Bool:
    ref st = col_pool_ptr()[]
    if key not in st.cols:
        return False
    st.tick += 1
    st.cols[key].last_use = st.tick
    st.cols[key].access_count += 1
    st.cols[key].refcount += 1
    st.hits += 1  # skip-materialize resident reuse (counts toward hit-rate)
    return True


# col_pool_borrow: SKIP-MATERIALIZE finalize-time read of an already-LEASED
# resident column. Returns ok=True + the buffer handle WITHOUT bumping refcount
# (the materialize-time col_pool_lease already holds the +1, whose ownership the
# caller transfers to the GpuPinned via pool_lease_keys). ok=False if the column
# is somehow gone (should be impossible while leased -> the was-hit backstop). The
# dummy buffer on the miss path is never read.
def col_pool_borrow(
    ctx: DeviceContext, key: String
) raises -> EnsureResult:
    ref st = col_pool_ptr()[]
    if key not in st.cols:
        return EnsureResult(False, False, ctx.enqueue_create_buffer[DType.int64](1))
    return EnsureResult(True, True, st.cols[key].buf)  # refcounted handle


# ---------------------------------------------------------------------------
# col_pool_resident_nrows: SKIP-MATERIALIZE residency probe used at
# materialize-SQL time (BEFORE the fact query runs, so the exact n_rows is not
# yet known to the caller). Scans for a resident column matching
# (table, column, representation, ordering) at ANY n_rows and returns its
# n_rows, or -1 if none. For a predicate-independent (full-table) fact column the
# pool holds at most one n_rows per (table,column), so the first match is THE
# resident row count -- which the caller then folds into the exact ColKey used by
# the finalize (a guaranteed HIT). Fail-closed: only STORAGE + int64-packed match
# (mirrors is_poolable); anything else returns -1 (never omitted).
# ---------------------------------------------------------------------------
def col_pool_resident_nrows(
    table: String, column: String, representation: Int, ordering: String
) raises -> Int:
    if not is_poolable(representation, ordering):
        return -1
    ref st = col_pool_ptr()[]
    var prefix = (
        table
        + _KEY_SEP
        + column
        + _KEY_SEP
        + String(representation)
        + _KEY_SEP
        + ordering
        + _KEY_SEP
    )
    for k in st.cols:  # key iteration (see pool_bytes note)
        # A ColKey is "<table>\x1f<col>\x1f<repr>\x1f<ordering>\x1f<n_rows>": the
        # only field after the final separator is n_rows, so a prefix match
        # uniquely identifies this (table,column,repr,ordering) and the suffix is
        # the resident row count. _KEY_SEP cannot appear in any field, so the
        # prefix can never match a different column whose name embeds the prefix.
        if _has_prefix(k, prefix):
            return st.cols[k].n_rows
    return -1


# Byte-level prefix test (no dependence on a possibly-renamed String.startswith).
def _has_prefix(s: String, prefix: String) -> Bool:
    var pn = prefix.byte_length()
    if s.byte_length() < pn:
        return False
    var sb = s.as_bytes()
    var pb = prefix.as_bytes()
    for i in range(pn):
        if sb[i] != pb[i]:
            return False
    return True


# ===-------------------------------------------------------------------===#
# Phase 2 -- cost-aware placement (RESIDENT_POOL_PLAN.md §C, the Mordred lever).
#
# Plain LRU evicts the oldest-touched column regardless of how often it is reused
# or how expensive it is to reload. The cost-aware policy instead evicts the
# column with the lowest KEEP-BENEFIT, a knapsack value-density:
#
#     keep_benefit(col) = access_count * reload_cost(col) / bytes(col)
#         reload_cost(col) ~= FIXED_OVERHEAD_US + bytes / H2D_BANDWIDTH
#
# i.e. value (how often we avoid a reload * how costly that reload is) per unit
# of VRAM. Hot, expensive-to-reload, not-too-large columns score high and stay;
# cold/cheap/huge ones score low and evict first. The fixed per-upload overhead
# makes small columns relatively MORE valuable per byte (the launch/latency floor
# dominates a tiny transfer), matching the "keep hot small columns" intuition.
#
# CORRECTNESS: eviction only changes WHICH column is cold-rebuilt on its next
# touch (invariant #2: evicted => COLD re-upload => slower, never wrong). So this
# is free of result risk; it is a residency-management lever, not a math change.
# Gated by GPU_OP_COLPOOL_COSTAWARE so plain LRU stays the default + the A/B is
# clean. All model parameters are env-tunable (Apple unified-mem vs NVIDIA PCIe
# have very different reload_cost -- calibrate per platform).
# ===-------------------------------------------------------------------===#

# Cost-aware policy gate. Composes with GPU_OP_COLPOOL=1|2 (it only swaps the
# eviction victim selection). Off => evict_victim falls back to plain LRU.
def costaware_on() -> Bool:
    return getenv("GPU_OP_COLPOOL_COSTAWARE", "") != ""


# Fixed per-upload overhead (microseconds): the H2D launch/latency floor a reload
# pays regardless of size. Env GPU_OP_COLPOOL_RELOAD_FIXED_US (default 10.0).
def _reload_fixed_us() -> Float64:
    var env = getenv("GPU_OP_COLPOOL_RELOAD_FIXED_US", "")
    if env != "":
        try:
            var v = atof(env)
            if v >= 0.0:
                return v
        except:
            pass
    return 10.0


# Effective H2D bandwidth in BYTES PER MICROSECOND. Env
# GPU_OP_COLPOOL_RELOAD_BW_GBPS gives GB/s (1 GB/s == 1000 bytes/us); default
# 12 GB/s (pageable PCIe gen3-ish; Apple unified is much higher -- recalibrate
# there, e.g. 100+). Clamped > 0 so reload_cost is finite.
def _reload_bw_bytes_per_us() -> Float64:
    var gbps = 12.0
    var env = getenv("GPU_OP_COLPOOL_RELOAD_BW_GBPS", "")
    if env != "":
        try:
            var v = atof(env)
            if v > 0.0:
                gbps = v
        except:
            pass
    return gbps * 1000.0


# Promotion threshold: a resident column whose access_count reaches this is
# "promoted" and survives budget pressure as long as ANY non-promoted column is
# evictable (proactive data placement -- keep proven-hot columns pinned). Env
# GPU_OP_COLPOOL_PROMOTE_HITS (default 4); 0 disables promotion (pure benefit
# ranking). Promotion is a PREFERENCE, not a hard pin: if every evictable column
# is promoted we still evict the lowest-benefit one (never OOM over a preference).
def _promote_threshold() -> Int:
    var env = getenv("GPU_OP_COLPOOL_PROMOTE_HITS", "")
    if env != "":
        try:
            var v = Int(env)
            if v >= 0:
                return v
        except:
            pass
    return 4


# keep_benefit for one resident column. Higher = more worth keeping. Pure
# function of the column's tracked stats + the env reload model; no mutation.
def keep_benefit(access_count: Int, bytes: Int) -> Float64:
    var b = Float64(bytes if bytes > 0 else 1)
    var reload_cost = _reload_fixed_us() + b / _reload_bw_bytes_per_us()
    return Float64(access_count) * reload_cost / b


# ---------------------------------------------------------------------------
# evict_lru: free the least-recently-used EVICTABLE (refcount == 0) pooled
# column. Returns its freed byte count, or 0 if nothing is evictable (every
# entry is leased / in flight). Caller-driven under budget pressure; mirrors the
# C++ `EvictOneLRU` shape. Never evicts a leased entry (correctness invariant).
# ---------------------------------------------------------------------------
def evict_lru() raises -> Int:
    ref st = col_pool_ptr()[]
    var victim_key = String("")
    var have = False
    var best = 0
    for k in st.cols:  # key iteration (see pool_bytes note)
        if st.cols[k].refcount > 0:
            continue  # in flight; never evict
        if not have or st.cols[k].last_use < best:
            best = st.cols[k].last_use
            victim_key = k
            have = True
    if not have:
        return 0
    var freed = st.cols[victim_key].bytes
    _ = st.cols.pop(victim_key)  # drops the DeviceBuffer -> frees VRAM
    st.evictions += 1
    return freed


# ---------------------------------------------------------------------------
# evict_by_keep_benefit: cost-aware eviction. Free the EVICTABLE (refcount == 0)
# pooled column with the LOWEST keep_benefit, preferring non-promoted columns.
# Two-pass:
#   1. among non-promoted evictable columns, pick min keep_benefit (tiebreak:
#      older last_use) -- the usual case.
#   2. only if every evictable column is promoted, pick min keep_benefit among
#      those (promotion yields under genuine pressure -- never OOM over it).
# Returns freed bytes, or 0 if nothing is evictable. Mirrors evict_lru's contract
# (refcount>0 never evicted; counts the eviction) so it is a drop-in victim picker.
# ---------------------------------------------------------------------------
def evict_by_keep_benefit() raises -> Int:
    ref st = col_pool_ptr()[]
    var threshold = _promote_threshold()
    var victim_key = String("")
    var have = False
    var best_score = Float64(0)
    var best_tick = 0
    # promoted_fallback_* tracks the lowest-benefit PROMOTED victim, used only if
    # no non-promoted column is evictable.
    var fb_key = String("")
    var have_fb = False
    var fb_score = Float64(0)
    var fb_tick = 0
    for k in st.cols:  # key iteration (see pool_bytes note)
        if st.cols[k].refcount > 0:
            continue  # in flight; never evict (correctness invariant)
        var ac = st.cols[k].access_count
        var by = st.cols[k].bytes
        var score = keep_benefit(ac, by)
        var lu = st.cols[k].last_use
        var promoted = threshold > 0 and ac >= threshold
        if promoted:
            # candidate only for the fallback pass
            if (not have_fb) or score < fb_score or (
                score == fb_score and lu < fb_tick
            ):
                fb_score = score
                fb_tick = lu
                fb_key = k
                have_fb = True
            continue
        # non-promoted: the preferred victim pool. Min score, older tick breaks ties.
        if (not have) or score < best_score or (
            score == best_score and lu < best_tick
        ):
            best_score = score
            best_tick = lu
            victim_key = k
            have = True
    if not have:
        # No non-promoted evictable column: fall back to the lowest-benefit
        # promoted one (promotion yields under real pressure).
        if not have_fb:
            return 0  # everything is leased / in flight
        victim_key = fb_key
    var freed = st.cols[victim_key].bytes
    _ = st.cols.pop(victim_key)  # drops the DeviceBuffer -> frees VRAM
    st.evictions += 1
    return freed


# ---------------------------------------------------------------------------
# evict_victim: the policy dispatcher every budget-pressure call site uses. Picks
# cost-aware keep-benefit eviction when GPU_OP_COLPOOL_COSTAWARE is set, else the
# default plain LRU. Returns freed bytes (0 if nothing evictable). Both callers
# (_colpool_make_room in gpu_kernels.mojo and ensure_column's OOM-retry) route
# through here so the policy applies uniformly.
# ---------------------------------------------------------------------------
def evict_victim() raises -> Int:
    if costaware_on():
        return evict_by_keep_benefit()
    return evict_lru()


# ---------------------------------------------------------------------------
# release_lease: drop one in-use lease on a pooled column and refresh its LRU
# tick (it was just used, so it should be among the most-recently-used). Called
# when a cached GpuPinned that assembled from this column is evicted (the
# lease's lifetime == the cached entry's lifetime), so the NEXT signature can
# still dedup until real pressure forces eviction. Safe no-op if the column is
# gone or already at refcount 0.
# ---------------------------------------------------------------------------
def release_lease(key: String) raises:
    ref st = col_pool_ptr()[]
    if key not in st.cols:
        return
    if st.cols[key].refcount > 0:
        st.cols[key].refcount -= 1
        st.tick += 1
        st.cols[key].last_use = st.tick


# ---------------------------------------------------------------------------
# _pin2 side-bookkeeping. gpu_kernels calls register/unregister around every
# _pin2 insertion/eviction (flag-on only) so the budget can account for the
# aggregate residency footprint without iterating the Movable-only _pin2 Dict.
# ---------------------------------------------------------------------------
def pin2_register(sig: String, footprint_bytes: Int) raises:
    ref st = col_pool_ptr()[]
    # Defensive: if a signature is re-registered (shouldn't happen -- a re-cache
    # would be a WARM hit), don't double-count; replace its byte charge.
    var found = False
    for i in range(len(st.pin2_keys)):
        if st.pin2_keys[i] == sig:
            found = True
            break
    if not found:
        st.pin2_keys.append(sig)
    st.pin2_bytes += footprint_bytes


def pin2_unregister(sig: String, footprint_bytes: Int) raises:
    ref st = col_pool_ptr()[]
    var out: List[String] = []
    for i in range(len(st.pin2_keys)):
        if st.pin2_keys[i] != sig:
            out.append(st.pin2_keys[i])
    st.pin2_keys = out^
    st.pin2_bytes -= footprint_bytes
    if st.pin2_bytes < 0:
        st.pin2_bytes = 0


def pin2_resident_bytes() raises -> Int:
    return col_pool_ptr()[].pin2_bytes


# Oldest (insertion-order, == LRU-ish for a single-source query stream) live
# _pin2 signature key, or "" if none. The conservative eviction victim.
def pin2_oldest_key() raises -> String:
    ref st = col_pool_ptr()[]
    if len(st.pin2_keys) == 0:
        return String("")
    return st.pin2_keys[0]


# Result of ensure_column. `ok` False => not poolable / allocation failed /
# nothing evictable => caller MUST fall back to the non-pooled path (upload
# straight to cols_d as today); `buf` is then a 1-element dummy, never read.
# `ok` True => `buf` is the resident pooled buffer to D2D-copy into the slot, and
# `was_hit` distinguishes a resident HIT (no H2D) from a MISS (one H2D, purely
# for the HIT/MISS diagnostic). DeviceBuffer is a cheap refcounted handle.
struct EnsureResult(Movable):
    var ok: Bool
    var was_hit: Bool
    var buf: DeviceBuffer[DType.int64]

    def __init__(
        out self, ok: Bool, was_hit: Bool, var buf: DeviceBuffer[DType.int64]
    ):
        self.ok = ok
        self.was_hit = was_hit
        self.buf = buf^


# ---------------------------------------------------------------------------
# ensure_column: the dedup entry point. On HIT (resident): bump access_count /
# last_use, take a lease (refcount++), return ok=True/was_hit=True -- caller
# SKIPS the H2D and D2D-copies the resident buffer into its slot. On MISS:
# upload the one column to its own DeviceBuffer[int64](n_rows) (the ONLY H2D
# here), insert, lease it, charge uploaded_bytes, return ok=True/was_hit=False.
#
# BOUNDING (flag-gated, caller passes the live budget): on a MISS that would
# exceed the budget the caller has already evicted what it can (pooled LRU first,
# then _pin2); ensure_column does an OOM-retry on a null device allocation by
# evicting one more pooled column and retrying. If allocation still fails (or the
# column is not poolable), returns ok=False and the caller falls back. NEVER
# crashes / never returns a wrong buffer.
#
# `host_col_ptr` points at the int64-widened column values (length n_rows) the
# caller already materialized for packing -- the SAME bytes the packing loop
# writes, so the pooled upload is bit-identical to a fresh pack.
#
# (SKIP-MATERIALIZE reads an already-leased resident column via col_pool_borrow,
# NOT ensure_column, so this path never sees an OMITTED column's unfilled host
# buffer -- ensure_column is only called for columns that ARE fed.)
#
# Returns the resident buffer inside EnsureResult for the caller to D2D-copy.
# ---------------------------------------------------------------------------
def ensure_column(
    ctx: DeviceContext,
    key: String,
    representation: Int,
    ordering: String,
    host_col_ptr: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    type_tag: Int64,
) raises -> EnsureResult:
    # Phase 1 fail-closed: only STORAGE + int64-packed are poolable. The dummy
    # buffer is never read (ok=False).
    if not is_poolable(representation, ordering):
        return EnsureResult(False, False, ctx.enqueue_create_buffer[DType.int64](1))

    ref st = col_pool_ptr()[]

    # ---- HIT: resident buffer; skip the H2D, lease it, refresh LRU. ----
    if key in st.cols:
        st.tick += 1
        st.cols[key].last_use = st.tick
        st.cols[key].access_count += 1
        st.cols[key].refcount += 1
        st.hits += 1
        return EnsureResult(True, True, st.cols[key].buf)  # refcounted handle

    # ---- MISS: upload the one column once into its own int64 buffer. ----
    # OOM-retry: if the device allocation throws (out of VRAM), evict one more
    # evictable pooled column and retry. Give up (fall back) when nothing more
    # is evictable -- the alloc would just keep failing.
    var dev: DeviceBuffer[DType.int64]
    var n = n_rows if n_rows > 0 else 1
    while True:
        try:
            dev = ctx.enqueue_create_buffer[DType.int64](n)
            break
        except:
            var freed = evict_victim()
            if freed == 0:
                # Nothing evictable and the alloc failed -> not poolable now.
                return EnsureResult(
                    False, False, ctx.enqueue_create_buffer[DType.int64](1)
                )
            # else: retry the allocation with more VRAM free.

    # The single H2D upload of this column (the whole point of the dedup: paid
    # once per physical column, not once per signature).
    ctx.enqueue_copy(dev, host_col_ptr)

    var pc = PooledColumn(dev^, n_rows, type_tag)
    st.tick += 1
    pc.last_use = st.tick
    pc.access_count = 1
    pc.refcount = 1  # leased by the in-flight query that just inserted it
    st.misses += 1
    st.uploaded_bytes += Int64(pc.bytes)
    var ret = pc.buf  # cheap refcounted handle copy before moving into the map
    st.cols[key] = pc^
    return EnsureResult(True, False, ret^)
