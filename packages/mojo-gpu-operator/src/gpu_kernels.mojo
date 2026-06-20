"""C-ABI GPU kernels for the mojo-gpu-operator extension.

Exposes a persistent-context cosine-distance engine as three `extern "C"`
entry points the C++ `GpuCosinePhysicalOp` calls over raw FLOAT[K] column
buffers:

    void *mojo_gpu_cosine_init(const float *q, int64_t K, int64_t capacity_rows);
    int32_t mojo_gpu_cosine_run(void *handle, const float *emb, int64_t n_rows,
                                float *out);   // 0 = ok, nonzero = error -> CPU fallback
    void mojo_gpu_cosine_free(void *handle);

The handle owns a DeviceContext + resident in/out/query DeviceBuffers, created
once per pipeline. `run` stages a morsel's array child into the resident input
buffer, launches the warp-reduction kernel (one warp per row, `warp.sum`, no
barriers), and copies the distances back. Built to `build/gpu_kernels.o` by
build.sh and linked straight into the one extension .so.

The compute core is the proven warp kernel from
`benchmark/gpu_table_function_poc.mojo`, with K promoted to a runtime argument.
"""

from std.gpu import block_idx, thread_idx, barrier
from std.gpu.primitives import warp
from std.gpu.memory import AddressSpace
from gpu_platform import WARP
from std.gpu.host import DeviceContext, DeviceBuffer, HostBuffer
from std.ffi import _Global
from std.io import FileDescriptor
from std.os import abort, getenv
from std.sys.info import (
    has_nvidia_gpu_accelerator,
    has_apple_gpu_accelerator,
)
from std.math import sqrt
from std.memory import alloc, memcpy, stack_allocation
from std.time import perf_counter_ns

from tc_knn import run_tc_knn_batch, tc_knn_supported
from tc_knn_apple import run_tc_knn_apple_batch, tc_knn_apple_supported

from native_decode import (
    bitpacking_decode_kernel,
    uncompressed_decode_kernel,
    BP_META_GROUP_SIZE,
)

import descriptor
from descriptor import (
    GpuPlanDescriptor,
    GpuAggregate,
    RawPlanReader,
    build_descriptor_impl,
)
from raw_plan_tags import (
    KIND_UNKNOWN,
    KIND_Q6,
    KIND_Q1,
    KIND_Q14,
    KIND_Q3,
    KIND_Q5,
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
    STRAT_HASH_GROUP,
    IDX_NONE,
    TYPE_DATE,
    TYPE_INTEGER,
    TYPE_DECIMAL,
    TYPE_BIGINT,
    TYPE_HUGEINT,
    TYPE_DOUBLE,
    TYPE_VARCHAR,
    AGG_SUM,
    AGG_AVG,
    AGG_COUNT_STAR,
    CMP_EQ,
    CMP_NE,
    CMP_GE,
    CMP_GT,
    CMP_LE,
    CMP_LT,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_PROMO_PRED,
    OP_LOAD_DIM,
    OP_EQ,
)
from segreduce import (
    segreduce_upload,
    segreduce_upload_from_packed,
    segreduce_upload_from_colptr,
    segreduce_run,
    segreduce_run_hash,
    HashGroupResult,
    SegResident,
)
from col_pool import (
    col_key,
    ensure_column,
    release_lease,
    evict_victim,
    pool_bytes,
    uploaded_bytes as col_pool_uploaded_bytes,
    pool_hits,
    pool_misses,
    pool_evictions,
    pool_resident_cols,
    pool_promoted_cols,
    costaware_on as col_pool_costaware_on,
    pin2_register,
    pin2_unregister,
    pin2_resident_bytes,
    pin2_oldest_key,
    col_pool_type_tag,
    col_pool_resident_nrows,
    col_pool_lease,
    col_pool_borrow,
    REPR_INT64_PACKED,
    ORDERING_STORAGE,
)
from std.collections import Dict


# ---------------------------------------------------------------------------
# Shared, process-wide DeviceContext.
#
# `DeviceContext()` pays a ~32 ms one-time GPU/driver init. The struct is
# `RegisterPassable` with a single `_handle` pointer, so copying a DeviceContext
# copies the handle and shares the same underlying AsyncRT context — copies are
# free, only construction is expensive. Every "pin" engine here only needs a
# context to alloc buffers / enqueue kernels / copy / sync, all of which are
# fine on a shared context. So we create it exactly once (lazily, on first
# `shared_device_context()` call, or eagerly at extension LOAD via
# `mojo_gpu_ctx_init`) and hand out cheap copies thereafter.
#
# `_Global` provides the proper process-wide storage + lazy guarded init. NOT
# thread-safe to *create* concurrently, but pins in this extension run
# single-threaded per query (every pin engine sets MaxThreads()==1 / declares
# ParallelSource()==false), and the eager LOAD-time init means the expensive
# creation happens before any query runs.
# ---------------------------------------------------------------------------
def _init_shared_device_context() -> DeviceContext:
    try:
        return DeviceContext()
    except e:
        abort("failed to create shared DeviceContext: " + String(e))


comptime _shared_ctx = _Global[
    "mojo_gpu_shared_ctx", _init_shared_device_context
]


def shared_device_context() raises -> DeviceContext:
    # Returns a cheap, shared copy of the process-wide DeviceContext, creating
    # the (expensive) underlying context on first call.
    return _shared_ctx.get_or_create_ptr()[]


# Force creation of the shared context (called once from C++ LoadInternal so the
# ~32 ms is paid at extension LOAD time, before any query runs).
@export("mojo_gpu_ctx_init")
def mojo_gpu_ctx_init() abi("C"):
    try:
        _ = shared_device_context()
    except:
        pass


# ---------------------------------------------------------------------------
# Kernel: one warp (32 lanes) per row; lane-strided dot/norm, warp.sum, no barriers.
# ---------------------------------------------------------------------------
def cosine_kernel_warp(
    emb: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    q: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    res: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    qnorm: Float32,
):
    var row = Int(block_idx.x)
    if row >= n_rows:
        return
    var lane = Int(thread_idx.x)
    var base = row * K
    var dot = Float32(0)
    var na = Float32(0)
    var i = lane
    while i < K:
        var av = emb[base + i]
        dot += av * q[i]
        na += av * av
        i += WARP
    dot = warp.sum(dot)
    na = warp.sum(na)
    if lane == 0:
        var denom = sqrt(na) * qnorm
        res[row] = Float32(1) - dot / denom if denom != 0 else Float32(0)


# ---------------------------------------------------------------------------
# fp16-resident variant of `cosine_kernel_warp`.
#
# Identical algorithm and tie-break to the fp32 kernel, but the embedding matrix
# is RESIDENT as float16 (half the VRAM, half the bandwidth of the dominant
# scan). Storage is half; MATH stays fp32: each float16 element is loaded and
# immediately cast to Float32, so the dot product and the L2-norm accumulate in
# fp32. The query stays fp32 (it is tiny and per-call). This preserves nearly all
# accuracy for unit-normalized embeddings while halving the bandwidth-bound read.
# ---------------------------------------------------------------------------
def cosine_kernel_warp_f16(
    emb: UnsafePointer[Scalar[DType.float16], MutAnyOrigin],
    q: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    res: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    qnorm: Float32,
):
    var row = Int(block_idx.x)
    if row >= n_rows:
        return
    var lane = Int(thread_idx.x)
    var base = row * K
    var dot = Float32(0)
    var na = Float32(0)
    var i = lane
    while i < K:
        # Load the stored half, cast to fp32 BEFORE any arithmetic so the dot and
        # norm accumulate in fp32 (only the storage/read is half-width).
        var av = emb[base + i].cast[DType.float32]()
        dot += av * q[i]
        na += av * av
        i += WARP
    dot = warp.sum(dot)
    na = warp.sum(na)
    if lane == 0:
        var denom = sqrt(na) * qnorm
        res[row] = Float32(1) - dot / denom if denom != 0 else Float32(0)


# ---------------------------------------------------------------------------
# Persistent state owned by the opaque handle.
# ---------------------------------------------------------------------------
struct CosineState(Movable):
    var ctx: DeviceContext
    var in_buf: DeviceBuffer[DType.float32]
    var out_buf: DeviceBuffer[DType.float32]
    var q_buf: DeviceBuffer[DType.float32]
    var qnorm: Float32
    var K: Int
    var capacity: Int

    def __init__(
        out self,
        var ctx: DeviceContext,
        var in_buf: DeviceBuffer[DType.float32],
        var out_buf: DeviceBuffer[DType.float32],
        var q_buf: DeviceBuffer[DType.float32],
        qnorm: Float32,
        K: Int,
        capacity: Int,
    ):
        self.ctx = ctx^
        self.in_buf = in_buf^
        self.out_buf = out_buf^
        self.q_buf = q_buf^
        self.qnorm = qnorm
        self.K = K
        self.capacity = capacity


# ---------------------------------------------------------------------------
# C-ABI entry points.
# ---------------------------------------------------------------------------
# Returns the handle as an integer address (0 == failure); UnsafePointer is
# non-nullable, and an Int return is ABI-compatible with the C++ `void*`.
@export("mojo_gpu_cosine_init")
def mojo_gpu_cosine_init(
    q: UnsafePointer[Float32, ImmutAnyOrigin],
    K: Int,
    capacity_rows: Int,
) abi("C") -> Int:
    try:
        var ctx = shared_device_context()
        var in_buf = ctx.enqueue_create_buffer[DType.float32](capacity_rows * K)
        var out_buf = ctx.enqueue_create_buffer[DType.float32](capacity_rows)
        var q_buf = ctx.enqueue_create_buffer[DType.float32](K)
        ctx.synchronize()

        # Upload the query vector and compute its norm on the host.
        var nrm = Float32(0)
        with q_buf.map_to_host() as h:
            var hp = h.unsafe_ptr()
            for i in range(K):
                var v = q[i]
                hp[i] = v
                nrm += v * v
        var qnorm = sqrt(nrm)
        ctx.synchronize()

        var p = alloc[CosineState](1)
        p.init_pointee_move(
            CosineState(
                ctx^, in_buf^, out_buf^, q_buf^, qnorm, K, capacity_rows
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_gpu_cosine_run")
def mojo_gpu_cosine_run(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    emb: UnsafePointer[Float32, ImmutAnyOrigin],
    n_rows: Int,
    out_ptr: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[CosineState]()
        ref st = s[]
        if n_rows > st.capacity:
            return 2
        # Stage this morsel into the resident input buffer (a copy). A plain
        # enqueue_copy is the fast path: the driver stages the pageable source
        # through its own pinned bounce buffer in one shot. (map_to_host is the
        # WRONG tool here — it is bidirectional, DMAing device->host on enter, so
        # for a pure upload it ~3.4x slower on PCIe; measured on RTX 4090.)
        var in_sub = DeviceBuffer(
            st.ctx, st.in_buf.unsafe_ptr(), n_rows * st.K, owning=False
        )
        st.ctx.enqueue_copy(in_sub, emb)
        st.ctx.enqueue_function[cosine_kernel_warp](
            st.in_buf,
            st.q_buf,
            st.out_buf,
            n_rows,
            st.K,
            st.qnorm,
            grid_dim=n_rows,
            block_dim=WARP,
        )
        var out_sub = DeviceBuffer(
            st.ctx, st.out_buf.unsafe_ptr(), n_rows, owning=False
        )
        st.ctx.enqueue_copy(out_ptr, out_sub)
        st.ctx.synchronize()
        return 0
    except:
        return 3


@export("mojo_gpu_cosine_free")
def mojo_gpu_cosine_free(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[CosineState]()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# Pin-resident engine: the whole column lives on the GPU; each query is one
# launch over the resident buffer (the query vector changes per call). This is
# the Phase-2 win, exposed via the gpu_cosine() table function.
# ===-------------------------------------------------------------------===#
struct PinState(Movable):
    var ctx: DeviceContext
    var emb_dev: DeviceBuffer[
        DType.float32
    ]  # resident column: n_rows * K floats
    var q_dev: DeviceBuffer[DType.float32]
    var out_dev: DeviceBuffer[DType.float32]  # resident dist scratch: n_rows
    # Resident top-k scratch, allocated ONCE at pin time and reused every query
    # so the warm top-k path does ZERO per-call device/host allocation. Sized for
    # the worst case at this n_rows: nblocks (capped, depends only on n_rows/k) at
    # the largest k we support (TOPK_MAX) => cand_cap candidates.
    var cand_dist_dev: DeviceBuffer[DType.float32]
    var cand_id_dev: DeviceBuffer[DType.int64]
    var cand_dist_h: UnsafePointer[Float32, MutAnyOrigin]
    var cand_id_h: UnsafePointer[Int64, MutAnyOrigin]
    var cand_cap: Int  # capacity of the candidate buffers (>= nblocks*k)
    var n_rows: Int
    var K: Int

    def __init__(
        out self,
        var ctx: DeviceContext,
        var emb_dev: DeviceBuffer[DType.float32],
        var q_dev: DeviceBuffer[DType.float32],
        var out_dev: DeviceBuffer[DType.float32],
        var cand_dist_dev: DeviceBuffer[DType.float32],
        var cand_id_dev: DeviceBuffer[DType.int64],
        cand_dist_h: UnsafePointer[Float32, MutAnyOrigin],
        cand_id_h: UnsafePointer[Int64, MutAnyOrigin],
        cand_cap: Int,
        n_rows: Int,
        K: Int,
    ):
        self.ctx = ctx^
        self.emb_dev = emb_dev^
        self.q_dev = q_dev^
        self.out_dev = out_dev^
        self.cand_dist_dev = cand_dist_dev^
        self.cand_id_dev = cand_id_dev^
        self.cand_dist_h = cand_dist_h
        self.cand_id_h = cand_id_h
        self.cand_cap = cand_cap
        self.n_rows = n_rows
        self.K = K


# Pin a column: upload n_rows*K floats to a resident device buffer once.
# Returns the handle as an integer address (0 == failure).
@export("mojo_gpu_pin")
def mojo_gpu_pin(
    emb: UnsafePointer[Float32, ImmutAnyOrigin],
    n_rows: Int,
    K: Int,
) abi("C") -> Int:
    try:
        var ctx = shared_device_context()
        var emb_dev = ctx.enqueue_create_buffer[DType.float32](n_rows * K)
        var q_dev = ctx.enqueue_create_buffer[DType.float32](K)
        var out_dev = ctx.enqueue_create_buffer[DType.float32](n_rows)
        # Resident top-k candidate scratch, sized ONCE for the worst case at this
        # n_rows: nblocks depends only on n_rows/k (capped in _topk_nblocks), so the
        # max candidate count is at the largest k we support (TOPK_MAX). Every warm
        # top-k query reuses these instead of allocating per call.
        var cand_cap = _topk_nblocks(n_rows, TOPK_MAX) * TOPK_MAX
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
        # One-time pin upload. enqueue_copy of the pageable source is the fast
        # path: the driver bounces it through its own pinned buffer in one shot.
        # (map_to_host would be ~3.4x slower here — it is bidirectional and DMAs
        # device->host on enter; measured on RTX 4090.) Single sync afterwards;
        # the buffer allocs above are ordered on the one runtime stream.
        ctx.enqueue_copy(emb_dev, emb)
        ctx.synchronize()
        var cand_dist_h = alloc[Float32](cand_cap)
        var cand_id_h = alloc[Int64](cand_cap)
        var p = alloc[PinState](1)
        p.init_pointee_move(
            PinState(
                ctx^,
                emb_dev^,
                q_dev^,
                out_dev^,
                cand_dist_dev^,
                cand_id_dev^,
                cand_dist_h,
                cand_id_h,
                cand_cap,
                n_rows,
                K,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Run one query over the pinned column: upload q (K floats), launch over all
# resident rows, copy the n_rows distances back into out_ptr.
@export("mojo_gpu_pin_query")
def mojo_gpu_pin_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    q: UnsafePointer[Float32, ImmutAnyOrigin],
    out_ptr: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[PinState]()
        ref st = s[]
        var qnorm = Float32(0)
        for i in range(st.K):
            qnorm += q[i] * q[i]
        qnorm = sqrt(qnorm)
        st.ctx.enqueue_copy(st.q_dev, q)
        st.ctx.enqueue_function[cosine_kernel_warp](
            st.emb_dev,
            st.q_dev,
            st.out_dev,
            st.n_rows,
            st.K,
            qnorm,
            grid_dim=st.n_rows,
            block_dim=WARP,
        )
        st.ctx.enqueue_copy(out_ptr, st.out_dev)
        st.ctx.synchronize()
        return 0
    except:
        return 3


@export("mojo_gpu_pin_free")
def mojo_gpu_pin_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[PinState]()
    # Free the cached host candidate buffers; the device buffers are owned by the
    # PinState's DeviceBuffer fields and released by destroy_pointee.
    ref st = p[]
    st.cand_dist_h.free()
    st.cand_id_h.free()
    p.destroy_pointee()
    p.free()


# Phase 1 column-pool measurement hook: the monotonic count of bytes actually
# pushed host->device on pool MISSES (GPU_OP_COLPOOL). The dedup proof reads the
# DELTA across queries -- once a shared column is resident later signatures HIT
# and add nothing, so the per-query delta shrinks. Returns 0 if the pool was
# never touched (flag off / no eligible query). Diagnostic only.
@export("mojo_gpu_colpool_uploaded_bytes")
def mojo_gpu_colpool_uploaded_bytes() abi("C") -> Int64:
    try:
        return col_pool_uploaded_bytes()
    except:
        return Int64(0)


# Current RESIDENT pool footprint (sum of pooled per-column buffer bytes) and the
# tracked _pin2 footprint. For the VRAM-bound assertion (stays under budget).
@export("mojo_gpu_colpool_pool_bytes")
def mojo_gpu_colpool_pool_bytes() abi("C") -> Int64:
    try:
        return Int64(pool_bytes())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_pin2_bytes")
def mojo_gpu_colpool_pin2_bytes() abi("C") -> Int64:
    try:
        return Int64(pin2_resident_bytes())
    except:
        return Int64(0)


# Phase 2 (cost-aware placement) observability. All diagnostic-only counters /
# live-state reads surfaced through gpu_colpool_status(); none feed back into a
# decision, so they cannot affect results.
# `hits`/`misses` are the column-level resident-reuse vs cold-upload counts (the
# hot-set hit-rate); `evictions` is the pooled-column drop count under budget
# pressure (the LRU-vs-keep-benefit A/B reads this); `resident_cols` /
# `promoted_cols` describe the current pool population; `costaware` echoes whether
# the cost-aware eviction policy is active.
@export("mojo_gpu_colpool_hits")
def mojo_gpu_colpool_hits() abi("C") -> Int64:
    try:
        return Int64(pool_hits())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_misses")
def mojo_gpu_colpool_misses() abi("C") -> Int64:
    try:
        return Int64(pool_misses())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_evictions")
def mojo_gpu_colpool_evictions() abi("C") -> Int64:
    try:
        return Int64(pool_evictions())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_resident_cols")
def mojo_gpu_colpool_resident_cols() abi("C") -> Int64:
    try:
        return Int64(pool_resident_cols())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_promoted_cols")
def mojo_gpu_colpool_promoted_cols() abi("C") -> Int64:
    try:
        return Int64(pool_promoted_cols())
    except:
        return Int64(0)


@export("mojo_gpu_colpool_costaware")
def mojo_gpu_colpool_costaware() abi("C") -> Int64:
    return Int64(1) if col_pool_costaware_on() else Int64(0)


# ===-------------------------------------------------------------------===#
# fp16-resident pin engine (additive; the fp32 path above is unchanged).
#
# Same contract as PinState/mojo_gpu_pin/mojo_gpu_pin_query_topk, but the
# resident embedding matrix is stored as float16: half the VRAM (doubling the
# max N at a given budget) and half the read bandwidth of the distance scan,
# which is the bandwidth floor at 1M rows. The fp32->fp16 conversion is done
# ONCE on the host at pin time (one-time cost), then uploaded as half. The
# distance kernel (`cosine_kernel_warp_f16`) casts each half back to fp32 and
# accumulates the dot/norm in fp32, so only storage is lossy. The candidate
# scratch, top-k partial kernel, and host merge are shared with the fp32 path.
#
# fp16 handles are a DISTINCT struct from PinState; free them via
# `mojo_gpu_pin_free_f16` (mojo_gpu_pin_free stays the fp32 free, unchanged).
# ===-------------------------------------------------------------------===#
struct PinStateF16(Movable):
    var ctx: DeviceContext
    var emb_dev: DeviceBuffer[DType.float16]  # resident column: n_rows*K halves
    var q_dev: DeviceBuffer[DType.float32]  # query stays fp32
    var out_dev: DeviceBuffer[DType.float32]  # resident dist scratch: n_rows
    var cand_dist_dev: DeviceBuffer[DType.float32]
    var cand_id_dev: DeviceBuffer[DType.int64]
    var cand_dist_h: UnsafePointer[Float32, MutAnyOrigin]
    var cand_id_h: UnsafePointer[Int64, MutAnyOrigin]
    var cand_cap: Int
    var n_rows: Int
    var K: Int

    def __init__(
        out self,
        var ctx: DeviceContext,
        var emb_dev: DeviceBuffer[DType.float16],
        var q_dev: DeviceBuffer[DType.float32],
        var out_dev: DeviceBuffer[DType.float32],
        var cand_dist_dev: DeviceBuffer[DType.float32],
        var cand_id_dev: DeviceBuffer[DType.int64],
        cand_dist_h: UnsafePointer[Float32, MutAnyOrigin],
        cand_id_h: UnsafePointer[Int64, MutAnyOrigin],
        cand_cap: Int,
        n_rows: Int,
        K: Int,
    ):
        self.ctx = ctx^
        self.emb_dev = emb_dev^
        self.q_dev = q_dev^
        self.out_dev = out_dev^
        self.cand_dist_dev = cand_dist_dev^
        self.cand_id_dev = cand_id_dev^
        self.cand_dist_h = cand_dist_h
        self.cand_id_h = cand_id_h
        self.cand_cap = cand_cap
        self.n_rows = n_rows
        self.K = K


# Pin a column as fp16: takes the SAME fp32 host embedding data, converts it to
# float16 on the host once, and uploads the halves to a resident device buffer.
# Returns the handle as an integer address (0 == failure).
@export("mojo_gpu_pin_f16")
def mojo_gpu_pin_f16(
    emb: UnsafePointer[Float32, ImmutAnyOrigin],
    n_rows: Int,
    K: Int,
) abi("C") -> Int:
    try:
        var ctx = shared_device_context()
        var emb_dev = ctx.enqueue_create_buffer[DType.float16](n_rows * K)
        var q_dev = ctx.enqueue_create_buffer[DType.float32](K)
        var out_dev = ctx.enqueue_create_buffer[DType.float32](n_rows)
        var cand_cap = _topk_nblocks(n_rows, TOPK_MAX) * TOPK_MAX
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)

        # One-time fp32->fp16 conversion on the host, then a single enqueue_copy
        # of the half buffer (the conversion cost is paid once at pin; every query
        # then reads half the bytes from VRAM). Convert into a plain host buffer
        # and let the driver bounce it to device in one shot: writing into
        # emb_dev.map_to_host() instead is ~3.4x slower, since map_to_host is
        # bidirectional and DMAs device->host on enter (measured on RTX 4090).
        var total = n_rows * K
        var h16 = alloc[Float16](total)
        for j in range(total):
            h16[j] = emb[j].cast[DType.float16]()
        ctx.enqueue_copy(emb_dev, h16)
        ctx.synchronize()  # kernels read emb_dev later; safe to free h16 now
        h16.free()

        var cand_dist_h = alloc[Float32](cand_cap)
        var cand_id_h = alloc[Int64](cand_cap)
        var p = alloc[PinStateF16](1)
        p.init_pointee_move(
            PinStateF16(
                ctx^,
                emb_dev^,
                q_dev^,
                out_dev^,
                cand_dist_dev^,
                cand_id_dev^,
                cand_dist_h,
                cand_id_h,
                cand_cap,
                n_rows,
                K,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Single-query exact top-k over the fp16-resident column. Same contract as
# `mojo_gpu_pin_query_topk`: query is fp32, only k (ids+dists) crosses PCIe, the
# (dist, rowid) tie-break and host merge are identical. The only difference is
# the distance kernel reads halves and casts to fp32 (math stays fp32).
@export("mojo_gpu_pin_query_topk_f16")
def mojo_gpu_pin_query_topk_f16(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    q: UnsafePointer[Float32, ImmutAnyOrigin],
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    if k <= 0 or k > TOPK_MAX:
        return 2
    try:
        var s = handle.bitcast[PinStateF16]()
        ref st = s[]
        # qnorm computed in fp32 from the fp32 query (matches the kernel denom).
        var qnorm = Float32(0)
        for i in range(st.K):
            qnorm += q[i] * q[i]
        qnorm = sqrt(qnorm)
        st.ctx.enqueue_copy(st.q_dev, q)

        var nblocks = _topk_nblocks(st.n_rows, k)
        var ncand = nblocks * k
        var cand_dist_dev = DeviceBuffer(
            st.ctx, st.cand_dist_dev.unsafe_ptr(), ncand, owning=False
        )
        var cand_id_dev = DeviceBuffer(
            st.ctx, st.cand_id_dev.unsafe_ptr(), ncand, owning=False
        )

        st.ctx.enqueue_function[cosine_kernel_warp_f16](
            st.emb_dev,
            st.q_dev,
            st.out_dev,
            st.n_rows,
            st.K,
            qnorm,
            grid_dim=st.n_rows,
            block_dim=WARP,
        )
        st.ctx.enqueue_function[topk_partial_kernel](
            st.out_dev,
            cand_dist_dev,
            cand_id_dev,
            st.n_rows,
            k,
            nblocks,
            grid_dim=nblocks,
            block_dim=WARP,
        )
        st.ctx.enqueue_copy(st.cand_dist_h, cand_dist_dev)
        st.ctx.enqueue_copy(st.cand_id_h, cand_id_dev)
        st.ctx.synchronize()

        _host_merge_topk(
            st.cand_dist_h, st.cand_id_h, ncand, k, out_ids, out_dists
        )
        return 0
    except:
        return 3


@export("mojo_gpu_pin_free_f16")
def mojo_gpu_pin_free_f16(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[PinStateF16]()
    ref st = p[]
    st.cand_dist_h.free()
    st.cand_id_h.free()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# GPU EXACT TOP-K over the pinned column.
#
# The plain `mojo_gpu_pin_query` returns ALL N distances to the host (DuckDB then
# does ORDER BY ... LIMIT k). For kNN that streams N floats across PCIe per query
# just to throw all but k away. The top-k path computes the same exact cosine
# distances on the GPU but returns ONLY the k nearest rows, so the transfer is
# ~nblocks*k (or k) floats+ids, never N.
#
# Algorithm (per-block k-best -> host merge), correctness-first:
#   1. Kernel A == the proven `cosine_kernel_warp`: compute all N distances into
#      a RESIDENT device `dist` buffer (no host transfer).
#   2. Kernel B (`topk_partial_kernel`): one block (WARP_SIZE lanes) per strided
#      row range maintains a per-block k-best in SHARED memory (sorted ascending
#      by the (dist, rowid) tie-break key) via threshold-insert, and writes its k
#      candidates as (dist, rowid) into a device `cand` buffer of size nblocks*k.
#   3. Host: copy the nblocks*k candidates back (nblocks*k << N) and stable-select
#      the final k under the SAME (dist asc, rowid asc) comparator. This guarantees
#      the result equals a CPU stable top-k bit-for-bit.
#
# Tie-break: every comparison is "smaller key wins", key = (dist, rowid). Equal
# distances are broken by ascending rowid, both in the GPU shared-buffer insert
# and the host merge, so it matches a CPU `stable_sort by (dist, id)` exactly.
#
# k is small (<= ~256 in practice; assume <= TOPK_MAX). The shared k-best is
# WARP_SIZE waves wide: at most TOPK_MAX dist floats + TOPK_MAX int rowids per
# block (the C++ wiring must keep k <= TOPK_MAX; the entry points return an error
# code otherwise).
# ===-------------------------------------------------------------------===#
comptime TOPK_MAX = 1024


# Per-block partial top-k over a resident `dist` buffer.
#
# Shared layout: `sd[k]` (distances) + `si[k]` (rowids), kept sorted ASCENDING by
# (dist, rowid) with `sd[cnt-1]` the current worst kept. A candidate row beats the
# buffer iff it is smaller than the worst (or the buffer isn't yet full); inserts
# are serialized across the warp's lanes (one lane at a time) so the shared buffer
# stays consistent without atomics. The expensive cosine math already ran in
# kernel A; this is a cheap scan over N float distances.
def topk_partial_kernel(
    dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    k: Int,
    nblocks: Int,
):
    # Raw-pointer shared memory (std.memory.stack_allocation) — avoids a dependency
    # on the `layout` package (TileTensor), which isn't installed in every Mojo env.
    var sd = stack_allocation[
        TOPK_MAX, Scalar[DType.float32], address_space = AddressSpace.SHARED
    ]()
    var si = stack_allocation[
        TOPK_MAX, Scalar[DType.int64], address_space = AddressSpace.SHARED
    ]()
    # `cnt` (current fill of the shared buffer) lives in a 1-elem shared slot so a
    # single lane owns the insert state and all lanes can read it after a barrier.
    var scnt = stack_allocation[
        1, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()

    var lane = Int(thread_idx.x)
    var bid = Int(block_idx.x)
    if lane == 0:
        scnt[0] = 0
    barrier()

    # Uniform wave count across the WHOLE block: every lane runs the same number
    # of waves and reaches every `barrier()` (a barrier with divergent
    # participation is undefined). Lanes whose row is out of range carry an
    # "invalid" candidate (rowid -1) that the insert step skips.
    var stride = nblocks * WARP
    var span = (n_rows - bid * WARP)  # rows from this block's first row onward
    var nwaves = 0
    if span > 0:
        nwaves = (span + stride - 1) // stride
    for w in range(nwaves):
        # Each lane reads its own candidate (dist, rowid) for this wave. The warp
        # then folds the (up to WARP) candidates into the one shared k-buffer, with
        # each lane inserting its OWN candidate when it is that lane's turn -- the
        # inserting lane holds the values in registers, so no cross-lane shuffle is
        # needed. Serializing by lane keeps the shared buffer consistent without
        # atomics. A barrier between turns publishes the previous insert.
        var i = bid * WARP + lane + w * stride
        var valid = i < n_rows
        var my_d = dist[i] if valid else Float32(3.0e38)
        var my_id = Int64(i) if valid else Int64(-1)
        for src in range(WARP):
            barrier()
            if lane == src and my_id >= 0:
                var cnt = Int(scnt[0])
                var cd = my_d
                var ci = my_id
                # Reject early iff buffer full and candidate not better than the
                # worst kept (the (dist, rowid) tie-break: smaller key wins).
                var accept = True
                if cnt >= k:
                    var wd = sd[k - 1]
                    var wi = si[k - 1]
                    if cd > wd or (cd == wd and ci >= wi):
                        accept = False
                if accept:
                    # Insertion-sort into the ascending (dist, rowid) buffer,
                    # shifting larger keys right (dropping the worst if full).
                    var pos = cnt if cnt < k else k - 1
                    while pos > 0:
                        var pd = sd[pos - 1]
                        var pi = si[pos - 1]
                        if pd > cd or (pd == cd and pi > ci):
                            sd[pos] = pd
                            si[pos] = pi
                            pos -= 1
                        else:
                            break
                    sd[pos] = cd
                    si[pos] = ci
                    if cnt < k:
                        scnt[0] = Int32(cnt + 1)
    barrier()

    # Emit this block's k candidates (pad unused slots with +inf / -1 so the host
    # merge ignores them deterministically).
    var cnt = Int(scnt[0])
    var out_base = bid * k
    var j = lane
    while j < k:
        if j < cnt:
            cand_dist[out_base + j] = sd[j]
            cand_id[out_base + j] = si[j]
        else:
            cand_dist[out_base + j] = Float32(3.0e38)
            cand_id[out_base + j] = Int64(-1)
        j += WARP


# Number of blocks the partial top-k launches with. Capped so nblocks*k stays a
# small host transfer; one warp per block.
def _topk_nblocks(n_rows: Int, k: Int) -> Int:
    var nb = (n_rows + WARP - 1) // WARP
    if nb < 1:
        nb = 1
    # Keep the candidate set small: at most ~256 blocks => nblocks*k transfer.
    var cap = 256
    # ...but never fewer rows-per-block-worth of parallelism than makes sense.
    if nb > cap:
        nb = cap
    return nb


# Number of blocks for the BATCHED fused kernel. Unlike the single-query top-k
# (whose candidate set is just nblocks*k, so 256 blocks suffices), the batched
# kernel does M dot products per row, so it is COMPUTE-bound: it must launch
# enough warps to saturate the GPU's SMs, or per-query latency plateaus far above
# the bandwidth floor. We therefore use a much higher block cap (one warp per
# ~rows_per_block rows). The candidate set is nblocks*qcount*k -- still a bounded
# host transfer because qcount*k <= TOPK_BATCH_CAND_CAP and nblocks is capped.
def _topk_nblocks_batch(n_rows: Int) -> Int:
    var nb = (n_rows + WARP - 1) // WARP
    if nb < 1:
        nb = 1
    # ~4096 warps keeps a modern GPU's SMs busy while bounding nblocks*qcount*k.
    var cap = 4096
    if nb > cap:
        nb = cap
    return nb


# Single-query exact top-k. Only the K-float query uploads per call; the embedding
# stays resident. Returns the k smallest into out_ids/out_dists (ascending).
@export("mojo_gpu_pin_query_topk")
def mojo_gpu_pin_query_topk(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    q: UnsafePointer[Float32, ImmutAnyOrigin],
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    if k <= 0 or k > TOPK_MAX:
        return 2
    try:
        var s = handle.bitcast[PinState]()
        ref st = s[]
        # qnorm on the host (matches the cosine kernel's denom).
        var qnorm = Float32(0)
        for i in range(st.K):
            qnorm += q[i] * q[i]
        qnorm = sqrt(qnorm)
        st.ctx.enqueue_copy(st.q_dev, q)

        var nblocks = _topk_nblocks(st.n_rows, k)
        var ncand = nblocks * k
        # Sub-views over the resident candidate scratch (sized for TOPK_MAX at pin
        # time, so ncand <= cand_cap always); no per-call device allocation.
        var cand_dist_dev = DeviceBuffer(
            st.ctx, st.cand_dist_dev.unsafe_ptr(), ncand, owning=False
        )
        var cand_id_dev = DeviceBuffer(
            st.ctx, st.cand_id_dev.unsafe_ptr(), ncand, owning=False
        )

        # Enqueue the whole pipeline on the stream, then a SINGLE synchronize
        # before the host merge: q H->D, distance kernel, partial top-k kernel,
        # candidate D->H -- one sync per warm query (was two), zero allocations.
        st.ctx.enqueue_function[cosine_kernel_warp](
            st.emb_dev,
            st.q_dev,
            st.out_dev,
            st.n_rows,
            st.K,
            qnorm,
            grid_dim=st.n_rows,
            block_dim=WARP,
        )
        st.ctx.enqueue_function[topk_partial_kernel](
            st.out_dev,
            cand_dist_dev,
            cand_id_dev,
            st.n_rows,
            k,
            nblocks,
            grid_dim=nblocks,
            block_dim=WARP,
        )
        st.ctx.enqueue_copy(st.cand_dist_h, cand_dist_dev)
        st.ctx.enqueue_copy(st.cand_id_h, cand_id_dev)
        st.ctx.synchronize()

        _host_merge_topk(
            st.cand_dist_h, st.cand_id_h, ncand, k, out_ids, out_dists
        )
        return 0
    except:
        return 3


# ===-------------------------------------------------------------------===#
# TRUE BATCHED EXACT TOP-K: read the resident N*K matrix ONCE per query-tile.
#
# The old batched entry just looped the single-query path: it re-read the whole
# N*K matrix M times (one cosine kernel launch per query) -- wasting the dominant
# bandwidth cost, which is exactly the regime where the GPU should crush a per-
# query index (HNSW can't use its index for batched queries at all).
#
# The fused kernel below amortizes that read. Layout:
#   * Grid  = nblocks (same cap as _topk_nblocks); one WARP per block.
#   * Each block strides over its assigned embedding rows. For each row it loads
#     the row's lane-strided slice into REGISTERS once and computes the L2 norm
#     once (norm is query-independent), then loops over the QCOUNT queries in this
#     tile, reusing the in-register row slice to form each query's dot product.
#     => every matrix element crosses the memory bus exactly ONCE per query-tile,
#        not once per query.
#   * Per-block, per-query top-k lives in SHARED memory: sd[QCOUNT*k] distances +
#     si[QCOUNT*k] rowids + scnt[QCOUNT] fills, each kept ascending by the SAME
#     (dist, rowid) tie-break as topk_partial_kernel / the single-query path, so a
#     batched result for query j is bit-for-bit the single-query result for j.
#   * The kernel emits nblocks*QCOUNT*k candidates (per query, per block); the host
#     merges per query exactly as the single-query path does.
#
# Query tiling: the per-block shared top-k is QCOUNT*(4+8) bytes + QCOUNT*4. To
# stay within a safe shared budget for ALL k up to TOPK_MAX we cap the live tile
# at TOPK_BATCH_CAND_CAP = QCOUNT*k candidate slots; the host splits M into tiles
# of qtile = min(M, TOPK_BATCH_CAND_CAP // k) queries and launches the fused
# kernel once per tile. So the matrix is read ceil(M / qtile) times, NOT M times:
# at k=10 a tile is ~410 queries (one matrix read for the whole M<=410 batch); at
# k=100 it is ~40; the read is amortized across the tile either way.
#
# The query vectors stay in GLOBAL memory (they are tiny, M*K, and after the
# first row they live in L2); only their per-element reads recur, never the N*K
# matrix.
# ===-------------------------------------------------------------------===#

# Max candidate slots (QCOUNT*k) kept live in one block's shared top-k. Bounds the
# shared-memory footprint per block: sd (4B) + si (8B) per slot + scnt (4B/query,
# and a tile holds at most CAP queries when k==1) => CAP*(4+8) + CAP*4 = 16*CAP
# bytes. 1536 => 24 KB, safely within a block's shared memory on ALL targets
# (Apple ~32 KB threadgroup; NVIDIA/AMD 48 KB+). Caps the per-tile query count.
comptime TOPK_BATCH_CAND_CAP = 1536
# Max embedding dims the in-register row cache holds per lane: ceil(K/WARP). At
# WARP=32 this supports K up to 32*64 = 2048 (covers 384/768/1024/1536 embeddings).
comptime BATCH_MAX_LANE_DIMS = 64


# Fused batched distance + per-query partial top-k over a query-tile.
#
# `qs` is the FULL M*K query buffer; `q0` is this tile's first query index and
# `qcount` (<= qtile, and qtile*k <= TOPK_BATCH_CAND_CAP) the number of queries in
# the tile. `qnorms` holds all M precomputed query norms. Candidates are written
# row-major [block*qcount*k + local_q*k + slot] into cand_dist/cand_id.
def topk_batch_kernel(
    emb: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    qs: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    qnorms: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    q0: Int,
    qcount: Int,
    k: Int,
    nblocks: Int,
):
    # Per-block per-query shared top-k: sd/si laid out [local_q*k + slot], kept
    # ascending by (dist, rowid). scnt[local_q] is the current fill for query q.
    var sd = stack_allocation[
        TOPK_BATCH_CAND_CAP,
        Scalar[DType.float32],
        address_space = AddressSpace.SHARED,
    ]()
    var si = stack_allocation[
        TOPK_BATCH_CAND_CAP,
        Scalar[DType.int64],
        address_space = AddressSpace.SHARED,
    ]()
    var scnt = stack_allocation[
        TOPK_BATCH_CAND_CAP, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()

    var lane = Int(thread_idx.x)
    var bid = Int(block_idx.x)
    # Lane 0 initializes the per-query fill counters.
    if lane == 0:
        for lq in range(qcount):
            scnt[lq] = 0
    barrier()

    # In-register cache of this lane's strided slice of the CURRENT embedding row
    # (lane i holds dims i, i+WARP, ...). Loaded ONCE per row, reused for every
    # query in the tile -- this is the amortization: each matrix element crosses
    # the bus once per tile, not once per query.
    var rowvals = stack_allocation[BATCH_MAX_LANE_DIMS, Scalar[DType.float32]]()

    # One WARP cooperates on ONE row at a time (lanes stride over K, warp.sum
    # reduces). The block sweeps rows bid, bid+nblocks, bid+2*nblocks, ...
    var row = bid
    while row < n_rows:
        var base = row * K
        # Load this lane's strided row slice into registers + partial L2 norm once.
        var na = Float32(0)
        var nd = 0
        var i = lane
        while i < K:
            var av = emb[base + i]
            rowvals[nd] = av
            na += av * av
            nd += 1
            i += WARP
        na = warp.sum(na)  # full row norm^2 (all lanes participate)

        # Score this row against every query in the tile, reusing rowvals.
        for lq in range(qcount):
            var qbase = (q0 + lq) * K
            var dot = Float32(0)
            var ii = lane
            var c = 0
            while ii < K:
                dot += rowvals[c] * qs[qbase + ii]
                c += 1
                ii += WARP
            dot = warp.sum(dot)
            # Lane 0 forms the distance (identical formula + tie-break to the
            # single-query kernel) and inserts into query lq's shared top-k.
            if lane == 0:
                var denom = sqrt(na) * qnorms[q0 + lq]
                var cd = (
                    Float32(1) - dot / denom if denom != 0 else Float32(0)
                )
                var ci = Int64(row)
                var off = lq * k
                var cnt = Int(scnt[lq])
                var accept = True
                if cnt >= k:
                    var wd = sd[off + k - 1]
                    var wi = si[off + k - 1]
                    if cd > wd or (cd == wd and ci >= wi):
                        accept = False
                if accept:
                    var pos = cnt if cnt < k else k - 1
                    while pos > 0:
                        var pd = sd[off + pos - 1]
                        var pi = si[off + pos - 1]
                        if pd > cd or (pd == cd and pi > ci):
                            sd[off + pos] = pd
                            si[off + pos] = pi
                            pos -= 1
                        else:
                            break
                    sd[off + pos] = cd
                    si[off + pos] = ci
                    if cnt < k:
                        scnt[lq] = Int32(cnt + 1)
        # Only lane 0 reads/writes the shared top-k; warp.sum already synchronizes
        # the warp each query, so no extra barrier is needed between rows.
        row += nblocks

    # Emit each query's k candidates for this block. Pad unused slots (+inf / -1).
    # Only lane 0 owns the shared buffers, so it writes the emit (the other lanes
    # would race lane 0's last inserts without a barrier); k <= TOPK_MAX is small.
    if lane == 0:
        for lq in range(qcount):
            var cnt = Int(scnt[lq])
            var soff = lq * k
            var out_base = (bid * qcount + lq) * k
            for j in range(k):
                if j < cnt:
                    cand_dist[out_base + j] = sd[soff + j]
                    cand_id[out_base + j] = si[soff + j]
                else:
                    cand_dist[out_base + j] = Float32(3.0e38)
                    cand_id[out_base + j] = Int64(-1)


# fp16-resident variant of topk_batch_kernel: the matrix is read as halves and
# each element cast to fp32 before any arithmetic (math stays fp32). Identical
# tiling, tie-break and amortization to the fp32 kernel above.
def topk_batch_kernel_f16(
    emb: UnsafePointer[Scalar[DType.float16], MutAnyOrigin],
    qs: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    qnorms: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    q0: Int,
    qcount: Int,
    k: Int,
    nblocks: Int,
):
    var sd = stack_allocation[
        TOPK_BATCH_CAND_CAP,
        Scalar[DType.float32],
        address_space = AddressSpace.SHARED,
    ]()
    var si = stack_allocation[
        TOPK_BATCH_CAND_CAP,
        Scalar[DType.int64],
        address_space = AddressSpace.SHARED,
    ]()
    var scnt = stack_allocation[
        TOPK_BATCH_CAND_CAP, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()

    var lane = Int(thread_idx.x)
    var bid = Int(block_idx.x)
    if lane == 0:
        for lq in range(qcount):
            scnt[lq] = 0
    barrier()

    var rowvals = stack_allocation[BATCH_MAX_LANE_DIMS, Scalar[DType.float32]]()

    var row = bid
    while row < n_rows:
        var base = row * K
        var na = Float32(0)
        var nd = 0
        var i = lane
        while i < K:
            # Cast the stored half to fp32 BEFORE arithmetic (storage-only loss).
            var av = emb[base + i].cast[DType.float32]()
            rowvals[nd] = av
            na += av * av
            nd += 1
            i += WARP
        na = warp.sum(na)

        for lq in range(qcount):
            var qbase = (q0 + lq) * K
            var dot = Float32(0)
            var ii = lane
            var c = 0
            while ii < K:
                dot += rowvals[c] * qs[qbase + ii]
                c += 1
                ii += WARP
            dot = warp.sum(dot)
            if lane == 0:
                var denom = sqrt(na) * qnorms[q0 + lq]
                var cd = (
                    Float32(1) - dot / denom if denom != 0 else Float32(0)
                )
                var ci = Int64(row)
                var off = lq * k
                var cnt = Int(scnt[lq])
                var accept = True
                if cnt >= k:
                    var wd = sd[off + k - 1]
                    var wi = si[off + k - 1]
                    if cd > wd or (cd == wd and ci >= wi):
                        accept = False
                if accept:
                    var pos = cnt if cnt < k else k - 1
                    while pos > 0:
                        var pd = sd[off + pos - 1]
                        var pi = si[off + pos - 1]
                        if pd > cd or (pd == cd and pi > ci):
                            sd[off + pos] = pd
                            si[off + pos] = pi
                            pos -= 1
                        else:
                            break
                    sd[off + pos] = cd
                    si[off + pos] = ci
                    if cnt < k:
                        scnt[lq] = Int32(cnt + 1)
        row += nblocks

    if lane == 0:
        for lq in range(qcount):
            var cnt = Int(scnt[lq])
            var soff = lq * k
            var out_base = (bid * qcount + lq) * k
            for j in range(k):
                if j < cnt:
                    cand_dist[out_base + j] = sd[soff + j]
                    cand_id[out_base + j] = si[soff + j]
                else:
                    cand_dist[out_base + j] = Float32(3.0e38)
                    cand_id[out_base + j] = Int64(-1)


# GPU second-stage merge: one block (one WARP) per query reduces that query's
# nblocks*k scattered per-block candidates down to the final k, in shared memory,
# under the SAME (dist, rowid) tie-break as topk_partial_kernel / the host merge.
# This keeps the cross-block merge on the GPU so the host only ever receives the
# final M*k results -- without it, the host merge over nblocks*k candidates per
# query (nblocks is large to saturate the GPU) becomes the throughput wall.
#
# Candidate layout (from topk_batch_kernel): for query lq in this tile, block b's
# k candidates live at cand[(b*qcount + lq)*k + j]. The merged final k for query
# lq are written to out[lq*k + j].
def topk_batch_merge_kernel(
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    out_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    out_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    qcount: Int,
    nblocks: Int,
    k: Int,
):
    var sd = stack_allocation[
        TOPK_MAX, Scalar[DType.float32], address_space = AddressSpace.SHARED
    ]()
    var si = stack_allocation[
        TOPK_MAX, Scalar[DType.int64], address_space = AddressSpace.SHARED
    ]()
    var scnt = stack_allocation[
        1, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()

    var lane = Int(thread_idx.x)
    var lq = Int(block_idx.x)  # this block merges query lq
    if lq >= qcount:
        return
    if lane == 0:
        scnt[0] = 0
    barrier()

    # Total candidates for this query: nblocks blocks * k each. Lane-strided scan,
    # warp-serialized insert into the shared k-best (same pattern as the partial).
    var ncand = nblocks * k
    var nwaves = (ncand + WARP - 1) // WARP
    for w in range(nwaves):
        var c = lane + w * WARP
        var valid = c < ncand
        var my_d = Float32(3.0e38)
        var my_id = Int64(-1)
        if valid:
            # candidate c => block b = c // k, slot j = c % k.
            var b = c // k
            var j = c % k
            var idx = (b * qcount + lq) * k + j
            my_id = cand_id[idx]
            my_d = cand_dist[idx]
        for src in range(WARP):
            barrier()
            if lane == src and my_id >= 0:
                var cnt = Int(scnt[0])
                var cd = my_d
                var ci = my_id
                var accept = True
                if cnt >= k:
                    var wd = sd[k - 1]
                    var wi = si[k - 1]
                    if cd > wd or (cd == wd and ci >= wi):
                        accept = False
                if accept:
                    var pos = cnt if cnt < k else k - 1
                    while pos > 0:
                        var pd = sd[pos - 1]
                        var pi = si[pos - 1]
                        if pd > cd or (pd == cd and pi > ci):
                            sd[pos] = pd
                            si[pos] = pi
                            pos -= 1
                        else:
                            break
                    sd[pos] = cd
                    si[pos] = ci
                    if cnt < k:
                        scnt[0] = Int32(cnt + 1)
    barrier()

    # Emit this query's final k (ascending; pad with +inf / -1 if fewer than k).
    var cnt = Int(scnt[0])
    var j = lane
    while j < k:
        if j < cnt:
            out_dist[lq * k + j] = sd[j]
            out_id[lq * k + j] = si[j]
        else:
            out_dist[lq * k + j] = Float32(3.0e38)
            out_id[lq * k + j] = Int64(-1)
        j += WARP


# Number of queries to score per matrix read-pass: as many as fit the shared
# candidate budget (qtile*k <= TOPK_BATCH_CAND_CAP), capped at M. >=1 always.
def _batch_qtile(M: Int, k: Int) -> Int:
    var qt = TOPK_BATCH_CAND_CAP // k
    if qt < 1:
        qt = 1
    if qt > M:
        qt = M
    return qt


# Shared device-side driver for both precisions: allocates a per-tile candidate
# buffer sized nblocks*qtile*k, launches the fused kernel once per query-tile
# (reading the matrix once per tile), and host-merges each query's k candidates.
# `is_f16` selects which resident matrix / fused kernel to use; exactly one of
# emb32/emb16 is the resident buffer (the other is a 0-length placeholder view).
def _run_topk_batch[
    is_f16: Bool
](
    ctx: DeviceContext,
    emb32: DeviceBuffer[DType.float32],
    emb16: DeviceBuffer[DType.float16],
    n_rows: Int,
    K: Int,
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    metric: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) raises:
    # `metric` (0=cosine, 1=L2, 2=inner-product; see tc_knn.TC_METRIC_*). Only
    # the FUSED tensor-core path below honors non-cosine metrics; the scalar
    # fallback is cosine-only, so a non-cosine request that cannot take the
    # fused path is rejected (the C++ caller then falls back to stock DuckDB).
    # FUSED tensor-core path (NVIDIA-only, opt-in). Routed ONLY when (a) the
    # resident matrix is fp16 (the fused MMA kernel is fp16), (b) the build
    # targets an NVIDIA accelerator (`has_nvidia_gpu_accelerator()`, the
    # HOST-side comptime query; the in-kernel `is_nvidia_gpu()` triple check is
    # always False in host context, so it can't gate this host branch), (c) the
    # env flag GPU_OP_TENSORCORE is set, and (d) (K, k) is a supported fused
    # shape. On Apple `has_nvidia_gpu_accelerator()` is comptime-False, so this
    # branch -- and the tensor-core kernel instantiation inside
    # `run_tc_knn_batch` -- is never compiled. On any runtime miss we fall
    # through to the existing scalar path below (zero behavior change with the
    # flag unset). Default OFF.
    comptime if is_f16 and has_nvidia_gpu_accelerator():
        if getenv("GPU_OP_TENSORCORE", "") != "" and tc_knn_supported(K, k):
            run_tc_knn_batch(
                ctx, emb16, n_rows, K, qs, M, k, metric, out_ids, out_dists
            )
            return

    # FUSED Apple-Silicon (M1-M4) path: the 8x8 `simdgroup_matrix` MMA analogue
    # of the NVIDIA tensor-core path above. Routed ONLY when (a) the resident
    # matrix is fp16 (the fused MMA is fp16), (b) the build targets an Apple GPU
    # (`has_apple_gpu_accelerator()`, the HOST comptime query; on NVIDIA this is
    # comptime-False so the Apple kernel instantiation never compiles -- and the
    # NVIDIA branch above is correspondingly never compiled on Apple), (c) the
    # env flag GPU_OP_TENSORCORE is set, (d) the metric is COSINE (the only
    # metric the Apple fused path implements -- normalized inputs), and (e)
    # (K, k) is a supported fused shape. Any runtime miss falls through to the
    # scalar path below (zero behavior change with the flag unset). Default OFF.
    comptime if is_f16 and has_apple_gpu_accelerator():
        if (
            getenv("GPU_OP_TENSORCORE", "") != ""
            and metric == 0
            and tc_knn_apple_supported(K, k)
        ):
            run_tc_knn_apple_batch(
                ctx, emb16, n_rows, K, qs, M, k, out_ids, out_dists
            )
            return

    # Non-cosine metrics are ONLY implemented on the fused path above. If we did
    # not route there (flag unset / non-NVIDIA / unsupported shape / fp32),
    # there is no correct scalar implementation -- error out so the caller falls
    # back to stock DuckDB rather than silently returning cosine results.
    if metric != 0:
        raise Error(
            "topk batch: non-cosine metric requires the fused tensor-core path"
            " (GPU_OP_TENSORCORE on an NVIDIA build, fp16, supported K/k)"
        )

    # The fused kernel caches each lane's strided row slice (ceil(K/WARP) dims) in
    # a fixed BATCH_MAX_LANE_DIMS register array; reject K that would overflow it.
    if (K + WARP - 1) // WARP > BATCH_MAX_LANE_DIMS:
        raise Error("batch top-k: K too large for the in-register row cache")
    var nblocks = _topk_nblocks_batch(n_rows)
    var qtile = _batch_qtile(M, k)

    # Precompute all M query norms on the host (matches the kernel denom) and
    # upload the M*K queries + M norms once for the whole batch.
    var qnorms_h = alloc[Float32](M)
    for m in range(M):
        var s = Float32(0)
        var qoff = m * K
        for i in range(K):
            s += qs[qoff + i] * qs[qoff + i]
        qnorms_h[m] = sqrt(s)

    var qs_dev = ctx.enqueue_create_buffer[DType.float32](M * K)
    var qnorm_dev = ctx.enqueue_create_buffer[DType.float32](M)
    var cand_cap = nblocks * qtile * k
    var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
    var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
    # Final merged results for one tile (qtile*k) live on the device; only these
    # M*k (NOT the nblocks*qtile*k candidates) ever cross PCIe -- the cross-block
    # merge runs on the GPU (topk_batch_merge_kernel), so the host does no top-k.
    var merged_dist_dev = ctx.enqueue_create_buffer[DType.float32](qtile * k)
    var merged_id_dev = ctx.enqueue_create_buffer[DType.int64](qtile * k)
    var merged_dist_h = alloc[Float32](qtile * k)
    var merged_id_h = alloc[Int64](qtile * k)
    ctx.synchronize()
    ctx.enqueue_copy(qs_dev, qs)
    var qnorm_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qnorms_h)
    )
    ctx.enqueue_copy(qnorm_dev, qnorm_imm)
    ctx.synchronize()

    var q0 = 0
    while q0 < M:
        var qcount = qtile
        if q0 + qcount > M:
            qcount = M - q0
        var ncand_tile = nblocks * qcount * k
        var cd_view = DeviceBuffer(
            ctx, cand_dist_dev.unsafe_ptr(), ncand_tile, owning=False
        )
        var cid_view = DeviceBuffer(
            ctx, cand_id_dev.unsafe_ptr(), ncand_tile, owning=False
        )
        var md_view = DeviceBuffer(
            ctx, merged_dist_dev.unsafe_ptr(), qcount * k, owning=False
        )
        var mid_view = DeviceBuffer(
            ctx, merged_id_dev.unsafe_ptr(), qcount * k, owning=False
        )

        # Stage 1: fused distance + per-block per-query top-k (matrix read once).
        comptime if is_f16:
            ctx.enqueue_function[topk_batch_kernel_f16](
                emb16,
                qs_dev,
                qnorm_dev,
                cd_view,
                cid_view,
                n_rows,
                K,
                q0,
                qcount,
                k,
                nblocks,
                grid_dim=nblocks,
                block_dim=WARP,
            )
        else:
            ctx.enqueue_function[topk_batch_kernel](
                emb32,
                qs_dev,
                qnorm_dev,
                cd_view,
                cid_view,
                n_rows,
                K,
                q0,
                qcount,
                k,
                nblocks,
                grid_dim=nblocks,
                block_dim=WARP,
            )
        # Stage 2 (GPU): merge each query's nblocks*k candidates to the final k.
        ctx.enqueue_function[topk_batch_merge_kernel](
            cd_view,
            cid_view,
            md_view,
            mid_view,
            qcount,
            nblocks,
            k,
            grid_dim=qcount,
            block_dim=WARP,
        )
        ctx.enqueue_copy(merged_dist_h, md_view)
        ctx.enqueue_copy(merged_id_h, mid_view)
        ctx.synchronize()

        # The GPU merge already produced exact ascending top-k per query; just copy
        # each query's k results into the caller's row-major M*k output.
        for lq in range(qcount):
            var m = q0 + lq
            for j in range(k):
                out_ids[m * k + j] = merged_id_h[lq * k + j]
                out_dists[m * k + j] = merged_dist_h[lq * k + j]
        q0 += qtile

    qnorms_h.free()
    merged_dist_h.free()
    merged_id_h.free()


# Batched exact top-k (fp32-resident). M query vectors (row-major M*K) scored
# against the resident matrix, results row-major M*k into out_ids/out_dists. The
# matrix is read ONCE per query-tile (see topk_batch_kernel), so the dominant
# bandwidth cost is amortized across the batch -- batch-M per-query latency drops
# sharply with M. Returns the SAME exact (ids, dists) as M single-query calls.
@export("mojo_gpu_pin_query_topk_batch")
def mojo_gpu_pin_query_topk_batch(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    if k <= 0 or k > TOPK_MAX or M <= 0:
        return 2
    try:
        var s = handle.bitcast[PinState]()
        ref st = s[]
        # 0-length fp16 placeholder (unused on the fp32 path).
        var emb16 = st.ctx.enqueue_create_buffer[DType.float16](1)
        _run_topk_batch[False](
            st.ctx,
            st.emb_dev,
            emb16,
            st.n_rows,
            st.K,
            qs,
            M,
            k,
            0,  # metric = cosine
            out_ids,
            out_dists,
        )
        return 0
    except:
        return 3


# Batched exact top-k (fp16-resident). Same contract + exact results as
# mojo_gpu_pin_query_topk_batch, but reads the resident matrix as halves (fp32
# accumulate). Free the handle with mojo_gpu_pin_free_f16.
# Shared implementation for the fp16 batched entries (cosine + metric). `metric`
# is 0=cosine, 1=L2 (squared euclidean / array_distance), 2=inner-product
# (-dot / array_negative_inner_product). Non-cosine is only honored by the fused
# tensor-core path (see _run_topk_batch); otherwise it returns a nonzero rc so
# the C++ caller falls back to stock DuckDB.
def _pin_query_topk_batch_f16_impl(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    metric: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) -> Int32:
    if Int(handle) == 0:
        return 1
    if k <= 0 or k > TOPK_MAX or M <= 0:
        return 2
    if metric < 0 or metric > 2:
        return 2
    try:
        var s = handle.bitcast[PinStateF16]()
        ref st = s[]
        # 0-length fp32 placeholder (unused on the fp16 path).
        var emb32 = st.ctx.enqueue_create_buffer[DType.float32](1)
        _run_topk_batch[True](
            st.ctx,
            emb32,
            st.emb_dev,
            st.n_rows,
            st.K,
            qs,
            M,
            k,
            metric,
            out_ids,
            out_dists,
        )
        return 0
    except:
        return 3


@export("mojo_gpu_pin_query_topk_batch_f16")
def mojo_gpu_pin_query_topk_batch_f16(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    # Unchanged signature/behavior: cosine (metric 0).
    return _pin_query_topk_batch_f16_impl(
        handle, qs, M, k, 0, out_ids, out_dists
    )


# Batched fp16 top-k with an explicit metric (NEW symbol; the existing cosine
# entry above is untouched). metric: 0=cosine, 1=L2 (array_distance, squared
# euclidean), 2=inner-product (array_negative_inner_product / -dot). Non-cosine
# only works on the fused tensor-core path (GPU_OP_TENSORCORE on an NVIDIA build,
# supported K/k); a nonzero rc signals the C++ caller to fall back to DuckDB.
@export("mojo_gpu_pin_query_topk_batch_f16_metric")
def mojo_gpu_pin_query_topk_batch_f16_metric(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    metric: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) abi("C") -> Int32:
    return _pin_query_topk_batch_f16_impl(
        handle, qs, M, k, metric, out_ids, out_dists
    )


# Host final merge: stable-select the k smallest (dist asc, rowid asc) from the
# `ncand` candidates produced by the partial kernel. Padded slots carry rowid -1
# and are skipped. k is small, so a simple k-pass selection is exact + cheap.
def _host_merge_topk(
    cand_dist_h: UnsafePointer[Float32, MutAnyOrigin],
    cand_id_h: UnsafePointer[Int64, MutAnyOrigin],
    ncand: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
):
    var taken = alloc[Bool](ncand if ncand > 0 else 1)
    for c in range(ncand):
        taken[c] = False
    for slot in range(k):
        var best = -1
        var best_d = Float32(3.0e38)
        var best_id = Int64(-1)
        for c in range(ncand):
            if taken[c]:
                continue
            var cid = cand_id_h[c]
            if cid < 0:
                continue
            var cd = cand_dist_h[c]
            if best < 0 or cd < best_d or (cd == best_d and cid < best_id):
                best = c
                best_d = cd
                best_id = cid
        if best < 0:
            out_ids[slot] = Int64(-1)
            out_dists[slot] = Float32(0)
        else:
            taken[best] = True
            out_ids[slot] = best_id
            out_dists[slot] = best_d
    taken.free()



# ===-------------------------------------------------------------------===#
# Descriptor C-ABI wrappers (Stage 1) -- MOVED here from descriptor.mojo so the
# descriptor logic and the GPU kernels share one compilation unit (the dylib).
# `import descriptor` brings the pure logic; a bare import would strip exports,
# which is why these thin wrappers are (re)defined in this root build file.
#
# The descriptor handle is an allocated `descriptor.GpuPlanDescriptor*`, stored
# opaquely the same way as the kernel handles (Int(p.bitcast[NoneType]())).
# ===-------------------------------------------------------------------===#
@export("mojo_gpu_build_descriptor")
def mojo_gpu_build_descriptor(
    tape: UnsafePointer[Int64, MutAnyOrigin],
    tape_len: Int,
    blob: UnsafePointer[UInt8, MutAnyOrigin],
    blob_len: Int,
) abi("C") -> Int:
    try:
        var r = RawPlanReader(tape, tape_len, blob, blob_len)
        var maybe = build_descriptor_impl(r)
        if not maybe:
            return 0
        var p = alloc[GpuPlanDescriptor](1)
        p.init_pointee_move(maybe.unsafe_take())
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_gpu_desc_free")
def mojo_gpu_desc_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    # Drop any Stage-2 exec state keyed by this handle (the pin cache is
    # process-lifetime and intentionally NOT evicted here).
    _exec_drop(Int(handle))
    var p = handle.bitcast[GpuPlanDescriptor]()
    p.destroy_pointee()
    p.free()


@export("mojo_gpu_desc_kind")
def mojo_gpu_desc_kind(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return Int(KIND_UNKNOWN)
    return Int(handle.bitcast[GpuPlanDescriptor]()[].kind)


@export("mojo_gpu_desc_strategy")
def mojo_gpu_desc_strategy(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return Int(STRAT_UNGROUPED)
    return Int(handle.bitcast[GpuPlanDescriptor]()[].strategy)


@export("mojo_gpu_desc_n_dims")
def mojo_gpu_desc_n_dims(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    return len(handle.bitcast[GpuPlanDescriptor]()[].dim_edges)


@export("mojo_gpu_desc_n_aggs")
def mojo_gpu_desc_n_aggs(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    return len(handle.bitcast[GpuPlanDescriptor]()[].aggregates)


@export("mojo_gpu_desc_fact_table")
def mojo_gpu_desc_fact_table(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    out_ptr: UnsafePointer[UInt8, MutAnyOrigin],
    cap: Int,
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    ref name = handle.bitcast[GpuPlanDescriptor]()[].fact_table
    var n = name.byte_length()
    var bytes = name.as_bytes()
    var to_copy = n if n < cap else cap
    for i in range(to_copy):
        out_ptr[i] = bytes[i]
    return n


# ===-------------------------------------------------------------------===#
# Stage-2 execution shuttle (Q6 class). The descriptor drives execution:
# materialize SQL -> feed flat columns -> finalize (run the existing Q6 kernel)
# -> typed results. Process-global maps keyed by the descriptor handle int.
# ===-------------------------------------------------------------------===#

# One fed flat column: an owned host copy of the transient DuckDB pointer, plus
# its element width (bytes) and the contract TypeTag it arrived as.
struct FedColumn(Movable):
    var data: Optional[UnsafePointer[UInt8, MutAnyOrigin]]
    var n_rows: Int
    var elem_size: Int
    var type_tag: Int64
    # For VARCHAR columns: an owned byte heap holding the deep copy of every
    # non-inlined string's bytes. The copied string_t structs in `data` are
    # rewritten to point into this heap so the column is self-contained after the
    # source DuckDB result (and its string heap) is freed. None for non-VARCHAR.
    var str_heap: Optional[UnsafePointer[UInt8, MutAnyOrigin]]

    def __init__(out self):
        self.data = None
        self.n_rows = 0
        self.elem_size = 0
        self.type_tag = 0
        self.str_heap = None

    def fill(
        mut self,
        src: UnsafePointer[NoneType, MutAnyOrigin],
        n_rows: Int,
        elem_size: Int,
        type_tag: Int64,
    ):
        self.free_data()
        var nbytes = n_rows * elem_size
        var p = alloc[UInt8](nbytes if nbytes > 0 else 1)
        var src_b = UnsafePointer[UInt8, ImmutAnyOrigin](
            unsafe_from_address=Int(src)
        )
        memcpy(dest=p, src=src_b, count=nbytes)
        self.data = p
        self.n_rows = n_rows
        self.elem_size = elem_size
        self.type_tag = type_tag
        self.str_heap = None
        if type_tag == TYPE_VARCHAR:
            self._deep_copy_strings(n_rows)

    # Deep-copy every non-inlined DuckDB string_t (length > 12) into an owned heap
    # and rewrite the copied struct's pointer (bytes 8..15) to point at it. The
    # source pointers are only valid NOW (the DuckDB result is freed before
    # finalize), so we must capture the bytes at feed time. Inlined strings
    # (length <= 12) carry their bytes in the struct, so they need no copy.
    def _deep_copy_strings(mut self, n_rows: Int):
        var base = self.data.value()
        # First pass: total bytes for non-inlined strings.
        var total = 0
        for i in range(n_rows):
            var p = base + i * 16
            var length = (
                Int(p[0])
                | (Int(p[1]) << 8)
                | (Int(p[2]) << 16)
                | (Int(p[3]) << 24)
            )
            if length > 12:
                total += length
        var heap = alloc[UInt8](total if total > 0 else 1)
        var w = 0
        for i in range(n_rows):
            var p = base + i * 16
            var length = (
                Int(p[0])
                | (Int(p[1]) << 8)
                | (Int(p[2]) << 16)
                | (Int(p[3]) << 24)
            )
            if length <= 12:
                continue
            # current (source) pointer is bytes 8..15.
            var addr = 0
            for b in range(8):
                addr |= Int(p[8 + b]) << (8 * b)
            var srcp = UnsafePointer[UInt8, ImmutAnyOrigin](
                unsafe_from_address=addr
            )
            var dstp = heap + w
            for k in range(length):
                dstp[k] = srcp[k]
            # rewrite the struct pointer to the owned heap location.
            var newaddr = Int(dstp)
            for b in range(8):
                p[8 + b] = UInt8((newaddr >> (8 * b)) & 0xFF)
            w += length
        self.str_heap = heap

    # Raw address of the owned buffer (0 if unfilled).
    def addr(self) -> Int:
        if self.data:
            return Int(self.data.value())
        return 0

    def free_data(mut self):
        if self.data:
            self.data.value().free()
            self.data = None
        if self.str_heap:
            self.str_heap.value().free()
            self.str_heap = None


# Per-descriptor Stage-2 state.
struct GpuExecState(Movable):
    var mat_cols: List[String]  # the fact-request (req 0) materialize/feed order
    var cols: List[FedColumn]  # fed fact columns, indexed by mat_cols order
    var n_rows: Int
    # --- SKIP-MATERIALIZE (GPU_OP_COLPOOL=2) request-0 bookkeeping ---
    # `colpool_omit[mj]` is True for a fact column (mat_cols index mj) the narrowed
    # SELECT OMITTED because it is pool-resident: it is NOT scanned/fed; the
    # finalize sources it from the pool (D2D) and never reads st.cols[mj].
    # `fed_pos_to_matcol[p]` maps the p-th EMITTED (narrowed) result column back to
    # its FULL mat_cols index, so feed_column keeps st.cols indexed by mat_cols
    # order (the finalize slot map is unchanged). Both are recomputed every
    # request-0 materialize_sql; empty when skip-materialize is inactive (then
    # `colpool_omit` is all-False / fed_pos_to_matcol is the identity).
    var colpool_omit: List[Bool]
    var fed_pos_to_matcol: List[Int]
    # The pool ColKeys this request leased AT OMIT-DECISION TIME (materialize_sql),
    # one per omitted column. The lease (refcount++) is held from the probe so the
    # column CANNOT be evicted before the finalize sources it (closing the
    # probe->finalize race). The cold finalize TRANSFERS each lease to the cached
    # GpuPinned (pool_lease_keys) and clears this; on a backstop bail it releases
    # them. Empty when skip-materialize is inactive.
    var colpool_omit_leases: List[String]
    # Dim requests (request index 1..n_dims for an n_dims>0 FK-join plan). Each
    # entry i corresponds to request index i+1; dim_mat_cols[i] is that dim's
    # SELECT column order and dim_cols[i] the fed columns. Empty for n_dims==0.
    var dim_mat_cols: List[List[String]]
    var dim_cols: List[List[FedColumn]]
    var dim_n_rows: List[Int]
    var warm: Bool
    # Result store: int128 limbs, row-major [row*n_cols + col].
    var res_lo: List[Int64]
    var res_hi: List[Int64]
    # DOUBLE result cells (e.g. Q1 avgs), same row-major layout as res_lo/hi.
    var res_f64: List[Float64]
    # String result cells, same row-major layout (group-key columns).
    var res_str: List[String]
    var res_rows: Int
    var res_cols: Int

    def __init__(out self, n_cols: Int):
        self.mat_cols = []
        self.cols = []
        for _ in range(n_cols):
            self.cols.append(FedColumn())
        self.n_rows = 0
        self.colpool_omit = []
        self.fed_pos_to_matcol = []
        self.colpool_omit_leases = []
        self.dim_mat_cols = []
        self.dim_cols = []
        self.dim_n_rows = []
        self.warm = False
        self.res_lo = []
        self.res_hi = []
        self.res_f64 = []
        self.res_str = []
        self.res_rows = 0
        self.res_cols = 0

    # Size the dim-request storage for `n_dims` FK-join dims (request indices
    # 1..n_dims). `dim_col_counts[i]` is the SELECT column count of dim i.
    def init_dims(mut self, dim_col_counts: List[Int]):
        self.dim_mat_cols = []
        self.dim_cols = []
        self.dim_n_rows = []
        for i in range(len(dim_col_counts)):
            self.dim_mat_cols.append(List[String]())
            var cs: List[FedColumn] = []
            for _ in range(dim_col_counts[i]):
                cs.append(FedColumn())
            self.dim_cols.append(cs^)
            self.dim_n_rows.append(0)

    def free_cols(mut self):
        for i in range(len(self.cols)):
            self.cols[i].free_data()
        for di in range(len(self.dim_cols)):
            for i in range(len(self.dim_cols[di])):
                self.dim_cols[di][i].free_data()


def _make_exec_map() -> Dict[Int, GpuExecState]:
    return Dict[Int, GpuExecState]()


comptime _exec_map = _Global["mojo_gpu_exec_map", _make_exec_map]


def _exec_ptr() raises -> UnsafePointer[Dict[Int, GpuExecState], MutAnyOrigin]:
    return _exec_map.get_or_create_ptr()


def _exec_drop(handle: Int):
    try:
        ref m = _exec_ptr()[]
        if handle in m:
            # SKIP-MATERIALIZE: release any materialize-time omit leases that a cold
            # finalize did not transfer to a GpuPinned (e.g. it bailed on an early
            # error before the assemble) so a dropped handle never leaks a pin.
            var leases = m[handle].colpool_omit_leases.copy()
            for li in range(len(leases)):
                release_lease(leases[li])
            m[handle].colpool_omit_leases = []
            m[handle].free_cols()
            _ = m.pop(handle)
    except:
        pass


# ---------------------------------------------------------------------------
# Process-global RESIDENT pin cache: signature -> GpuPinned.
#
# `GpuPinned` owns the resident device buffers (SegResident) PLUS everything
# needed to (a) re-run the kernel on a WARM hit (the small per-run programs) and
# (b) assemble the result table WITHOUT any fed host data. On a WARM hit the C++
# side feeds nothing (pin_begin returned 0), so the finalize re-launches the
# kernel on the resident buffers and assembles from this cached metadata alone.
#
# Lifetime: process-global (the intended "pin"). Entries are never evicted; the
# SegResident keeps its device buffers resident as long as the entry lives.
# ---------------------------------------------------------------------------

# Per-output-candidate group/segment description, constant for a cache entry
# (the predicate constants are in the signature, so a WARM hit ⇒ identical
# predicate ⇒ identical group/segment layout). `assemble` re-runs the kernel,
# computes the emit rule per candidate, and places the cached group-key cells +
# the freshly-computed sums into the current handle's exec-state result table.
struct GpuPinned(Movable):
    # --- resident device buffers (upload once, re-run many) ---
    var res: SegResident
    # --- per-run kernel parameters (stable for the entry: constants are in sig) ---
    var mode: Int64
    var G: Int
    var gid_slot: Int
    var M: Int
    var pass_prog: List[Int64]
    var pass_len: Int
    var metric_ops: List[Int64]
    var n_ops_total: Int
    var metric_offsets: List[Int64]
    var metric_lens: List[Int64]
    var n_seg: Int  # SORT_SEGREDUCE: number of segments (== n candidates)
    # --- result-assembly metadata (constant-independent for the entry) ---
    var n_cols: Int  # out_types arity
    var n_keys: Int  # number of group-key output columns
    # per-output-aggregate kind + the summed-value scale + metric slot indices.
    var agg_kind: List[Int64]
    var agg_scale: List[Int64]
    var agg_m0: List[Int]
    var agg_m1: List[Int]  # AVG count metric idx, else -1
    # Per group-key output column: is it placed as a string (res_str) or int64
    # (res_lo)?  Length n_keys.
    var gk_is_str: List[Bool]
    # Per output candidate, the group-key cell values (string + int64 variants).
    # gk_str_vals[g][k] / gk_i64_vals[g][k]. Length == n candidates (G for DENSE,
    # n_seg for SORT, 1 for UNGROUPED).
    var gk_str_vals: List[List[String]]
    var gk_i64_vals: List[List[Int64]]
    # Number of output candidates (G / n_seg / 1).
    var n_cand: Int
    # Emit rule: -1 = emit every candidate; otherwise the aggregate index whose
    # i128 sum gates emission. `emit_gt0` => emit iff sum > 0 (Q3/Q5).
    var emit_agg: Int
    var emit_gt0: Bool
    # --- HASH_GROUP extra state (mode == STRAT_HASH_GROUP) ---
    # The fact integer group key's packed-column slot, the hash-table capacity,
    # and host copies of the carried dim arrays (one per group key; empty for a
    # fact group key) so result assembly can look up o_orderdate/o_shippriority
    # by the kernel-discovered group key without re-reading device memory.
    var hash_gk_slot: Int
    var hash_cap: Int
    var hash_gk_dim_arr: List[List[Int64]]  # per group key; carried dim array
    # Query KIND (KIND_Q1/Q6/Q14/Q5/...), used to route segreduce_run to the
    # comptime-specialized kernel for that shape (KIND_UNKNOWN -> generic VM).
    var kind: Int64
    # --- Phase G Stage 2: Q6 predicate-independent residency (flag-gated) ---
    # When `q6_pred` is True, the resident columns include the Q6 filter inputs and
    # the filter is evaluated IN-KERNEL from these slots + the CURRENT query's
    # bounds (threaded into _assemble every run), NOT from a host pass column. The
    # slots are constant-independent (they depend only on the column layout), so
    # they live in the cache entry; the bounds do NOT (they come from `d`).
    # Slot order: [ship_slot, disc_slot, qty_slot].
    var q6_pred: Bool
    var q6_pred_slots: List[Int]
    # --- Phase G Stage 2 (generalized): Q1/Q14 predicate-independent residency ---
    # When `gen_pred` is True, the row filter is the general in-kernel fact-range
    # predicate evaluated from the resident columns + the CURRENT query's bounds.
    # The constant-INDEPENDENT part lives in the cache entry: `gen_pred_slots[p]`
    # and `gen_pred_cmps[p]` for each fact filter `p` (in descriptor order). The
    # constant-DEPENDENT bounds do NOT (they are threaded into _assemble fresh from
    # the live descriptor each run, aligned to the same descriptor-filter order).
    var gen_pred: Bool
    var gen_pred_slots: List[Int]
    var gen_pred_cmps: List[Int64]
    # --- Phase G Stage 2 (Path B): Q5 predicate-independent residency ---
    # When `q5_pred` is True the resident dim arrays + gid are the CONSTANT-
    # INDEPENDENT raw forms (arr0=o_orderdate, arr1=order_cust_nation,
    # arr2=supp_nation, arr3=supp_region; gid = raw supp_nation), and the Q5 filter
    # (region + orderdate window + cust_nation==supp_nation) is evaluated IN-KERNEL
    # from these + the CURRENT query's scalars (o_lo/o_hi/asia_region) threaded into
    # _assemble every run. The constant-INDEPENDENT slots (l_orderkey / l_suppkey)
    # live here; the bounds do NOT. The region_name->regionkey map (ALL 5 regions,
    # itself region-independent) is cached so the WARM path can resolve THIS query's
    # region const to its key with no region dim fed.
    var q5_pred: Bool
    var q5_pred_slots: List[Int]  # [l_orderkey_slot, l_suppkey_slot]
    var q5_region_names: List[String]
    var q5_region_keys: List[Int64]
    # --- Phase 1 column pool (GPU_OP_COLPOOL): the pooled-column ColKeys this
    # entry leased to assemble its cols_d. The lease lifetime == this cached
    # entry's lifetime, so the NEXT signature can dedup against the same resident
    # columns. Released (refcount--) when this entry is evicted from _pin2 (under
    # budget pressure). EMPTY when the flag is off or the path didn't pool (then
    # there is nothing to release -> byte-identical behavior).
    var pool_lease_keys: List[String]

    def __init__(out self, var res: SegResident):
        self.res = res^
        self.mode = STRAT_UNGROUPED
        self.G = 1
        self.gid_slot = 0
        self.M = 0
        self.pass_prog = []
        self.pass_len = 0
        self.metric_ops = []
        self.n_ops_total = 0
        self.metric_offsets = []
        self.metric_lens = []
        self.n_seg = 0
        self.n_cols = 0
        self.n_keys = 0
        self.agg_kind = []
        self.agg_scale = []
        self.agg_m0 = []
        self.agg_m1 = []
        self.gk_is_str = []
        self.gk_str_vals = []
        self.gk_i64_vals = []
        self.n_cand = 0
        self.emit_agg = -1
        self.emit_gt0 = False
        self.hash_gk_slot = 0
        self.hash_cap = 0
        self.hash_gk_dim_arr = []
        self.kind = KIND_UNKNOWN
        self.q6_pred = False
        self.q6_pred_slots = []
        self.gen_pred = False
        self.gen_pred_slots = []
        self.gen_pred_cmps = []
        self.q5_pred = False
        self.q5_pred_slots = []
        self.q5_region_names = []
        self.q5_region_keys = []
        self.pool_lease_keys = []


def _make_pin2() -> Dict[String, GpuPinned]:
    return Dict[String, GpuPinned]()


comptime _pin2 = _Global["mojo_gpu_pin2", _make_pin2]


def _pin2_ptr() raises -> UnsafePointer[Dict[String, GpuPinned], MutAnyOrigin]:
    return _pin2.get_or_create_ptr()


# ===-------------------------------------------------------------------===#
# Phase 1 column pool (GPU_OP_COLPOOL) -- helpers shared by the 3 cold paths.
#
# The pool deduplicates the COLD H2D upload of aggregate FACT columns shared
# across query signatures. KERNELS AND C++ ARE UNTOUCHED: each query still owns a
# packed `cols_d` of the SAME bytes/layout/length as today; the only change is
# HOW it gets filled -- shared columns are D2D-copied from a resident per-column
# buffer instead of re-uploaded from host. Flag off => none of this runs and the
# behavior is byte-identical (incl. _pin2 unbounded).
# ===-------------------------------------------------------------------===#

def _colpool_on() -> Bool:
    return getenv("GPU_OP_COLPOOL", "") != ""


# Phase 3 (Option B) column-pointer-table flag. When on, eligible queries read
# pooled columns DIRECTLY via a per-column device-pointer table instead of a
# packed D2D copy -- eliminating the Phase 1/2 cols_d duplicate (~44% resident
# VRAM). Requires the pool (it references pooled buffers); implies _colpool_on.
# Off by default => the packed path runs (byte-identical).
def _colptr_on() -> Bool:
    return _colpool_on() and getenv("GPU_OP_COLPTR", "") != ""


# Phase 3 is active for THIS query iff: the colptr flag is on AND the query is the
# in-scope all-pooled class. Initial scope: Q6 UNGROUPED with the in-kernel
# predicate-independent filter (`_q6_pred_enabled`) -- its cols_d is entirely
# pooled fact columns, so the pointer table captures the full win and the only
# colptr-capable kernel wired is seg_ungrouped_kernel_q6_pred[True]. ANY other
# kind/strategy returns False -> the packed pool path runs (correct). Composes
# with skip-materialize (omitted slots are pool borrows -> pointers, not copies).
def _colptr_eligible(d: GpuPlanDescriptor) -> Bool:
    if not _colptr_on():
        return False
    if d.kind == KIND_Q6:
        return d.strategy == STRAT_UNGROUPED and _q6_pred_enabled(d)
    if d.kind == KIND_Q14:
        # Q14 UNGROUPED FK-join, predicate-independent: fact cols are all pooled;
        # the dim arrays stay a separate buffer (read unchanged by the kernel).
        return d.strategy == STRAT_UNGROUPED and _gen_pred_enabled(d)
    return False


# SKIP-MATERIALIZE sub-flag (the cold WALL-TIME follow-up to Phase 1). When on,
# the SQL-feed path emits a NARROWER fact SELECT that omits fact columns already
# pool-resident: DuckDB's columnar scan reads only the non-resident fact columns,
# and the finalize sources the resident ones from the pool (Phase 1 already
# D2D's pool HITs). `GPU_OP_COLPOOL=2` (Phase 1 + narrow-SQL) or the explicit
# GPU_OP_SKIP_MATERIALIZE toggle enables it. `_colpool_on()` (!= "") stays the
# pool gate; off or =1 => narrowing never happens => byte-identical to Phase 1.
def _skipmat_on() -> Bool:
    return (
        getenv("GPU_OP_COLPOOL", "") == "2"
        or getenv("GPU_OP_SKIP_MATERIALIZE", "") != ""
    )


# SKIP-MATERIALIZE is active for THIS query iff: the pool is on, the sub-flag is
# on, the descriptor is one of the in-scope ungrouped predicate-independent
# classes (Q6 / Q14 -- Q1 is DENSE_GROUP and deferred, Q5 deferred, Q3 excluded),
# the predicate-independent path is ACTIVE (so the host pass-bake reads NO fact
# data -> omitted fact columns are never needed on the host), and the row order
# is STORAGE (no ORDER BY; SORT_SEGREDUCE never eligible). ANY uncertainty here
# returns False -> the full SELECT is emitted (feed everything) -> correct.
def _skipmat_active(d: GpuPlanDescriptor) -> Bool:
    if not _colpool_on() or not _skipmat_on():
        return False
    # STORAGE ordering only: SORT_SEGREDUCE appends ORDER BY (different row order)
    # and is excluded; Q6/Q14 are UNGROUPED (no ORDER BY).
    if d.strategy == STRAT_SORT_SEGREDUCE:
        return False
    if d.kind == KIND_Q6:
        # Pure UNGROUPED Q6 with the in-kernel (predicate-independent) filter.
        return d.strategy == STRAT_UNGROUPED and _q6_pred_enabled(d)
    if d.kind == KIND_Q14:
        # UNGROUPED FK-join Q14 with the generalized in-kernel fact filter.
        return d.strategy == STRAT_UNGROUPED and _gen_pred_enabled(d)
    return False


# Resident-byte budget in bytes. Mirrors the C++ PinBudgetBytes: GPU_OP_PIN_BUDGET_MB
# overrides (in MB); default 4 GiB. 0 => unbounded. Read every call (cheap; the
# value is a process-wide policy but re-reading keeps it stateless on the Mojo side).
def _pin_budget_bytes() -> Int:
    var env = getenv("GPU_OP_PIN_BUDGET_MB", "")
    if env != "":
        try:
            var mb = Int(env)
            if mb >= 0:
                return mb * 1024 * 1024
        except:
            pass
    return 4096 * 1024 * 1024  # 4 GiB default cap


# Approximate VRAM footprint of one cached GpuPinned's resident device buffers
# (the SegResident). cols_d (n_cols*n_rows int64) dominates; add the dim arrays +
# seg offsets. Conservative so the budget never under-counts what is resident.
def _gp_footprint_bytes(gp: GpuPinned) -> Int:
    var b: Int
    if gp.res.use_colptr:
        # Phase 3: no packed cols_d -- the pooled columns live ONLY in the pool
        # (counted by pool_bytes, NOT here). This resident owns just the small
        # pointer table (n_cols int64) + the per-query derived/fallback buffers
        # (each n_rows int64). That delta IS the ~44% VRAM win.
        b = gp.res.n_cols * 8
        b += len(gp.res.derived_bufs) * gp.res.n_rows * 8
    else:
        b = gp.res.n_cols * gp.res.n_rows * 8
    # dim arrays: dim_offsets[n_dims] elements (int64); seg offsets: n_seg+1.
    if gp.res.n_dims > 0 and len(gp.res.dim_offsets) > gp.res.n_dims:
        b += Int(gp.res.dim_offsets[gp.res.n_dims]) * 8
    if gp.res.n_seg > 0:
        b += (gp.res.n_seg + 1) * 8
    return b


# Footprints of all cached _pin2 entries (the column bytes the aggregate
# residency holds outside the pool). Tracked via the pool's side-bookkeeping
# (the Movable-only _pin2 Dict can't be iterated under this nightly), kept in
# lockstep by _pin2_cache() / _evict_one_pin2(). Charged with pool_bytes()
# against the budget when GPU_OP_COLPOOL is on.
def _pin2_resident_bytes() raises -> Int:
    return pin2_resident_bytes()


# Register a just-inserted _pin2 entry's footprint with the pool side-bookkeeping
# (flag-on only). Called right AFTER `p2[sig] = gp^` at every wired insertion
# site, using a footprint captured BEFORE the move. Uses the col_pool global
# (not _pin2), so it never re-borrows the live `ref p2`. Flag off => no-op, so
# _pin2 stays exactly as today (unbounded, untracked).
def _pin2_track(sig: String, footprint_bytes: Int) raises:
    if _colpool_on():
        pin2_register(sig, footprint_bytes)


# Evict the oldest (insertion-order ~ LRU for a single-source stream) cached
# _pin2 entry to reclaim VRAM. Releases the entry's pooled-column leases first
# (so those columns become evictable), unregisters its footprint, then drops the
# entry (freeing its SegResident device buffers). Returns the freed footprint
# bytes, or 0 if the cache is empty. Used only under budget pressure with the
# flag on. Correctness-safe: an evicted signature simply goes COLD next run.
def _evict_one_pin2() raises -> Int:
    var victim = pin2_oldest_key()
    if victim == "":
        return 0
    ref p2 = _pin2_ptr()[]
    if victim not in p2:
        # Side list drifted (shouldn't happen); drop the stale key + retry once.
        pin2_unregister(victim, 0)
        return 0
    var freed = _gp_footprint_bytes(p2[victim])
    # Release this entry's pooled-column leases so they can be evicted too.
    for k in range(len(p2[victim].pool_lease_keys)):
        release_lease(p2[victim].pool_lease_keys[k])
    _ = p2.pop(victim)
    pin2_unregister(victim, freed)
    return freed


# Make room for `need` more resident bytes (a pending pool miss) under the
# budget. Evict pooled LRU columns FIRST (conservative: prefer dropping pooled
# columns over the aggregate residency, to avoid warm-hit regression), then LRU
# _pin2 entries. Stops when within budget or nothing more is evictable (the
# subsequent allocation's OOM-retry is the backstop). budget == 0 => unbounded.
def _colpool_make_room(need: Int) raises:
    var budget = _pin_budget_bytes()
    if budget == 0:
        return
    while True:
        var resident = pool_bytes() + _pin2_resident_bytes()
        if resident + need <= budget:
            return
        # Prefer evicting pooled columns (cheap to D2D-refill on a later cold
        # miss) over the cached aggregate residency. evict_victim() applies the
        # cost-aware keep-benefit policy when GPU_OP_COLPOOL_COSTAWARE is set,
        # else plain LRU (Phase 1 default).
        var freed = evict_victim()
        if freed > 0:
            continue
        # No evictable pooled column left -> drop an aggregate residency.
        freed = _evict_one_pin2()
        if freed == 0:
            return  # nothing evictable; allocation OOM-retry handles the rest


# Assemble the per-signature packed `cols_d` via the column pool (flag-on,
# STORAGE-only callers). `cols_host` is the FULLY packed host buffer the cold
# path already built (numeric slots + gid/pass slots), byte-identical to today.
# For each NUMERIC fact slot we ensure_column the pooled per-column buffer and
# D2D-copy it into slot offset `slot*n`; non-numeric/derived slots (gid, pass)
# are H2D-copied from `cols_host` as today (they are per-query derived, never
# pooled). On a not-poolable / fallback slot we H2D-copy that slot straight from
# `cols_host` -- so the result is ALWAYS byte-identical regardless of HIT/MISS/
# fallback. Returns the assembled cols_d (ownership to caller) and appends the
# leased ColKeys to `out_lease_keys` (stored in the GpuPinned for release on
# eviction). Per-column HIT/MISS is logged when GPU_OP_PIN_LOG is set.
#
# `mat_col_of_slot[slot]` is the mat_cols index of numeric slot `slot` (i.e.
# numeric_matcols); slots >= n_numeric are derived (gid/pass) and copied whole.
# Build the per-NUMERIC-SLOT omit mask from st.colpool_omit (indexed by mat_cols).
# omit_slot[slot] == st.colpool_omit[numeric_matcols[slot]] (False when the omit
# mask is absent/short -> nothing omitted -> Phase 1 behavior). Used by the three
# cold finalize paths to gate the packing loop + drive _colpool_assemble_cols_d.
def _omit_slot_mask(st: GpuExecState, numeric_matcols: List[Int]) -> List[Bool]:
    var out: List[Bool] = []
    for slot in range(len(numeric_matcols)):
        var mj = numeric_matcols[slot]
        if mj >= 0 and mj < len(st.colpool_omit):
            out.append(st.colpool_omit[mj])
        else:
            out.append(False)
    return out^


def _colpool_assemble_cols_d(
    ctx: DeviceContext,
    fact_table: String,
    st: GpuExecState,
    cols_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_slots: Int,
    n: Int,
    numeric_matcols: List[Int],
    omit_slot: List[Bool],
    mut out_lease_keys: List[String],
    mut fail: Bool,
) raises -> DeviceBuffer[DType.int64]:
    var pin_log = getenv("GPU_OP_PIN_LOG", "") != ""
    var n_numeric = len(numeric_matcols)
    var skipped_cols = 0  # SKIP-MATERIALIZE diagnostic: omitted (pool-sourced) cols
    fail = False
    # Allocate the device-side packed buffer (same size as segreduce_upload).
    var total = n_slots * n if n_slots * n > 0 else 1
    var cols_d = ctx.enqueue_create_buffer[DType.int64](total)

    # --- numeric fact slots: dedup via the pool, D2D into slot offset. ---
    for slot in range(n_numeric):
        var mj = numeric_matcols[slot]
        var col_name = st.mat_cols[mj]
        var key = col_key(fact_table, col_name, REPR_INT64_PACKED, ORDERING_STORAGE, n)
        # Host pointer to this slot's already-packed int64 values (slot*n..+n).
        # For an OMITTED slot this host region is UNFILLED (the packing loop and
        # the feed both skipped it) -- it MUST be sourced from the pool, never from
        # host. For a non-omitted slot it holds the freshly-packed bytes.
        var slot_host = cols_host + slot * n
        var omit = omit_slot[slot] if slot < len(omit_slot) else False

        if omit:
            # SKIP-MATERIALIZE: the column was NOT scanned/fed. It is GUARANTEED
            # resident -- materialize_sql only omitted it after taking an in-use
            # LEASE (refcount++) on this exact key, and a leased column is never
            # evicted by _colpool_make_room, so the probe->finalize race is closed.
            # col_pool_borrow returns the buffer WITHOUT a refcount bump (the
            # materialize lease in st.colpool_omit_leases is the only +1 for this
            # column; the finalize transfers ALL of those to the GpuPinned after a
            # successful assemble, so we DO NOT append `key` to out_lease_keys here
            # -- that would double-count the lease). was_hit backstop (landmine #4):
            # if the column is somehow gone (impossible while leased), we CANNOT
            # reconstruct it (it was never fed) -> signal failure -> finalize nonzero
            # rc -> CPU fallback (never wrong). slot_host is unfilled, never read.
            var er = col_pool_borrow(ctx, key)
            if not (er.ok and er.was_hit):
                fail = True
                if pin_log:
                    print(
                        "[gpu-op colpool] OMIT-MISS (fallback) col=", col_name,
                        " n=", n, file=FileDescriptor(2),
                    )
                # Return early; the caller checks `fail` and bails to CPU.
                return cols_d^
            var dst = cols_d.create_sub_buffer[DType.int64](slot * n, n)
            ctx.enqueue_copy(dst, er.buf)
            skipped_cols += 1
            if pin_log:
                print(
                    "[gpu-op colpool] SKIP-MAT col=", col_name,
                    " n=", n, file=FileDescriptor(2),
                )
            continue

        # Budget: make room for one more resident column (n int64) before a miss.
        # (HITs add nothing, but make_room is cheap when already within budget.)
        _colpool_make_room(n * 8)
        var er = ensure_column(
            ctx, key, REPR_INT64_PACKED, ORDERING_STORAGE, slot_host, n,
            st.cols[mj].type_tag,
        )
        if er.ok:
            out_lease_keys.append(key)
            # D2D copy the resident column into THIS query's slot offset.
            var dst = cols_d.create_sub_buffer[DType.int64](slot * n, n)
            ctx.enqueue_copy(dst, er.buf)
            if pin_log:
                var tag = "HIT" if er.was_hit else "MISS"
                print(
                    "[gpu-op colpool] ", tag, " col=", col_name,
                    " n=", n, file=FileDescriptor(2),
                )
        else:
            # Not poolable / nothing evictable -> upload this slot straight from
            # host, exactly as today (byte-identical; just not deduped).
            var dst = cols_d.create_sub_buffer[DType.int64](slot * n, n)
            ctx.enqueue_copy(dst, slot_host)
            if pin_log:
                print(
                    "[gpu-op colpool] FALLBACK col=", col_name,
                    " n=", n, file=FileDescriptor(2),
                )

    # --- derived slots (gid, pass): H2D straight from the packed host buffer. ---
    for slot in range(n_numeric, n_slots):
        var dst = cols_d.create_sub_buffer[DType.int64](slot * n, n)
        var slot_host = cols_host + slot * n
        ctx.enqueue_copy(dst, slot_host)

    ctx.synchronize()
    if pin_log and skipped_cols > 0:
        print(
            "[gpu-op colpool] skipped_cols=", skipped_cols,
            " (fact cols sourced from pool, not scanned/fed)",
            file=FileDescriptor(2),
        )
    return cols_d^


# ===-------------------------------------------------------------------===#
# Phase 3 (GPU_OP_COLPTR) -- column-POINTER-TABLE assembly. The Option B sibling
# of _colpool_assemble_cols_d: instead of allocating a packed cols_d and D2D-
# copying every pooled column into it (a full SECOND copy of the pooled data),
# this builds a small device table of n_slots DEVICE ADDRESSES:
#   - pooled slot (HIT / MISS / skip-mat borrow): the address is the resident POOL
#     buffer's own device pointer -> read DIRECTLY, no copy (the VRAM win).
#   - non-poolable fallback / derived (gid/pass) slot: a per-query buffer filled
#     H2D from the packed host buffer's slot region, kept alive in `out_derived_bufs`
#     for the resident's lifetime; its pointer goes in the table.
# The kernels (launched USE_COLPTR=True) read column `slot` via table[slot] -> the
# SAME int64 values the packed layout holds, so results are bit-exact. The pooled
# addresses stay valid because the GpuPinned holds the pool leases (out_lease_keys
# + the transferred omit leases) for its whole lifetime (invariant #3).
#
# Returns the device pointer-table buffer (ownership to caller). `fail` is set on a
# skip-mat omit that is not a guaranteed HIT (same backstop as the packed path) ->
# caller bails to CPU.
# ===-------------------------------------------------------------------===#
def _colpool_assemble_col_ptrs(
    ctx: DeviceContext,
    fact_table: String,
    st: GpuExecState,
    cols_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_slots: Int,
    n: Int,
    numeric_matcols: List[Int],
    omit_slot: List[Bool],
    mut out_lease_keys: List[String],
    mut out_derived_bufs: List[DeviceBuffer[DType.int64]],
    mut fail: Bool,
) raises -> DeviceBuffer[DType.int64]:
    var pin_log = getenv("GPU_OP_PIN_LOG", "") != ""
    var n_numeric = len(numeric_matcols)
    var skipped_cols = 0
    fail = False
    # Host array of the n_slots device addresses (uploaded to the table at the end).
    var addr_h = alloc[Int64](n_slots if n_slots > 0 else 1)

    # --- numeric fact slots: pooled -> pool pointer; fallback -> per-query buf. ---
    for slot in range(n_numeric):
        var mj = numeric_matcols[slot]
        var col_name = st.mat_cols[mj]
        var key = col_key(fact_table, col_name, REPR_INT64_PACKED, ORDERING_STORAGE, n)
        var slot_host = cols_host + slot * n
        var omit = omit_slot[slot] if slot < len(omit_slot) else False

        if omit:
            # skip-materialize: GUARANTEED resident pool borrow (lease held at
            # materialize). Point directly at the pooled buffer; no copy.
            var er = col_pool_borrow(ctx, key)
            if not (er.ok and er.was_hit):
                fail = True
                addr_h.free()
                if pin_log:
                    print(
                        "[gpu-op colptr] OMIT-MISS (fallback) col=", col_name,
                        file=FileDescriptor(2),
                    )
                return ctx.enqueue_create_buffer[DType.int64](1)
            addr_h[slot] = Int64(Int(er.buf.unsafe_ptr()))
            skipped_cols += 1
            continue

        _colpool_make_room(n * 8)
        var er = ensure_column(
            ctx, key, REPR_INT64_PACKED, ORDERING_STORAGE, slot_host, n,
            st.cols[mj].type_tag,
        )
        if er.ok:
            out_lease_keys.append(key)
            addr_h[slot] = Int64(Int(er.buf.unsafe_ptr()))  # pooled buffer ptr
            if pin_log:
                var tag = "HIT" if er.was_hit else "MISS"
                print("[gpu-op colptr] ", tag, " col=", col_name, file=FileDescriptor(2))
        else:
            # Not poolable -> per-query buffer, H2D from host (kept alive).
            var buf = ctx.enqueue_create_buffer[DType.int64](n if n > 0 else 1)
            ctx.enqueue_copy(buf, slot_host)
            addr_h[slot] = Int64(Int(buf.unsafe_ptr()))
            out_derived_bufs.append(buf)
            if pin_log:
                print("[gpu-op colptr] FALLBACK col=", col_name, file=FileDescriptor(2))

    # --- derived slots (gid/pass): per-query buffers, H2D from packed host. ---
    for slot in range(n_numeric, n_slots):
        var buf = ctx.enqueue_create_buffer[DType.int64](n if n > 0 else 1)
        ctx.enqueue_copy(buf, cols_host + slot * n)
        addr_h[slot] = Int64(Int(buf.unsafe_ptr()))
        out_derived_bufs.append(buf)

    # Upload the address table. Sync first so every pooled/derived buffer's address
    # is final before the table (holding those addresses) is copied to device.
    ctx.synchronize()
    var col_ptrs_d = ctx.enqueue_create_buffer[DType.int64](
        n_slots if n_slots > 0 else 1
    )
    ctx.enqueue_copy(col_ptrs_d, addr_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()
    addr_h.free()
    if pin_log and skipped_cols > 0:
        print(
            "[gpu-op colptr] skipped_cols=", skipped_cols,
            " (pooled fact cols read via pointer table, no copy)",
            file=FileDescriptor(2),
        )
    return col_ptrs_d^


# Re-run the resident kernel + assemble the result table into `dst` from the
# cached GpuPinned metadata + the freshly-computed sums. Used by BOTH the COLD
# path (after it builds + stores the GpuPinned) and the WARM path, so the two
# can never drift. Reads NO fed host columns.
#
# Phase G Stage 2: `q6_bounds` carries the CURRENT query's Q6 filter bounds when
# `gp.q6_pred` is set. They are NOT part of the cache entry (the whole point is
# warm-across-constants), so they are passed in fresh every run from the live
# descriptor. When `gp.q6_pred` is False the spec is ignored (stock pass-column
# path). The slots come from the cache entry (constant-independent); the bounds
# from `q6_bounds`.
#
# Phase G Stage 2 (generalized): `gen_bounds` carries the CURRENT query's Q1/Q14
# fact-range filter bounds (one int64 per fact filter, in descriptor order) when
# `gp.gen_pred` is set. Like `q6_bounds`, they are threaded fresh from the live
# descriptor every run; the constant-INDEPENDENT slots + cmps come from the cache
# entry. The (slot,cmp,bound) triples are assembled here into `fpred_list`.
def _assemble(
    mut dst: GpuExecState,
    mut gp: GpuPinned,
    q6_bounds: Q6PredSpec = Q6PredSpec(False, 0, 0, 0, 0, 0, 0, 0, 0),
    gen_bounds: List[Int64] = [],
    q5_bounds: List[Int64] = [],
) raises:
    var q6_active = gp.q6_pred and gp.kind == KIND_Q6
    var ss = gp.q6_pred_slots[0] if (q6_active and len(gp.q6_pred_slots) == 3) else 0
    var ds = gp.q6_pred_slots[1] if (q6_active and len(gp.q6_pred_slots) == 3) else 0
    var qs = gp.q6_pred_slots[2] if (q6_active and len(gp.q6_pred_slots) == 3) else 0
    # Generalized Q1/Q14 in-kernel predicate. Assemble (slot,cmp,bound) triples
    # from the cached slots+cmps + the live bounds (same descriptor-filter order).
    var gen_active = (
        gp.gen_pred
        and (gp.kind == KIND_Q1 or gp.kind == KIND_Q14)
        and len(gp.gen_pred_slots) == len(gen_bounds)
        and len(gp.gen_pred_slots) == len(gp.gen_pred_cmps)
        and len(gp.gen_pred_slots) > 0
    )
    var fpred_list: List[Int64] = []
    if gen_active:
        for p in range(len(gp.gen_pred_slots)):
            fpred_list.append(Int64(gp.gen_pred_slots[p]))
            fpred_list.append(gp.gen_pred_cmps[p])
            fpred_list.append(gen_bounds[p])
    # Phase G Stage 2 (Path B): Q5 in-kernel predicate. The resident dim arrays +
    # gid are constant-independent; THIS query's region/date scalars come fresh from
    # `q5_bounds` ([o_lo, o_hi, asia_region], assembled by the caller from the live
    # descriptor + the cached region map). Slots (l_orderkey / l_suppkey) are
    # constant-independent and live in the cache entry.
    var q5_active = (
        gp.q5_pred
        and gp.kind == KIND_Q5
        and len(gp.q5_pred_slots) == 2
        and len(q5_bounds) == 3
    )
    var q5_lok = gp.q5_pred_slots[0] if q5_active else 0
    var q5_lsk = gp.q5_pred_slots[1] if q5_active else 0
    var q5_o_lo = q5_bounds[0] if q5_active else Int64(0)
    var q5_o_hi = q5_bounds[1] if q5_active else Int64(0)
    var q5_asia = q5_bounds[2] if q5_active else Int64(0)
    var sums = segreduce_run(
        gp.res,
        gp.mode,
        gp.pass_prog.unsafe_ptr(),
        gp.pass_len,
        gp.metric_ops.unsafe_ptr(),
        gp.n_ops_total,
        gp.metric_offsets.unsafe_ptr(),
        gp.metric_lens.unsafe_ptr(),
        gp.M,
        gp.gid_slot,
        gp.G,
        gp.kind,
        q6_pred_active=q6_active,
        q6_ship_slot=ss,
        q6_disc_slot=ds,
        q6_qty_slot=qs,
        q6_ship_lo=q6_bounds.ship_lo,
        q6_ship_hi=q6_bounds.ship_hi,
        q6_disc_lo=q6_bounds.disc_lo,
        q6_disc_hi=q6_bounds.disc_hi,
        q6_qty_hi=q6_bounds.qty_hi,
        gen_pred_active=gen_active,
        fpred_list=fpred_list^,
        q5_pred_active=q5_active,
        q5_lok_slot=q5_lok,
        q5_lsk_slot=q5_lsk,
        q5_o_lo=q5_o_lo,
        q5_o_hi=q5_o_hi,
        q5_asia_region=q5_asia,
    )

    var n_cols = gp.n_cols
    var n_keys = gp.n_keys
    var res_lo: List[Int64] = []
    var res_hi: List[Int64] = []
    var res_f64: List[Float64] = []
    var res_str: List[String] = []
    var out_rows = 0
    for g in range(gp.n_cand):
        # Emit rule: optionally gate on a SUM aggregate's freshly-computed sum.
        if gp.emit_agg >= 0:
            var gv = sums[g * gp.M + gp.agg_m0[gp.emit_agg]]
            if gp.emit_gt0:
                if gv <= Int128(0):
                    continue
            else:
                if gv == Int128(0):
                    continue
        for _ in range(n_cols):
            res_lo.append(0)
            res_hi.append(0)
            res_f64.append(0.0)
            res_str.append(String(""))
        var base = out_rows * n_cols
        # group-key cells (in group-key order).
        for gk in range(n_keys):
            if gp.gk_is_str[gk]:
                res_str[base + gk] = gp.gk_str_vals[g][gk]
            else:
                res_lo[base + gk] = gp.gk_i64_vals[g][gk]
        # aggregate cells.
        for ai in range(len(gp.agg_kind)):
            var col = n_keys + ai
            var v0 = sums[g * gp.M + gp.agg_m0[ai]]
            if gp.agg_kind[ai] == AGG_COUNT_STAR:
                res_lo[base + col] = v0.cast[DType.int64]()
            elif gp.agg_kind[ai] == AGG_AVG:
                var cnt = sums[g * gp.M + gp.agg_m1[ai]].cast[DType.int64]()
                var sumf = Float64(v0.cast[DType.int64]())
                var scale_div = Float64(1)
                for _ in range(Int(gp.agg_scale[ai])):
                    scale_div *= 10.0
                res_f64[base + col] = (
                    sumf / scale_div / Float64(cnt)
                ) if cnt != 0 else 0.0
            else:  # AGG_SUM -> i128 limbs at ret_scale
                res_lo[base + col] = v0.cast[DType.int64]()
                res_hi[base + col] = (v0 >> 64).cast[DType.int64]()
        out_rows += 1

    dst.res_rows = out_rows
    dst.res_cols = n_cols
    dst.res_lo = res_lo^
    dst.res_hi = res_hi^
    dst.res_f64 = res_f64^
    dst.res_str = res_str^


# Re-run the HASH_GROUP kernel + assemble. Used by BOTH cold and warm Q3 paths
# on NVIDIA/AMD. The kernel discovers the occupied groups (l_orderkey), atomic-
# accumulates the int64 revenue per group, and we widen to int128 here (the same
# exactness contract: per-order revenue fits int64; see _pin_finalize). For each
# group we emit one row iff its gated SUM > 0, look up the dim-carried group keys
# (o_orderdate / o_shippriority) by the group key from the cached host dim arrays
# (constant within an orderkey), and place the aggregate cells exactly as
# `_assemble` does. Output row ORDER is unspecified (the parent ORDER BY sorts).
def _assemble_hash(mut dst: GpuExecState, mut gp: GpuPinned) raises:
    var hr = segreduce_run_hash(
        gp.res,
        gp.hash_gk_slot,
        gp.hash_cap,
        gp.pass_prog.unsafe_ptr(),
        gp.pass_len,
        gp.metric_ops.unsafe_ptr(),
        gp.n_ops_total,
        gp.metric_offsets.unsafe_ptr(),
        gp.metric_lens.unsafe_ptr(),
        gp.M,
    )
    var n_groups = len(hr.keys)

    var n_cols = gp.n_cols
    var n_keys = gp.n_keys
    var res_lo: List[Int64] = []
    var res_hi: List[Int64] = []
    var res_f64: List[Float64] = []
    var res_str: List[String] = []
    var out_rows = 0
    for g in range(n_groups):
        # Emit rule: gate on a SUM aggregate's freshly-computed group sum.
        if gp.emit_agg >= 0:
            var gv = hr.sums[g * gp.M + gp.agg_m0[gp.emit_agg]]
            if gp.emit_gt0:
                if gv <= Int128(0):
                    continue
            else:
                if gv == Int128(0):
                    continue
        for _ in range(n_cols):
            res_lo.append(0)
            res_hi.append(0)
            res_f64.append(0.0)
            res_str.append(String(""))
        var base = out_rows * n_cols
        var gkey = hr.keys[g]
        # group-key cells: the fact key == gkey; dim-carried keys gathered from
        # the cached host dim array indexed by gkey (constant within an orderkey).
        for gk in range(n_keys):
            if gk < len(gp.hash_gk_dim_arr) and len(gp.hash_gk_dim_arr[gk]) > 0:
                ref arr = gp.hash_gk_dim_arr[gk]
                var idx = Int(gkey)
                var v = Int64(0)
                if idx >= 0 and idx < len(arr):
                    v = arr[idx]
                res_lo[base + gk] = v
            else:
                res_lo[base + gk] = gkey  # the fact group key
        # aggregate cells (mirrors _assemble; Q3 has one DECIMAL SUM, no AVG).
        for ai in range(len(gp.agg_kind)):
            var col = n_keys + ai
            var v0 = hr.sums[g * gp.M + gp.agg_m0[ai]]
            if gp.agg_kind[ai] == AGG_COUNT_STAR:
                res_lo[base + col] = v0.cast[DType.int64]()
            elif gp.agg_kind[ai] == AGG_AVG:
                var cnt = hr.sums[g * gp.M + gp.agg_m1[ai]].cast[DType.int64]()
                var sumf = Float64(v0.cast[DType.int64]())
                var scale_div = Float64(1)
                for _ in range(Int(gp.agg_scale[ai])):
                    scale_div *= 10.0
                res_f64[base + col] = (
                    sumf / scale_div / Float64(cnt)
                ) if cnt != 0 else 0.0
            else:  # AGG_SUM -> i128 limbs at ret_scale
                res_lo[base + col] = v0.cast[DType.int64]()
                res_hi[base + col] = (v0 >> 64).cast[DType.int64]()
        out_rows += 1

    dst.res_rows = out_rows
    dst.res_cols = n_cols
    dst.res_lo = res_lo^
    dst.res_hi = res_hi^
    dst.res_f64 = res_f64^
    dst.res_str = res_str^


# ---------------------------------------------------------------------------
# Schema getters.
# ---------------------------------------------------------------------------
@export("mojo_gpu_desc_group_index")
def mojo_gpu_desc_group_index(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return Int(IDX_NONE)
    return Int(handle.bitcast[GpuPlanDescriptor]()[].group_index)


@export("mojo_gpu_desc_aggregate_index")
def mojo_gpu_desc_aggregate_index(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return Int(IDX_NONE)
    return Int(handle.bitcast[GpuPlanDescriptor]()[].aggregate_index)


@export("mojo_gpu_desc_out_arity")
def mojo_gpu_desc_out_arity(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    return len(handle.bitcast[GpuPlanDescriptor]()[].out_types)


@export("mojo_gpu_desc_out_type")
def mojo_gpu_desc_out_type(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    i: Int,
    out_tag: UnsafePointer[Int64, MutAnyOrigin],
    out_scale: UnsafePointer[Int64, MutAnyOrigin],
    out_width: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int:
    if Int(handle) == 0:
        return 1
    ref ots = handle.bitcast[GpuPlanDescriptor]()[].out_types
    if i < 0 or i >= len(ots):
        return 1
    ref t = ots[i]
    out_tag[] = t[0]
    out_scale[] = t[1]
    out_width[] = t[2]
    return 0


# ---------------------------------------------------------------------------
# Materialization.
# ---------------------------------------------------------------------------
@export("mojo_gpu_desc_materialize_count")
def mojo_gpu_desc_materialize_count(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    ref d = handle.bitcast[GpuPlanDescriptor]()[]
    # Request 0 is always the fact-table query. For an FK-join plan each dim_edge
    # adds one dim request (request index 1..n_dims), so the count is 1 + n_dims.
    # 0-dim shapes (ungrouped Q6 / grouped DENSE_GROUP Q1) -> one fact request.
    return 1 + len(d.dim_edges)


# The DISTINCT fact-table columns the shuttle must SELECT + feed, in a
# deterministic order: fact group-key columns first (in group-key order), then
# `fact_projected_columns` (fact filters in filter order, then aggregate-program
# LOAD_COLs). De-dups across the two (a group key also referenced by an
# aggregate is fed once). For Q6 (no group keys) this equals
# fact_projected_columns, so the Q6 path is unchanged.
def _materialize_columns(d: GpuPlanDescriptor) -> List[String]:
    var cols: List[String] = []

    def _add(mut cols: List[String], name: String):
        for i in range(len(cols)):
            if cols[i] == name:
                return
        cols.append(name)

    for gk in range(len(d.group_keys)):
        ref k = d.group_keys[gk]
        if k.table == d.fact_table:
            _add(cols, k.column)
    var proj = descriptor.fact_projected_columns(d)
    for i in range(len(proj)):
        _add(cols, proj[i])
    # FK-join: the fact-side join key (e.g. l_partkey) is referenced only by the
    # join edge, not by any filter/agg program, so add it here. It is gathered
    # on-GPU via OP_LOAD_DIM (the dim array is indexed by this key value).
    for de in range(len(d.dim_edges)):
        ref e = d.dim_edges[de]
        if e.fact_key.table == d.fact_table:
            _add(cols, e.fact_key.column)
    # A join cond can reference a fact column that the chosen attach edge did NOT
    # pick as its fact_key (Q5: supplier attaches transitively via a nation cond,
    # so the `l_suppkey = s_suppkey` cond's fact side l_suppkey is otherwise never
    # materialized). The Q5 lowering gathers supplier per lineitem row by
    # l_suppkey, so every fact-table join-cond column must be fed. Additive: only
    # adds key columns already implied by the join graph.
    for ji in range(len(d.joins)):
        ref jn = d.joins[ji]
        for ci in range(len(jn.conds)):
            ref c = jn.conds[ci]
            if c.lt == d.fact_table:
                _add(cols, c.lc)
            if c.rt == d.fact_table:
                _add(cols, c.rc)
    return cols^


# The columns to SELECT from dim-edge `de`'s dimension table, in a deterministic
# order: the dim PK (join key) first, then every carried dim column referenced by
# an aggregate program (LOAD_COL or PROMO_PRED on this dim table), then any dim
# filter columns. The pin builds a DENSE array indexed by the PK value from the
# carried payload (Q14: the promo flag derived from p_type). Returns parallel
# lists (column name, contract TypeTag-ish role) — the role distinguishes the PK
# / carried / filter columns for the pin's dense-array build.
def _dim_columns(d: GpuPlanDescriptor, de: Int) -> List[String]:
    ref e = d.dim_edges[de]
    var dim_table = e.dim_table
    var cols: List[String] = []

    def _add(mut cols: List[String], name: String):
        for i in range(len(cols)):
            if cols[i] == name:
                return
        cols.append(name)

    # 1. the dim PK (the join key on the dim side) — drives the dense index.
    _add(cols, e.dim_key.column)
    # 2. carried columns referenced by aggregate programs (LOAD_COL / PROMO_PRED).
    for ai in range(len(d.aggregates)):
        ref agg = d.aggregates[ai]
        for li in range(len(agg.load_cols)):
            if agg.load_cols[li].table == dim_table:
                _add(cols, agg.load_cols[li].column)
        for pi in range(len(agg.promo_cols)):
            if agg.promo_cols[pi].table == dim_table:
                _add(cols, agg.promo_cols[pi].column)
    # 3. dim filter columns (for the dim-filter-AND mechanism; Q3/Q5).
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != dim_table:
            continue
        for fi in range(len(g.filters)):
            if g.filters[fi].col.table == dim_table:
                _add(cols, g.filters[fi].col.column)
    # 4. dim-carried GROUP-KEY columns (SORT_SEGREDUCE: o_orderdate /
    #    o_shippriority live on the orders dim and are emitted per group). They
    #    are gathered per output segment by the fact group key's FK value into
    #    this dim, so they must be materialized as carried dim arrays.
    for gk in range(len(d.group_keys)):
        if d.group_keys[gk].table == dim_table:
            _add(cols, d.group_keys[gk].column)
    # 5. CHILD-edge join columns: when another dim_edge attaches to THIS dim
    #    (a transitive dim->dim join, e.g. customer joins orders on o_custkey),
    #    its near-side column lives on this dim and is the FK used to fold the
    #    child's pass flag into this dim. Materialize it so the host fold can
    #    gather is_building[o_custkey]. This is the generalization that makes a
    #    dim attached to a dim work like a dim attached to the fact.
    for ce in range(len(d.dim_edges)):
        if ce == de:
            continue
        if d.dim_edges[ce].fact_key.table == dim_table:
            _add(cols, d.dim_edges[ce].fact_key.column)
    # 6. CORRELATED join-cond columns on this dim (Q5): a join cond can couple two
    #    dims on a non-edge column (c_nationkey = s_nationkey) — neither side is
    #    the chosen attach edge's key, so rules 1/5 miss it. Materialize every
    #    join-cond column that lives on THIS dim table so the correlated compare
    #    (evaluated per fact row on the GPU via OP_EQ) has the value available.
    for ji in range(len(d.joins)):
        ref jn = d.joins[ji]
        for ci in range(len(jn.conds)):
            ref c = jn.conds[ci]
            if c.lt == dim_table:
                _add(cols, c.lc)
            if c.rt == dim_table:
                _add(cols, c.rc)
    return cols^


# Build the materialization SQL for request `i` and remember the column order on
# the exec state (created lazily, keyed by the descriptor handle). The columns
# are the DISTINCT fact-table columns referenced by the fact filters + aggregate
# programs, in deterministic (first-seen) order:
#   SELECT <c0>, <c1>, ... FROM <fact_table>
# `ORDER BY` is appended iff strategy == SORT_SEGREDUCE (no-op for Q6).
@export("mojo_gpu_desc_materialize_sql")
def mojo_gpu_desc_materialize_sql(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    i: Int,
    out_ptr: UnsafePointer[UInt8, MutAnyOrigin],
    cap: Int,
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    try:
        ref d = handle.bitcast[GpuPlanDescriptor]()[]
        var n_dims = len(d.dim_edges)
        ref m = _exec_ptr()[]
        var key = Int(handle)

        var sql = String("")
        if i == 0:
            # --- request 0: the fact-table query ---
            # KEEP mat_cols = the FULL list (the finalize slot map + result
            # assembly index by it). SKIP-MATERIALIZE only narrows the SELECT (and
            # the feed) -- it never changes mat_cols.
            var cols = _materialize_columns(d)
            # Lazily create the exec state, sized for the fact columns + dims.
            if key not in m:
                m[key] = GpuExecState(len(cols))
            m[key].mat_cols = cols.copy()
            # Pre-size dim storage so feed_column can route by request index.
            if n_dims > 0 and len(m[key].dim_cols) != n_dims:
                var counts: List[Int] = []
                for de in range(n_dims):
                    counts.append(len(_dim_columns(d, de)))
                m[key].init_dims(counts)

            # --- SKIP-MATERIALIZE: compute which fact columns to OMIT. ---
            # Omit col c IFF skip-materialize is active for this query AND a column
            # is pool-resident under the EXACT key the finalize will form
            # (fact_table, c, REPR_INT64_PACKED, STORAGE, resident_n) AND we can
            # take an in-use LEASE on it. The lease (held from here to the finalize)
            # closes the probe->finalize eviction race: a leased column can never be
            # evicted by _colpool_make_room, so the finalize HIT is GUARANTEED. Any
            # exception -> emit the FULL SELECT (omit nothing) -> correct.
            #
            # materialize_sql(0) is called TWICE by C++ (length then fill), so first
            # RELEASE any leases a prior call took, then recompute from scratch.
            for li in range(len(m[key].colpool_omit_leases)):
                release_lease(m[key].colpool_omit_leases[li])
            m[key].colpool_omit_leases = []

            var omit: List[Bool] = []
            for _ in range(len(cols)):
                omit.append(False)
            var any_omit = False
            var resident_n = -1
            var omit_leases: List[String] = []
            if _skipmat_active(d):
                try:
                    for c in range(len(cols)):
                        var rn = col_pool_resident_nrows(
                            d.fact_table, cols[c], REPR_INT64_PACKED,
                            ORDERING_STORAGE,
                        )
                        if rn < 0:
                            continue
                        # All resident fact cols of one predicate-independent table
                        # share the row count; capture it once. Only omit at THAT n.
                        if resident_n < 0:
                            resident_n = rn
                        if rn != resident_n:
                            continue
                        # Take the lease on the EXACT key the finalize will form.
                        var ck = col_key(
                            d.fact_table, cols[c], REPR_INT64_PACKED,
                            ORDERING_STORAGE, rn,
                        )
                        if col_pool_lease(ck):
                            omit[c] = True
                            any_omit = True
                            omit_leases.append(ck)
                        # lease failed (raced away) -> leave omit[c] False (feed it).
                except:
                    # Omit computation failed -> release any leases we took and
                    # emit the FULL SELECT (feed all). Never omit on uncertainty.
                    for li in range(len(omit_leases)):
                        release_lease(omit_leases[li])
                    omit_leases = []
                    for c in range(len(cols)):
                        omit[c] = False
                    any_omit = False
                    resident_n = -1
            m[key].colpool_omit_leases = omit_leases.copy()

            # Record the omit mask + the emitted-position -> mat_cols-index map so
            # feed_column can keep st.cols indexed by FULL mat_cols order. (When
            # nothing is omitted these are all-False / the identity -- so the
            # feed/finalize behave exactly as Phase 1.)
            m[key].colpool_omit = omit.copy()
            var fpm: List[Int] = []
            for c in range(len(cols)):
                if not omit[c]:
                    fpm.append(c)
            m[key].fed_pos_to_matcol = fpm.copy()

            # If ALL fact columns are omitted (everything resident), a 0-column
            # SELECT is illegal SQL. Seed st.n_rows from the resident row count so
            # the C++ side can SKIP the fact query and rely on it (feed_rowcount is
            # the belt-and-suspenders backstop). The finalize sources every column
            # from the pool. We still emit the (empty) "SELECT  FROM <table>" so
            # the byte length signals C++ via the all-omit detection; C++ checks
            # the parsed column count, not the literal text.
            if any_omit and len(fpm) == 0 and resident_n >= 0:
                m[key].n_rows = resident_n

            sql += "SELECT "
            var emitted = 0
            for c in range(len(cols)):
                if omit[c]:
                    continue
                if emitted > 0:
                    sql += ", "
                sql += cols[c]
                emitted += 1
            sql += " FROM " + d.fact_table
            # SORT_SEGREDUCE: order by the FACT group key (the segment key, e.g.
            # l_orderkey) so each order's lineitems are contiguous for one-warp-
            # per-segment reduction. Dim-carried group keys are NOT sort columns.
            # (SORT_SEGREDUCE is never skip-materialize eligible -> omit is all
            # False here, so this rebuilds the full ORDER-BY SELECT unchanged.)
            if d.strategy == STRAT_SORT_SEGREDUCE and len(d.group_keys) > 0:
                var sort_col = String("")
                for gk in range(len(d.group_keys)):
                    if d.group_keys[gk].table == d.fact_table:
                        sort_col = d.group_keys[gk].column
                        break
                if sort_col == "":
                    sort_col = d.group_keys[0].column
                sql += " ORDER BY " + sort_col
        else:
            # --- request i (1..n_dims): dim-edge i-1's dimension query ---
            var de = i - 1
            if de < 0 or de >= n_dims:
                return 0
            if key not in m:
                # materialize request 0 normally creates the state; be defensive.
                var cols0 = _materialize_columns(d)
                m[key] = GpuExecState(len(cols0))
                m[key].mat_cols = cols0.copy()
            if len(m[key].dim_cols) != n_dims:
                var counts: List[Int] = []
                for k2 in range(n_dims):
                    counts.append(len(_dim_columns(d, k2)))
                m[key].init_dims(counts)
            var dcols = _dim_columns(d, de)
            m[key].dim_mat_cols[de] = dcols.copy()
            sql += "SELECT "
            for c in range(len(dcols)):
                if c > 0:
                    sql += ", "
                sql += dcols[c]
            sql += " FROM " + d.dim_edges[de].dim_table
            # NOTE: dim filters are NOT applied in SQL. The pin materializes ALL
            # dim rows, builds a dense per-PK pass-flag array from the dim filter
            # columns, and ANDs that flag into the row pass program via
            # OP_LOAD_DIM (the dim-filter-AND-via-MUL mechanism). That keeps const
            # formatting (dates/decimals/strings) out of the SQL builder and
            # mirrors how the bespoke kernels gate on is_building[custkey].

        var n = sql.byte_length()
        var bytes = sql.as_bytes()
        var to_copy = n if n < cap else cap
        for k in range(to_copy):
            out_ptr[k] = bytes[k]
        return n
    except:
        return 0


# ---------------------------------------------------------------------------
# Pin + feed + finalize.
# ---------------------------------------------------------------------------
# Compute the RESIDENT-pin signature: fact table + sorted projected fact columns
# (incl. group-key columns) + strategy + dim tables/carried columns AND every
# filter's column+cmp+const lo/hi (fact AND dim filters, including VARCHAR const
# values). Including the constants means a WARM hit ⇒ an IDENTICAL predicate, so
# the resident device buffers AND the per-group/segment result layout cached in
# GpuPinned are valid as-is on re-run (no re-bake needed). This trades "warm
# across different constants" for correctness + simplicity.
def _signature(d: GpuPlanDescriptor) -> String:
    var cols = _materialize_columns(d)
    # sort (insertion) for a stable signature independent of first-seen order
    var sorted: List[String] = []
    for c in range(len(cols)):
        sorted.append(cols[c])
    for a in range(len(sorted)):
        for b in range(a + 1, len(sorted)):
            if sorted[b] < sorted[a]:
                var tmp = sorted[a]
                sorted[a] = sorted[b]
                sorted[b] = tmp
    var sig = d.fact_table + "|"
    for s in range(len(sorted)):
        sig += sorted[s] + ","
    sig += "|strat=" + String(Int(d.strategy))
    sig += "|kind=" + String(Int(d.kind))
    # FK-join: include each dim edge's table + carried/filter columns + the
    # fact-key column so a Q14 pin keys distinctly and a WARM hit is sound.
    for de in range(len(d.dim_edges)):
        ref e = d.dim_edges[de]
        sig += "|dim=" + e.dim_table + ":" + e.fact_key.column + ":"
        var dcols = _dim_columns(d, de)
        for c in range(len(dcols)):
            sig += dcols[c] + ","
    # Phase G Stage 2: for the Q6 predicate-independent path the resident columns'
    # CONTENT is filter-const-independent (materialize SQL has no WHERE) and the
    # filter is evaluated in-kernel from per-run bounds, so a WARM hit across
    # DIFFERENT constants reuses the same resident buffers. EXCLUDE the filter
    # constants (but keep cmp/column structure) so distinct-constant Q6 maps to one
    # signature. We still fold in the cmp+column shape so a genuinely different
    # predicate STRUCTURE keys distinctly. The bounds are threaded per run.
    if _q6_pred_enabled(d):
        for gi in range(len(d.gets)):
            ref g = d.gets[gi]
            for fi in range(len(g.filters)):
                ref p = g.filters[fi]
                sig += (
                    "|q6f="
                    + p.col.table
                    + "."
                    + p.col.column
                    + ":"
                    + String(Int(p.cmp))
                )
        sig += "|q6_pred=1"
        return sig
    # Phase G Stage 2 (generalized): the same const-decoupling for the canonical
    # Q1/Q14 fact-range filter. The resident columns' CONTENT is filter-const-
    # independent (materialize SQL has no WHERE) and the fact filter is evaluated
    # in-kernel from per-run bounds; Q14's promo dim array is itself constant-
    # independent (p_type LIKE 'PROMO%'). So EXCLUDE the fact filter constants
    # (keep cmp+column structure) -> distinct-constant Q1/Q14 map to one signature.
    # The dim edges (already folded above) + cmp/column shape still key a genuinely
    # different predicate STRUCTURE distinctly. Distinct per-kind tag.
    if _gen_pred_enabled(d):
        for gi in range(len(d.gets)):
            ref g = d.gets[gi]
            for fi in range(len(g.filters)):
                ref p = g.filters[fi]
                sig += (
                    "|gpf="
                    + p.col.table
                    + "."
                    + p.col.column
                    + ":"
                    + String(Int(p.cmp))
                )
        sig += "|gen_pred=" + String(Int(d.kind))
        return sig
    # Phase G Stage 2 (Path B): Q5's predicate-independent residency decouples the
    # resident dim arrays + gid from the region/date constants (raw o_orderdate /
    # cust_nation / supp_nation / supp_region; gid = raw supp_nation), evaluating
    # the region + orderdate window + cust_nation==supp_nation filter IN-KERNEL from
    # per-run scalars. So EXCLUDE the region (r_name) + o_orderdate constants (keep
    # the cmp/column shape) -> r_name in {ASIA,EUROPE,AMERICA,...} and ANY date
    # window collapse to ONE signature, reusing the resident buffers warm. The dim
    # edges (already folded above) + the dim-filter cmp/column shape still key a
    # genuinely different predicate STRUCTURE distinctly.
    if _q5_pred_enabled(d):
        for gi in range(len(d.gets)):
            ref g = d.gets[gi]
            for fi in range(len(g.filters)):
                ref p = g.filters[fi]
                if (p.col.table == "region" and p.col.column == "r_name") or (
                    p.col.table == "orders" and p.col.column == "o_orderdate"
                ):
                    sig += (
                        "|q5f="
                        + p.col.table
                        + "."
                        + p.col.column
                        + ":"
                        + String(Int(p.cmp))
                    )
        sig += "|q5_pred=1"
        return sig
    # Filter CONSTANTS: every GET's predicates (fact AND dim) by table.column,
    # cmp, and the const lo/hi (+ str_val for VARCHAR consts). This is what makes
    # a WARM hit guarantee an identical predicate so the cached layout is valid.
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            ref c = d.consts[p.const_id]
            sig += (
                "|f="
                + p.col.table
                + "."
                + p.col.column
                + ":"
                + String(Int(p.cmp))
                + ":"
                + String(Int(c.lo))
                + ":"
                + String(Int(c.hi))
                + ":"
                + c.str_val
            )
    return sig


@export("mojo_gpu_pin_begin")
def mojo_gpu_pin_begin(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    # 0 = WARM (resident buffers cached, skip feeding), 1 = COLD.
    # WARM iff the signature is already in the process-global resident pin cache
    # (_pin2) — for ALL classes, including FK-joins. A WARM hit means the resident
    # device buffers + the cached per-group/segment result layout are valid as-is
    # (the signature includes the filter constants), so the finalize re-runs the
    # kernel on the resident buffers and assembles from cached metadata, with NO
    # access to fed host columns (C++ feeds nothing on warm).
    if Int(handle) == 0:
        return 1
    try:
        ref d = handle.bitcast[GpuPlanDescriptor]()[]
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            # materialize_sql normally creates it; be defensive.
            var cols = _materialize_columns(d)
            m[key] = GpuExecState(len(cols))
            m[key].mat_cols = cols.copy()

        var sig = _signature(d)
        ref p2 = _pin2_ptr()[]
        # Phase G Stage 2 observability: gated WARM/COLD + signature trace on
        # stderr (GPU_OP_PIN_LOG). Diagnostic only -- does not affect results.
        var pin_log = getenv("GPU_OP_PIN_LOG", "") != ""
        if sig in p2:
            m[key].warm = True
            if pin_log:
                print(
                    "[gpu-op pin] WARM sig=", sig, file=FileDescriptor(2)
                )
            return 0
        m[key].warm = False
        if pin_log:
            print("[gpu-op pin] COLD sig=", sig, file=FileDescriptor(2))
        return 1
    except:
        return 1


# Stash a fed flat column into the exec state. `col_j` indexes the mat_cols
# order chosen in materialize_sql. The element width follows `type_tag`:
# TYPE_DATE/INTEGER -> int32 (4 bytes); everything else (DECIMAL int64-backed /
# BIGINT / HUGEINT-as-2x... ) -> int64 (8 bytes). Copies into an owned buffer
# (the DuckDB pointer is transient).
@export("mojo_gpu_feed_column")
def mojo_gpu_feed_column(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    req_i: Int,
    col_j: Int,
    ptr: UnsafePointer[NoneType, MutAnyOrigin],
    n_rows: Int,
    type_tag: Int64,
) abi("C") -> Int:
    if Int(handle) == 0:
        return 1
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 2
        ref st = m[key]
        # Element width by contract TypeTag: VARCHAR arrives as DuckDB string_t
        # (16-byte struct: 4-byte length + 12 inlined bytes, or 4-byte length +
        # 4-byte prefix + 8-byte pointer); DATE/INTEGER as int32 (4 bytes);
        # everything else (int64-backed DECIMAL / BIGINT) as int64 (8 bytes).
        var elem_size: Int
        if type_tag == TYPE_VARCHAR:
            elem_size = 16
        elif type_tag == TYPE_DATE or type_tag == TYPE_INTEGER:
            elem_size = 4
        else:
            elem_size = 8
        # Request 0 is the fact table; requests 1..n_dims are dim-edge queries.
        if req_i == 0:
            # SKIP-MATERIALIZE: the narrowed SELECT fed fewer fact columns, so
            # `col_j` indexes the EMITTED order. Translate it to the FULL mat_cols
            # index via fed_pos_to_matcol so st.cols stays indexed by mat_cols
            # order (the finalize slot map is unchanged). materialize_sql(0) always
            # populates fed_pos_to_matcol (the identity when nothing is omitted),
            # and C++ feeds exactly the emitted columns, so col_j is always a valid
            # index into it. Fall back to the raw col_j only if the map is empty
            # (a defensively-created state where materialize_sql(0) didn't run).
            var mj = col_j
            if len(st.fed_pos_to_matcol) > 0:
                if col_j < 0 or col_j >= len(st.fed_pos_to_matcol):
                    return 3
                mj = st.fed_pos_to_matcol[col_j]
            if mj < 0 or mj >= len(st.cols):
                return 3
            st.cols[mj].fill(ptr, n_rows, elem_size, type_tag)
            st.n_rows = n_rows
            return 0
        var de = req_i - 1
        if de < 0 or de >= len(st.dim_cols):
            return 3
        if col_j < 0 or col_j >= len(st.dim_cols[de]):
            return 3
        st.dim_cols[de][col_j].fill(ptr, n_rows, elem_size, type_tag)
        st.dim_n_rows[de] = n_rows
        return 0
    except:
        return 4


# SKIP-MATERIALIZE landmine #1: set st.n_rows for the FACT request UNCONDITIONALLY
# from the row count C++ observed. Called for request 0 BEFORE the feed loop with
# `res->RowCount()`. When the narrowed SELECT omits ALL fact columns (everything
# resident), C++ SKIPS the fact Connection::Query entirely (a 0-column SELECT is
# illegal) and this is the ONLY thing that sets st.n_rows -- so the finalize knows
# the row count even with no fed fact column. When at least one column is fed,
# feed_column ALSO sets st.n_rows to the same value (same predicate-independent
# scan), so this is harmless/idempotent. Returns 0 on success.
@export("mojo_gpu_feed_rowcount")
def mojo_gpu_feed_rowcount(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    n_rows: Int,
) abi("C") -> Int:
    if Int(handle) == 0:
        return 1
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 2
        m[key].n_rows = n_rows
        return 0
    except:
        return 3


# SKIP-MATERIALIZE: is the narrow-SQL skip-materialize path active for THIS query?
# C++ calls it BEFORE the feed loop to decide whether to (a) bypass the GPU-direct
# fact feed (it uses the narrowed SQL feed instead) and (b) call feed_rowcount +
# detect a 0-column SELECT. Returns 1 (active) / 0 (not). Same gate as the Mojo
# omit decision so C++ and Mojo agree exactly.
@export("mojo_gpu_skipmat_active")
def mojo_gpu_skipmat_active(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    ref d = handle.bitcast[GpuPlanDescriptor]()[]
    return 1 if _skipmat_active(d) else 0


# Map a fed column index to a typed (immutable-origin) pointer over the owned
# host buffer -- the origin `mojo_q6_pin` expects. Rebuilt from the raw address.
def _col_i32(
    st: GpuExecState, j: Int
) -> UnsafePointer[Int32, ImmutAnyOrigin]:
    return UnsafePointer[Int32, ImmutAnyOrigin](
        unsafe_from_address=st.cols[j].addr()
    )


def _col_i64(
    st: GpuExecState, j: Int
) -> UnsafePointer[Int64, ImmutAnyOrigin]:
    return UnsafePointer[Int64, ImmutAnyOrigin](
        unsafe_from_address=st.cols[j].addr()
    )


# Find the mat_cols index of a column by name (-1 if absent).
def _col_index(st: GpuExecState, name: String) -> Int:
    for j in range(len(st.mat_cols)):
        if st.mat_cols[j] == name:
            return j
    return -1


# Decode the DuckDB string_t at row `i` of fed VARCHAR column `j` into a String.
# Layout (16 bytes): bytes[0:4] = uint32 length (little-endian). If length <= 12
# the bytes follow inline at offset 4; otherwise byte[8:16] is a `char*` pointer
# (offset 4:8 is a copy of the first 4 bytes, the prefix). Matches DuckDB's
# string_t (src/include/duckdb/common/types/string_type.hpp) and how the C++ Q1
# path reads l_returnflag/l_linestatus via string_t::GetData()/GetSize().
def _col_str(st: GpuExecState, j: Int, i: Int) raises -> String:
    var base = st.cols[j].addr()
    if base == 0:
        return String("")
    var p = UnsafePointer[UInt8, ImmutAnyOrigin](
        unsafe_from_address=base + i * 16
    )
    # length: little-endian uint32 in the first 4 bytes.
    var length = (
        Int(p[0])
        | (Int(p[1]) << 8)
        | (Int(p[2]) << 16)
        | (Int(p[3]) << 24)
    )
    if length <= 0:
        return String("")
    var data_ptr: UnsafePointer[UInt8, ImmutAnyOrigin]
    if length <= 12:
        data_ptr = p + 4  # inlined right after the length
    else:
        # bytes 8..15 hold the pointer (native-endian uintptr).
        var addr = 0
        for b in range(8):
            addr |= Int(p[8 + b]) << (8 * b)
        data_ptr = UnsafePointer[UInt8, ImmutAnyOrigin](
            unsafe_from_address=addr
        )
    var s = String("")
    for k in range(length):
        s += chr(Int(data_ptr[k]))
    return s


# Read the Q6 filter constants from the descriptor's fact filters, mapping each
# GpuPredicate by column name + cmp tag to the right kernel arg. l_shipdate
# constants are DATE days (int32, in `lo`); l_discount/l_quantity are scale-2
# int64 (also in `lo`). Mirrors the C++ Q6 matcher exactly.
def _q6_consts(
    d: GpuPlanDescriptor,
) raises -> Tuple[Int32, Int32, Int64, Int64, Int64]:
    var ship_lo = Int32(0)
    var ship_hi = Int32(0)
    var disc_lo = Int64(0)
    var disc_hi = Int64(0)
    var qty_hi = Int64(0)
    var have = 0
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            ref c = d.consts[p.const_id]
            if p.col.column == "l_shipdate":
                if p.cmp == CMP_GE:
                    ship_lo = Int32(c.lo)
                    have += 1
                elif p.cmp == CMP_LT:
                    ship_hi = Int32(c.lo)
                    have += 1
            elif p.col.column == "l_discount":
                if p.cmp == CMP_GE:
                    disc_lo = c.lo
                    have += 1
                elif p.cmp == CMP_LE:
                    disc_hi = c.lo
                    have += 1
            elif p.col.column == "l_quantity":
                if p.cmp == CMP_LT:
                    qty_hi = c.lo
                    have += 1
    if have != 5:
        raise Error("Q6: expected 5 fact filter constants, got " + String(have))
    return (ship_lo, ship_hi, disc_lo, disc_hi, qty_hi)


# Build dim arrays / stage / upload (if COLD) + run kernel + int128 reduce.
# Q6 shape: map fed columns BY NAME to the Q6 kernel inputs and reuse the
# existing mojo_q6_pin (upload) + mojo_q6_query path. Stores the int128 result
# (two limbs) as the single result row/col in the exec state.
# Read the Q1 shipdate cutoff from the fact filters. Q1's single fact filter is
# `l_shipdate <= cutoff` (CMP_LE) -> ship_hi = const days (inclusive, matching
# the q1_kernel's `sd <= ship_hi`). A `< cutoff` (CMP_LT) is normalized to
# `<= cutoff-1`. Returns the days int32.
def _q1_ship_cutoff(d: GpuPlanDescriptor) raises -> Int32:
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.column == "l_shipdate":
                ref c = d.consts[p.const_id]
                if p.cmp == CMP_LE:
                    return Int32(c.lo)
                elif p.cmp == CMP_LT:
                    return Int32(c.lo - 1)
    raise Error("Q1: no l_shipdate cutoff filter found")


# ===-------------------------------------------------------------------===#
# Generic descriptor -> generic-kernel lowering (n_dims == 0).
#
# Replaces the bespoke Q6/Q1 compute in the shuttle's pin_finalize. Given the fed
# fact columns (in mat_cols order) + the descriptor it:
#   1. assigns each distinct *numeric* fact column a slot in a packed int64 buffer
#      cols[slot*n_rows+row] (int32 DATE/INTEGER widened to int64);
#   2. resolves each aggregate program's PUSH_CONST pool-id -> consts[id].lo and
#      LOAD_COL string-id -> fact-column slot, producing (op,a,b) int64 triples;
#   3. computes a HOST 0/1 pass column (AND of the fact range predicates) into an
#      extra slot, and uses a 1-op `LOAD_COL(pass_slot)` pass program -- the VM
#      has no CMP/AND ops, so this is the exact + simplest correct lowering;
#   4. lowers COUNT_STAR -> PUSH_CONST(1), SUM -> the resolved program, AVG -> two
#      internal metrics (sum + count) reduced to a host DOUBLE;
#   5. for 0 group keys runs UNGROUPED (G=1); for group keys reuses the VARCHAR
#      dense-gid assignment (sorted distinct tuple -> gid) as an int64 gid slot;
#   6. calls run_segreduce and assembles the result table in out_types order.
# ===-------------------------------------------------------------------===#

# Read fed numeric column `j` at row `i` as an Int64 (widening int32 DATE/INTEGER).
def _col_val(st: GpuExecState, j: Int, i: Int) -> Int64:
    ref c = st.cols[j]
    var base = c.addr()
    if base == 0:
        return Int64(0)
    if c.elem_size == 4:
        var p = UnsafePointer[Int32, ImmutAnyOrigin](
            unsafe_from_address=base + i * 4
        )
        return Int64(p[])
    var p = UnsafePointer[Int64, ImmutAnyOrigin](
        unsafe_from_address=base + i * 8
    )
    return p[]


# Read dim-request `de`'s fed numeric column `c` at row `i` as Int64 (widening
# int32 DATE/INTEGER). Mirrors _col_val but reads from the dim-column storage.
def _dim_col_val(st: GpuExecState, de: Int, c: Int, i: Int) -> Int64:
    ref col = st.dim_cols[de][c]
    var base = col.addr()
    if base == 0:
        return Int64(0)
    if col.elem_size == 4:
        var p = UnsafePointer[Int32, ImmutAnyOrigin](
            unsafe_from_address=base + i * 4
        )
        return Int64(p[])
    var p = UnsafePointer[Int64, ImmutAnyOrigin](
        unsafe_from_address=base + i * 8
    )
    return p[]


# Decode the DuckDB string_t at row `i` of dim-request `de`'s VARCHAR column `c`.
# Same string_t layout as _col_str (which reads fact columns).
def _dim_col_str(st: GpuExecState, de: Int, c: Int, i: Int) raises -> String:
    var base = st.dim_cols[de][c].addr()
    if base == 0:
        return String("")
    var p = UnsafePointer[UInt8, ImmutAnyOrigin](
        unsafe_from_address=base + i * 16
    )
    var length = (
        Int(p[0]) | (Int(p[1]) << 8) | (Int(p[2]) << 16) | (Int(p[3]) << 24)
    )
    if length <= 0:
        return String("")
    var data_ptr: UnsafePointer[UInt8, ImmutAnyOrigin]
    if length <= 12:
        data_ptr = p + 4
    else:
        var addr = 0
        for b in range(8):
            addr |= Int(p[8 + b]) << (8 * b)
        data_ptr = UnsafePointer[UInt8, ImmutAnyOrigin](
            unsafe_from_address=addr
        )
    var s = String("")
    for k in range(length):
        s += chr(Int(data_ptr[k]))
    return s


# p_type LIKE 'PROMO%' — true iff the first 5 bytes are exactly "PROMO".
# Mirrors the C++ EnsureQ14Pinned promo test exactly.
def _starts_promo(s: String) -> Bool:
    if s.byte_length() < 5:
        return False
    var b = s.as_bytes()
    return (
        b[0] == UInt8(ord("P"))
        and b[1] == UInt8(ord("R"))
        and b[2] == UInt8(ord("O"))
        and b[3] == UInt8(ord("M"))
        and b[4] == UInt8(ord("O"))
    )


# A resolved metric program + bookkeeping for result assembly.
@fieldwise_init
struct MetricPlan(Copyable, Movable):
    var ops: List[Int64]  # flattened (op,a,b) triples
    var n_ops: Int


# Resolve one aggregate's postfix program into VM (op,a,b) int64 triples:
# LOAD_COL (raw string ids) -> fact-column slot index (via the agg's load_cols
# list, walked in program order); PUSH_CONST (const-pool id) -> consts[id].lo.
def _resolve_program(
    d: GpuPlanDescriptor,
    agg: GpuAggregate,
    col_slot: Dict[String, Int],
) raises -> MetricPlan:
    var ops: List[Int64] = []
    var load_i = 0
    for k in range(len(agg.program)):
        ref o = agg.program[k]
        if o.op == OP_LOAD_COL:
            var name = agg.load_cols[load_i].column
            load_i += 1
            if name not in col_slot:
                raise Error("generic: LOAD_COL references unfed column " + name)
            ops.append(OP_LOAD_COL)
            ops.append(Int64(col_slot[name]))
            ops.append(Int64(0))
        elif o.op == OP_PUSH_CONST:
            # o.a is a const-pool id at this layer -> resolve to the literal.
            var cid = Int(o.a)
            if cid < 0 or cid >= len(d.consts):
                raise Error("generic: PUSH_CONST bad const id")
            ops.append(OP_PUSH_CONST)
            ops.append(d.consts[cid].lo)
            ops.append(Int64(0))
        else:
            ops.append(o.op)
            ops.append(o.a)
            ops.append(o.b)
    return MetricPlan(ops^, len(agg.program))


# Symbolically evaluate the decimal scale a metric program produces, given a
# per-(numeric)-column scale map (indexed by fact-column slot). Mirrors the VM:
# LOAD_COL -> the column's scale; PUSH_CONST -> its const scale; ADD/SUB keep the
# scale (operands share it for the supported shapes); MUL adds the two scales.
# Used only to scale AVG's int64 sum back to a real for the DOUBLE result.
def _program_scale(
    agg: GpuAggregate,
    col_scale_of_slot: List[Int64],
    col_slot: Dict[String, Int],
    d: GpuPlanDescriptor,
) raises -> Int64:
    var stack: List[Int64] = []
    var load_i = 0
    for k in range(len(agg.program)):
        ref o = agg.program[k]
        if o.op == OP_LOAD_COL:
            var name = agg.load_cols[load_i].column
            load_i += 1
            stack.append(col_scale_of_slot[col_slot[name]])
        elif o.op == OP_PUSH_CONST:
            stack.append(d.consts[Int(o.a)].scale)
        elif o.op == OP_ADD or o.op == OP_SUB:
            var b = stack.pop()
            _ = stack.pop()
            stack.append(b)
        elif o.op == OP_MUL:
            var b = stack.pop()
            var a = stack.pop()
            stack.append(a + b)
        elif o.op == OP_SELECT:
            var e = stack.pop()
            _ = stack.pop()
            _ = stack.pop()
            stack.append(e)
    return stack[len(stack) - 1] if len(stack) > 0 else Int64(0)


# Apply one fact range predicate to a per-row int64 value -> pass (Bool).
def _pred_pass(v: Int64, cmp: Int64, k: Int64) -> Bool:
    if cmp == CMP_EQ:
        return v == k
    elif cmp == CMP_NE:
        return v != k
    elif cmp == CMP_LT:
        return v < k
    elif cmp == CMP_LE:
        return v <= k
    elif cmp == CMP_GT:
        return v > k
    elif cmp == CMP_GE:
        return v >= k
    return False


# ---------------------------------------------------------------------------
# Phase G Stage 2: Q6 predicate-independent residency spec.
#
# Resolved view of the Q6 WHERE clause as 3 column slots + 5 int64 bounds, so the
# kernel can evaluate the filter in-kernel (over the resident columns) for ANY
# constant set without a host-baked pass column. `eligible` is False unless the
# descriptor's filters are EXACTLY the canonical Q6 shape, in which case the
# caller MUST fall back to the per-constant pass-column path (correctness rule).
#
# Slot indices index `cols[slot * n_rows + row]` (mat_cols / numeric-slot order);
# for Q6 every fact column is numeric so the slot == the _materialize_columns
# index. Bounds are the raw `const.lo` int64s — the exact `k` _pred_pass uses.
@fieldwise_init
struct Q6PredSpec(ImplicitlyCopyable, Movable):
    var eligible: Bool
    var ship_slot: Int
    var disc_slot: Int
    var qty_slot: Int
    var ship_lo: Int64
    var ship_hi: Int64
    var disc_lo: Int64
    var disc_hi: Int64
    var qty_hi: Int64


# Resolve the canonical Q6 predicate from the descriptor. Matches the filter set
# by (column, cmp) ROLE rather than hardcoded names, so it is robust to schema
# renaming while staying strictly the Q6 shape:
#   * one column with BOTH a CMP_GE (-> ship_lo) and a CMP_LT (-> ship_hi) bound
#   * one column with BOTH a CMP_GE (-> disc_lo) and a CMP_LE (-> disc_hi) bound
#   * one column with a single CMP_LT (-> qty_hi) bound
# All filters must be on the fact table, there must be EXACTLY 5 of them, exactly
# these three columns, and no group keys / dims (UNGROUPED scalar). Any deviation
# => eligible == False (the caller stays on the per-constant signature).
def _q6_pred_spec(d: GpuPlanDescriptor) -> Q6PredSpec:
    var bad = Q6PredSpec(False, 0, 0, 0, 0, 0, 0, 0, 0)
    if d.kind != KIND_Q6:
        return bad
    if d.strategy != STRAT_UNGROUPED:
        return bad
    if len(d.group_keys) != 0 or len(d.dim_edges) != 0:
        return bad

    # Collect fact-table filters as (column, cmp, const.lo) triples.
    var fcol: List[String] = []
    var fcmp: List[Int64] = []
    var fk: List[Int64] = []
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            # A non-fact GET with filters means a join/dim shape, not pure Q6.
            for fi in range(len(g.filters)):
                _ = fi
                return bad
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table:
                return bad
            fcol.append(p.col.column)
            fcmp.append(p.cmp)
            fk.append(d.consts[p.const_id].lo)
    if len(fcol) != 5:
        return bad

    # Bucket bounds per column by cmp role. Each column may carry GE/LT/LE bounds.
    var names: List[String] = []
    var has_ge: List[Bool] = []
    var has_lt: List[Bool] = []
    var has_le: List[Bool] = []
    var v_ge: List[Int64] = []
    var v_lt: List[Int64] = []
    var v_le: List[Int64] = []

    def _idx(mut names: List[String], name: String) -> Int:
        for i in range(len(names)):
            if names[i] == name:
                return i
        return -1

    for fi in range(len(fcol)):
        var ci = _idx(names, fcol[fi])
        if ci < 0:
            ci = len(names)
            names.append(fcol[fi])
            has_ge.append(False)
            has_lt.append(False)
            has_le.append(False)
            v_ge.append(Int64(0))
            v_lt.append(Int64(0))
            v_le.append(Int64(0))
        if fcmp[fi] == CMP_GE:
            if has_ge[ci]:
                return bad  # duplicate GE on same column -> not canonical Q6
            has_ge[ci] = True
            v_ge[ci] = fk[fi]
        elif fcmp[fi] == CMP_LT:
            if has_lt[ci]:
                return bad
            has_lt[ci] = True
            v_lt[ci] = fk[fi]
        elif fcmp[fi] == CMP_LE:
            if has_le[ci]:
                return bad
            has_le[ci] = True
            v_le[ci] = fk[fi]
        else:
            return bad  # only GE/LT/LE appear in Q6
    if len(names) != 3:
        return bad

    # Classify the three columns by their bound roles.
    var ship_ci = -1
    var disc_ci = -1
    var qty_ci = -1
    for ci in range(3):
        if has_ge[ci] and has_lt[ci] and not has_le[ci]:
            if ship_ci >= 0:
                return bad
            ship_ci = ci  # [lo, hi) range  -> l_shipdate
        elif has_ge[ci] and has_le[ci] and not has_lt[ci]:
            if disc_ci >= 0:
                return bad
            disc_ci = ci  # [lo, hi] range -> l_discount
        elif has_lt[ci] and not has_ge[ci] and not has_le[ci]:
            if qty_ci >= 0:
                return bad
            qty_ci = ci  # < hi          -> l_quantity
        else:
            return bad
    if ship_ci < 0 or disc_ci < 0 or qty_ci < 0:
        return bad

    # Map column names to numeric slots (== _materialize_columns index for Q6).
    var mat = _materialize_columns(d)
    def _slot(mat: List[String], name: String) -> Int:
        for i in range(len(mat)):
            if mat[i] == name:
                return i
        return -1

    var ship_slot = _slot(mat, names[ship_ci])
    var disc_slot = _slot(mat, names[disc_ci])
    var qty_slot = _slot(mat, names[qty_ci])
    if ship_slot < 0 or disc_slot < 0 or qty_slot < 0:
        return bad

    return Q6PredSpec(
        True,
        ship_slot,
        disc_slot,
        qty_slot,
        v_ge[ship_ci],
        v_lt[ship_ci],
        v_ge[disc_ci],
        v_le[disc_ci],
        v_lt[qty_ci],
    )


# Phase G Stage 2 master switch: the Q6 predicate-independent residency path is
# only taken when the native-decode flag is on (Stage 1 feeds Q6's fact columns
# GPU-direct, the prerequisite) AND the descriptor is the canonical Q6 shape.
def _q6_pred_enabled(d: GpuPlanDescriptor) -> Bool:
    if getenv("GPU_OP_NATIVE_DECODE", "") == "":
        return False
    return _q6_pred_spec(d).eligible


# ---------------------------------------------------------------------------
# Phase G Stage 2 (generalized): Q1 / Q14 predicate-independent residency.
#
# Unlike Q6 (resolved to fixed named role slots), Q1 and Q14 have a SIMPLE fact
# range filter that the generic finalize already collects as a list of fact
# filters in descriptor order. The constant-INDEPENDENT part (which resident
# column slot + which cmp) is cached; the constant-DEPENDENT bounds are threaded
# fresh per run. So the "spec" splits into:
#   * `_gen_pred_eligible(d)` -- a structural gate (kind/strategy + the exact
#     canonical fact-filter shape) used by `_signature` and the finalize. ANY
#     deviation -> False -> the caller stays on the per-constant pass-column path
#     (still correct, just not warm-across-constants).
#   * `_gen_pred_bounds(d)` -- the per-run bounds (one int64 per fact filter, in
#     the SAME descriptor order the finalize collects f_slot/f_cmp), threaded into
#     _assemble. The slots+cmps are cached on the COLD path (where col_slot is
#     available) into gp.gen_pred_slots / gp.gen_pred_cmps.
#
# Canonical shapes (must match EXACTLY or eligible=False):
#   Q1 (KIND_Q1, DENSE_GROUP): every fact filter is on `l_shipdate` (one column),
#     all CMP_LE/CMP_LT/CMP_GE/CMP_GT (range, no EQ/NE). TPC-H Q1 is a single
#     `l_shipdate <= cutoff`; we accept any all-range single-column shipdate filter
#     set since the in-kernel eval is the exact host _pred_pass per filter.
#   Q14 (KIND_Q14, UNGROUPED + dims): every fact filter is on `l_shipdate`, all
#     range (Q14 is `l_shipdate >= lo AND l_shipdate < hi`). The promo dim gather
#     stays in the metric programs (constant-independent), untouched.
# Both require: >=1 fact filter, ALL fact filters range-only, all on ONE fact
# column, and NO non-fact-table filters that are range predicates folded into the
# fact pass (Q14's dim promo flag is a dim ARRAY, not a fact filter, so it does
# not appear in the fact-filter list -- correctly excluded here).
def _gen_pred_eligible(d: GpuPlanDescriptor) -> Bool:
    if d.kind == KIND_Q1:
        if d.strategy != STRAT_DENSE_GROUP:
            return False
    elif d.kind == KIND_Q14:
        if d.strategy != STRAT_UNGROUPED:
            return False
    else:
        return False
    # Collect fact-table filters; require >=1, all range, all on one fact column.
    # CORRECTNESS: any NON-fact-table filter carries a constant we would otherwise
    # drop from the signature (a dim filter feeds a dim pass-flag ARRAY, which IS
    # constant-dependent), so its presence makes the constant-decoupling unsound ->
    # eligible=False (stay per-constant). Canonical Q1/Q14 have zero dim filters
    # (Q14's promo is an aggregate CASE, not a filter), so this only excludes
    # genuinely const-coupled shapes.
    var n_fact = 0
    var the_col = String("")
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            if len(g.filters) > 0:
                return False  # a dim filter is constant-coupled -> not eligible
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table:
                return False
            # only true range comparisons (the in-kernel filter mirrors these).
            if not (
                p.cmp == CMP_LT
                or p.cmp == CMP_LE
                or p.cmp == CMP_GT
                or p.cmp == CMP_GE
            ):
                return False
            if the_col == "":
                the_col = p.col.column
            elif p.col.column != the_col:
                return False
            n_fact += 1
    if n_fact < 1:
        return False
    return True


# Master switch: the generalized Q1/Q14 predicate-independent path is only taken
# when the native-decode flag is on AND the descriptor is a canonical Q1/Q14 shape.
def _gen_pred_enabled(d: GpuPlanDescriptor) -> Bool:
    if getenv("GPU_OP_NATIVE_DECODE", "") == "":
        return False
    return _gen_pred_eligible(d)


# Per-run bounds: one int64 per fact filter, in the SAME order the finalize
# collects them (descriptor order over fact-table filters). These are the exact
# `d.consts[p.const_id].lo` ints the host pass-bake compares against (so the
# in-kernel filter is bit-identical). Returns an empty list if not eligible.
def _gen_pred_bounds(d: GpuPlanDescriptor) -> List[Int64]:
    var bounds: List[Int64] = []
    if not _gen_pred_eligible(d):
        return bounds^
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            bounds.append(d.consts[p.const_id].lo)
    return bounds^


# ---------------------------------------------------------------------------
# Phase G Stage 2 (Path B): Q5 predicate-independent residency.
#
# Mirrors the Q6/Q1/Q14 mechanism for the Q5 shape: the resident dim arrays + gid
# are the CONSTANT-INDEPENDENT raw forms, and the region/date filter is evaluated
# IN-KERNEL from per-run scalars, so DIFFERENT region/date constants reuse the same
# residency (warm-across-constants).
#
#   * `_q5_pred_eligible(d)` -- a PURE structural gate: kind==Q5, DENSE_GROUP, the
#     canonical 5 dims (orders/customer/supplier/nation/region), and the ONLY
#     constant-bearing filters are region.r_name (CMP_EQ VARCHAR) + orders.
#     o_orderdate (a GE/GT lo bound AND a LT/LE hi bound). No other dim/fact
#     filters. ANY deviation -> False -> the caller stays on the per-constant
#     |f= signature + the existing per-constant Q5 residency (still correct, just
#     not warm-across-constants).
#   * `_q5_pred_enabled(d)` -- the master switch: flag on AND _q5_pred_eligible.
#   * `_q5_pred_params(d, names, keys)` -- the per-run scalars threaded into
#     _assemble: (o_lo, o_hi, asia_region). o_lo/o_hi from the o_orderdate filters
#     (GE->lo, GT->lo+1, LT->hi, LE->hi+1 -- exactly _pin_finalize_q5 today);
#     asia_region from the region_name->regionkey map (ALL 5 regions, cached on
#     cold; itself region-independent) so WARM is self-contained with no region dim.
def _q5_pred_eligible(d: GpuPlanDescriptor) -> Bool:
    if d.kind != KIND_Q5:
        return False
    if d.strategy != STRAT_DENSE_GROUP:
        return False
    # Exactly the canonical 5 dims (orders/customer/supplier/nation/region).
    if len(d.dim_edges) != 5:
        return False
    if (
        _de_of_table(d, "orders") < 0
        or _de_of_table(d, "customer") < 0
        or _de_of_table(d, "supplier") < 0
        or _de_of_table(d, "nation") < 0
        or _de_of_table(d, "region") < 0
    ):
        return False
    # The ONLY constant-bearing filters allowed: region.r_name (CMP_EQ VARCHAR) and
    # orders.o_orderdate (GE/GT lo + LT/LE hi). Anything else -> not eligible.
    var have_region = False
    var have_o_lo = False
    var have_o_hi = False
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table == "region" and p.col.column == "r_name":
                if p.cmp != CMP_EQ:
                    return False
                if d.consts[p.const_id].type_tag != TYPE_VARCHAR:
                    return False
                if have_region:
                    return False  # duplicate region filter -> not canonical
                have_region = True
            elif p.col.table == "orders" and p.col.column == "o_orderdate":
                if p.cmp == CMP_GE or p.cmp == CMP_GT:
                    if have_o_lo:
                        return False
                    have_o_lo = True
                elif p.cmp == CMP_LT or p.cmp == CMP_LE:
                    if have_o_hi:
                        return False
                    have_o_hi = True
                else:
                    return False
            else:
                # Any other filter (fact or dim) carries a constant we would drop
                # from the signature -> the decoupling would be unsound.
                return False
    if not have_region or not have_o_lo or not have_o_hi:
        return False
    return True


def _q5_pred_enabled(d: GpuPlanDescriptor) -> Bool:
    if getenv("GPU_OP_NATIVE_DECODE", "") == "":
        return False
    return _q5_pred_eligible(d)


# Per-run scalars [o_lo, o_hi, asia_region] for the in-kernel Q5 predicate. The
# region->regionkey map (ALL 5 regions) is passed in (cached on cold), so WARM has
# no region dim fed yet resolves THIS query's region const to its key. o_lo/o_hi
# match _pin_finalize_q5 exactly (GE->lo, GT->lo+1, LT->hi, LE->hi+1). Returns an
# empty list if not eligible or the region const is unknown (the caller then must
# fall back; but eligibility already guarantees the filter shape).
def _q5_pred_params(
    d: GpuPlanDescriptor,
    region_names: List[String],
    region_keys: List[Int64],
) -> List[Int64]:
    var out: List[Int64] = []
    if not _q5_pred_eligible(d):
        return out^
    var o_lo = Int64(0)
    var o_hi = Int64(0)
    var region_name = String("")
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table == "orders" and p.col.column == "o_orderdate":
                ref c = d.consts[p.const_id]
                if p.cmp == CMP_GE:
                    o_lo = c.lo
                elif p.cmp == CMP_GT:
                    o_lo = c.lo + 1
                elif p.cmp == CMP_LT:
                    o_hi = c.lo
                elif p.cmp == CMP_LE:
                    o_hi = c.lo + 1
            elif p.col.table == "region" and p.col.column == "r_name":
                region_name = d.consts[p.const_id].str_val
    var asia_region = Int64(-1)
    for i in range(len(region_names)):
        if region_names[i] == region_name:
            asia_region = region_keys[i]
    out.append(o_lo)
    out.append(o_hi)
    out.append(asia_region)
    return out^


# The fully generic finalize for n_dims == 0 (UNGROUPED + DENSE_GROUP).
def _pin_finalize_generic(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) raises -> Int:
    ref d = handle.bitcast[GpuPlanDescriptor]()[]
    ref m = _exec_ptr()[]
    var key = Int(handle)
    if key not in m:
        return 2

    # WARM: the resident buffers + assembly metadata are cached under `sig`
    # (pin_begin returned 0, so C++ fed nothing). Re-run the kernel on the
    # resident buffers and assemble from the cached metadata alone. For the Q6
    # predicate-independent path the CURRENT query's filter bounds are extracted
    # from the live descriptor `d` and threaded into the re-run (the resident
    # buffers are constant-independent; the kernel applies THIS query's predicate).
    var sig = _signature(d)
    ref p2 = _pin2_ptr()[]
    if sig in p2:
        ref dst = m[key]
        var q6b = _q6_pred_spec(d)
        # Generalized Q1 (DENSE_GROUP): thread THIS query's fact-filter bounds.
        var genb = _gen_pred_bounds(d)
        _assemble(dst, p2[sig], q6b, genb^)
        return 0

    # COLD: build the host inputs + programs + assembly metadata, upload the
    # resident buffers once, construct + cache a GpuPinned, then assemble.
    ref st = m[key]
    var n = st.n_rows

    # --- slot map: every fed NUMERIC fact column (mat_cols order) -> slot ---
    # VARCHAR columns (group keys) are not packed as data; they only drive gids.
    var col_slot = Dict[String, Int]()
    var slot_of_matcol: List[Int] = []  # mat_cols idx -> slot (or -1 for varchar)
    var numeric_matcols: List[Int] = []  # slot -> mat_cols idx
    for j in range(len(st.mat_cols)):
        if st.cols[j].type_tag == TYPE_VARCHAR:
            slot_of_matcol.append(-1)
        else:
            var slot = len(numeric_matcols)
            col_slot[st.mat_cols[j]] = slot
            slot_of_matcol.append(slot)
            numeric_matcols.append(j)
    var n_numeric = len(numeric_matcols)

    # SKIP-MATERIALIZE: per-numeric-slot omit mask. omit_slot[slot] True => the
    # column was NOT scanned/fed (sourced from the pool); never read st.cols[mj]
    # for it. Empty/all-False when skip-materialize is inactive (== Phase 1).
    var omit_slot = _omit_slot_mask(st, numeric_matcols)

    # Per-numeric-slot decimal scale, used only for AVG's DOUBLE rescale. Seed
    # from the fed type (DATE/INTEGER/BIGINT -> 0); refine decimal-backed int64
    # columns from any fact filter const that compares against them, else 2 (the
    # TPC-H lineitem decimal scale -- l_quantity/extendedprice/discount/tax).
    # SKIP-MATERIALIZE landmine #2: for an OMITTED slot st.cols[mj].type_tag is
    # UNSET (0) -> would wrongly seed scale 2 (e.g. for a DATE) -> wrong AVG. Read
    # the type_tag the column was POOLED with instead (col_pool_type_tag on the
    # exact resident key), recovering the correct DATE/INTEGER/BIGINT->0 vs ->2.
    var col_scale_of_slot: List[Int64] = []
    for slot in range(n_numeric):
        var tt: Int64
        if slot < len(omit_slot) and omit_slot[slot]:
            var ok2 = col_key(
                d.fact_table, st.mat_cols[numeric_matcols[slot]],
                REPR_INT64_PACKED, ORDERING_STORAGE, st.n_rows,
            )
            tt = col_pool_type_tag(ok2)
        else:
            tt = st.cols[numeric_matcols[slot]].type_tag
        if tt == TYPE_DATE or tt == TYPE_INTEGER or tt == TYPE_BIGINT:
            col_scale_of_slot.append(Int64(0))
        else:
            col_scale_of_slot.append(Int64(2))
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table or p.col.column not in col_slot:
                continue
            ref c = d.consts[p.const_id]
            if c.type_tag == TYPE_DECIMAL:
                col_scale_of_slot[col_slot[p.col.column]] = c.scale

    # --- group: 0 keys -> UNGROUPED; else dense-gid from VARCHAR group keys ---
    var n_groups = 1
    var gid_slot = -1
    var G = 1
    var mode = STRAT_UNGROUPED
    # dense-gid bookkeeping (sorted distinct tuple -> gid)
    var order: List[Int] = []
    var gkey_vals: List[List[String]] = []  # per group-key col, per distinct-idx
    var row_gid = alloc[Int64](n if n > 0 else 1)
    if len(d.group_keys) > 0:
        mode = STRAT_DENSE_GROUP
        var n_keys = len(d.group_keys)
        # mat_cols index of each group-key column.
        var gk_j: List[Int] = []
        for gk in range(n_keys):
            var gj = _col_index(st, d.group_keys[gk].column)
            if gj < 0:
                row_gid.free()
                return 3
            gk_j.append(gj)
        var tuple_keys: List[String] = []
        for _ in range(n_keys):
            gkey_vals.append(List[String]())
        var seen = Dict[String, Int]()
        var row_didx = alloc[Int32](n if n > 0 else 1)
        for i in range(n):
            var tk = String("")
            var parts: List[String] = []
            for gk in range(n_keys):
                var sv = _col_str(st, gk_j[gk], i)
                parts.append(sv)
                if gk > 0:
                    tk += String("\x01")
                tk += sv
            if tk in seen:
                row_didx[i] = Int32(seen[tk])
            else:
                var idx = len(tuple_keys)
                seen[tk] = idx
                tuple_keys.append(tk)
                for gk in range(n_keys):
                    gkey_vals[gk].append(parts[gk])
                row_didx[i] = Int32(idx)
        n_groups = len(tuple_keys)
        if n_groups <= 0:
            row_didx.free()
            row_gid.free()
            return 7
        for g in range(n_groups):
            order.append(g)
        for a in range(n_groups):
            for b in range(a + 1, n_groups):
                if tuple_keys[order[b]] < tuple_keys[order[a]]:
                    var t = order[a]
                    order[a] = order[b]
                    order[b] = t
        var didx_to_gid = alloc[Int32](n_groups)
        for g in range(n_groups):
            didx_to_gid[order[g]] = Int32(g)
        for i in range(n):
            row_gid[i] = Int64(Int(didx_to_gid[Int(row_didx[i])]))
        didx_to_gid.free()
        row_didx.free()
        G = n_groups
        gid_slot = n_numeric  # gid is the slot right after the numeric cols

    # Phase G Stage 2: is the Q6 predicate-independent path active for this entry?
    # (flag on + canonical Q6 shape). When True we DO NOT pre-bake a host pass
    # column; the kernel evaluates the filter in-kernel from the resident filter-
    # input columns + per-run bounds. The pass_slot column still exists in the
    # packed buffer (kept zero) so the layout / slot indices are unchanged.
    var q6_spec = _q6_pred_spec(d)
    var q6_pred_on = (
        getenv("GPU_OP_NATIVE_DECODE", "") != "" and q6_spec.eligible
    )
    # Generalized Q1 (DENSE_GROUP) predicate-independent path (flag on + canonical
    # Q1 shape). Same idea as q6_pred_on: skip the host pass bake; the kernel
    # evaluates the fact filter in-kernel from the resident column slots + per-run
    # bounds. f_slot/f_cmp collected below (descriptor order) are the cached
    # constant-independent slots+cmps; the bounds are threaded per run.
    var gen_pred_on = _gen_pred_enabled(d) and d.kind == KIND_Q1

    # --- host pass column: AND of the fact range predicates (one int64/row) ---
    # The VM has no CMP/AND ops, so we compute the pass column on the host and
    # feed it via a 1-op `LOAD_COL(pass_slot)` pass program (exact + simplest).
    var pass_slot = n_numeric + (1 if gid_slot >= 0 else 0)
    var pass_col = alloc[Int64](n if n > 0 else 1)
    # Collect fact filters as (slot, cmp, const-lo) -- skip non-fact filters.
    var f_slot: List[Int] = []
    var f_cmp: List[Int64] = []
    var f_k: List[Int64] = []
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table:
                continue
            if p.col.column not in col_slot:
                pass_col.free()
                row_gid.free()
                return 8
            f_slot.append(col_slot[p.col.column])
            f_cmp.append(p.cmp)
            f_k.append(d.consts[p.const_id].lo)
    var n_filters = len(f_slot)
    if q6_pred_on or gen_pred_on:
        # No host pass bake: zero the slot (the in-kernel predicate gates rows).
        for i in range(n):
            pass_col[i] = Int64(0)
    else:
        # SKIP-MATERIALIZE airtight guard: the host pass-bake reads fact filter
        # columns. Omitting a filter column is ONLY safe when the in-kernel
        # predicate replaces this bake (q6_pred_on / gen_pred_on) -- guaranteed by
        # _skipmat_active. If we are HERE (host bake) yet a filter slot is omitted,
        # that is a contract violation -> fail closed (CPU fallback), never read an
        # unfilled st.cols[mj].
        for fi in range(n_filters):
            var fs = f_slot[fi]
            if fs < len(omit_slot) and omit_slot[fs]:
                pass_col.free()
                row_gid.free()
                return 8
        for i in range(n):
            var ok = True
            for fi in range(n_filters):
                var v = _col_val(st, numeric_matcols[f_slot[fi]], i)
                if not _pred_pass(v, f_cmp[fi], f_k[fi]):
                    ok = False
                    break
            pass_col[i] = Int64(1) if ok else Int64(0)

    # --- build the packed columns buffer cols[slot*n_rows+row] ---
    var n_slots = pass_slot + 1
    var cols = alloc[Int64](n_slots * n if n_slots * n > 0 else 1)
    for slot in range(n_numeric):
        # SKIP-MATERIALIZE: an OMITTED slot is sourced from the pool by
        # _colpool_assemble_cols_d (D2D); its st.cols[mj] is UNFILLED, so DO NOT
        # pack it here (leave the host region untouched -- it is never read).
        if slot < len(omit_slot) and omit_slot[slot]:
            continue
        var mj = numeric_matcols[slot]
        for i in range(n):
            cols[slot * n + i] = _col_val(st, mj, i)
    if gid_slot >= 0:
        for i in range(n):
            cols[gid_slot * n + i] = row_gid[i]
    for i in range(n):
        cols[pass_slot * n + i] = pass_col[i]

    # --- metrics: per output aggregate, lower to internal metric program(s) ---
    # AGG_COUNT_STAR -> PUSH_CONST(1); AGG_SUM -> resolved program;
    # AGG_AVG -> two internal metrics (sum, count) -> host DOUBLE ratio.
    # Track, per output aggregate, its metric indices + kind for assembly.
    var agg_kind: List[Int64] = []
    var agg_scale: List[Int64] = []  # for AVG: the summed value's decimal scale
    var agg_m0: List[Int] = []  # primary metric idx (sum/count)
    var agg_m1: List[Int] = []  # secondary (count for AVG), else -1
    var metric_ops: List[Int64] = []  # concatenated triples
    var metric_offsets: List[Int64] = []  # op-offset per metric
    var metric_lens: List[Int64] = []  # op-count per metric
    var n_ops_total = 0

    def _emit_count(
        mut metric_ops: List[Int64],
        mut metric_offsets: List[Int64],
        mut metric_lens: List[Int64],
        mut n_ops_total: Int,
    ) -> Int:
        var idx = len(metric_offsets)
        metric_offsets.append(Int64(n_ops_total))
        metric_ops.append(OP_PUSH_CONST)
        metric_ops.append(Int64(1))
        metric_ops.append(Int64(0))
        metric_lens.append(Int64(1))
        n_ops_total += 1
        return idx

    def _emit_prog(
        plan: MetricPlan,
        mut metric_ops: List[Int64],
        mut metric_offsets: List[Int64],
        mut metric_lens: List[Int64],
        mut n_ops_total: Int,
    ) -> Int:
        var idx = len(metric_offsets)
        metric_offsets.append(Int64(n_ops_total))
        for x in range(len(plan.ops)):
            metric_ops.append(plan.ops[x])
        metric_lens.append(Int64(plan.n_ops))
        n_ops_total += plan.n_ops
        return idx

    for ai in range(len(d.aggregates)):
        ref agg = d.aggregates[ai]
        agg_kind.append(agg.kind)
        if agg.kind == AGG_COUNT_STAR:
            agg_scale.append(Int64(0))
            var mi = _emit_count(
                metric_ops, metric_offsets, metric_lens, n_ops_total
            )
            agg_m0.append(mi)
            agg_m1.append(-1)
        elif agg.kind == AGG_AVG:
            # AVG's DOUBLE rescale uses the summed value's scale, NOT ret_scale
            # (which is 0 for the DOUBLE output column).
            agg_scale.append(
                _program_scale(agg, col_scale_of_slot, col_slot, d)
            )
            var plan = _resolve_program(d, agg, col_slot)
            var mi = _emit_prog(
                plan, metric_ops, metric_offsets, metric_lens, n_ops_total
            )
            var ci = _emit_count(
                metric_ops, metric_offsets, metric_lens, n_ops_total
            )
            agg_m0.append(mi)
            agg_m1.append(ci)
        else:  # AGG_SUM (MIN/MAX not in the n_dims==0 classes here)
            agg_scale.append(agg.ret_scale)
            var plan = _resolve_program(d, agg, col_slot)
            var mi = _emit_prog(
                plan, metric_ops, metric_offsets, metric_lens, n_ops_total
            )
            agg_m0.append(mi)
            agg_m1.append(-1)

    var M = len(metric_offsets)

    # --- pass program: 1-op LOAD_COL(pass_slot) ---
    # Phase G Stage 2: the Q6 predicate-independent kernel evaluates the filter
    # in-kernel from per-run bounds, so there is NO pass program (pass_len 0).
    var pass_prog: List[Int64] = [OP_LOAD_COL, Int64(pass_slot), Int64(0)]
    var pass_len_eff = 1
    if q6_pred_on or gen_pred_on:
        pass_prog = []
        pass_len_eff = 0

    # --- per-candidate group-key cells (constant for the cache entry) ---
    # UNGROUPED: 1 candidate, no keys. DENSE_GROUP: n_groups candidates, the
    # group-key strings ordered by gid (g -> order[g] -> didx -> gkey_vals).
    var n_keys = len(d.group_keys)
    var gk_is_str: List[Bool] = []
    for _ in range(n_keys):
        gk_is_str.append(True)  # n_dims==0 group keys are VARCHAR
    var gk_str_vals: List[List[String]] = []
    var gk_i64_vals: List[List[Int64]] = []
    for g in range(n_groups):
        var sv: List[String] = []
        var iv: List[Int64] = []
        if n_keys > 0:
            var didx = order[g]
            for gk in range(n_keys):
                sv.append(gkey_vals[gk][didx])
                iv.append(Int64(0))
        gk_str_vals.append(sv^)
        gk_i64_vals.append(iv^)

    # --- upload the resident buffers once (no FK-join dims for this class) ---
    # This class is UNGROUPED / DENSE_GROUP => STORAGE row order (no ORDER BY),
    # so it is column-pool eligible. When GPU_OP_COLPOOL is on we dedup the per-
    # column H2D via the pool and assemble cols_d by D2D repack; off => verbatim.
    var ctx = shared_device_context()
    var seg_off_dummy = alloc[Int64](1)
    seg_off_dummy[0] = 0
    var dims_dummy = alloc[Int64](1)
    dims_dummy[0] = 0
    var doff_dummy = alloc[Int64](1)
    doff_dummy[0] = 0
    var pool_lease_keys: List[String] = []
    var resident: SegResident
    if _colptr_eligible(d):
        # Phase 3: build a per-column POINTER TABLE; pooled slots read directly
        # from the pool (no packed copy -> the VRAM win). Same lease/omit handling
        # + skip-mat backstop as the packed pool path below.
        var assemble_fail = False
        var derived_bufs: List[DeviceBuffer[DType.int64]] = []
        var col_ptrs_d = _colpool_assemble_col_ptrs(
            ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
            omit_slot, pool_lease_keys, derived_bufs, assemble_fail,
        )
        if assemble_fail:
            for li in range(len(pool_lease_keys)):
                release_lease(pool_lease_keys[li])
            for li in range(len(st.colpool_omit_leases)):
                release_lease(st.colpool_omit_leases[li])
            st.colpool_omit_leases = []
            seg_off_dummy.free()
            dims_dummy.free()
            doff_dummy.free()
            cols.free()
            pass_col.free()
            row_gid.free()
            return 11
        for li in range(len(st.colpool_omit_leases)):
            pool_lease_keys.append(st.colpool_omit_leases[li])
        st.colpool_omit_leases = []
        resident = segreduce_upload_from_colptr(
            ctx, col_ptrs_d^, derived_bufs^, n_slots, n,
            seg_off_dummy, 0, dims_dummy, doff_dummy, 0,
        )
    elif _colpool_on():
        var assemble_fail = False
        var cols_d = _colpool_assemble_cols_d(
            ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
            omit_slot, pool_lease_keys, assemble_fail,
        )
        if assemble_fail:
            # SKIP-MATERIALIZE backstop (landmine #4): an OMITTED column was not a
            # guaranteed pool HIT. It was never scanned/fed, so we cannot rebuild
            # it -> bail to CPU (nonzero rc -> C++ throws). Release all leases (the
            # non-omitted ensure-leases in pool_lease_keys + the materialize-time
            # omit leases) so they don't pin VRAM, then clear the omit leases.
            for li in range(len(pool_lease_keys)):
                release_lease(pool_lease_keys[li])
            for li in range(len(st.colpool_omit_leases)):
                release_lease(st.colpool_omit_leases[li])
            st.colpool_omit_leases = []
            seg_off_dummy.free()
            dims_dummy.free()
            doff_dummy.free()
            cols.free()
            pass_col.free()
            row_gid.free()
            return 11
        # SUCCESS: transfer the materialize-time omit leases into the per-signature
        # lease set so the GpuPinned owns them (released when it is evicted). Clear
        # the exec-state copy (ownership moved -> no double release).
        for li in range(len(st.colpool_omit_leases)):
            pool_lease_keys.append(st.colpool_omit_leases[li])
        st.colpool_omit_leases = []
        resident = segreduce_upload_from_packed(
            ctx, cols_d^, n_slots, n, seg_off_dummy, 0, dims_dummy, doff_dummy, 0
        )
    else:
        resident = segreduce_upload(
            ctx, cols, n_slots, n, seg_off_dummy, 0, dims_dummy, doff_dummy, 0
        )
    seg_off_dummy.free()
    dims_dummy.free()
    doff_dummy.free()
    cols.free()
    pass_col.free()
    row_gid.free()

    # --- construct + cache the GpuPinned, then assemble via the shared path ---
    var gp = GpuPinned(resident^)
    gp.pool_lease_keys = pool_lease_keys^
    gp.mode = mode
    gp.G = G
    gp.gid_slot = gid_slot if gid_slot >= 0 else 0
    gp.M = M
    gp.pass_prog = pass_prog^
    gp.pass_len = pass_len_eff
    gp.metric_ops = metric_ops^
    gp.n_ops_total = n_ops_total
    gp.metric_offsets = metric_offsets^
    gp.metric_lens = metric_lens^
    gp.n_seg = 0
    gp.n_cols = len(d.out_types)
    gp.n_keys = n_keys
    gp.agg_kind = agg_kind^
    gp.agg_scale = agg_scale^
    gp.agg_m0 = agg_m0^
    gp.agg_m1 = agg_m1^
    gp.gk_is_str = gk_is_str^
    gp.gk_str_vals = gk_str_vals^
    gp.gk_i64_vals = gk_i64_vals^
    gp.n_cand = n_groups
    gp.emit_agg = -1
    gp.emit_gt0 = False
    gp.kind = d.kind  # routes Q1/Q6 to the comptime-specialized kernels
    # Phase G Stage 2: record the constant-independent Q6 predicate slots so the
    # WARM path can re-run the in-kernel filter with a fresh constant set.
    gp.q6_pred = q6_pred_on
    if q6_pred_on:
        gp.q6_pred_slots = [
            q6_spec.ship_slot, q6_spec.disc_slot, q6_spec.qty_slot
        ]
    # Generalized Q1: cache the constant-independent fact-filter slots + cmps (in
    # descriptor order, == f_slot/f_cmp here). The bounds are threaded per run.
    gp.gen_pred = gen_pred_on
    if gen_pred_on:
        var gslots: List[Int] = []
        var gcmps: List[Int64] = []
        for fi in range(n_filters):
            gslots.append(f_slot[fi])
            gcmps.append(f_cmp[fi])
        gp.gen_pred_slots = gslots^
        gp.gen_pred_cmps = gcmps^
    var _fp = _gp_footprint_bytes(gp) if _colpool_on() else 0
    p2[sig] = gp^
    _pin2_track(sig, _fp)

    ref dst = m[key]
    # The COLD path assembles with THIS query's bounds too (the kernel is the same
    # code as WARM; q6_spec / gen-bounds carry the first constant set).
    var gen_b0 = _gen_pred_bounds(d)
    _assemble(dst, p2[sig], q6_spec, gen_b0^)
    return 0



# ===-------------------------------------------------------------------===#
# Generic FK-join finalize (n_dims > 0). UNGROUPED only for now (Q14); the dim
# handling here is exactly what Q3 (segreduce + dim-carried group keys) and Q5
# (dense group on dim-carried n_name, 5 dims) will reuse.
#
# Builds, for each dim_edge, one or more DENSE int64 arrays indexed by the dim PK
# value (sized to max key + 1). The payload is the dim-derived value a program
# needs:
#   * a PROMO flag (p_type LIKE 'PROMO%')         -> OP_PROMO_PRED resolves to it
#   * a carried dim column (e.g. o_orderdate)      -> dim-side OP_LOAD_COL "
#   * a per-dim 0/1 pass flag (dim filters)        -> ANDed into the row pass
# All dim arrays are concatenated into one buffer + dim_offsets, exactly the
# shape run_segreduce / eval_program's OP_LOAD_DIM expect. The fact's FK column
# (e.g. l_partkey) is gathered on-GPU as the dense index.
# ===-------------------------------------------------------------------===#

# A single dense dim array to build: which dim_edge, which dim-request source
# column index (into dim_mat_cols/dim_cols), and a "kind":
#   0 = promo flag (p_type starts 'PROMO')   1 = carried numeric (widened int64)
struct DimArraySpec(Copyable, Movable):
    var de: Int  # dim_edge index
    var src_col: Int  # column index within dim request `de+1`
    var kind: Int  # 0 promo, 1 carried-numeric
    var fk_slot: Int  # fact-column slot of this dim's fact_key (the gather index)

    def __init__(out self, de: Int, src_col: Int, kind: Int, fk_slot: Int):
        self.de = de
        self.src_col = src_col
        self.kind = kind
        self.fk_slot = fk_slot


# Column index of `col` within dim request `de+1` (dim_mat_cols[de]).
def _dim_src_col(st: GpuExecState, de: Int, col: String) raises -> Int:
    ref names = st.dim_mat_cols[de]
    for ci in range(len(names)):
        if names[ci] == col:
            return ci
    raise Error("generic-dims: dim column not materialized: " + col)


# Find the dim_edge index whose dim_table == table (-1 if none / it's the fact).
def _de_of_table(d: GpuPlanDescriptor, table: String) -> Int:
    for de in range(len(d.dim_edges)):
        if d.dim_edges[de].dim_table == table:
            return de
    return -1


# Get-or-create the dense dim array keyed by `dkey`; returns its index.
def _get_or_make_dim(
    mut dim_index: Dict[String, Int],
    mut dim_specs: List[DimArraySpec],
    fk_slot_of_de: List[Int],
    dkey: String,
    de: Int,
    src_col: Int,
    kind: Int,
) raises -> Int:
    if dkey in dim_index:
        return dim_index[dkey]
    var idx = len(dim_specs)
    dim_specs.append(DimArraySpec(de, src_col, kind, fk_slot_of_de[de]))
    dim_index[dkey] = idx
    return idx


# Resolve one aggregate program into VM (op,a,b) triples, turning dim refs into
# OP_LOAD_DIM gathers. Mirrors _resolve_program but dim-aware:
#   * fact-side OP_LOAD_COL -> OP_LOAD_COL(fact slot)
#   * dim-side OP_LOAD_COL  -> OP_LOAD_DIM(carried-numeric array, fk slot)
#   * OP_PROMO_PRED         -> OP_LOAD_DIM(promo-flag array, fk slot)
def _resolve_dim_program(
    d: GpuPlanDescriptor,
    agg: GpuAggregate,
    st: GpuExecState,
    col_slot: Dict[String, Int],
    fk_slot_of_de: List[Int],
    mut dim_index: Dict[String, Int],
    mut dim_specs: List[DimArraySpec],
) raises -> MetricPlan:
    var ops: List[Int64] = []
    var load_i = 0
    var promo_i = 0
    for k in range(len(agg.program)):
        ref o = agg.program[k]
        if o.op == OP_LOAD_COL:
            ref cref = agg.load_cols[load_i]
            load_i += 1
            if cref.table == d.fact_table:
                if cref.column not in col_slot:
                    raise Error(
                        "generic-dims: LOAD_COL unfed fact col " + cref.column
                    )
                ops.append(OP_LOAD_COL)
                ops.append(Int64(col_slot[cref.column]))
                ops.append(Int64(0))
            else:
                var de = _de_of_table(d, cref.table)
                if de < 0:
                    raise Error(
                        "generic-dims: LOAD_COL on unknown dim " + cref.table
                    )
                var sc = _dim_src_col(st, de, cref.column)
                var ai = _get_or_make_dim(
                    dim_index,
                    dim_specs,
                    fk_slot_of_de,
                    String(de) + ":col:" + cref.column,
                    de,
                    sc,
                    1,
                )
                ops.append(OP_LOAD_DIM)
                ops.append(Int64(ai))
                ops.append(Int64(fk_slot_of_de[de]))
        elif o.op == OP_PROMO_PRED:
            ref pref = agg.promo_cols[promo_i]
            promo_i += 1
            var de = _de_of_table(d, pref.table)
            if de < 0:
                raise Error(
                    "generic-dims: PROMO_PRED on unknown dim " + pref.table
                )
            var sc = _dim_src_col(st, de, pref.column)
            var ai = _get_or_make_dim(
                dim_index,
                dim_specs,
                fk_slot_of_de,
                String(de) + ":promo:" + pref.column,
                de,
                sc,
                0,
            )
            ops.append(OP_LOAD_DIM)
            ops.append(Int64(ai))
            ops.append(Int64(fk_slot_of_de[de]))
        elif o.op == OP_PUSH_CONST:
            var cid = Int(o.a)
            if cid < 0 or cid >= len(d.consts):
                raise Error("generic-dims: PUSH_CONST bad const id")
            ops.append(OP_PUSH_CONST)
            ops.append(d.consts[cid].lo)
            ops.append(Int64(0))
        else:
            ops.append(o.op)
            ops.append(o.a)
            ops.append(o.b)
    return MetricPlan(ops^, len(agg.program))


# ===-------------------------------------------------------------------===#
# TPC-H Q5 finalize (5 dims, DENSE_GROUP over a dim-carried VARCHAR n_name).
#
# Q5 has a correlated dim<->dim equality (c_nationkey == s_nationkey) coupling
# customer and supplier on the SAME fact (lineitem) row, plus a VARCHAR group
# key (n_name) carried from nation and DENSE_GROUP over it. The fully generic
# dim classifier can't express the correlated compare, so this is a dedicated
# branch that mirrors the bespoke `EnsureQ5Pinned` (gpu_operator.cpp) exactly:
#
# HOST precomputes (built from the fed dim columns, all single-level so every
# GPU gather is dim[fact_col] with a fact-column slot):
#   * region:   regionkey whose r_name == r_name-filter-const.
#   * nation:   nation_in_asia[nk]=(n_regionkey==asia), nation_name[nk] (string),
#               gid_of_nation[nk] = dense rank among ASIA nations (else -1), G.
#   * supplier: supp_nation[sk]=s_nationkey,
#               supp_in_asia[sk]=nation_in_asia[supp_nation[sk]],
#               supp_group[sk]=gid_of_nation[supp_nation[sk]] (clamped >=0).
#   * customer: cust_nation[ck]=c_nationkey.
#   * orders:   order_pass[ok]=(o_orderdate in [lo,hi)),
#               order_cust_nation[ok]=cust_nation[o_custkey].
#
# GPU dim arrays (concatenated): 0=order_pass, 1=order_cust_nation,
# 2=supp_nation, 3=supp_in_asia. Pass program (single-level gathers):
#   LOAD_DIM(order_pass, l_orderkey)
#   LOAD_DIM(order_cust_nation, l_orderkey)
#   LOAD_DIM(supp_nation, l_suppkey)
#   OP_EQ                            -> (cust_nation == supp_nation) ? 1 : 0
#   OP_MUL                           -> AND with order_pass
#   LOAD_DIM(supp_in_asia, l_suppkey)
#   OP_MUL                           -> AND with supplier-in-ASIA
# Per-row gid column = supp_group[l_suppkey] (host gather into an int64 slot;
# clamped to 0 for non-ASIA rows, which never pass anyway). Metric: revenue
# (LOAD ext; PUSH 100; LOAD disc; SUB; MUL) at scale 4. Result: G rows, group
# col = nation_name[nation_of_gid] via res_str, revenue i128 scale-4; emit only
# groups with revenue>0 (stock GROUP BY over passing rows).
# ===-------------------------------------------------------------------===#
def _pin_finalize_q5(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) raises -> Int:
    ref d = handle.bitcast[GpuPlanDescriptor]()[]
    ref m = _exec_ptr()[]
    var key = Int(handle)
    if key not in m:
        return 2

    # WARM: re-run on the resident buffers + assemble from cached metadata. For
    # the Path B predicate-independent residency the resident dim arrays + gid are
    # constant-INDEPENDENT; THIS query's region/date scalars are extracted from the
    # live descriptor `d` + the cached region map and threaded into the re-run.
    var sig = _signature(d)
    ref p2 = _pin2_ptr()[]
    if sig in p2:
        ref dst = m[key]
        if p2[sig].q5_pred:
            var q5b = _q5_pred_params(
                d, p2[sig].q5_region_names, p2[sig].q5_region_keys
            )
            _assemble(dst, p2[sig], q5_bounds=q5b^)
        else:
            _assemble(dst, p2[sig])
        return 0

    # COLD: build dim arrays + packed columns from fed host columns, upload once.
    ref st = m[key]
    var n = st.n_rows
    var q5_pred_on = _q5_pred_enabled(d)

    # --- locate the 5 dim edges by table name ---
    var de_orders = _de_of_table(d, "orders")
    var de_supplier = _de_of_table(d, "supplier")
    var de_customer = _de_of_table(d, "customer")
    var de_nation = _de_of_table(d, "nation")
    var de_region = _de_of_table(d, "region")
    if (
        de_orders < 0 or de_supplier < 0 or de_customer < 0
        or de_nation < 0 or de_region < 0
    ):
        return 20

    # --- fact numeric slot map (need l_orderkey + l_suppkey gather slots) ---
    var col_slot = Dict[String, Int]()
    var numeric_matcols: List[Int] = []
    for j in range(len(st.mat_cols)):
        if st.cols[j].type_tag != TYPE_VARCHAR:
            var slot = len(numeric_matcols)
            col_slot[st.mat_cols[j]] = slot
            numeric_matcols.append(j)
    var n_numeric = len(numeric_matcols)
    if "l_orderkey" not in col_slot or "l_suppkey" not in col_slot:
        return 21
    var lok_slot = col_slot["l_orderkey"]
    var lsk_slot = col_slot["l_suppkey"]

    # =======================================================================
    # Phase G Stage 2 (Path B): predicate-INDEPENDENT residency for Q5. The
    # resident dim arrays + gid are the RAW (region/date-independent) forms and the
    # region + orderdate window + cust_nation==supp_nation filter is evaluated
    # IN-KERNEL from per-run scalars (o_lo/o_hi/asia_region), so DIFFERENT region/
    # date constants reuse the SAME residency (warm-across-constants). This mirrors
    # the shipped Q6/Q1/Q14 mechanism. Built only when _q5_pred_enabled(d).
    # =======================================================================
    if q5_pred_on:
        # --- nation: name[nk] + region[nk] for ALL nationkeys (region-independent).
        var c_nk = _dim_src_col(st, de_nation, "n_nationkey")
        var c_nn = _dim_src_col(st, de_nation, "n_name")
        var c_nrk = _dim_src_col(st, de_nation, "n_regionkey")
        var ndn = st.dim_n_rows[de_nation]
        var max_nk = 0
        for i in range(ndn):
            var nk = Int(_dim_col_val(st, de_nation, c_nk, i))
            if nk > max_nk:
                max_nk = nk
        # G = max_nationkey + 1 (raw nationkey is the gid). The dense kernel's
        # per-lane accumulator is sized SEG_MAX_METRICS*SEG_MAX_METRICS == 64 cells
        # (M==1 for Q5), so G must be <= 64. sf1 max nationkey == 24; if a larger
        # schema appears, fall back to the per-constant (ASIA-rank gid) path, which
        # is still correct -- just not warm-across-constants.
        var G = max_nk + 1
        if G > 64:
            q5_pred_on = False
        else:
            var nation_region = alloc[Int64](max_nk + 1)
            var nation_name: List[String] = []
            for _ in range(max_nk + 1):
                nation_name.append(String(""))
            for k in range(max_nk + 1):
                nation_region[k] = -1
            for i in range(ndn):
                var nk = Int(_dim_col_val(st, de_nation, c_nk, i))
                nation_region[nk] = _dim_col_val(st, de_nation, c_nrk, i)
                nation_name[nk] = _dim_col_str(st, de_nation, c_nn, i)

            # --- region map: r_name -> r_regionkey for ALL regions (region-indep). ---
            var c_rk_p = _dim_src_col(st, de_region, "r_regionkey")
            var c_rn_p = _dim_src_col(st, de_region, "r_name")
            var rdn_p = st.dim_n_rows[de_region]
            var q5_region_names: List[String] = []
            var q5_region_keys: List[Int64] = []
            for i in range(rdn_p):
                q5_region_names.append(_dim_col_str(st, de_region, c_rn_p, i))
                q5_region_keys.append(_dim_col_val(st, de_region, c_rk_p, i))

            # --- customer: cust_nation[c_custkey] (region/date-independent). ---
            var c_cck = _dim_src_col(st, de_customer, "c_custkey")
            var c_cnk = _dim_src_col(st, de_customer, "c_nationkey")
            var cdn = st.dim_n_rows[de_customer]
            var max_ck = 0
            for i in range(cdn):
                var ck = Int(_dim_col_val(st, de_customer, c_cck, i))
                if ck > max_ck:
                    max_ck = ck
            var cust_nation = alloc[Int64](max_ck + 1)
            for k in range(max_ck + 1):
                cust_nation[k] = -1
            for i in range(cdn):
                var ck = Int(_dim_col_val(st, de_customer, c_cck, i))
                cust_nation[ck] = _dim_col_val(st, de_customer, c_cnk, i)

            # --- supplier: supp_nation[sk] + supp_region[sk] (region-independent). ---
            var c_sk = _dim_src_col(st, de_supplier, "s_suppkey")
            var c_snk = _dim_src_col(st, de_supplier, "s_nationkey")
            var sdn = st.dim_n_rows[de_supplier]
            var max_sk = 0
            for i in range(sdn):
                var sk = Int(_dim_col_val(st, de_supplier, c_sk, i))
                if sk > max_sk:
                    max_sk = sk
            var supp_nation = alloc[Int64](max_sk + 1)
            var supp_region = alloc[Int64](max_sk + 1)
            for k in range(max_sk + 1):
                supp_nation[k] = -1
                supp_region[k] = -1
            for i in range(sdn):
                var sk = Int(_dim_col_val(st, de_supplier, c_sk, i))
                var sn = Int(_dim_col_val(st, de_supplier, c_snk, i))
                supp_nation[sk] = Int64(sn)
                if sn >= 0 and sn <= max_nk:
                    supp_region[sk] = nation_region[sn]

            # --- orders: o_orderdate[ok] (RAW) + order_cust_nation[ok] (region/
            #     date-independent; no host order_pass bake). ---
            var c_ook = _dim_src_col(st, de_orders, "o_orderkey")
            var c_ock = _dim_src_col(st, de_orders, "o_custkey")
            var c_od = _dim_src_col(st, de_orders, "o_orderdate")
            var odn = st.dim_n_rows[de_orders]
            var max_ok = 0
            for i in range(odn):
                var ok = Int(_dim_col_val(st, de_orders, c_ook, i))
                if ok > max_ok:
                    max_ok = ok
            var order_date = alloc[Int64](max_ok + 1)
            var order_cust_nation = alloc[Int64](max_ok + 1)
            for k in range(max_ok + 1):
                order_date[k] = 0
                order_cust_nation[k] = -1
            for i in range(odn):
                var ok = Int(_dim_col_val(st, de_orders, c_ook, i))
                order_date[ok] = _dim_col_val(st, de_orders, c_od, i)
                var ck = Int(_dim_col_val(st, de_orders, c_ock, i))
                if ck >= 0 and ck <= max_ck:
                    order_cust_nation[ok] = cust_nation[ck]

            # --- pack the 4 dim arrays + offsets (FIXED indices 0..3):
            #     arr0=o_orderdate[ok], arr1=order_cust_nation[ok],
            #     arr2=supp_nation[sk], arr3=supp_region[sk]. ---
            var len0 = max_ok + 1
            var len1 = max_ok + 1
            var len2 = max_sk + 1
            var len3 = max_sk + 1
            var total_dim = len0 + len1 + len2 + len3
            var dims_host = alloc[Int64](total_dim)
            var w = 0
            for i in range(len0):
                dims_host[w] = order_date[i]; w += 1
            for i in range(len1):
                dims_host[w] = order_cust_nation[i]; w += 1
            for i in range(len2):
                dims_host[w] = supp_nation[i]; w += 1
            for i in range(len3):
                dims_host[w] = supp_region[i]; w += 1
            var n_dim_arrays = 4
            var doff_host = alloc[Int64](n_dim_arrays + 1)
            doff_host[0] = 0
            doff_host[1] = Int64(len0)
            doff_host[2] = Int64(len0 + len1)
            doff_host[3] = Int64(len0 + len1 + len2)
            doff_host[4] = Int64(total_dim)

            # --- packed fact columns + the per-row gid column (RAW supp_nation
            #     [l_suppkey]; the in-kernel region gate zeroes non-region nations). ---
            var gid_slot = n_numeric
            var n_slots = n_numeric + 1
            var cols = alloc[Int64](n_slots * n if n_slots * n > 0 else 1)
            for slot in range(n_numeric):
                var mj = numeric_matcols[slot]
                for i in range(n):
                    cols[slot * n + i] = _col_val(st, mj, i)
            for i in range(n):
                var sk = Int(_col_val(st, numeric_matcols[lsk_slot], i))
                var gv = Int64(0)
                if sk >= 0 and sk <= max_sk:
                    var sn = supp_nation[sk]
                    if sn >= 0:
                        gv = sn
                cols[gid_slot * n + i] = gv

            # --- pass program: NONE (in-kernel predicate gates rows). ---
            var pass_prog: List[Int64] = []
            var pass_len = 0

            # --- metric: the single SUM(revenue) program (scale 4), unchanged. ---
            var rev_ai = -1
            for ai in range(len(d.aggregates)):
                if d.aggregates[ai].kind == AGG_SUM:
                    rev_ai = ai
                    break
            if rev_ai < 0:
                nation_region.free(); cust_nation.free()
                supp_nation.free(); supp_region.free()
                order_date.free(); order_cust_nation.free()
                dims_host.free(); doff_host.free(); cols.free()
                return 25
            var plan = _resolve_program(d, d.aggregates[rev_ai], col_slot)
            var metric_ops = plan.ops.copy()
            var metric_offsets: List[Int64] = [Int64(0)]
            var metric_lens: List[Int64] = [Int64(plan.n_ops)]
            var M = 1
            var n_ops_total = plan.n_ops

            # --- per-candidate (gid == raw nationkey) labels: n_name[gid] for ALL
            #     nationkeys; gid_to_nation is the identity. n_cand = G. ---
            var n_cols = len(d.out_types)
            var n_keys = len(d.group_keys)  # == 1 (n_name)
            var gk_is_str: List[Bool] = []
            for _ in range(n_keys):
                gk_is_str.append(True)
            var gk_str_vals: List[List[String]] = []
            var gk_i64_vals: List[List[Int64]] = []
            for g in range(G):
                var sv: List[String] = []
                var iv: List[Int64] = []
                if n_keys > 0:
                    sv.append(nation_name[g])
                    iv.append(Int64(0))
                gk_str_vals.append(sv^)
                gk_i64_vals.append(iv^)

            # --- upload the resident buffers once (DENSE_GROUP, 4 dim arrays). ---
            # Q5 is DENSE_GROUP => STORAGE row order, so the fact columns are
            # column-pool eligible. gid_slot (raw supp_nation[l_suppkey]) is a
            # per-query derived slot (>= n_numeric) => NOT pooled, copied whole.
            var ctx = shared_device_context()
            var seg_off_dummy = alloc[Int64](1)
            seg_off_dummy[0] = 0
            var pool_lease_keys: List[String] = []
            var resident: SegResident
            if _colpool_on():
                # Q5 is NOT skip-materialize-eligible (deferred): the omit mask is
                # all-False, so no column is ever pool-sourced and `_assemble_fail`
                # stays False. We still pass them for the unified signature.
                var omit_slot_q5 = _omit_slot_mask(st, numeric_matcols)
                var assemble_fail = False
                var cols_d = _colpool_assemble_cols_d(
                    ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
                    omit_slot_q5, pool_lease_keys, assemble_fail,
                )
                resident = segreduce_upload_from_packed(
                    ctx, cols_d^, n_slots, n, seg_off_dummy, 0,
                    dims_host, doff_host, n_dim_arrays,
                )
            else:
                resident = segreduce_upload(
                    ctx, cols, n_slots, n, seg_off_dummy, 0,
                    dims_host, doff_host, n_dim_arrays,
                )
            seg_off_dummy.free()

            nation_region.free(); cust_nation.free()
            supp_nation.free(); supp_region.free()
            order_date.free(); order_cust_nation.free()
            dims_host.free(); doff_host.free(); cols.free()

            # --- construct + cache the GpuPinned, then assemble (emit revenue!=0). ---
            var agg_kind: List[Int64] = [AGG_SUM]
            var agg_scale: List[Int64] = [d.aggregates[rev_ai].ret_scale]
            var agg_m0: List[Int] = [0]
            var agg_m1: List[Int] = [-1]

            var gp = GpuPinned(resident^)
            gp.pool_lease_keys = pool_lease_keys^
            gp.mode = STRAT_DENSE_GROUP
            gp.G = G
            gp.gid_slot = gid_slot
            gp.M = M
            gp.pass_prog = pass_prog^
            gp.pass_len = pass_len
            gp.metric_ops = metric_ops^
            gp.n_ops_total = n_ops_total
            gp.metric_offsets = metric_offsets^
            gp.metric_lens = metric_lens^
            gp.n_seg = 0
            gp.n_cols = n_cols
            gp.n_keys = n_keys
            gp.agg_kind = agg_kind^
            gp.agg_scale = agg_scale^
            gp.agg_m0 = agg_m0^
            gp.agg_m1 = agg_m1^
            gp.gk_is_str = gk_is_str^
            gp.gk_str_vals = gk_str_vals^
            gp.gk_i64_vals = gk_i64_vals^
            gp.n_cand = G
            gp.emit_agg = 0  # gate on revenue
            gp.emit_gt0 = False  # emit iff revenue != 0 (selected-region nations)
            gp.kind = d.kind
            gp.q5_pred = True
            gp.q5_pred_slots = [lok_slot, lsk_slot]
            gp.q5_region_names = q5_region_names.copy()
            gp.q5_region_keys = q5_region_keys.copy()
            var _fp5p = _gp_footprint_bytes(gp) if _colpool_on() else 0
            p2[sig] = gp^
            _pin2_track(sig, _fp5p)

            ref dst = m[key]
            # COLD assembles with THIS query's scalars too (same kernel as WARM):
            # resolve the region const against the just-built region map.
            var q5b0 = _q5_pred_params(d, q5_region_names, q5_region_keys)
            _assemble(dst, p2[sig], q5_bounds=q5b0^)
            return 0

    # --- region: the regionkey whose r_name == the region filter const ---
    var region_name = String("")
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != "region":
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.column == "r_name" and p.cmp == CMP_EQ:
                region_name = d.consts[p.const_id].str_val
    var c_rk = _dim_src_col(st, de_region, "r_regionkey")
    var c_rn = _dim_src_col(st, de_region, "r_name")
    var asia_region = -1
    var rdn = st.dim_n_rows[de_region]
    for i in range(rdn):
        var rn = _dim_col_str(st, de_region, c_rn, i)
        if rn == region_name:
            asia_region = Int(_dim_col_val(st, de_region, c_rk, i))
    if asia_region < 0:
        return 22

    # --- nation: in_asia + name + dense gid (rank among ASIA nations) ---
    var c_nk = _dim_src_col(st, de_nation, "n_nationkey")
    var c_nn = _dim_src_col(st, de_nation, "n_name")
    var c_nrk = _dim_src_col(st, de_nation, "n_regionkey")
    var ndn = st.dim_n_rows[de_nation]
    var max_nk = 0
    for i in range(ndn):
        var nk = Int(_dim_col_val(st, de_nation, c_nk, i))
        if nk > max_nk:
            max_nk = nk
    var nation_in_asia = alloc[Int64](max_nk + 1)
    var gid_of_nation = alloc[Int64](max_nk + 1)
    var nation_name: List[String] = []
    for _ in range(max_nk + 1):
        nation_name.append(String(""))
    for k in range(max_nk + 1):
        nation_in_asia[k] = 0
        gid_of_nation[k] = -1
    for i in range(ndn):
        var nk = Int(_dim_col_val(st, de_nation, c_nk, i))
        var nrk = Int(_dim_col_val(st, de_nation, c_nrk, i))
        nation_in_asia[nk] = Int64(1) if nrk == asia_region else Int64(0)
        nation_name[nk] = _dim_col_str(st, de_nation, c_nn, i)
    # Dense gid per ASIA nation, in ascending nationkey order (deterministic;
    # the parent ORDER BY revenue re-sorts the output anyway).
    var G = 0
    var gid_to_nation: List[Int] = []
    for k in range(max_nk + 1):
        if nation_in_asia[k] != 0:
            gid_of_nation[k] = Int64(G)
            gid_to_nation.append(k)
            G += 1
    if G <= 0:
        nation_in_asia.free(); gid_of_nation.free()
        return 23

    # --- customer: cust_nation[c_custkey] ---
    var c_cck = _dim_src_col(st, de_customer, "c_custkey")
    var c_cnk = _dim_src_col(st, de_customer, "c_nationkey")
    var cdn = st.dim_n_rows[de_customer]
    var max_ck = 0
    for i in range(cdn):
        var ck = Int(_dim_col_val(st, de_customer, c_cck, i))
        if ck > max_ck:
            max_ck = ck
    var cust_nation = alloc[Int64](max_ck + 1)
    for k in range(max_ck + 1):
        cust_nation[k] = -1
    for i in range(cdn):
        var ck = Int(_dim_col_val(st, de_customer, c_cck, i))
        cust_nation[ck] = _dim_col_val(st, de_customer, c_cnk, i)

    # --- supplier: supp_nation / supp_in_asia / supp_group (by s_suppkey) ---
    var c_sk = _dim_src_col(st, de_supplier, "s_suppkey")
    var c_snk = _dim_src_col(st, de_supplier, "s_nationkey")
    var sdn = st.dim_n_rows[de_supplier]
    var max_sk = 0
    for i in range(sdn):
        var sk = Int(_dim_col_val(st, de_supplier, c_sk, i))
        if sk > max_sk:
            max_sk = sk
    var supp_nation = alloc[Int64](max_sk + 1)
    var supp_in_asia = alloc[Int64](max_sk + 1)
    var supp_group = alloc[Int64](max_sk + 1)
    for k in range(max_sk + 1):
        supp_nation[k] = -1
        supp_in_asia[k] = 0
        supp_group[k] = 0
    for i in range(sdn):
        var sk = Int(_dim_col_val(st, de_supplier, c_sk, i))
        var sn = Int(_dim_col_val(st, de_supplier, c_snk, i))
        supp_nation[sk] = Int64(sn)
        if sn >= 0 and sn <= max_nk:
            supp_in_asia[sk] = nation_in_asia[sn]
            var g = gid_of_nation[sn]
            supp_group[sk] = g if g >= 0 else Int64(0)

    # --- orders: order_pass (o_orderdate range) + order_cust_nation ---
    var c_ook = _dim_src_col(st, de_orders, "o_orderkey")
    var c_ock = _dim_src_col(st, de_orders, "o_custkey")
    var c_od = _dim_src_col(st, de_orders, "o_orderdate")
    # o_orderdate range from the orders GET filters (GE lo, LT hi).
    var o_lo = Int64(0)
    var o_hi = Int64(0)
    var have_lo = False
    var have_hi = False
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != "orders":
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.column != "o_orderdate":
                continue
            ref c = d.consts[p.const_id]
            if p.cmp == CMP_GE:
                o_lo = c.lo; have_lo = True
            elif p.cmp == CMP_GT:
                o_lo = c.lo + 1; have_lo = True
            elif p.cmp == CMP_LT:
                o_hi = c.lo; have_hi = True
            elif p.cmp == CMP_LE:
                o_hi = c.lo + 1; have_hi = True
    if not have_lo or not have_hi:
        nation_in_asia.free(); gid_of_nation.free(); cust_nation.free()
        supp_nation.free(); supp_in_asia.free(); supp_group.free()
        return 24
    var odn = st.dim_n_rows[de_orders]
    var max_ok = 0
    for i in range(odn):
        var ok = Int(_dim_col_val(st, de_orders, c_ook, i))
        if ok > max_ok:
            max_ok = ok
    var order_pass = alloc[Int64](max_ok + 1)
    var order_cust_nation = alloc[Int64](max_ok + 1)
    for k in range(max_ok + 1):
        order_pass[k] = 0
        order_cust_nation[k] = -1
    for i in range(odn):
        var ok = Int(_dim_col_val(st, de_orders, c_ook, i))
        var od = _dim_col_val(st, de_orders, c_od, i)
        order_pass[ok] = Int64(1) if (od >= o_lo and od < o_hi) else Int64(0)
        var ck = Int(_dim_col_val(st, de_orders, c_ock, i))
        if ck >= 0 and ck <= max_ck:
            order_cust_nation[ok] = cust_nation[ck]

    # --- pack the 4 dim arrays + offsets (order_pass, order_cust_nation,
    #     supp_nation, supp_in_asia). ---
    var len0 = max_ok + 1  # order_pass
    var len1 = max_ok + 1  # order_cust_nation
    var len2 = max_sk + 1  # supp_nation
    var len3 = max_sk + 1  # supp_in_asia
    var total_dim = len0 + len1 + len2 + len3
    var dims_host = alloc[Int64](total_dim)
    var w = 0
    for i in range(len0):
        dims_host[w] = order_pass[i]; w += 1
    for i in range(len1):
        dims_host[w] = order_cust_nation[i]; w += 1
    for i in range(len2):
        dims_host[w] = supp_nation[i]; w += 1
    for i in range(len3):
        dims_host[w] = supp_in_asia[i]; w += 1
    var n_dim_arrays = 4
    var doff_host = alloc[Int64](n_dim_arrays + 1)
    doff_host[0] = 0
    doff_host[1] = Int64(len0)
    doff_host[2] = Int64(len0 + len1)
    doff_host[3] = Int64(len0 + len1 + len2)
    doff_host[4] = Int64(total_dim)

    # --- packed fact columns + the per-row gid column (supp_group[l_suppkey]) ---
    # slot layout: numeric fact slots [0..n_numeric), then gid slot.
    var gid_slot = n_numeric
    var n_slots = n_numeric + 1
    var cols = alloc[Int64](n_slots * n if n_slots * n > 0 else 1)
    for slot in range(n_numeric):
        var mj = numeric_matcols[slot]
        for i in range(n):
            cols[slot * n + i] = _col_val(st, mj, i)
    for i in range(n):
        var sk = Int(_col_val(st, numeric_matcols[lsk_slot], i))
        var gv = Int64(0)
        if sk >= 0 and sk <= max_sk:
            gv = supp_group[sk]
        cols[gid_slot * n + i] = gv

    # --- pass program (single-level gathers + OP_EQ + OP_MUL) ---
    var pass_prog: List[Int64] = [
        OP_LOAD_DIM, Int64(0), Int64(lok_slot),        # order_pass[l_orderkey]
        OP_LOAD_DIM, Int64(1), Int64(lok_slot),        # order_cust_nation[l_orderkey]
        OP_LOAD_DIM, Int64(2), Int64(lsk_slot),        # supp_nation[l_suppkey]
        OP_EQ, Int64(0), Int64(0),                     # cust_nation == supp_nation
        OP_MUL, Int64(0), Int64(0),                    # AND order_pass
        OP_LOAD_DIM, Int64(3), Int64(lsk_slot),        # supp_in_asia[l_suppkey]
        OP_MUL, Int64(0), Int64(0),                    # AND supplier-in-ASIA
    ]
    var pass_len = len(pass_prog) // 3

    # --- metric: the single SUM(revenue) program (scale 4) ---
    var rev_ai = -1
    for ai in range(len(d.aggregates)):
        if d.aggregates[ai].kind == AGG_SUM:
            rev_ai = ai
            break
    if rev_ai < 0:
        nation_in_asia.free(); gid_of_nation.free(); cust_nation.free()
        supp_nation.free(); supp_in_asia.free(); supp_group.free()
        order_pass.free(); order_cust_nation.free()
        dims_host.free(); doff_host.free(); cols.free()
        return 25
    var plan = _resolve_program(d, d.aggregates[rev_ai], col_slot)
    var metric_ops = plan.ops.copy()
    var metric_offsets: List[Int64] = [Int64(0)]
    var metric_lens: List[Int64] = [Int64(plan.n_ops)]
    var M = 1
    var n_ops_total = plan.n_ops

    # --- per-candidate (gid) group-key cells: n_name of the gid's nation ---
    var n_cols = len(d.out_types)
    var n_keys = len(d.group_keys)  # == 1 (n_name)
    var gk_is_str: List[Bool] = []
    for _ in range(n_keys):
        gk_is_str.append(True)  # n_name is a dim-carried VARCHAR
    var gk_str_vals: List[List[String]] = []
    var gk_i64_vals: List[List[Int64]] = []
    for g in range(G):
        var sv: List[String] = []
        var iv: List[Int64] = []
        if n_keys > 0:
            sv.append(nation_name[gid_to_nation[g]])
            iv.append(Int64(0))
        gk_str_vals.append(sv^)
        gk_i64_vals.append(iv^)

    # --- upload the resident buffers once (DENSE_GROUP, 4 dim arrays) ---
    # DENSE_GROUP => STORAGE row order; fact columns are column-pool eligible.
    # gid_slot (per-query derived supp-group) is >= n_numeric => copied whole.
    var ctx = shared_device_context()
    var seg_off_dummy = alloc[Int64](1)
    seg_off_dummy[0] = 0
    var pool_lease_keys: List[String] = []
    var resident: SegResident
    if _colpool_on():
        # Q5 deferred: omit mask all-False (see the Path-A Q5 site above).
        var omit_slot_q5 = _omit_slot_mask(st, numeric_matcols)
        var assemble_fail = False
        var cols_d = _colpool_assemble_cols_d(
            ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
            omit_slot_q5, pool_lease_keys, assemble_fail,
        )
        resident = segreduce_upload_from_packed(
            ctx, cols_d^, n_slots, n, seg_off_dummy, 0,
            dims_host, doff_host, n_dim_arrays,
        )
    else:
        resident = segreduce_upload(
            ctx, cols, n_slots, n, seg_off_dummy, 0,
            dims_host, doff_host, n_dim_arrays,
        )
    seg_off_dummy.free()

    nation_in_asia.free(); gid_of_nation.free(); cust_nation.free()
    supp_nation.free(); supp_in_asia.free(); supp_group.free()
    order_pass.free(); order_cust_nation.free()
    dims_host.free(); doff_host.free(); cols.free()

    # --- construct + cache the GpuPinned, then assemble (emit only revenue!=0) ---
    var agg_kind: List[Int64] = [AGG_SUM]
    var agg_scale: List[Int64] = [d.aggregates[rev_ai].ret_scale]
    var agg_m0: List[Int] = [0]
    var agg_m1: List[Int] = [-1]

    var gp = GpuPinned(resident^)
    gp.pool_lease_keys = pool_lease_keys^
    gp.mode = STRAT_DENSE_GROUP
    gp.G = G
    gp.gid_slot = gid_slot
    gp.M = M
    gp.pass_prog = pass_prog^
    gp.pass_len = pass_len
    gp.metric_ops = metric_ops^
    gp.n_ops_total = n_ops_total
    gp.metric_offsets = metric_offsets^
    gp.metric_lens = metric_lens^
    gp.n_seg = 0
    gp.n_cols = n_cols
    gp.n_keys = n_keys
    gp.agg_kind = agg_kind^
    gp.agg_scale = agg_scale^
    gp.agg_m0 = agg_m0^
    gp.agg_m1 = agg_m1^
    gp.gk_is_str = gk_is_str^
    gp.gk_str_vals = gk_str_vals^
    gp.gk_i64_vals = gk_i64_vals^
    gp.n_cand = G
    gp.emit_agg = 0  # gate on revenue
    gp.emit_gt0 = False  # emit iff revenue != 0 (stock GROUP BY over passers)
    gp.kind = d.kind  # routes Q5 to the comptime-specialized dense kernel
    var _fp5 = _gp_footprint_bytes(gp) if _colpool_on() else 0
    p2[sig] = gp^
    _pin2_track(sig, _fp5)

    ref dst = m[key]
    _assemble(dst, p2[sig])
    return 0


def _pin_finalize_generic_dims(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) raises -> Int:
    ref d = handle.bitcast[GpuPlanDescriptor]()[]
    ref m = _exec_ptr()[]
    var key = Int(handle)
    if key not in m:
        return 2

    # WARM: re-run on the resident buffers + assemble from cached metadata. For the
    # generalized Q14 predicate-independent path the CURRENT query's fact-filter
    # bounds are threaded into the re-run (the resident columns + promo dim array
    # are constant-independent; the kernel applies THIS query's shipdate range).
    var sig = _signature(d)
    ref p2 = _pin2_ptr()[]
    if sig in p2:
        ref dst = m[key]
        if p2[sig].mode == STRAT_HASH_GROUP:
            _assemble_hash(dst, p2[sig])
        else:
            var genb = _gen_pred_bounds(d)
            _assemble(
                dst, p2[sig],
                Q6PredSpec(False, 0, 0, 0, 0, 0, 0, 0, 0),
                genb^,
            )
        return 0

    # COLD: build dim arrays + packed columns from fed host columns, upload once.
    ref st = m[key]
    var n = st.n_rows
    var n_dims_edges = len(d.dim_edges)

    # --- fact numeric slot map (mat_cols order) -> packed-column slot ---
    var col_slot = Dict[String, Int]()
    var numeric_matcols: List[Int] = []  # slot -> mat_cols idx
    for j in range(len(st.mat_cols)):
        if st.cols[j].type_tag != TYPE_VARCHAR:
            var slot = len(numeric_matcols)
            col_slot[st.mat_cols[j]] = slot
            numeric_matcols.append(j)
    var n_numeric = len(numeric_matcols)

    # SKIP-MATERIALIZE: per-numeric-slot omit mask (see _pin_finalize_generic).
    var omit_slot = _omit_slot_mask(st, numeric_matcols)

    # Per-numeric-slot decimal scale (for AVG rescale; Q14 has no AVG). For an
    # OMITTED slot st.cols[mj].type_tag is unset (0) -> recover the pooled type_tag
    # (landmine #2). Q14 has no AVG so this is precautionary here, but it keeps the
    # generic-dims path airtight for any future AVG-bearing FK-join shape.
    var col_scale_of_slot: List[Int64] = []
    for slot in range(n_numeric):
        var tt: Int64
        if slot < len(omit_slot) and omit_slot[slot]:
            var ok2 = col_key(
                d.fact_table, st.mat_cols[numeric_matcols[slot]],
                REPR_INT64_PACKED, ORDERING_STORAGE, st.n_rows,
            )
            tt = col_pool_type_tag(ok2)
        else:
            tt = st.cols[numeric_matcols[slot]].type_tag
        if tt == TYPE_DATE or tt == TYPE_INTEGER or tt == TYPE_BIGINT:
            col_scale_of_slot.append(Int64(0))
        else:
            col_scale_of_slot.append(Int64(2))
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table or p.col.column not in col_slot:
                continue
            ref c = d.consts[p.const_id]
            if c.type_tag == TYPE_DECIMAL:
                col_scale_of_slot[col_slot[p.col.column]] = c.scale

    # Classify each dim_edge. A FACT-edge joins the fact table directly (its
    # fact_key column is a materialized fact column -> a gather slot). A CHILD-edge
    # joins ANOTHER dim instead (transitive dim->dim, e.g. Q3 customer joins orders
    # on o_custkey): there is no fact gather slot; the child is folded into its
    # parent dim's pass flag on host. `fk_slot_of_de[de]` = the fact gather slot
    # for a fact-edge, or -1 for a child-edge. `parent_de_of_de[de]` = the parent
    # dim_edge index for a child-edge (the dim whose table == this edge's
    # fact_key.table), or -1 for a fact-edge.
    var fk_slot_of_de: List[Int] = []
    var parent_de_of_de: List[Int] = []
    for de in range(n_dims_edges):
        ref e = d.dim_edges[de]
        if e.fact_key.table == d.fact_table:
            if e.fact_key.column not in col_slot:
                return 9  # fact key not materialized (shouldn't happen)
            fk_slot_of_de.append(col_slot[e.fact_key.column])
            parent_de_of_de.append(-1)
        else:
            # Transitive child edge: find the parent dim_edge it attaches to.
            var pde = _de_of_table(d, e.fact_key.table)
            if pde < 0:
                return 9  # child edge attaches to no known dim
            fk_slot_of_de.append(-1)
            parent_de_of_de.append(pde)

    # ------------------------------------------------------------------
    # Resolve dim-array indices while resolving programs. A dim array is keyed by
    # "<de>:promo:<col>" / "<de>:col:<col>" / "<de>:pass" so repeated references
    # share one array. `dim_specs` records how to build each.
    # ------------------------------------------------------------------
    var dim_index = Dict[String, Int]()
    var dim_specs: List[DimArraySpec] = []

    # --- lower each aggregate (UNGROUPED: G=1). Q14: 2 SUMs (promo, total). ---
    var agg_kind: List[Int64] = []
    var agg_m0: List[Int] = []
    var metric_ops: List[Int64] = []
    var metric_offsets: List[Int64] = []
    var metric_lens: List[Int64] = []
    var n_ops_total = 0
    for ai in range(len(d.aggregates)):
        ref agg = d.aggregates[ai]
        agg_kind.append(agg.kind)
        if agg.kind == AGG_COUNT_STAR:
            metric_offsets.append(Int64(n_ops_total))
            metric_ops.append(OP_PUSH_CONST)
            metric_ops.append(Int64(1))
            metric_ops.append(Int64(0))
            metric_lens.append(Int64(1))
            n_ops_total += 1
            agg_m0.append(len(metric_offsets) - 1)
        else:  # AGG_SUM (Q14)
            var plan = _resolve_dim_program(
                d, agg, st, col_slot, fk_slot_of_de, dim_index, dim_specs
            )
            metric_offsets.append(Int64(n_ops_total))
            for x in range(len(plan.ops)):
                metric_ops.append(plan.ops[x])
            metric_lens.append(Int64(plan.n_ops))
            n_ops_total += plan.n_ops
            agg_m0.append(len(metric_offsets) - 1)
    var M = len(metric_offsets)

    # --- dim-carried GROUP-KEY arrays (SORT_SEGREDUCE) -> kind-1 carried ---
    # o_orderdate / o_shippriority live on the orders dim; gathered per output
    # segment by the fact group key's FK value. Build one kind-1 dim array per
    # dim-carried group key so result assembly can look it up by seg_key.
    # `gkey_dim_arr[gk]` = dim-array idx for dim group key gk (-1 for fact keys).
    var gkey_dim_arr: List[Int] = []
    for gk in range(len(d.group_keys)):
        ref gkey = d.group_keys[gk]
        if gkey.table == d.fact_table:
            gkey_dim_arr.append(-1)
            continue
        var gde = _de_of_table(d, gkey.table)
        if gde < 0:
            return 9
        var gsc = _dim_src_col(st, gde, gkey.column)
        var gai = _get_or_make_dim(
            dim_index, dim_specs, fk_slot_of_de,
            String(gde) + ":col:" + gkey.column, gde, gsc, 1,
        )
        gkey_dim_arr.append(gai)

    # --- dim pass-flag arrays (dim filters) -> ANDed into the row pass ---
    # For each FACT-edge that carries dim filters (and/or absorbs a transitive
    # child fold), build a 0/1 pass-flag dim array indexed by the dim PK and AND
    # it into the pass program via OP_LOAD_DIM + OP_MUL. A CHILD-edge (a dim
    # attached to another dim, e.g. Q3 customer->orders) is NOT ANDed directly;
    # it is FOLDED on host into its parent fact-edge's pass array. So a fact-edge
    # needs a pass array iff it has its own filters OR any child folds into it.
    var dim_pass_de: List[Int] = []  # dim-array idx of each fact-edge pass flag
    for de in range(n_dims_edges):
        if fk_slot_of_de[de] < 0:
            continue  # child edge: folded into its parent below, not ANDed here
        var needs_pass = False
        for gi in range(len(d.gets)):
            if d.gets[gi].table == d.dim_edges[de].dim_table and len(
                d.gets[gi].filters
            ) > 0:
                needs_pass = True
        # any child edge folding into this fact-edge also forces a pass array
        for ce in range(n_dims_edges):
            if parent_de_of_de[ce] == de:
                needs_pass = True
        if needs_pass:
            var idx = len(dim_specs)
            # src_col -1 marks a synthetic pass-flag array (built from filters
            # + transitive child folds during the kind-2 build below).
            dim_specs.append(DimArraySpec(de, -1, 2, fk_slot_of_de[de]))
            dim_pass_de.append(idx)

    # ------------------------------------------------------------------
    # Transitive child folds. For each CHILD-edge (a dim attached to another dim,
    # e.g. customer joins orders on o_custkey), build a dense 0/1 pass array
    # indexed by the CHILD dim PK from the child's own filters (Q3:
    # is_building[c_custkey] = (c_mktsegment=='BUILDING')). The parent fact-edge's
    # kind-2 build below ANDs in is_building[parent_row's near_col value]. This is
    # the general "dim attached to a dim" mechanism: a child folds into its parent
    # exactly like a fact-edge folds into the fact row pass, just one level up.
    # `child_pass_of_de[ce]` holds the array (empty for non-child edges).
    var child_pass_of_de: List[List[Int64]] = []
    for _ in range(n_dims_edges):
        child_pass_of_de.append(List[Int64]())
    for ce in range(n_dims_edges):
        if parent_de_of_de[ce] < 0:
            continue  # not a child edge
        var dn = st.dim_n_rows[ce]
        var maxpk = 0
        for i in range(dn):
            var pk = Int(_dim_col_val(st, ce, 0, i))
            if pk > maxpk:
                maxpk = pk
        var cpass = List[Int64]()
        for _ in range(maxpk + 1):
            cpass.append(Int64(0))
        # collect the child's own filters (e.g. c_mktsegment = 'BUILDING').
        var fcol: List[Int] = []
        var fcmp: List[Int64] = []
        var fk: List[Int64] = []
        var fstr: List[String] = []
        var fis_str: List[Bool] = []
        for gi in range(len(d.gets)):
            ref g = d.gets[gi]
            if g.table != d.dim_edges[ce].dim_table:
                continue
            for fi in range(len(g.filters)):
                ref p = g.filters[fi]
                fcol.append(_dim_src_col(st, ce, p.col.column))
                fcmp.append(p.cmp)
                ref c = d.consts[p.const_id]
                fk.append(c.lo)
                if c.type_tag == TYPE_VARCHAR:
                    fstr.append(c.str_val)
                    fis_str.append(True)
                else:
                    fstr.append(String(""))
                    fis_str.append(False)
        for i in range(dn):
            var pk = Int(_dim_col_val(st, ce, 0, i))
            var ok = True
            for fj in range(len(fcol)):
                if fis_str[fj]:
                    # VARCHAR equality (c_mktsegment = 'BUILDING'): exact match.
                    var sv = _dim_col_str(st, ce, fcol[fj], i)
                    var eq = sv == fstr[fj]
                    if fcmp[fj] == CMP_EQ:
                        if not eq:
                            ok = False
                            break
                    elif fcmp[fj] == CMP_NE:
                        if eq:
                            ok = False
                            break
                    else:
                        ok = False
                        break
                else:
                    var v = _dim_col_val(st, ce, fcol[fj], i)
                    if not _pred_pass(v, fcmp[fj], fk[fj]):
                        ok = False
                        break
            cpass[pk] = Int64(1) if ok else Int64(0)
        child_pass_of_de[ce] = cpass^

    # ------------------------------------------------------------------
    # Build each dense dim array from the fed dim columns. Size = max PK + 1.
    # ------------------------------------------------------------------
    var n_dim_arrays = len(dim_specs)
    var dim_arrays: List[List[Int64]] = []
    for _ in range(n_dim_arrays):
        dim_arrays.append(List[Int64]())

    for ax in range(n_dim_arrays):
        ref spec = dim_specs[ax]
        var de = spec.de
        var dn = st.dim_n_rows[de]
        # PK column is dim request column 0 (we put dim_key first in _dim_columns)
        # Find max PK to size the dense array.
        var maxpk = 0
        for i in range(dn):
            var pk = Int(_dim_col_val(st, de, 0, i))
            if pk > maxpk:
                maxpk = pk
        var arr = List[Int64]()
        for _ in range(maxpk + 1):
            arr.append(Int64(0))
        if spec.kind == 0:
            # promo flag: p_type (VARCHAR src col) starts with 'PROMO'.
            for i in range(dn):
                var pk = Int(_dim_col_val(st, de, 0, i))
                var s = _dim_col_str(st, de, spec.src_col, i)
                arr[pk] = Int64(1) if _starts_promo(s) else Int64(0)
        elif spec.kind == 1:
            # carried numeric: widen to int64 at the column's native scale.
            for i in range(dn):
                var pk = Int(_dim_col_val(st, de, 0, i))
                arr[pk] = _dim_col_val(st, de, spec.src_col, i)
        else:  # kind == 2: synthetic 0/1 dim pass flag from dim filters
            # Collect this dim's OWN filters as (src_col, cmp, const-lo) -> ANDed.
            var fcol: List[Int] = []
            var fcmp: List[Int64] = []
            var fk: List[Int64] = []
            for gi in range(len(d.gets)):
                ref g = d.gets[gi]
                if g.table != d.dim_edges[de].dim_table:
                    continue
                for fi in range(len(g.filters)):
                    ref p = g.filters[fi]
                    fcol.append(_dim_src_col(st, de, p.col.column))
                    fcmp.append(p.cmp)
                    fk.append(d.consts[p.const_id].lo)
            # Transitive child folds attaching to THIS dim: gather each child's
            # near-side column (e.g. orders.o_custkey) value per row and AND in
            # the child's pass flag (is_building[o_custkey]).
            var cf_near_col: List[Int] = []  # parent dim src col of the FK
            var cf_child: List[Int] = []  # child dim_edge index
            for ce in range(n_dims_edges):
                if parent_de_of_de[ce] != de:
                    continue
                cf_near_col.append(
                    _dim_src_col(st, de, d.dim_edges[ce].fact_key.column)
                )
                cf_child.append(ce)
            for i in range(dn):
                var pk = Int(_dim_col_val(st, de, 0, i))
                var ok = True
                for fj in range(len(fcol)):
                    var v = _dim_col_val(st, de, fcol[fj], i)
                    if not _pred_pass(v, fcmp[fj], fk[fj]):
                        ok = False
                        break
                if ok:
                    for cj in range(len(cf_child)):
                        var ck = Int(_dim_col_val(st, de, cf_near_col[cj], i))
                        ref cpass = child_pass_of_de[cf_child[cj]]
                        var cflag = Int64(0)
                        if ck >= 0 and ck < len(cpass):
                            cflag = cpass[ck]
                        if cflag == 0:
                            ok = False
                            break
                arr[pk] = Int64(1) if ok else Int64(0)
        dim_arrays[ax] = arr^

    # concatenate dim arrays + offsets.
    var dim_offsets: List[Int64] = [Int64(0)]
    var total_dim = 0
    for ax in range(n_dim_arrays):
        total_dim += len(dim_arrays[ax])
        dim_offsets.append(Int64(total_dim))
    var dims_host = alloc[Int64](total_dim if total_dim > 0 else 1)
    var doff_host = alloc[Int64](n_dim_arrays + 1)
    var w = 0
    for ax in range(n_dim_arrays):
        for i in range(len(dim_arrays[ax])):
            dims_host[w] = dim_arrays[ax][i]
            w += 1
    for ax in range(n_dim_arrays + 1):
        doff_host[ax] = dim_offsets[ax]

    # Generalized Q14 (UNGROUPED) predicate-independent path: flag on + canonical
    # Q14 shape (single fact column, all-range, no dim filters -> dim_pass_de is
    # empty). When active, skip the host fact pass bake AND emit no pass program;
    # the kernel evaluates the fact filter in-kernel from the resident column slot
    # + per-run bounds. The promo dim array (constant-independent) is still gathered
    # by the METRIC programs, untouched. Only safe when there are no dim pass-flag
    # arrays to AND (guaranteed by _gen_pred_eligible's no-dim-filter rule).
    var gen_pred_on = (
        _gen_pred_enabled(d)
        and d.kind == KIND_Q14
        and d.strategy == STRAT_UNGROUPED
        and len(dim_pass_de) == 0
    )

    # --- host fact pass column (AND of fact range predicates), then AND in the
    # dim pass-flag arrays via OP_LOAD_DIM in the pass PROGRAM (not the host col).
    var pass_slot = n_numeric
    var n_slots = n_numeric + 1
    var pass_col = alloc[Int64](n if n > 0 else 1)
    var f_slot: List[Int] = []
    var f_cmp: List[Int64] = []
    var f_k: List[Int64] = []
    for gi in range(len(d.gets)):
        ref g = d.gets[gi]
        if g.table != d.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table != d.fact_table or p.col.column not in col_slot:
                continue
            f_slot.append(col_slot[p.col.column])
            f_cmp.append(p.cmp)
            f_k.append(d.consts[p.const_id].lo)
    var n_filters = len(f_slot)
    if gen_pred_on:
        # No host pass bake: zero the slot (the in-kernel predicate gates rows).
        for i in range(n):
            pass_col[i] = Int64(0)
    else:
        # SKIP-MATERIALIZE airtight guard (see _pin_finalize_generic): omitting a
        # fact filter column is only safe when the in-kernel predicate replaces the
        # host bake (gen_pred_on). If we are on the host-bake path yet a filter slot
        # is omitted, fail closed (CPU fallback) rather than read an unfilled slot.
        for fi in range(n_filters):
            var fs = f_slot[fi]
            if fs < len(omit_slot) and omit_slot[fs]:
                pass_col.free(); dims_host.free(); doff_host.free()
                return 8
        for i in range(n):
            var ok = True
            for fi in range(n_filters):
                var v = _col_val(st, numeric_matcols[f_slot[fi]], i)
                if not _pred_pass(v, f_cmp[fi], f_k[fi]):
                    ok = False
                    break
            pass_col[i] = Int64(1) if ok else Int64(0)

    # --- pack the fact columns + pass column ---
    var cols = alloc[Int64](n_slots * n if n_slots * n > 0 else 1)
    for slot in range(n_numeric):
        # SKIP-MATERIALIZE: an OMITTED slot is sourced from the pool (D2D); its
        # st.cols[mj] is UNFILLED, so DO NOT pack it (the host region is unread).
        if slot < len(omit_slot) and omit_slot[slot]:
            continue
        var mj = numeric_matcols[slot]
        for i in range(n):
            cols[slot * n + i] = _col_val(st, mj, i)
    for i in range(n):
        cols[pass_slot * n + i] = pass_col[i]

    # --- pass program: LOAD_COL(pass_slot), then for each dim pass-flag array
    # LOAD_DIM(idx, fk_slot); MUL (the dim-filter-AND-via-MUL mechanism). ---
    var pass_prog: List[Int64] = [OP_LOAD_COL, Int64(pass_slot), Int64(0)]
    for pj in range(len(dim_pass_de)):
        ref spec = dim_specs[dim_pass_de[pj]]
        pass_prog.append(OP_LOAD_DIM)
        pass_prog.append(Int64(dim_pass_de[pj]))
        pass_prog.append(Int64(spec.fk_slot))
        pass_prog.append(OP_MUL)
        pass_prog.append(Int64(0))
        pass_prog.append(Int64(0))
    var pass_len = len(pass_prog) // 3
    if gen_pred_on:
        # Generalized Q14: the in-kernel fact filter replaces the pass program
        # entirely (dim_pass_de is empty here, so nothing else was ANDed in).
        pass_prog = []
        pass_len = 0

    var ctx = shared_device_context()
    var n_cols = len(d.out_types)
    var n_keys = len(d.group_keys)

    # =====================================================================
    # HASH_GROUP branch (Q3 on NVIDIA / AMD): one-pass GPU hash-aggregate keyed
    # by the integer fact group key (l_orderkey). No sort, no ORDER BY, no 1-warp
    # -per-tiny-segment launch. The kernel atomic-accumulates each order's int64
    # revenue into an open-addressing hash slot; we read back the occupied slots,
    # widen to int128 on host, gate emit on revenue>0 (== stock GROUP BY), and
    # look up the dim-carried group keys (o_orderdate / o_shippriority) by the
    # orderkey from the host carried dim arrays (constant within an orderkey).
    #
    # EXACTNESS BOUND: each group's revenue must fit int64. Per-order revenue is
    # SUM(l_extendedprice*(100-l_discount)) over an order's few lineitems: per
    # lineitem < 1e7(price,scale2) * 100 < 1e9, and orders have O(1..10) lineitems
    # at TPC-H scale => well under 9.2e18 (int64 max). If a single group could
    # overflow int64 the matcher must NOT route here (it would need int128
    # atomics, which this path does not provide). For the supported Q3 shape this
    # holds by construction; the int128 widening on read-back keeps the OUTPUT
    # decimal-exact.
    # =====================================================================
    if d.strategy == STRAT_HASH_GROUP and n_keys > 0:
        var fact_gk = String("")
        for gk in range(n_keys):
            if d.group_keys[gk].table == d.fact_table:
                fact_gk = d.group_keys[gk].column
                break
        if fact_gk == "" or fact_gk not in col_slot:
            cols.free(); pass_col.free(); dims_host.free(); doff_host.free()
            return 10
        var gk_slot = col_slot[fact_gk]

        # Capacity bound on DISTINCT groups. The fact group key (l_orderkey) joins
        # to the orders dim, whose row count is a tight upper bound on the number
        # of distinct orderkeys among the fact rows (every fact group key value is
        # one orders PK). Prefer that; fall back to the fact row count `n` (always
        # a safe superset) if no dim carries the key. cap = next pow2 >= 2*bound
        # keeps the open-addressing load factor <= 0.5 so probe chains stay short
        # AND keeps the device table + D->H read-back proportional to the group
        # universe, not the (much larger) fact row count.
        var group_bound = n  # safe fallback
        for gk in range(n_keys):
            if d.group_keys[gk].table != d.fact_table:
                var gde = _de_of_table(d, d.group_keys[gk].table)
                if gde >= 0 and st.dim_n_rows[gde] > 0:
                    group_bound = st.dim_n_rows[gde]
                    break
        var cap = 1
        var target = 2 * group_bound + 1
        while cap < target:
            cap <<= 1
        if cap < 2:
            cap = 2

        # Locate the single SUM metric (Q3 revenue) for the emit-rule gate.
        var rev_ai = -1
        for ai in range(len(d.aggregates)):
            if agg_kind[ai] == AGG_SUM:
                rev_ai = ai
                break

        # Carried dim arrays (host copies) for each dim-carried group key, so
        # _assemble_hash can look up o_orderdate / o_shippriority by orderkey.
        var hash_gk_dim_arr: List[List[Int64]] = []
        for gk in range(n_keys):
            if gkey_dim_arr[gk] >= 0:
                hash_gk_dim_arr.append(dim_arrays[gkey_dim_arr[gk]].copy())
            else:
                hash_gk_dim_arr.append(List[Int64]())

        # Upload resident buffers once (no seg offsets needed for hash).
        # HASH_GROUP appends NO ORDER BY => STORAGE row order, so the fact columns
        # (incl. the integer fact group key l_orderkey, read in storage order) are
        # column-pool eligible. pass_slot is per-query derived => copied whole.
        var seg_off_dummy_h = alloc[Int64](1)
        seg_off_dummy_h[0] = 0
        var pool_lease_keys_h: List[String] = []
        var resident: SegResident
        if _colpool_on():
            # HASH_GROUP (Q3) is NOT skip-materialize-eligible (excluded): omit_slot
            # is all-False here, so nothing is pool-sourced and `_assemble_fail`
            # stays False. Passed for the unified signature.
            var assemble_fail = False
            var cols_d = _colpool_assemble_cols_d(
                ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
                omit_slot, pool_lease_keys_h, assemble_fail,
            )
            resident = segreduce_upload_from_packed(
                ctx, cols_d^, n_slots, n, seg_off_dummy_h, 0,
                dims_host, doff_host, n_dim_arrays,
            )
        else:
            resident = segreduce_upload(
                ctx, cols, n_slots, n, seg_off_dummy_h, 0,
                dims_host, doff_host, n_dim_arrays,
            )
        seg_off_dummy_h.free()
        cols.free(); pass_col.free(); dims_host.free(); doff_host.free()

        var agg_scale: List[Int64] = []
        var agg_m1: List[Int] = []
        for ai in range(len(d.aggregates)):
            agg_scale.append(d.aggregates[ai].ret_scale)
            agg_m1.append(-1)

        # Group-key placement: all int64 (l_orderkey BIGINT, o_orderdate DATE,
        # o_shippriority INTEGER). _assemble_hash places them via res_lo.
        var gk_is_str: List[Bool] = []
        for _ in range(n_keys):
            gk_is_str.append(False)

        var gp = GpuPinned(resident^)
        gp.pool_lease_keys = pool_lease_keys_h^
        gp.mode = STRAT_HASH_GROUP
        gp.G = 1
        gp.gid_slot = 0
        gp.M = M
        gp.pass_prog = pass_prog^
        gp.pass_len = pass_len
        gp.metric_ops = metric_ops^
        gp.n_ops_total = n_ops_total
        gp.metric_offsets = metric_offsets^
        gp.metric_lens = metric_lens^
        gp.n_seg = 0
        gp.n_cols = n_cols
        gp.n_keys = n_keys
        gp.agg_kind = agg_kind^
        gp.agg_scale = agg_scale^
        gp.agg_m0 = agg_m0^
        gp.agg_m1 = agg_m1^
        gp.gk_is_str = gk_is_str^
        gp.gk_str_vals = []
        gp.gk_i64_vals = []
        gp.n_cand = 0
        gp.emit_agg = rev_ai
        gp.emit_gt0 = True
        gp.hash_gk_slot = gk_slot
        gp.hash_cap = cap
        gp.hash_gk_dim_arr = hash_gk_dim_arr^
        gp.kind = d.kind  # Q3 HASH_GROUP stays on the generic interpreter path
        var _fph = _gp_footprint_bytes(gp) if _colpool_on() else 0
        p2[sig] = gp^
        _pin2_track(sig, _fph)

        ref dst = m[key]
        _assemble_hash(dst, p2[sig])
        return 0

    # =====================================================================
    # SORT_SEGREDUCE branch (Q3): the fact is materialized ORDER BY the fact
    # group key (l_orderkey); build segments from it, one warp per order, and
    # emit one output row per order with revenue > 0 (matching stock's
    # `if (r <= 0) continue` rule: an order appears iff it has >=1 lineitem
    # passing l_shipdate>cutoff AND order_pass). Dim-carried group keys
    # (o_orderdate / o_shippriority) are gathered per segment by seg_key.
    # =====================================================================
    if d.strategy == STRAT_SORT_SEGREDUCE and n_keys > 0:
        # The fact group key is the FIRST group key on the fact table; it is the
        # sort column and the segment key. It must be a numeric fact column.
        var fact_gk = String("")
        for gk in range(n_keys):
            if d.group_keys[gk].table == d.fact_table:
                fact_gk = d.group_keys[gk].column
                break
        if fact_gk == "" or fact_gk not in col_slot:
            cols.free(); pass_col.free(); dims_host.free(); doff_host.free()
            return 10
        var gk_slot = col_slot[fact_gk]

        # Build seg_off (boundaries where the sorted key changes) + seg_key[s].
        var seg_off_l: List[Int64] = [Int64(0)]
        var seg_key_l: List[Int64] = []
        if n > 0:
            var cur = _col_val(st, numeric_matcols[gk_slot], 0)
            seg_key_l.append(cur)
            for i in range(1, n):
                var v = _col_val(st, numeric_matcols[gk_slot], i)
                if v != cur:
                    seg_off_l.append(Int64(i))
                    seg_key_l.append(v)
                    cur = v
            seg_off_l.append(Int64(n))
        var n_seg = len(seg_key_l)
        var seg_off_h = alloc[Int64](n_seg + 1)
        for s in range(n_seg + 1):
            seg_off_h[s] = seg_off_l[s]

        # Locate the single SUM metric (Q3 revenue) for the emit-rule test.
        var rev_ai = -1
        for ai in range(len(d.aggregates)):
            if agg_kind[ai] == AGG_SUM:
                rev_ai = ai
                break

        # Per-segment group-key cells (constant for the cache entry): the fact
        # group key == seg_key; dim-carried keys (o_orderdate / o_shippriority)
        # gathered now by seg_key from the dim arrays (which are about to be
        # uploaded — read them on host while still alive). All int64 cells.
        var gk_is_str: List[Bool] = []
        for _ in range(n_keys):
            gk_is_str.append(False)
        var gk_str_vals: List[List[String]] = []
        var gk_i64_vals: List[List[Int64]] = []
        for s in range(n_seg):
            var skey = Int(seg_key_l[s])
            var sv: List[String] = []
            var iv: List[Int64] = []
            for gk in range(n_keys):
                sv.append(String(""))
                if d.group_keys[gk].table == d.fact_table:
                    iv.append(seg_key_l[s])  # the fact key == seg key
                else:
                    var ai2 = gkey_dim_arr[gk]
                    var off = Int(doff_host[ai2])
                    var dlen = Int(doff_host[ai2 + 1]) - off
                    var v = Int64(0)
                    if skey >= 0 and skey < dlen:
                        v = dims_host[off + skey]
                    iv.append(v)
            gk_str_vals.append(sv^)
            gk_i64_vals.append(iv^)

        # --- upload resident buffers once (incl. seg offsets + dim arrays) ---
        var resident = segreduce_upload(
            ctx, cols, n_slots, n, seg_off_h, n_seg,
            dims_host, doff_host, n_dim_arrays,
        )
        seg_off_h.free()
        cols.free(); pass_col.free(); dims_host.free(); doff_host.free()

        # COUNT/SUM only here (no AVG) -> neutral scale/m1.
        var agg_scale: List[Int64] = []
        var agg_m1: List[Int] = []
        for ai in range(len(d.aggregates)):
            agg_scale.append(d.aggregates[ai].ret_scale)
            agg_m1.append(-1)

        var gp = GpuPinned(resident^)
        gp.mode = STRAT_SORT_SEGREDUCE
        gp.G = 1
        gp.gid_slot = 0
        gp.M = M
        gp.pass_prog = pass_prog^
        gp.pass_len = pass_len
        gp.metric_ops = metric_ops^
        gp.n_ops_total = n_ops_total
        gp.metric_offsets = metric_offsets^
        gp.metric_lens = metric_lens^
        gp.n_seg = n_seg
        gp.n_cols = n_cols
        gp.n_keys = n_keys
        gp.agg_kind = agg_kind^
        gp.agg_scale = agg_scale^
        gp.agg_m0 = agg_m0^
        gp.agg_m1 = agg_m1^
        gp.gk_is_str = gk_is_str^
        gp.gk_str_vals = gk_str_vals^
        gp.gk_i64_vals = gk_i64_vals^
        gp.n_cand = n_seg
        gp.emit_agg = rev_ai  # emit a row iff revenue > 0 (stock GROUP BY)
        gp.emit_gt0 = True
        gp.kind = d.kind  # Q3 SORT_SEGREDUCE stays on the generic interpreter
        p2[sig] = gp^

        ref dst = m[key]
        _assemble(dst, p2[sig])
        return 0

    # --- upload resident buffers once (UNGROUPED, n_dims = n_dim_arrays) ---
    # UNGROUPED (Q14) appends NO ORDER BY => STORAGE row order, so the fact
    # columns are column-pool eligible. pass_slot is per-query derived (copied
    # whole). (The SORT_SEGREDUCE branch above returned already; it is the only
    # ineligible strategy here and stays VERBATIM -- it never pools.)
    var seg_off_dummy = alloc[Int64](1)
    seg_off_dummy[0] = 0
    var pool_lease_keys: List[String] = []
    var resident: SegResident
    if _colptr_eligible(d):
        # Phase 3 (Q14): pooled fact columns read directly via a pointer table
        # (no packed cols_d). The dim arrays stay a separate buffer, uploaded as
        # usual. Same skip-mat omit/lease handling + backstop as the packed path.
        var assemble_fail = False
        var derived_bufs: List[DeviceBuffer[DType.int64]] = []
        var col_ptrs_d = _colpool_assemble_col_ptrs(
            ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
            omit_slot, pool_lease_keys, derived_bufs, assemble_fail,
        )
        if assemble_fail:
            for li in range(len(pool_lease_keys)):
                release_lease(pool_lease_keys[li])
            for li in range(len(st.colpool_omit_leases)):
                release_lease(st.colpool_omit_leases[li])
            st.colpool_omit_leases = []
            seg_off_dummy.free()
            cols.free(); pass_col.free(); dims_host.free(); doff_host.free()
            return 11
        for li in range(len(st.colpool_omit_leases)):
            pool_lease_keys.append(st.colpool_omit_leases[li])
        st.colpool_omit_leases = []
        resident = segreduce_upload_from_colptr(
            ctx, col_ptrs_d^, derived_bufs^, n_slots, n, seg_off_dummy, 0,
            dims_host, doff_host, n_dim_arrays,
        )
    elif _colpool_on():
        # SKIP-MATERIALIZE (Q14 in scope): omit_slot carries the pool-resident
        # fact columns omitted from the narrowed SELECT. On a non-HIT for an
        # omitted slot the helper sets `assemble_fail` -> bail to CPU (the column
        # was never scanned/fed, so we cannot rebuild it -> never wrong).
        var assemble_fail = False
        var cols_d = _colpool_assemble_cols_d(
            ctx, d.fact_table, st, cols, n_slots, n, numeric_matcols,
            omit_slot, pool_lease_keys, assemble_fail,
        )
        if assemble_fail:
            for li in range(len(pool_lease_keys)):
                release_lease(pool_lease_keys[li])
            for li in range(len(st.colpool_omit_leases)):
                release_lease(st.colpool_omit_leases[li])
            st.colpool_omit_leases = []
            seg_off_dummy.free()
            cols.free(); pass_col.free(); dims_host.free(); doff_host.free()
            return 11
        # SUCCESS: transfer the materialize-time omit leases to the per-signature
        # set (GpuPinned owns them); clear the exec-state copy (no double release).
        for li in range(len(st.colpool_omit_leases)):
            pool_lease_keys.append(st.colpool_omit_leases[li])
        st.colpool_omit_leases = []
        resident = segreduce_upload_from_packed(
            ctx, cols_d^, n_slots, n, seg_off_dummy, 0,
            dims_host, doff_host, n_dim_arrays,
        )
    else:
        resident = segreduce_upload(
            ctx, cols, n_slots, n, seg_off_dummy, 0,
            dims_host, doff_host, n_dim_arrays,
        )
    seg_off_dummy.free()
    cols.free()
    pass_col.free()
    dims_host.free()
    doff_host.free()

    # UNGROUPED: 1 candidate, no group keys (Q14: 2 SUMs / COUNT, no AVG).
    var gk_is_str: List[Bool] = []
    var gk_str_vals: List[List[String]] = [List[String]()]
    var gk_i64_vals: List[List[Int64]] = [List[Int64]()]
    var agg_scale: List[Int64] = []
    var agg_m1: List[Int] = []
    for ai in range(len(d.aggregates)):
        agg_scale.append(d.aggregates[ai].ret_scale)
        agg_m1.append(-1)

    var gp = GpuPinned(resident^)
    gp.pool_lease_keys = pool_lease_keys^
    gp.mode = STRAT_UNGROUPED
    gp.G = 1
    gp.gid_slot = 0
    gp.M = M
    gp.pass_prog = pass_prog^
    gp.pass_len = pass_len
    gp.metric_ops = metric_ops^
    gp.n_ops_total = n_ops_total
    gp.metric_offsets = metric_offsets^
    gp.metric_lens = metric_lens^
    gp.n_seg = 0
    gp.n_cols = n_cols
    gp.n_keys = 0
    gp.agg_kind = agg_kind^
    gp.agg_scale = agg_scale^
    gp.agg_m0 = agg_m0^
    gp.agg_m1 = agg_m1^
    gp.gk_is_str = gk_is_str^
    gp.gk_str_vals = gk_str_vals^
    gp.gk_i64_vals = gk_i64_vals^
    gp.n_cand = 1
    gp.emit_agg = -1
    gp.emit_gt0 = False
    gp.kind = d.kind  # routes Q14 (UNGROUPED) to the comptime-specialized kernel
    # Generalized Q14: cache the constant-independent fact-filter slots + cmps (in
    # descriptor order, == f_slot/f_cmp here). The bounds are threaded per run.
    gp.gen_pred = gen_pred_on
    if gen_pred_on:
        var gslots: List[Int] = []
        var gcmps: List[Int64] = []
        for fi in range(n_filters):
            gslots.append(f_slot[fi])
            gcmps.append(f_cmp[fi])
        gp.gen_pred_slots = gslots^
        gp.gen_pred_cmps = gcmps^
    var _fpud = _gp_footprint_bytes(gp) if _colpool_on() else 0
    p2[sig] = gp^
    _pin2_track(sig, _fpud)

    ref dst = m[key]
    # COLD assembles with THIS query's bounds too (same kernel code as WARM).
    var gen_b0 = _gen_pred_bounds(d)
    _assemble(
        dst, p2[sig], Q6PredSpec(False, 0, 0, 0, 0, 0, 0, 0, 0), gen_b0^
    )
    return 0


@export("mojo_gpu_pin_finalize")
def mojo_gpu_pin_finalize(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 1
    try:
        ref d = handle.bitcast[GpuPlanDescriptor]()[]
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 2
        ref st = m[key]

        # The shuttle drives the GENERIC kernels (run_segreduce + eval_program)
        # for every class. No-join (Q6 UNGROUPED / Q1 DENSE_GROUP):
        if len(d.dim_edges) == 0 and (
            d.strategy == STRAT_UNGROUPED or d.strategy == STRAT_DENSE_GROUP
        ):
            return _pin_finalize_generic(handle)

        # Q5 (5 dims, DENSE_GROUP over a dim-carried VARCHAR n_name, with a
        # correlated dim<->dim equality cust_nation==supp_nation on the same fact
        # row): self-contained host-precompute + OP_EQ pass program + DENSE_GROUP
        # segreduce.
        if d.kind == KIND_Q5 and d.strategy == STRAT_DENSE_GROUP:
            return _pin_finalize_q5(handle)

        # FK-join (Q14 UNGROUPED, Q3 SORT_SEGREDUCE / HASH_GROUP): generic
        # descriptor-driven path with on-GPU dim gather (OP_LOAD_DIM) +
        # transitive dim->dim folds. HASH_GROUP is the NVIDIA/AMD Q3 path (one-
        # pass GPU hash-aggregate instead of sort+segreduce).
        if len(d.dim_edges) > 0 and (
            d.strategy == STRAT_UNGROUPED
            or d.strategy == STRAT_SORT_SEGREDUCE
            or d.strategy == STRAT_HASH_GROUP
        ):
            return _pin_finalize_generic_dims(handle)

        # Unsupported descriptor shape -> let the C++ side fall back to CPU.
        return 3
    except:
        return 6


# ---------------------------------------------------------------------------
# Results.
# ---------------------------------------------------------------------------
@export("mojo_gpu_result_rows")
def mojo_gpu_result_rows(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int:
    if Int(handle) == 0:
        return 0
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 0
        return m[key].res_rows
    except:
        return 0


@export("mojo_gpu_result_i128")
def mojo_gpu_result_i128(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    row: Int,
    col: Int,
    out_lo: UnsafePointer[Int64, MutAnyOrigin],
    out_hi: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int:
    if Int(handle) == 0:
        return 1
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 1
        ref st = m[key]
        if row < 0 or row >= st.res_rows or col < 0 or col >= st.res_cols:
            return 1
        var idx = row * st.res_cols + col
        out_lo[] = st.res_lo[idx]
        out_hi[] = st.res_hi[idx]
        return 0
    except:
        return 1


@export("mojo_gpu_result_i64")
def mojo_gpu_result_i64(
    handle: UnsafePointer[NoneType, MutAnyOrigin], row: Int, col: Int
) abi("C") -> Int64:
    # BIGINT/INTEGER/DATE cells are stored as a plain int64 in res_lo.
    if Int(handle) == 0:
        return 0
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 0
        ref st = m[key]
        if row < 0 or row >= st.res_rows or col < 0 or col >= st.res_cols:
            return 0
        return st.res_lo[row * st.res_cols + col]
    except:
        return 0


@export("mojo_gpu_result_f64")
def mojo_gpu_result_f64(
    handle: UnsafePointer[NoneType, MutAnyOrigin], row: Int, col: Int
) abi("C") -> Float64:
    if Int(handle) == 0:
        return 0.0
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 0.0
        ref st = m[key]
        if row < 0 or row >= st.res_rows or col < 0 or col >= st.res_cols:
            return 0.0
        return st.res_f64[row * st.res_cols + col]
    except:
        return 0.0


@export("mojo_gpu_result_str")
def mojo_gpu_result_str(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    row: Int,
    col: Int,
    out_ptr: UnsafePointer[UInt8, MutAnyOrigin],
    cap: Int,
) abi("C") -> Int:
    # Write the cell string's UTF-8 bytes into out_ptr (up to cap) and return the
    # full byte length (caller can detect truncation if returned len > cap).
    if Int(handle) == 0:
        return 0
    try:
        ref m = _exec_ptr()[]
        var key = Int(handle)
        if key not in m:
            return 0
        ref st = m[key]
        if row < 0 or row >= st.res_rows or col < 0 or col >= st.res_cols:
            return 0
        ref s = st.res_str[row * st.res_cols + col]
        var nbytes = s.byte_length()
        var bytes = s.as_bytes()
        var to_copy = nbytes if nbytes < cap else cap
        for k in range(to_copy):
            out_ptr[k] = bytes[k]
        return nbytes
    except:
        return 0

# ===-------------------------------------------------------------------===#
# Phase C: GPU-direct native-storage segment decode (@export wrapper).
#
# Decodes ONE column segment (raw on-disk/pinned bytes) on the GPU into a host
# out-buffer slice. The C++ side (gpu_native_decode_check) pins the segment via
# BufferManager, hands us {raw bytes, seg_bytes, n_rows, codec, type} and a host
# pointer to write the decoded values at `segment_start`. Bit-exact decode is
# verified in C++ against Connection::Query.
#
#   codec: 0 = UNCOMPRESSED fixed-width, 1 = BITPACKING (CONSTANT/FOR groups)
#   type_code: 0 = int32, 1 = int64
#   out_ptr: HOST buffer; we write n_rows decoded values of the given type.
#
# rc: 0 ok; 1 bad args; 2 unsupported type/codec; 3 internal error.
# ===-------------------------------------------------------------------===#
def _decode_segment_typed[
    T: DType
](
    seg_bytes: UnsafePointer[UInt8, ImmutAnyOrigin],
    seg_nbytes: Int,
    n_rows: Int,
    codec: Int,
    out_ptr: UnsafePointer[Scalar[T], MutAnyOrigin],
) raises -> Int32:
    var ctx = shared_device_context()
    var seg_d = ctx.enqueue_create_buffer[DType.uint8](seg_nbytes)
    var out_d = ctx.enqueue_create_buffer[T](n_rows)
    ctx.synchronize()
    ctx.enqueue_copy(seg_d, seg_bytes)
    ctx.synchronize()

    if codec == 0:
        # UNCOMPRESSED fixed-width: data_off = 0, one grid-strided launch.
        comptime ukernel = uncompressed_decode_kernel[T]
        ctx.enqueue_function[ukernel](
            seg_d, out_d, n_rows, 0, 0,
            grid_dim=256, block_dim=256,
        )
    else:
        # BITPACKING: one block per 2048-row metadata group.
        comptime bkernel = bitpacking_decode_kernel[T]
        var n_groups = (n_rows + BP_META_GROUP_SIZE - 1) // BP_META_GROUP_SIZE
        for g in range(n_groups):
            var g0 = g * BP_META_GROUP_SIZE
            var rows = (
                BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= n_rows
                else n_rows - g0
            )
            ctx.enqueue_function[bkernel](
                seg_d, seg_nbytes, out_d, g, rows, g0,
                grid_dim=1, block_dim=256,
            )
    ctx.synchronize()

    # Copy decoded values back into the caller's host out-buffer.
    var out_sub = DeviceBuffer(ctx, out_d.unsafe_ptr(), n_rows, owning=False)
    ctx.enqueue_copy(out_ptr, out_sub)
    ctx.synchronize()
    return 0


@export("mojo_gpu_decode_segment")
def mojo_gpu_decode_segment(
    seg_bytes: UnsafePointer[UInt8, ImmutAnyOrigin],
    seg_nbytes: Int,
    n_rows: Int,
    codec: Int,
    type_code: Int,
    out_ptr: UnsafePointer[NoneType, MutAnyOrigin],
) abi("C") -> Int32:
    if seg_nbytes <= 0 or n_rows <= 0:
        return 1
    if codec != 0 and codec != 1:
        return 2
    try:
        if type_code == 0:
            return _decode_segment_typed[DType.int32](
                seg_bytes, seg_nbytes, n_rows, codec,
                out_ptr.bitcast[Scalar[DType.int32]](),
            )
        elif type_code == 1:
            return _decode_segment_typed[DType.int64](
                seg_bytes, seg_nbytes, n_rows, codec,
                out_ptr.bitcast[Scalar[DType.int64]](),
            )
        else:
            return 2
    except:
        return 3
