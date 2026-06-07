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

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer, HostBuffer
from std.ffi import _Global
from std.os import abort
from std.math import sqrt
from std.memory import alloc
from std.time import perf_counter_ns


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
        i += 32
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
        # Stage this morsel into the resident input buffer (a copy).
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
            block_dim=32,
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
    var out_dev: DeviceBuffer[DType.float32]
    var n_rows: Int
    var K: Int

    def __init__(
        out self,
        var ctx: DeviceContext,
        var emb_dev: DeviceBuffer[DType.float32],
        var q_dev: DeviceBuffer[DType.float32],
        var out_dev: DeviceBuffer[DType.float32],
        n_rows: Int,
        K: Int,
    ):
        self.ctx = ctx^
        self.emb_dev = emb_dev^
        self.q_dev = q_dev^
        self.out_dev = out_dev^
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
        ctx.synchronize()
        ctx.enqueue_copy(emb_dev, emb)  # the one-time pin upload
        ctx.synchronize()
        var p = alloc[PinState](1)
        p.init_pointee_move(
            PinState(ctx^, emb_dev^, q_dev^, out_dev^, n_rows, K)
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
            block_dim=32,
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
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q6 engine: pin the 4 needed lineitem columns resident, then each query
# is a fused filter + exact-decimal sum(l_extendedprice * l_discount) kernel.
#
# DECIMAL(15,2) -> int64 (scale 2); DATE -> int32 (days). The per-row product
# and per-block partial fit int64; only the cross-block reduction needs int128
# (done on the host), so the kernel is pure-integer and the result is EXACT.
# ===-------------------------------------------------------------------===#
comptime Q6_NBLOCKS = 4096  # one warp (32 lanes) per block


def q6_kernel(
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    qty: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_lo: Int32,
    ship_hi: Int32,
    disc_lo: Int64,
    disc_hi: Int64,
    qty_hi: Int64,
):
    var lane = Int(thread_idx.x)
    var stride = Q6_NBLOCKS * 32
    var local = Int64(0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd >= ship_lo and sd < ship_hi:
            var d = disc[i]
            if d >= disc_lo and d <= disc_hi and qty[i] < qty_hi:
                local += ext[i] * d
        i += stride
    var s = warp.sum(local)
    if lane == 0:
        partials[Int(block_idx.x)] = s


struct Q6State(Movable):
    var ctx: DeviceContext
    var ship_d: DeviceBuffer[DType.int32]
    var disc_d: DeviceBuffer[DType.int64]
    var ext_d: DeviceBuffer[DType.int64]
    var qty_d: DeviceBuffer[DType.int64]
    var part_d: DeviceBuffer[DType.int64]
    var part_h: UnsafePointer[Int64, MutAnyOrigin]
    var n_rows: Int
    # Pinned host-staging buffers (only populated by the "pinned" pin path;
    # held here so they outlive the fill + upload and are freed with the state).
    var ship_h: Optional[HostBuffer[DType.int32]]
    var disc_h: Optional[HostBuffer[DType.int64]]
    var ext_h: Optional[HostBuffer[DType.int64]]
    var qty_h: Optional[HostBuffer[DType.int64]]

    def __init__(
        out self,
        var ctx: DeviceContext,
        var ship_d: DeviceBuffer[DType.int32],
        var disc_d: DeviceBuffer[DType.int64],
        var ext_d: DeviceBuffer[DType.int64],
        var qty_d: DeviceBuffer[DType.int64],
        var part_d: DeviceBuffer[DType.int64],
        part_h: UnsafePointer[Int64, MutAnyOrigin],
        n_rows: Int,
    ):
        self.ctx = ctx^
        self.ship_d = ship_d^
        self.disc_d = disc_d^
        self.ext_d = ext_d^
        self.qty_d = qty_d^
        self.part_d = part_d^
        self.part_h = part_h
        self.n_rows = n_rows
        self.ship_h = None
        self.disc_h = None
        self.ext_h = None
        self.qty_h = None


# Pin the 4 columns (ship int32, disc/ext/qty int64) resident. 0 == failure.
# `timing` != 0 -> print device-side ctx/alloc/copy sub-phase timings to stderr.
@export("mojo_q6_pin")
def mojo_q6_pin(
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    qty: UnsafePointer[Int64, ImmutAnyOrigin],
    n_rows: Int,
    timing: Int32 = 0,
) abi("C") -> Int:
    try:
        var t0 = perf_counter_ns()
        var ctx = shared_device_context()
        var tctx = (
            perf_counter_ns()
        )  # DeviceContext() done (shared: ~0 after LOAD)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var qty_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](Q6_NBLOCKS)
        ctx.synchronize()
        var t1 = perf_counter_ns()  # ctx + buffer alloc done
        ctx.enqueue_copy(ship_d, ship)
        ctx.enqueue_copy(disc_d, disc)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(qty_d, qty)
        ctx.synchronize()
        var t2 = perf_counter_ns()  # host->device copies done
        if timing != 0:
            print(
                "[q6-pin baseline mojo] DeviceContext()=",
                Float64(tctx - t0) / 1.0e6,
                "ms  buf_alloc=",
                Float64(t1 - tctx) / 1.0e6,
                "ms  h2d_copy=",
                Float64(t2 - t1) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        var part_h = alloc[Int64](Q6_NBLOCKS)
        var p = alloc[Q6State](1)
        p.init_pointee_move(
            Q6State(
                ctx^, ship_d^, disc_d^, ext_d^, qty_d^, part_d^, part_h, n_rows
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# ---------------------------------------------------------------------------
# Pinned-HostBuffer staging pin (the (b)+(c) collapse). Mojo allocates ctx + 5
# resident DeviceBuffers + 4 *pinned* HostBuffers (alloc_host_pinned) sized to
# n_rows, returns the opaque Q6State* handle and the 4 pinned host pointers. C++
# memcpys each fetched chunk straight into those pinned pointers (pre-sized -> no
# realloc, no intermediate std::vector). Then mojo_q6_pin_upload does ONE
# enqueue_copy(device, pinned-host) per column + synchronize. Pinned source
# memory + a single copy-per-column is the fast host->device DMA path.
#
#   mojo_q6_pin_alloc(n_rows, &ship_h,&disc_h,&ext_h,&qty_h) -> handle
#   (C++ fills the 4 pinned buffers via memcpy)
#   mojo_q6_pin_upload(handle, timing)  -> 0 ok
# The resulting Q6State has device buffers in the same place, so mojo_q6_query /
# mojo_q6_free work on it unchanged.
# ---------------------------------------------------------------------------
@export("mojo_q6_pin_alloc")
def mojo_q6_pin_alloc(
    n_rows: Int,
    ship_h_out: UnsafePointer[UnsafePointer[Int32, MutAnyOrigin], MutAnyOrigin],
    disc_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    ext_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    qty_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
) abi("C") -> Int:
    try:
        var ctx = shared_device_context()
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var qty_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](Q6_NBLOCKS)
        var ship_h = ctx.enqueue_create_host_buffer[DType.int32](n_rows)
        var disc_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var ext_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var qty_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        ctx.synchronize()
        # Hand the pinned host pointers back to C++ for the chunk memcpys.
        ship_h_out[] = ship_h.unsafe_ptr()
        disc_h_out[] = disc_h.unsafe_ptr()
        ext_h_out[] = ext_h.unsafe_ptr()
        qty_h_out[] = qty_h.unsafe_ptr()
        var part_h = alloc[Int64](Q6_NBLOCKS)
        var p = alloc[Q6State](1)
        p.init_pointee_move(
            Q6State(
                ctx^, ship_d^, disc_d^, ext_d^, qty_d^, part_d^, part_h, n_rows
            )
        )
        ref st = p[]
        st.ship_h = ship_h^
        st.disc_h = disc_h^
        st.ext_h = ext_h^
        st.qty_h = qty_h^
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q6_pin_upload")
def mojo_q6_pin_upload(
    handle: UnsafePointer[NoneType, MutAnyOrigin], timing: Int32 = 0
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        ref st = handle.bitcast[Q6State]()[]
        if not st.ship_h:
            return 2
        var t0 = perf_counter_ns()
        # One DMA copy per column straight from pinned host memory.
        st.ctx.enqueue_copy(st.ship_d, st.ship_h.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.disc_d, st.disc_h.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ext_d, st.ext_h.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.qty_d, st.qty_h.value().unsafe_ptr())
        st.ctx.synchronize()
        var t1 = perf_counter_ns()
        if timing != 0:
            print(
                "[q6-pin pinned mojo] h2d_copy(pinned)=",
                Float64(t1 - t0) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        return 0
    except:
        return 3


# ---------------------------------------------------------------------------
# Streaming pin (Option B, Mojo-forward): the C++ side streams the result and
# hands each chunk's raw column pointers straight here; we copy them into the
# resident DeviceBuffers at a running offset, skipping the materialized
# collection (copy 1) and the std::vector intermediates (copy 2). The resulting
# Q6State has the exact same layout as mojo_q6_pin's, so mojo_q6_query /
# mojo_q6_free work on it unchanged.
#
#   mojo_q6_pin_begin(n_rows)            -> handle (allocs ctx + 5 buffers)
#   mojo_q6_pin_chunk(handle, ship, disc, ext, qty, n, offset)  -> 0 ok
#   mojo_q6_pin_end(handle)              -> 0 ok  (final synchronize)
# ---------------------------------------------------------------------------
@export("mojo_q6_pin_begin")
def mojo_q6_pin_begin(n_rows: Int) abi("C") -> Int:
    try:
        var ctx = DeviceContext()
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var qty_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](Q6_NBLOCKS)
        ctx.synchronize()
        var part_h = alloc[Int64](Q6_NBLOCKS)
        var p = alloc[Q6State](1)
        p.init_pointee_move(
            Q6State(
                ctx^, ship_d^, disc_d^, ext_d^, qty_d^, part_d^, part_h, n_rows
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q6_pin_chunk")
def mojo_q6_pin_chunk(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    qty: UnsafePointer[Int64, ImmutAnyOrigin],
    n: Int,
    offset: Int,
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        ref st = handle.bitcast[Q6State]()[]
        # Device sub-buffers starting at `offset` (owning=False: views into the
        # resident buffers). enqueue_copy stages n elements straight from the
        # DuckDB chunk's flat vector data into device memory at the offset.
        var ship_sub = DeviceBuffer(
            st.ctx, st.ship_d.unsafe_ptr() + offset, n, owning=False
        )
        var disc_sub = DeviceBuffer(
            st.ctx, st.disc_d.unsafe_ptr() + offset, n, owning=False
        )
        var ext_sub = DeviceBuffer(
            st.ctx, st.ext_d.unsafe_ptr() + offset, n, owning=False
        )
        var qty_sub = DeviceBuffer(
            st.ctx, st.qty_d.unsafe_ptr() + offset, n, owning=False
        )
        st.ctx.enqueue_copy(ship_sub, ship)
        st.ctx.enqueue_copy(disc_sub, disc)
        st.ctx.enqueue_copy(ext_sub, ext)
        st.ctx.enqueue_copy(qty_sub, qty)
        return 0
    except:
        return 2


@export("mojo_q6_pin_end")
def mojo_q6_pin_end(
    handle: UnsafePointer[NoneType, MutAnyOrigin]
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        handle.bitcast[Q6State]()[].ctx.synchronize()
        return 0
    except:
        return 2


# Run one Q6 query over the pinned columns. Writes the int128 result as two
# int64 limbs into out[0]=low, out[1]=high (matching duckdb hugeint_t).
@export("mojo_q6_query")
def mojo_q6_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship_lo: Int32,
    ship_hi: Int32,
    disc_lo: Int64,
    disc_hi: Int64,
    qty_hi: Int64,
    out_ptr: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q6State]()
        ref st = s[]
        st.ctx.enqueue_function[q6_kernel](
            st.ship_d,
            st.disc_d,
            st.ext_d,
            st.qty_d,
            st.part_d,
            st.n_rows,
            ship_lo,
            ship_hi,
            disc_lo,
            disc_hi,
            qty_hi,
            grid_dim=Q6_NBLOCKS,
            block_dim=32,
        )
        var part_sub = DeviceBuffer(
            st.ctx, st.part_d.unsafe_ptr(), Q6_NBLOCKS, owning=False
        )
        st.ctx.enqueue_copy(st.part_h, part_sub)
        st.ctx.synchronize()
        var acc = Int128(0)
        for b in range(Q6_NBLOCKS):
            acc += Int128(st.part_h[b])
        out_ptr[0] = acc.cast[DType.int64]()  # low 64 bits
        out_ptr[1] = (acc >> 64).cast[DType.int64]()  # high 64 bits
        return 0
    except:
        return 3


@export("mojo_q6_free")
def mojo_q6_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q6State]()
    p[].part_h.free()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q1 engine: grouped exact-decimal aggregation.
#
# Group ids (0..n_groups-1) are pre-assigned on the host from the single-char
# l_returnflag/l_linestatus. Columns are DECIMAL(15,2)->int64 (scale 2) except
# l_shipdate DATE->int32 (days). Per group the kernel accumulates 6 integer
# quantities (all exact):
#     [0] count
#     [1] sum(l_quantity)                            scale 2
#     [2] sum(l_extendedprice)                       scale 2
#     [3] sum(l_discount)                            scale 2
#     [4] sum(ext_raw*(100-disc_raw))                scale 4  (sum_disc_price)
#     [5] sum(ext_raw*(100-disc_raw)*(100+tax_raw))  scale 6  (sum_charge)
# Per-row charge ~1.1e11, per-block partial fits int64; cross-block reduction is
# int128 on the host -> bit-exact.
#
# One warp (32 lanes) per block. Each lane keeps private int64 accumulators
# [NGROUPS][6], lane-strided over rows; warp.sum reduces each (group,metric)
# across lanes; lane 0 writes per-block partials[(block*NGROUPS+g)*6+m].
# ===-------------------------------------------------------------------===#
comptime Q1_NBLOCKS = 4096
comptime Q1_NGROUPS = 8  # cap (Q1 uses 4)
comptime Q1_NMETRICS = 6


def q1_kernel(
    gid: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    qty: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    tax: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_hi: Int32,  # filter: l_shipdate <= ship_hi (inclusive)
):
    var lane = Int(thread_idx.x)
    var stride = Q1_NBLOCKS * 32
    var acc = InlineArray[Int64, Q1_NGROUPS * Q1_NMETRICS](fill=0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd <= ship_hi:
            var g = Int(gid[i])
            var e = ext[i]
            var d = disc[i]
            var t = tax[i]
            var ed = e * (Int64(100) - d)  # scale 4
            var charge = ed * (Int64(100) + t)  # scale 6
            var b = g * Q1_NMETRICS
            acc[b + 0] += 1
            acc[b + 1] += qty[i]
            acc[b + 2] += e
            acc[b + 3] += d
            acc[b + 4] += ed
            acc[b + 5] += charge
        i += stride
    var blk = Int(block_idx.x)
    for g in range(Q1_NGROUPS):
        for m in range(Q1_NMETRICS):
            var s = warp.sum(acc[g * Q1_NMETRICS + m])
            if lane == 0:
                partials[(blk * Q1_NGROUPS + g) * Q1_NMETRICS + m] = s


struct Q1State(Movable):
    var ctx: DeviceContext
    var gid_d: DeviceBuffer[DType.uint8]
    var qty_d: DeviceBuffer[DType.int64]
    var ext_d: DeviceBuffer[DType.int64]
    var disc_d: DeviceBuffer[DType.int64]
    var tax_d: DeviceBuffer[DType.int64]
    var ship_d: DeviceBuffer[DType.int32]
    var part_d: DeviceBuffer[DType.int64]
    var part_h: UnsafePointer[Int64, MutAnyOrigin]
    var n_rows: Int
    var n_groups: Int
    # Pinned host-staging buffers for the 6 per-row arrays (pinned pin path). gid
    # is computed per row on the host (from l_returnflag/l_linestatus) straight
    # into the pinned uint8 buffer; the rest are direct chunk memcpys.
    var gid_hb: Optional[HostBuffer[DType.uint8]]
    var qty_hb: Optional[HostBuffer[DType.int64]]
    var ext_hb: Optional[HostBuffer[DType.int64]]
    var disc_hb: Optional[HostBuffer[DType.int64]]
    var tax_hb: Optional[HostBuffer[DType.int64]]
    var ship_hb: Optional[HostBuffer[DType.int32]]

    def __init__(
        out self,
        var ctx: DeviceContext,
        var gid_d: DeviceBuffer[DType.uint8],
        var qty_d: DeviceBuffer[DType.int64],
        var ext_d: DeviceBuffer[DType.int64],
        var disc_d: DeviceBuffer[DType.int64],
        var tax_d: DeviceBuffer[DType.int64],
        var ship_d: DeviceBuffer[DType.int32],
        var part_d: DeviceBuffer[DType.int64],
        part_h: UnsafePointer[Int64, MutAnyOrigin],
        n_rows: Int,
        n_groups: Int,
    ):
        self.ctx = ctx^
        self.gid_d = gid_d^
        self.qty_d = qty_d^
        self.ext_d = ext_d^
        self.disc_d = disc_d^
        self.tax_d = tax_d^
        self.ship_d = ship_d^
        self.part_d = part_d^
        self.part_h = part_h
        self.n_rows = n_rows
        self.n_groups = n_groups
        self.gid_hb = None
        self.qty_hb = None
        self.ext_hb = None
        self.disc_hb = None
        self.tax_hb = None
        self.ship_hb = None


# Pin the 6 columns resident. n_groups must be <= Q1_NGROUPS. 0 == failure.
@export("mojo_q1_pin")
def mojo_q1_pin(
    gid: UnsafePointer[UInt8, ImmutAnyOrigin],
    qty: UnsafePointer[Int64, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    tax: UnsafePointer[Int64, ImmutAnyOrigin],
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    n_rows: Int,
    n_groups: Int,
) abi("C") -> Int:
    if n_groups > Q1_NGROUPS or n_groups <= 0:
        return 0
    try:
        var ctx = shared_device_context()
        var gid_d = ctx.enqueue_create_buffer[DType.uint8](n_rows)
        var qty_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var tax_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](
            Q1_NBLOCKS * Q1_NGROUPS * Q1_NMETRICS
        )
        ctx.synchronize()
        ctx.enqueue_copy(gid_d, gid)
        ctx.enqueue_copy(qty_d, qty)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(disc_d, disc)
        ctx.enqueue_copy(tax_d, tax)
        ctx.enqueue_copy(ship_d, ship)
        ctx.synchronize()
        var part_h = alloc[Int64](Q1_NBLOCKS * Q1_NGROUPS * Q1_NMETRICS)
        var p = alloc[Q1State](1)
        p.init_pointee_move(
            Q1State(
                ctx^,
                gid_d^,
                qty_d^,
                ext_d^,
                disc_d^,
                tax_d^,
                ship_d^,
                part_d^,
                part_h,
                n_rows,
                n_groups,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Pinned-HostBuffer staging pin for Q1 (mirrors mojo_q6_pin_alloc). Allocates the
# 6 resident DeviceBuffers + 6 pinned HostBuffers sized to n_rows and returns the
# pinned host pointers. n_groups is NOT known yet (the host discovers it while
# computing gid during the chunk fill) -> it is supplied later via
# mojo_q1_pin_upload. C++ computes gid per row into the pinned uint8 buffer and
# memcpys the 5 numeric/date columns into their pinned buffers; then pin_upload
# does one enqueue_copy(device, pinned-host) per column.
@export("mojo_q1_pin_alloc")
def mojo_q1_pin_alloc(
    n_rows: Int,
    gid_h_out: UnsafePointer[UnsafePointer[UInt8, MutAnyOrigin], MutAnyOrigin],
    qty_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    ext_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    disc_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    tax_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    ship_h_out: UnsafePointer[UnsafePointer[Int32, MutAnyOrigin], MutAnyOrigin],
) abi("C") -> Int:
    try:
        var ctx = shared_device_context()
        var gid_d = ctx.enqueue_create_buffer[DType.uint8](n_rows)
        var qty_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var tax_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](
            Q1_NBLOCKS * Q1_NGROUPS * Q1_NMETRICS
        )
        var gid_h = ctx.enqueue_create_host_buffer[DType.uint8](n_rows)
        var qty_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var ext_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var disc_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var tax_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var ship_h = ctx.enqueue_create_host_buffer[DType.int32](n_rows)
        ctx.synchronize()
        gid_h_out[] = gid_h.unsafe_ptr()
        qty_h_out[] = qty_h.unsafe_ptr()
        ext_h_out[] = ext_h.unsafe_ptr()
        disc_h_out[] = disc_h.unsafe_ptr()
        tax_h_out[] = tax_h.unsafe_ptr()
        ship_h_out[] = ship_h.unsafe_ptr()
        var part_h = alloc[Int64](Q1_NBLOCKS * Q1_NGROUPS * Q1_NMETRICS)
        var p = alloc[Q1State](1)
        p.init_pointee_move(
            Q1State(
                ctx^,
                gid_d^,
                qty_d^,
                ext_d^,
                disc_d^,
                tax_d^,
                ship_d^,
                part_d^,
                part_h,
                n_rows,
                0,  # n_groups: set later by mojo_q1_pin_upload
            )
        )
        ref st = p[]
        st.gid_hb = gid_h^
        st.qty_hb = qty_h^
        st.ext_hb = ext_h^
        st.disc_hb = disc_h^
        st.tax_hb = tax_h^
        st.ship_hb = ship_h^
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q1_pin_upload")
def mojo_q1_pin_upload(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    n_groups: Int,
    timing: Int32 = 0,
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    if n_groups <= 0 or n_groups > Q1_NGROUPS:
        return 4
    try:
        ref st = handle.bitcast[Q1State]()[]
        if not st.gid_hb:
            return 2
        st.n_groups = n_groups
        var t0 = perf_counter_ns()
        st.ctx.enqueue_copy(st.gid_d, st.gid_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.qty_d, st.qty_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ext_d, st.ext_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.disc_d, st.disc_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.tax_d, st.tax_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ship_d, st.ship_hb.value().unsafe_ptr())
        st.ctx.synchronize()
        var t1 = perf_counter_ns()
        if timing != 0:
            print(
                "[q1-pin pinned mojo] h2d_copy(pinned)=",
                Float64(t1 - t0) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        return 0
    except:
        return 3


# Run one Q1 query over the pinned columns with filter l_shipdate <= ship_hi.
# Writes per group, into `out`, 6 int128 quantities as 2 int64 limbs each
# (low, high), laid out as out[group*12 + metric*2 + {0,1}] where:
#   metric 0 = count, 1 = Sqty, 2 = Sext, 3 = Sdisc,
#   metric 4 = Sdisc_price (scale 4), metric 5 = Scharge (scale 6).
# `out` must hold n_groups * 12 int64 values.
@export("mojo_q1_query")
def mojo_q1_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship_hi: Int32,
    out_ptr: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q1State]()
        ref st = s[]
        st.ctx.enqueue_function[q1_kernel](
            st.gid_d,
            st.qty_d,
            st.ext_d,
            st.disc_d,
            st.tax_d,
            st.ship_d,
            st.part_d,
            st.n_rows,
            ship_hi,
            grid_dim=Q1_NBLOCKS,
            block_dim=32,
        )
        var npart = Q1_NBLOCKS * Q1_NGROUPS * Q1_NMETRICS
        var part_sub = DeviceBuffer(
            st.ctx, st.part_d.unsafe_ptr(), npart, owning=False
        )
        st.ctx.enqueue_copy(st.part_h, part_sub)
        st.ctx.synchronize()
        # host int128 reduction across blocks, per (group, metric)
        for g in range(st.n_groups):
            for m in range(Q1_NMETRICS):
                var acc = Int128(0)
                for b in range(Q1_NBLOCKS):
                    acc += Int128(
                        st.part_h[(b * Q1_NGROUPS + g) * Q1_NMETRICS + m]
                    )
                var base = g * (Q1_NMETRICS * 2) + m * 2
                out_ptr[base + 0] = acc.cast[DType.int64]()
                out_ptr[base + 1] = (acc >> 64).cast[DType.int64]()
        return 0
    except:
        return 3


@export("mojo_q1_free")
def mojo_q1_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q1State]()
    p[].part_h.free()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q14 engine: GPU hash-probe FK join (lineitem -> part) + probe-side
# exact-decimal aggregation.
#
# The build side (part) is small: the HOST builds an open-addressing
# (linear-probing) hash table keyed by p_partkey (int64) with a 1-byte payload
# is_promo (p_type LIKE 'PROMO%'). Empty slot = key 0 (TPC-H partkeys start at
# 1). The C++ side passes the prebuilt keys[] + promo[] arrays (size = pow2);
# we just pin them resident alongside the 4 probe columns.
#
# The probe runs on the GPU: each thread strides over filtered lineitem rows;
# for a passing row (l_shipdate in [lo,hi)), it hashes l_partkey, linear-probes
# the resident table, reads is_promo, computes prod = ext_raw*(100-disc_raw)
# (scale 4 int64), and accumulates total (+ promo if the part is promo). Per-row
# product ~1e9, per-block partial fits int64; the cross-block reduction is
# int128 on the host -> bit-exact vs DuckDB's int128 sum.
# ===-------------------------------------------------------------------===#
comptime Q14_NBLOCKS = 4096  # one warp (32 lanes) per block


# splitmix-ish 64-bit integer hash; identical math on host (C++) and device.
def q14_hash(k: Int64) -> UInt64:
    var x = UInt64(k)
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
    x = (x ^ (x >> 27)) * 0x94D049BB133111EB
    x = x ^ (x >> 31)
    return x


def q14_kernel(
    ht_keys: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ht_promo: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    ht_mask: UInt64,
    lpartkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_total: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_promo: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_miss: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_lo: Int32,
    ship_hi: Int32,
):
    var lane = Int(thread_idx.x)
    var stride = Q14_NBLOCKS * 32
    var total = Int64(0)
    var promo = Int64(0)
    var miss = Int64(0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd >= ship_lo and sd < ship_hi:
            var key = lpartkey[i]
            var h = q14_hash(key) & ht_mask
            var found = False
            var is_promo = UInt8(0)
            while ht_keys[Int(h)] != 0:
                if ht_keys[Int(h)] == key:
                    is_promo = ht_promo[Int(h)]
                    found = True
                    break
                h = (h + 1) & ht_mask
            if found:
                var prod = ext[i] * (Int64(100) - disc[i])  # scale 4
                total += prod
                if is_promo != 0:
                    promo += prod
            else:
                miss += 1
        i += stride
    var st = warp.sum(total)
    var sp = warp.sum(promo)
    var sm = warp.sum(miss)
    if lane == 0:
        part_total[Int(block_idx.x)] = st
        part_promo[Int(block_idx.x)] = sp
        part_miss[Int(block_idx.x)] = sm


struct Q14State(Movable):
    var ctx: DeviceContext
    var keys_d: DeviceBuffer[DType.int64]
    var promo_d: DeviceBuffer[DType.uint8]
    var lpk_d: DeviceBuffer[DType.int64]
    var ship_d: DeviceBuffer[DType.int32]
    var ext_d: DeviceBuffer[DType.int64]
    var disc_d: DeviceBuffer[DType.int64]
    var pt_d: DeviceBuffer[DType.int64]
    var pp_d: DeviceBuffer[DType.int64]
    var pm_d: DeviceBuffer[DType.int64]
    var pt_h: UnsafePointer[Int64, MutAnyOrigin]
    var pp_h: UnsafePointer[Int64, MutAnyOrigin]
    var pm_h: UnsafePointer[Int64, MutAnyOrigin]
    var ht_mask: UInt64
    var n_rows: Int
    # Pinned host-staging buffers for the 4 large probe columns (only populated
    # by the pinned pin path; held here so they outlive fill+upload, freed with
    # the state).
    var lpk_hb: Optional[HostBuffer[DType.int64]]
    var ship_hb: Optional[HostBuffer[DType.int32]]
    var ext_hb: Optional[HostBuffer[DType.int64]]
    var disc_hb: Optional[HostBuffer[DType.int64]]

    def __init__(
        out self,
        var ctx: DeviceContext,
        var keys_d: DeviceBuffer[DType.int64],
        var promo_d: DeviceBuffer[DType.uint8],
        var lpk_d: DeviceBuffer[DType.int64],
        var ship_d: DeviceBuffer[DType.int32],
        var ext_d: DeviceBuffer[DType.int64],
        var disc_d: DeviceBuffer[DType.int64],
        var pt_d: DeviceBuffer[DType.int64],
        var pp_d: DeviceBuffer[DType.int64],
        var pm_d: DeviceBuffer[DType.int64],
        pt_h: UnsafePointer[Int64, MutAnyOrigin],
        pp_h: UnsafePointer[Int64, MutAnyOrigin],
        pm_h: UnsafePointer[Int64, MutAnyOrigin],
        ht_mask: UInt64,
        n_rows: Int,
    ):
        self.ctx = ctx^
        self.keys_d = keys_d^
        self.promo_d = promo_d^
        self.lpk_d = lpk_d^
        self.ship_d = ship_d^
        self.ext_d = ext_d^
        self.disc_d = disc_d^
        self.pt_d = pt_d^
        self.pp_d = pp_d^
        self.pm_d = pm_d^
        self.pt_h = pt_h
        self.pp_h = pp_h
        self.pm_h = pm_h
        self.ht_mask = ht_mask
        self.n_rows = n_rows
        self.lpk_hb = None
        self.ship_hb = None
        self.ext_hb = None
        self.disc_hb = None


# Pin the host-built hash table (keys+promo, size ht_size = pow2) + the 4 probe
# columns resident. ht_size must be a power of two. 0 == failure.
@export("mojo_q14_pin")
def mojo_q14_pin(
    ht_keys: UnsafePointer[Int64, ImmutAnyOrigin],
    ht_promo: UnsafePointer[UInt8, ImmutAnyOrigin],
    ht_size: Int,
    lpartkey: UnsafePointer[Int64, ImmutAnyOrigin],
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    n_rows: Int,
) abi("C") -> Int:
    # ht_size must be a power of two.
    if ht_size <= 0 or (ht_size & (ht_size - 1)) != 0:
        return 0
    try:
        var ctx = shared_device_context()
        var keys_d = ctx.enqueue_create_buffer[DType.int64](ht_size)
        var promo_d = ctx.enqueue_create_buffer[DType.uint8](ht_size)
        var lpk_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var pt_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        var pp_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        var pm_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        ctx.synchronize()
        ctx.enqueue_copy(keys_d, ht_keys)
        ctx.enqueue_copy(promo_d, ht_promo)
        ctx.enqueue_copy(lpk_d, lpartkey)
        ctx.enqueue_copy(ship_d, ship)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(disc_d, disc)
        ctx.synchronize()
        var pt_h = alloc[Int64](Q14_NBLOCKS)
        var pp_h = alloc[Int64](Q14_NBLOCKS)
        var pm_h = alloc[Int64](Q14_NBLOCKS)
        var p = alloc[Q14State](1)
        p.init_pointee_move(
            Q14State(
                ctx^,
                keys_d^,
                promo_d^,
                lpk_d^,
                ship_d^,
                ext_d^,
                disc_d^,
                pt_d^,
                pp_d^,
                pm_d^,
                pt_h,
                pp_h,
                pm_h,
                UInt64(ht_size - 1),
                n_rows,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Pinned-HostBuffer staging pin for Q14 (mirrors mojo_q6_pin_alloc). The small
# host-built hash table (keys+promo, size ht_size) is uploaded immediately here;
# the 4 large probe columns (lpartkey/ship/ext/disc, ~6M rows) get pinned
# HostBuffers whose pointers are returned for C++ to memcpy chunks into; then
# mojo_q14_pin_upload does one enqueue_copy(device, pinned-host) per probe column.
@export("mojo_q14_pin_alloc")
def mojo_q14_pin_alloc(
    ht_keys: UnsafePointer[Int64, ImmutAnyOrigin],
    ht_promo: UnsafePointer[UInt8, ImmutAnyOrigin],
    ht_size: Int,
    n_rows: Int,
    lpk_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    ship_h_out: UnsafePointer[UnsafePointer[Int32, MutAnyOrigin], MutAnyOrigin],
    ext_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    disc_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
) abi("C") -> Int:
    if ht_size <= 0 or (ht_size & (ht_size - 1)) != 0:
        return 0
    try:
        var ctx = shared_device_context()
        var keys_d = ctx.enqueue_create_buffer[DType.int64](ht_size)
        var promo_d = ctx.enqueue_create_buffer[DType.uint8](ht_size)
        var lpk_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var pt_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        var pp_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        var pm_d = ctx.enqueue_create_buffer[DType.int64](Q14_NBLOCKS)
        var lpk_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var ship_h = ctx.enqueue_create_host_buffer[DType.int32](n_rows)
        var ext_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var disc_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        # Upload the small hash table immediately (it is already host-built).
        ctx.enqueue_copy(keys_d, ht_keys)
        ctx.enqueue_copy(promo_d, ht_promo)
        ctx.synchronize()
        lpk_h_out[] = lpk_h.unsafe_ptr()
        ship_h_out[] = ship_h.unsafe_ptr()
        ext_h_out[] = ext_h.unsafe_ptr()
        disc_h_out[] = disc_h.unsafe_ptr()
        var pt_h = alloc[Int64](Q14_NBLOCKS)
        var pp_h = alloc[Int64](Q14_NBLOCKS)
        var pm_h = alloc[Int64](Q14_NBLOCKS)
        var p = alloc[Q14State](1)
        p.init_pointee_move(
            Q14State(
                ctx^,
                keys_d^,
                promo_d^,
                lpk_d^,
                ship_d^,
                ext_d^,
                disc_d^,
                pt_d^,
                pp_d^,
                pm_d^,
                pt_h,
                pp_h,
                pm_h,
                UInt64(ht_size - 1),
                n_rows,
            )
        )
        ref st = p[]
        st.lpk_hb = lpk_h^
        st.ship_hb = ship_h^
        st.ext_hb = ext_h^
        st.disc_hb = disc_h^
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q14_pin_upload")
def mojo_q14_pin_upload(
    handle: UnsafePointer[NoneType, MutAnyOrigin], timing: Int32 = 0
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        ref st = handle.bitcast[Q14State]()[]
        if not st.lpk_hb:
            return 2
        var t0 = perf_counter_ns()
        st.ctx.enqueue_copy(st.lpk_d, st.lpk_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ship_d, st.ship_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ext_d, st.ext_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.disc_d, st.disc_hb.value().unsafe_ptr())
        st.ctx.synchronize()
        var t1 = perf_counter_ns()
        if timing != 0:
            print(
                "[q14-pin pinned mojo] h2d_copy(pinned)=",
                Float64(t1 - t0) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        return 0
    except:
        return 3


# Run one Q14 probe over the pinned table+columns with filter
# l_shipdate in [ship_lo, ship_hi). Writes each int128 sum as two int64 limbs:
#   out_total[0]=low, out_total[1]=high ; out_promo[0]=low, out_promo[1]=high.
# Returns 0 on success; nonzero rc on GPU error; rc 4 if any probe miss occurred.
@export("mojo_q14_query")
def mojo_q14_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship_lo: Int32,
    ship_hi: Int32,
    out_total: UnsafePointer[Int64, MutAnyOrigin],
    out_promo: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q14State]()
        ref st = s[]
        st.ctx.enqueue_function[q14_kernel](
            st.keys_d,
            st.promo_d,
            st.ht_mask,
            st.lpk_d,
            st.ship_d,
            st.ext_d,
            st.disc_d,
            st.pt_d,
            st.pp_d,
            st.pm_d,
            st.n_rows,
            ship_lo,
            ship_hi,
            grid_dim=Q14_NBLOCKS,
            block_dim=32,
        )
        var pt_sub = DeviceBuffer(
            st.ctx, st.pt_d.unsafe_ptr(), Q14_NBLOCKS, owning=False
        )
        var pp_sub = DeviceBuffer(
            st.ctx, st.pp_d.unsafe_ptr(), Q14_NBLOCKS, owning=False
        )
        var pm_sub = DeviceBuffer(
            st.ctx, st.pm_d.unsafe_ptr(), Q14_NBLOCKS, owning=False
        )
        st.ctx.enqueue_copy(st.pt_h, pt_sub)
        st.ctx.enqueue_copy(st.pp_h, pp_sub)
        st.ctx.enqueue_copy(st.pm_h, pm_sub)
        st.ctx.synchronize()
        var acc_total = Int128(0)
        var acc_promo = Int128(0)
        var acc_miss = Int128(0)
        for b in range(Q14_NBLOCKS):
            acc_total += Int128(st.pt_h[b])
            acc_promo += Int128(st.pp_h[b])
            acc_miss += Int128(st.pm_h[b])
        out_total[0] = acc_total.cast[DType.int64]()
        out_total[1] = (acc_total >> 64).cast[DType.int64]()
        out_promo[0] = acc_promo.cast[DType.int64]()
        out_promo[1] = (acc_promo >> 64).cast[DType.int64]()
        if acc_miss != Int128(0):
            return 4  # probe miss (FK violation) -> caller should fall back
        return 0
    except:
        return 3


@export("mojo_q14_free")
def mojo_q14_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q14State]()
    p[].pt_h.free()
    p[].pp_h.free()
    p[].pm_h.free()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q3 engine: GPU multi-way-join probe over lineitem + per-order revenue
# accumulation.
#
# The 3-way FK join customer <- orders <- lineitem is collapsed on the HOST
# (C++ EnsureQ3Pinned) into a single dense per-order pass flag:
#     order_pass[o_orderkey] = (c_mktsegment[o_custkey] == seg)
#                              AND (o_orderdate < o_cutoff)
# Dense array indexed by o_orderkey, size = max_orderkey+1 (TPC-H keys are
# bounded; SF1 max o_orderkey ~ 6,000,000 -> ~6MB uint8 + ~48MB int64 accum).
# o_orderdate / o_shippriority stay on the host for the final attach.
#
# The GPU probe over lineitem: for each row with l_shipdate > l_cutoff AND
# order_pass[l_orderkey], compute rev = ext_raw*(100-disc_raw) (scale-4 int64)
# and write it to a resident per-row revenue buffer rev_row[i] (0 otherwise).
# The GPU does the join-probe (dense dimension lookup), the shipdate filter, and
# the exact decimal product -- the expensive part.
#
# Why no GPU per-order atomic accumulation: the Apple GPU (Metal) supports
# 32-bit atomics but NOT 64-bit atomics -- a global int64 Atomic.fetch_add fails
# at GPU pipeline-state creation. So mojo_q3_query, after the kernel, performs
# the per-order sum on the HOST (one O(n_rows) scan) into the dense int64
# accumulator using the host copy of l_orderkey kept in the state. Each order
# has <=7 lines so its revenue fits int64 exactly -- no int128 needed and the
# result is bit-exact vs DuckDB's int128 sum (verified in q3_kernel_test.mojo).
# ===-------------------------------------------------------------------===#
comptime Q3_NBLOCKS = 4096  # one warp (32 lanes) per block


def q3_kernel(
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    lorderkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    rev_row: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_cutoff: Int32,  # l_shipdate > ship_cutoff (strict)
):
    var lane = Int(thread_idx.x)
    var stride = Q3_NBLOCKS * 32
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var r = Int64(0)
        var sd = ship[i]
        if sd > ship_cutoff:
            var ok = lorderkey[i]
            if order_pass[Int(ok)] != 0:
                r = ext[i] * (Int64(100) - disc[i])  # scale 4
        rev_row[i] = r
        i += stride


struct Q3State(Movable):
    var ctx: DeviceContext
    var op_d: DeviceBuffer[DType.uint8]  # dense order_pass, size max_orderkey+1
    var lok_d: DeviceBuffer[DType.int64]  # l_orderkey (probe)
    var ship_d: DeviceBuffer[DType.int32]
    var ext_d: DeviceBuffer[DType.int64]
    var disc_d: DeviceBuffer[DType.int64]
    var rev_d: DeviceBuffer[DType.int64]  # per-row revenue (resident scratch)
    var lok_h: UnsafePointer[Int64, MutAnyOrigin]  # host copy of l_orderkey
    var rev_h: UnsafePointer[Int64, MutAnyOrigin]  # host per-row revenue buffer
    var n_rows: Int
    var n_slots: Int  # max_orderkey+1

    def __init__(
        out self,
        var ctx: DeviceContext,
        var op_d: DeviceBuffer[DType.uint8],
        var lok_d: DeviceBuffer[DType.int64],
        var ship_d: DeviceBuffer[DType.int32],
        var ext_d: DeviceBuffer[DType.int64],
        var disc_d: DeviceBuffer[DType.int64],
        var rev_d: DeviceBuffer[DType.int64],
        lok_h: UnsafePointer[Int64, MutAnyOrigin],
        rev_h: UnsafePointer[Int64, MutAnyOrigin],
        n_rows: Int,
        n_slots: Int,
    ):
        self.ctx = ctx^
        self.op_d = op_d^
        self.lok_d = lok_d^
        self.ship_d = ship_d^
        self.ext_d = ext_d^
        self.disc_d = disc_d^
        self.rev_d = rev_d^
        self.lok_h = lok_h
        self.rev_h = rev_h
        self.n_rows = n_rows
        self.n_slots = n_slots


# Pin the dense order_pass flags + the 4 probe columns resident. The host keeps
# a copy of l_orderkey for the per-order sum. max_orderkey is the max o_orderkey
# (the dense accumulator / order_pass are sized max_orderkey+1). 0 == failure.
@export("mojo_q3_pin")
def mojo_q3_pin(
    order_pass: UnsafePointer[UInt8, ImmutAnyOrigin],
    lorderkey: UnsafePointer[Int64, ImmutAnyOrigin],
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    n_rows: Int,
    max_orderkey: Int,
) abi("C") -> Int:
    if n_rows <= 0 or max_orderkey <= 0:
        return 0
    try:
        var n_slots = max_orderkey + 1
        var ctx = DeviceContext()
        var op_d = ctx.enqueue_create_buffer[DType.uint8](n_slots)
        var lok_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var rev_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        ctx.synchronize()
        ctx.enqueue_copy(op_d, order_pass)
        ctx.enqueue_copy(lok_d, lorderkey)
        ctx.enqueue_copy(ship_d, ship)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(disc_d, disc)
        ctx.synchronize()
        var lok_h = alloc[Int64](n_rows)
        for i in range(n_rows):
            lok_h[i] = lorderkey[i]
        var rev_h = alloc[Int64](n_rows)
        var p = alloc[Q3State](1)
        p.init_pointee_move(
            Q3State(
                ctx^,
                op_d^,
                lok_d^,
                ship_d^,
                ext_d^,
                disc_d^,
                rev_d^,
                lok_h,
                rev_h,
                n_rows,
                n_slots,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Run one Q3 probe over the pinned columns with filter l_shipdate > ship_cutoff.
# Zeroes out_revenue (size n_slots = max_orderkey+1), runs the GPU probe to
# compute per-row revenue, then sums per l_orderkey on the host into
# out_revenue[orderkey]. Returns 0 on success; nonzero rc on error.
@export("mojo_q3_query")
def mojo_q3_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship_cutoff: Int32,
    out_revenue: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q3State]()
        ref st = s[]
        st.ctx.enqueue_function[q3_kernel](
            st.op_d,
            st.lok_d,
            st.ship_d,
            st.ext_d,
            st.disc_d,
            st.rev_d,
            st.n_rows,
            ship_cutoff,
            grid_dim=Q3_NBLOCKS,
            block_dim=32,
        )
        st.ctx.enqueue_copy(st.rev_h, st.rev_d)
        st.ctx.synchronize()
        for k in range(st.n_slots):
            out_revenue[k] = 0
        for i in range(st.n_rows):
            var r = st.rev_h[i]
            if r != 0:
                out_revenue[Int(st.lok_h[i])] += r
        return 0
    except:
        return 3


@export("mojo_q3_free")
def mojo_q3_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q3State]()
    p[].lok_h.free()
    p[].rev_h.free()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q3 engine v2: on-GPU high-cardinality group-by via sort + segmented
# reduction (NO int64 atomics, NO host per-order sum loop).
#
# The host (C++ EnsureQ3Pinned) materializes lineitem ORDERED BY l_orderkey, so
# all rows of one order are contiguous. From the sorted l_orderkey it builds the
# distinct orderkey list seg_key[s] and seg_offset[s] (first row of segment s;
# seg_offset[n_seg] = n_rows) in one linear pass, and the dense
# order_pass[o_orderkey] flag as before. All pinned resident.
#
# The GPU group-by: ONE WARP (block_dim=32) per order segment, grid_dim=n_seg.
# Each warp checks order_pass[seg_key[s]] once, then its 32 lanes stride over the
# segment's rows [seg_offset[s], seg_offset[s+1]) applying l_shipdate>cutoff and
# computing rev=ext*(100-disc) (scale-4 int64); warp.sum reduces; lane 0 writes
# seg_rev[s]. Each segment is owned by exactly one warp -> no atomics, no
# cross-block merge. An order has <=7 lines so per-order revenue fits int64
# exactly -> bit-exact (verified in bench/q3_groupby_test.mojo).
#
# query2 returns one int64 revenue per segment (indexed by s); the host maps back
# to orderkey via the seg_key list it kept. The readback is n_seg int64 (~12MB at
# SF1), NOT the 6M per-row values, and there is NO host summation loop.
# ===-------------------------------------------------------------------===#
def q3_seg_kernel(
    seg_offset: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],  # n_seg+1
    seg_key: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],  # n_seg
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    seg_rev: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],  # n_seg out
    n_seg: Int,
    ship_cutoff: Int32,  # l_shipdate > ship_cutoff (strict)
):
    var s = Int(block_idx.x)
    if s >= n_seg:
        return
    var lane = Int(thread_idx.x)
    if order_pass[Int(seg_key[s])] == 0:
        if lane == 0:
            seg_rev[s] = 0
        return
    var lo = Int(seg_offset[s])
    var hi = Int(seg_offset[s + 1])
    var local = Int64(0)
    var i = lo + lane
    while i < hi:
        if ship[i] > ship_cutoff:
            local += ext[i] * (Int64(100) - disc[i])  # scale 4
        i += 32
    var tot = warp.sum(local)
    if lane == 0:
        seg_rev[s] = tot


struct Q3State2(Movable):
    var ctx: DeviceContext
    var soff_d: DeviceBuffer[DType.int64]  # seg_offset, size n_seg+1
    var skey_d: DeviceBuffer[
        DType.int64
    ]  # seg_key (distinct orderkeys), size n_seg
    var op_d: DeviceBuffer[DType.uint8]  # dense order_pass, size max_orderkey+1
    var ship_d: DeviceBuffer[DType.int32]  # sorted-by-orderkey probe columns
    var ext_d: DeviceBuffer[DType.int64]
    var disc_d: DeviceBuffer[DType.int64]
    var srev_d: DeviceBuffer[DType.int64]  # per-segment revenue out, size n_seg
    var n_rows: Int
    var n_seg: Int
    # Pinned host-staging buffers for the 3 large sorted probe columns (pinned pin
    # path). l_orderkey is consumed on the host to build the segmentation, so it
    # is NOT pinned here; ship/ext/disc are direct chunk memcpys.
    var ship_hb: Optional[HostBuffer[DType.int32]]
    var ext_hb: Optional[HostBuffer[DType.int64]]
    var disc_hb: Optional[HostBuffer[DType.int64]]

    def __init__(
        out self,
        var ctx: DeviceContext,
        var soff_d: DeviceBuffer[DType.int64],
        var skey_d: DeviceBuffer[DType.int64],
        var op_d: DeviceBuffer[DType.uint8],
        var ship_d: DeviceBuffer[DType.int32],
        var ext_d: DeviceBuffer[DType.int64],
        var disc_d: DeviceBuffer[DType.int64],
        var srev_d: DeviceBuffer[DType.int64],
        n_rows: Int,
        n_seg: Int,
    ):
        self.ctx = ctx^
        self.soff_d = soff_d^
        self.skey_d = skey_d^
        self.op_d = op_d^
        self.ship_d = ship_d^
        self.ext_d = ext_d^
        self.disc_d = disc_d^
        self.srev_d = srev_d^
        self.n_rows = n_rows
        self.n_seg = n_seg
        self.ship_hb = None
        self.ext_hb = None
        self.disc_hb = None


# Pin the dense order_pass flags + the sorted-by-orderkey probe columns + the
# segment layout (seg_offset n_seg+1, seg_key n_seg) resident. The probe columns
# (ship/ext/disc) MUST be ordered by l_orderkey to match seg_offset. 0 == failure.
@export("mojo_q3_pin2")
def mojo_q3_pin2(
    order_pass: UnsafePointer[UInt8, ImmutAnyOrigin],
    seg_offset: UnsafePointer[Int64, ImmutAnyOrigin],
    seg_key: UnsafePointer[Int64, ImmutAnyOrigin],
    ship: UnsafePointer[Int32, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    n_rows: Int,
    n_seg: Int,
    max_orderkey: Int,
) abi("C") -> Int:
    if n_rows <= 0 or n_seg <= 0 or max_orderkey <= 0:
        return 0
    try:
        var n_slots = max_orderkey + 1
        var ctx = shared_device_context()
        var soff_d = ctx.enqueue_create_buffer[DType.int64](n_seg + 1)
        var skey_d = ctx.enqueue_create_buffer[DType.int64](n_seg)
        var op_d = ctx.enqueue_create_buffer[DType.uint8](n_slots)
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var srev_d = ctx.enqueue_create_buffer[DType.int64](n_seg)
        ctx.synchronize()
        ctx.enqueue_copy(soff_d, seg_offset)
        ctx.enqueue_copy(skey_d, seg_key)
        ctx.enqueue_copy(op_d, order_pass)
        ctx.enqueue_copy(ship_d, ship)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(disc_d, disc)
        ctx.synchronize()
        var p = alloc[Q3State2](1)
        p.init_pointee_move(
            Q3State2(
                ctx^,
                soff_d^,
                skey_d^,
                op_d^,
                ship_d^,
                ext_d^,
                disc_d^,
                srev_d^,
                n_rows,
                n_seg,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Pinned-HostBuffer staging pin for Q3 v2 (mirrors mojo_q6_pin_alloc). Two-phase
# because the segmentation (n_seg) and the dense order_pass are discovered/built
# on the host *while* scanning the sorted lineitem stream:
#   mojo_q3_pin_alloc2(n_rows) -> handle + 3 pinned host ptrs (ship/ext/disc)
#     (C++ memcpys each sorted chunk's ship/ext/disc straight in; consumes
#      l_orderkey itself to build seg_offset/seg_key)
#   mojo_q3_pin_upload2(handle, order_pass, seg_offset, seg_key, n_seg,
#                       max_orderkey) -> allocs op/soff/skey/srev device buffers,
#     uploads them + the 3 pinned probe columns. query2/free2 work unchanged.
@export("mojo_q3_pin_alloc2")
def mojo_q3_pin_alloc2(
    n_rows: Int,
    ship_h_out: UnsafePointer[UnsafePointer[Int32, MutAnyOrigin], MutAnyOrigin],
    ext_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    disc_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
) abi("C") -> Int:
    if n_rows <= 0:
        return 0
    try:
        var ctx = shared_device_context()
        var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ship_h = ctx.enqueue_create_host_buffer[DType.int32](n_rows)
        var ext_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var disc_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        ctx.synchronize()
        ship_h_out[] = ship_h.unsafe_ptr()
        ext_h_out[] = ext_h.unsafe_ptr()
        disc_h_out[] = disc_h.unsafe_ptr()
        # Placeholder 1-element device buffers for op/soff/skey/srev; real ones are
        # created in pin_upload2 once n_seg / max_orderkey are known.
        var op_d = ctx.enqueue_create_buffer[DType.uint8](1)
        var soff_d = ctx.enqueue_create_buffer[DType.int64](1)
        var skey_d = ctx.enqueue_create_buffer[DType.int64](1)
        var srev_d = ctx.enqueue_create_buffer[DType.int64](1)
        ctx.synchronize()
        var p = alloc[Q3State2](1)
        p.init_pointee_move(
            Q3State2(
                ctx^,
                soff_d^,
                skey_d^,
                op_d^,
                ship_d^,
                ext_d^,
                disc_d^,
                srev_d^,
                n_rows,
                0,  # n_seg: set in pin_upload2
            )
        )
        ref st = p[]
        st.ship_hb = ship_h^
        st.ext_hb = ext_h^
        st.disc_hb = disc_h^
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q3_pin_upload2")
def mojo_q3_pin_upload2(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    order_pass: UnsafePointer[UInt8, ImmutAnyOrigin],
    seg_offset: UnsafePointer[Int64, ImmutAnyOrigin],
    seg_key: UnsafePointer[Int64, ImmutAnyOrigin],
    n_seg: Int,
    max_orderkey: Int,
    timing: Int32 = 0,
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    if n_seg <= 0 or max_orderkey <= 0:
        return 4
    try:
        ref st = handle.bitcast[Q3State2]()[]
        if not st.ship_hb:
            return 2
        var n_slots = max_orderkey + 1
        # Allocate the segment-/order-sized device buffers now that sizes are known.
        var op_d = st.ctx.enqueue_create_buffer[DType.uint8](n_slots)
        var soff_d = st.ctx.enqueue_create_buffer[DType.int64](n_seg + 1)
        var skey_d = st.ctx.enqueue_create_buffer[DType.int64](n_seg)
        var srev_d = st.ctx.enqueue_create_buffer[DType.int64](n_seg)
        st.ctx.synchronize()
        var t0 = perf_counter_ns()
        st.ctx.enqueue_copy(op_d, order_pass)
        st.ctx.enqueue_copy(soff_d, seg_offset)
        st.ctx.enqueue_copy(skey_d, seg_key)
        st.ctx.enqueue_copy(st.ship_d, st.ship_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ext_d, st.ext_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.disc_d, st.disc_hb.value().unsafe_ptr())
        st.ctx.synchronize()
        var t1 = perf_counter_ns()
        # Swap the placeholder buffers for the real ones.
        st.op_d = op_d^
        st.soff_d = soff_d^
        st.skey_d = skey_d^
        st.srev_d = srev_d^
        st.n_seg = n_seg
        if timing != 0:
            print(
                "[q3-pin pinned mojo] h2d_copy(pinned+dims)=",
                Float64(t1 - t0) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        return 0
    except:
        return 3


# Run one Q3 group-by over the pinned columns with filter l_shipdate > ship_cutoff.
# Launches one warp per order segment; writes per-segment revenue (scale-4 int64,
# size n_seg) into out_seg_rev[s]. The host maps s -> orderkey via its seg_key
# copy. Returns 0 on success; nonzero rc on error.
@export("mojo_q3_query2")
def mojo_q3_query2(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    ship_cutoff: Int32,
    out_seg_rev: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q3State2]()
        ref st = s[]
        st.ctx.enqueue_function[q3_seg_kernel](
            st.soff_d,
            st.skey_d,
            st.op_d,
            st.ship_d,
            st.ext_d,
            st.disc_d,
            st.srev_d,
            st.n_seg,
            ship_cutoff,
            grid_dim=st.n_seg,
            block_dim=32,
        )
        st.ctx.enqueue_copy(out_seg_rev, st.srev_d)
        st.ctx.synchronize()
        return 0
    except:
        return 3


@export("mojo_q3_free2")
def mojo_q3_free2(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q3State2]()
    p.destroy_pointee()
    p.free()


# ===-------------------------------------------------------------------===#
# TPC-H Q5 engine: GPU 6-table-join probe over lineitem + small-cardinality
# grouped (per-nation) exact-decimal aggregation.
#
# The 5 dimension joins (customer, orders, supplier, nation, region) are
# collapsed on the HOST (C++ EnsureQ5Pinned) into dense per-key arrays:
#   order_pass[o_orderkey]        uint8  : o_orderdate in [o_lo, o_hi)
#   order_cust_nation[o_orderkey] int32  : folds customer lookup
#                                          c_nationkey[o_custkey]
#   supp_nation[l_suppkey]        int32  : s_nationkey
#   nation_in_asia[nationkey]     uint8  : region of nation == region_name
# o_orderdate filter is baked into order_pass at pin time (no runtime params).
#
# GPU probe over lineitem: for each row, if order_pass[l_orderkey]:
#     cn = order_cust_nation[l_orderkey]; sn = supp_nation[l_suppkey]
#     if cn == sn (the correlated condition) AND nation_in_asia[sn]:
#         accumulate rev = ext_raw*(100-disc_raw) (scale-4 int64) into group=sn.
# Group-by uses per-block partials (like Q1), NOT GPU atomics: the Apple GPU
# lacks int64 Atomic.fetch_add. Each block keeps private [N_NATIONS] int64 lane
# accumulators; warp.sum reduces each group across the 32 lanes; lane 0 writes
# per-block partials[block*N_NATIONS+group]. The HOST reduces across blocks in
# int128 -> bit-exact vs DuckDB's int128 sum.
#
# N_NATIONS is capped at 25 (TPC-H n_nationkey 0..24). The C++ side guards the
# nationkey range; here the kernel indexes acc[Int(sn)] so out-of-range sn would
# be a bug -> the host validates before pinning.
# ===-------------------------------------------------------------------===#
comptime Q5_NBLOCKS = 4096  # one warp (32 lanes) per block
comptime Q5_NNATIONS = 25  # n_nationkey range 0..24


def q5_kernel(
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    order_cust_nation: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    supp_nation: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    nation_in_asia: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    lorderkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    lsuppkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
):
    var lane = Int(thread_idx.x)
    var stride = Q5_NBLOCKS * 32
    var acc = InlineArray[Int64, Q5_NNATIONS](fill=0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var ok = lorderkey[i]
        if order_pass[Int(ok)] != 0:
            var cn = order_cust_nation[Int(ok)]
            var sn = supp_nation[Int(lsuppkey[i])]
            if cn == sn and nation_in_asia[Int(sn)] != 0:
                acc[Int(sn)] += ext[i] * (Int64(100) - disc[i])  # scale 4
        i += stride
    var blk = Int(block_idx.x)
    for g in range(Q5_NNATIONS):
        var s = warp.sum(acc[g])
        if lane == 0:
            partials[blk * Q5_NNATIONS + g] = s


struct Q5State(Movable):
    var ctx: DeviceContext
    var op_d: DeviceBuffer[DType.uint8]  # order_pass, size max_orderkey+1
    var ocn_d: DeviceBuffer[
        DType.int32
    ]  # order_cust_nation, size max_orderkey+1
    var sn_d: DeviceBuffer[DType.int32]  # supp_nation, size max_suppkey+1
    var asia_d: DeviceBuffer[DType.uint8]  # nation_in_asia, size n_nations
    var lok_d: DeviceBuffer[DType.int64]  # l_orderkey (probe)
    var lsk_d: DeviceBuffer[DType.int64]  # l_suppkey (probe)
    var ext_d: DeviceBuffer[DType.int64]
    var disc_d: DeviceBuffer[DType.int64]
    var part_d: DeviceBuffer[DType.int64]  # per-block per-nation partials
    var part_h: UnsafePointer[Int64, MutAnyOrigin]
    var n_rows: Int
    var n_nations: Int
    # Pinned host-staging buffers for the 4 large probe columns (pinned pin path).
    var lok_hb: Optional[HostBuffer[DType.int64]]
    var lsk_hb: Optional[HostBuffer[DType.int64]]
    var ext_hb: Optional[HostBuffer[DType.int64]]
    var disc_hb: Optional[HostBuffer[DType.int64]]

    def __init__(
        out self,
        var ctx: DeviceContext,
        var op_d: DeviceBuffer[DType.uint8],
        var ocn_d: DeviceBuffer[DType.int32],
        var sn_d: DeviceBuffer[DType.int32],
        var asia_d: DeviceBuffer[DType.uint8],
        var lok_d: DeviceBuffer[DType.int64],
        var lsk_d: DeviceBuffer[DType.int64],
        var ext_d: DeviceBuffer[DType.int64],
        var disc_d: DeviceBuffer[DType.int64],
        var part_d: DeviceBuffer[DType.int64],
        part_h: UnsafePointer[Int64, MutAnyOrigin],
        n_rows: Int,
        n_nations: Int,
    ):
        self.ctx = ctx^
        self.op_d = op_d^
        self.ocn_d = ocn_d^
        self.sn_d = sn_d^
        self.asia_d = asia_d^
        self.lok_d = lok_d^
        self.lsk_d = lsk_d^
        self.ext_d = ext_d^
        self.disc_d = disc_d^
        self.part_d = part_d^
        self.part_h = part_h
        self.n_rows = n_rows
        self.n_nations = n_nations
        self.lok_hb = None
        self.lsk_hb = None
        self.ext_hb = None
        self.disc_hb = None


# Pin the dense dimension lookups (order_pass / order_cust_nation sized
# max_orderkey+1; supp_nation sized max_suppkey+1; nation_in_asia sized
# n_nations) + the 4 probe columns resident. The o_orderdate filter is already
# baked into order_pass. n_nations must be <= Q5_NNATIONS. 0 == failure.
@export("mojo_q5_pin")
def mojo_q5_pin(
    order_pass: UnsafePointer[UInt8, ImmutAnyOrigin],
    order_cust_nation: UnsafePointer[Int32, ImmutAnyOrigin],
    supp_nation: UnsafePointer[Int32, ImmutAnyOrigin],
    nation_in_asia: UnsafePointer[UInt8, ImmutAnyOrigin],
    lorderkey: UnsafePointer[Int64, ImmutAnyOrigin],
    lsuppkey: UnsafePointer[Int64, ImmutAnyOrigin],
    ext: UnsafePointer[Int64, ImmutAnyOrigin],
    disc: UnsafePointer[Int64, ImmutAnyOrigin],
    n_rows: Int,
    max_orderkey: Int,
    max_suppkey: Int,
    n_nations: Int,
) abi("C") -> Int:
    if n_rows <= 0 or max_orderkey <= 0 or max_suppkey <= 0:
        return 0
    if n_nations <= 0 or n_nations > Q5_NNATIONS:
        return 0
    try:
        var n_oslots = max_orderkey + 1
        var n_sslots = max_suppkey + 1
        var ctx = shared_device_context()
        var op_d = ctx.enqueue_create_buffer[DType.uint8](n_oslots)
        var ocn_d = ctx.enqueue_create_buffer[DType.int32](n_oslots)
        var sn_d = ctx.enqueue_create_buffer[DType.int32](n_sslots)
        var asia_d = ctx.enqueue_create_buffer[DType.uint8](n_nations)
        var lok_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var lsk_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](
            Q5_NBLOCKS * Q5_NNATIONS
        )
        ctx.synchronize()
        ctx.enqueue_copy(op_d, order_pass)
        ctx.enqueue_copy(ocn_d, order_cust_nation)
        ctx.enqueue_copy(sn_d, supp_nation)
        ctx.enqueue_copy(asia_d, nation_in_asia)
        ctx.enqueue_copy(lok_d, lorderkey)
        ctx.enqueue_copy(lsk_d, lsuppkey)
        ctx.enqueue_copy(ext_d, ext)
        ctx.enqueue_copy(disc_d, disc)
        ctx.synchronize()
        var part_h = alloc[Int64](Q5_NBLOCKS * Q5_NNATIONS)
        var p = alloc[Q5State](1)
        p.init_pointee_move(
            Q5State(
                ctx^,
                op_d^,
                ocn_d^,
                sn_d^,
                asia_d^,
                lok_d^,
                lsk_d^,
                ext_d^,
                disc_d^,
                part_d^,
                part_h,
                n_rows,
                n_nations,
            )
        )
        return Int(p.bitcast[NoneType]())
    except:
        return 0


# Pinned-HostBuffer staging pin for Q5 (mirrors mojo_q6_pin_alloc). The small
# dense dimension lookups (order_pass / order_cust_nation / supp_nation /
# nation_in_asia) are uploaded immediately here; the 4 large probe columns
# (l_orderkey/l_suppkey/ext/disc, ~6M rows) get pinned HostBuffers whose pointers
# are returned for C++ to memcpy chunks into; then mojo_q5_pin_upload does one
# enqueue_copy(device, pinned-host) per probe column.
@export("mojo_q5_pin_alloc")
def mojo_q5_pin_alloc(
    order_pass: UnsafePointer[UInt8, ImmutAnyOrigin],
    order_cust_nation: UnsafePointer[Int32, ImmutAnyOrigin],
    supp_nation: UnsafePointer[Int32, ImmutAnyOrigin],
    nation_in_asia: UnsafePointer[UInt8, ImmutAnyOrigin],
    n_rows: Int,
    max_orderkey: Int,
    max_suppkey: Int,
    n_nations: Int,
    lok_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    lsk_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    ext_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
    disc_h_out: UnsafePointer[UnsafePointer[Int64, MutAnyOrigin], MutAnyOrigin],
) abi("C") -> Int:
    if n_rows <= 0 or max_orderkey <= 0 or max_suppkey <= 0:
        return 0
    if n_nations <= 0 or n_nations > Q5_NNATIONS:
        return 0
    try:
        var n_oslots = max_orderkey + 1
        var n_sslots = max_suppkey + 1
        var ctx = shared_device_context()
        var op_d = ctx.enqueue_create_buffer[DType.uint8](n_oslots)
        var ocn_d = ctx.enqueue_create_buffer[DType.int32](n_oslots)
        var sn_d = ctx.enqueue_create_buffer[DType.int32](n_sslots)
        var asia_d = ctx.enqueue_create_buffer[DType.uint8](n_nations)
        var lok_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var lsk_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
        var part_d = ctx.enqueue_create_buffer[DType.int64](
            Q5_NBLOCKS * Q5_NNATIONS
        )
        var lok_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var lsk_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var ext_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        var disc_h = ctx.enqueue_create_host_buffer[DType.int64](n_rows)
        # Upload the small dense dimension lookups immediately.
        ctx.enqueue_copy(op_d, order_pass)
        ctx.enqueue_copy(ocn_d, order_cust_nation)
        ctx.enqueue_copy(sn_d, supp_nation)
        ctx.enqueue_copy(asia_d, nation_in_asia)
        ctx.synchronize()
        lok_h_out[] = lok_h.unsafe_ptr()
        lsk_h_out[] = lsk_h.unsafe_ptr()
        ext_h_out[] = ext_h.unsafe_ptr()
        disc_h_out[] = disc_h.unsafe_ptr()
        var part_h = alloc[Int64](Q5_NBLOCKS * Q5_NNATIONS)
        var p = alloc[Q5State](1)
        p.init_pointee_move(
            Q5State(
                ctx^,
                op_d^,
                ocn_d^,
                sn_d^,
                asia_d^,
                lok_d^,
                lsk_d^,
                ext_d^,
                disc_d^,
                part_d^,
                part_h,
                n_rows,
                n_nations,
            )
        )
        ref st = p[]
        st.lok_hb = lok_h^
        st.lsk_hb = lsk_h^
        st.ext_hb = ext_h^
        st.disc_hb = disc_h^
        return Int(p.bitcast[NoneType]())
    except:
        return 0


@export("mojo_q5_pin_upload")
def mojo_q5_pin_upload(
    handle: UnsafePointer[NoneType, MutAnyOrigin], timing: Int32 = 0
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        ref st = handle.bitcast[Q5State]()[]
        if not st.lok_hb:
            return 2
        var t0 = perf_counter_ns()
        st.ctx.enqueue_copy(st.lok_d, st.lok_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.lsk_d, st.lsk_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.ext_d, st.ext_hb.value().unsafe_ptr())
        st.ctx.enqueue_copy(st.disc_d, st.disc_hb.value().unsafe_ptr())
        st.ctx.synchronize()
        var t1 = perf_counter_ns()
        if timing != 0:
            print(
                "[q5-pin pinned mojo] h2d_copy(pinned)=",
                Float64(t1 - t0) / 1.0e6,
                "ms",
                file=FileDescriptor(2),
            )
        return 0
    except:
        return 3


# Run one Q5 probe over the pinned dimensions + columns. Writes per nation the
# int128 revenue as two int64 limbs: out_revenue[nation*2+0]=low,
# out_revenue[nation*2+1]=high (n_nations entries). Returns 0 on success.
@export("mojo_q5_query")
def mojo_q5_query(
    handle: UnsafePointer[NoneType, MutAnyOrigin],
    out_revenue: UnsafePointer[Int64, MutAnyOrigin],
) abi("C") -> Int32:
    if Int(handle) == 0:
        return 1
    try:
        var s = handle.bitcast[Q5State]()
        ref st = s[]
        st.ctx.enqueue_function[q5_kernel](
            st.op_d,
            st.ocn_d,
            st.sn_d,
            st.asia_d,
            st.lok_d,
            st.lsk_d,
            st.ext_d,
            st.disc_d,
            st.part_d,
            st.n_rows,
            grid_dim=Q5_NBLOCKS,
            block_dim=32,
        )
        var npart = Q5_NBLOCKS * Q5_NNATIONS
        var part_sub = DeviceBuffer(
            st.ctx, st.part_d.unsafe_ptr(), npart, owning=False
        )
        st.ctx.enqueue_copy(st.part_h, part_sub)
        st.ctx.synchronize()
        for g in range(st.n_nations):
            var acc = Int128(0)
            for b in range(Q5_NBLOCKS):
                acc += Int128(st.part_h[b * Q5_NNATIONS + g])
            out_revenue[g * 2 + 0] = acc.cast[DType.int64]()
            out_revenue[g * 2 + 1] = (acc >> 64).cast[DType.int64]()
        return 0
    except:
        return 3


@export("mojo_q5_free")
def mojo_q5_free(handle: UnsafePointer[NoneType, MutAnyOrigin]) abi("C"):
    if Int(handle) == 0:
        return
    var p = handle.bitcast[Q5State]()
    p[].part_h.free()
    p.destroy_pointee()
    p.free()
