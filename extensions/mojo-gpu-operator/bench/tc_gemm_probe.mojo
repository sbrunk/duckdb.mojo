"""Q.Emb^T GEMM — block/warp-tiled tensor-core vs scalar-warp-dot.

Computes S[M,N] = Q[M,K] . Emb[N,K]^T two ways and compares correctness + speed:
  - tc_gemm_kernel: block/warp-tiled tensor-core MMA (fp16->fp32, m16n8k8,
    transpose_b). Structure adapted from MAX's matmul_kernel_tc:
      * BM x BN block tile staged once per BK chunk into shared via async copy
        (copy_dram_to_sram_async + async_copy_wait_all), so A/B are read from
        HBM once per BK chunk and reused across the warp tile.
      * WM x WN warp tile; per-warp fp32 register accumulator
        (LayoutTensor row_major(WM/MMA_M, (WN*4)/MMA_N) in LOCAL space).
      * comptime triple-loop over (mma_k, mma_m, mma_n) issuing
        c_reg.copy_from(mma.mma_op(load_a(...), load_b(...), c_reg)) — the
        copy_from form is what actually ACCUMULATES across the K loop
        (reassigning c_reg = mma_op(...) silently yields zeros).
      * transpose_b=True: Emb is stored N x K (row = embedding), so it is staged
        as a BN x BK shared tile and load_b reads MMA_N x MMA_K fragments
        directly — no explicit transpose needed.
  - scalar_gemm_kernel: warp-cooperative scalar dot (one warp per output element,
    lane-strided over K + warp.sum) — mirrors the current batched-kNN inner loop.

Two regimes are measured:
  * SMALL N (N=4096) kept L2-resident: isolates COMPUTE throughput. The output is
    tiny (M=128) so the GPU is occupancy-bound, not compute-bound — this is the
    "peak compute" probe and tops out ~54 TFLOP/s.
  * LARGE N (N=1,048,576): Emb streams from HBM, matching the real kNN case. With
    M=128 the arithmetic intensity is fixed at M=128 FLOP/byte of Emb read, so the
    roofline cap is ~128 * 1008 GB/s ~= 129 TFLOP/s. We reach ~89 TFLOP/s here =
    ~69% of HBM bandwidth: the kernel is HBM-bandwidth-bound on the Emb read, not
    tensor-core-bound. (Confirmed by an M-scaling probe: doubling M to 256 keeps
    TFLOP/s flat while time doubles -> throughput is set by streaming Emb.)

FLOPs = 2*M*N*K. Tile sizes chosen by sweep on an RTX 4090 (sm_89)."""

from std.gpu import (
    WARP_SIZE,
    thread_idx,
    block_idx,
    barrier,
    warp_id as get_warp_id,
)
from std.gpu.primitives import warp
from std.gpu.memory import AddressSpace, async_copy_wait_all
from std.gpu.host import DeviceContext
from layout import Layout, LayoutTensor
from layout._utils import ManagedLayoutTensor
from layout.layout_tensor import copy_dram_to_sram_async
from layout.tensor_core import TensorCore
from std.utils.index import IndexList
from std.math.uutils import udivmod
from std.time import perf_counter_ns
from std.math import abs

comptime M = 128
comptime N = 4096           # small-N (L2-resident "peak compute" probe)
comptime NBIG = 1048576     # large-N (HBM-streaming, the real kNN case)
comptime KD = 768           # K (embedding dim); avoid clashing with kNN's k
comptime MMA_M = 16
comptime MMA_N = 8
comptime MMA_K = 8          # m16n8k8 — m16n8k16 fails instruction selection here
comptime ITERS = 50

# Two tiles win in the two regimes (they pull in opposite directions):
#   * SMALL N: the output is tiny, so SMALL blocks win (more blocks -> more SMs
#     busy). Best compute probe: BM=32/BN=64/BK=32, WM=16/WN=64, 2 warps.
#   * LARGE N: BK=64 (deep K-staging: fewer barriers, more MMAs per shared load)
#     and BM=128 (covers all of M=128 in one block-row -> maximal Q reuse across
#     the long N axis) win. Best: BM=128/BN=128/BK=64, WM=64/WN=64, 4 warps.
# Small-N tile ("peak compute"):
comptime SBM = 32
comptime SBN = 64
comptime SBK = 32
comptime SWM = 16
comptime SWN = 64
# Large-N tile (the real kNN case):
comptime BM = 128
comptime BN = 128
comptime BK = 64
comptime WM = 64
comptime WN = 64
comptime NUM_WARPS = (BM // WM) * (BN // WN)   # 4
comptime NUM_THREADS = NUM_WARPS * WARP_SIZE   # 128


def tc_gemm_kernel[
    a_layout: Layout, b_layout: Layout, c_layout: Layout,
    BM: Int, BN: Int, BK: Int, WM: Int, WN: Int,
](
    a: LayoutTensor[DType.float16, a_layout, MutAnyOrigin],
    b: LayoutTensor[DType.float16, b_layout, MutAnyOrigin],
    c: LayoutTensor[DType.float32, c_layout, MutAnyOrigin],
):
    comptime NWARPS = (BM // WM) * (BN // WN)
    comptime NTHREADS = NWARPS * WARP_SIZE
    var mma = TensorCore[
        DType.float32, DType.float16, IndexList[3](MMA_M, MMA_N, MMA_K),
        transpose_b=True,
    ]()

    # Warp tiling within the block: (BM/WM) rows x (BN/WN) cols of warps.
    var warp_id = get_warp_id()
    warp_y, warp_x = udivmod(warp_id, BN // WN)

    # This warp's WM x WN slice of the BM x BN output block tile. transpose_b
    # means B rows index N, so C columns index N directly (store_d to a strided
    # global view is fine).
    C_warp_tile = c.tile[BM, BN](block_idx.y, block_idx.x).tile[WM, WN](
        warp_y, warp_x
    )

    # Contiguous shared staging: A as BM x BK, B (=Emb, N x K) as BN x BK.
    # Fragment loads require a contiguous tile; a strided global .tile() view
    # would load wrong elements.
    var a_smem = LayoutTensor[
        DType.float16, Layout.row_major(BM, BK), MutAnyOrigin,
        address_space = AddressSpace.SHARED,
    ].stack_allocation()
    var b_smem = LayoutTensor[
        DType.float16, Layout.row_major(BN, BK), MutAnyOrigin,
        address_space = AddressSpace.SHARED,
    ].stack_allocation()

    # Per-warp fp32 register accumulator (persists across the whole K loop).
    var c_reg = (
        LayoutTensor[
            DType.float32, Layout.row_major(WM // MMA_M, (WN * 4) // MMA_N),
            MutAnyOrigin, address_space = AddressSpace.LOCAL,
        ]
        .stack_allocation()
        .fill(0.0)
    )

    # Async-copy thread layout: NTHREADS threads cover a BM x BK (resp. BN x BK)
    # tile, each loading a 4-wide fp16 vector.
    comptime CPW = BK // 4                # vectorized cols per thread-row
    comptime CRows = NTHREADS // CPW      # thread-rows
    for k_i in range(KD // BK):
        barrier()
        var A_dram_tile = a.tile[BM, BK](block_idx.y, k_i)
        var B_dram_tile = b.tile[BN, BK](block_idx.x, k_i)
        copy_dram_to_sram_async[thread_layout = Layout.row_major(CRows, CPW)](
            a_smem.vectorize[1, 4](), A_dram_tile.vectorize[1, 4]()
        )
        copy_dram_to_sram_async[thread_layout = Layout.row_major(CRows, CPW)](
            b_smem.vectorize[1, 4](), B_dram_tile.vectorize[1, 4]()
        )
        async_copy_wait_all()
        barrier()

        var A_warp_tile = a_smem.tile[WM, BK](warp_y, 0)
        var B_warp_tile = b_smem.tile[WN, BK](warp_x, 0)

        comptime for mma_k in range(BK // MMA_K):
            comptime for mma_m in range(WM // MMA_M):
                comptime for mma_n in range(WN // MMA_N):
                    var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
                    var A_mma_tile = A_warp_tile.tile[MMA_M, MMA_K](mma_m, mma_k)
                    var B_mma_tile = B_warp_tile.tile[MMA_N, MMA_K](mma_n, mma_k)
                    var a_reg = mma.load_a(A_mma_tile)
                    var b_reg = mma.load_b(B_mma_tile)
                    # copy_from accumulates into the persistent c_reg; a plain
                    # `c_reg_m_n = mma.mma_op(...)` would NOT accumulate.
                    var d_reg = mma.mma_op(a_reg, b_reg, c_reg_m_n)
                    c_reg_m_n.copy_from(d_reg)

    comptime for mma_m in range(WM // MMA_M):
        comptime for mma_n in range(WN // MMA_N):
            var C_mma_tile = C_warp_tile.tile[MMA_M, MMA_N](mma_m, mma_n)
            var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
            mma.store_d(C_mma_tile, c_reg_m_n)


def scalar_gemm_kernel[
    a_layout: Layout, b_layout: Layout, c_layout: Layout
](
    a: LayoutTensor[DType.float16, a_layout, MutAnyOrigin],
    b: LayoutTensor[DType.float16, b_layout, MutAnyOrigin],
    c: LayoutTensor[DType.float32, c_layout, MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var idx = Int(block_idx.x)   # one warp per output element
    var m = idx // N
    var n = idx % N
    var acc = Float32(0)
    var k = lane
    while k < KD:
        var av = rebind[Scalar[DType.float16]](a[m, k]).cast[DType.float32]()
        var bv = rebind[Scalar[DType.float16]](b[n, k]).cast[DType.float32]()
        acc += av * bv
        k += WARP_SIZE
    acc = warp.sum(acc)
    if lane == 0:
        c[m, n] = acc


def time_tc[
    MM: Int, NN: Int,
    BM: Int, BN: Int, BK: Int, WM: Int, WN: Int,
    qa_l: Layout, eb_l: Layout, s_l: Layout,
](
    ctx: DeviceContext,
    qd: LayoutTensor[DType.float16, qa_l, MutAnyOrigin],
    ed: LayoutTensor[DType.float16, eb_l, MutAnyOrigin],
    sd: LayoutTensor[DType.float32, s_l, MutAnyOrigin],
) raises -> Float64:
    comptime NWARPS = (BM // WM) * (BN // WN)
    comptime NTHREADS = NWARPS * WARP_SIZE
    comptime tcfn = tc_gemm_kernel[qa_l, eb_l, s_l, BM, BN, BK, WM, WN]
    comptime grid = (NN // BN, MM // BM)
    for _ in range(3):
        ctx.enqueue_function[tcfn](qd, ed, sd, grid_dim=grid, block_dim=NTHREADS)
    ctx.synchronize()
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[tcfn](qd, ed, sd, grid_dim=grid, block_dim=NTHREADS)
    ctx.synchronize()
    return Float64(perf_counter_ns() - t0) / Float64(ITERS) / 1e6


def main() raises:
    var ctx = DeviceContext()
    var qa = ManagedLayoutTensor[DType.float16, Layout.row_major(M, KD)](ctx)
    var eb = ManagedLayoutTensor[DType.float16, Layout.row_major(N, KD)](ctx)
    var s_tc = ManagedLayoutTensor[DType.float32, Layout.row_major(M, N)](ctx)
    var s_sc = ManagedLayoutTensor[DType.float32, Layout.row_major(M, N)](ctx)

    var qh = qa.tensor()
    for m in range(M):
        for k in range(KD):
            qh[m, k] = Float16(Float32((m + k) % 7) * 0.1)
    var eh = eb.tensor()
    for n in range(N):
        for k in range(KD):
            eh[n, k] = Float16(Float32((n + k) % 5) * 0.1)
    _ = s_tc.tensor().fill(0.0)
    _ = s_sc.tensor().fill(0.0)

    var qd = qa.device_tensor()
    var ed = eb.device_tensor()
    var sd = s_tc.device_tensor()

    # ---- tensor-core GEMM (small N, L2-resident = "peak compute" probe) ----
    #      uses the small-N-optimal tile (small blocks -> more SMs busy).
    var tc_ms = time_tc[M, N, SBM, SBN, SBK, SWM, SWN, qa.layout, eb.layout, s_tc.layout](
        ctx, qd, ed, sd
    )

    # ---- scalar warp-dot: grid M*N, one warp per output element ----
    comptime scfn = scalar_gemm_kernel[qa.layout, eb.layout, s_sc.layout]
    var ssd = s_sc.device_tensor()
    for _ in range(3):
        ctx.enqueue_function[scfn](qd, ed, ssd, grid_dim=(M * N), block_dim=WARP_SIZE)
    ctx.synchronize()
    var t1 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[scfn](qd, ed, ssd, grid_dim=(M * N), block_dim=WARP_SIZE)
    ctx.synchronize()
    var sc_ms = Float64(perf_counter_ns() - t1) / Float64(ITERS) / 1e6

    # ---- correctness: max|tc - sc| must be within fp16 rounding ----
    var sth = s_tc.tensor()
    var ssh = s_sc.tensor()
    var maxerr = Float32(0)
    for m in range(M):
        for n in range(N):
            var tv = rebind[Scalar[DType.float32]](sth[m, n])
            var sv = rebind[Scalar[DType.float32]](ssh[m, n])
            var d = abs(tv - sv)
            if d > maxerr:
                maxerr = d

    var flop = 2.0 * Float64(M) * Float64(N) * Float64(KD)
    print("S_tc[0,0]=", sth[0, 0], " S_sc[0,0]=", ssh[0, 0], " max|tc-sc|=", maxerr)
    print("--- small N =", N, "(L2-resident, peak compute) ---")
    print("tile: BM", SBM, "BN", SBN, "BK", SBK, "WM", SWM, "WN", SWN,
          "warps", (SBM // SWM) * (SBN // SWN))
    print("TC    :", tc_ms, "ms  ", flop / (tc_ms / 1e3) / 1e12, "TFLOP/s")
    print("scalar:", sc_ms, "ms  ", flop / (sc_ms / 1e3) / 1e12, "TFLOP/s")
    print("speedup (scalar/TC):", sc_ms / tc_ms, "x")

    # ---- large N (HBM-streaming, the real kNN case) ----
    var ebL = ManagedLayoutTensor[DType.float16, Layout.row_major(NBIG, KD)](ctx)
    var sL = ManagedLayoutTensor[DType.float32, Layout.row_major(M, NBIG)](ctx)
    var ehL = ebL.tensor()
    for n in range(NBIG):
        for k in range(KD):
            ehL[n, k] = Float16(Float32((n + k) % 5) * 0.1)
    _ = sL.tensor().fill(0.0)
    var big_ms = time_tc[
        M, NBIG, BM, BN, BK, WM, WN, qa.layout, ebL.layout, sL.layout
    ](ctx, qd, ebL.device_tensor(), sL.device_tensor())
    var big_flop = 2.0 * Float64(M) * Float64(NBIG) * Float64(KD)
    var emb_gb = 2.0 * Float64(NBIG) * Float64(KD) / 1e9
    print("--- large N =", NBIG, "(HBM-streaming, real kNN case) ---")
    print("tile: BM", BM, "BN", BN, "BK", BK, "WM", WM, "WN", WN, "warps", NUM_WARPS)
    print("TC    :", big_ms, "ms  ", big_flop / (big_ms / 1e3) / 1e12, "TFLOP/s")
    print("  Emb HBM read =", emb_gb, "GB; effective B-bandwidth =",
          emb_gb / (big_ms / 1e3), "GB/s (roofline @ M=128 ~= 129 TFLOP/s)")
