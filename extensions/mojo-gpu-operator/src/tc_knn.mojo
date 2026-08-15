"""FUSED tensor-core kNN for the batched fp16 top-k path (NVIDIA-only).

Ported from `bench/tc_knn_fused.mojo` (proven: recall@10 = 1.0, 0 genuine misses
at N=1M K=768 M=128 k=10; ~67x the scalar batched path on an RTX 4090). It
computes per-query top-k via a tiled MMA `Q.Emb^T` (m16n8k8, transpose_b) fused
with a streaming per-query top-k, so the M x N similarity matrix is NEVER
materialized -- only Emb is read, only M x k results are written.

This module is wired into `_run_topk_batch` in `gpu_kernels.mojo` behind the
`GPU_OP_TENSORCORE` env flag (default OFF) AND a comptime NVIDIA gate. The gate
uses TWO different comptime queries depending on context (see memory
`mojo-gpu-target-introspection`):
  * HOST functions (`run_tc_knn_batch`, `_run_tc_knn_for_kd`, and the routing
    branch in gpu_kernels) gate on `has_nvidia_gpu_accelerator()` -- the
    host-context query. The in-kernel `is_nvidia_gpu()` triple check is ALWAYS
    False in host context, so it cannot gate a host branch.
  * KERNEL bodies (`tc_fused_knn_kernel`, `tc_merge_kernel`, `_tc_enorm_kernel`)
    gate on `is_nvidia_gpu()` -- evaluated for the GPU compilation target.
On Apple both queries are comptime-False, so NOTHING that touches the `layout`
package's `TensorCore` is ever instantiated for the Metal target. The
module-level `from layout... import ...` lines are pure host-side imports that
type-check on every target (verified: a gated TensorCore kernel built with
`--emit shared-lib` for the Apple target compiles clean).

Shape (matches the proven reference): per M-tile of BM=128 queries, the fused
kernel grid-strides NBLOCKS=256 one-warp-grid-tile blocks over N in BN=64 chunks,
MMAs the BM x BN S-tile into fp32 registers, and updates a persistent per-thread
register top-k. Stage 2 (`merge_kernel`) reduces each query's NBLOCKS*k
candidates to the final k. The host tiles over M in BM-chunks and over the
supported embedding dims KD (768/384/1024/1536) via comptime instantiation;
unsupported K falls back to the scalar path (the caller keeps the scalar driver).
"""

from std.sys.info import is_nvidia_gpu, has_nvidia_gpu_accelerator
from std.gpu import (
    WARP_SIZE,
    thread_idx,
    block_idx,
    warp_id as get_warp_id,
)
from max.gpu.sync import barrier
from max.gpu.memory import AddressSpace, async_copy_wait_all
from max.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc, stack_allocation
from layout import Layout, LayoutTensor, UNKNOWN_VALUE
from layout.runtime_layout import RuntimeLayout
from layout.layout_tensor import copy_dram_to_sram_async
from layout.tensor_core import TensorCore
from std.utils.index import IndexList, Index
from std.math.uutils import udivmod
from std.math import sqrt


# ===-------------------------------------------------------------------===#
# Fused-kernel tile (identical to the proven reference). BM=128 queries per
# launch; small BN so the BM x BN fp32 S-tile fits shared (128*64*4 = 32 KB +
# a/b stages). Each block GRID-STRIDES over N in BN-chunks, so the grid dim
# (NBLOCKS) is fixed and the merge cost (NBLOCKS*k per query) stays bounded as N
# scales -- the flash-attention streaming shape.
# ===-------------------------------------------------------------------===#
comptime TC_MMA_M = 16
comptime TC_MMA_N = 8
comptime TC_MMA_K = 8

comptime TC_BM = 128
comptime TC_BN = 64
comptime TC_BK = 32
comptime TC_WM = 64
comptime TC_WN = 32
comptime TC_NUM_WARPS = (TC_BM // TC_WM) * (TC_BN // TC_WN)  # 4
comptime TC_NUM_THREADS = TC_NUM_WARPS * WARP_SIZE  # 128
comptime TC_QPT = TC_BM // TC_NUM_THREADS  # 1
comptime TC_NBLOCKS = 256

# Max k the fused register top-k supports (the per-thread bd/bi arrays are
# TC_QPT * TC_K_CAP wide). Production kNN k is small (<= ~10 in practice); the
# caller falls back to the scalar path for k beyond this.
comptime TC_K_CAP = 64

# ===-------------------------------------------------------------------===#
# Metric selector. The MMA core (Q.Emb^T dot) is IDENTICAL for all three; only
# the per-candidate distance EPILOGUE and the norm-buffer semantics change:
#   COSINE (0): cd = 1 - dot/(|q|*|e|)         norms = L2 norm (sqrt of sum sq)
#   L2     (1): cd = |q|^2 + |e|^2 - 2*dot     norms = SQUARED L2 norm (no sqrt)
#   IP     (2): cd = -dot                       norms = unused
# In every case top-k is by SMALLEST cd with the SAME (cd, rowid) tie-break, so
# only the ~3-line `cd` computation differs. The metric is a COMPTIME parameter
# of the kernel/driver so the switch is resolved at compile time (no runtime
# branch in the streaming top-k inner loop); the host entry maps a runtime enum
# to the comptime instantiation.
# ===-------------------------------------------------------------------===#
comptime TC_METRIC_COSINE = 0
comptime TC_METRIC_L2 = 1
comptime TC_METRIC_IP = 2


# Whether the fused tensor-core path supports this (K, k). K must be one of the
# comptime-specialized embedding dims (a multiple of TC_BK that we instantiate),
# k must fit the register top-k cap, and M must be positive. The caller uses this
# to decide fused-vs-scalar; on a False here it keeps the existing scalar path.
def tc_knn_supported(K: Int, k: Int) -> Bool:
    if k <= 0 or k > TC_K_CAP:
        return False
    if K == 768 or K == 384 or K == 1024 or K == 1536:
        return True
    return False


# ===-------------------------------------------------------------------===#
# Stage 1: fused MMA GEMM + per-block per-query streaming top-k over the N-slab.
# Parameterized over the (comptime) embedding dim KD so KCH = KD // TC_BK is
# comptime. q is a BM x KD tile (this M-tile's queries), e is the full N x KD
# resident matrix. Everything here is inside `comptime if is_nvidia_gpu()`.
# ===-------------------------------------------------------------------===#
def tc_fused_knn_kernel[
    KD: Int, metric: Int, q_layout: Layout, e_layout: Layout,
](
    q: LayoutTensor[DType.float16, q_layout, MutUntrackedOrigin],
    e: LayoutTensor[DType.float16, e_layout, MutUntrackedOrigin],
    qnorm: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    enorm: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutUntrackedOrigin],
    n_rows_dp: Int64,
    k_dp: Int64,
    nblocks_dp: Int64,
):
    var n_rows = Int(n_rows_dp)
    var k = Int(k_dp)
    var nblocks = Int(nblocks_dp)
    comptime if is_nvidia_gpu():
        var mma = TensorCore[
            DType.float32, DType.float16,
            IndexList[3](TC_MMA_M, TC_MMA_N, TC_MMA_K),
            transpose_b=True,
        ]()

        var warp_id = get_warp_id()
        warp_y, warp_x = udivmod(warp_id, TC_BN // TC_WN)

        var a_smem = LayoutTensor[
            DType.float16, Layout.row_major(TC_BM, TC_BK), MutUntrackedOrigin,
            address_space = AddressSpace.SHARED,
        ].stack_allocation()
        var b_smem = LayoutTensor[
            DType.float16, Layout.row_major(TC_BN, TC_BK), MutUntrackedOrigin,
            address_space = AddressSpace.SHARED,
        ].stack_allocation()
        var s_smem = LayoutTensor[
            DType.float32, Layout.row_major(TC_BM, TC_BN), MutUntrackedOrigin,
            address_space = AddressSpace.SHARED,
        ].stack_allocation()

        var c_reg = (
            LayoutTensor[
                DType.float32,
                Layout.row_major(TC_WM // TC_MMA_M, (TC_WN * 4) // TC_MMA_N),
                MutUntrackedOrigin, address_space = AddressSpace.LOCAL,
            ]
            .stack_allocation()
            .fill(0.0)
        )

        var tid = Int(thread_idx.x)

        var bd = stack_allocation[TC_QPT * TC_K_CAP, Scalar[DType.float32]]()
        var bi = stack_allocation[TC_QPT * TC_K_CAP, Scalar[DType.int64]]()
        var bcnt = stack_allocation[TC_QPT, Scalar[DType.int32]]()
        comptime for p in range(TC_QPT):
            bcnt[p] = 0

        comptime CPW = TC_BK // 4
        comptime CRows = TC_NUM_THREADS // CPW
        comptime KCH = KD // TC_BK  # K chunks (comptime via KD)
        comptime NSTRIDE = TC_NBLOCKS * TC_BN

        var n0 = Int(block_idx.x) * TC_BN
        while n0 < n_rows:
            _ = c_reg.fill(0.0)
            var nchunk = n0 // TC_BN
            for k_i in range(KCH):
                barrier()
                var A_dram_tile = q.tile[TC_BM, TC_BK](0, k_i)
                var B_dram_tile = e.tile[TC_BN, TC_BK](nchunk, k_i)
                copy_dram_to_sram_async[
                    thread_layout = Layout.row_major(CRows, CPW)
                ](a_smem.vectorize[1, 4](), A_dram_tile.vectorize[1, 4]())
                copy_dram_to_sram_async[
                    thread_layout = Layout.row_major(CRows, CPW)
                ](b_smem.vectorize[1, 4](), B_dram_tile.vectorize[1, 4]())
                async_copy_wait_all()
                barrier()

                var A_warp_tile = a_smem.tile[TC_WM, TC_BK](warp_y, 0)
                var B_warp_tile = b_smem.tile[TC_WN, TC_BK](warp_x, 0)
                comptime for mma_k in range(TC_BK // TC_MMA_K):
                    comptime for mma_m in range(TC_WM // TC_MMA_M):
                        comptime for mma_n in range(TC_WN // TC_MMA_N):
                            var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
                            var A_mma_tile = A_warp_tile.tile[
                                TC_MMA_M, TC_MMA_K
                            ](mma_m, mma_k)
                            var B_mma_tile = B_warp_tile.tile[
                                TC_MMA_N, TC_MMA_K
                            ](mma_n, mma_k)
                            var a_reg = mma.load_a(A_mma_tile)
                            var b_reg = mma.load_b(B_mma_tile)
                            var d_reg = mma.mma_op(a_reg, b_reg, c_reg_m_n)
                            c_reg_m_n.copy_from(d_reg)

            var S_warp_tile = s_smem.tile[TC_WM, TC_WN](warp_y, warp_x)
            comptime for mma_m in range(TC_WM // TC_MMA_M):
                comptime for mma_n in range(TC_WN // TC_MMA_N):
                    var S_mma_tile = S_warp_tile.tile[TC_MMA_M, TC_MMA_N](
                        mma_m, mma_n
                    )
                    var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
                    mma.store_d(S_mma_tile, c_reg_m_n)
            barrier()

            comptime for p in range(TC_QPT):
                var m = tid + p * TC_NUM_THREADS
                var qn = qnorm[m]
                var off = p * TC_K_CAP
                var cnt = Int(bcnt[p])
                for nn in range(TC_BN):
                    var row = n0 + nn
                    if row >= n_rows:
                        break
                    var dotv = rebind[Scalar[DType.float32]](s_smem[m, nn])
                    # Distance epilogue: only this varies by metric (comptime
                    # switch -> no runtime branch). The MMA `dotv` above is
                    # shared by all three; top-k is by smallest `cd` for each.
                    var cd: Float32
                    comptime if metric == TC_METRIC_L2:
                        # Squared euclidean: |q|^2 + |e|^2 - 2*dot. qn / enorm
                        # carry SQUARED norms here (host/enorm-kernel skip sqrt
                        # for L2). NB: catastrophic cancellation for
                        # non-normalized data + fp16 dot -- see run_tc_knn_batch.
                        cd = qn + enorm[row] - Float32(2) * dotv
                    elif metric == TC_METRIC_IP:
                        # Negative inner product (top-k by largest dot = smallest
                        # -dot). Norms unused.
                        cd = -dotv
                    else:
                        # Cosine (default): 1 - dot/(|q|*|e|). enorm pre-sqrt'd
                        # on host; byte-for-byte the original epilogue.
                        var denom = enorm[row] * qn
                        cd = (
                            Float32(1) - dotv / denom if denom
                            != 0 else Float32(0)
                        )
                    var ci = Int64(row)
                    var accept = True
                    if cnt >= k:
                        var wd = bd[off + k - 1]
                        var wi = bi[off + k - 1]
                        if cd > wd or (cd == wd and ci >= wi):
                            accept = False
                    if accept:
                        var pos = cnt if cnt < k else k - 1
                        while pos > 0:
                            var pd = bd[off + pos - 1]
                            var pi = bi[off + pos - 1]
                            if pd > cd or (pd == cd and pi > ci):
                                bd[off + pos] = pd
                                bi[off + pos] = pi
                                pos -= 1
                            else:
                                break
                        bd[off + pos] = cd
                        bi[off + pos] = ci
                        if cnt < k:
                            cnt += 1
                bcnt[p] = Int32(cnt)
            barrier()
            n0 += NSTRIDE

        comptime for p in range(TC_QPT):
            var m = tid + p * TC_NUM_THREADS
            var off = p * TC_K_CAP
            var cnt = Int(bcnt[p])
            var out_base = (Int(block_idx.x) * TC_BM + m) * k
            for j in range(k):
                if j < cnt:
                    cand_dist[out_base + j] = bd[off + j]
                    cand_id[out_base + j] = bi[off + j]
                else:
                    cand_dist[out_base + j] = Float32(3.0e38)
                    cand_id[out_base + j] = Int64(-1)


# ===-------------------------------------------------------------------===#
# Stage 2: per-query merge of nblocks*k candidates -> final k (one warp/query).
# Candidate layout is [block*BM*k + m*k + j] (the BM stride matches stage 1's
# `(block_idx.x*TC_BM + m)*k` emit). NVIDIA-only via the comptime gate.
# ===-------------------------------------------------------------------===#
def tc_merge_kernel(
    cand_dist: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutUntrackedOrigin],
    out_dist: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    out_id: UnsafePointer[Scalar[DType.int64], MutUntrackedOrigin],
    Mq_dp: Int64,
    nblocks_dp: Int64,
    k_dp: Int64,
):
    var Mq = Int(Mq_dp)
    var nblocks = Int(nblocks_dp)
    var k = Int(k_dp)
    comptime if is_nvidia_gpu():
        var sd = stack_allocation[
            TC_K_CAP, Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            TC_K_CAP, Scalar[DType.int64],
            address_space = AddressSpace.SHARED,
        ]()
        var scnt = stack_allocation[
            1, Scalar[DType.int32], address_space = AddressSpace.SHARED
        ]()

        var lane = Int(thread_idx.x)
        var mq = Int(block_idx.x)  # this block merges query mq
        if mq >= Mq:
            return
        if lane == 0:
            scnt[0] = 0
        barrier()

        var ncand = nblocks * k
        var nwaves = (ncand + WARP_SIZE - 1) // WARP_SIZE
        for w in range(nwaves):
            var c = lane + w * WARP_SIZE
            var my_d = Float32(3.0e38)
            var my_id = Int64(-1)
            if c < ncand:
                var b = c // k
                var j = c % k
                var idx = (b * Mq + mq) * k + j
                my_id = cand_id[idx]
                my_d = cand_dist[idx]
            for src in range(WARP_SIZE):
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

        var cnt = Int(scnt[0])
        var j = lane
        while j < k:
            if j < cnt:
                out_dist[mq * k + j] = sd[j]
                out_id[mq * k + j] = si[j]
            else:
                out_dist[mq * k + j] = Float32(3.0e38)
                out_id[mq * k + j] = Int64(-1)
            j += WARP_SIZE


# ===-------------------------------------------------------------------===#
# Host driver for ONE supported embedding dim KD (comptime). Tiles over M in
# BM-chunks; each M-tile launches the fused stage-1 kernel (reads the full N
# matrix once) + the stage-2 merge, and writes that tile's BM*k results into the
# caller's row-major M*k output. NVIDIA-only via the comptime gate.
#
# emb16 is the resident N x KD fp16 matrix; qs_dev holds the M x KD fp16 queries;
# qnorm_dev/enorm_dev hold the host-precomputed fp32 norms (qnorm = sqrt(sum
# q^2); enorm = sqrt(sum (fp16-cast emb)^2) -- the SAME denom the scalar f16 path
# forms). cand_* / merged_* are reused per tile.
# ===-------------------------------------------------------------------===#
def _run_tc_knn_for_kd[
    KD: Int, metric: Int
](
    ctx: DeviceContext,
    emb16: DeviceBuffer[DType.float16],
    qs_dev: DeviceBuffer[DType.float16],
    qnorm_dev: DeviceBuffer[DType.float32],
    enorm_dev: DeviceBuffer[DType.float32],
    n_rows: Int,
    M: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutUntrackedOrigin],
    out_dists: UnsafePointer[Float32, MutUntrackedOrigin],
) raises:
    # HOST gate: `has_nvidia_gpu_accelerator()` (the in-kernel `is_nvidia_gpu()`
    # is False in host context). On Apple this is comptime-False so the body --
    # which instantiates the tensor-core kernels via enqueue_function -- never
    # compiles.
    comptime if has_nvidia_gpu_accelerator():
        comptime e_layout = Layout.row_major(UNKNOWN_VALUE, KD)
        comptime q_layout = Layout.row_major(TC_BM, KD)

        var e_rt = RuntimeLayout[e_layout].row_major(Index(n_rows, KD))
        var e_span = Span[Scalar[DType.float16], MutUntrackedOrigin](
            unsafe_ptr=emb16.unsafe_ptr()
            .unsafe_mut_cast[True]()
            .unsafe_origin_cast[MutUntrackedOrigin](),
            length=n_rows * KD,
        )
        var e_tensor = LayoutTensor[DType.float16, e_layout, MutUntrackedOrigin](
            e_span, e_rt
        )

        var nblocks = TC_NBLOCKS
        var cand_cap = nblocks * TC_BM * k
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
        var merged_dist_dev = ctx.enqueue_create_buffer[DType.float32](
            TC_BM * k
        )
        var merged_id_dev = ctx.enqueue_create_buffer[DType.int64](TC_BM * k)
        var merged_dist_h = alloc[Float32](TC_BM * k)
        var merged_id_h = alloc[Int64](TC_BM * k)
        ctx.synchronize()

        comptime fk = tc_fused_knn_kernel[KD, metric, q_layout, e_layout]

        var q0 = 0
        while q0 < M:
            # This tile's query rows [q0, q0+qcount). The kernel always reads BM
            # query rows from the passed q tile (q.tile[BM,...](0, ..)); the
            # qs_dev buffer is padded to a multiple of BM rows so a full BM tile
            # is always in bounds. qcount is the real query count in this tile.
            var qcount = TC_BM
            if q0 + qcount > M:
                qcount = M - q0

            var q_sub = DeviceBuffer(
                ctx,
                qs_dev.unsafe_ptr() + q0 * KD,
                TC_BM * KD,
                owning=False,
            )
            var q_span = Span[Scalar[DType.float16], MutUntrackedOrigin](
                unsafe_ptr=q_sub.unsafe_ptr()
                .unsafe_mut_cast[True]()
                .unsafe_origin_cast[MutUntrackedOrigin](),
                length=TC_BM * KD,
            )
            var q_tensor = LayoutTensor[
                DType.float16, q_layout, MutUntrackedOrigin
            ](q_span)
            # qnorm offset for this tile.
            var qnorm_sub = qnorm_dev.unsafe_ptr() + q0

            ctx.enqueue_function[fk](
                q_tensor,
                e_tensor,
                qnorm_sub,
                enorm_dev.unsafe_ptr(),
                cand_dist_dev.unsafe_ptr(),
                cand_id_dev.unsafe_ptr(),
                Int64(n_rows),
                Int64(k),
                Int64(nblocks),
                grid_dim=nblocks,
                block_dim=TC_NUM_THREADS)
            ctx.enqueue_function[tc_merge_kernel](
                cand_dist_dev.unsafe_ptr(),
                cand_id_dev.unsafe_ptr(),
                merged_dist_dev.unsafe_ptr(),
                merged_id_dev.unsafe_ptr(),
                Int64(TC_BM),
                Int64(nblocks),
                Int64(k),
                grid_dim=TC_BM,
                block_dim=WARP_SIZE)
            ctx.enqueue_copy(merged_dist_h, merged_dist_dev)
            ctx.enqueue_copy(merged_id_h, merged_id_dev)
            ctx.synchronize()

            for lq in range(qcount):
                var m = q0 + lq
                for j in range(k):
                    out_ids[m * k + j] = merged_id_h[lq * k + j]
                    out_dists[m * k + j] = merged_dist_h[lq * k + j]
            q0 += TC_BM

        merged_dist_h.free()
        merged_id_h.free()


# ===-------------------------------------------------------------------===#
# Top-level fused batched fp16 driver. Builds the fp16 query tile + the fp32
# query/embedding norms on the host, uploads them once, then dispatches to the
# comptime-specialized per-(KD, metric) driver. For cosine it returns the SAME
# exact (ids, dists) as the scalar batched path for the supported (K, k).
# NVIDIA-only via the gate; the caller guards with `tc_knn_supported(K, k)` +
# the env flag.
#
# `metric` (TC_METRIC_*): 0 cosine, 1 L2/squared-euclidean (array_distance),
# 2 inner-product (array_negative_inner_product, score = -dot). The MMA core is
# byte-for-byte identical across metrics; only the per-candidate distance
# epilogue (in tc_fused_knn_kernel) and the norm-buffer contents differ.
#
# NUMERICAL NOTE for L2 (validated): the kernel forms squared euclidean as
# |q|^2 + |e|^2 - 2*dot, where `dot` comes from the fp16-INPUT MMA. For
# UNIT-NORMALIZED data this is exact (|q|^2 = |e|^2 = 1, the subtraction is
# well-conditioned and the top-k matches a scalar reference exactly). For
# NON-NORMALIZED data with large/similar |q|^2, |e|^2 the subtraction suffers
# CATASTROPHIC CANCELLATION against the lower-precision fp16-product dot -> the
# ranking degrades (genuine misses). DECISION: ship L2 for normalized inputs
# (the dominant real-embedding case -- array_distance over normalized vectors is
# equivalent to a monotonic transform of cosine) and DOCUMENT the non-normalized
# limitation here + in the metric bench. A numerically robust non-normalized L2
# would need fp32/tf32 MMA inputs (≈2x the Emb-read bandwidth, the kernel's
# bound) or a fused stable form; not implemented (cost noted). Cosine and IP are
# unaffected (IP is a pure dot; cosine divides by the norm rather than
# subtracting it).
# ===-------------------------------------------------------------------===#
def run_tc_knn_batch(
    ctx: DeviceContext,
    emb16: DeviceBuffer[DType.float16],
    n_rows: Int,
    K: Int,
    qs: UnsafePointer[Float32, ImmUntrackedOrigin],
    M: Int,
    k: Int,
    metric: Int,
    out_ids: UnsafePointer[Int64, MutUntrackedOrigin],
    out_dists: UnsafePointer[Float32, MutUntrackedOrigin],
) raises:
    # HOST gate (see `_run_tc_knn_for_kd`): NVIDIA-accelerator-only via the
    # host-side comptime query; Apple never compiles this body.
    #
    # `metric` is a RUNTIME enum here (TC_METRIC_*) dispatched to the comptime
    # instantiation below. Norm-buffer semantics depend on it:
    #   COSINE: qnorm/enorm = L2 norm  (sqrt of sum of squares)
    #   L2    : qnorm/enorm = SQUARED L2 norm (skip the sqrt) -- the epilogue
    #           forms |q|^2 + |e|^2 - 2*dot.
    #   IP    : norms unused (epilogue is just -dot); we still fill them with a
    #           harmless value so the kernel's reads are defined.
    comptime if has_nvidia_gpu_accelerator():
        # Host: fp16 query tile (padded to a multiple of BM rows so the kernel's
        # full-BM q tile is always in bounds), fp32 query norms, fp32 emb norms.
        var ntile = ((M + TC_BM - 1) // TC_BM) * TC_BM
        var qh16 = alloc[Float16](ntile * K)
        var qnorm_h = alloc[Float32](ntile)
        for m in range(M):
            var qoff = m * K
            var s = Float32(0)
            for i in range(K):
                var v = qs[qoff + i]
                qh16[qoff + i] = v.cast[DType.float16]()
                s += v * v
            # COSINE wants the L2 norm; L2/IP want the squared norm (L2) or do
            # not read it (IP). For L2 we keep s (= |q|^2); for cosine sqrt(s).
            if metric == TC_METRIC_COSINE:
                qnorm_h[m] = sqrt(s)
            else:
                qnorm_h[m] = s
        # Pad rows [M, ntile) with zeros (qnorm 1 so the cosine denom != 0;
        # padded queries' results are discarded by the qcount guard).
        for m in range(M, ntile):
            var qoff = m * K
            for i in range(K):
                qh16[qoff + i] = Float16(0)
            qnorm_h[m] = Float32(1)

        var qs_dev = ctx.enqueue_create_buffer[DType.float16](ntile * K)
        var qnorm_dev = ctx.enqueue_create_buffer[DType.float32](ntile)
        var enorm_dev = ctx.enqueue_create_buffer[DType.float32](n_rows)
        ctx.synchronize()
        ctx.enqueue_copy(qs_dev, qh16)
        var qnorm_imm = UnsafePointer[Float32, ImmUntrackedOrigin](
            unsafe_from_address=Int(qnorm_h)
        )
        ctx.enqueue_copy(qnorm_dev, qnorm_imm)

        # Emb norms: for COSINE, sqrt(sum of squares of the fp16-stored values)
        # -- bit-identical to the scalar f16 path's denom (na += av*av, av =
        # emb_half.cast[fp32]()). For L2 we want the SQUARED norm (skip the
        # sqrt). IP does not read enorm. The kernel takes a `squared` flag.
        var enorm_squared = Int(1) if metric != TC_METRIC_COSINE else Int(0)
        ctx.enqueue_function[_tc_enorm_kernel](
            emb16.unsafe_ptr(),
            enorm_dev.unsafe_ptr(),
            Int64(n_rows),
            Int64(K),
            Int64(enorm_squared),
            grid_dim=n_rows,
            block_dim=WARP_SIZE)
        ctx.synchronize()

        # Dispatch the runtime metric to the comptime kernel instantiation. Each
        # (K, metric) pair is a separate specialization; only the listed K are
        # supported (guard with tc_knn_supported in the caller).
        if metric == TC_METRIC_L2:
            if K == 768:
                _run_tc_knn_for_kd[768, TC_METRIC_L2](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 384:
                _run_tc_knn_for_kd[384, TC_METRIC_L2](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1024:
                _run_tc_knn_for_kd[1024, TC_METRIC_L2](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1536:
                _run_tc_knn_for_kd[1536, TC_METRIC_L2](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            else:
                raise Error("tc_knn: unsupported K (guard tc_knn_supported)")
        elif metric == TC_METRIC_IP:
            if K == 768:
                _run_tc_knn_for_kd[768, TC_METRIC_IP](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 384:
                _run_tc_knn_for_kd[384, TC_METRIC_IP](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1024:
                _run_tc_knn_for_kd[1024, TC_METRIC_IP](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1536:
                _run_tc_knn_for_kd[1536, TC_METRIC_IP](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            else:
                raise Error("tc_knn: unsupported K (guard tc_knn_supported)")
        else:
            if K == 768:
                _run_tc_knn_for_kd[768, TC_METRIC_COSINE](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 384:
                _run_tc_knn_for_kd[384, TC_METRIC_COSINE](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1024:
                _run_tc_knn_for_kd[1024, TC_METRIC_COSINE](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            elif K == 1536:
                _run_tc_knn_for_kd[1536, TC_METRIC_COSINE](
                    ctx, emb16, qs_dev, qnorm_dev, enorm_dev,
                    n_rows, M, k, out_ids, out_dists,
                )
            else:
                raise Error("tc_knn: unsupported K (guard tc_knn_supported)")

        qh16.free()
        qnorm_h.free()


# Per-row norm of the fp16-resident matrix (one warp per row, warp-strided).
# `squared == 0` returns the L2 norm (sqrt of sum of (fp16-cast-to-fp32)^2) --
# matching the scalar f16 cosine denom exactly. `squared != 0` returns the
# SQUARED norm (sum of squares, no sqrt) for the L2-distance epilogue. NVIDIA
# -only via the comptime gate.
def _tc_enorm_kernel(
    emb: UnsafePointer[Scalar[DType.float16], MutUntrackedOrigin],
    enorm: UnsafePointer[Scalar[DType.float32], MutUntrackedOrigin],
    n_rows_dp: Int64,
    K_dp: Int64,
    squared_dp: Int64,
):
    var n_rows = Int(n_rows_dp)
    var K = Int(K_dp)
    var squared = Int(squared_dp)
    comptime if is_nvidia_gpu():
        from std.gpu.primitives import warp

        var row = Int(block_idx.x)
        if row >= n_rows:
            return
        var lane = Int(thread_idx.x)
        var base = row * K
        var na = Float32(0)
        var i = lane
        while i < K:
            var av = emb[base + i].cast[DType.float32]()
            na += av * av
            i += WARP_SIZE
        na = warp.sum(na)
        if lane == 0:
            enorm[row] = na if squared != 0 else sqrt(na)
