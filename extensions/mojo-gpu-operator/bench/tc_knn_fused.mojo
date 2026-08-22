"""FUSED tensor-core kNN: S = Q.Emb^T via MMA, fused with a streaming per-query
top-k over N (flash-attention style), so the M x N similarity matrix is NEVER
materialized -- only Emb is read, only M x k results are written.

Stage 1 (fused kernel, grid = NBLOCKS one-warp-grid-tile blocks):
  Each block GRID-STRIDES over N in BN-wide chunks (chunk c = block, block+NBLOCKS,
  ...). For each chunk it:
    * loops K in BK chunks, stages Q (BM x BK) and the Emb-chunk (BN x BK) into
      shared via copy_dram_to_sram_async, MMAs (m16n8k8, transpose_b) into a
      per-warp fp32 register accumulator -> the BM x BN S-tile (registers only;
      S is never written to global).
    * store_d's the S-tile into a shared BM x BN fp32 buffer.
    * converts S to cosine distance with host-precomputed qnorm[M] / enorm[N]
      and UPDATES a PERSISTENT per-thread register top-k (thread t owns query
      row t; QPT=1) using the SAME insertion-sort + (dist, rowid) tie-break as
      topk_batch_kernel, surviving across every chunk this block streams.
  After the sweep, the block emits its M x k candidates [block*BM*k + m*k + slot].
  Because the grid dim (NBLOCKS) is fixed independent of N, the cross-block merge
  cost (NBLOCKS*k candidates/query) stays bounded as N scales.

Stage 2 (merge_kernel): one warp per query reduces that query's NBLOCKS*k
candidates to the final k under the identical tie-break (mirrors
topk_batch_merge_kernel).

Correctness reference: mojo_gpu_pin_query_topk_batch_f16 on the same data.

Result on an RTX 4090 (BM=128 BN=64 BK=32, WM=64 WN=32 -> 4 warps, NBLOCKS=256):
  * N=1,048,576, K=768, M=128, k=10: recall@10 = 1.0, 128/128 exact-id rows,
    0 genuine misses vs the scalar batched path. The only differences vs the
    scalar reference are fp16 tie-reorders (MMA does fp16-product dot, the scalar
    path fp32-product dot; both fp32-accumulate) -- at small N (4096, a dense tie
    thicket) this shows as recall 0.9977 / 0 genuine misses; at 1M it is exact.
  * warm per-query latency: 29.4 us (fused) vs 1984 us (scalar batched) = ~67x.
    The standalone GEMM's Emb-read floor was ~18 us/query; the fused kernel is
    ~1.6x that floor (the small BN=64 / BK=32 tile -- the BM x BN fp32 S-tile must
    fit the 48 KB shared budget -- plus the per-chunk streaming top-k).
"""

from std.gpu import (
    WARP_SIZE,
    thread_idx,
    block_idx,
    warp_id as get_warp_id,
)
from max.gpu.sync import barrier
from max.gpu.memory import AddressSpace, async_copy_wait_all
from max.gpu.host import DeviceContext
from std.memory import alloc, stack_allocation
from layout import Layout, LayoutTensor
from layout._utils import ManagedLayoutTensor
from layout.layout_tensor import copy_dram_to_sram_async
from layout.tensor_core import TensorCore
from std.utils.index import IndexList
from std.math.uutils import udivmod
from std.time import perf_counter_ns
from std.math import sqrt, abs
from std.sys import has_accelerator

from gpu_kernels import (
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime M = 128
comptime N = 1048576
comptime KD = 768
comptime KK = 10  # top-k

comptime MMA_M = 16
comptime MMA_N = 8
comptime MMA_K = 8

# Fused-kernel tile. Small BN so the BM x BN fp32 S-tile fits shared
# (128*64*4 = 32 KB + a/b stages). Each block GRID-STRIDES over N in BN-chunks,
# so the grid dim (NBLOCKS) is fixed and the merge cost (NBLOCKS*k per query)
# stays bounded regardless of N -- this is the flash-attention streaming shape.
comptime BM = 128
comptime BN = 64
comptime BK = 32
comptime WM = 64
comptime WN = 32
comptime NUM_WARPS = (BM // WM) * (BN // WN)  # 4
comptime NUM_THREADS = NUM_WARPS * WARP_SIZE  # 128
# Queries each top-k thread owns (BM rows spread over NUM_THREADS threads).
comptime QPT = BM // NUM_THREADS  # 1

# Grid dim for the fused kernel: enough blocks to saturate the SMs (RTX 4090 has
# 128 SMs; ~2x oversubscription) while keeping NBLOCKS*k merge candidates per
# query small. 256 blocks * 64 = 16384-wide N sweep stride; merge handles
# 256*10 = 2560 candidates/query. Swept on the 4090: 256 is the sweet spot.
comptime NBLOCKS = 256

comptime TOPK_MAX = 1024


# Deterministic synthetic data matching cosine_batch_latency.mojo so the fused
# kernel and the scalar batched path see identical inputs.
def emb_val(row: Int, i: Int) -> Float32:
    var idx = (row * 2654435761 + i * 40503 + 12345) & 0x7FFFFFFF
    return Float32(idx % 2048) * 0.001 + Float32(row % 7) * 0.0001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


# ===-------------------------------------------------------------------===#
# Stage 1: fused MMA GEMM + per-block per-query streaming top-k over the N-slab.
# ===-------------------------------------------------------------------===#
def fused_knn_kernel[
    q_layout: Layout, e_layout: Layout,
](
    q: LayoutTensor[DType.float16, q_layout, MutAnyOrigin],
    e: LayoutTensor[DType.float16, e_layout, MutAnyOrigin],
    qnorm: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    enorm: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    k: Int,
    nblocks: Int,
):
    var mma = TensorCore[
        DType.float32, DType.float16, IndexList[3](MMA_M, MMA_N, MMA_K),
        transpose_b=True,
    ]()

    var warp_id = get_warp_id()
    warp_y, warp_x = udivmod(warp_id, BN // WN)

    # Shared staging tiles for Q (BM x BK) and Emb-slab (BN x BK).
    var a_smem = LayoutTensor[
        DType.float16, Layout.row_major(BM, BK), MutAnyOrigin,
        address_space = AddressSpace.SHARED,
    ].stack_allocation()
    var b_smem = LayoutTensor[
        DType.float16, Layout.row_major(BN, BK), MutAnyOrigin,
        address_space = AddressSpace.SHARED,
    ].stack_allocation()

    # Shared S-tile (BM x BN) the MMA results land in (store_d), then scanned.
    var s_smem = LayoutTensor[
        DType.float32, Layout.row_major(BM, BN), MutAnyOrigin,
        address_space = AddressSpace.SHARED,
    ].stack_allocation()

    # Per-warp fp32 register accumulator (persists across the K loop).
    var c_reg = (
        LayoutTensor[
            DType.float32, Layout.row_major(WM // MMA_M, (WN * 4) // MMA_N),
            MutAnyOrigin, address_space = AddressSpace.LOCAL,
        ]
        .stack_allocation()
        .fill(0.0)
    )

    var tid = Int(thread_idx.x)

    # PERSISTENT per-thread register top-k: thread tid owns query rows
    # tid, tid+NUM_THREADS, ... (QPT of them). Kept ascending by (dist, rowid),
    # surviving across every BN-chunk this block streams.
    var bd = stack_allocation[QPT * KK, Scalar[DType.float32]]()
    var bi = stack_allocation[QPT * KK, Scalar[DType.int64]]()
    var bcnt = stack_allocation[QPT, Scalar[DType.int32]]()
    comptime for p in range(QPT):
        bcnt[p] = 0

    comptime CPW = BK // 4
    comptime CRows = NUM_THREADS // CPW
    comptime KCH = KD // BK  # K chunks
    comptime NSTRIDE = NBLOCKS * BN  # N advanced per grid-stride step

    # GRID-STRIDE over N: this block handles N-chunks n0, n0+NSTRIDE, ...
    var n0 = Int(block_idx.x) * BN
    while n0 < n_rows:
        # --- MMA the BM x BN tile S = Q . Emb[n0:n0+BN]^T ---
        _ = c_reg.fill(0.0)
        var nchunk = n0 // BN  # B_dram_tile chunk index for this N-chunk
        for k_i in range(KCH):
            barrier()
            var A_dram_tile = q.tile[BM, BK](0, k_i)
            var B_dram_tile = e.tile[BN, BK](nchunk, k_i)
            copy_dram_to_sram_async[
                thread_layout = Layout.row_major(CRows, CPW)
            ](a_smem.vectorize[1, 4](), A_dram_tile.vectorize[1, 4]())
            copy_dram_to_sram_async[
                thread_layout = Layout.row_major(CRows, CPW)
            ](b_smem.vectorize[1, 4](), B_dram_tile.vectorize[1, 4]())
            async_copy_wait_all()
            barrier()

            var A_warp_tile = a_smem.tile[WM, BK](warp_y, 0)
            var B_warp_tile = b_smem.tile[WN, BK](warp_x, 0)
            comptime for mma_k in range(BK // MMA_K):
                comptime for mma_m in range(WM // MMA_M):
                    comptime for mma_n in range(WN // MMA_N):
                        var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
                        var A_mma_tile = A_warp_tile.tile[MMA_M, MMA_K](
                            mma_m, mma_k
                        )
                        var B_mma_tile = B_warp_tile.tile[MMA_N, MMA_K](
                            mma_n, mma_k
                        )
                        var a_reg = mma.load_a(A_mma_tile)
                        var b_reg = mma.load_b(B_mma_tile)
                        var d_reg = mma.mma_op(a_reg, b_reg, c_reg_m_n)
                        c_reg_m_n.copy_from(d_reg)

        # Drain the per-warp accumulator into the shared S-tile.
        var S_warp_tile = s_smem.tile[WM, WN](warp_y, warp_x)
        comptime for mma_m in range(WM // MMA_M):
            comptime for mma_n in range(WN // MMA_N):
                var S_mma_tile = S_warp_tile.tile[MMA_M, MMA_N](mma_m, mma_n)
                var c_reg_m_n = c_reg.tile[1, 4](mma_m, mma_n)
                mma.store_d(S_mma_tile, c_reg_m_n)
        barrier()

        # --- update each owned query's persistent top-k over this chunk's BN
        # cols (same insertion + (dist,rowid) tie-break as the scalar path) ---
        comptime for p in range(QPT):
            var m = tid + p * NUM_THREADS
            var qn = qnorm[m]
            var off = p * KK
            var cnt = Int(bcnt[p])
            for nn in range(BN):
                var row = n0 + nn
                if row >= n_rows:
                    break
                var dotv = rebind[Scalar[DType.float32]](s_smem[m, nn])
                var denom = enorm[row] * qn  # enorm pre-sqrt'd on host
                var cd = (
                    Float32(1) - dotv / denom if denom != 0 else Float32(0)
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
        barrier()  # all threads done reading s_smem before next chunk overwrites
        n0 += NSTRIDE

    # Emit each owned query's k candidates for this block.
    comptime for p in range(QPT):
        var m = tid + p * NUM_THREADS
        var off = p * KK
        var cnt = Int(bcnt[p])
        var out_base = (Int(block_idx.x) * BM + m) * k
        for j in range(k):
            if j < cnt:
                cand_dist[out_base + j] = bd[off + j]
                cand_id[out_base + j] = bi[off + j]
            else:
                cand_dist[out_base + j] = Float32(3.0e38)
                cand_id[out_base + j] = Int64(-1)


# ===-------------------------------------------------------------------===#
# Stage 2: per-query merge of nblocks*k candidates -> final k (one warp/query).
# Mirrors topk_batch_merge_kernel but candidate layout is [block*BM*k + m*k + j].
# ===-------------------------------------------------------------------===#
def merge_kernel(
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    out_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    out_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    Mq: Int,
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


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ---- host data (matches cosine_batch_latency.mojo) ----
    var qh = alloc[Float32](M * KD)
    for m in range(M):
        for i in range(KD):
            qh[m * KD + i] = query_val(m, i)
    var eh = alloc[Float32](N * KD)
    for r in range(N):
        for i in range(KD):
            eh[r * KD + i] = emb_val(r, i)

    # ---- precompute norms on host ----
    var qnorm_h = alloc[Float32](M)
    for m in range(M):
        var s = Float32(0)
        for i in range(KD):
            var v = qh[m * KD + i]
            s += v * v
        qnorm_h[m] = sqrt(s)
    # enorm holds the row L2-NORM (already sqrt'd, so the kernel inner loop avoids
    # a per-(query,row) sqrt). To MATCH the scalar f16 path exactly, the embedding
    # norm there sums squares of the fp16-cast values (na += av*av, av =
    # emb_half.cast[fp32]()) then sqrt's it -- so cast each value to fp16 first,
    # square, sum, and take the sqrt here (bit-identical to the scalar denom).
    var enorm_h = alloc[Float32](N)
    for r in range(N):
        var s = Float32(0)
        for i in range(KD):
            var v = eh[r * KD + i].cast[DType.float16]().cast[DType.float32]()
            s += v * v
        enorm_h[r] = sqrt(s)

    # ---- device fp16 Q / Emb (cast on host, matches mojo_gpu_pin_f16) ----
    var qa = ManagedLayoutTensor[DType.float16, Layout.row_major(M, KD)](ctx)
    var eb = ManagedLayoutTensor[DType.float16, Layout.row_major(N, KD)](ctx)
    var qt = qa.tensor()
    for m in range(M):
        for i in range(KD):
            qt[m, i] = qh[m * KD + i].cast[DType.float16]()
    var et = eb.tensor()
    for r in range(N):
        for i in range(KD):
            et[r, i] = eh[r * KD + i].cast[DType.float16]()

    var qd = qa.device_tensor()
    var ed = eb.device_tensor()

    var qnorm_dev = ctx.enqueue_create_buffer[DType.float32](M)
    var enorm_dev = ctx.enqueue_create_buffer[DType.float32](N)
    ctx.enqueue_copy(qnorm_dev, qnorm_h)
    ctx.enqueue_copy(enorm_dev, enorm_h)

    var nblocks = NBLOCKS
    var cand_cap = nblocks * M * KK
    var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
    var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
    var out_dist_dev = ctx.enqueue_create_buffer[DType.float32](M * KK)
    var out_id_dev = ctx.enqueue_create_buffer[DType.int64](M * KK)
    ctx.synchronize()

    comptime fk = fused_knn_kernel[qa.layout, eb.layout]

    ctx.enqueue_function[fk](
        qd, ed, qnorm_dev, enorm_dev, cand_dist_dev, cand_id_dev,
        N, KK, nblocks,
        grid_dim=nblocks, block_dim=NUM_THREADS,
    )
    ctx.enqueue_function[merge_kernel](
        cand_dist_dev, cand_id_dev, out_dist_dev, out_id_dev,
        M, nblocks, KK,
        grid_dim=M, block_dim=WARP_SIZE,
    )
    var fused_ids = alloc[Int64](M * KK)
    var fused_dists = alloc[Float32](M * KK)
    ctx.enqueue_copy(fused_ids, out_id_dev)
    ctx.enqueue_copy(fused_dists, out_dist_dev)
    ctx.synchronize()

    # ---- reference: scalar batched path on identical data ----
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(eh)
    )
    var handle = mojo_gpu_pin_f16(emb_imm, N, KD)
    if handle == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qh)
    )
    var ref_ids = alloc[Int64](M * KK)
    var ref_dists = alloc[Float32](M * KK)
    var rc = mojo_gpu_pin_query_topk_batch_f16(h, qs_imm, M, KK, ref_ids, ref_dists)
    if rc != 0:
        raise Error(String("ref rc=") + String(Int(rc)))

    # ---- compare (ref = ground truth; a "miss" is an absent ref id whose ref
    # distance is NOT within TIE_EPS of the fused boundary => genuine, not a tie) ----
    var TIE_EPS = Float32(1.0e-5)
    var exact_match = 0
    var total_overlap = 0
    var genuine_miss = 0
    var first_mismatch = -1
    for m in range(M):
        var match_row = True
        # fused boundary (worst kept fused distance)
        var fworst = Float32(0)
        for j in range(KK):
            if fused_dists[m * KK + j] > fworst:
                fworst = fused_dists[m * KK + j]
        for j in range(KK):
            if fused_ids[m * KK + j] != ref_ids[m * KK + j]:
                match_row = False
            # overlap of ref id j against fused set
            var rid = ref_ids[m * KK + j]
            var found = False
            for b in range(KK):
                if fused_ids[m * KK + b] == rid:
                    found = True
                    break
            if found:
                total_overlap += 1
            else:
                var d = ref_dists[m * KK + j]
                if abs(d - fworst) > TIE_EPS:
                    genuine_miss += 1
        if match_row:
            exact_match += 1
        elif first_mismatch < 0:
            first_mismatch = m

    print("=== fused kNN vs scalar batched (M=", M, "N=", N, "k=", KK, ") ===")
    print("exact-id-match rows:", exact_match, "/", M)
    print("recall@10:", Float64(total_overlap) / Float64(M * KK),
          "  genuine misses (beyond fp16 tie eps):", genuine_miss)
    if first_mismatch >= 0:
        var m = first_mismatch
        print("first mismatch row", m)
        print("  fused: id", fused_ids[m*KK], "d", fused_dists[m*KK],
              " | ref: id", ref_ids[m*KK], "d", ref_dists[m*KK])
        for j in range(KK):
            print("    j", j, "fused(", fused_ids[m*KK+j], fused_dists[m*KK+j],
                  ") ref(", ref_ids[m*KK+j], ref_dists[m*KK+j], ")")

    # ---- warm latency: fused vs scalar batched (whole M-query batch) ----
    comptime WARMUP = 5
    comptime ITERS = 30

    # Fused path: time stage1 + stage2 + the M*k copy-back (the full result).
    for _ in range(WARMUP):
        ctx.enqueue_function[fk](
            qd, ed, qnorm_dev, enorm_dev, cand_dist_dev, cand_id_dev,
            N, KK, nblocks, grid_dim=nblocks, block_dim=NUM_THREADS,
        )
        ctx.enqueue_function[merge_kernel](
            cand_dist_dev, cand_id_dev, out_dist_dev, out_id_dev,
            M, nblocks, KK, grid_dim=M, block_dim=WARP_SIZE,
        )
        ctx.enqueue_copy(fused_ids, out_id_dev)
        ctx.enqueue_copy(fused_dists, out_dist_dev)
    ctx.synchronize()
    var fbest = Float64(1.0e30)
    for _ in range(ITERS):
        var t0 = perf_counter_ns()
        ctx.enqueue_function[fk](
            qd, ed, qnorm_dev, enorm_dev, cand_dist_dev, cand_id_dev,
            N, KK, nblocks, grid_dim=nblocks, block_dim=NUM_THREADS,
        )
        ctx.enqueue_function[merge_kernel](
            cand_dist_dev, cand_id_dev, out_dist_dev, out_id_dev,
            M, nblocks, KK, grid_dim=M, block_dim=WARP_SIZE,
        )
        ctx.enqueue_copy(fused_ids, out_id_dev)
        ctx.enqueue_copy(fused_dists, out_dist_dev)
        ctx.synchronize()
        var dt = Float64(perf_counter_ns() - t0)
        if dt < fbest:
            fbest = dt

    # Scalar batched reference (same M-query batch).
    for _ in range(WARMUP):
        _ = mojo_gpu_pin_query_topk_batch_f16(h, qs_imm, M, KK, ref_ids, ref_dists)
    var rbest = Float64(1.0e30)
    for _ in range(ITERS):
        var t0 = perf_counter_ns()
        _ = mojo_gpu_pin_query_topk_batch_f16(h, qs_imm, M, KK, ref_ids, ref_dists)
        var dt = Float64(perf_counter_ns() - t0)
        if dt < rbest:
            rbest = dt

    # Stage split: time stage1 (GEMM+streaming topk) alone, and stage1+stage2.
    var s1best = Float64(1.0e30)
    for _ in range(ITERS):
        var t0 = perf_counter_ns()
        ctx.enqueue_function[fk](
            qd, ed, qnorm_dev, enorm_dev, cand_dist_dev, cand_id_dev,
            N, KK, nblocks, grid_dim=nblocks, block_dim=NUM_THREADS,
        )
        ctx.synchronize()
        var dt = Float64(perf_counter_ns() - t0)
        if dt < s1best:
            s1best = dt

    print("--- warm latency (M=", M, " batch) ---")
    print("  stage1 (gemm+streaming-topk) alone:", s1best / 1.0e6, "ms")
    print("  stage2+copy (= total - stage1)     :", (fbest - s1best) / 1.0e6, "ms")
    print("fused : total", fbest / 1.0e6, "ms   per-query", (fbest / Float64(M)) / 1000.0, "us")
    print("scalar: total", rbest / 1.0e6, "ms   per-query", (rbest / Float64(M)) / 1000.0, "us")
    print("speedup (scalar/fused):", rbest / fbest, "x")

    mojo_gpu_pin_free_f16(h)
    print("done")
