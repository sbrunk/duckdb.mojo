"""Top-k cosine kNN on Apple Silicon (M1-M4) using the 8x8 `simdgroup_matrix` MMA.

Apple's counterpart to `tc_knn.mojo` (NVIDIA). A tiled MMA of `queries · embᵀ`
is fused with a per-query top-k kept in registers. The kernel reads the
embeddings once and writes M x k distances. Cosine metric, unit-normalized
inputs. The tie-break and the stage-2 merge follow the NVIDIA kernel, so a
result equals the output of a scalar single-query top-k.

One 32-thread block handles one grid-stride chunk of the embedding matrix.
`qsub` sets the tile height to qsub * 8 queries and the driver picks it from the
batch size. The driver pads the query tile to a whole number of tiles, so a batch
of one query runs a 16-row tile instead of a 128-row one. The reciprocal L2 norms
of the embeddings depend only on the pinned fp16 bytes, so the driver caches them
per pin and scans the matrix once per pin.

Apple-specific code sits inside `comptime if is_apple_gpu()` in the kernel and
`default_accelerator().is_apple_gpu()` on the host. `layout.TensorCore` has no
Apple support, so this file calls the LLVM intrinsic directly.
"""

from std.sys import llvm_intrinsic
from std.sys.info import is_apple_gpu, default_accelerator
from max.gpu import lane_id, block_idx, thread_idx
from max.gpu.primitives.warp import shuffle_xor
from max.gpu.memory import AddressSpace
from max.gpu.sync import barrier
from max.gpu.host import DeviceContext, DeviceBuffer
from std.memory import stack_allocation
from std.memory.alloc import alloc, Layout
from std.math import sqrt


# ===-------------------------------------------------------------------===#
# 8x8 simdgroup-matrix primitives, following Modular's
# `.repos/modular/.../apple/matmul_8x8.mojo` (`_frag8_layout`, `_mma8x8`).
# bench/apple_tc_knn_test.mojo checks them against a scalar reference.
# ===-------------------------------------------------------------------===#
comptime AP_MMA = 8  # 8x8x8 simdgroup-matrix shape
comptime FRAG8 = 2  # 8x8 = 64 elems / 32 lanes = 2 per lane

# Query-tile amortization: a block loads each 8-row Emb chunk once and MMAs it
# against AP_QSUB query blocks of 8 queries, so AP_MM = AP_QSUB*8 queries share
# one Emb-chunk read. The fp32 accumulator is AP_QSUB FRAG8 vectors, one 8x8 S
# sub-tile per query block. AP_QSUB is the largest tile and the default, and the
# driver passes a smaller `qsub` to the kernel for small batches.
comptime AP_QSUB = 16  # query blocks of 8 per Emb pass (largest tile)
comptime AP_MM = AP_QSUB * AP_MMA  # 128 queries per M-tile (largest tile)
comptime AP_MN = 8  # embedding rows per MMA step / N-chunk
# Grid-stride blocks over N, one simdgroup each. Stage 2 merges nblocks*k
# candidates per query, so more blocks mean more merge traffic at fixed k, and
# stage 1 is unaffected.
comptime AP_NBLOCKS = 1024  # measured better than 2048 on M3
comptime AP_THREADS = 32  # one simdgroup per block
# Largest k the per-query shared top-k supports. The shared footprint grows with
# the tile height and the largest tile has to fit Apple's ~32 KB threadgroup:
#   sd    AP_MM*AP_K_CAP*4 = 8 KB
#   si    AP_MM*AP_K_CAP*4 = 8 KB
#   scnt  AP_MM*4          = 0.5 KB   => ~16.5 KB total
comptime AP_K_CAP = 16


# Whether the Apple fused path supports this (K, k). K must be a multiple of the
# 8x8 MMA K dim, and the tested dispatch uses K%16==0, so this requires
# K%16==0. k must fit the shared top-k cap.
def tc_knn_apple_supported(K: Int, k: Int) -> Bool:
    if k <= 0 or k > AP_K_CAP:
        return False
    if K <= 0 or K % 16 != 0:
        return False
    # Match tc_knn's supported embedding dims so the routing is symmetric.
    if K == 768 or K == 384 or K == 1024 or K == 1536:
        return True
    return False


def _frag8_layout(lane: Int) -> Tuple[Int, Int]:
    """Apple 8x8 simdgroup-matrix per-lane layout (checked against Metal
    `thread_elements()`). Lane owns (row, col_base) and (row, col_base+1)."""
    return (
        ((lane & 6) >> 1) + ((lane & 16) >> 2),
        ((lane & 1) << 1) + ((lane & 8) >> 1),
    )


def _mma8x8[
    a_type: DType, b_type: DType
](
    a: SIMD[a_type, FRAG8],
    b: SIMD[b_type, FRAG8],
    c: SIMD[DType.float32, FRAG8],
) -> SIMD[DType.float32, FRAG8]:
    """One 8x8x8 simdgroup-matrix multiply-accumulate: D = A @ B + C."""
    return llvm_intrinsic[
        "llvm.air.simdgroup_matrix_8x8_multiply_accumulate",
        SIMD[DType.float32, FRAG8],
    ](a, b, c)


# Sentinels for the unused slots of a stage-1 top-k list. SENT_D sits far below
# any real score (|dot * renorm| <= ~1e3) and SENT_I is larger than every row
# id, so a slot holding a sentinel sorts last.
comptime SENT_D = -1.0e30
comptime SENT_I = 2147483647


@inline(.always)
def _selv(b: Bool, t: Float32, f: Float32) -> Float32:
    return t if b else f


@inline(.always)
def _selvi(b: Bool, t: Int32, f: Int32) -> Int32:
    return t if b else f


# ===-------------------------------------------------------------------===#
# Stage 1: one pass over the embedding matrix (fused 8x8 MMA Q.Emb^T) that folds
# each score into a per-query candidate list as it is produced.
#
# q: MM x K fp16 tile with this M-tile's queries, padded to MM rows on the
# host. The tile height MM = qsub * 8 comes from the comptime parameter
# `qsub`, the number of 8-row query blocks per Emb pass. The host driver picks
# qsub from the batch size.
# e: the full N x K fp16 matrix, resident on the device.
# qrecip/renorm: fp32 reciprocal L2 norms (1/||q||, 1/||emb||) computed on the
# host, so a score is dot * renorm and the inner loop uses only multiplies.
# Candidates are written row-major as [block*MM*k + m*k + j], the layout the
# NVIDIA emit and the stage-2 merge expect, with distance 1 - score * qrecip.
# Apple only: gated by the in-kernel `is_apple_gpu()`.
# ===-------------------------------------------------------------------===#
def tc_apple_fused_knn_kernel[qsub: Int](
    q: Pointer[Scalar[DType.float16], MutUntrackedOrigin],
    e: Pointer[Scalar[DType.float16], MutUntrackedOrigin],
    qrecip: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    renorm: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_dist: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_id: Pointer[Scalar[DType.int64], MutUntrackedOrigin],
    n_rows_dp: Int64,
    K_dp: Int64,
    k_dp: Int64,
    nblocks_dp: Int64,
):
    var n_rows = Int(n_rows_dp)
    var K = Int(K_dp)
    var k = Int(k_dp)
    var nblocks = Int(nblocks_dp)
    var mm = qsub * AP_MMA  # query rows per tile (qsub is comptime)
    comptime if is_apple_gpu():
        var lane = Int(lane_id())
        # This lane's two elements of the 8x8 fragment: (frow, fcol) and
        # (frow, fcol+1).
        var frow = ((lane & 6) >> 1) + ((lane & 16) >> 2)
        var fcol = ((lane & 1) << 1) + ((lane & 8) >> 1)
        var pos = (lane & 1) + ((lane & 8) >> 2)

        # Per-query top-k lists: sd[q*k + slot] is the score, si[...] the row
        # id, ordered by score descending then id ascending, and scnt[q] is the
        # list length. Row m = qb*8 + frow belongs to the lane with pos == qb&3
        # and frow == m&7, so each list has exactly one writer.
        var sd = stack_allocation[
            qsub * AP_MMA * AP_K_CAP,
            Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            qsub * AP_MMA * AP_K_CAP,
            Scalar[DType.int32],
            address_space = AddressSpace.SHARED,
        ]()
        var scnt = stack_allocation[
            qsub * AP_MMA,
            Scalar[DType.int32],
            address_space = AddressSpace.SHARED,
        ]()
        # Init the lists this lane owns (rows qb*8+frow for qb = pos+4h): count 0
        # and sentinels in every slot. The insert writes all 16 slots, which keeps
        # that state consistent for the rest of the kernel.
        var ci0 = lane
        while ci0 < mm:
            scnt[unsafe_offset=ci0] = 0
            ci0 += 32
        # ceil(qsub/4) half-rounds. The runtime `qb < qsub` guard skips the rounds
        # past the tile when qsub < 16.
        comptime for h0 in range((qsub + 3) >> 2):
            var qb0 = pos + 4 * h0
            if qb0 < qsub:
                var mi0 = (qb0 * AP_MMA + frow) * AP_K_CAP
                comptime for s0 in range(AP_K_CAP):
                    sd[unsafe_offset=mi0 + s0] = Float32(SENT_D)
                    si[unsafe_offset=mi0 + s0] = Int32(SENT_I)

        # Start of each query block for this lane (row qb*8+frow). Keeping
        # these outside the k-loop leaves the loop body with plain adds.
        var qoffs = Array[Int, qsub](fill=Int(0))
        comptime for qb0 in range(qsub):
            qoffs[qb0] = (qb0 * AP_MMA + frow) * K + fcol

        # Walk N in chunks of MN=8 rows, each block taking every nblocks-th
        # chunk.
        var nstride = nblocks * AP_MN
        var n0 = Int(block_idx.x) * AP_MN
        while n0 < n_rows:
            # --- MMA of this 8-row Emb chunk against all qsub query blocks.
            # B is the transposed chunk: its slot s differs in the N index
            # (n0+fcol+s) and the K coordinate is frow. Rows past n_rows read
            # as zero. ---
            var acc = Array[SIMD[DType.float32, FRAG8], qsub](
                fill=SIMD[DType.float32, FRAG8](0)
            )
            var g0 = n0 + fcol
            var g1 = g0 + 1
            var ok0 = g0 < n_rows
            var ok1 = g1 < n_rows
            var ebase0 = e.unsafe_offset(g0 * K + frow)
            var ebase1 = e.unsafe_offset(g1 * K + frow)
            var b0 = Float16(0)
            var b1 = Float16(0)
            if ok0:
                b0 = ebase0[unsafe_offset=0]
            if ok1:
                b1 = ebase1[unsafe_offset=0]
            var ks = 0
            while ks < K:
                # Load the B pair for the next k-step before the MMAs that consume
                # the current one, which hides the load behind MMA issue. A step
                # past K repeats the current offset and the extra load is dropped.
                var kn = ks + AP_MMA
                if kn >= K:
                    kn = ks
                var nb0 = b0
                var nb1 = b1
                if ok0:
                    nb0 = ebase0[unsafe_offset=kn]
                if ok1:
                    nb1 = ebase1[unsafe_offset=kn]
                var bf = SIMD[DType.float16, FRAG8](b0, b1)
                b0 = nb0
                b1 = nb1
                comptime for qb in range(qsub):
                    var af = q.unsafe_offset(qoffs[qb] + ks).unsafe_load[width=FRAG8]()
                    acc[qb] = _mma8x8[DType.float16, DType.float16](af, bf, acc[qb])
                ks += AP_MMA

            # --- epilogue: for each query block, six single element fp32 shuffles
            # move that row's 8 emb-column scores to one lane. Lanes l, l^1, l^8
            # and l^9 share frow and hold two columns each. The owner lane merges
            # all 8 into the list it owns, so a list has one writer and needs no
            # barrier and no shared S-tile. ---
            var nv = n_rows - n0
            if nv > AP_MMA:
                nv = AP_MMA
            var r0 = Float32(0)
            var r1 = Float32(0)
            if ok0:
                r0 = renorm[unsafe_offset=g0]
            if ok1:
                r1 = renorm[unsafe_offset=g1]
            comptime for qb in range(qsub):
                var own0 = acc[qb][0] * r0
                var own1 = acc[qb][1] * r1
                var v0 = SIMD[DType.float32, 1](own0)
                var v1 = SIMD[DType.float32, 1](own1)
                var s1 = shuffle_xor(v0, UInt32(1))[0]
                var s2 = shuffle_xor(v0, UInt32(8))[0]
                var s3 = shuffle_xor(v0, UInt32(9))[0]
                var s4 = shuffle_xor(v1, UInt32(1))[0]
                var s5 = shuffle_xor(v1, UInt32(8))[0]
                var s6 = shuffle_xor(v1, UInt32(9))[0]
                if pos == (qb & 3):
                    var m = qb * AP_MMA + frow
                    var off = m * AP_K_CAP
                    var cnt = Int(scnt[unsafe_offset=m])
                    var wd = Float32(SENT_D)
                    var wi = Int32(SENT_I)
                    if cnt >= k:
                        wd = sd[unsafe_offset=off + k - 1]
                        wi = si[unsafe_offset=off + k - 1]
                    var cds = SIMD[DType.float32, 8](own0, s1, s2, s3, own1, s4, s5, s6)
                    var cis = SIMD[DType.int32, 8](
                        Int32(n0 + fcol),
                        Int32(n0 + (fcol ^ 2)),
                        Int32(n0 + (fcol ^ 4)),
                        Int32(n0 + (fcol ^ 6)),
                        Int32(n0 + fcol + 1),
                        Int32(n0 + (fcol ^ 2) + 1),
                        Int32(n0 + (fcol ^ 4) + 1),
                        Int32(n0 + (fcol ^ 6) + 1),
                    )
                    var okm = cis.lt(SIMD[DType.int32, 8](Int32(n_rows)))
                    cds = okm.select(cds, SIMD[DType.float32, 8](Float32(SENT_D)))
                    cis = okm.select(cis, SIMD[DType.int32, 8](Int32(SENT_I)))
                    # Skip the insert when the list is full and all 8 candidates
                    # rank below its last slot.
                    var acc = cnt < k
                    if not acc:
                        comptime for j in range(8):
                            if (cds[j] > wd) or ((cds[j] == wd) and (cis[j] < wi)):
                                acc = True
                    if acc:
                        # Insert by compare-and-select: each candidate moves through
                        # the 16 slots and swaps with every slot it out-ranks.
                        # Out-of-range rows already carry sentinels, and every slot
                        # is written back.
                        var ld = Array[Float32, AP_K_CAP](fill=Float32(SENT_D))
                        var li = Array[Int32, AP_K_CAP](fill=Int32(SENT_I))
                        comptime for s in range(AP_K_CAP):
                            ld[s] = sd[unsafe_offset=off + s]
                            li[s] = si[unsafe_offset=off + s]
                        comptime for j in range(8):
                            var cd = cds[j]
                            var ci = cis[j]
                            comptime for s in range(AP_K_CAP):
                                var pd = ld[s]
                                var pi = li[s]
                                var beat = (cd > pd) or ((cd == pd) and (ci < pi))
                                ld[s] = _selv(beat, cd, pd)
                                li[s] = _selvi(beat, ci, pi)
                                cd = _selv(beat, pd, cd)
                                ci = _selvi(beat, pi, ci)
                        var newcnt = cnt + nv
                        if newcnt > k:
                            newcnt = k
                        comptime for s in range(AP_K_CAP):
                            var keep = s < newcnt
                            sd[unsafe_offset=off + s] = _selv(
                                keep, ld[s], Float32(SENT_D)
                            )
                            si[unsafe_offset=off + s] = _selvi(
                                keep, li[s], Int32(SENT_I)
                            )
                        cnt = newcnt
                    scnt[unsafe_offset=m] = Int32(cnt)
            n0 += nstride

        # --- emit: each lane writes the rows it owns (qb = pos+4h), k candidates
        # each, and turns a score into a cosine distance. Output layout is
        # [(block*MM + m)*k + j]. The `qb < qsub` guard leaves the rows past the
        # tile, which exist when qsub < 16, untouched. ---
        comptime for h in range((qsub + 3) >> 2):
            var qb = pos + 4 * h
            if qb < qsub:
                var m = qb * AP_MMA + frow
                var cnt = Int(scnt[unsafe_offset=m])
                var off = m * AP_K_CAP
                var qr = qrecip[unsafe_offset=m]
                var out_base = (Int(block_idx.x) * mm + m) * k
                for j in range(k):
                    if j < cnt:
                        cand_dist[unsafe_offset=out_base + j] = Float32(
                            1
                        ) - sd[unsafe_offset=off + j] * qr
                        cand_id[unsafe_offset=out_base + j] = Int64(
                            si[unsafe_offset=off + j]
                        )
                    else:
                        cand_dist[unsafe_offset=out_base + j] = Float32(3.0e38)
                        cand_id[unsafe_offset=out_base + j] = Int64(-1)


# ===-------------------------------------------------------------------===#
# Stage 2: reduce each query's nblocks*k candidates to its final k. One
# simdgroup (one block) per query. The candidate layout [(block*MM + m)*k + j]
# matches stage 1's emit. Apple-only via the in-kernel gate.
#
# One lane would have to scan nblocks*k candidates on its own, so the merge runs
# across the warp.
#   * Lane l scans the stride-32 stripe c = l, l+32, ... of this query's
#     candidates and keeps that stripe's top-k in registers by distance
#     ascending, then id ascending. Consecutive lanes read consecutive
#     candidates, so every step is coalesced. Sentinels (id < 0, dist 3e38)
#     are skipped. Splitting by stripe keeps every entry of the global top-k: a
#     global top-k entry is out-ranked by at most k-1 candidates overall, and
#     therefore by at most k-1 inside its own stripe.
#   * The 32 stripe lists go to threadgroup memory (32*AP_K_CAP fp32 scores and
#     int32 ids, 4 KB), then a barrier, and lane 0 merges the 32*k partials with
#     the (dist asc, id asc) insert logic the NVIDIA merge uses and writes the
#     final k.
# The candidate distances are cosine distances, so ascending is better. Stage 1
# keeps its internal lists score-descending and converts them at emit.
# ===-------------------------------------------------------------------===#
def tc_apple_merge_kernel(
    cand_dist: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    cand_id: Pointer[Scalar[DType.int64], MutUntrackedOrigin],
    out_dist: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    out_id: Pointer[Scalar[DType.int64], MutUntrackedOrigin],
    Mq_dp: Int64,
    nblocks_dp: Int64,
    k_dp: Int64,
):
    var Mq = Int(Mq_dp)
    var nblocks = Int(nblocks_dp)
    var k = Int(k_dp)
    comptime if is_apple_gpu():
        # Stripe partials (lane l owns [l*CAP, l*CAP+CAP)) and, after the
        # barrier, lane 0's merge reads them through these same buffers.
        var sd = stack_allocation[
            AP_K_CAP * 32, Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            AP_K_CAP * 32, Scalar[DType.int32],
            address_space = AddressSpace.SHARED,
        ]()

        var lane = Int(lane_id())
        var mq = Int(block_idx.x)  # this block merges query mq
        if mq >= Mq:
            return

        # --- phase 1: lane l reduces its stripe to a register top-k. ---
        var pd = Array[Float32, AP_K_CAP](fill=Float32(SENT_D))
        var pi = Array[Int32, AP_K_CAP](fill=Int32(SENT_I))
        var pcnt = 0
        var ncand = nblocks * k
        var c = lane
        while c < ncand:
            var idx = ((c // k) * Mq + mq) * k + (c % k)
            var cid64 = cand_id[unsafe_offset=idx]
            if cid64 >= 0:
                # Stripe lists keep int32 ids: the pinned matrix has fewer than
                # 2^31 rows and stage 1 emits int32 ids.
                var cid = Int32(cid64)
                var cd = cand_dist[unsafe_offset=idx]
                var accept = True
                if pcnt >= k:
                    if cd > pd[pcnt - 1] or (
                        (cd == pd[pcnt - 1]) and (cid >= pi[pcnt - 1])
                    ):
                        accept = False
                if accept:
                    var pos = pcnt if pcnt < k else k - 1
                    while pos > 0:
                        if pd[pos - 1] > cd or (
                            (pd[pos - 1] == cd) and (pi[pos - 1] > cid)
                        ):
                            pd[pos] = pd[pos - 1]
                            pi[pos] = pi[pos - 1]
                            pos -= 1
                        else:
                            break
                    pd[pos] = cd
                    pi[pos] = cid
                    if pcnt < k:
                        pcnt += 1
            c += 32

        # --- publish this lane's stripe top-k (sentinel-filled past pcnt). ---
        var po = lane * AP_K_CAP
        for j in range(AP_K_CAP):
            var keep = j < pcnt
            sd[unsafe_offset=po + j] = _selv(keep, pd[j], Float32(SENT_D))
            si[unsafe_offset=po + j] = _selvi(keep, pi[j], Int32(SENT_I))
        barrier()

        # --- phase 2: lane 0 merges the 32 stripe lists into the final k. ---
        if lane == 0:
            var fd = Array[Float32, AP_K_CAP](fill=Float32(SENT_D))
            var fi = Array[Int32, AP_K_CAP](fill=Int32(SENT_I))
            var fcnt = 0
            for e in range(32 * AP_K_CAP):
                var eid = si[unsafe_offset=e]
                if eid == Int32(SENT_I):
                    continue
                var ed = sd[unsafe_offset=e]
                var accept = True
                if fcnt >= k:
                    if ed > fd[fcnt - 1] or (
                        (ed == fd[fcnt - 1]) and (eid >= fi[fcnt - 1])
                    ):
                        accept = False
                if accept:
                    var pos = fcnt if fcnt < k else k - 1
                    while pos > 0:
                        if fd[pos - 1] > ed or (
                            (fd[pos - 1] == ed) and (fi[pos - 1] > eid)
                        ):
                            fd[pos] = fd[pos - 1]
                            fi[pos] = fi[pos - 1]
                            pos -= 1
                        else:
                            break
                    fd[pos] = ed
                    fi[pos] = eid
                    if fcnt < k:
                        fcnt += 1

            for j in range(k):
                if j < fcnt:
                    out_dist[unsafe_offset=mq * k + j] = fd[j]
                    out_id[unsafe_offset=mq * k + j] = Int64(fi[j])
                else:
                    out_dist[unsafe_offset=mq * k + j] = Float32(3.0e38)
                    out_id[unsafe_offset=mq * k + j] = Int64(-1)


# renorm[row] = 1 / L2 norm of row `row` of the fp16 matrix, 0 for all-zero
# rows. One simdgroup per row, the squares summed across lanes. Apple-only via
# the in-kernel gate.
def _tc_apple_enorm_kernel(
    emb: Pointer[Scalar[DType.float16], MutUntrackedOrigin],
    renorm: Pointer[Scalar[DType.float32], MutUntrackedOrigin],
    n_rows_dp: Int64,
    K_dp: Int64,
):
    var n_rows = Int(n_rows_dp)
    var K = Int(K_dp)
    comptime if is_apple_gpu():
        var row = Int(block_idx.x)
        if row >= n_rows:
            return
        var lane = Int(lane_id())
        # Each lane sums a strided part of the row, and the partials are combined
        # in shared memory.
        var part = stack_allocation[
            AP_THREADS, Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var base = row * K
        var na = Float32(0)
        var i = lane
        while i < K:
            var av = emb[unsafe_offset=base + i].cast[DType.float32]()
            na += av * av
            i += AP_THREADS
        part[unsafe_offset=lane] = na
        if lane == 0:
            var tot = Float32(0)
            for t in range(AP_THREADS):
                tot += part[unsafe_offset=t]
            var rt = sqrt(tot)
            renorm[unsafe_offset=row] = Float32(1) / rt if rt != 0 else Float32(0)


# ===-------------------------------------------------------------------===#
# Host driver. Builds the fp16 query tile and the fp32 reciprocal L2 norms of
# the queries on the host, uploads them, then walks M in MM-chunks. Each chunk
# runs the fused stage-1 kernel (one read of the full N matrix) and the stage-2
# merge, and writes its MM*k results into the caller's row-major M*k output.
#
# `renorm` and `renorm_ready` make up the per-pin norm cache held by the caller
# (PinStateF16 in gpu_kernels.mojo). The embedding norms depend only on the
# pinned fp16 bytes, so the first fused call for a pin allocates the buffer and
# runs the norm kernel, and later calls reuse it. The scan reads the whole
# N x K matrix.
# Apple only, cosine only, gated on the host by `default_accelerator().is_apple_gpu()`.
# ===-------------------------------------------------------------------===#
def run_tc_knn_apple_batch(
    ctx: DeviceContext,
    emb16: DeviceBuffer[DType.float16],
    mut renorm: Optional[DeviceBuffer[DType.float32]],
    mut renorm_ready: Int,
    n_rows: Int,
    K: Int,
    qs: Pointer[Float32, ImmUntrackedOrigin],
    M: Int,
    k: Int,
    out_ids: Pointer[Int64, MutUntrackedOrigin],
    out_dists: Pointer[Float32, MutUntrackedOrigin],
) raises:
    comptime if default_accelerator().is_apple_gpu():
        # Tile height follows the batch size, so a small batch runs a small tile.
        # The choice is fixed for the whole call, which keeps one qsub per launch.
        var qsub = 16
        var mm = AP_MM
        var nblocks = AP_NBLOCKS
        if M <= 16:
            qsub = 2
            mm = 16
            nblocks = 4096
        elif M <= 32:
            qsub = 4
            mm = 32
            nblocks = 2048

        # Host side: the fp16 query tile, padded to a whole mm multiple so the
        # kernel reads a full tile, and the fp32 query reciprocal norms
        # (1/||q||, 0 for a zero query).
        var ntile = ((M + mm - 1) // mm) * mm
        var qh16_mem = alloc(Layout[Float16](count=ntile * K)).into_managed()
        var qh16 = qh16_mem.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]()
        var qrecip_h_mem = alloc(Layout[Float32](count=ntile)).into_managed()
        var qrecip_h = qrecip_h_mem.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]()
        for m in range(M):
            var qoff = m * K
            var s = Float32(0)
            for i in range(K):
                var v = qs[unsafe_offset=qoff + i]
                qh16[unsafe_offset=qoff + i] = v.cast[DType.float16]()
                s += v * v
            var rq = sqrt(s)
            qrecip_h[unsafe_offset=m] = Float32(1) / rq if rq != 0 else Float32(0)
        for m in range(M, ntile):
            var qoff = m * K
            for i in range(K):
                qh16[unsafe_offset=qoff + i] = Float16(0)
            qrecip_h[unsafe_offset=m] = Float32(0)  # dropped by the qcount guard

        var qs_dev = ctx.enqueue_create_buffer[DType.float16](ntile * K)
        var qrecip_dev = ctx.enqueue_create_buffer[DType.float32](ntile)
        ctx.synchronize()
        ctx.enqueue_copy(
            qs_dev,
            Pointer[Float16, ImmUntrackedOrigin](
                unsafe_from_address=Int(qh16)
            ),
        )
        ctx.enqueue_copy(
            qrecip_dev,
            Pointer[Float32, ImmUntrackedOrigin](
                unsafe_from_address=Int(qrecip_h)
            ),
        )

        # Reciprocal embedding norms, one simdgroup per row, computed once per
        # pin. The pinned fp16 bytes stay constant while the pin lives, so the
        # first fused call allocates the n_rows floats and runs the norm kernel,
        # and later calls see `renorm_ready == 1` and reuse the buffer.
        var renorm_fresh = renorm_ready == 0
        if renorm_fresh:
            renorm = ctx.enqueue_create_buffer[DType.float32](n_rows)
            renorm_ready = 1
        var renorm_ptr = renorm.value().unsafe_ptr().unsafe_mut_cast[True]()
        if renorm_fresh:
            ctx.enqueue_function[_tc_apple_enorm_kernel](
                emb16.unsafe_ptr().unsafe_mut_cast[True](),
                renorm_ptr,
                Int64(n_rows),
                Int64(K),
                grid_dim=n_rows,
                block_dim=AP_THREADS)
            ctx.synchronize()  # first call for this pin only

        var cand_cap = nblocks * mm * k
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
        var merged_dist_dev = ctx.enqueue_create_buffer[DType.float32](
            mm * k
        )
        var merged_id_dev = ctx.enqueue_create_buffer[DType.int64](mm * k)
        var merged_dist_h_mem = alloc(Layout[Float32](count=mm * k)).into_managed()
        var merged_dist_h = merged_dist_h_mem.unsafe_ptr()
        var merged_id_h_mem = alloc(Layout[Int64](count=mm * k)).into_managed()
        var merged_id_h = merged_id_h_mem.unsafe_ptr()
        ctx.synchronize()

        var q0 = 0
        while q0 < M:
            var qcount = mm
            if q0 + qcount > M:
                qcount = M - q0

            var q_ptr = qs_dev.unsafe_ptr().unsafe_offset(q0 * K)
            var qnorm_ptr = qrecip_dev.unsafe_ptr().unsafe_offset(q0)

            # qsub is a comptime argument, so the kernel is picked by a runtime
            # branch on a host value. It runs once per tile, and one call uses
            # one qsub.
            if qsub == 2:
                ctx.enqueue_function[tc_apple_fused_knn_kernel[2]](
                    q_ptr.unsafe_mut_cast[True](),
                    emb16.unsafe_ptr().unsafe_mut_cast[True](),
                    qnorm_ptr.unsafe_mut_cast[True](),
                    renorm_ptr,
                    cand_dist_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    cand_id_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    Int64(n_rows),
                    Int64(K),
                    Int64(k),
                    Int64(nblocks),
                    grid_dim=nblocks,
                    block_dim=AP_THREADS)
            elif qsub == 4:
                ctx.enqueue_function[tc_apple_fused_knn_kernel[4]](
                    q_ptr.unsafe_mut_cast[True](),
                    emb16.unsafe_ptr().unsafe_mut_cast[True](),
                    qnorm_ptr.unsafe_mut_cast[True](),
                    renorm_ptr,
                    cand_dist_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    cand_id_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    Int64(n_rows),
                    Int64(K),
                    Int64(k),
                    Int64(nblocks),
                    grid_dim=nblocks,
                    block_dim=AP_THREADS)
            else:
                ctx.enqueue_function[tc_apple_fused_knn_kernel[16]](
                    q_ptr.unsafe_mut_cast[True](),
                    emb16.unsafe_ptr().unsafe_mut_cast[True](),
                    qnorm_ptr.unsafe_mut_cast[True](),
                    renorm_ptr,
                    cand_dist_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    cand_id_dev.unsafe_ptr().unsafe_mut_cast[True](),
                    Int64(n_rows),
                    Int64(K),
                    Int64(k),
                    Int64(nblocks),
                    grid_dim=nblocks,
                    block_dim=AP_THREADS)
            ctx.enqueue_function[tc_apple_merge_kernel](
                cand_dist_dev.unsafe_ptr().unsafe_mut_cast[True](),
                cand_id_dev.unsafe_ptr().unsafe_mut_cast[True](),
                merged_dist_dev.unsafe_ptr().unsafe_mut_cast[True](),
                merged_id_dev.unsafe_ptr().unsafe_mut_cast[True](),
                Int64(mm),
                Int64(nblocks),
                Int64(k),
                grid_dim=mm,
                block_dim=AP_THREADS)
            ctx.enqueue_copy(merged_dist_h, merged_dist_dev)
            ctx.enqueue_copy(merged_id_h, merged_id_dev)
            ctx.synchronize()

            for lq in range(qcount):
                var m = q0 + lq
                for j in range(k):
                    out_ids[unsafe_offset=m * k + j] = merged_id_h[unsafe_offset=lq * k + j]
                    out_dists[unsafe_offset=m * k + j] = merged_dist_h[unsafe_offset=lq * k + j]
            q0 += mm

        deinit(qh16_mem^)
        deinit(qrecip_h_mem^)
        deinit(merged_dist_h_mem^)
        deinit(merged_id_h_mem^)
