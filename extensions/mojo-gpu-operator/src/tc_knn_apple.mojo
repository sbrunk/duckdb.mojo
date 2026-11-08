"""Fused Apple-Silicon (M1-M4) GPU kNN via the 8x8 `simdgroup_matrix` MMA.

The Apple analogue of `tc_knn.mojo` (NVIDIA). It computes per-query top-k via a
tiled MMA `Q.Emb^T` fused with a streaming per-query top-k, so the M x N
similarity matrix is never materialized: only Emb is read, and only M x k results
are written. Cosine metric, unit-normalized inputs (the common case for real
embeddings). The same `(dist, rowid)` tie-break and stage-2 merge as the NVIDIA
fused kernel, so a result is bit-for-bit a scalar single-query top-k.

The MMA core is the verified 8x8 `simdgroup_matrix` path from
`.repos/modular/.../apple/matmul_8x8.mojo` (bit-exact on this M3, K=64, fp16):
  * `llvm.air.simdgroup_matrix_8x8_multiply_accumulate`, FRAG8=2 elems/lane,
    32-thread simdgroup, `_frag8_layout` per-lane mapping.
  * A (queries) is M x K row-major. A lane's 2 frag elems are consecutive K cols.
  * B (embeddings) is N x K row-major. We need Emb^T, so we gather the
    transposed fragment manually (slot s differs in the N index, row index is
    the K coordinate). This is exactly the `transpose_b=True` path in the 8x8
    reference, reused element-for-element.

Everything Apple-specific is inside `comptime if default_accelerator().is_apple_gpu()`
(host) / `comptime if is_apple_gpu()` (kernel target), so nothing here compiles
for an NVIDIA / Metal-absent target, mirroring the `tc_knn.mojo` gating. The
`layout` package's `TensorCore` is never touched (it has no Apple support). The
MMA is the raw `llvm_intrinsic`.

Tile shape (one simdgroup per block; tuned on M3 Max, see bench/):
  * MM=AP_MM=128 queries per launch-tile, MN=8 embedding rows per MMA step.
    Each block grid-strides over N in MN-chunks. The 8-row Emb chunk is read
    once and MMAed against all AP_QSUB=16 query blocks (QSUB x 8 = MM rows),
    so one Emb read serves MM queries.
  * The MMA loop loads the B fragment of the next k-step before the 16 MMAs
    consume the current one, and query row bases are hoisted out of the
    k-loop, which then only adds offsets. MMA phase: 74 -> ~50 ms on M3 Max.
  * The epilogue works across lanes. A lane's fragment holds 2 of a query
    row's 8 emb-column scores, and lanes {l, l^1, l^8, l^9} hold all 8. Six
    single element fp32 shuffles bring them to the owning lane (pos == qb&3),
    which merges them into its own list in threadgroup memory, sorted by
    score descending then id ascending. Each list has one writer, so there is
    no shared S-tile, no barrier and no contention. The shuffles cost nothing
    measurable, they hide under MMA latency.
  * Scores stay in the form dot * (1/||emb||), so the inner loop never
    divides. The cosine distance is computed once per candidate at emit.
    Stage 2 (`tc_apple_merge_kernel`) reduces each query's NBLOCKS*k
    candidates to the final k, identical to the NVIDIA merge.
"""

from std.sys import llvm_intrinsic
from std.sys.info import is_apple_gpu, default_accelerator
from max.gpu import lane_id, block_idx, thread_idx
from max.gpu.primitives.warp import shuffle_xor
from max.gpu.memory import AddressSpace
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
# against AP_QSUB query-blocks of 8 queries, so AP_MM = AP_QSUB*8 queries share
# each Emb-chunk read (the same amortization as flash attention and NVIDIA BM=128). The
# fp32 accumulator is AP_QSUB FRAG8 vectors (one 8x8 S sub-tile per query-block).
comptime AP_QSUB = 16  # query-blocks of 8 per Emb pass
comptime AP_MM = AP_QSUB * AP_MMA  # 128 queries per M-tile
comptime AP_MN = 8  # embedding rows per MMA step / N-chunk
# Grid-stride blocks over N, one simdgroup each. Stage 2 merges nblocks*k
# candidates per query, so more blocks mean more merge traffic at fixed k, and
# stage 1 is unaffected.
comptime AP_NBLOCKS = 1024  # measured better than 2048 on M3
comptime AP_THREADS = 32  # one simdgroup per block
# Max k the per-query shared top-k supports. Sized so the per-block shared
# footprint fits Apple's ~32 KB threadgroup at AP_MM=128 queries:
#   s_smem  AP_MM*8*4   = 4 KB
#   sd      AP_MM*K*4   = 8 KB  (K=16)
#   si      AP_MM*K*8   = 16 KB
#   scnt    AP_MM*4     = 0.5 KB        => ~28.5 KB total.
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
# Stage 1: fused 8x8 MMA Q.Emb^T plus a per-query streaming top-k, in one
# pass, so the M x N score matrix is never stored.
#
# q: MM x K fp16 tile with this M-tile's queries, padded to MM rows on the host.
# e: the full N x K fp16 matrix, resident on the device.
# qrecip/renorm: fp32 reciprocal L2 norms (1/||q||, 1/||emb||) computed on the
# host, so scores stay dot * renorm and the inner loop never divides.
# Candidates are written row-major as [block*MM*k + m*k + j], the layout the
# NVIDIA emit and the stage-2 merge expect, with distance 1 - score * qrecip.
# Apple only: gated by the in-kernel `is_apple_gpu()`.
# ===-------------------------------------------------------------------===#
def tc_apple_fused_knn_kernel(
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
    comptime if is_apple_gpu():
        var lane = Int(lane_id())
        # This lane's two elements of the 8x8 fragment: (frow, fcol) and
        # (frow, fcol+1).
        var frow = ((lane & 6) >> 1) + ((lane & 16) >> 2)
        var fcol = ((lane & 1) << 1) + ((lane & 8) >> 1)
        var pos = (lane & 1) + ((lane & 8) >> 2)

        # Per-query top-k lists: sd[q*k + slot] is the score, si[...] the row
        # id, sorted by score descending then id ascending, and scnt[q] is the
        # list length. A list has a single writer: the lane with pos == qb&3
        # and frow == m&7 owns row m = qb*8 + frow. Shared memory is ~17.5 KB
        # at CAP=16.
        var sd = stack_allocation[
            AP_MM * AP_K_CAP,
            Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            AP_MM * AP_K_CAP,
            Scalar[DType.int32],
            address_space = AddressSpace.SHARED,
        ]()
        var scnt = stack_allocation[
            AP_MM, Scalar[DType.int32], address_space = AddressSpace.SHARED
        ]()
        # The 4 lists this lane owns (rows qb*8+frow for qb = pos+4h): zero
        # their counts and fill their slots with sentinels. The insert writes
        # every slot, so this state holds for the whole kernel and the lists
        # need no other init.
        var ci0 = lane
        while ci0 < AP_MM:
            scnt[unsafe_offset=ci0] = 0
            ci0 += 32
        comptime for h0 in range(4):
            var mi0 = ((pos + 4 * h0) * AP_MMA + frow) * AP_K_CAP
            comptime for s0 in range(AP_K_CAP):
                sd[unsafe_offset=mi0 + s0] = Float32(SENT_D)
                si[unsafe_offset=mi0 + s0] = Int32(SENT_I)

        # Start of each query block for this lane (row qb*8+frow). Keeping
        # these outside the k-loop leaves the loop body with plain adds.
        var qoffs = Array[Int, AP_QSUB](fill=Int(0))
        comptime for qb0 in range(AP_QSUB):
            qoffs[qb0] = (qb0 * AP_MMA + frow) * K + fcol

        # Walk N in chunks of MN=8 rows, each block taking every nblocks-th
        # chunk.
        var nstride = nblocks * AP_MN
        var n0 = Int(block_idx.x) * AP_MN
        while n0 < n_rows:
            # --- MMA of this 8-row Emb chunk against all AP_QSUB query
            # blocks. B is the transposed chunk: its slot s differs in the N
            # index (n0+fcol+s) and the K coordinate is frow. Rows past
            # n_rows read as zero. ---
            var acc = Array[SIMD[DType.float32, FRAG8], AP_QSUB](
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
                comptime for qb in range(AP_QSUB):
                    var af = q.unsafe_offset(qoffs[qb] + ks).unsafe_load[width=FRAG8]()
                    acc[qb] = _mma8x8[DType.float16, DType.float16](af, bf, acc[qb])
                ks += AP_MMA

            # --- epilogue: for each query block, six single element fp32 shuffles
            # move that row's 8 emb-column scores to one lane. Lanes l, l^1, l^8
            # and l^9 share frow and hold two columns each. The owner lane merges
            # all 8 into the list it owns, so a list has one writer and needs no
            # barrier and no shared S-tile. ---
            var nv = n_rows - n0
            if nv > 8:
                nv = 8
            var r0 = Float32(0)
            var r1 = Float32(0)
            if ok0:
                r0 = renorm[unsafe_offset=g0]
            if ok1:
                r1 = renorm[unsafe_offset=g1]
            comptime for qb in range(AP_QSUB):
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

        # --- emit: each lane writes out the 4 rows it owns, k candidates each,
        # and turns the score back into a cosine distance. Output layout is
        # [(block*MM + m)*k + j]. ---
        comptime for h in range(4):
            var qb = pos + 4 * h
            var m = qb * AP_MMA + frow
            var cnt = Int(scnt[unsafe_offset=m])
            var off = m * AP_K_CAP
            var qr = qrecip[unsafe_offset=m]
            var out_base = (Int(block_idx.x) * AP_MM + m) * k
            for j in range(k):
                if j < cnt:
                    cand_dist[unsafe_offset=out_base + j] = Float32(1) - sd[
                        unsafe_offset=off + j
                    ] * qr
                    cand_id[unsafe_offset=out_base + j] = Int64(si[unsafe_offset=off + j])
                else:
                    cand_dist[unsafe_offset=out_base + j] = Float32(3.0e38)
                    cand_id[unsafe_offset=out_base + j] = Int64(-1)


# ===-------------------------------------------------------------------===#
# Stage 2: per-query merge of nblocks*k candidates into the final k, one simdgroup
# (one block) per query. The candidate layout [(block*MM + m)*k + j] matches
# stage 1's emit. Apple-only via the in-kernel gate. Same as tc_merge_kernel,
# except that lane 0 does the merge: nblocks*k is small, so one lane is enough.
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
        var sd = stack_allocation[
            AP_K_CAP, Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            AP_K_CAP, Scalar[DType.int64],
            address_space = AddressSpace.SHARED,
        ]()

        var lane = Int(lane_id())
        var mq = Int(block_idx.x)  # this block merges query mq
        if mq >= Mq:
            return
        if lane != 0:
            return

        var cnt = 0
        var ncand = nblocks * k
        for c in range(ncand):
            var b = c // k
            var j = c % k
            var idx = (b * Mq + mq) * k + j
            var ci = cand_id[unsafe_offset=idx]
            if ci < 0:
                continue
            var cd = cand_dist[unsafe_offset=idx]
            var accept = True
            if cnt >= k:
                var wd = sd[unsafe_offset=k - 1]
                var wi = si[unsafe_offset=k - 1]
                if cd > wd or (cd == wd and ci >= wi):
                    accept = False
            if accept:
                var pos = cnt if cnt < k else k - 1
                while pos > 0:
                    var pd = sd[unsafe_offset=pos - 1]
                    var pi = si[unsafe_offset=pos - 1]
                    if pd > cd or (pd == cd and pi > ci):
                        sd[unsafe_offset=pos] = pd
                        si[unsafe_offset=pos] = pi
                        pos -= 1
                    else:
                        break
                sd[unsafe_offset=pos] = cd
                si[unsafe_offset=pos] = ci
                if cnt < k:
                    cnt += 1

        for j in range(k):
            if j < cnt:
                out_dist[unsafe_offset=mq * k + j] = sd[unsafe_offset=j]
                out_id[unsafe_offset=mq * k + j] = si[unsafe_offset=j]
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
# queries and embeddings on the host, uploads them once, then walks M in
# MM-chunks. Each chunk runs the fused stage-1 kernel (one read of the full N
# matrix) and the stage-2 merge, and writes its MM*k results into the caller's
# row-major M*k output. Apple only, cosine only, gated on the host by
# `default_accelerator().is_apple_gpu()`.
# ===-------------------------------------------------------------------===#
def run_tc_knn_apple_batch(
    ctx: DeviceContext,
    emb16: DeviceBuffer[DType.float16],
    n_rows: Int,
    K: Int,
    qs: Pointer[Float32, ImmUntrackedOrigin],
    M: Int,
    k: Int,
    out_ids: Pointer[Int64, MutUntrackedOrigin],
    out_dists: Pointer[Float32, MutUntrackedOrigin],
) raises:
    comptime if default_accelerator().is_apple_gpu():
        # Host side: the fp16 query tile, padded to a whole MM multiple so the
        # kernel always reads a full tile, and the fp32 query reciprocal norms
        # (1/||q||, 0 for a zero query).
        var ntile = ((M + AP_MM - 1) // AP_MM) * AP_MM
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
        var renorm_dev = ctx.enqueue_create_buffer[DType.float32](n_rows)
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

        # Reciprocal embedding norms, one simdgroup per row.
        ctx.enqueue_function[_tc_apple_enorm_kernel](
            emb16.unsafe_ptr().unsafe_mut_cast[True](),
            renorm_dev.unsafe_ptr().unsafe_mut_cast[True](),
            Int64(n_rows),
            Int64(K),
            grid_dim=n_rows,
            block_dim=AP_THREADS)
        ctx.synchronize()

        var nblocks = AP_NBLOCKS
        var cand_cap = nblocks * AP_MM * k
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
        var merged_dist_dev = ctx.enqueue_create_buffer[DType.float32](
            AP_MM * k
        )
        var merged_id_dev = ctx.enqueue_create_buffer[DType.int64](AP_MM * k)
        var merged_dist_h_mem = alloc(Layout[Float32](count=AP_MM * k)).into_managed()
        var merged_dist_h = merged_dist_h_mem.unsafe_ptr()
        var merged_id_h_mem = alloc(Layout[Int64](count=AP_MM * k)).into_managed()
        var merged_id_h = merged_id_h_mem.unsafe_ptr()
        ctx.synchronize()

        var q0 = 0
        while q0 < M:
            var qcount = AP_MM
            if q0 + qcount > M:
                qcount = M - q0

            var q_ptr = qs_dev.unsafe_ptr().unsafe_offset(q0 * K)
            var qnorm_ptr = qrecip_dev.unsafe_ptr().unsafe_offset(q0)

            ctx.enqueue_function[tc_apple_fused_knn_kernel](
                q_ptr.unsafe_mut_cast[True](),
                emb16.unsafe_ptr().unsafe_mut_cast[True](),
                qnorm_ptr.unsafe_mut_cast[True](),
                renorm_dev.unsafe_ptr().unsafe_mut_cast[True](),
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
                Int64(AP_MM),
                Int64(nblocks),
                Int64(k),
                grid_dim=AP_MM,
                block_dim=AP_THREADS)
            ctx.enqueue_copy(merged_dist_h, merged_dist_dev)
            ctx.enqueue_copy(merged_id_h, merged_id_dev)
            ctx.synchronize()

            for lq in range(qcount):
                var m = q0 + lq
                for j in range(k):
                    out_ids[unsafe_offset=m * k + j] = merged_id_h[unsafe_offset=lq * k + j]
                    out_dists[unsafe_offset=m * k + j] = merged_dist_h[unsafe_offset=lq * k + j]
            q0 += AP_MM

        deinit(qh16_mem^)
        deinit(qrecip_h_mem^)
        deinit(merged_dist_h_mem^)
        deinit(merged_id_h_mem^)
