"""FUSED Apple-Silicon (M1-M4) GPU kNN via the 8x8 `simdgroup_matrix` MMA.

The Apple analogue of `tc_knn.mojo` (NVIDIA). It computes per-query top-k via a
tiled MMA `Q.Emb^T` fused with a streaming per-query top-k, so the M x N
similarity matrix is NEVER materialized -- only Emb is read, only M x k results
are written. Cosine metric, unit-normalized inputs (the dominant real-embedding
case); the same `(dist, rowid)` tie-break + stage-2 merge as the NVIDIA fused
kernel, so a result is bit-for-bit a scalar single-query top-k.

The MMA core is the VERIFIED 8x8 `simdgroup_matrix` path from
`.repos/modular/.../apple/matmul_8x8.mojo` (bit-exact on this M3, K=64, fp16):
  * `llvm.air.simdgroup_matrix_8x8_multiply_accumulate`, FRAG8=2 elems/lane,
    32-thread simdgroup, `_frag8_layout` per-lane mapping.
  * A (queries) is M x K row-major; lane's 2 frag elems are consecutive K cols.
  * B (embeddings) is N x K row-major -> we need Emb^T, so we gather the
    transposed fragment manually (slot s differs in the N index, row index is
    the K coordinate). This is exactly the `transpose_b=True` path in the 8x8
    reference, reused element-for-element.

Everything Apple-specific is inside `comptime if has_apple_gpu_accelerator()`
(host) / `comptime if is_apple_gpu()` (kernel target), so NOTHING here compiles
for a NVIDIA / Metal-absent target -- mirroring the `tc_knn.mojo` gating. The
`layout` package's `TensorCore` is NEVER touched (it has no Apple support); the
MMA is the raw `llvm_intrinsic`.

Tile shape (one simdgroup per block, kept simple for correctness-first):
  * MM=8 queries per launch-tile (the 8x8 MMA row dim), MN=8 embedding rows per
    MMA step. Each block grids over N in MN-chunks (grid-stride), MMAs the
    MM x MN S-tile (= Q-tile . Emb-chunk^T) into the simdgroup-matrix fp32
    accumulator across all K (K%8==0), then updates a per-query streaming top-k.
  * Per-query top-k lives in SHARED memory (one (dist,id) buffer per query, kept
    ascending by (dist,rowid)); the 8 S-tile columns are scanned by lane 0 (the
    8x8 S-tile is tiny and the scan is the same threshold-insert as the NVIDIA
    register top-k). Stage 2 (`tc_apple_merge_kernel`) reduces each query's
    NBLOCKS*k candidates to the final k -- identical to the NVIDIA merge.
"""

from std.sys import llvm_intrinsic
from std.sys.info import is_apple_gpu, has_apple_gpu_accelerator
from std.gpu import lane_id, block_idx, thread_idx, barrier
from std.gpu.memory import AddressSpace
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc, stack_allocation
from std.collections import InlineArray
from std.math import sqrt


# ===-------------------------------------------------------------------===#
# 8x8 simdgroup-matrix primitives (VERIFIED on this M3, K=64, fp16, err=0).
# Lifted from `.repos/modular/.../apple/matmul_8x8.mojo` (`_frag8_layout`,
# `_mma8x8`); see apple_8x8_probe.mojo for the bit-exact validation.
# ===-------------------------------------------------------------------===#
comptime AP_MMA = 8  # 8x8x8 simdgroup-matrix shape
comptime FRAG8 = 2  # 8x8 = 64 elems / 32 lanes = 2 per lane

# QUERY-TILE AMORTIZATION: a block loads each 8-row Emb chunk ONCE and MMAs it
# against AP_QSUB query-blocks of 8 queries -> AP_MM = AP_QSUB*8 queries share
# each Emb-chunk read (the flash-attention / NVIDIA-BM=128 amortization). The
# fp32 accumulator is AP_QSUB FRAG8 vectors (one 8x8 S sub-tile per query-block).
comptime AP_QSUB = 16  # query-blocks of 8 per Emb pass
comptime AP_MM = AP_QSUB * AP_MMA  # 128 queries per M-tile
comptime AP_MN = 8  # embedding rows per MMA step / N-chunk
comptime AP_NBLOCKS = 1024  # grid-stride blocks over N (one simdgroup each)
comptime AP_THREADS = 32  # one simdgroup per block
# Max k the per-query shared top-k supports. Sized so the per-block shared
# footprint fits Apple's ~32 KB threadgroup at AP_MM=128 queries:
#   s_smem  AP_MM*8*4   = 4 KB
#   sd      AP_MM*K*4   = 8 KB  (K=16)
#   si      AP_MM*K*8   = 16 KB
#   scnt    AP_MM*4     = 0.5 KB        => ~28.5 KB total.
comptime AP_K_CAP = 16


# Whether the Apple fused path supports this (K, k). K must be a multiple of the
# 8x8 MMA K dim (8); the proven library dispatch uses k%16==0, so we require
# K%16==0 to stay on the validated path. k must fit the shared top-k cap.
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
    """Apple 8x8 simdgroup-matrix per-lane layout (ground-truthed via Metal
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


# ===-------------------------------------------------------------------===#
# Stage 1: fused 8x8 MMA Q.Emb^T + per-block per-query streaming top-k.
#
# q is an MM x K fp16 tile (this M-tile's queries, padded to MM rows on host).
# e is the full N x K resident fp16 matrix. qnorm/enorm are host-precomputed
# fp32 L2 norms (cosine: sqrt of sum of squares). Candidates are written
# row-major [block*MM*k + m*k + j] (matching the NVIDIA emit + merge).
# Apple-only via the in-kernel `is_apple_gpu()` gate.
# ===-------------------------------------------------------------------===#
def tc_apple_fused_knn_kernel(
    q: UnsafePointer[Scalar[DType.float16], MutAnyOrigin],
    e: UnsafePointer[Scalar[DType.float16], MutAnyOrigin],
    qnorm: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    enorm: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    k: Int,
    nblocks: Int,
):
    comptime if is_apple_gpu():
        var lane = Int(lane_id())
        var fl = _frag8_layout(lane)
        var frow = fl[0]  # which of the 8 rows (queries) this lane's frag owns
        var fcol = fl[1]  # col base of this lane's 2 frag elems

        # Shared S-tile (MM queries x MN emb-rows) for this N-chunk. The MMA
        # writes its fp32 accumulator here; lane 0 then scans it into the
        # per-query top-k. 8x8 fp32 = 256 B.
        var s_smem = stack_allocation[
            AP_MM * AP_MN,
            Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        # Per-query shared top-k: sd[q*k + slot] / si[...], kept ascending by
        # (dist, rowid); scnt[q] is the fill. AP_MM queries * AP_K_CAP slots.
        var sd = stack_allocation[
            AP_MM * AP_K_CAP,
            Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var si = stack_allocation[
            AP_MM * AP_K_CAP,
            Scalar[DType.int64],
            address_space = AddressSpace.SHARED,
        ]()
        var scnt = stack_allocation[
            AP_MM, Scalar[DType.int32], address_space = AddressSpace.SHARED
        ]()

        if lane == 0:
            for q_i in range(AP_MM):
                scnt[q_i] = 0
        # `scnt` is owned by lane 0 throughout, so no fence needed for it; but the
        # `s_smem` S-tile IS written by all lanes and read by lane 0 each
        # iteration, so it needs a threadgroup barrier on both sides of the read
        # (lockstep is not a substitute for a threadgroup-memory fence).

        # Grid-stride over N in MN-chunks. NSTRIDE = nblocks * MN emb-rows.
        var nstride = nblocks * AP_MN
        var n0 = Int(block_idx.x) * AP_MN
        while n0 < n_rows:
            # --- MMA: for this 8-row Emb chunk, compute the 8x8 S sub-tile for
            # EACH of the AP_QSUB query-blocks. The B (Emb^T) fragment is loaded
            # ONCE per K-step and reused across all query-blocks; only the A
            # (query) fragment differs per block. So the Emb chunk is read once
            # and shared by AP_MM = AP_QSUB*8 queries -- the amortization. ---
            var acc = InlineArray[SIMD[DType.float32, FRAG8], AP_QSUB](
                fill=SIMD[DType.float32, FRAG8](0)
            )
            var ks = 0
            while ks < K:
                # B = Emb_chunk^T (shared across query-blocks). Emb is N x K
                # row-major, so B[k_idx, j] = e[(n0+j)*K + k_idx]: slot s differs
                # in the N index (col j = fcol+s), row index is the K coord
                # (ks+frow). OOB rows zero-fill (results discarded by row-bound).
                var bf = SIMD[DType.float16, FRAG8](0)
                comptime for s in range(FRAG8):
                    var gj = n0 + fcol + s
                    if gj < n_rows:
                        bf[s] = e[gj * K + ks + frow]
                comptime for qb in range(AP_QSUB):
                    # A (queries) MM x K row-major: this query-block's row is
                    # qb*8 + frow; lane owns cols (ks+fcol, ks+fcol+1).
                    var qrow = qb * AP_MMA + frow
                    var af = (q + qrow * K + ks + fcol).load[width=FRAG8]()
                    acc[qb] = _mma8x8[DType.float16, DType.float16](
                        af, bf, acc[qb]
                    )
                ks += AP_MMA
            # Store each query-block's 8x8 S sub-tile into the shared S-tile.
            # S[qb*8 + frow, fcol{,+1}] for every query-block qb.
            comptime for qb in range(AP_QSUB):
                comptime for s in range(FRAG8):
                    s_smem[(qb * AP_MMA + frow) * AP_MN + fcol + s] = acc[qb][s]
            # Publish all lanes' S-tile writes before lane 0 reads them.
            barrier()

            # --- Streaming per-query top-k over this S-chunk (lane 0). ---
            # S[m, nn] = dot(query m, emb row n0+nn). Cosine distance epilogue +
            # (dist, rowid) tie-break -- identical to tc_knn's cosine epilogue.
            if lane == 0:
                for m in range(AP_MM):
                    var qn = qnorm[m]
                    var off = m * k
                    var cnt = Int(scnt[m])
                    for nn in range(AP_MN):
                        var row = n0 + nn
                        if row >= n_rows:
                            break
                        var dotv = s_smem[m * AP_MN + nn]
                        var denom = enorm[row] * qn
                        var cd = (
                            Float32(1) - dotv / denom if denom
                            != 0 else Float32(0)
                        )
                        var ci = Int64(row)
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
                                cnt += 1
                    scnt[m] = Int32(cnt)
            # Hold all lanes until lane 0 has consumed this S-tile, so the next
            # iteration's MMA stores don't overwrite s_smem mid-read.
            barrier()
            n0 += nstride

        # Emit this block's k candidates per query (lane 0 owns the buffers).
        # Layout [(block*MM + m)*k + j] -- matches tc_merge / tc_apple_merge.
        if lane == 0:
            for m in range(AP_MM):
                var cnt = Int(scnt[m])
                var off = m * k
                var out_base = (Int(block_idx.x) * AP_MM + m) * k
                for j in range(k):
                    if j < cnt:
                        cand_dist[out_base + j] = sd[off + j]
                        cand_id[out_base + j] = si[off + j]
                    else:
                        cand_dist[out_base + j] = Float32(3.0e38)
                        cand_id[out_base + j] = Int64(-1)


# ===-------------------------------------------------------------------===#
# Stage 2: per-query merge of nblocks*k candidates -> final k. One simdgroup
# (one block) per query. Candidate layout [(block*MM + m)*k + j] matches stage
# 1's emit. Apple-only via the in-kernel gate. Mirrors tc_merge_kernel but uses
# lane 0 to do the serial merge (Apple has no warp shuffle helper here; the
# candidate count nblocks*k is small, so a single-lane merge is fine).
# ===-------------------------------------------------------------------===#
def tc_apple_merge_kernel(
    cand_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    cand_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    out_dist: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    out_id: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    Mq: Int,
    nblocks: Int,
    k: Int,
):
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
            var ci = cand_id[idx]
            if ci < 0:
                continue
            var cd = cand_dist[idx]
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
                    cnt += 1

        for j in range(k):
            if j < cnt:
                out_dist[mq * k + j] = sd[j]
                out_id[mq * k + j] = si[j]
            else:
                out_dist[mq * k + j] = Float32(3.0e38)
                out_id[mq * k + j] = Int64(-1)


# Per-row L2 norm of the fp16-resident matrix (one simdgroup per row, lane
# -strided + manual reduce). `squared==0` returns the L2 norm (sqrt of sum of
# (fp16-cast-to-fp32)^2) -- the SAME cosine denom as the scalar f16 path.
# Apple-only via the in-kernel gate.
def _tc_apple_enorm_kernel(
    emb: UnsafePointer[Scalar[DType.float16], MutAnyOrigin],
    enorm: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    n_rows: Int,
    K: Int,
):
    comptime if is_apple_gpu():
        var row = Int(block_idx.x)
        if row >= n_rows:
            return
        var lane = Int(lane_id())
        # Lane-strided partial; reduce via shared (no warp.sum dependency here).
        var part = stack_allocation[
            AP_THREADS, Scalar[DType.float32],
            address_space = AddressSpace.SHARED,
        ]()
        var base = row * K
        var na = Float32(0)
        var i = lane
        while i < K:
            var av = emb[base + i].cast[DType.float32]()
            na += av * av
            i += AP_THREADS
        part[lane] = na
        if lane == 0:
            var tot = Float32(0)
            for t in range(AP_THREADS):
                tot += part[t]
            enorm[row] = sqrt(tot)


# ===-------------------------------------------------------------------===#
# Host driver. Builds the fp16 query tile + fp32 query/embedding L2 norms on the
# host, uploads once, then tiles over M in MM-chunks; each tile launches the
# fused stage-1 kernel (reads the full N matrix once) + the stage-2 merge, and
# writes that tile's MM*k results into the caller's row-major M*k output.
# Apple-only via the host `has_apple_gpu_accelerator()` gate (cosine only).
# ===-------------------------------------------------------------------===#
def run_tc_knn_apple_batch(
    ctx: DeviceContext,
    emb16: DeviceBuffer[DType.float16],
    n_rows: Int,
    K: Int,
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
) raises:
    comptime if has_apple_gpu_accelerator():
        # Host: fp16 query tile (padded to a multiple of MM rows so the kernel's
        # full-MM q tile is always in bounds), fp32 query L2 norms.
        var ntile = ((M + AP_MM - 1) // AP_MM) * AP_MM
        var qh16 = alloc[Float16](ntile * K)
        var qnorm_h = alloc[Float32](ntile)
        for m in range(M):
            var qoff = m * K
            var s = Float32(0)
            for i in range(K):
                var v = qs[qoff + i]
                qh16[qoff + i] = v.cast[DType.float16]()
                s += v * v
            qnorm_h[m] = sqrt(s)
        for m in range(M, ntile):
            var qoff = m * K
            for i in range(K):
                qh16[qoff + i] = Float16(0)
            qnorm_h[m] = Float32(1)  # padded queries discarded by qcount guard

        var qs_dev = ctx.enqueue_create_buffer[DType.float16](ntile * K)
        var qnorm_dev = ctx.enqueue_create_buffer[DType.float32](ntile)
        var enorm_dev = ctx.enqueue_create_buffer[DType.float32](n_rows)
        ctx.synchronize()
        ctx.enqueue_copy(
            qs_dev,
            UnsafePointer[Float16, ImmutAnyOrigin](
                unsafe_from_address=Int(qh16)
            ),
        )
        ctx.enqueue_copy(
            qnorm_dev,
            UnsafePointer[Float32, ImmutAnyOrigin](
                unsafe_from_address=Int(qnorm_h)
            ),
        )

        # Emb L2 norms (cosine denom). One simdgroup per row.
        ctx.enqueue_function[_tc_apple_enorm_kernel](
            emb16.unsafe_ptr(),
            enorm_dev.unsafe_ptr(),
            n_rows,
            K,
            grid_dim=n_rows,
            block_dim=AP_THREADS,
        )
        ctx.synchronize()

        var nblocks = AP_NBLOCKS
        var cand_cap = nblocks * AP_MM * k
        var cand_dist_dev = ctx.enqueue_create_buffer[DType.float32](cand_cap)
        var cand_id_dev = ctx.enqueue_create_buffer[DType.int64](cand_cap)
        var merged_dist_dev = ctx.enqueue_create_buffer[DType.float32](
            AP_MM * k
        )
        var merged_id_dev = ctx.enqueue_create_buffer[DType.int64](AP_MM * k)
        var merged_dist_h = alloc[Float32](AP_MM * k)
        var merged_id_h = alloc[Int64](AP_MM * k)
        ctx.synchronize()

        var q0 = 0
        while q0 < M:
            var qcount = AP_MM
            if q0 + qcount > M:
                qcount = M - q0

            var q_ptr = qs_dev.unsafe_ptr() + q0 * K
            var qnorm_ptr = qnorm_dev.unsafe_ptr() + q0

            ctx.enqueue_function[tc_apple_fused_knn_kernel](
                q_ptr,
                emb16.unsafe_ptr(),
                qnorm_ptr,
                enorm_dev.unsafe_ptr(),
                cand_dist_dev.unsafe_ptr(),
                cand_id_dev.unsafe_ptr(),
                n_rows,
                K,
                k,
                nblocks,
                grid_dim=nblocks,
                block_dim=AP_THREADS,
            )
            ctx.enqueue_function[tc_apple_merge_kernel](
                cand_dist_dev.unsafe_ptr(),
                cand_id_dev.unsafe_ptr(),
                merged_dist_dev.unsafe_ptr(),
                merged_id_dev.unsafe_ptr(),
                AP_MM,
                nblocks,
                k,
                grid_dim=AP_MM,
                block_dim=AP_THREADS,
            )
            ctx.enqueue_copy(merged_dist_h, merged_dist_dev)
            ctx.enqueue_copy(merged_id_h, merged_id_dev)
            ctx.synchronize()

            for lq in range(qcount):
                var m = q0 + lq
                for j in range(k):
                    out_ids[m * k + j] = merged_id_h[lq * k + j]
                    out_dists[m * k + j] = merged_dist_h[lq * k + j]
            q0 += AP_MM

        qh16.free()
        qnorm_h.free()
        merged_dist_h.free()
        merged_id_h.free()
