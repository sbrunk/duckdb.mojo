"""FP16 vs FP32 exact-cosine top-k: recall AND latency.

Validates the fp16-resident variant of the GPU exact-cosine top-k path against
the fp32 path (which is treated as ground truth). The whole point of fp16 is to
attack the bandwidth floor of the distance scan (the resident matrix read
dominates at 1M rows), so this bench measures BOTH:

  * recall@10  -- overlap of the fp16 top-10 id set vs the fp32-exact top-10 id
    set, averaged over the query set. For unit-normalized embeddings this should
    be ~0.99+; misses are distinguished from boundary FP ties (a "miss" whose
    fp32 distance is within a tiny epsilon of the kept boundary is a tie, not a
    genuine accuracy loss).
  * warm-median latency -- fp32 vs fp16 at each (N, K), >= WARM iters, so we can
    see whether the fp16 scan roughly halves (the bandwidth win).

Embeddings are CLUSTERED, UNIT-NORMALIZED, non-zero, and fully deterministic (no
random()): each row is assigned to one of NCLUST cluster centers, perturbed by a
small deterministic jitter, then L2-normalized. Queries sit near cluster
centers. This is far more realistic for kNN than uniform noise (real embeddings
are normalized and clustered) and gives a meaningful recall number.

Run locally:
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/cosine_f16_test.mojo
On a CUDA box (frederick / RTX 4090), build the ext then run with
  LD_LIBRARY_PATH=/run/opengl-driver/lib:$LD_LIBRARY_PATH
"""

from std.sys import has_accelerator
from std.math import sqrt, sin, cos
from std.memory import alloc
from std.time import perf_counter_ns

from gpu_kernels import (
    mojo_gpu_pin,
    mojo_gpu_pin_query_topk,
    mojo_gpu_pin_free,
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_free_f16,
)

comptime NCLUST = 256  # number of synthetic cluster centers
comptime TOPK = 10  # recall@10
comptime NQUERY = 64  # queries averaged for recall
comptime WARM = 20  # warm iters for latency (>= 20 as required)
comptime WARMUP = 5
comptime TIE_EPS = Float32(1.0e-5)  # FP-tie band for "miss vs boundary tie"


# ---------------------------------------------------------------------------
# Deterministic helpers (no random()). A cheap hash spreads values without any
# RNG so runs are bit-reproducible.
# ---------------------------------------------------------------------------
def _hash01(a: Int, b: Int) -> Float32:
    # Returns a deterministic value in roughly [-1, 1] from two ints.
    var x = (a * 2654435761 + b * 40503 + 12345) & 0x7FFFFFFF
    return Float32(x % 20011) * (Float32(2) / 20011.0) - Float32(1)


# Build one unit-normalized cluster center as a deterministic function of its id.
def _center_val(c: Int, i: Int) -> Float32:
    # A smooth-ish, per-dim varying signal so centers genuinely differ.
    return sin(Float32(c) * 0.013 + Float32(i) * 0.027) + 0.5 * cos(
        Float32(c * 7 + i * 3) * 0.011
    )


# ---------------------------------------------------------------------------
# Synthesize a clustered, unit-normalized N x K embedding matrix (deterministic).
# Each row picks a cluster, copies its center + small jitter, then L2-normalizes.
# ---------------------------------------------------------------------------
def build_embeddings(
    emb: UnsafePointer[Float32, MutAnyOrigin], N: Int, K: Int
):
    for row in range(N):
        var c = row % NCLUST
        var base = row * K
        var nrm = Float32(0)
        # A per-row jitter scale that varies across rows so each row sits at a
        # genuinely DIFFERENT radius from its cluster center -> the top-k
        # distances are well-separated rather than a dense thicket of near-ties
        # (so recall measures real accuracy, not boundary tie reordering).
        var jit = Float32(0.10) + Float32(row % 4096) * (Float32(0.30) / 4096.0)
        for i in range(K):
            var v = _center_val(c, i) + jit * _hash01(row, i)
            emb[base + i] = v
            nrm += v * v
        nrm = sqrt(nrm)
        if nrm == 0:
            nrm = 1
        var inv = Float32(1) / nrm
        for i in range(K):
            emb[base + i] = emb[base + i] * inv


# Synthesize query m near cluster center (m % NCLUST), unit-normalized.
def build_query(q: UnsafePointer[Float32, MutAnyOrigin], m: Int, K: Int):
    var c = m % NCLUST
    var nrm = Float32(0)
    for i in range(K):
        var v = _center_val(c, i) + 0.05 * _hash01(m * 131 + 7, i)
        q[i] = v
        nrm += v * v
    nrm = sqrt(nrm)
    if nrm == 0:
        nrm = 1
    var inv = Float32(1) / nrm
    for i in range(K):
        q[i] = q[i] * inv


# Recall of a single query: |fp16_ids ∩ fp32_ids| / k, plus a count of genuine
# misses (an fp16-missing fp32 id whose fp32 distance is NOT within TIE_EPS of
# the fp16 result's worst kept distance -> a real accuracy loss, not a tie).
@fieldwise_init
struct RecallResult(Copyable, Movable):
    var overlap: Int
    var genuine_miss: Int


def recall_one(
    f32_ids: UnsafePointer[Int64, MutAnyOrigin],
    f32_dists: UnsafePointer[Float32, MutAnyOrigin],
    f16_ids: UnsafePointer[Int64, MutAnyOrigin],
    f16_dists: UnsafePointer[Float32, MutAnyOrigin],
    k: Int,
) -> RecallResult:
    # Worst kept distance among the fp16 result (its top-k boundary).
    var f16_worst = Float32(0)
    for j in range(k):
        if f16_dists[j] > f16_worst:
            f16_worst = f16_dists[j]

    var overlap = 0
    var genuine_miss = 0
    for a in range(k):
        var id32 = f32_ids[a]
        var found = False
        for b in range(k):
            if f16_ids[b] == id32:
                found = True
                break
        if found:
            overlap += 1
        else:
            # This fp32 top-k id is absent from the fp16 top-k. If its fp32
            # distance is within TIE_EPS of the fp16 boundary, it is a boundary
            # FP tie (two near-equal distances reorder under fp16 storage), not a
            # genuine accuracy loss.
            var d = f32_dists[a]
            if abs(d - f16_worst) > TIE_EPS:
                genuine_miss += 1
    return RecallResult(overlap, genuine_miss)


# Median of an Int64 timing array (in-place insertion sort; n small).
def _median_ns(times: UnsafePointer[Int64, MutAnyOrigin], n: Int) -> Int64:
    for a in range(n):
        for b in range(a + 1, n):
            if times[b] < times[a]:
                var t = times[a]
                times[a] = times[b]
                times[b] = t
    return times[n // 2]


def run_case(N: Int, K: Int, check_exact: Bool) raises:
    print("====================================================")
    print("case: N =", N, " K =", K, " topk =", TOPK)

    var emb = alloc[Float32](N * K)
    build_embeddings(emb, N, K)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    # Pin both representations of the SAME host data.
    var h32_i = mojo_gpu_pin(emb_imm, N, K)
    if h32_i == 0:
        raise Error("mojo_gpu_pin (fp32) failed")
    var h16_i = mojo_gpu_pin_f16(emb_imm, N, K)
    if h16_i == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    var h32 = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=h32_i
    )
    var h16 = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=h16_i
    )

    # Query set (deterministic, near cluster centers).
    var qbuf = alloc[Float32](NQUERY * K)
    for m in range(NQUERY):
        build_query(qbuf + m * K, m, K)

    var f32_ids = alloc[Int64](TOPK)
    var f32_dists = alloc[Float32](TOPK)
    var f16_ids = alloc[Int64](TOPK)
    var f16_dists = alloc[Float32](TOPK)

    # --- recall@10 over the query set ---
    var total_overlap = 0
    var total_miss = 0
    for m in range(NQUERY):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qbuf) + m * K * 4
        )
        var rc32 = mojo_gpu_pin_query_topk(h32, q, TOPK, f32_ids, f32_dists)
        if rc32 != 0:
            raise Error(String("fp32 topk rc=") + String(Int(rc32)))
        var rc16 = mojo_gpu_pin_query_topk_f16(
            h16, q, TOPK, f16_ids, f16_dists
        )
        if rc16 != 0:
            raise Error(String("fp16 topk rc=") + String(Int(rc16)))
        var r = recall_one(f32_ids, f32_dists, f16_ids, f16_dists, TOPK)
        total_overlap += r.overlap
        total_miss += r.genuine_miss

    var recall = Float64(total_overlap) / Float64(NQUERY * TOPK)
    print("  recall@10 (fp16 vs fp32-exact):", recall, " over", NQUERY, "queries")
    print(
        "    avg overlap:",
        Float64(total_overlap) / Float64(NQUERY),
        "/ 10   genuine misses (beyond FP-tie eps):",
        total_miss,
    )

    # Small-N exact correctness sanity: at 50k we additionally print whether the
    # fp16 top-1 id matches fp32 top-1 for query 0 (cheap visible check).
    if check_exact:
        var q0 = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qbuf)
        )
        _ = mojo_gpu_pin_query_topk(h32, q0, TOPK, f32_ids, f32_dists)
        _ = mojo_gpu_pin_query_topk_f16(h16, q0, TOPK, f16_ids, f16_dists)
        print(
            "    [50k check] q0 top-1: fp32 id=",
            f32_ids[0],
            " d=",
            f32_dists[0],
            " | fp16 id=",
            f16_ids[0],
            " d=",
            f16_dists[0],
        )

    # --- warm-median latency, fp32 vs fp16 ---
    # Distinct query per iter so nothing is trivially cached.
    var lat_q = alloc[Float32]((WARMUP + WARM) * K)
    for m in range(WARMUP + WARM):
        build_query(lat_q + m * K, m + 1000, K)  # different cluster mix

    var times = alloc[Int64](WARM)

    # fp32 warm latency
    for m in range(WARMUP):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(lat_q) + m * K * 4
        )
        _ = mojo_gpu_pin_query_topk(h32, q, TOPK, f32_ids, f32_dists)
    for m in range(WARM):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(lat_q) + (WARMUP + m) * K * 4
        )
        var t0 = perf_counter_ns()
        var rc = mojo_gpu_pin_query_topk(h32, q, TOPK, f32_ids, f32_dists)
        var t1 = perf_counter_ns()
        if rc != 0:
            raise Error(String("fp32 lat rc=") + String(Int(rc)))
        times[m] = Int64(t1 - t0)
    var med32 = _median_ns(times, WARM)

    # fp16 warm latency
    for m in range(WARMUP):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(lat_q) + m * K * 4
        )
        _ = mojo_gpu_pin_query_topk_f16(h16, q, TOPK, f16_ids, f16_dists)
    for m in range(WARM):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(lat_q) + (WARMUP + m) * K * 4
        )
        var t0 = perf_counter_ns()
        var rc = mojo_gpu_pin_query_topk_f16(h16, q, TOPK, f16_ids, f16_dists)
        var t1 = perf_counter_ns()
        if rc != 0:
            raise Error(String("fp16 lat rc=") + String(Int(rc)))
        times[m] = Int64(t1 - t0)
    var med16 = _median_ns(times, WARM)

    var us32 = Float64(med32) / 1000.0
    var us16 = Float64(med16) / 1000.0
    print("  warm median latency:")
    print("    fp32:", us32, "us  (", us32 / 1000.0, "ms)")
    print("    fp16:", us16, "us  (", us16 / 1000.0, "ms)")
    var speedup = us32 / us16 if us16 > 0 else Float64(0)
    print("    speedup (fp32/fp16):", speedup, "x")

    mojo_gpu_pin_free(h32)
    mojo_gpu_pin_free_f16(h16)
    emb.free()
    qbuf.free()
    lat_q.free()
    times.free()
    f32_ids.free()
    f32_dists.free()
    f16_ids.free()
    f16_dists.free()


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("fp16 vs fp32 exact-cosine top-k: recall + latency")
    # Quick small-N correctness check first.
    run_case(50_000, 384, True)
    # The bandwidth-bound regime: 1M rows at 384 and 768 dims.
    run_case(1_000_000, 384, False)
    run_case(1_000_000, 768, False)
    print("DONE")
