"""Apple fused 8x8-MMA batched kNN: correctness vs scalar + A/B latency.

Validates `run_tc_knn_apple_batch` (routed through
`mojo_gpu_pin_query_topk_batch_f16` when GPU_OP_TENSORCORE is set on an Apple
build, cosine, supported K/k) against the SCALAR single-query top-k
(`mojo_gpu_pin_query_topk_f16`, ground truth) on the SAME pinned fp16 data, with
the `cosine_f16_test.mojo` genuine-miss methodology (recall@10, genuine misses
beyond an FP-tie band).

Run BOTH passes:
  # scalar batched (flag off):
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/apple_tc_knn_test.mojo
  # Apple fused MMA (flag on):
  GPU_OP_TENSORCORE=1 pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/apple_tc_knn_test.mojo
The reported per-query latency from the two passes is the A/B (scalar vs fused).
"""

from std.sys import has_accelerator
from std.os import getenv
from std.math import sqrt, sin, cos
from std.memory import alloc
from std.time import perf_counter_ns

from gpu_kernels import (
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime NCLUST = 256
comptime TOPK = 10
comptime TIE_EPS = Float32(1.0e-4)  # FP-tie band (fp16 + MMA reorder)


# --- deterministic clustered, unit-normalized embeddings (from cosine_f16) ---
def _hash01(a: Int, b: Int) -> Float32:
    var x = (a * 2654435761 + b * 40503 + 12345) & 0x7FFFFFFF
    return Float32(x % 20011) * (Float32(2) / 20011.0) - Float32(1)


def _center_val(c: Int, i: Int) -> Float32:
    return sin(Float32(c) * 0.013 + Float32(i) * 0.027) + 0.5 * cos(
        Float32(c * 7 + i * 3) * 0.011
    )


def build_embeddings(emb: UnsafePointer[Float32, MutAnyOrigin], N: Int, K: Int):
    for row in range(N):
        var c = row % NCLUST
        var base = row * K
        var nrm = Float32(0)
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


@fieldwise_init
struct RecallResult(Copyable, Movable):
    var overlap: Int
    var genuine_miss: Int


def recall_one(
    ref_ids: UnsafePointer[Int64, MutAnyOrigin],
    ref_dists: UnsafePointer[Float32, MutAnyOrigin],
    got_ids: UnsafePointer[Int64, MutAnyOrigin],
    got_dists: UnsafePointer[Float32, MutAnyOrigin],
    k: Int,
) -> RecallResult:
    var got_worst = Float32(0)
    for j in range(k):
        if got_dists[j] > got_worst:
            got_worst = got_dists[j]
    var overlap = 0
    var genuine_miss = 0
    for a in range(k):
        var idr = ref_ids[a]
        var found = False
        for b in range(k):
            if got_ids[b] == idr:
                found = True
                break
        if found:
            overlap += 1
        else:
            var d = ref_dists[a]
            if abs(d - got_worst) > TIE_EPS:
                genuine_miss += 1
    return RecallResult(overlap, genuine_miss)


def run_case(N: Int, K: Int, M: Int) raises:
    var on = getenv("GPU_OP_TENSORCORE", "") != ""
    print("====================================================")
    print(
        "case: N =",
        N,
        " K =",
        K,
        " M =",
        M,
        " k =",
        TOPK,
        " path =",
        "APPLE-FUSED-MMA" if on else "SCALAR",
    )

    var emb = alloc[Float32](N * K)
    build_embeddings(emb, N, K)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    var h_i = mojo_gpu_pin_f16(emb_imm, N, K)
    if h_i == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=h_i)

    # Query set.
    var qbuf = alloc[Float32](M * K)
    for m in range(M):
        build_query(qbuf + m * K, m, K)
    var qbuf_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qbuf)
    )

    # --- ground truth: scalar single-query top-k for every query ---
    var ref_ids = alloc[Int64](M * TOPK)
    var ref_dists = alloc[Float32](M * TOPK)
    for m in range(M):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qbuf) + m * K * 4
        )
        var rc = mojo_gpu_pin_query_topk_f16(
            h, q, TOPK, ref_ids + m * TOPK, ref_dists + m * TOPK
        )
        if rc != 0:
            raise Error(String("scalar single rc=") + String(Int(rc)))

    # --- system under test: batched path (scalar or Apple-fused per flag) ---
    var got_ids = alloc[Int64](M * TOPK)
    var got_dists = alloc[Float32](M * TOPK)
    var rcb = mojo_gpu_pin_query_topk_batch_f16(
        h, qbuf_imm, M, TOPK, got_ids, got_dists
    )
    if rcb != 0:
        raise Error(String("batch rc=") + String(Int(rcb)))

    var total_overlap = 0
    var total_miss = 0
    for m in range(M):
        var r = recall_one(
            ref_ids + m * TOPK,
            ref_dists + m * TOPK,
            got_ids + m * TOPK,
            got_dists + m * TOPK,
            TOPK,
        )
        total_overlap += r.overlap
        total_miss += r.genuine_miss
    var recall = Float64(total_overlap) / Float64(M * TOPK)
    print("  recall@10 (batch vs scalar-exact):", recall, " over", M, "queries")
    print(
        "    avg overlap:",
        Float64(total_overlap) / Float64(M),
        "/ 10   genuine misses (beyond FP-tie eps):",
        total_miss,
    )

    # --- warm batched latency (best of N iters) ---
    for _ in range(3):
        _ = mojo_gpu_pin_query_topk_batch_f16(
            h, qbuf_imm, M, TOPK, got_ids, got_dists
        )
    var best = Float64(1.0e30)
    var iters = 30
    for _ in range(iters):
        var t0 = perf_counter_ns()
        _ = mojo_gpu_pin_query_topk_batch_f16(
            h, qbuf_imm, M, TOPK, got_ids, got_dists
        )
        var t1 = perf_counter_ns()
        var dt = Float64(t1 - t0)
        if dt < best:
            best = dt
    var per_q_us = (best / Float64(M)) / 1000.0
    var total_ms = best / 1.0e6
    print(
        "  warm batch latency: total",
        total_ms,
        "ms  per-query",
        per_q_us,
        "us  (",
        Float64(M) * 1.0e9 / best,
        "qps )",
    )

    mojo_gpu_pin_free_f16(h)
    emb.free()
    qbuf.free()
    ref_ids.free()
    ref_dists.free()
    got_ids.free()
    got_dists.free()


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("Apple fused 8x8-MMA batched kNN: correctness + latency")
    run_case(64_000, 768, 128)
    run_case(256_000, 768, 128)
    run_case(1_000_000, 768, 256)
    print("DONE")
