"""Recall of the FUSED tensor-core batched kNN for L2 + inner-product metrics.

Validates the new metric path (mojo_gpu_pin_query_topk_batch_f16_metric) added
to the fused tensor-core kNN (tc_knn.mojo). The fused MMA core is shared with
cosine; only the distance epilogue + norm-buffer semantics change per metric:
  COSINE (0): 1 - dot/(|q||e|)
  L2     (1): |q|^2 + |e|^2 - 2*dot   (array_distance; squared euclidean)
  IP     (2): -dot                     (array_negative_inner_product)

For each metric we compute a SCALAR fp32 CPU ground truth on the SAME host data
(top-k by smallest distance, (dist,rowid) tie-break) and compare against the GPU
fused result, reporting recall@10 + genuine misses (a ref id absent from the GPU
set whose ref distance is NOT within TIE_EPS of the GPU boundary -- the same
genuine-miss methodology as cosine_f16_test.mojo).

Run on frederick with GPU_OP_TENSORCORE=1 (the metric path REQUIRES the fused
kernel; without the flag the entry returns rc != 0):
  GPU_OP_TENSORCORE=1 LD_LIBRARY_PATH=/run/opengl-driver/lib:$LD_LIBRARY_PATH \
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/tc_knn_metric_test.mojo

Datasets:
  * NORMALIZED   (unit-norm rows+queries): exact for all three metrics.
  * UNNORMALIZED (raw rows): L2 via |q|^2+|e|^2-2*dot with the fp16-product dot
    suffers catastrophic cancellation -- this run DOCUMENTS that behavior (it is
    expected to show genuine misses for L2; IP is unaffected).
"""

from std.sys import has_accelerator
from std.os import getenv
from std.math import sqrt, abs
from std.memory import alloc

from gpu_kernels import (
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_batch_f16_metric,
    mojo_gpu_pin_free_f16,
)

comptime N = 262_144  # 256k rows: the CPU fp32 ground truth is O(N*K*M) per
# case (x4 cases), so a smaller N keeps validation tractable while exercising
# the identical kernel epilogue + streaming top-k as the 1M cosine path.
comptime K = 768
comptime M = 64  # fewer queries -> faster CPU reference; still a full BM tile
comptime KK = 10  # top-k
comptime NCLUST = 256
comptime TIE_EPS = Float32(1.0e-4)  # FP-tie band (L2 magnitudes are larger)

comptime METRIC_COSINE = 0
comptime METRIC_L2 = 1
comptime METRIC_IP = 2


# Deterministic value generators (clustered, like cosine_f16_test.mojo so the
# top-k distances are well-separated rather than a dense tie thicket).
def _hash01(a: Int, b: Int) -> Float32:
    var x = (a * 2654435761 + b * 40503 + 12345) & 0x7FFFFFFF
    return Float32(x % 20011) * (Float32(2) / 20011.0) - Float32(1)


def _center_val(c: Int, i: Int) -> Float32:
    from std.math import sin, cos

    return sin(Float32(c) * 0.013 + Float32(i) * 0.027) + 0.5 * cos(
        Float32(c * 7 + i * 3) * 0.011
    )


def _emb_raw(row: Int, i: Int) -> Float32:
    var c = row % NCLUST
    var jit = Float32(0.10) + Float32(row % 4096) * (Float32(0.30) / 4096.0)
    return _center_val(c, i) + jit * _hash01(row, i)


def _query_raw(m: Int, i: Int) -> Float32:
    var c = m % NCLUST
    return _center_val(c, i) + 0.05 * _hash01(m * 131 + 7, i)


# Build emb (N*K) + queries (M*K). If `normalize`, each row/query is L2-unit.
def build_data(
    emb: UnsafePointer[Float32, MutAnyOrigin],
    qs: UnsafePointer[Float32, MutAnyOrigin],
    normalize: Bool,
):
    for row in range(N):
        var base = row * K
        var nrm = Float32(0)
        for i in range(K):
            var v = _emb_raw(row, i)
            emb[base + i] = v
            nrm += v * v
        if normalize:
            nrm = sqrt(nrm)
            if nrm == 0:
                nrm = 1
            var inv = Float32(1) / nrm
            for i in range(K):
                emb[base + i] = emb[base + i] * inv
    for m in range(M):
        var base = m * K
        var nrm = Float32(0)
        for i in range(K):
            var v = _query_raw(m, i)
            qs[base + i] = v
            nrm += v * v
        if normalize:
            nrm = sqrt(nrm)
            if nrm == 0:
                nrm = 1
            var inv = Float32(1) / nrm
            for i in range(K):
                qs[base + i] = qs[base + i] * inv


# Scalar fp32 CPU ground truth for one query, one metric. To MATCH the GPU
# (which reads fp16-stored emb), the embedding values are cast through fp16
# before forming dot + norm -- bit-faithful to the resident matrix. Top-k by
# smallest distance with the (dist, rowid) tie-break.
def cpu_topk(
    emb: UnsafePointer[Float32, MutAnyOrigin],
    q: UnsafePointer[Float32, MutAnyOrigin],
    metric: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
):
    # query squared norm in fp32 (matches host qnorm computation).
    var qsq = Float32(0)
    for i in range(K):
        qsq += q[i] * q[i]
    var qn = sqrt(qsq)

    # running ascending top-k (insertion).
    var bd = alloc[Float32](KK)
    var bi = alloc[Int64](KK)
    var cnt = 0
    for row in range(N):
        var base = row * K
        var dot = Float32(0)
        var esq = Float32(0)
        for i in range(K):
            var ev = emb[base + i].cast[DType.float16]().cast[DType.float32]()
            dot += q[i] * ev  # NB: scalar uses fp32 product (ref ground truth)
            esq += ev * ev
        var cd: Float32
        if metric == METRIC_L2:
            cd = qsq + esq - Float32(2) * dot
        elif metric == METRIC_IP:
            cd = -dot
        else:
            var denom = sqrt(esq) * qn
            cd = Float32(1) - dot / denom if denom != 0 else Float32(0)
        var ci = Int64(row)
        var accept = True
        if cnt >= KK:
            var wd = bd[KK - 1]
            var wi = bi[KK - 1]
            if cd > wd or (cd == wd and ci >= wi):
                accept = False
        if accept:
            var pos = cnt if cnt < KK else KK - 1
            while pos > 0:
                var pd = bd[pos - 1]
                var pi = bi[pos - 1]
                if pd > cd or (pd == cd and pi > ci):
                    bd[pos] = pd
                    bi[pos] = pi
                    pos -= 1
                else:
                    break
            bd[pos] = cd
            bi[pos] = ci
            if cnt < KK:
                cnt += 1
    for j in range(KK):
        out_ids[j] = bi[j]
        out_dists[j] = bd[j]
    bd.free()
    bi.free()


def run_case(metric: Int, normalize: Bool, name: String) raises -> Bool:
    print("====================================================")
    print(
        "metric:", name, " data:",
        "NORMALIZED" if normalize else "UNNORMALIZED",
        " N =", N, " K =", K, " M =", M, " k =", KK,
    )

    var emb = alloc[Float32](N * K)
    var qs = alloc[Float32](M * K)
    build_data(emb, qs, normalize)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qs)
    )

    var handle = mojo_gpu_pin_f16(emb_imm, N, K)
    if handle == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)

    # GPU fused metric path.
    var gpu_ids = alloc[Int64](M * KK)
    var gpu_dists = alloc[Float32](M * KK)
    var rc = mojo_gpu_pin_query_topk_batch_f16_metric(
        h, qs_imm, M, KK, metric, gpu_ids, gpu_dists
    )
    if rc != 0:
        mojo_gpu_pin_free_f16(h)
        emb.free()
        qs.free()
        gpu_ids.free()
        gpu_dists.free()
        raise Error(
            String("metric entry rc=") + String(Int(rc))
            + " (need GPU_OP_TENSORCORE=1 on an NVIDIA build for non-cosine)"
        )

    # CPU ground truth per query.
    var ref_ids = alloc[Int64](M * KK)
    var ref_dists = alloc[Float32](M * KK)
    var oneq_ids = alloc[Int64](KK)
    var oneq_dists = alloc[Float32](KK)
    for m in range(M):
        var qptr = UnsafePointer[Float32, MutAnyOrigin](
            unsafe_from_address=Int(qs) + m * K * 4
        )
        cpu_topk(emb, qptr, metric, oneq_ids, oneq_dists)
        for j in range(KK):
            ref_ids[m * KK + j] = oneq_ids[j]
            ref_dists[m * KK + j] = oneq_dists[j]

    var exact_rows = 0
    var total_overlap = 0
    var genuine_miss = 0
    for m in range(M):
        var match_row = True
        var worst = Float32(-3.0e38)
        for j in range(KK):
            if gpu_dists[m * KK + j] > worst:
                worst = gpu_dists[m * KK + j]
        for j in range(KK):
            if gpu_ids[m * KK + j] != ref_ids[m * KK + j]:
                match_row = False
            var rid = ref_ids[m * KK + j]
            var found = False
            for b in range(KK):
                if gpu_ids[m * KK + b] == rid:
                    found = True
                    break
            if found:
                total_overlap += 1
            else:
                var d = ref_dists[m * KK + j]
                if abs(d - worst) > TIE_EPS:
                    genuine_miss += 1
        if match_row:
            exact_rows += 1

    var recall = Float64(total_overlap) / Float64(M * KK)
    print("exact-id-match rows:", exact_rows, "/", M)
    print(
        "recall@10:", recall, "  genuine misses (beyond tie eps):",
        genuine_miss,
    )
    var ok = recall >= 0.99 and genuine_miss == 0
    if ok:
        print("RESULT: PASS (recall >= 0.99, 0 genuine misses)")
    else:
        print("RESULT: (recall<0.99 or genuine misses present)")

    mojo_gpu_pin_free_f16(h)
    emb.free()
    qs.free()
    gpu_ids.free()
    gpu_dists.free()
    ref_ids.free()
    ref_dists.free()
    oneq_ids.free()
    oneq_dists.free()
    return ok


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var flag = getenv("GPU_OP_TENSORCORE", "")
    print("=== fused tensor-core kNN: L2 + inner-product metric validation ===")
    print("GPU_OP_TENSORCORE =", "<set>" if flag != "" else "<unset>")

    # Normalized: all three exact.
    var ip_norm = run_case(METRIC_IP, True, "inner-product")
    var l2_norm = run_case(METRIC_L2, True, "L2 (euclidean)")
    # Unnormalized L2: documents the catastrophic-cancellation behavior.
    var l2_raw = run_case(METRIC_L2, False, "L2 (euclidean)")
    # Unnormalized IP: unaffected by cancellation (pure dot).
    var ip_raw = run_case(METRIC_IP, False, "inner-product")

    print("====================================================")
    print("SUMMARY:")
    print("  IP  normalized   :", "PASS" if ip_norm else "FAIL")
    print("  L2  normalized   :", "PASS" if l2_norm else "FAIL")
    print("  L2  unnormalized :",
          "PASS" if l2_raw else "(expected misses: fp16 cancellation)")
    print("  IP  unnormalized :", "PASS" if ip_raw else "FAIL")
    print("done")
