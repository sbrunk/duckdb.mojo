"""Recall of the batched fp16 top-k THROUGH THE OPERATOR ENTRY POINT.

Calls the real exported `mojo_gpu_pin_query_topk_batch_f16` (the entry the C++
GpuCosinePhysicalOp invokes) and compares its M*k result against a per-query
ground truth computed via `mojo_gpu_pin_query_topk_f16` (the single-query exact
path, which NEVER routes through the batched fused kernel). Identical data to
cosine_batch_latency.mojo / tc_knn_fused.mojo.

Run it twice on frederick:
  * GPU_OP_TENSORCORE unset  -> batched scalar path; sanity recall should be ~1.0
  * GPU_OP_TENSORCORE=1       -> batched FUSED tensor-core path; require
                                 recall@10 >= 0.99 and 0 genuine misses.

  LD_LIBRARY_PATH=/run/opengl-driver/lib:$LD_LIBRARY_PATH \
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/tc_knn_operator_recall.mojo
"""

from std.sys import has_accelerator
from std.os import getenv
from std.math import sqrt, abs
from std.memory import alloc

from gpu_kernels import (
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime N = 1_048_576
comptime K = 768
comptime M = 128
comptime KK = 10  # top-k
comptime TIE_EPS = Float32(1.0e-5)


def emb_val(row: Int, i: Int) -> Float32:
    var idx = (row * 2654435761 + i * 40503 + 12345) & 0x7FFFFFFF
    return Float32(idx % 2048) * 0.001 + Float32(row % 7) * 0.0001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var flag = getenv("GPU_OP_TENSORCORE", "")
    print("=== batched fp16 top-k recall through the operator entry ===")
    print(
        "N =", N, " K =", K, " M =", M, " k =", KK,
        "  GPU_OP_TENSORCORE =", "<set>" if flag != "" else "<unset>",
    )

    var emb = alloc[Float32](N * K)
    for r in range(N):
        for i in range(K):
            emb[r * K + i] = emb_val(r, i)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    var handle = mojo_gpu_pin_f16(emb_imm, N, K)
    if handle == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)

    var qs = alloc[Float32](M * K)
    for m in range(M):
        for i in range(K):
            qs[m * K + i] = query_val(m, i)
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qs)
    )

    # Ground truth: per-query single-query exact top-k (scalar warp path; does
    # NOT touch the batched fused kernel regardless of the flag).
    var ref_ids = alloc[Int64](M * KK)
    var ref_dists = alloc[Float32](M * KK)
    var oneq_ids = alloc[Int64](KK)
    var oneq_dists = alloc[Float32](KK)
    for m in range(M):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qs) + m * K * 4
        )
        var rc = mojo_gpu_pin_query_topk_f16(h, q, KK, oneq_ids, oneq_dists)
        if rc != 0:
            raise Error(String("single rc=") + String(Int(rc)))
        for j in range(KK):
            ref_ids[m * KK + j] = oneq_ids[j]
            ref_dists[m * KK + j] = oneq_dists[j]

    # Path under test: the BATCHED operator entry (flag selects scalar vs fused).
    var bat_ids = alloc[Int64](M * KK)
    var bat_dists = alloc[Float32](M * KK)
    var brc = mojo_gpu_pin_query_topk_batch_f16(
        h, qs_imm, M, KK, bat_ids, bat_dists
    )
    if brc != 0:
        raise Error(String("batch rc=") + String(Int(brc)))

    var exact_rows = 0
    var total_overlap = 0
    var genuine_miss = 0
    var first_mismatch = -1
    for m in range(M):
        var match_row = True
        var worst = Float32(0)  # batched result's top-k boundary
        for j in range(KK):
            if bat_dists[m * KK + j] > worst:
                worst = bat_dists[m * KK + j]
        for j in range(KK):
            if bat_ids[m * KK + j] != ref_ids[m * KK + j]:
                match_row = False
            var rid = ref_ids[m * KK + j]
            var found = False
            for b in range(KK):
                if bat_ids[m * KK + b] == rid:
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
        elif first_mismatch < 0:
            first_mismatch = m

    var recall = Float64(total_overlap) / Float64(M * KK)
    print("exact-id-match rows:", exact_rows, "/", M)
    print("recall@10:", recall, "  genuine misses (beyond fp16 tie eps):",
          genuine_miss)
    if first_mismatch >= 0:
        var m = first_mismatch
        print("first non-exact row", m)
        for j in range(KK):
            print("    j", j, "batch(", bat_ids[m*KK+j], bat_dists[m*KK+j],
                  ") ref(", ref_ids[m*KK+j], ref_dists[m*KK+j], ")")

    if recall >= 0.99 and genuine_miss == 0:
        print("RESULT: PASS (recall >= 0.99, 0 genuine misses)")
    else:
        print("RESULT: FAIL")

    mojo_gpu_pin_free_f16(h)
    emb.free()
    qs.free()
    print("done")
