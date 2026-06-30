"""Batched-vs-single-query exact correctness test for the GPU cosine top-k path.

The whole point of the batched kernel (`mojo_gpu_pin_query_topk_batch[_f16]`) is
that it reads the resident N x K matrix ONCE per query-tile instead of once per
query -- but it must still return EXACTLY what M separate single-query calls
return. This test asserts precisely that:

  for each of M queries j, the batched top-k (ids + dists) for query j ==
  the single-query `mojo_gpu_pin_query_topk[_f16]` result for query j,

bit-for-bit on the ids and within ~1e-6 on the dists, for BOTH precisions
(fp32 + fp16), for M in {8, 64} and a couple of k values, on synthetic
*clustered* unit-norm data (so many rows are genuinely close to each query and
the top-k selection / tie-break is exercised, not just trivially separated).

Run (local Apple GPU or frederick):
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/cosine_batch_test.mojo
"""

from std.sys import has_accelerator
from std.math import sqrt, sin, cos
from std.memory import alloc

from gpu_kernels import (
    mojo_gpu_pin,
    mojo_gpu_pin_query_topk,
    mojo_gpu_pin_query_topk_batch,
    mojo_gpu_pin_free,
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime N = 40_000
comptime K = 384
comptime NCLUSTERS = 16


# Deterministic cluster-center direction (no random()): each center is a smooth
# function of (cluster, dim), normalized later. Clustering makes many rows close
# to a given query so the top-k boundary + (dist, rowid) ties matter.
def center_val(c: Int, i: Int) -> Float32:
    var t = Float32((c * 131 + i * 7) % 360) * Float32(3.14159265 / 180.0)
    return sin(t) + Float32(0.5) * cos(Float32(2) * t)


# A row near cluster `c` with a small per-row perturbation, then unit-normalized.
def emb_unit(row: Int, i: Int, c: Int) -> Float32:
    var pert = Float32(((row * 2654435761 + i * 40503) & 0x7FFFFFFF) % 101 - 50) * Float32(0.002)
    return center_val(c, i) + pert


# A query near a cluster (different perturbation), unit-normalized at use sites.
def query_raw(m: Int, i: Int, c: Int) -> Float32:
    var pert = Float32(((m * 48271 + i * 31) & 0x7FFFFFFF) % 81 - 40) * Float32(0.003)
    return center_val(c, i) + pert


def fill_unit(p: UnsafePointer[Float32, MutAnyOrigin], off: Int):
    var nrm = Float32(0)
    for i in range(K):
        nrm += p[off + i] * p[off + i]
    nrm = sqrt(nrm)
    if nrm == 0:
        nrm = 1
    for i in range(K):
        p[off + i] = p[off + i] / nrm


def check_eq(
    label: String,
    batch_ids: UnsafePointer[Int64, MutAnyOrigin],
    batch_dists: UnsafePointer[Float32, MutAnyOrigin],
    single_ids: UnsafePointer[Int64, MutAnyOrigin],
    single_dists: UnsafePointer[Float32, MutAnyOrigin],
    k: Int,
) raises:
    for j in range(k):
        var id_ok = batch_ids[j] == single_ids[j]
        var derr = abs(batch_dists[j] - single_dists[j])
        if not id_ok or derr > Float32(1.0e-6):
            print(
                "  MISMATCH",
                label,
                "slot",
                j,
                ": batch(id=",
                batch_ids[j],
                ", d=",
                batch_dists[j],
                ") single(id=",
                single_ids[j],
                ", d=",
                single_dists[j],
                ") |derr|=",
                derr,
            )
            raise Error(String("FAIL: ") + label)


def run_case[
    is_f16: Bool
](handle: Int, qs: UnsafePointer[Float32, MutAnyOrigin], M: Int, k: Int) raises:
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qs)
    )

    # Batched call (one matrix read per query-tile).
    var b_ids = alloc[Int64](M * k)
    var b_dists = alloc[Float32](M * k)
    var rc: Int32

    comptime if is_f16:
        rc = mojo_gpu_pin_query_topk_batch_f16(h, qs_imm, M, k, b_ids, b_dists)
    else:
        rc = mojo_gpu_pin_query_topk_batch(h, qs_imm, M, k, b_ids, b_dists)
    if rc != 0:
        raise Error(String("batch rc=") + String(Int(rc)))

    # Single-query reference per query.
    var s_ids = alloc[Int64](k)
    var s_dists = alloc[Float32](k)
    for m in range(M):
        var q_imm = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qs) + m * K * 4
        )

        comptime if is_f16:
            rc = mojo_gpu_pin_query_topk_f16(h, q_imm, k, s_ids, s_dists)
        else:
            rc = mojo_gpu_pin_query_topk(h, q_imm, k, s_ids, s_dists)
        if rc != 0:
            raise Error(String("single rc=") + String(Int(rc)))
        check_eq(
            String("M=") + String(M) + " k=" + String(k) + " q[" + String(m) + "]",
            b_ids + m * k,
            b_dists + m * k,
            s_ids,
            s_dists,
            k,
        )

    var prec = "fp16" if is_f16 else "fp32"
    print("  PASS", prec, "M =", M, "k =", k, "(batch == single, all", M, "queries)")
    b_ids.free()
    b_dists.free()
    s_ids.free()
    s_dists.free()


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("cosine BATCH==SINGLE test: N =", N, " K =", K, " clusters =", NCLUSTERS)

    var emb = alloc[Float32](N * K)
    for r in range(N):
        var c = r % NCLUSTERS
        for i in range(K):
            emb[r * K + i] = emb_unit(r, i, c)
        fill_unit(emb, r * K)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    comptime MMAX = 64
    var qs = alloc[Float32](MMAX * K)
    for m in range(MMAX):
        var c = m % NCLUSTERS  # each query lands near a cluster
        for i in range(K):
            qs[m * K + i] = query_raw(m, i, c)
        fill_unit(qs, m * K)

    # fp32
    var h32 = mojo_gpu_pin(emb_imm, N, K)
    if h32 == 0:
        raise Error("mojo_gpu_pin failed")
    run_case[False](h32, qs, 8, 10)
    run_case[False](h32, qs, 64, 10)
    run_case[False](h32, qs, 64, 100)
    mojo_gpu_pin_free(
        UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=h32)
    )

    # fp16
    var h16 = mojo_gpu_pin_f16(emb_imm, N, K)
    if h16 == 0:
        raise Error("mojo_gpu_pin_f16 failed")
    run_case[True](h16, qs, 8, 10)
    run_case[True](h16, qs, 64, 10)
    run_case[True](h16, qs, 64, 100)
    mojo_gpu_pin_free_f16(
        UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=h16)
    )

    emb.free()
    qs.free()
    print("ALL PASS")
