"""Batch-amortization latency sweep for the GPU exact-cosine top-k path.

Pins a 1M x 768 matrix once (fp16, the gpu_cosine_topk default), then measures
the BATCHED per-query latency as a function of batch size M: warm-times one
`mojo_gpu_pin_query_topk_batch_f16` call over M queries and divides by M. The
point: because the batched kernel reads the resident matrix ONCE per query-tile
(not once per query), per-query latency should DROP sharply as M grows -- the
N*K bandwidth cost is amortized across the whole batch. At M=1 it matches the
single-query latency; at M=1000 it should be far below it.

For contrast we also time the single-query path at M=1 (the ~2ms floor a
per-query index/HNSW lookup competes with, except HNSW can't batch at all).

Run on frederick (RTX 4090):
  LD_LIBRARY_PATH=/run/opengl-driver/lib:$LD_LIBRARY_PATH \
  pixi run mojo run -I extensions/mojo-gpu-operator/src \
      extensions/mojo-gpu-operator/bench/cosine_batch_latency.mojo
"""

from std.sys import has_accelerator
from std.math import sqrt
from std.memory import alloc
from std.time import perf_counter_ns

from gpu_kernels import (
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime N = 1_000_000
comptime K = 768
comptime KK = 10  # top-k


def emb_val(row: Int, i: Int) -> Float32:
    var idx = (row * 2654435761 + i * 40503 + 12345) & 0x7FFFFFFF
    return Float32(idx % 2048) * 0.001 + Float32(row % 7) * 0.0001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


def bench_batch(
    h: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
    iters: Int,
) raises -> Float64:
    var ids = alloc[Int64](M * KK)
    var dists = alloc[Float32](M * KK)
    # warmup
    for _ in range(3):
        var rc = mojo_gpu_pin_query_topk_batch_f16(h, qs, M, KK, ids, dists)
        if rc != 0:
            raise Error(String("batch rc=") + String(Int(rc)))
    var best = Float64(1.0e30)
    for _ in range(iters):
        var t0 = perf_counter_ns()
        _ = mojo_gpu_pin_query_topk_batch_f16(h, qs, M, KK, ids, dists)
        var t1 = perf_counter_ns()
        var dt = Float64(t1 - t0)
        if dt < best:
            best = dt
    ids.free()
    dists.free()
    return best  # ns for the whole M-query batch


def report(
    h: UnsafePointer[NoneType, MutAnyOrigin],
    qs: UnsafePointer[Float32, ImmutAnyOrigin],
    M: Int,
) raises:
    var iters = 30 if M >= 100 else 50
    var ns = bench_batch(h, qs, M, iters)
    var total_ms = ns / 1.0e6
    var per_q_us = (ns / Float64(M)) / 1000.0
    var qps = Float64(M) * 1.0e9 / ns
    print("  ", M, "   ", total_ms, "   ", per_q_us, "   ", qps)


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("batch latency sweep: N =", N, " K =", K, " k =", KK, "(fp16)")

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

    comptime MMAX = 1000
    var qs = alloc[Float32](MMAX * K)
    for m in range(MMAX):
        for i in range(K):
            qs[m * K + i] = query_val(m, i)
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qs)
    )

    # Single-query reference (the ~2ms floor a per-query lookup competes with).
    var sids = alloc[Int64](KK)
    var sdists = alloc[Float32](KK)
    for _ in range(5):
        _ = mojo_gpu_pin_query_topk_f16(h, qs_imm, KK, sids, sdists)
    var sbest = Float64(1.0e30)
    for _ in range(50):
        var t0 = perf_counter_ns()
        _ = mojo_gpu_pin_query_topk_f16(h, qs_imm, KK, sids, sdists)
        var t1 = perf_counter_ns()
        var dt = Float64(t1 - t0)
        if dt < sbest:
            sbest = dt
    print("single-query latency:", sbest / 1000.0, "us/query")
    sids.free()
    sdists.free()

    print("")
    print("  M     batch_total_ms   per_query_us   queries/sec")
    report(h, qs_imm, 1)
    report(h, qs_imm, 16)
    report(h, qs_imm, 100)
    report(h, qs_imm, 1000)

    mojo_gpu_pin_free_f16(h)
    emb.free()
    qs.free()
    print("done")
