"""Warm per-query latency microbench for the GPU exact-cosine top-k path.

Pins an N x K matrix once via `mojo_gpu_pin`, then times many warm
`mojo_gpu_pin_query_topk` calls with high-resolution `perf_counter_ns` and
reports the warm median in microseconds. This drives the exact same C-ABI entry
points the `gpu_cosine_topk` table function calls, so the number is the warm
per-query latency the TF sees (minus DuckDB's own bind plumbing).

Run (set N/K via the comptime constants below or copies of this file):
  pixi run mojo run -I extensions/mojo-gpu-operator/src \
      extensions/mojo-gpu-operator/bench/cosine_topk_latency.mojo
"""

from std.sys import has_accelerator
from std.math import sqrt
from std.memory import alloc
from std.time import perf_counter_ns

from gpu_kernels import (
    mojo_gpu_pin,
    mojo_gpu_pin_query_topk,
    mojo_gpu_pin_free,
)

comptime N = 100_000
comptime K = 384
comptime KK = 10  # top-k
comptime WARMUP = 5
comptime ITERS = 50


def emb_val(row: Int, i: Int) -> Float32:
    var idx = (row * 2654435761 + i * 40503 + 12345) & 0x7FFFFFFF
    return Float32(idx % 2048) * 0.001 + Float32(row % 7) * 0.0001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("cosine top-k latency: N =", N, " K =", K, " k =", KK)

    var emb = alloc[Float32](N * K)
    for r in range(N):
        for i in range(K):
            emb[r * K + i] = emb_val(r, i)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    var handle = mojo_gpu_pin(emb_imm, N, K)
    if handle == 0:
        raise Error("mojo_gpu_pin failed")
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)

    var out_ids = alloc[Int64](KK)
    var out_dists = alloc[Float32](KK)

    # Distinct query per iter so nothing is trivially cached.
    var qbuf = alloc[Float32]((WARMUP + ITERS) * K)
    for m in range(WARMUP + ITERS):
        for i in range(K):
            qbuf[m * K + i] = query_val(m, i)

    # Warm-up (includes the cold first call that triggers any lazy state).
    for m in range(WARMUP):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qbuf) + m * K * 4
        )
        var rc = mojo_gpu_pin_query_topk(h, q, KK, out_ids, out_dists)
        if rc != 0:
            raise Error(String("topk rc=") + String(Int(rc)))

    var times = alloc[Int64](ITERS)
    for m in range(ITERS):
        var q = UnsafePointer[Float32, ImmutAnyOrigin](
            unsafe_from_address=Int(qbuf) + (WARMUP + m) * K * 4
        )
        var t0 = perf_counter_ns()
        var rc = mojo_gpu_pin_query_topk(h, q, KK, out_ids, out_dists)
        var t1 = perf_counter_ns()
        if rc != 0:
            raise Error(String("topk rc=") + String(Int(rc)))
        times[m] = Int64(t1 - t0)

    # Median (simple insertion sort; ITERS is small).
    for a in range(ITERS):
        for b in range(a + 1, ITERS):
            if times[b] < times[a]:
                var t = times[a]
                times[a] = times[b]
                times[b] = t
    var med = times[ITERS // 2]
    var mn = times[0]
    var mx = times[ITERS - 1]
    print("  warm median:", Float64(med) / 1000.0, "us  (min", Float64(mn) / 1000.0, "us, max", Float64(mx) / 1000.0, "us)")
    print("  warm median:", Float64(med) / 1_000_000.0, "ms")

    mojo_gpu_pin_free(h)
    emb.free()
    out_ids.free()
    out_dists.free()
    qbuf.free()
    times.free()
    print("DONE")
