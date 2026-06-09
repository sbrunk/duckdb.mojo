"""Benchmark: the pin-resident win.

A DB-wide unified allocator isn't buildable (see apple_alloc_route_probe.mojo), so
the amortized win comes from KEEPING THE COLUMN GPU-RESIDENT and running many queries
against it. This benchmark compares, sweeping K (per-row work):

  CPU SIMD            - M queries, SIMD over the host-resident column.
  GPU re-upload       - M queries, each re-copies the whole column H->D.
  GPU pin-resident    - copy the column H->D ONCE (the "pin"), then M queries are
                        pure kernel launches over the resident buffer.

The thesis: pin-resident GPU per-query collapses once the one-time upload is
amortized across queries, and the margin widens with K. Run single-threaded.
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.math import sqrt
from std.sys.info import simd_width_of, has_apple_gpu_accelerator
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime ROWS = 50_000        # keep ROWS*K*4 bytes within unified-memory budget at K=4096 (~0.8 GB)
comptime M = 20               # number of queries to amortize the one-time pin over
comptime W = simd_width_of[DType.float32]()


def cosine_kernel_warp(
    emb: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    q: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    res: UnsafePointer[Scalar[DType.float32], MutAnyOrigin],
    n_rows: Int,
    K: Int,
    qnorm: Float32,
):
    var row = Int(block_idx.x)
    if row >= n_rows:
        return
    var lane = Int(thread_idx.x)
    var base = row * K
    var dot = Float32(0)
    var na = Float32(0)
    var i = lane
    while i < K:
        var av = emb[base + i]
        dot += av * q[i]
        na += av * av
        i += 32
    dot = warp.sum(dot)
    na = warp.sum(na)
    if lane == 0:
        var denom = sqrt(na) * qnorm
        res[row] = Float32(1) - dot / denom if denom != 0 else Float32(0)


def gen_val(idx: Int) -> Float32:
    return Float32((idx * 1103515245 + 12345) % 2048) * 0.001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


def cpu_cosine(
    emb: UnsafePointer[Float32, MutAnyOrigin],
    q: UnsafePointer[Float32, MutAnyOrigin],
    out_ptr: UnsafePointer[Float32, MutAnyOrigin],
    K: Int,
    qnorm: Float32,
):
    for row in range(ROWS):
        var base = row * K
        var vdot = SIMD[DType.float32, W](0)
        var vna = SIMD[DType.float32, W](0)
        var i = 0
        while i < K:
            var av = (emb + base + i).load[width=W]()
            var qv = (q + i).load[width=W]()
            vdot += av * qv
            vna += av * av
            i += W
        var d = vdot.reduce_add()
        var na = vna.reduce_add()
        var denom = sqrt(na) * qnorm
        out_ptr[row] = Float32(1) - d / denom if denom != 0 else Float32(0)


def run_k(ctx: DeviceContext, K: Int) raises:
    # ---- host column + query vectors ----
    var emb = alloc[Float32](ROWS * K)
    for r in range(ROWS):
        for i in range(K):
            emb[r * K + i] = gen_val(r * K + i)

    var queries = alloc[Float32](M * K)
    var qnorms = alloc[Float32](M)
    for m in range(M):
        var nrm = Float32(0)
        for i in range(K):
            var v = query_val(m, i)
            queries[m * K + i] = v
            nrm += v * v
        qnorms[m] = sqrt(nrm)

    var cpu_out = alloc[Float32](ROWS)
    var gpu_out = alloc[Float32](ROWS)

    # ---- CPU SIMD: M queries ----
    var t0 = perf_counter_ns()
    for m in range(M):
        cpu_cosine(emb, queries + m * K, cpu_out, K, qnorms[m])
    var cpu_ns = perf_counter_ns() - t0

    # ---- GPU resident buffers ----
    var emb_dev = ctx.enqueue_create_buffer[DType.float32](ROWS * K)
    var q_dev = ctx.enqueue_create_buffer[DType.float32](K)
    var out_dev = ctx.enqueue_create_buffer[DType.float32](ROWS)
    ctx.synchronize()

    # ---- GPU pin: one-time upload of the whole column ----
    var tp = perf_counter_ns()
    ctx.enqueue_copy(emb_dev, emb)
    ctx.synchronize()
    var pin_ns = perf_counter_ns() - tp

    # Warm up: pay the one-time kernel first-dispatch/compile cost before timing,
    # so the pin-resident loop (which runs first) isn't unfairly penalized.
    ctx.enqueue_copy(q_dev, queries)
    ctx.enqueue_function[cosine_kernel_warp](
        emb_dev, q_dev, out_dev, ROWS, K, qnorms[0], grid_dim=ROWS, block_dim=32,
    )
    ctx.enqueue_copy(gpu_out, out_dev)
    ctx.synchronize()

    # ---- GPU pin-resident: M queries are pure launches over the resident buffer ----
    var tg = perf_counter_ns()
    for m in range(M):
        ctx.enqueue_copy(q_dev, queries + m * K)  # small: K floats
        ctx.enqueue_function[cosine_kernel_warp](
            emb_dev, q_dev, out_dev, ROWS, K, qnorms[m],
            grid_dim=ROWS, block_dim=32,
        )
        ctx.enqueue_copy(gpu_out, out_dev)
        ctx.synchronize()
    var gpu_pin_ns = perf_counter_ns() - tg

    # ---- GPU re-upload: M queries, each re-copies the whole column ----
    var tr = perf_counter_ns()
    for m in range(M):
        ctx.enqueue_copy(emb_dev, emb)             # the per-query upload (scan/upload-bound)
        ctx.enqueue_copy(q_dev, queries + m * K)
        ctx.enqueue_function[cosine_kernel_warp](
            emb_dev, q_dev, out_dev, ROWS, K, qnorms[m],
            grid_dim=ROWS, block_dim=32,
        )
        ctx.enqueue_copy(gpu_out, out_dev)
        ctx.synchronize()
    var gpu_reup_ns = perf_counter_ns() - tr

    # ---- correctness: last query GPU vs CPU ----
    cpu_cosine(emb, queries + (M - 1) * K, cpu_out, K, qnorms[M - 1])
    var max_err = Float32(0)
    for r in range(ROWS):
        var e = abs(cpu_out[r] - gpu_out[r])
        if e > max_err:
            max_err = e

    var cpu_q = Float64(cpu_ns) / Float64(M) / 1e6
    var pin_q = Float64(gpu_pin_ns) / Float64(M) / 1e6
    var reup_q = Float64(gpu_reup_ns) / Float64(M) / 1e6

    print("K =", K, " (", ROWS, "rows,", M, "queries )")
    print("  one-time pin (H->D upload):", Float64(pin_ns) / 1e6, "ms")
    print("  CPU SIMD        / query:", cpu_q, "ms")
    print("  GPU re-upload   / query:", reup_q, "ms   speedup vs CPU:", cpu_q / reup_q, "x")
    print("  GPU pin-resident/ query:", pin_q, "ms   speedup vs CPU:", cpu_q / pin_q, "x")
    print("  max abs err (GPU vs CPU):", max_err)
    print()

    emb.free()
    queries.free()
    qnorms.free()
    cpu_out.free()
    gpu_out.free()


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("has_apple_gpu_accelerator:", has_apple_gpu_accelerator(), " CPU SIMD width:", W)
    print()
    var ctx = DeviceContext()
    run_k(ctx, 256)
    run_k(ctx, 1024)
    run_k(ctx, 4096)
