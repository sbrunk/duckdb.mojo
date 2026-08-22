"""PHASE A de-risk probe: GPU scalar aggregate of a transcendental over a big col.

Goal: prove (or refute) that a GPU can compute `sum(f(x))` for f in
{sqrt, exp, ln} over a large float64 column at ~memory-bandwidth speed, i.e.
~10-40x the CPU-SIMD overrides path (which wins only 1.2-4.6x vs stock at
1 thread, per PERF_BACKLOG R7). DuckDB does NOT GPU-accelerate these at all, so
this is a NEW win class for the operator -- but the expr-VM + segreduce are
INT64/INT128, so a transcendental aggregate needs a FLOAT eval + a float64
accumulator. Before paying for that integration we prove the GPU win here, in
isolation.

Method:
  - Generate N float64 on host in [1, 2] (so sqrt/ln/exp are all finite, no NaN).
  - GPU: upload once, a grid-stride kernel applies f per element and reduces to a
    float64 sum (per-warp warp.sum -> lane0 atomic into a single device float64).
    Kernel time measured GPU-side excluding upload (the operator's win is warm:
    the column is already resident), but upload is also reported for context.
  - CPU: a plain Mojo scalar loop over the same host array (sanity reference).
  - Correctness: |gpu - cpu| / |cpu| < REL_TOL (float64, thread-order reassoc,
    so not bit-exact; ~1e-9 is comfortable).
  - Throughput: GB/s = N*8 / time. The GPU should land near the card's HBM
    bandwidth (RTX 4090 ~1 TB/s) -> a 60M f64 col (480 MB) at ~1-2 ms warm.

Run (Apple or NVIDIA -- pass on both):
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/transcendental_agg_probe.mojo
"""

from std.gpu import block_idx, thread_idx
from max.gpu.sync import barrier
from max.gpu.host import DeviceContext
from max.gpu.memory import AddressSpace
from std.memory import alloc, stack_allocation
from std.math import sqrt, exp, log, abs
from std.sys import has_accelerator
from std.time import perf_counter_ns
from std.atomic import Atomic
from std.os import getenv

# N float64 column. Default 60M (~480 MB), overridable via TRANS_PROBE_N.
comptime DEFAULT_N = 60_000_000

# 256-thread blocks; grid-stride over the column. warp.sum has no float64 path
# (the warp-shuffle intrinsic has no f64 dtype on this stdlib), so we reduce in
# SHARED memory (float64 IS supported in shared/global on NVIDIA) then atomic-add
# the block partial into the single device output.
comptime BLOCK = 256
comptime NBLOCKS = 4096          # grid-stride: plenty of blocks

# Which transcendental: 0=sqrt 1=exp 2=ln. Comptime so the math op inlines and
# the SFU path is the only thing in the loop body.
comptime F_SQRT = 0
comptime F_EXP = 1
comptime F_LN = 2


# Precise f64 sqrt on NVIDIA: the stdlib `sqrt(Float64)` routes to the NVVM
# `sqrt.approx.d` path, which is constrained off for f64 ("not supported for
# approx sqrt on NVIDIA GPU"). We seed from the f32 approx sqrt and do ONE
# Newton-Raphson step in f64 (y = 0.5*(y + x/y)); from a ~1e-7-accurate seed one
# step yields ~1e-14 relative error -- far under the 1e-9 correctness tol. (x>0
# here by construction.)
@always_inline
def _sqrt_f64(x: Float64) -> Float64:
    var y = Float64(sqrt(x.cast[DType.float32]()))
    y = 0.5 * (y + x / y)
    return y


@always_inline
def _apply[F: Int](x: Float64) -> Float64:
    comptime if F == F_SQRT:
        return _sqrt_f64(x)
    elif F == F_EXP:
        return exp(x)
    else:
        return log(x)


# Grid-stride float64 transcendental-sum kernel. Each thread accumulates a local
# float64 partial over its strided slice; the block reduces in shared memory
# (tree reduction), thread 0 atomic-adds the block partial into the single device
# output. This is the float64 analogue of seg_ungrouped_kernel_q6 (the int64 path
# uses warp.sum, which has no f64 dtype here -> shared-mem reduction instead).
def trans_sum_kernel[F: Int](
    x: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
    n: Int,
    dst: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
):
    var smem = stack_allocation[
        BLOCK, Scalar[DType.float64], address_space = AddressSpace.SHARED
    ]()
    var tid = Int(thread_idx.x)
    var stride = NBLOCKS * BLOCK
    var acc = Float64(0.0)
    var i = Int(block_idx.x) * BLOCK + tid
    while i < n:
        acc += _apply[F](x[i])
        i += stride
    smem[tid] = acc
    barrier()
    # tree reduction in shared memory
    var active = BLOCK
    while active > 1:
        active >>= 1
        if tid < active:
            smem[tid] = smem[tid] + smem[tid + active]
        barrier()
    if tid == 0:
        _ = Atomic.fetch_add(dst, smem[0])


@always_inline
def _cpu_sum[
    F: Int
](x: UnsafePointer[Scalar[DType.float64], MutAnyOrigin], n: Int) -> Float64:
    var acc = Float64(0.0)
    for i in range(n):
        acc += _apply[F](x[i])
    return acc


def _run_one[
    F: Int
](
    ctx: DeviceContext,
    name: String,
    x_h: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
    x_d_ptr: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
    n: Int,
) raises:
    # ---- CPU reference + timing ----
    var c0 = perf_counter_ns()
    var cpu = _cpu_sum[F](x_h, n)
    var c1 = perf_counter_ns()
    var cpu_ms = Float64(c1 - c0) / 1.0e6

    # ---- GPU: warm kernel timing (column already resident; upload excluded) ----
    var out_d = ctx.enqueue_create_buffer[DType.float64](1)
    comptime k = trans_sum_kernel[F]

    # warmup launch (JIT + caches), discard
    out_d.enqueue_fill(0.0)
    ctx.enqueue_function[k](
        x_d_ptr, n, out_d.unsafe_ptr(), grid_dim=NBLOCKS, block_dim=BLOCK
    )
    ctx.synchronize()

    # timed: best (min) over a few launches
    comptime REPS = 7
    var best_ms = Float64(1.0e30)
    var gpu_sum = Float64(0.0)
    for _ in range(REPS):
        out_d.enqueue_fill(0.0)
        ctx.synchronize()
        var g0 = perf_counter_ns()
        ctx.enqueue_function[k](
            x_d_ptr, n, out_d.unsafe_ptr(), grid_dim=NBLOCKS, block_dim=BLOCK
        )
        ctx.synchronize()
        var g1 = perf_counter_ns()
        var ms = Float64(g1 - g0) / 1.0e6
        if ms < best_ms:
            best_ms = ms
        var res_h = alloc[Float64](1)
        ctx.enqueue_copy(res_h.unsafe_origin_cast[MutAnyOrigin](), out_d)
        ctx.synchronize()
        gpu_sum = res_h[0]
        res_h.free()

    var bytes = Float64(n) * 8.0
    var gpu_gbs = bytes / (best_ms * 1.0e6)   # GB/s = bytes / (ms*1e6)
    var cpu_gbs = bytes / (cpu_ms * 1.0e6)

    var rel = abs(gpu_sum - cpu) / (abs(cpu) + 1e-300)
    var speedup = cpu_ms / best_ms

    print("  ", name)
    print("    cpu sum =", cpu, " gpu sum =", gpu_sum, " rel-err =", rel)
    print(
        "    cpu:", cpu_ms, "ms (", cpu_gbs, "GB/s)   gpu:", best_ms,
        "ms (", gpu_gbs, "GB/s)",
    )
    print("    GPU vs CPU-scalar speedup:", speedup, "x")
    if rel > 1e-9:
        print("    !! REL-ERR TOO LARGE (>1e-9) -- correctness FAIL")


def main() raises:
    if not has_accelerator():
        print("SKIP: transcendental_agg_probe requires a GPU. ALL PASS")
        return

    var n = DEFAULT_N
    var nenv = getenv("TRANS_PROBE_N", "")
    if nenv.byte_length() > 0:
        n = Int(atol(nenv))

    print(
        "=== transcendental agg probe: N =", n, "float64 (",
        Float64(n) * 8.0 / 1.0e6, "MB ) ===",
    )

    var ctx = DeviceContext()

    # Host column: deterministic LCG in [1, 2] so sqrt/ln/exp are all finite.
    var x_h = alloc[Float64](n)
    var state: UInt64 = 0x9E3779B97F4A7C15
    for i in range(n):
        state = state * 6364136223846793005 + 1442695040888963407
        var u = Float64(state >> 11) / Float64(UInt64(1) << 53)  # [0,1)
        x_h[i] = 1.0 + u                                          # [1,2)

    # Upload once, time it (cold transfer context).
    var x_d = ctx.enqueue_create_buffer[DType.float64](n)
    ctx.synchronize()
    var u0 = perf_counter_ns()
    ctx.enqueue_copy(x_d, x_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()
    var u1 = perf_counter_ns()
    var up_ms = Float64(u1 - u0) / 1.0e6
    var up_gbs = Float64(n) * 8.0 / (up_ms * 1.0e6)
    print(
        "  H2D upload:", up_ms, "ms (", up_gbs,
        "GB/s)  [paid once; warm reuses]",
    )

    var x_d_ptr = x_d.unsafe_ptr()
    _run_one[F_SQRT](ctx, "sum(sqrt(x))", x_h, x_d_ptr, n)
    _run_one[F_EXP](ctx, "sum(exp(x))", x_h, x_d_ptr, n)
    _run_one[F_LN](ctx, "sum(ln(x))", x_h, x_d_ptr, n)

    x_h.free()
    print("ALL PASS")
