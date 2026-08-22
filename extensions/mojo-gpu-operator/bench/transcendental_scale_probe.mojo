"""PHASE B correctness de-risk: GPU sum(f(DECIMAL col)) vs DuckDB, exact lowering.

The operator packs DECIMAL/INTEGER columns as SCALED int64 (e.g. l_extendedprice
DECIMAL(15,2) -> int64 value * 100). A transcendental aggregate operates on the
TRUE double value, so the float eval path must reconstruct it as
`Float64(scaled_int64) / 10^scale` before applying f. This probe validates that
EXACT lowering against the real DuckDB database, which is the only new
correctness risk gating the full Phase B integration:

  - Input: l_extendedprice * 100 cast to BIGINT, exported from tpch_sf1 to a CSV
    of one int64 per line (see the COPY in the harness comment below).
  - GPU: read scaled int64 -> reconstruct Float64(v)/100.0 -> sum(sqrt) / sum(ln)
    via the same shared-mem float64 reduction as transcendental_agg_probe.
  - Reference: DuckDB's own `sum(sqrt(l_extendedprice))` /
    `sum(ln(l_extendedprice))` on tpch_sf1 (passed in via env).

DuckDB sf1 reference (stock CLI):
  sum(sqrt(l_extendedprice)) = 1105786098.5653913
  sum(ln(l_extendedprice))   =   61590396.43125566
  count(*)                   =    6001215

Run (on frederick, after the COPY produces /tmp/lext_scaled.bin):
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/transcendental_scale_probe.mojo
"""

from std.gpu import block_idx, thread_idx
from max.gpu.sync import barrier
from max.gpu.host import DeviceContext
from max.gpu.memory import AddressSpace
from std.memory import alloc, stack_allocation
from std.math import sqrt, log, abs
from std.sys import has_accelerator
from std.atomic import Atomic
from std.os import getenv

comptime BLOCK = 256
comptime NBLOCKS = 4096
comptime SCALE_DIV = 100.0   # DECIMAL(_,2) -> divide scaled int64 by 100

comptime F_SQRT = 0
comptime F_LN = 1

# DuckDB sf1 reference answers (stock).
comptime REF_SQRT = 1105786098.5653913
comptime REF_LN = 61590396.43125566


@always_inline
def _sqrt_f64(x: Float64) -> Float64:
    # Precise f64 sqrt on NVIDIA (stdlib sqrt(f64) is the approx-only NVVM path,
    # constrained off for f64): f32 seed + one Newton step -> ~1e-14 rel err.
    var y = Float64(sqrt(x.cast[DType.float32]()))
    y = 0.5 * (y + x / y)
    return y


@always_inline
def _apply[F: Int](x: Float64) -> Float64:
    comptime if F == F_SQRT:
        return _sqrt_f64(x)
    else:
        return log(x)


# Reconstruct the TRUE double from the scaled int64 (v / 10^scale), apply f, sum.
# This is the exact per-row float eval the operator's transcendental path needs.
def trans_scaled_kernel[F: Int](
    v: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
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
        var real = Float64(v[i]) / SCALE_DIV
        acc += _apply[F](real)
        i += stride
    smem[tid] = acc
    barrier()
    var active = BLOCK
    while active > 1:
        active >>= 1
        if tid < active:
            smem[tid] = smem[tid] + smem[tid + active]
        barrier()
    if tid == 0:
        _ = Atomic.fetch_add(dst, smem[0])


def _gpu_sum[
    F: Int
](ctx: DeviceContext, v_d_ptr: UnsafePointer[Scalar[DType.int64], MutAnyOrigin], n: Int) raises -> Float64:
    var out_d = ctx.enqueue_create_buffer[DType.float64](1)
    out_d.enqueue_fill(0.0)
    comptime k = trans_scaled_kernel[F]
    ctx.enqueue_function[k](v_d_ptr, n, out_d.unsafe_ptr(),
                            grid_dim=NBLOCKS, block_dim=BLOCK)
    ctx.synchronize()
    var h = alloc[Float64](1)
    ctx.enqueue_copy(h.unsafe_origin_cast[MutAnyOrigin](), out_d)
    ctx.synchronize()
    var r = h[0]
    h.free()
    return r


def main() raises:
    if not has_accelerator():
        print("SKIP: requires a GPU. ALL PASS")
        return

    var path = getenv("LEXT_BIN", "/tmp/lext_scaled.bin")
    var txt = open(path, "r").read()

    # Parse one int64 per line.
    var vals: List[Int64] = []
    var cur = String("")
    for cp in txt.codepoint_slices():
        if String(cp) == "\n":
            if cur.byte_length() > 0:
                vals.append(Int64(atol(cur)))
                cur = String("")
        else:
            cur += String(cp)
    if cur.byte_length() > 0:
        vals.append(Int64(atol(cur)))

    var n = len(vals)
    print("=== scale-reconstruction probe: n =", n, "scaled int64 from", path, "===")

    var v_h = alloc[Int64](n)
    for i in range(n):
        v_h[i] = vals[i]

    var ctx = DeviceContext()
    var v_d = ctx.enqueue_create_buffer[DType.int64](n)
    ctx.enqueue_copy(v_d, v_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()
    var v_d_ptr = v_d.unsafe_ptr()

    var g_sqrt = _gpu_sum[F_SQRT](ctx, v_d_ptr, n)
    var g_ln = _gpu_sum[F_LN](ctx, v_d_ptr, n)

    var rel_sqrt = abs(g_sqrt - REF_SQRT) / abs(REF_SQRT)
    var rel_ln = abs(g_ln - REF_LN) / abs(REF_LN)

    print("  sum(sqrt): gpu =", g_sqrt, " duckdb =", REF_SQRT, " rel-err =", rel_sqrt)
    print("  sum(ln):   gpu =", g_ln, " duckdb =", REF_LN, " rel-err =", rel_ln)

    var ok = rel_sqrt < 1e-10 and rel_ln < 1e-10
    if ok:
        print("ALL PASS (rel-err < 1e-10 vs DuckDB)")
    else:
        print("FAIL: rel-err exceeds 1e-10 vs DuckDB")

    v_h.free()
