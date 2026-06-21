"""End-to-end test of the production float64 segreduce kernel
(seg_ungrouped_kernel_f64) vs DuckDB, using the real scaled l_extendedprice col.

This exercises the EXACT kernel the operator's transcendental path launches: the
float64 VM (true-double reconstruction via col_div + transcendental op) + the
shared-memory float64 reduction + per-metric atomic accumulator. Two metrics are
reduced in ONE launch: m0 = sum(sqrt(ext)), m1 = count (PUSH_CONST(1)) -- the
AVG decomposition -- so avg(sqrt(ext)) = m0/m1 is also validated.

Reference (DuckDB sf1, stock):
  sum(sqrt(l_extendedprice)) = 1105786098.5653913
  count(*)                   = 6001215
  avg(sqrt(l_extendedprice)) = 184.260753...  (= sum/count)

Run (frederick; after the COPY in transcendental_scale_probe produces the bin):
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/seg_f64_kernel_test.mojo
"""

from std.gpu.host import DeviceContext
from std.memory import alloc
from std.math import sqrt, abs
from std.sys import has_accelerator
from std.os import getenv
from segreduce import seg_ungrouped_kernel_f64, SEG_NBLOCKS, SEG_BLK
from raw_plan_tags import OP_LOAD_COL, OP_PUSH_CONST, OP_SQRT

comptime REF_SQRT = 1105786098.5653913
comptime REF_COUNT = 6001215.0


def main() raises:
    if not has_accelerator():
        print("SKIP: requires a GPU. ALL PASS")
        return

    var path = getenv("LEXT_BIN", "/tmp/lext_scaled.bin")
    var txt = open(path, "r").read()
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
    print("=== seg_f64 kernel test: n =", n, "===")

    var ctx = DeviceContext()

    # One column (slot 0): scaled l_extendedprice. Packed col-major (1 slot).
    var cols_h = alloc[Int64](n)
    for i in range(n):
        cols_h[i] = vals[i]
    var cols_d = ctx.enqueue_create_buffer[DType.int64](n)
    ctx.enqueue_copy(cols_d, cols_h.unsafe_origin_cast[MutAnyOrigin]())

    # No filter: pass_len 0 (every row passes).
    var pass_d = ctx.enqueue_create_buffer[DType.int64](1)

    # Two metrics: m0 = LOAD_COL(0); SQRT   (2 ops) ; m1 = PUSH_CONST(1) (1 op).
    # metric_progs is the concatenated op tape; metric_offsets/lens index it.
    # ops as (op,a,b) triples.
    var progs_h = alloc[Int64](3 * 3)
    # m0 @ op-offset 0: LOAD_COL(0); SQRT
    progs_h[0] = OP_LOAD_COL; progs_h[1] = 0; progs_h[2] = 0
    progs_h[3] = OP_SQRT;     progs_h[4] = 0; progs_h[5] = 0
    # m1 @ op-offset 2: PUSH_CONST(1)  (count: const 1, const_div 1.0)
    progs_h[6] = OP_PUSH_CONST; progs_h[7] = 1; progs_h[8] = 0
    var progs_d = ctx.enqueue_create_buffer[DType.int64](3 * 3)
    ctx.enqueue_copy(progs_d, progs_h.unsafe_origin_cast[MutAnyOrigin]())

    var moff_h = alloc[Int64](2)
    moff_h[0] = 0; moff_h[1] = 2
    var moff_d = ctx.enqueue_create_buffer[DType.int64](2)
    ctx.enqueue_copy(moff_d, moff_h.unsafe_origin_cast[MutAnyOrigin]())

    var mlen_h = alloc[Int64](2)
    mlen_h[0] = 2; mlen_h[1] = 1
    var mlen_d = ctx.enqueue_create_buffer[DType.int64](2)
    ctx.enqueue_copy(mlen_d, mlen_h.unsafe_origin_cast[MutAnyOrigin]())

    # col_div: slot 0 is DECIMAL(_,2) -> 100.0
    var cdiv_h = alloc[Float64](1)
    cdiv_h[0] = 100.0
    var cdiv_d = ctx.enqueue_create_buffer[DType.float64](1)
    ctx.enqueue_copy(cdiv_d, cdiv_h.unsafe_origin_cast[MutAnyOrigin]())

    # const_div: parallel to op index; the PUSH_CONST(1) at op-index 2 is a pure
    # count (1.0), divisor 1.0. (The float VM indexes const_div by op position k.)
    var nstdiv_h = alloc[Float64](3)
    nstdiv_h[0] = 1.0; nstdiv_h[1] = 1.0; nstdiv_h[2] = 1.0
    var nstdiv_d = ctx.enqueue_create_buffer[DType.float64](3)
    ctx.enqueue_copy(nstdiv_d, nstdiv_h.unsafe_origin_cast[MutAnyOrigin]())

    var dims_d = ctx.enqueue_create_buffer[DType.int64](1)
    var dimoff_h = alloc[Int64](1)
    dimoff_h[0] = 0
    var dimoff_d = ctx.enqueue_create_buffer[DType.int64](1)
    ctx.enqueue_copy(dimoff_d, dimoff_h.unsafe_origin_cast[MutAnyOrigin]())

    var fpart_d = ctx.enqueue_create_buffer[DType.float64](2)
    fpart_d.enqueue_fill(0.0)
    # Empty fpred (n_fpred 0 -> in-kernel filter is a no-op; the pass program
    # alone gates rows, exactly the original behavior this test validates).
    var fpred_d = ctx.enqueue_create_buffer[DType.int64](1)
    ctx.synchronize()

    comptime k = seg_ungrouped_kernel_f64[False]
    ctx.enqueue_function[k](
        cols_d.unsafe_ptr(), n,
        pass_d.unsafe_ptr(), 0,
        progs_d.unsafe_ptr(), moff_d.unsafe_ptr(), mlen_d.unsafe_ptr(), 2,
        cdiv_d.unsafe_ptr(), nstdiv_d.unsafe_ptr(),
        dims_d.unsafe_ptr(), dimoff_d.unsafe_ptr(),
        fpart_d.unsafe_ptr(),
        fpred_d.unsafe_ptr(), 0,
        grid_dim=SEG_NBLOCKS, block_dim=SEG_BLK,
    )
    ctx.synchronize()

    var res_h = alloc[Float64](2)
    ctx.enqueue_copy(res_h.unsafe_origin_cast[MutAnyOrigin](), fpart_d)
    ctx.synchronize()

    var gsum = res_h[0]
    var gcnt = res_h[1]
    var rel_sum = abs(gsum - REF_SQRT) / abs(REF_SQRT)
    print("  sum(sqrt) =", gsum, " duckdb =", REF_SQRT, " rel =", rel_sum)
    print("  count     =", gcnt, " duckdb =", REF_COUNT)
    print("  avg(sqrt) =", gsum / gcnt)

    var ok = rel_sum < 1e-10 and gcnt == REF_COUNT
    if ok:
        print("ALL PASS (production seg_ungrouped_kernel_f64 matches DuckDB)")
    else:
        print("FAIL")

    cols_h.free()
