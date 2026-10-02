"""Unit test for the additive float64 expr-VM (eval_program_f64).

Checks that (1) the int64 eval_program is unchanged (a known program), and (2)
eval_program_f64 reconstructs the real doubles from scaled int64 storage and
applies arithmetic + transcendental ops correctly, compared with a host
reference. Runs on the CPU (the VM functions are @always_inline def and can be
called from the host), so it is a cheap compile and logic check that does not
need a GPU or a build of the full operator.

Run:
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/expr_vm_f64_test.mojo
"""

from std.memory.alloc import unsafe_alloc
from std.math import sqrt, log, abs
from expr_vm import eval_program, eval_program_f64
from raw_plan_tags import (
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_MUL,
    OP_SUB,
    OP_SQRT,
    OP_LN,
)


def main() raises:
    comptime N = 4
    # Two columns: slot0 = l_extendedprice scaled *100 (DECIMAL(_,2)); slot1 =
    # l_discount scaled *100. Packed col-major: cols[slot*N + row].
    var ext = [2116823, 4598316, 1330960, 999900]  # 21168.23, 45983.16, ...
    var disc = [6, 9, 8, 10]                        # 0.06, 0.09, 0.08, 0.10
    var cols = unsafe_alloc[Int64](2 * N)
    for i in range(N):
        cols[unsafe_offset=0 * N + i] = Int64(ext[i])
        cols[unsafe_offset=1 * N + i] = Int64(disc[i])

    # col_div: 10^scale per slot (scale 2 gives 100). const_div parallel to ops.
    var col_div = unsafe_alloc[Float64](2)
    col_div[unsafe_offset=0] = 100.0
    col_div[unsafe_offset=1] = 100.0

    var dims = unsafe_alloc[Int64](1)
    var dim_off = unsafe_alloc[Int64](1)
    dim_off[unsafe_offset=0] = 0

    # --- Test A: int64 VM unchanged. program = LOAD_COL(0) (raw scaled value).
    var progA = unsafe_alloc[Int64](3 * 1)
    progA[unsafe_offset=0] = OP_LOAD_COL; progA[unsafe_offset=1] = 0; progA[unsafe_offset=2] = 0
    for i in range(N):
        var got = eval_program(progA, 1, cols, N, i, dims, dim_off)
        if got != Int64(ext[i]):
            raise Error("int64 VM regressed at row " + String(i))
    print("PASS: int64 eval_program unchanged (raw scaled load)")

    # --- Test B: sum(sqrt(l_extendedprice)). program = LOAD_COL(0); SQRT.
    var progB = unsafe_alloc[Int64](3 * 2)
    progB[unsafe_offset=0] = OP_LOAD_COL; progB[unsafe_offset=1] = 0; progB[unsafe_offset=2] = 0
    progB[unsafe_offset=3] = OP_SQRT; progB[unsafe_offset=4] = 0; progB[unsafe_offset=5] = 0
    var const_div = unsafe_alloc[Float64](2)
    const_div[unsafe_offset=0] = 1.0; const_div[unsafe_offset=1] = 1.0
    var ssum = Float64(0.0)
    var ref_ssum = Float64(0.0)
    for i in range(N):
        var v = eval_program_f64(
            progB, 2, cols, N, i, col_div, const_div, dims, dim_off
        )
        ssum += v
        ref_ssum += sqrt(Float64(ext[i]) / 100.0)
    var relB = abs(ssum - ref_ssum) / abs(ref_ssum)
    print("  sum(sqrt) vm =", ssum, " ref =", ref_ssum, " rel =", relB)
    if relB > 1e-9:
        raise Error("float VM sqrt mismatch")
    print("PASS: eval_program_f64 sqrt + scale reconstruction")

    # --- Test C: sum(ln(ext*(1-disc))), a revenue-style arg, then LN.
    #   program: LOAD_COL(0); PUSH_CONST(100); LOAD_COL(1); SUB; MUL; LN
    #   where PUSH_CONST(100) is the scaled-2 representation of 1.0 (1.0*100),
    #   (1 - disc): 100/100 - disc/100; then * ext; then ln. Matches the revenue
    #   arg shape (ext*(1-disc)) the operator already lowers for the int path.
    var progC = unsafe_alloc[Int64](3 * 6)
    progC[unsafe_offset=0] = OP_LOAD_COL; progC[unsafe_offset=1] = 0; progC[unsafe_offset=2] = 0      # ext (div 100)
    progC[unsafe_offset=3] = OP_PUSH_CONST; progC[unsafe_offset=4] = 100; progC[unsafe_offset=5] = 0  # 1.0 (scaled 2)
    progC[unsafe_offset=6] = OP_LOAD_COL; progC[unsafe_offset=7] = 1; progC[unsafe_offset=8] = 0      # disc (div 100)
    progC[unsafe_offset=9] = OP_SUB; progC[unsafe_offset=10] = 0; progC[unsafe_offset=11] = 0         # 1 - disc
    progC[unsafe_offset=12] = OP_MUL; progC[unsafe_offset=13] = 0; progC[unsafe_offset=14] = 0        # ext*(1-disc)
    progC[unsafe_offset=15] = OP_LN; progC[unsafe_offset=16] = 0; progC[unsafe_offset=17] = 0         # ln(...)
    var cdivC = unsafe_alloc[Float64](6)
    for k in range(6):
        cdivC[unsafe_offset=k] = 1.0
    cdivC[unsafe_offset=1] = 100.0  # the PUSH_CONST at op index 1 is scale 2, so div 100
    var lsum = Float64(0.0)
    var ref_lsum = Float64(0.0)
    for i in range(N):
        var v = eval_program_f64(
            progC, 6, cols, N, i, col_div, cdivC, dims, dim_off
        )
        lsum += v
        var e = Float64(ext[i]) / 100.0
        var d = Float64(disc[i]) / 100.0
        ref_lsum += log(e * (1.0 - d))
    var relC = abs(lsum - ref_lsum) / abs(ref_lsum)
    print("  sum(ln(ext*(1-disc))) vm =", lsum, " ref =", ref_lsum, " rel =", relC)
    if relC > 1e-9:
        raise Error("float VM ln(revenue) mismatch")
    print("PASS: eval_program_f64 arithmetic + ln (revenue arg)")

    print("ALL PASS")
