"""Unit test for the additive float64 expr-VM (eval_program_f64).

Validates: (1) the int64 eval_program is unchanged (a known program), and (2)
eval_program_f64 reconstructs true doubles from scaled int64 storage and applies
arithmetic + transcendental ops correctly, vs a host reference. Runs on CPU (the
VM functions are @always_inline def, host-callable) -- a cheap compile+logic
gate that does NOT require a GPU or building the full operator.

Run:
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/expr_vm_f64_test.mojo
"""

from std.memory import alloc
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
    var cols = alloc[Int64](2 * N)
    for i in range(N):
        cols[0 * N + i] = Int64(ext[i])
        cols[1 * N + i] = Int64(disc[i])

    # col_div: 10^scale per slot (scale 2 -> 100). const_div parallel to ops.
    var col_div = alloc[Float64](2)
    col_div[0] = 100.0
    col_div[1] = 100.0

    var dims = alloc[Int64](1)
    var dim_off = alloc[Int64](1)
    dim_off[0] = 0

    # --- Test A: int64 VM unchanged -- program = LOAD_COL(0) (raw scaled value).
    var progA = alloc[Int64](3 * 1)
    progA[0] = OP_LOAD_COL; progA[1] = 0; progA[2] = 0
    for i in range(N):
        var got = eval_program(progA, 1, cols, N, i, dims, dim_off)
        if got != Int64(ext[i]):
            raise Error("int64 VM regressed at row " + String(i))
    print("PASS: int64 eval_program unchanged (raw scaled load)")

    # --- Test B: sum(sqrt(l_extendedprice)) -- program = LOAD_COL(0); SQRT.
    var progB = alloc[Int64](3 * 2)
    progB[0] = OP_LOAD_COL; progB[1] = 0; progB[2] = 0
    progB[3] = OP_SQRT; progB[4] = 0; progB[5] = 0
    var const_div = alloc[Float64](2)
    const_div[0] = 1.0; const_div[1] = 1.0
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

    # --- Test C: sum(ln(ext*(1-disc))) -- revenue-style arg, then LN.
    #   program: LOAD_COL(0); PUSH_CONST(100); LOAD_COL(1); SUB; MUL; LN
    #   where PUSH_CONST(100) is the scaled-2 representation of 1.0 (1.0*100),
    #   (1 - disc): 100/100 - disc/100; then * ext; then ln. Matches the revenue
    #   arg shape (ext*(1-disc)) the operator already lowers for the int path.
    var progC = alloc[Int64](3 * 6)
    progC[0] = OP_LOAD_COL; progC[1] = 0; progC[2] = 0      # ext (div 100)
    progC[3] = OP_PUSH_CONST; progC[4] = 100; progC[5] = 0  # 1.0 (scaled 2)
    progC[6] = OP_LOAD_COL; progC[7] = 1; progC[8] = 0      # disc (div 100)
    progC[9] = OP_SUB; progC[10] = 0; progC[11] = 0         # 1 - disc
    progC[12] = OP_MUL; progC[13] = 0; progC[14] = 0        # ext*(1-disc)
    progC[15] = OP_LN; progC[16] = 0; progC[17] = 0         # ln(...)
    var cdivC = alloc[Float64](6)
    for k in range(6):
        cdivC[k] = 1.0
    cdivC[1] = 100.0  # the PUSH_CONST at op index 1 is scale-2 -> div 100
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
