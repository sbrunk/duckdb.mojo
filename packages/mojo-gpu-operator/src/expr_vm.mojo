"""Generic integer postfix-expression VM (device-callable).

This is foundational GPU primitive #1 for the mojo-gpu-operator extension. It
replaces the bespoke per-row arithmetic baked into q6_kernel / q1_kernel /
q3_seg_kernel (e.g. `ext * disc`, `ext*(100-disc)*(100+tax)`) with a tiny
stack-machine that evaluates a *uploaded* postfix program for ONE row and
returns an `Int64` (the per-row metric value at the metric's decimal scale).

All arithmetic is pure integer, exactly like the existing kernels — the caller
guarantees per-row magnitudes fit int64 (true for TPC-H SF-scale data, which is
what the existing kernels rely on). The int128 widening happens later, on the
host, in the cross-block reduction (see segreduce.mojo).

PROGRAM ENCODING
----------------
A program is a flat device array of `Int64`, three slots per op:

    prog[3*k + 0] = op       (one of the OP_* tags from raw_plan_tags.mojo)
    prog[3*k + 1] = a        (operand a, meaning depends on op)
    prog[3*k + 2] = b        (operand b, currently unused / reserved -> 0)

`prog_len` is the number of ops (so the array holds `3 * prog_len` int64s).

OPS (tag values imported from raw_plan_tags.mojo)
-------------------------------------------------
    OP_LOAD_COL    a = column slot index  -> push cols[a][row]
    OP_PUSH_CONST  a = the scaled int64 constant value, pushed directly.
                   NOTE: at THIS layer constants are already resolved to int64
                   values (the const-pool id resolution happens in the caller /
                   lowering step), so `a` is the literal value, not a pool id.
    OP_ADD         pop b, a -> push a + b
    OP_SUB         pop b, a -> push a - b
    OP_MUL         pop b, a -> push a * b
    OP_SELECT      pop else, then, pred -> push (pred != 0 ? then : else)
    OP_LOAD_DIM    a = dim-array index, b = fact-key column slot ->
                   push dims[ dim_offsets[a] + cols[b][row] ]. This is an
                   on-GPU dense-array gather: the fact's FK value (an int64 in
                   column slot b) is used as a dense index into dim array `a`.
                   It lets the segreduce kernels resolve TPC-H FK-join dimension
                   lookups (Q14/Q3/Q5) without a separate join kernel, exactly
                   how the bespoke kernels index order_pass[orderkey] /
                   is_building[custkey]. The caller guarantees the key is a
                   valid dense index (dim array sized to max key + 1).

    OP_EQ          pop b, a -> push (a == b) ? 1 : 0. Used by Q5 to test the
                   correlated equality customer-nation == supplier-nation across
                   two per-row dim gathers (both precomputed to single-level
                   gathers indexed by a fact-column slot), ANDed (via OP_MUL)
                   into the row pass program.

`OP_PROMO_PRED` is intentionally NOT handled here: the promo/LIKE CASE is
lowered by the caller into a precomputed 0/1 column + OP_LOAD_COL + OP_SELECT,
so the VM needs no string/LIKE op.

COLUMN-BUFFER LAYOUT
--------------------
All input columns are packed into ONE int64 device buffer addressed by a single
pointer, column-major by slot:

    cols[slot * n_rows + row]

i.e. slot 0 occupies cols[0 .. n_rows-1], slot 1 occupies cols[n_rows .. 2*n_rows-1],
etc. So `OP_LOAD_COL a` for the current `row` reads `cols[a * n_rows + row]`.

Non-int64 source columns (DATE int32, dense group-id uint8, precomputed 0/1
predicate/promo flags) are widened to int64 by the caller when it packs the
buffer. This keeps the VM's column access a single typed pointer + integer
arithmetic, exactly matching the integer contract of the existing kernels.

DIM-ARRAY (GATHER) BUFFER LAYOUT
--------------------------------
The N FK-join dimension arrays are packed into ONE int64 device buffer `dims`,
concatenated back-to-back, addressed via a `dim_offsets` array of length
`n_dims + 1`:

    dim array `a` occupies dims[ dim_offsets[a] .. dim_offsets[a+1] - 1 ]

so `OP_LOAD_DIM a b` for the current `row` reads
`dims[ dim_offsets[a] + cols[b * n_rows + row] ]`. `dim_offsets[0]` is 0 and
`dim_offsets[n_dims]` is the total element count. When `n_dims == 0` there are
no dim arrays and no program emits OP_LOAD_DIM (the no-dim path is byte-for-byte
the prior behavior).

STACK
-----
A fixed-size int64 register stack (EXPR_STACK_MAX = 16 slots). Programs produced
by the planner for the supported TPC-H shapes never exceed this depth.
"""

from std.gpu.memory import AddressSpace
from std.math import sqrt, exp, log, sin, cos, nan
from raw_plan_tags import (
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_LOAD_DIM,
    OP_EQ,
    OP_SQRT,
    OP_EXP,
    OP_LN,
    OP_LOG10,
    OP_SIN,
    OP_COS,
    OP_POW,
    OP_LOG2,
)

comptime EXPR_STACK_MAX = 16


# ---------------------------------------------------------------------------
# _col_at: read column `slot` at `row`, from EITHER the packed buffer (Phase 1/2
# default) or a per-column POINTER TABLE (Phase 3 / Option B, GPU_OP_COLPTR).
#
# The `cols` argument is REUSED to carry both representations (so no kernel/VM
# gains an extra runtime argument -- only a comptime flag):
#   USE_COLPTR == False: `cols` is the PACKED buffer -> cols[slot * n_rows + row]
#                        (today's layout, byte-identical; the ptr branch elides).
#   USE_COLPTR == True:  `cols` is the per-column POINTER TABLE -> cols[slot] holds
#                        the DEVICE ADDRESS of slot's column buffer; reconstruct a
#                        GLOBAL-address-space pointer from it and read [row]. This
#                        lets kernels read POOLED per-column buffers DIRECTLY (no
#                        packed copy) -> eliminates the Phase 1/2 cols_d duplicate
#                        (~44% resident-VRAM cut).
#
# AddressSpace.GLOBAL is REQUIRED: a pointer reconstructed from a raw integer with
# the default GENERIC address space reads 0 on Apple Metal (separate address
# spaces). GLOBAL works on both Apple M3 + NVIDIA (verified by bench/colptr_probe).
# The value read is identical to the packed layout, so results are bit-exact.
# ---------------------------------------------------------------------------
@always_inline
def _col_at[
    USE_COLPTR: Bool
](
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    slot: Int,
    row: Int,
) -> Int64:
    @parameter
    if USE_COLPTR:
        var addr = Int(cols[slot])
        var p = UnsafePointer[
            Scalar[DType.int64],
            MutAnyOrigin,
            address_space = AddressSpace.GLOBAL,
        ](unsafe_from_address=addr)
        return p[row]
    else:
        return cols[slot * n_rows + row]


@always_inline
def eval_program[
    USE_COLPTR: Bool = False
](
    prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    prog_len: Int,
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
) -> Int64:
    """Evaluate one postfix program for a single row, returning its int64 value.

    Args:
        prog: flattened (op, a, b) int64 triples; `prog_len` ops total.
        prog_len: number of ops in `prog`.
        cols: packed int64 columns, laid out cols[slot * n_rows + row].
        n_rows: row count (column stride within `cols`).
        row: the row to evaluate.
        dims: Packed int64 FK-join dim arrays, concatenated back-to-back;
              dim array `a` is dims[dim_offsets[a] .. dim_offsets[a+1]-1].
        dim_offsets: Per-dim start offsets (length n_dims+1) into `dims`.
                     Unused when no program op is OP_LOAD_DIM; may be a 1-elem
                     placeholder buffer in that case.

    Returns:
        The per-row int64 metric value (at the metric's decimal scale).
    """
    var stack = InlineArray[Int64, EXPR_STACK_MAX](fill=0)
    var sp = 0  # next free slot (stack depth)
    var k = 0
    while k < prog_len:
        var op = prog[3 * k + 0]
        var a = prog[3 * k + 1]
        var b = prog[3 * k + 2]
        if op == OP_LOAD_COL:
            stack[sp] = _col_at[USE_COLPTR](cols, n_rows, Int(a), row)
            sp += 1
        elif op == OP_LOAD_DIM:
            # FK gather: key = cols[b][row]; push dims[dim_offsets[a] + key].
            var key = Int(_col_at[USE_COLPTR](cols, n_rows, Int(b), row))
            stack[sp] = dims[Int(dim_offsets[Int(a)]) + key]
            sp += 1
        elif op == OP_PUSH_CONST:
            stack[sp] = a
            sp += 1
        elif op == OP_ADD:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs + rhs
        elif op == OP_SUB:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs - rhs
        elif op == OP_MUL:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs * rhs
        elif op == OP_SELECT:
            var else_v = stack[sp - 1]
            var then_v = stack[sp - 2]
            var pred = stack[sp - 3]
            sp -= 2
            stack[sp - 1] = then_v if pred != 0 else else_v
        elif op == OP_EQ:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = Int64(1) if lhs == rhs else Int64(0)
        # unknown op: ignore (defensive; planner only emits the ops above)
        k += 1
    return stack[0] if sp > 0 else Int64(0)


# ---------------------------------------------------------------------------
# FLOAT64 expression VM (transcendental aggregate path; GPU_OP_TRANSCENDENTAL).
#
# Purely ADDITIVE: the int64 eval_program above is untouched and byte-identical.
# This VM is only invoked by the float64 segreduce accumulator for a
# DOUBLE-returning transcendental aggregate (sum/avg of f(col)). It evaluates the
# SAME postfix encoding but on a float64 stack, and -- crucially -- reconstructs
# the TRUE double of each operand from the SCALED int64 column / const:
#
#   OP_LOAD_COL a  -> push Float64(cols[a][row]) / col_div[a]
#   OP_PUSH_CONST a-> push Float64(a) / const_div_of_this_op   (a is scaled int64)
#   OP_ADD/SUB/MUL -> float arithmetic
#   OP_SQRT/EXP/LN/LOG10/SIN/COS -> unary: pop x, push f(x)
#
# `col_div[slot]` = 10^scale (as Float64) for that column; 1.0 for DATE/INTEGER.
# `const_div` is a parallel array indexed by op position k (only meaningful for
# PUSH_CONST ops; 1.0 otherwise) carrying the const's 10^scale divisor.
#
# Precise f64 sqrt: stdlib sqrt(Float64) routes to the NVVM approx path which is
# constrained off for f64 on NVIDIA. We seed from the f32 approx and do ONE
# Newton step (~1e-14 rel err, validated vs DuckDB). x>=0 by construction for the
# DECIMAL/price columns these aggregates apply to.
#
# DOMAIN (audit Group G): stock DuckDB RAISES "cannot take square root of a
# negative number" for x<0. The f32 seed of a negative is NaN and the `y > 0.0`
# Newton guard is then false, so the result is already NaN -- but make the domain
# violation EXPLICIT/deterministic: x<0 -> NaN sentinel. The host f64 finalize
# detects this NaN and raises (matching stock), so a single out-of-domain row no
# longer poisons the aggregate into a silent nan. (sqrt(0)==0 stays exact.)
# ---------------------------------------------------------------------------
@always_inline
def _vm_sqrt_f64(x: Float64) -> Float64:
    if x < 0.0:
        return nan[DType.float64]()
    var y = Float64(sqrt(x.cast[DType.float32]()))
    if y > 0.0:
        y = 0.5 * (y + x / y)
    return y


# Natural log with DOMAIN normalization (audit Group G): stock DuckDB RAISES
# "cannot take logarithm of zero" / "of a negative number" for x<=0. The raw
# in-kernel f64 log gives -inf for x==0 and NaN for x<0 -- NORMALIZE BOTH to a NaN
# sentinel so the domain violation is an unambiguous signal distinct from a valid
# Inf (exp overflow). The host f64 finalize detects this NaN and raises (matching
# stock). Used by OP_LN/OP_LOG10/OP_LOG2 (log10/log2 derive from this natural log).
@always_inline
def _vm_ln_f64(x: Float64) -> Float64:
    if x <= 0.0:
        return nan[DType.float64]()
    return log(x)


# 1 / ln(10): log10(x) = log(x) * this. NVIDIA has no f64 log10 (libm, CPU-only),
# but f64 `log` (natural) works -> derive log10 exactly from it.
comptime _INV_LN10: Float64 = 0.43429448190325182765112891891660508229439700580367
# 1 / ln(2): log2(x) = log(x) * this. Same rationale as _INV_LN10 (no f64 log2 on
# NVIDIA; derive from the working f64 natural log).
comptime _INV_LN2: Float64 = 1.4426950408889634073599246810018921374266459541530


# power(base, exp) for INTEGER-valued exponents via exact binary exponentiation —
# pure multiplies (and one reciprocal for negative exponents), so it stays bit-faithful
# for ANY base sign and uses ONLY in-kernel-proven f64 ops (no f64 `pow` intrinsic,
# which is libm/CPU-only on NVIDIA — same class as the missing f64 sin/cos). The
# C++ EmitProgram only emits OP_POW when the exponent is an integer-valued constant,
# so `exp` here is always integral; the fallback path is defensive only.
@always_inline
def _vm_pow_f64(base: Float64, exp_v: Float64) -> Float64:
    var ei = Int(exp_v)
    if Float64(ei) == exp_v and ei >= -64 and ei <= 64:
        var n = ei if ei >= 0 else -ei
        var r: Float64 = 1.0
        var b = base
        while n > 0:
            if (n & 1) == 1:
                r = r * b
            b = b * b
            n = n >> 1
        return r if ei >= 0 else (1.0 / r)
    # Non-integer exponent (not emitted by the planner): base>0 domain via exp/ln.
    return exp(exp_v * log(base))


# NVIDIA has no precise f64 sin/cos (only f32 approx PTX; the f64 path is libm /
# CPU-only). sin/cos are the lowest-value transcendental aggregates (~1.17x even
# on CPU-SIMD per R7), so we compute them via the f32 approx here: ~1e-7 relative
# accuracy, which is acceptable for a flag-gated DOUBLE aggregate where DuckDB's
# own double sum already varies at ~1e-12 from thread order. (sqrt/exp/ln/log10
# are f64-precise; only sin/cos take this f32 route.)
@always_inline
def _vm_sin_f64(x: Float64) -> Float64:
    return Float64(sin(x.cast[DType.float32]()))


@always_inline
def _vm_cos_f64(x: Float64) -> Float64:
    return Float64(cos(x.cast[DType.float32]()))


@always_inline
def eval_program_f64[
    USE_COLPTR: Bool = False
](
    prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    prog_len: Int,
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    col_div: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
    const_div: UnsafePointer[Scalar[DType.float64], MutAnyOrigin],
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
) -> Float64:
    """Evaluate one postfix program for a single row on a float64 stack.

    Reconstructs each operand's true double from the scaled int64 storage via
    `col_div` / `const_div`, then applies arithmetic + transcendental ops.
    Returns the per-row float64 metric value. See module note above.
    """
    var stack = InlineArray[Float64, EXPR_STACK_MAX](fill=0.0)
    var sp = 0
    var k = 0
    while k < prog_len:
        var op = prog[3 * k + 0]
        var a = prog[3 * k + 1]
        if op == OP_LOAD_COL:
            var raw = _col_at[USE_COLPTR](cols, n_rows, Int(a), row)
            stack[sp] = Float64(raw) / col_div[Int(a)]
            sp += 1
        elif op == OP_LOAD_DIM:
            var b = prog[3 * k + 2]
            var key = Int(_col_at[USE_COLPTR](cols, n_rows, Int(b), row))
            stack[sp] = Float64(dims[Int(dim_offsets[Int(a)]) + key])
            sp += 1
        elif op == OP_PUSH_CONST:
            stack[sp] = Float64(a) / const_div[k]
            sp += 1
        elif op == OP_ADD:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs + rhs
        elif op == OP_SUB:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs - rhs
        elif op == OP_MUL:
            var rhs = stack[sp - 1]
            var lhs = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = lhs * rhs
        elif op == OP_SELECT:
            var else_v = stack[sp - 1]
            var then_v = stack[sp - 2]
            var pred = stack[sp - 3]
            sp -= 2
            stack[sp - 1] = then_v if pred != 0.0 else else_v
        elif op == OP_SQRT:
            stack[sp - 1] = _vm_sqrt_f64(stack[sp - 1])
        elif op == OP_EXP:
            stack[sp - 1] = exp(stack[sp - 1])
        elif op == OP_LN:
            stack[sp - 1] = _vm_ln_f64(stack[sp - 1])
        elif op == OP_LOG10:
            stack[sp - 1] = _vm_ln_f64(stack[sp - 1]) * _INV_LN10
        elif op == OP_SIN:
            stack[sp - 1] = _vm_sin_f64(stack[sp - 1])
        elif op == OP_COS:
            stack[sp - 1] = _vm_cos_f64(stack[sp - 1])
        elif op == OP_POW:
            var ex = stack[sp - 1]
            var base = stack[sp - 2]
            sp -= 1
            stack[sp - 1] = _vm_pow_f64(base, ex)
        elif op == OP_LOG2:
            stack[sp - 1] = _vm_ln_f64(stack[sp - 1]) * _INV_LN2
        # unknown op: ignore (defensive)
        k += 1
    return stack[0] if sp > 0 else Float64(0.0)
