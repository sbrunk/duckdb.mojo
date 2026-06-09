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

from raw_plan_tags import (
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_LOAD_DIM,
    OP_EQ,
)

comptime EXPR_STACK_MAX = 16


@always_inline
def eval_program(
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
            stack[sp] = cols[Int(a) * n_rows + row]
            sp += 1
        elif op == OP_LOAD_DIM:
            # FK gather: key = cols[b][row]; push dims[dim_offsets[a] + key].
            var key = Int(cols[Int(b) * n_rows + row])
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
