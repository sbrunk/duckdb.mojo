"""Generic segmented N-metric int128 reduction (foundational GPU primitive #2).

Computes, for each of M metrics and each group/segment, the int128 sum over all
rows that pass a row filter of the per-row metric value produced by the
expression VM (see expr_vm.mojo). Three host-selected modes generalize the three
existing bespoke kernels while preserving their EXACT integer contract:

    UNGROUPED       (1 output group)   -> reproduces q6_kernel (M=1) and the
                                          single-group multi-metric q1 shape.
    DENSE_GROUP     (G small groups)   -> reproduces q1_kernel.
    SORT_SEGREDUCE  (sorted segments)  -> reproduces q3_seg_kernel.

THE EXACTNESS CONTRACT (unchanged from the existing kernels)
------------------------------------------------------------
Per-row metric values and per-block partials fit int64; warp.sum accumulates
int64; only the cross-block / cross-segment reduction widens to int128, done on
the HOST. So the device side is pure int64 integer arithmetic and the result is
bit-exact vs a CPU int128 reference.

ROW FILTER
----------
The row-pass predicate is itself an expression VM program (`pass_prog`) that
returns 0/1 for the row: a passing row has a non-zero result. This is strictly
more general than a precomputed pass column, and a precomputed pass column is
trivially representable as a 1-op `OP_LOAD_COL pass_slot` program. Range
predicates (q6's shipdate/discount/quantity windows) are lowered by the caller
to comparisons-as-arithmetic that the VM's ADD/SUB/MUL/SELECT can express, OR —
the simplest path the planner will use — to a single precomputed 0/1 pass
column. The promo/LIKE CASE is likewise lowered to a precomputed 0/1 column +
OP_SELECT inside the *metric* program (never here).

If `pass_len == 0`, all rows pass (no filter).

METRIC PROGRAMS
---------------
The M metric programs are concatenated into one int64 device buffer
(`metric_progs`), with `metric_offsets[m]` giving the op-offset (in ops, not
int64s) of metric m's first op and `metric_lens[m]` its op count. So metric m's
program starts at `metric_progs + 3 * metric_offsets[m]`.

OUTPUTS
-------
`run_segreduce` returns a `List[Int128]` of length (n_out_groups * M), laid out
row-major as result[g * M + m]. For UNGROUPED n_out_groups == 1; for DENSE_GROUP
n_out_groups == G; for SORT_SEGREDUCE n_out_groups == n_seg.
"""

from std.gpu import block_idx, thread_idx, global_idx, block_dim, grid_dim, barrier
from std.gpu.primitives import warp
from std.gpu.memory import AddressSpace
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc, stack_allocation
from std.atomic import Atomic
from std.os import abort, getenv
from std.sys.info import is_nvidia_gpu, is_amd_gpu
from gpu_platform import WARP
from raw_plan_tags import (
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
    STRAT_HASH_GROUP,
    KIND_UNKNOWN,
    KIND_Q6,
    KIND_Q1,
    KIND_Q14,
    KIND_Q3,
    KIND_Q5,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_LOAD_DIM,
    OP_EQ,
    CMP_EQ,
    CMP_NE,
    CMP_LT,
    CMP_LE,
    CMP_GT,
    CMP_GE,
)
from expr_vm import eval_program

comptime SEG_NBLOCKS = 4096  # one warp per block (matches the existing kernels)
# Multi-warp block variant (GPU_OP_BLOCK128): on sm_89 a 1-warp block caps
# occupancy at 50% (24-block/SM limit); a 4-warp block lifts the ceiling to ~100%
# (regs/smem leave headroom). NWARPS warps reduce per-warp via warp.sum, then
# combine across warps through shared memory + one barrier.
comptime SEG_NWARPS = 4
comptime SEG_BLK = SEG_NWARPS * WARP
comptime HASH_BLOCK = 256  # threads/block for the one-thread-per-row hash kernel
# Sentinel marking an empty hash slot. l_orderkey (and every TPC-H group key the
# host routes here) is >= 1, so INT64_MIN can never be a real key.
comptime HASH_EMPTY: Int64 = -0x8000_0000_0000_0000
comptime SEG_MAX_METRICS = 8  # per-lane accumulator cap for the grid kernels


@always_inline
def _row_passes(
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
) -> Bool:
    if pass_len == 0:
        return True
    return eval_program(
        pass_prog, pass_len, cols, n_rows, row, dims, dim_offsets
    ) != 0


# ===========================================================================
# COMPTIME-SPECIALIZED (kind-aware) path. Improvement #2: the per-row metric +
# filter postfix programs that `seg_*_kernel` evaluate with the runtime stack
# machine (`eval_program`) are, for a given query KIND, a FIXED-SHAPE expression.
# `eval_program_fast` is a stackless, comptime-shape-dispatched evaluator that
# computes the SAME int64 value as `eval_program` (bit-identical: same integer
# ops, same order, same operand reads) without the 16-slot register stack, the
# `sp` bookkeeping, or the long per-op op-tag elif chain. It stays data-driven
# for OPERANDS (column slots / consts / dim-array indices are read from the
# program tape), so it is correct for ANY dynamic slot assignment the host
# builders emit -- which is what guarantees bit-exactness across query shapes.
#
# It recognizes exactly the program shapes the supported kinds (Q1/Q5/Q6/Q14/Q3)
# emit; anything it does not recognize falls through to `eval_program` so the
# result is never wrong. The per-kind specialized kernels below are byte-for-byte
# copies of the matching generic grid kernel with the inner `eval_program` metric
# calls (and the 1-op pass-column filter, where applicable) swapped for this fast
# path: the lane striding, `warp.sum`, per-block partial layout, and host int128
# reduction are UNCHANGED, so the GPU output is identical to the interpreter.
# ===========================================================================
@always_inline
def eval_program_fast(
    prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    prog_len: Int,
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
) -> Int64:
    """Stackless evaluator for the fixed expression shapes the GPU kinds emit.

    Returns the SAME int64 value as `eval_program` for the recognized shapes:
      len 1: LOAD_COL a            -> cols[a][row]
             PUSH_CONST v           -> v
             LOAD_DIM a b           -> dims[dim_offsets[a] + cols[b][row]]
      len 3: <x> <y> MUL            -> x * y      (operands LOAD_COL/LOAD_DIM)
      len 5: LOAD_x e; PUSH k; LOAD_x d; SUB; MUL -> e * (k - d)
      len 9: ... ; PUSH k2; LOAD_x t; ADD; MUL    -> e*(k-d) * (k2 + t)
      len 8: LOAD_DIM p; LOAD e; PUSH k; LOAD d; SUB; MUL; PUSH z; SELECT
                                    -> (p != 0) ? e*(k-d) : z   (Q14 promo)
    Any other shape defers to `eval_program` (universal fallback).
    """

    # Read one LOAD_COL/LOAD_DIM/PUSH_CONST operand at op index `k` into a value.
    @always_inline
    def _operand(
        prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
        k: Int,
        cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
        n_rows: Int,
        row: Int,
        dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
        dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ) -> Int64:
        var op = prog[3 * k + 0]
        var a = prog[3 * k + 1]
        var b = prog[3 * k + 2]
        if op == OP_LOAD_COL:
            return cols[Int(a) * n_rows + row]
        elif op == OP_PUSH_CONST:
            return a
        elif op == OP_LOAD_DIM:
            var key = Int(cols[Int(b) * n_rows + row])
            return dims[Int(dim_offsets[Int(a)]) + key]
        # operand op should be a leaf; non-leaf here means an unrecognized shape.
        return Int64(0)

    if prog_len == 1:
        return _operand(prog, 0, cols, n_rows, row, dims, dim_offsets)

    if prog_len == 3 and prog[3 * 2 + 0] == OP_MUL:
        var x = _operand(prog, 0, cols, n_rows, row, dims, dim_offsets)
        var y = _operand(prog, 1, cols, n_rows, row, dims, dim_offsets)
        return x * y

    # len 5: e ; k ; d ; SUB ; MUL  ->  e * (k - d)
    if (
        prog_len == 5
        and prog[3 * 3 + 0] == OP_SUB
        and prog[3 * 4 + 0] == OP_MUL
    ):
        var e = _operand(prog, 0, cols, n_rows, row, dims, dim_offsets)
        var k = _operand(prog, 1, cols, n_rows, row, dims, dim_offsets)
        var d = _operand(prog, 2, cols, n_rows, row, dims, dim_offsets)
        return e * (k - d)

    # len 9: e ; k ; d ; SUB ; MUL ; k2 ; t ; ADD ; MUL -> e*(k-d) * (k2 + t)
    if (
        prog_len == 9
        and prog[3 * 3 + 0] == OP_SUB
        and prog[3 * 4 + 0] == OP_MUL
        and prog[3 * 7 + 0] == OP_ADD
        and prog[3 * 8 + 0] == OP_MUL
    ):
        var e = _operand(prog, 0, cols, n_rows, row, dims, dim_offsets)
        var k = _operand(prog, 1, cols, n_rows, row, dims, dim_offsets)
        var d = _operand(prog, 2, cols, n_rows, row, dims, dim_offsets)
        var k2 = _operand(prog, 5, cols, n_rows, row, dims, dim_offsets)
        var t = _operand(prog, 6, cols, n_rows, row, dims, dim_offsets)
        return (e * (k - d)) * (k2 + t)

    # len 8 (Q14 promo): p ; e ; k ; d ; SUB ; MUL ; z ; SELECT
    #   stack at SELECT: [p, prod, z] -> pred=p, then=prod, else=z.
    if (
        prog_len == 8
        and prog[3 * 4 + 0] == OP_SUB
        and prog[3 * 5 + 0] == OP_MUL
        and prog[3 * 7 + 0] == OP_SELECT
    ):
        var p = _operand(prog, 0, cols, n_rows, row, dims, dim_offsets)
        var e = _operand(prog, 1, cols, n_rows, row, dims, dim_offsets)
        var k = _operand(prog, 2, cols, n_rows, row, dims, dim_offsets)
        var d = _operand(prog, 3, cols, n_rows, row, dims, dim_offsets)
        var z = _operand(prog, 6, cols, n_rows, row, dims, dim_offsets)
        var prod = e * (k - d)
        return prod if p != 0 else z

    # Unrecognized shape: universal fallback (bit-identical by definition).
    return eval_program(prog, prog_len, cols, n_rows, row, dims, dim_offsets)


@always_inline
def _row_passes_fast(
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
) -> Bool:
    if pass_len == 0:
        return True
    # Fast path covers the 1-op LOAD_COL(pass_slot) filter (Q1/Q6) and, when a
    # shape is unrecognized (Q5/Q14 multi-op dim-gather pass programs), falls back
    # to eval_program inside eval_program_fast -- still bit-identical.
    return eval_program_fast(
        pass_prog, pass_len, cols, n_rows, row, dims, dim_offsets
    ) != 0


# Q6 (UNGROUPED, M==1): specialized copy of seg_ungrouped_kernel. Filter is the
# 1-op pass column; the single metric is the fast-path expression. Identical lane
# striding / warp.sum / partial layout to seg_ungrouped_kernel.
def seg_ungrouped_kernel_q6(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes_fast(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# Q6 PREDICATE-INDEPENDENT (Phase G Stage 2, flag-gated): identical to
# seg_ungrouped_kernel_q6 except the row filter is evaluated IN-KERNEL from the
# resident filter-input columns + the 5 Q6 bounds passed as LAUNCH PARAMS, rather
# than from a host-baked 0/1 pass column. This decouples residency from the filter
# constants: the resident `cols` buffers already hold ALL rows (the materialize
# SQL has no WHERE), so the same buffers serve any constant set; only the bounds
# below change per run.
#
# The predicate is byte-for-byte the host pass-column's semantics
# (`_pred_pass(v, cmp, k)` in gpu_kernels.mojo) for the fixed Q6 shape:
#   l_shipdate >= ship_lo (CMP_GE)  AND  l_shipdate <  ship_hi (CMP_LT)
#   l_discount >= disc_lo (CMP_GE)  AND  l_discount <= disc_hi (CMP_LE)
#   l_quantity <  qty_hi  (CMP_LT)
# All five operands are read from `cols[slot * n_rows + row]` — the SAME int64-
# packed values _col_val widened into the buffer — so the pass set is identical.
# The metric is evaluated with the SAME eval_program_fast as the stock Q6 kernel,
# so passing rows contribute identical products and the int128 reduction matches.
def seg_ungrouped_kernel_q6_pred(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship_slot: Int,
    disc_slot: Int,
    qty_slot: Int,
    ship_lo: Int64,
    ship_hi: Int64,
    disc_lo: Int64,
    disc_hi: Int64,
    qty_hi: Int64,
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        var sd = cols[ship_slot * n_rows + i]
        var dc = cols[disc_slot * n_rows + i]
        var qt = cols[qty_slot * n_rows + i]
        if (
            sd >= ship_lo
            and sd < ship_hi
            and dc >= disc_lo
            and dc <= disc_hi
            and qt < qty_hi
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# ===========================================================================
# Phase G Stage 2 (generalized): predicate-independent residency for Q1 / Q14.
#
# A GENERAL in-kernel fact-range filter, evaluated per row from the resident
# columns + a small device-side predicate tape `fpred` of `n_fpred` triples
#   fpred[3*p + 0] = column SLOT  (cols[slot * n_rows + row])
#   fpred[3*p + 1] = cmp tag      (CMP_LT/LE/GT/GE/EQ/NE -- raw_plan_tags ints)
#   fpred[3*p + 2] = bound k      (the int64 the host pass-bake compared against)
# A row PASSES iff every triple passes (the same AND-of-fact-range-predicates the
# host pass column bakes). This is byte-for-byte the host `_pred_pass(v, cmp, k)`
# semantics over the SAME int64 slot values, so the pass set is identical and the
# resulting reduction is bit-exact -- but the bounds are LAUNCH-time inputs, so
# the resident columns are decoupled from the filter constants (warm across
# constants). The metric evaluation (incl. Q14's promo dim gather) is UNCHANGED:
# it still uses eval_program_fast over the same metric programs.
# ===========================================================================
@always_inline
def _fpred_pass_dev(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    row: Int,
    fpred: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_fpred: Int,
) -> Bool:
    for p in range(n_fpred):
        var slot = Int(fpred[3 * p + 0])
        var cmp = fpred[3 * p + 1]
        var k = fpred[3 * p + 2]
        var v = cols[slot * n_rows + row]
        # Mirror host _pred_pass exactly (same cmp tags / same int64 compares).
        if cmp == CMP_EQ:
            if not (v == k):
                return False
        elif cmp == CMP_NE:
            if not (v != k):
                return False
        elif cmp == CMP_LT:
            if not (v < k):
                return False
        elif cmp == CMP_LE:
            if not (v <= k):
                return False
        elif cmp == CMP_GT:
            if not (v > k):
                return False
        elif cmp == CMP_GE:
            if not (v >= k):
                return False
        else:
            return False
    return True


# Q1 PREDICATE-INDEPENDENT (DENSE_GROUP): identical to seg_dense_kernel_q1 except
# the row filter is the general in-kernel fact-range predicate (Q1: the single
# `l_shipdate <= cutoff`) evaluated from `fpred` launch params instead of a host
# pass column. Group-id slot + metric programs are unchanged -> bit-identical.
def seg_dense_kernel_q1_pred(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    fpred: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_fpred: Int,
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _fpred_pass_dev(cols, n_rows, i, fpred, n_fpred):
            var g = Int(cols[gid_slot * n_rows + i])
            var base = g * M
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[base + m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for g in range(G):
        for m in range(M):
            var s = warp.sum(acc[g * M + m])
            if lane == 0:
                partials[(blk * G + g) * M + m] = s


# Q14 PREDICATE-INDEPENDENT (UNGROUPED + 1 dim): identical to
# seg_ungrouped_kernel_q14 except the row filter is the general in-kernel fact-
# range predicate (Q14: `l_shipdate >= lo AND l_shipdate < hi`) evaluated from
# `fpred` launch params instead of a host pass column. The promo CASE stays in
# the metric programs (eval_program_fast's len-8 promo shape over the resident
# promo-flag dim gather) and is constant-INDEPENDENT, so it is untouched.
def seg_ungrouped_kernel_q14_pred(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    fpred: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_fpred: Int,
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _fpred_pass_dev(cols, n_rows, i, fpred, n_fpred):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# Q14 (UNGROUPED + 1 dim): same shape as Q6's ungrouped kernel, but the pass
# program and metric programs use OP_LOAD_DIM gathers (handled by the fast path /
# its fallback). Kept as a distinct symbol for clarity + per-kind dispatch.
def seg_ungrouped_kernel_q14(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes_fast(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# Q1 (DENSE_GROUP): specialized copy of seg_dense_kernel with the fast metric +
# filter path. Per-lane [G*M] accumulators, partials[(block*G+g)*M+m] layout, and
# warp.sum reduction are byte-identical to seg_dense_kernel.
def seg_dense_kernel_q1(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes_fast(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            var g = Int(cols[gid_slot * n_rows + i])
            var base = g * M
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[base + m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for g in range(G):
        for m in range(M):
            var s = warp.sum(acc[g * M + m])
            if lane == 0:
                partials[(blk * G + g) * M + m] = s


# Q5 (DENSE_GROUP + 5 dims): same structure as seg_dense_kernel_q1; the pass
# program uses OP_LOAD_DIM gathers + OP_EQ + OP_MUL (fast-path fallback handles
# it) and the metric is the fact-only revenue expression.
def seg_dense_kernel_q5(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes_fast(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            var g = Int(cols[gid_slot * n_rows + i])
            var base = g * M
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[base + m] += eval_program_fast(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for g in range(G):
        for m in range(M):
            var s = warp.sum(acc[g * M + m])
            if lane == 0:
                partials[(blk * G + g) * M + m] = s


# Q5 PREDICATE-INDEPENDENT (DENSE_GROUP + 5 dims): identical to seg_dense_kernel_q5
# except the row filter and the gid are evaluated IN-KERNEL from the resident
# columns + dim arrays + per-run scalars (o_lo/o_hi/asia_region) instead of a
# host-baked pass column + ASIA-rank gid. The resident buffers are therefore
# constant-INDEPENDENT (raw o_orderdate / cust_nation / supp_nation / supp_region
# dim arrays; gid = raw supp_nation), so DIFFERENT region/date constants reuse the
# same residency (warm-across-constants). The dim-array LAYOUT is fixed:
#   dims[doff[0] + orderkey] = o_orderdate (RAW int64 date)
#   dims[doff[1] + orderkey] = order_cust_nation (customer's nationkey)
#   dims[doff[2] + suppkey ] = supp_nation (supplier's nationkey)
#   dims[doff[3] + suppkey ] = supp_region (supplier's regionkey)
# A row PASSES iff (od in [o_lo, o_hi)) AND (cust_n == supp_n) AND
# (supp_r == asia_region) -- byte-for-byte the host Q5 pass set for THIS region+
# date, so the per-gid int128 SUM is bit-exact vs the per-constant Q5 path. The
# gid is the supplier's RAW nationkey (G = max_nationkey+1); the in-kernel region
# gate means only the selected region's nations get nonzero revenue, and the
# host emit-rule (revenue != 0) drops the rest -- exactly stock's row set. Lane
# striding / warp.sum / partials[(blk*G+g)*M+m] layout are IDENTICAL to
# seg_dense_kernel_q5.
def seg_dense_kernel_q5_pred(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    lok_slot: Int,
    lsk_slot: Int,
    o_lo: Int64,
    o_hi: Int64,
    asia_region: Int64,
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    var doff0 = Int(dim_offsets[0])
    var doff1 = Int(dim_offsets[1])
    var doff2 = Int(dim_offsets[2])
    var doff3 = Int(dim_offsets[3])
    while i < n_rows:
        var ok = Int(cols[lok_slot * n_rows + i])
        var sk = Int(cols[lsk_slot * n_rows + i])
        var od = dims[doff0 + ok]
        var cust_n = dims[doff1 + ok]
        var supp_n = dims[doff2 + sk]
        var supp_r = dims[doff3 + sk]
        var passes = (
            od >= o_lo
            and od < o_hi
            and cust_n == supp_n
            and supp_r == asia_region
        )
        if passes:
            var g = Int(cols[gid_slot * n_rows + i])
            if g >= 0 and g < G:
                var base = g * M
                for m in range(M):
                    var prog = metric_progs + 3 * Int(metric_offsets[m])
                    acc[base + m] += eval_program_fast(
                        prog, Int(metric_lens[m]), cols, n_rows, i,
                        dims, dim_offsets,
                    )
        i += stride
    var blk = Int(block_idx.x)
    for g in range(G):
        for m in range(M):
            var s = warp.sum(acc[g * M + m])
            if lane == 0:
                partials[(blk * G + g) * M + m] = s


# ---------------------------------------------------------------------------
# UNGROUPED kernel: NBLOCKS blocks x WARP lanes, lane-strided over all rows.
# Per-block int64 partials laid out partials[block * M + m].
# Reproduces q6_kernel for M=1 and a one-group multi-metric q1 shape.
# ---------------------------------------------------------------------------
def seg_ungrouped_kernel(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# ---------------------------------------------------------------------------
# DENSE_GROUP kernel: each row has a dense group id (in slot `gid_slot` of the
# packed columns, already widened to int64). Per-lane accumulators [G * M].
# Per-block partials laid out partials[(block * G + g) * M + m].
# Reproduces q1_kernel.
# ---------------------------------------------------------------------------
def seg_dense_kernel(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = SEG_NBLOCKS * WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if _row_passes(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            var g = Int(cols[gid_slot * n_rows + i])
            var base = g * M
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[base + m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    var blk = Int(block_idx.x)
    for g in range(G):
        for m in range(M):
            var s = warp.sum(acc[g * M + m])
            if lane == 0:
                partials[(blk * G + g) * M + m] = s


# ---------------------------------------------------------------------------
# Multi-warp variants of the two grid kernels (GPU_OP_BLOCK128). Identical math
# to seg_ungrouped_kernel / seg_dense_kernel, but each block is SEG_NWARPS warps
# (SEG_BLK threads): every warp reduces its lanes with warp.sum, lane 0 of each
# warp publishes its partial to shared memory, a single barrier, then the first
# threads sum across the warps and write ONE per-block partial. The partials
# layout and host int128 reduction are unchanged (one partial per block over
# SEG_NBLOCKS blocks), so only the in-block reduction differs.
# ---------------------------------------------------------------------------
def seg_ungrouped_kernel_mw(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x) % WARP
    var wid = Int(thread_idx.x) // WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var stride = SEG_NBLOCKS * SEG_BLK
    var i = Int(block_idx.x) * SEG_BLK + Int(thread_idx.x)
    while i < n_rows:
        if _row_passes(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    # per-warp reduce into shared (lane 0 of each warp writes), then combine.
    var sh = stack_allocation[
        SEG_NWARPS * SEG_MAX_METRICS,
        Scalar[DType.int64],
        address_space = AddressSpace.SHARED,
    ]()
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            sh[wid * SEG_MAX_METRICS + m] = s
    barrier()
    # threads [0, M) each sum one metric across the SEG_NWARPS warps.
    if Int(thread_idx.x) < M:
        var tot = Int64(0)
        for w in range(SEG_NWARPS):
            tot += sh[w * SEG_MAX_METRICS + Int(thread_idx.x)]
        partials[Int(block_idx.x) * M + Int(thread_idx.x)] = tot


def seg_dense_kernel_mw(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gid_slot: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    G: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x) % WARP
    var wid = Int(thread_idx.x) // WARP
    var acc = InlineArray[Int64, SEG_MAX_METRICS * SEG_MAX_METRICS](fill=0)
    var stride = SEG_NBLOCKS * SEG_BLK
    var i = Int(block_idx.x) * SEG_BLK + Int(thread_idx.x)
    while i < n_rows:
        if _row_passes(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            var g = Int(cols[gid_slot * n_rows + i])
            var base = g * M
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[base + m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += stride
    # per-warp reduce each (g, m) into shared, combine across warps. Shared is
    # sized to the comptime max [SEG_NWARPS][G*M <= SEG_MAX_METRICS^2].
    var sh = stack_allocation[
        SEG_NWARPS * SEG_MAX_METRICS * SEG_MAX_METRICS,
        Scalar[DType.int64],
        address_space = AddressSpace.SHARED,
    ]()
    var gm = G * M
    for x in range(gm):
        var s = warp.sum(acc[x])
        if lane == 0:
            sh[wid * (SEG_MAX_METRICS * SEG_MAX_METRICS) + x] = s
    barrier()
    # threads stride over the gm partials, summing each across the warps.
    var xi = Int(thread_idx.x)
    while xi < gm:
        var tot = Int64(0)
        for w in range(SEG_NWARPS):
            tot += sh[w * (SEG_MAX_METRICS * SEG_MAX_METRICS) + xi]
        var g = xi // M
        var m = xi % M
        partials[(Int(block_idx.x) * G + g) * M + m] = tot
        xi += SEG_BLK


# ---------------------------------------------------------------------------
# SORT_SEGREDUCE kernel: rows pre-sorted by group key; one warp per segment,
# lane-strided over [seg_off[s], seg_off[s+1]). Per-segment fits int64
# (per-order revenue in q3), so lane 0 writes the per-segment int64 directly:
# seg_out[s * M + m]. Reproduces q3_seg_kernel.
# ---------------------------------------------------------------------------
def seg_sort_kernel(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    seg_off: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_seg: Int,
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    seg_out: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var s = Int(block_idx.x)
    if s >= n_seg:
        return
    var lane = Int(thread_idx.x)
    var lo = Int(seg_off[s])
    var hi = Int(seg_off[s + 1])
    var acc = InlineArray[Int64, SEG_MAX_METRICS](fill=0)
    var i = lo + lane
    while i < hi:
        if _row_passes(
            pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
        ):
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i,
                    dims, dim_offsets,
                )
        i += WARP
    for m in range(M):
        var tot = warp.sum(acc[m])
        if lane == 0:
            seg_out[s * M + m] = tot


# ---------------------------------------------------------------------------
# HASH_GROUP kernel (NVIDIA / AMD only; needs 64-bit atomics).
#
# An open-addressing (linear-probe) hash table lives on the device:
#   slot_key[cap]      : the claimed group key, or HASH_EMPTY if free.
#   slot_acc[cap * M]   : M int64 metric accumulators per slot.
# One THREAD per fact row (grid-stride). A passing row computes its group key
# (the integer fact group key, in column slot `gk_slot`) and each metric value
# via the expr VM, then linear-probes from hash(key): at each probe it tries to
# CLAIM the slot with an atomic compare-exchange of the key word (EMPTY->key);
# success or finding the slot already holding `key` both stop the probe, and the
# row Atomic.fetch_adds its metric int64s into that slot. Per-group totals fit
# int64 (see the exactness bound in run_hashgroup), so the device side is pure
# int64 atomics; the int128 widening happens on the host read-back exactly like
# the other modes. No sort, no ORDER BY, single pass.
#
# 64-bit atomics gate: the ENTIRE body is wrapped in
# `comptime if is_nvidia_gpu() or is_amd_gpu()`. On Apple (no 64-bit atomics)
# the body compiles to an empty/abort kernel and the host never launches it (it
# keeps SORT_SEGREDUCE), so the comptime-false branch is never reached.
# ---------------------------------------------------------------------------
def seg_hash_kernel(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    gk_slot: Int,
    cap: Int,  # hash table capacity (power of two)
    pass_prog: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    dims: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    slot_key: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    slot_acc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    comptime if is_nvidia_gpu() or is_amd_gpu():
        # grid-stride: global_idx folds block_idx*block_dim + thread_idx; stride
        # by the full launched thread count each step.
        var i = Int(global_idx.x)
        var grid = Int(block_dim.x) * Int(grid_dim.x)
        var mask = cap - 1  # cap is pow2 => key & mask == key % cap
        while i < n_rows:
            if _row_passes(
                pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
            ):
                var key = cols[gk_slot * n_rows + i]
                # mix the key (Knuth multiplicative) then mask to [0, cap).
                var h = Int((UInt64(key) * 0x9E3779B97F4A7C15) >> 33) & mask
                # linear probe to claim or find the slot for `key`.
                var slot = h
                var placed = False
                var guard = 0
                while guard <= cap:
                    var expected = HASH_EMPTY
                    # Try to claim an empty slot for this key.
                    if Atomic[DType.int64].compare_exchange(
                        slot_key + slot, expected, key
                    ):
                        placed = True  # we claimed it
                    elif expected == key:
                        placed = True  # already ours (claimed by another row)
                    if placed:
                        var base = slot * M
                        for m in range(M):
                            var prog = metric_progs + 3 * Int(metric_offsets[m])
                            var v = eval_program(
                                prog, Int(metric_lens[m]), cols, n_rows, i,
                                dims, dim_offsets,
                            )
                            _ = Atomic[DType.int64].fetch_add(
                                slot_acc + base + m, v
                            )
                        break
                    slot = (slot + 1) & mask
                    guard += 1
            i += grid
    else:
        # Apple has no 64-bit atomics; the host never routes here.
        abort("seg_hash_kernel requires 64-bit atomics (NVIDIA/AMD)")


# ---------------------------------------------------------------------------
# Resident state: the device buffers that are STABLE across runs of the same
# pinned dataset. These are independent of the (tiny) filter/metric PROGRAMS,
# which may change per run, so a caller can upload these once and re-launch
# kernels many times without re-uploading.
#
# OWNERSHIP: the DeviceBuffer fields are ref-counted device-allocation handles
# (DeviceBuffer is ImplicitlyCopyable); the device allocations live as long as
# this struct (or any copy of it) is alive. Keep the SegResident alive for as
# long as you want to re-run kernels against the resident data; dropping it
# releases the GPU buffers. `ctx` is a cheap-to-copy shared context handle.
# ---------------------------------------------------------------------------
@fieldwise_init
struct SegResident(Movable):
    var ctx: DeviceContext
    var cols_d: DeviceBuffer[DType.int64]
    var n_rows: Int
    var n_cols: Int
    var dims_d: DeviceBuffer[DType.int64]
    var dim_offsets: List[Int64]  # small; kept on host (length n_dims+1 or empty)
    var n_dims: Int
    var seg_off_d: DeviceBuffer[DType.int64]
    var n_seg: Int


# ---------------------------------------------------------------------------
# segreduce_upload: allocate + upload the buffers that are STABLE across runs of
# the same pinned dataset (the packed columns, the FK-join dim arrays and the
# sort-segment offsets). Mirrors exactly what `run_segreduce` uploaded for these
# inputs, including the n_dims==0 / n_seg==0 dummy-buffer handling.
# ---------------------------------------------------------------------------
def segreduce_upload(
    ctx: DeviceContext,
    cols_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_cols: Int,
    n_rows: Int,
    seg_off_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_seg: Int,
    dims_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_dims: Int,
) raises -> SegResident:
    # These resident uploads are one-time (the pin-resident design uploads once
    # and reuses across warm runs), so a plain enqueue_copy is the fast path: the
    # driver bounces the pageable source through its own pinned buffer in one
    # shot. (map_to_host is the WRONG tool — it is bidirectional, DMAing
    # device->host on enter, so for a pure upload it is ~3.4x slower on PCIe;
    # measured on RTX 4090.)
    # ---- packed columns ----
    var cols_d = ctx.enqueue_create_buffer[DType.int64](n_cols * n_rows)
    ctx.enqueue_copy(cols_d, cols_host)

    # ---- FK-join dim arrays (concatenated). dims_total == dim_offsets_host[n_dims].
    # Allocate at least 1 element so the buffer is always valid (n_dims==0 path).
    var dims_total = Int(dim_offsets_host[n_dims]) if n_dims > 0 else 0
    var dims_n = dims_total if dims_total > 0 else 1
    var dims_d = ctx.enqueue_create_buffer[DType.int64](dims_n)
    if dims_total > 0:
        # dims_n == dims_total here, so copy fills the whole buffer.
        ctx.enqueue_copy(dims_d, dims_host)

    # ---- small host copy of the dim offsets (length n_dims+1; empty if n_dims==0).
    var dim_offsets = List[Int64]()
    if n_dims > 0:
        for i in range(n_dims + 1):
            dim_offsets.append(dim_offsets_host[i])

    # ---- sort-segment offsets. Allocate at least 1 element (n_seg==0 path).
    var soff_n = n_seg + 1 if n_seg > 0 else 1
    var seg_off_d = ctx.enqueue_create_buffer[DType.int64](soff_n)
    if n_seg > 0:
        # soff_n == n_seg+1 here, so copy fills the whole buffer.
        ctx.enqueue_copy(seg_off_d, seg_off_host)

    ctx.synchronize()
    return SegResident(
        ctx, cols_d, n_rows, n_cols, dims_d, dim_offsets^, n_dims,
        seg_off_d, n_seg,
    )


# ---------------------------------------------------------------------------
# segreduce_upload_from_packed: variant of segreduce_upload for the column-pool
# (GPU_OP_COLPOOL) path. The packed columns buffer `cols_d` has ALREADY been
# assembled on the device (by D2D copies from the pooled per-column buffers into
# their slot offsets), so this SKIPS the cols enqueue_create_buffer + enqueue_copy
# entirely. Everything else -- the dim arrays, dim_offsets, and sort-segment
# offsets -- is uploaded EXACTLY as segreduce_upload does (same n_dims==0 /
# n_seg==0 dummy-buffer handling, same final synchronize), and the returned
# SegResident is identical in shape. Because `cols_d` is byte-identical to what
# segreduce_upload would have produced (same per-column _col_val bytes at the same
# slot offsets), the kernels read it unchanged => bit-identical results.
#
# `cols_d` ownership transfers in (the caller assembled it and hands it over).
# ---------------------------------------------------------------------------
def segreduce_upload_from_packed(
    ctx: DeviceContext,
    var cols_d: DeviceBuffer[DType.int64],
    n_cols: Int,
    n_rows: Int,
    seg_off_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_seg: Int,
    dims_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_dims: Int,
) raises -> SegResident:
    # ---- packed columns: already assembled on device; NO upload here. ----

    # ---- FK-join dim arrays (concatenated). dims_total == dim_offsets_host[n_dims].
    # Allocate at least 1 element so the buffer is always valid (n_dims==0 path).
    var dims_total = Int(dim_offsets_host[n_dims]) if n_dims > 0 else 0
    var dims_n = dims_total if dims_total > 0 else 1
    var dims_d = ctx.enqueue_create_buffer[DType.int64](dims_n)
    if dims_total > 0:
        # dims_n == dims_total here, so copy fills the whole buffer.
        ctx.enqueue_copy(dims_d, dims_host)

    # ---- small host copy of the dim offsets (length n_dims+1; empty if n_dims==0).
    var dim_offsets = List[Int64]()
    if n_dims > 0:
        for i in range(n_dims + 1):
            dim_offsets.append(dim_offsets_host[i])

    # ---- sort-segment offsets. Allocate at least 1 element (n_seg==0 path).
    var soff_n = n_seg + 1 if n_seg > 0 else 1
    var seg_off_d = ctx.enqueue_create_buffer[DType.int64](soff_n)
    if n_seg > 0:
        # soff_n == n_seg+1 here, so copy fills the whole buffer.
        ctx.enqueue_copy(seg_off_d, seg_off_host)

    ctx.synchronize()
    return SegResident(
        ctx, cols_d^, n_rows, n_cols, dims_d, dim_offsets^, n_dims,
        seg_off_d, n_seg,
    )


# ---------------------------------------------------------------------------
# segreduce_run: upload ONLY the (small) filter + metric program buffers, launch
# the mode's kernel on `res`'s resident buffers, and perform the int128 host
# reduction. Returns result[g * M + m] over n_out_groups (same shape as today).
# The per-run output partial buffers are allocated inside this call.
# ---------------------------------------------------------------------------
def segreduce_run(
    mut res: SegResident,
    mode: Int64,
    pass_prog_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_progs_n_ops: Int,
    metric_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    gid_slot: Int,
    G: Int,
    kind: Int64 = KIND_UNKNOWN,
    # Phase G Stage 2 (flag-gated): Q6 predicate-independent residency. When
    # `q6_pred_active` is True (UNGROUPED + KIND_Q6 only), the row filter is
    # evaluated IN-KERNEL from these resident slots + per-run bounds instead of a
    # host-baked pass column (pass_len is then 0). Off by default -> stock path.
    q6_pred_active: Bool = False,
    q6_ship_slot: Int = 0,
    q6_disc_slot: Int = 0,
    q6_qty_slot: Int = 0,
    q6_ship_lo: Int64 = 0,
    q6_ship_hi: Int64 = 0,
    q6_disc_lo: Int64 = 0,
    q6_disc_hi: Int64 = 0,
    q6_qty_hi: Int64 = 0,
    # Phase G Stage 2 (generalized): Q1/Q14 predicate-independent residency. When
    # `gen_pred_active` is True (KIND_Q1 DENSE_GROUP / KIND_Q14 UNGROUPED+1dim),
    # the row filter is the general in-kernel fact-range predicate built from
    # `fpred_list` (flattened (slot,cmp,bound) triples; len == 3 * n_fpred)
    # instead of a host-baked pass column (pass_len is then 0). Off by default ->
    # stock path. Passed by value (a tiny list) so the bounds are fresh per run.
    gen_pred_active: Bool = False,
    fpred_list: List[Int64] = [],
    # Phase G Stage 2 (Path B): Q5 predicate-independent residency. When
    # `q5_pred_active` is True (KIND_Q5 DENSE_GROUP only), the row filter (region +
    # orderdate window + cust_nation==supp_nation) is evaluated IN-KERNEL from the
    # resident raw dim arrays + per-run scalars (o_lo/o_hi/asia_region) instead of a
    # host-baked pass column + ASIA-rank gid (pass_len is then 0, gid_slot holds the
    # RAW supplier nationkey). Off by default -> stock per-constant Q5 path.
    q5_pred_active: Bool = False,
    q5_lok_slot: Int = 0,
    q5_lsk_slot: Int = 0,
    q5_o_lo: Int64 = 0,
    q5_o_hi: Int64 = 0,
    q5_asia_region: Int64 = 0,
) raises -> List[Int128]:
    var ctx = res.ctx
    var cols_d = res.cols_d
    var n_rows = res.n_rows
    var dims_d = res.dims_d
    var n_seg = res.n_seg
    # GPU_OP_BLOCK128: route the two grid kernels to their multi-warp variants
    # (SEG_BLK threads/block) to lift occupancy past the 1-warp-block 50% ceiling.
    var use_mw = getenv("GPU_OP_BLOCK128", "") != ""

    # ---- rebuild the small host dim-offsets pointer for the kernels ----
    # doff buffer always valid (at least 1 element; n_dims==0 path).
    var doff_n = res.n_dims + 1 if res.n_dims > 0 else 1
    var doff_d = ctx.enqueue_create_buffer[DType.int64](doff_n)
    if res.n_dims > 0:
        # doff_n == n_dims+1 here, so copy fills the whole buffer.
        ctx.enqueue_copy(doff_d, res.dim_offsets.unsafe_ptr())

    # ---- filter program (allocate at least 1 element so the buffer is valid) ----
    var pass_n = pass_len * 3 if pass_len > 0 else 1
    var pass_d = ctx.enqueue_create_buffer[DType.int64](pass_n)
    if pass_len > 0:
        ctx.enqueue_copy(pass_d, pass_prog_host)

    var mp_n = metric_progs_n_ops * 3 if metric_progs_n_ops > 0 else 1
    var mp_d = ctx.enqueue_create_buffer[DType.int64](mp_n)
    if metric_progs_n_ops > 0:
        ctx.enqueue_copy(mp_d, metric_progs_host)

    # ---- generalized predicate tape (Q1/Q14): (slot,cmp,bound) triples. The
    # buffer is always valid (>=1 element); the kernels only read n_fpred triples.
    var n_fpred = len(fpred_list) // 3
    var fpred_n = len(fpred_list) if len(fpred_list) > 0 else 1
    var fpred_d = ctx.enqueue_create_buffer[DType.int64](fpred_n)
    if len(fpred_list) > 0:
        ctx.enqueue_copy(fpred_d, fpred_list.unsafe_ptr())

    var moff_d = ctx.enqueue_create_buffer[DType.int64](M)
    ctx.enqueue_copy(moff_d, metric_offsets_host)
    var mlen_d = ctx.enqueue_create_buffer[DType.int64](M)
    ctx.enqueue_copy(mlen_d, metric_lens_host)
    # No sync here: the kernel launches below are ordered after these small
    # program uploads on the one runtime stream; only the result read-back syncs.

    var result = List[Int128]()

    if mode == STRAT_SORT_SEGREDUCE:
        # one warp per segment; lane 0 writes per-segment int64 -> host int128
        var out_d = ctx.enqueue_create_buffer[DType.int64](n_seg * M)
        ctx.enqueue_function[seg_sort_kernel](
            cols_d, n_rows, res.seg_off_d, n_seg,
            pass_d, pass_len,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            out_d,
            grid_dim=n_seg, block_dim=WARP,
        )
        var out_h = alloc[Int64](n_seg * M)
        var out_sub = DeviceBuffer(ctx, out_d.unsafe_ptr(), n_seg * M, owning=False)
        ctx.enqueue_copy(out_h, out_sub)
        ctx.synchronize()
        for s in range(n_seg):
            for m in range(M):
                result.append(Int128(out_h[s * M + m]))
        out_h.free()
        return result^

    if mode == STRAT_DENSE_GROUP:
        var npart = SEG_NBLOCKS * G * M
        var part_d = ctx.enqueue_create_buffer[DType.int64](npart)
        if q5_pred_active and kind == KIND_Q5:
            # Phase G Stage 2 (Path B): Q5 in-kernel predicate over the resident
            # raw dim arrays + per-run scalars (no host pass column; gid is the raw
            # supplier nationkey). Takes priority over use_mw because the pass
            # column is absent (pass_len==0) on this path. Lane striding / warp.sum
            # / partial layout match seg_dense_kernel_q5.
            ctx.enqueue_function[seg_dense_kernel_q5_pred](
                cols_d, n_rows, gid_slot,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                q5_lok_slot, q5_lsk_slot,
                q5_o_lo, q5_o_hi, q5_asia_region,
                grid_dim=SEG_NBLOCKS, block_dim=WARP,
            )
        elif gen_pred_active and kind == KIND_Q1:
            # Phase G Stage 2 (generalized): Q1 in-kernel predicate over resident
            # columns + per-run bounds (no host pass column). Takes priority over
            # use_mw because the pass column is absent (pass_len==0) on this path.
            # Lane striding / warp.sum / partial layout match seg_dense_kernel_q1.
            ctx.enqueue_function[seg_dense_kernel_q1_pred](
                cols_d, n_rows, gid_slot,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                fpred_d, n_fpred,
                grid_dim=SEG_NBLOCKS, block_dim=WARP,
            )
        elif use_mw:
            # Multi-warp occupancy variant is unchanged (generic interpreter):
            # the comptime-specialized kernels mirror the 1-warp-block layout.
            ctx.enqueue_function[seg_dense_kernel_mw](
                cols_d, n_rows, gid_slot,
                pass_d, pass_len,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                grid_dim=SEG_NBLOCKS, block_dim=SEG_BLK,
            )
        elif kind == KIND_Q1:
            ctx.enqueue_function[seg_dense_kernel_q1](
                cols_d, n_rows, gid_slot,
                pass_d, pass_len,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                grid_dim=SEG_NBLOCKS, block_dim=WARP,
            )
        elif kind == KIND_Q5:
            ctx.enqueue_function[seg_dense_kernel_q5](
                cols_d, n_rows, gid_slot,
                pass_d, pass_len,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                grid_dim=SEG_NBLOCKS, block_dim=WARP,
            )
        else:
            ctx.enqueue_function[seg_dense_kernel](
                cols_d, n_rows, gid_slot,
                pass_d, pass_len,
                mp_d, moff_d, mlen_d, M, G,
                dims_d, doff_d,
                part_d,
                grid_dim=SEG_NBLOCKS, block_dim=WARP,
            )
        var part_h = alloc[Int64](npart)
        var part_sub = DeviceBuffer(ctx, part_d.unsafe_ptr(), npart, owning=False)
        ctx.enqueue_copy(part_h, part_sub)
        ctx.synchronize()
        for g in range(G):
            for m in range(M):
                var acc = Int128(0)
                for b in range(SEG_NBLOCKS):
                    acc += Int128(part_h[(b * G + g) * M + m])
                result.append(acc)
        part_h.free()
        return result^

    # default: STRAT_UNGROUPED
    var npart = SEG_NBLOCKS * M
    var part_d = ctx.enqueue_create_buffer[DType.int64](npart)
    if q6_pred_active:
        # Phase G Stage 2: Q6 in-kernel predicate over resident columns + per-run
        # bounds (no host pass column). Takes priority over use_mw because the
        # pass column is absent (pass_len==0) on this path; the predicate kernel
        # is the only one that evaluates the filter from the bounds. The lane
        # striding / warp.sum / partial layout match seg_ungrouped_kernel_q6.
        ctx.enqueue_function[seg_ungrouped_kernel_q6_pred](
            cols_d, n_rows,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            q6_ship_slot, q6_disc_slot, q6_qty_slot,
            q6_ship_lo, q6_ship_hi, q6_disc_lo, q6_disc_hi, q6_qty_hi,
            grid_dim=SEG_NBLOCKS, block_dim=WARP,
        )
    elif gen_pred_active and kind == KIND_Q14:
        # Phase G Stage 2 (generalized): Q14 in-kernel predicate over resident
        # columns + per-run bounds (no host pass column). Takes priority over
        # use_mw (pass column absent). The promo CASE stays in the metric programs
        # (constant-independent). Lane / warp.sum / partial layout match
        # seg_ungrouped_kernel_q14.
        ctx.enqueue_function[seg_ungrouped_kernel_q14_pred](
            cols_d, n_rows,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            fpred_d, n_fpred,
            grid_dim=SEG_NBLOCKS, block_dim=WARP,
        )
    elif use_mw:
        # Multi-warp occupancy variant is unchanged (generic interpreter).
        ctx.enqueue_function[seg_ungrouped_kernel_mw](
            cols_d, n_rows,
            pass_d, pass_len,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            grid_dim=SEG_NBLOCKS, block_dim=SEG_BLK,
        )
    elif kind == KIND_Q6:
        ctx.enqueue_function[seg_ungrouped_kernel_q6](
            cols_d, n_rows,
            pass_d, pass_len,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            grid_dim=SEG_NBLOCKS, block_dim=WARP,
        )
    elif kind == KIND_Q14:
        ctx.enqueue_function[seg_ungrouped_kernel_q14](
            cols_d, n_rows,
            pass_d, pass_len,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            grid_dim=SEG_NBLOCKS, block_dim=WARP,
        )
    else:
        ctx.enqueue_function[seg_ungrouped_kernel](
            cols_d, n_rows,
            pass_d, pass_len,
            mp_d, moff_d, mlen_d, M,
            dims_d, doff_d,
            part_d,
            grid_dim=SEG_NBLOCKS, block_dim=WARP,
        )
    var part_h = alloc[Int64](npart)
    var part_sub = DeviceBuffer(ctx, part_d.unsafe_ptr(), npart, owning=False)
    ctx.enqueue_copy(part_h, part_sub)
    ctx.synchronize()
    for m in range(M):
        var acc = Int128(0)
        for b in range(SEG_NBLOCKS):
            acc += Int128(part_h[b * M + m])
        result.append(acc)
    part_h.free()
    return result^


# ---------------------------------------------------------------------------
# HASH_GROUP result: the occupied-slot group keys + their int128 metric sums,
# laid out sums[g * M + m] (same row-major shape segreduce_run returns), with
# `keys[g]` the integer group key of output group g. Order is hash-slot order
# (unspecified); the caller (and the parent ORDER BY) does not depend on it.
# ---------------------------------------------------------------------------
@fieldwise_init
struct HashGroupResult(Movable):
    var keys: List[Int64]
    var sums: List[Int128]
    var M: Int


# ---------------------------------------------------------------------------
# segreduce_run_hash: the HASH_GROUP driver. NVIDIA/AMD only (the kernel body is
# comptime-gated on 64-bit atomics; the host must only call this when
# has_nvidia_gpu_accelerator()/has_amd_gpu_accelerator()).
#
# Allocates a device hash table (slot_key[cap] init HASH_EMPTY, slot_acc[cap*M]
# init 0), launches one thread per fact row (grid-stride) to atomic-accumulate
# each group's metric int64s, then reads back the occupied slots and widens to
# int128 on the host (same exactness contract as the other modes: per-row + per-
# group values fit int64; the int128 widening is host-side).
#
# `cap` must be a power of two and a safe bound on the distinct group count
# (caller picks next_pow2 >= 2 * n_distinct_estimate; see run_hashgroup caller).
# ---------------------------------------------------------------------------
def segreduce_run_hash(
    mut res: SegResident,
    gk_slot: Int,
    cap: Int,
    pass_prog_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_progs_n_ops: Int,
    metric_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
) raises -> HashGroupResult:
    var ctx = res.ctx
    var cols_d = res.cols_d
    var n_rows = res.n_rows
    var dims_d = res.dims_d

    # ---- small per-run program buffers (mirrors segreduce_run) ----
    var doff_n = res.n_dims + 1 if res.n_dims > 0 else 1
    var doff_d = ctx.enqueue_create_buffer[DType.int64](doff_n)
    if res.n_dims > 0:
        ctx.enqueue_copy(doff_d, res.dim_offsets.unsafe_ptr())

    var pass_n = pass_len * 3 if pass_len > 0 else 1
    var pass_d = ctx.enqueue_create_buffer[DType.int64](pass_n)
    if pass_len > 0:
        ctx.enqueue_copy(pass_d, pass_prog_host)

    var mp_n = metric_progs_n_ops * 3 if metric_progs_n_ops > 0 else 1
    var mp_d = ctx.enqueue_create_buffer[DType.int64](mp_n)
    if metric_progs_n_ops > 0:
        ctx.enqueue_copy(mp_d, metric_progs_host)
    var moff_d = ctx.enqueue_create_buffer[DType.int64](M)
    ctx.enqueue_copy(moff_d, metric_offsets_host)
    var mlen_d = ctx.enqueue_create_buffer[DType.int64](M)
    ctx.enqueue_copy(mlen_d, metric_lens_host)

    # ---- the device hash table: keys init to HASH_EMPTY, accumulators to 0 ----
    var slot_key_d = ctx.enqueue_create_buffer[DType.int64](cap)
    var slot_acc_d = ctx.enqueue_create_buffer[DType.int64](cap * M)
    slot_key_d.enqueue_fill(HASH_EMPTY)
    slot_acc_d.enqueue_fill(Int64(0))
    # No sync: the fills are ordered before the kernel that reads the table on
    # the same stream; only the result read-back below needs a sync.

    # ---- launch: one thread per row, grid-stride over n_rows ----
    var nblocks = (n_rows + HASH_BLOCK - 1) // HASH_BLOCK
    if nblocks < 1:
        nblocks = 1
    # Cap the grid so the stride loop stays efficient (still covers all rows).
    if nblocks > SEG_NBLOCKS:
        nblocks = SEG_NBLOCKS
    ctx.enqueue_function[seg_hash_kernel](
        cols_d, n_rows, gk_slot, cap,
        pass_d, pass_len,
        mp_d, moff_d, mlen_d, M,
        dims_d, doff_d,
        slot_key_d, slot_acc_d,
        grid_dim=nblocks, block_dim=HASH_BLOCK,
    )

    # ---- read back occupied slots, widen int64 -> int128 on the host ----
    var key_h = alloc[Int64](cap)
    var acc_h = alloc[Int64](cap * M)
    ctx.enqueue_copy(key_h, slot_key_d)
    ctx.enqueue_copy(acc_h, slot_acc_d)
    ctx.synchronize()

    var keys = List[Int64]()
    var sums = List[Int128]()
    for slot in range(cap):
        if key_h[slot] == HASH_EMPTY:
            continue
        keys.append(key_h[slot])
        for m in range(M):
            sums.append(Int128(acc_h[slot * M + m]))
    key_h.free()
    acc_h.free()
    return HashGroupResult(keys^, sums^, M)


# ---------------------------------------------------------------------------
# Host driver (thin wrapper).
#
# Uploads the packed columns + the (optional) filter program + the concatenated
# metric programs + the group/segment metadata, launches the mode's kernel, and
# performs the int128 host reduction. Returns result[g * M + m] over n_out_groups.
#
# This is now a thin wrapper over segreduce_upload + segreduce_run; its signature
# and returned values are byte-identical to before the upload/launch split.
#
# `cols_host`        : packed int64 columns (cols[slot * n_rows + row]), n_cols slots.
# `pass_prog_host`   : flattened filter program (3 * pass_len int64s); pass_len==0 -> no filter.
# `metric_progs_host`: concatenated metric programs (flattened triples).
# `metric_offsets`   : op-offset of each metric's first op (length M).
# `metric_lens`      : op-count of each metric (length M).
# DENSE_GROUP: `gid_slot` = column slot holding the dense group id; `G` = #groups.
# SORT_SEGREDUCE: `seg_off_host` = segment offsets (length n_seg+1); `n_seg` segments.
#
# FK-JOIN DIM ARRAYS (on-GPU dense-array gather, OP_LOAD_DIM):
# `dims_host`        : N dense dim arrays concatenated back-to-back (one int64
#                      buffer); total length = dim_offsets_host[n_dims].
# `dim_offsets_host` : per-dim start offsets (length n_dims+1); offset[0]==0 and
#                      offset[n_dims]==total element count. The caller builds dim
#                      array `a` so that dim_array[a][key] is the carried value /
#                      pass flag for FK value `key`, sized to max key + 1.
# `n_dims`           : number of dim arrays. n_dims==0 -> no gather; the program
#                      emits no OP_LOAD_DIM and behavior is byte-for-byte the
#                      pre-existing no-dim path (Q6/Q1 unaffected). Pass a 1-elem
#                      placeholder for dims_host / dim_offsets_host in that case.
# ---------------------------------------------------------------------------
def run_segreduce(
    ctx: DeviceContext,
    mode: Int64,
    n_rows: Int,
    cols_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_cols: Int,
    pass_prog_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    pass_len: Int,
    metric_progs_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_progs_n_ops: Int,
    metric_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    metric_lens_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    M: Int,
    gid_slot: Int,
    G: Int,
    seg_off_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_seg: Int,
    dims_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    dim_offsets_host: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_dims: Int,
) raises -> List[Int128]:
    var res = segreduce_upload(
        ctx, cols_host, n_cols, n_rows,
        seg_off_host, n_seg,
        dims_host, dim_offsets_host, n_dims,
    )
    return segreduce_run(
        res, mode,
        pass_prog_host, pass_len,
        metric_progs_host, metric_progs_n_ops,
        metric_offsets_host, metric_lens_host,
        M, gid_slot, G,
    )
