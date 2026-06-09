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

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from gpu_platform import WARP
from raw_plan_tags import (
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
)
from expr_vm import eval_program

comptime SEG_NBLOCKS = 4096  # one warp per block (matches the existing kernels)
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
# Host driver.
#
# Uploads the packed columns + the (optional) filter program + the concatenated
# metric programs + the group/segment metadata, launches the mode's kernel, and
# performs the int128 host reduction. Returns result[g * M + m] over n_out_groups.
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
    # ---- upload the shared inputs ----
    var cols_d = ctx.enqueue_create_buffer[DType.int64](n_cols * n_rows)
    ctx.enqueue_copy(cols_d, cols_host)

    # FK-join dim arrays (concatenated). dims_total == dim_offsets_host[n_dims].
    # Allocate at least 1 element so the buffer is always valid (n_dims==0 path).
    var dims_total = Int(dim_offsets_host[n_dims]) if n_dims > 0 else 0
    var dims_n = dims_total if dims_total > 0 else 1
    var dims_d = ctx.enqueue_create_buffer[DType.int64](dims_n)
    if dims_total > 0:
        # dims_n == dims_total here, so copy fills the whole buffer.
        ctx.enqueue_copy(dims_d, dims_host)
    var doff_n = n_dims + 1 if n_dims > 0 else 1
    var doff_d = ctx.enqueue_create_buffer[DType.int64](doff_n)
    if n_dims > 0:
        # doff_n == n_dims+1 here, so copy fills the whole buffer.
        ctx.enqueue_copy(doff_d, dim_offsets_host)

    # filter program (allocate at least 1 element so the buffer is valid)
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
    ctx.synchronize()

    var result = List[Int128]()

    if mode == STRAT_SORT_SEGREDUCE:
        # one warp per segment; lane 0 writes per-segment int64 -> host int128
        var soff_d = ctx.enqueue_create_buffer[DType.int64](n_seg + 1)
        ctx.enqueue_copy(soff_d, seg_off_host)
        var out_d = ctx.enqueue_create_buffer[DType.int64](n_seg * M)
        ctx.synchronize()
        ctx.enqueue_function[seg_sort_kernel](
            cols_d, n_rows, soff_d, n_seg,
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
        ctx.synchronize()
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
    ctx.synchronize()
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
