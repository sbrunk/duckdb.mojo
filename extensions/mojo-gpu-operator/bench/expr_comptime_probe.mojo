"""Feasibility probe: comptime-specialized expression kernels vs the runtime VM.

Improvement #2 for the mojo-gpu-operator: today the segreduce kernels evaluate
the per-row metric/filter expressions with a RUNTIME stack-machine interpreter
(`eval_program` in src/expr_vm.mojo). Per row, per metric it loops over a flat
int64 program in global memory, switches on an op tag, and push/pops an
InlineArray stack. This probe asks: if the program STRUCTURE is a compile-time
value, does Mojo's comptime machinery unroll it into branch-free, register-only,
straight-line arithmetic — and is that materially faster on this Apple M3 Max?

It is the structural analogue of cuDF's runtime nvrtc JIT: cuDF needs nvrtc at
runtime; Mojo specializes at compile time and the same source ports to
Apple/AMD/NVIDIA.

WHAT THIS PROBE DOES
--------------------
Two GPU kernels over a synthetic ~6M-row Q1-like workload (the 8 Q1 metric
expressions + the Q1 filter, integer inputs exactly like the kernel oracles):

  (A) interpreter   — calls the existing `eval_program` per row per metric
                      (imported from src/expr_vm.mojo). Programs live in device
                      memory, addressed exactly as segreduce builds them.
  (B) comptime      — the 8 metric programs + filter are COMPTIME values, and a
                      `comptime for` unrolls them into straight-line register
                      arithmetic: no stack, no op-tag switch, no per-op global
                      reads. For a fixed synthetic query ALL operands are
                      comptime (the realistic production split keeps filter
                      *constants* as runtime kernel args; see the report).

Both kernels mirror `seg_ungrouped_kernel`'s lane-strided layout and write
per-block int64 partials `partials[block * M + m]`. The probe:

  (a) asserts (B) produces BIT-IDENTICAL per-block partials to (A) — fails loud;
  (b) warm-times each kernel over many iterations (excludes the first), prints
      GPU ms and the speedup, for BOTH the Q1-like (8-metric) and Q6-like
      (1-metric) cases.

CAN `comptime for` LOOP OVER A RUNTIME-UNKNOWN PROGRAM?  No.
------------------------------------------------------------
`comptime for` requires a compile-time-known iterable. That is exactly the
point: the improvement makes the op STRUCTURE a comptime value. Here, because
the synthetic query is fixed, the whole program (ops + operands) is comptime, so
the unroll is direct. In production the op structure is comptime per query KIND
and the kernel is dispatched from a runtime `kind` through a finite switch (one
comptime-specialized kernel instance per kind), with the interpreter as the
universal fallback. The probe models the per-kind specialized instance.
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns
from gpu_platform import WARP
from raw_plan_tags import (
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
)
from expr_vm import eval_program

comptime N = 6_000_000  # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096  # one warp per block (matches SEG_NBLOCKS / the oracles)

# Packed-column slot assignment (column-major: cols[slot * N + row]). Mirrors how
# the host packs the segreduce `cols` buffer. Slot 0 is the precomputed 0/1 Q1
# filter pass flag (l_shipdate <= cutoff lowered to a pass column, the simplest
# path the planner uses); 1..4 are qty/ext/disc/tax.
comptime S_PASS = 0
comptime S_QTY = 1
comptime S_EXT = 2
comptime S_DISC = 3
comptime S_TAX = 4
comptime N_COLS = 5

comptime M_Q1 = 8  # Q1 metric count (== SEG_MAX_METRICS)
comptime M_Q6 = 1  # Q6 single-metric case


# ===========================================================================
# (A) INTERPRETER kernels — call the existing eval_program, exactly as the real
# seg_ungrouped_kernel does (filter via a 1-op pass-column program).
# ===========================================================================
def interp_kernel(
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
    var stride = NBLOCKS * WARP
    var acc = InlineArray[Int64, M_Q1](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        var passes = True
        if pass_len != 0:
            passes = (
                eval_program(
                    pass_prog, pass_len, cols, n_rows, i, dims, dim_offsets
                )
                != 0
            )
        if passes:
            for m in range(M):
                var prog = metric_progs + 3 * Int(metric_offsets[m])
                acc[m] += eval_program(
                    prog, Int(metric_lens[m]), cols, n_rows, i, dims, dim_offsets
                )
        i += stride
    var blk = Int(block_idx.x)
    for m in range(M):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M + m] = s


# ===========================================================================
# (B) COMPTIME-UNROLLED kernels — the program STRUCTURE + operands are comptime.
#
# Q1: the 8 metrics, in segreduce VM terms (constants already resolved to int64;
# "1 - discount" at scale 2 is "100 - disc"), are:
#     m0 count        : PUSH 1
#     m1 sum(qty)     : LOAD qty
#     m2 sum(ext)     : LOAD ext
#     m3 sum(disc)    : LOAD disc
#     m4 disc_price s4: LOAD ext; PUSH 100; LOAD disc; SUB; MUL   = ext*(100-disc)
#     m5 charge    s6 : ...; PUSH 100; LOAD tax; ADD; MUL         = m4*(100+tax)
#     m6 avg-qty num  : LOAD qty   (same as m1; avg's int64 numerator)
#     m7 avg-ext num  : LOAD ext   (same as m2)
#
# The single-LOAD / count metrics are driven by a comptime slot table and a
# `comptime for` unroll (slot == -1 means the constant-1 count metric). The two
# multi-op metrics (m4, m5) are direct register expressions — the unrolled form
# of their postfix programs. This is exactly what the planner would emit per Q1.
# ===========================================================================

# Per-metric column slot for the LOAD/count metrics; -1 == count (PUSH 1),
# -2 marks the two composite metrics handled by direct expressions (m4, m5).
comptime Q1_SLOT: InlineArray[Int, M_Q1] = [
    -1, S_QTY, S_EXT, S_DISC, -2, -2, S_QTY, S_EXT
]


def comptime_kernel_q1(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * WARP
    var acc = InlineArray[Int64, M_Q1](fill=0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        # Filter: precomputed 0/1 pass column (S_PASS), comptime-known slot.
        if cols[S_PASS * n_rows + i] != 0:
            var e = cols[S_EXT * n_rows + i]
            var d = cols[S_DISC * n_rows + i]
            var t = cols[S_TAX * n_rows + i]
            # m4 = ext*(100-disc) scale4; m5 = m4*(100+tax) scale6 — direct.
            var disc_price = e * (Int64(100) - d)
            var charge = disc_price * (Int64(100) + t)
            # m0..m3, m6, m7 via comptime-unrolled slot table.
            comptime for m in range(M_Q1):
                comptime slot = Q1_SLOT[m]
                comptime if slot == -1:
                    acc[m] += Int64(1)  # count
                elif slot == -2:
                    pass  # composite, handled below
                else:
                    acc[m] += cols[slot * n_rows + i]
            acc[4] += disc_price
            acc[5] += charge
        i += stride
    var blk = Int(block_idx.x)
    comptime for m in range(M_Q1):
        var s = warp.sum(acc[m])
        if lane == 0:
            partials[blk * M_Q1 + m] = s


# Q6 single metric: filter via pass column, metric = ext*disc (LOAD ext; LOAD
# disc; MUL). The Q6 oracle's range predicates are likewise lowered to the
# precomputed pass column here (the simplest planner path).
def comptime_kernel_q6(
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * WARP
    var acc = Int64(0)
    var i = Int(block_idx.x) * WARP + lane
    while i < n_rows:
        if cols[S_PASS * n_rows + i] != 0:
            acc += cols[S_EXT * n_rows + i] * cols[S_DISC * n_rows + i]
        i += stride
    var s = warp.sum(acc)
    if lane == 0:
        partials[Int(block_idx.x)] = s


# ---------------------------------------------------------------------------
# Build the metric programs in the EXACT segreduce concatenated-program layout
# (flat int64 triples [op, a, b], metric m starts at 3 * metric_offsets[m]).
# Returns (flat_prog, offsets, lens, total_ops).
# ---------------------------------------------------------------------------
@fieldwise_init
struct ProgPack(Movable):
    var prog: List[Int64]
    var offs: List[Int64]
    var lens: List[Int64]


def _emit(mut p: List[Int64], op: Int64, a: Int64, b: Int64):
    p.append(op)
    p.append(a)
    p.append(b)


def build_q1_progs() -> ProgPack:
    var prog = List[Int64]()
    var offs = List[Int64]()
    var lens = List[Int64]()

    def begin(mut offs: List[Int64], cur_ops: Int):
        offs.append(Int64(cur_ops))

    # m0 count : PUSH 1
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_PUSH_CONST, 1, 0)
    lens.append(1)
    # m1 sum(qty)
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_QTY, 0)
    lens.append(1)
    # m2 sum(ext)
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_EXT, 0)
    lens.append(1)
    # m3 sum(disc)
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_DISC, 0)
    lens.append(1)
    # m4 disc_price : LOAD ext; PUSH 100; LOAD disc; SUB; MUL
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_EXT, 0)
    _emit(prog, OP_PUSH_CONST, 100, 0)
    _emit(prog, OP_LOAD_COL, S_DISC, 0)
    _emit(prog, OP_SUB, 0, 0)
    _emit(prog, OP_MUL, 0, 0)
    lens.append(5)
    # m5 charge : LOAD ext; PUSH 100; LOAD disc; SUB; MUL; PUSH 100; LOAD tax; ADD; MUL
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_EXT, 0)
    _emit(prog, OP_PUSH_CONST, 100, 0)
    _emit(prog, OP_LOAD_COL, S_DISC, 0)
    _emit(prog, OP_SUB, 0, 0)
    _emit(prog, OP_MUL, 0, 0)
    _emit(prog, OP_PUSH_CONST, 100, 0)
    _emit(prog, OP_LOAD_COL, S_TAX, 0)
    _emit(prog, OP_ADD, 0, 0)
    _emit(prog, OP_MUL, 0, 0)
    lens.append(9)
    # m6 avg-qty numerator : LOAD qty
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_QTY, 0)
    lens.append(1)
    # m7 avg-ext numerator : LOAD ext
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_EXT, 0)
    lens.append(1)

    return ProgPack(prog^, offs^, lens^)


def build_q6_progs() -> ProgPack:
    var prog = List[Int64]()
    var offs = List[Int64]()
    var lens = List[Int64]()
    # m0 : LOAD ext; LOAD disc; MUL
    offs.append(Int64(len(prog) // 3))
    _emit(prog, OP_LOAD_COL, S_EXT, 0)
    _emit(prog, OP_LOAD_COL, S_DISC, 0)
    _emit(prog, OP_MUL, 0, 0)
    lens.append(3)
    return ProgPack(prog^, offs^, lens^)


# ---------------------------------------------------------------------------
# Warm-timed launch helper: runs the bound kernel `iters` times after a warmup,
# returns the average GPU ms (launch + synchronize, first excluded). We time
# kernel-only (the resident column buffer is uploaded once, like the real
# pin-resident segreduce path).
# ---------------------------------------------------------------------------


def main() raises:
    comptime assert has_accelerator(), "expr_comptime_probe requires a GPU"
    var ctx = DeviceContext()

    # ---- synthetic lineitem-like columns (integer, like the oracles) ----
    var qty = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var disc = alloc[Int64](N)
    var tax = alloc[Int64](N)
    var ship = alloc[Int32](N)
    for i in range(N):
        qty[i] = Int64(100 + (i * 22695477) % 4900)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)
        disc[i] = Int64((i * 48271) % 11)
        tax[i] = Int64((i * 69069) % 9)
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)

    var ship_hi = Int32(9131)

    # ---- pack columns column-major: cols[slot * N + row]. Slot 0 is the Q1
    # precomputed pass flag (ship <= ship_hi). ----
    var cols_h = alloc[Int64](N_COLS * N)
    for i in range(N):
        cols_h[S_PASS * N + i] = Int64(1) if ship[i] <= ship_hi else Int64(0)
        cols_h[S_QTY * N + i] = qty[i]
        cols_h[S_EXT * N + i] = ext[i]
        cols_h[S_DISC * N + i] = disc[i]
        cols_h[S_TAX * N + i] = tax[i]

    var cols_d = ctx.enqueue_create_buffer[DType.int64](N_COLS * N)
    ctx.enqueue_copy(cols_d, cols_h)

    # dummy dim buffers (n_dims == 0 path).
    var dims_d = ctx.enqueue_create_buffer[DType.int64](1)
    var doff_d = ctx.enqueue_create_buffer[DType.int64](1)
    dims_d.enqueue_fill(Int64(0))
    doff_d.enqueue_fill(Int64(0))

    # =====================================================================
    # Q1-like, 8 metrics.
    # =====================================================================
    var q1 = build_q1_progs()
    var q1_total_ops = len(q1.prog) // 3

    # pass program: 1 op, LOAD_COL S_PASS
    var pass_prog = List[Int64]()
    _emit(pass_prog, OP_LOAD_COL, S_PASS, 0)
    var pass_len = 1

    var pass_d = ctx.enqueue_create_buffer[DType.int64](3)
    ctx.enqueue_copy(pass_d, pass_prog.unsafe_ptr())
    var mp_d = ctx.enqueue_create_buffer[DType.int64](q1_total_ops * 3)
    ctx.enqueue_copy(mp_d, q1.prog.unsafe_ptr())
    var moff_d = ctx.enqueue_create_buffer[DType.int64](M_Q1)
    ctx.enqueue_copy(moff_d, q1.offs.unsafe_ptr())
    var mlen_d = ctx.enqueue_create_buffer[DType.int64](M_Q1)
    ctx.enqueue_copy(mlen_d, q1.lens.unsafe_ptr())

    var npart1 = NBLOCKS * M_Q1
    var part_interp_d = ctx.enqueue_create_buffer[DType.int64](npart1)
    var part_ct_d = ctx.enqueue_create_buffer[DType.int64](npart1)
    ctx.synchronize()

    comptime WARMUP = 3
    comptime ITERS = 30

    # --- (A) interpreter, Q1 ---
    for _ in range(WARMUP):
        ctx.enqueue_function[interp_kernel](
            cols_d, N, pass_d, pass_len, mp_d, moff_d, mlen_d, M_Q1,
            dims_d, doff_d, part_interp_d,
            grid_dim=NBLOCKS, block_dim=WARP,
        )
    ctx.synchronize()
    var t0 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[interp_kernel](
            cols_d, N, pass_d, pass_len, mp_d, moff_d, mlen_d, M_Q1,
            dims_d, doff_d, part_interp_d,
            grid_dim=NBLOCKS, block_dim=WARP,
        )
    ctx.synchronize()
    var interp_q1_ms = Float64(perf_counter_ns() - t0) / 1e6 / ITERS

    # --- (B) comptime, Q1 ---
    for _ in range(WARMUP):
        ctx.enqueue_function[comptime_kernel_q1](
            cols_d, N, part_ct_d, grid_dim=NBLOCKS, block_dim=WARP
        )
    ctx.synchronize()
    var t1 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[comptime_kernel_q1](
            cols_d, N, part_ct_d, grid_dim=NBLOCKS, block_dim=WARP
        )
    ctx.synchronize()
    var ct_q1_ms = Float64(perf_counter_ns() - t1) / 1e6 / ITERS

    # --- bit-identical check on per-block partials ---
    var pa = alloc[Int64](npart1)
    var pb = alloc[Int64](npart1)
    var pa_sub = DeviceBuffer(ctx, part_interp_d.unsafe_ptr(), npart1, owning=False)
    var pb_sub = DeviceBuffer(ctx, part_ct_d.unsafe_ptr(), npart1, owning=False)
    ctx.enqueue_copy(pa, pa_sub)
    ctx.enqueue_copy(pb, pb_sub)
    ctx.synchronize()
    var q1_ok = True
    var first_mismatch = -1
    for k in range(npart1):
        if pa[k] != pb[k]:
            q1_ok = False
            if first_mismatch < 0:
                first_mismatch = k
    # also reduce both to int128 totals per metric for a human-readable check.
    var tot_a = InlineArray[Int128, M_Q1](fill=Int128(0))
    var tot_b = InlineArray[Int128, M_Q1](fill=Int128(0))
    for b in range(NBLOCKS):
        for m in range(M_Q1):
            tot_a[m] += Int128(pa[b * M_Q1 + m])
            tot_b[m] += Int128(pb[b * M_Q1 + m])

    print("=== Q1-like (8 metrics), N =", N, "===")
    print("  interpreter : ", interp_q1_ms, "ms")
    print("  comptime    : ", ct_q1_ms, "ms")
    if ct_q1_ms > 0.0:
        print("  speedup     : ", interp_q1_ms / ct_q1_ms, "x")
    print("  per-block partials BIT-IDENTICAL:", q1_ok)
    if not q1_ok:
        print("  FIRST MISMATCH at index", first_mismatch,
              "A=", pa[first_mismatch], "B=", pb[first_mismatch])
    for m in range(M_Q1):
        if tot_a[m] != tot_b[m]:
            print("  metric", m, "total MISMATCH A=", tot_a[m], "B=", tot_b[m])

    # =====================================================================
    # Q6-like, single metric (bandwidth-bound).
    # =====================================================================
    var q6 = build_q6_progs()
    var q6_total_ops = len(q6.prog) // 3
    var mp6_d = ctx.enqueue_create_buffer[DType.int64](q6_total_ops * 3)
    ctx.enqueue_copy(mp6_d, q6.prog.unsafe_ptr())
    var moff6_d = ctx.enqueue_create_buffer[DType.int64](M_Q6)
    ctx.enqueue_copy(moff6_d, q6.offs.unsafe_ptr())
    var mlen6_d = ctx.enqueue_create_buffer[DType.int64](M_Q6)
    ctx.enqueue_copy(mlen6_d, q6.lens.unsafe_ptr())
    var npart6 = NBLOCKS * M_Q6
    var p6_interp_d = ctx.enqueue_create_buffer[DType.int64](npart6)
    var p6_ct_d = ctx.enqueue_create_buffer[DType.int64](npart6)
    ctx.synchronize()

    # --- (A) interpreter, Q6 ---
    for _ in range(WARMUP):
        ctx.enqueue_function[interp_kernel](
            cols_d, N, pass_d, pass_len, mp6_d, moff6_d, mlen6_d, M_Q6,
            dims_d, doff_d, p6_interp_d,
            grid_dim=NBLOCKS, block_dim=WARP,
        )
    ctx.synchronize()
    var t2 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[interp_kernel](
            cols_d, N, pass_d, pass_len, mp6_d, moff6_d, mlen6_d, M_Q6,
            dims_d, doff_d, p6_interp_d,
            grid_dim=NBLOCKS, block_dim=WARP,
        )
    ctx.synchronize()
    var interp_q6_ms = Float64(perf_counter_ns() - t2) / 1e6 / ITERS

    # --- (B) comptime, Q6 ---
    for _ in range(WARMUP):
        ctx.enqueue_function[comptime_kernel_q6](
            cols_d, N, p6_ct_d, grid_dim=NBLOCKS, block_dim=WARP
        )
    ctx.synchronize()
    var t3 = perf_counter_ns()
    for _ in range(ITERS):
        ctx.enqueue_function[comptime_kernel_q6](
            cols_d, N, p6_ct_d, grid_dim=NBLOCKS, block_dim=WARP
        )
    ctx.synchronize()
    var ct_q6_ms = Float64(perf_counter_ns() - t3) / 1e6 / ITERS

    var p6a = alloc[Int64](npart6)
    var p6b = alloc[Int64](npart6)
    var p6a_sub = DeviceBuffer(ctx, p6_interp_d.unsafe_ptr(), npart6, owning=False)
    var p6b_sub = DeviceBuffer(ctx, p6_ct_d.unsafe_ptr(), npart6, owning=False)
    ctx.enqueue_copy(p6a, p6a_sub)
    ctx.enqueue_copy(p6b, p6b_sub)
    ctx.synchronize()
    var q6_ok = True
    var q6_first = -1
    for k in range(npart6):
        if p6a[k] != p6b[k]:
            q6_ok = False
            if q6_first < 0:
                q6_first = k

    print("=== Q6-like (1 metric), N =", N, "===")
    print("  interpreter : ", interp_q6_ms, "ms")
    print("  comptime    : ", ct_q6_ms, "ms")
    if ct_q6_ms > 0.0:
        print("  speedup     : ", interp_q6_ms / ct_q6_ms, "x")
    print("  per-block partials BIT-IDENTICAL:", q6_ok)
    if not q6_ok:
        print("  FIRST MISMATCH at index", q6_first,
              "A=", p6a[q6_first], "B=", p6b[q6_first])

    # ---- hard fail if either correctness check failed ----
    if not q1_ok or not q6_ok:
        raise Error("CORRECTNESS FAILURE: comptime kernel != interpreter")
    print()
    print("ALL CORRECTNESS CHECKS PASSED (comptime == interpreter, bit-exact)")

    qty.free(); ext.free(); disc.free(); tax.free(); ship.free()
    cols_h.free(); pa.free(); pb.free(); p6a.free(); p6b.free()
