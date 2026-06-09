"""De-risk the TPC-H Q3 GPU *segmented-reduction* group-by (bit-exact vs CPU).

This replaces the host-side per-order sum (the O(n_rows) loop in q3_kernel_test /
mojo_q3_query) with a real on-GPU high-cardinality group-by using the standard
sort + segmented-reduce technique (no int64 atomics -- Apple GPU lacks them):

  1. Input is SORTED by l_orderkey (DuckDB does this at pin time via ORDER BY).
     All rows of one order are contiguous.
  2. The host computes, from the sorted l_orderkey array, the list of distinct
     orderkeys (seg_key[s]) and a seg_offset[] array: seg_offset[s] = first row
     of segment s, seg_offset[n_seg] = n_rows. A single linear pass.
  3. GPU segmented reduction: ONE WARP per order segment. Each warp's 32 lanes
     stride over its contiguous rows [seg_offset[s], seg_offset[s+1]); each lane
     applies the per-row l_shipdate > cutoff filter and the per-segment
     order_pass[seg_key[s]] test, computes rev = ext*(100-disc) (scale-4 int64),
     warp.sum reduces the 32 partials, lane 0 writes seg_rev[s]. No atomics, no
     cross-block merge -- each segment is owned by exactly one warp.
  4. Host: seg_rev[s] (one int64 per order, ~1.5M) maps back via seg_key[s].

Exactness: ext is DECIMAL(15,2)=int64 scale2; (1-disc)=(100-disc_raw) scale2;
per-row product scale4 int64 (~1e9). An order has <=7 lines so per-order revenue
fits int64. We compare the full per-segment revenue array bit-for-bit against a
CPU int128-accumulated reference (int128 on the CPU side purely as a paranoia
check that int64 never overflows).
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N_ORDERS = 1_500_000   # ~ TPC-H SF1 orders rows (distinct orderkeys)
comptime N_PROBE = 6_000_000    # ~ TPC-H SF1 lineitem rows


# ---------------------------------------------------------------------------
# Segmented reduction kernel: one warp (block_dim=32) per order segment.
# grid_dim = n_seg. Each warp sums revenue over its contiguous row range.
# ---------------------------------------------------------------------------
def q3_seg_kernel(
    seg_offset: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],  # n_seg+1
    seg_key: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],     # n_seg
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    seg_rev: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],     # n_seg out
    n_seg: Int,
    ship_cutoff: Int32,   # l_shipdate > ship_cutoff (strict)
):
    var s = Int(block_idx.x)
    if s >= n_seg:
        return
    var lane = Int(thread_idx.x)
    # order_pass checked once per segment (constant across the segment's rows).
    if order_pass[Int(seg_key[s])] == 0:
        if lane == 0:
            seg_rev[s] = 0
        return
    var lo = Int(seg_offset[s])
    var hi = Int(seg_offset[s + 1])
    var local = Int64(0)
    var i = lo + lane
    while i < hi:
        if ship[i] > ship_cutoff:
            local += ext[i] * (Int64(100) - disc[i])  # scale 4
        i += 32
    var tot = warp.sum(local)
    if lane == 0:
        seg_rev[s] = tot


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ===== 1. synthetic dimensions (dense order metadata) =====
    var max_orderkey = N_ORDERS
    var n_slots = max_orderkey + 1
    var order_pass = alloc[UInt8](n_slots)
    for k in range(n_slots):
        order_pass[k] = 0
    var o_cutoff = Int32(9000)
    for k in range(1, n_slots):
        var od = Int32(7000 + (k * 2654435761) % 4000)  # 7000..10999
        var is_building = (k * 40503) % 2 == 0
        if is_building and od < o_cutoff:
            order_pass[k] = 1

    # ===== 2. synthetic lineitem, SORTED by l_orderkey =====
    # Mirror DuckDB's ORDER BY l_orderkey at pin time: rows are grouped by order.
    # Distinct orderkeys 1..max_orderkey (some orders absent to mimic sparsity),
    # 1..7 lines each, contiguous. We build the columns in sorted order directly.
    var lorderkey = alloc[Int64](N_PROBE)
    var ship = alloc[Int32](N_PROBE)
    var ext = alloc[Int64](N_PROBE)
    var disc = alloc[Int64](N_PROBE)

    # Build segments: walk orderkeys, assign a line count, fill rows, until full.
    var seg_key_tmp = alloc[Int64](N_PROBE)   # over-sized; trimmed to n_seg
    var seg_off_tmp = alloc[Int64](N_PROBE + 1)
    var row = 0
    var n_seg = 0
    var ok = 1
    while row < N_PROBE and ok <= max_orderkey:
        var nlines = 1 + (ok * 2654435761) % 7   # 1..7 lines
        seg_key_tmp[n_seg] = Int64(ok)
        seg_off_tmp[n_seg] = Int64(row)
        for _l in range(nlines):
            if row >= N_PROBE:
                break
            lorderkey[row] = Int64(ok)
            ship[row] = Int32(8000 + (row * 1103515245 + 12345) % 2000)  # 8000..9999
            ext[row] = Int64(100 + (row * 16807) % 9_999_900)            # up to ~1e7 scale2
            disc[row] = Int64((row * 48271) % 11)                        # 0..10 scale2
            row += 1
        n_seg += 1
        ok += 1
    var n_rows = row
    seg_off_tmp[n_seg] = Int64(n_rows)

    # Trim to exact sizes.
    var seg_key = alloc[Int64](n_seg)
    var seg_off = alloc[Int64](n_seg + 1)
    for s in range(n_seg):
        seg_key[s] = seg_key_tmp[s]
        seg_off[s] = seg_off_tmp[s]
    seg_off[n_seg] = seg_off_tmp[n_seg]
    seg_key_tmp.free(); seg_off_tmp.free()

    var ship_cutoff = Int32(9000)  # l_shipdate > 9000 -> ~half the rows pass

    # ===== 3. CPU reference (int128 accumulate per segment) =====
    var cpu_seg = alloc[Int64](n_seg)
    var t0 = perf_counter_ns()
    for s in range(n_seg):
        var acc = Int128(0)
        if order_pass[Int(seg_key[s])] != 0:
            var lo = Int(seg_off[s])
            var hi = Int(seg_off[s + 1])
            for i in range(lo, hi):
                if ship[i] > ship_cutoff:
                    acc += Int128(ext[i]) * Int128(Int64(100) - disc[i])
        # assert fits int64
        cpu_seg[s] = acc.cast[DType.int64]()
        if Int128(cpu_seg[s]) != acc:
            print("!! per-order revenue overflowed int64 at seg", s)
    var cpu_ns = perf_counter_ns() - t0

    # ===== 4. GPU segmented reduction =====
    var op_d = ctx.enqueue_create_buffer[DType.uint8](n_slots)
    var soff_d = ctx.enqueue_create_buffer[DType.int64](n_seg + 1)
    var skey_d = ctx.enqueue_create_buffer[DType.int64](n_seg)
    var ship_d = ctx.enqueue_create_buffer[DType.int32](n_rows)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](n_rows)
    var srev_d = ctx.enqueue_create_buffer[DType.int64](n_seg)
    ctx.synchronize()
    ctx.enqueue_copy(op_d, order_pass)
    ctx.enqueue_copy(soff_d, seg_off)
    ctx.enqueue_copy(skey_d, seg_key)
    ctx.enqueue_copy(ship_d, ship)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(disc_d, disc)
    ctx.synchronize()

    var gpu_seg = alloc[Int64](n_seg)
    # warm-up (the first launch pays one-time pipeline-state creation)
    ctx.enqueue_function[q3_seg_kernel](
        soff_d, skey_d, op_d, ship_d, ext_d, disc_d, srev_d, n_seg, ship_cutoff,
        grid_dim=n_seg, block_dim=32,
    )
    ctx.synchronize()
    var tg = perf_counter_ns()
    ctx.enqueue_function[q3_seg_kernel](
        soff_d, skey_d, op_d, ship_d, ext_d, disc_d, srev_d, n_seg, ship_cutoff,
        grid_dim=n_seg, block_dim=32,
    )
    ctx.enqueue_copy(gpu_seg, srev_d)
    ctx.synchronize()
    var gpu_ns = perf_counter_ns() - tg

    # ===== 5. compare per-segment revenue bit-for-bit =====
    var mismatches = 0
    var nonzero = 0
    var multi_line_nonzero = 0
    var filtered_to_zero = 0
    for s in range(n_seg):
        var lines = Int(seg_off[s + 1] - seg_off[s])
        if cpu_seg[s] != 0:
            nonzero += 1
            if lines > 1:
                multi_line_nonzero += 1
        elif order_pass[Int(seg_key[s])] != 0:
            filtered_to_zero += 1   # passing order but all rows shipdate-filtered
        if cpu_seg[s] != gpu_seg[s]:
            mismatches += 1
    var exact = mismatches == 0

    print("==== Q3 segmented-reduction group-by ====")
    print("segments(orders):", n_seg, " probe rows:", n_rows)
    print("CPU ref (", Float64(cpu_ns) / 1e6, "ms ), GPU seg-reduce (",
          Float64(gpu_ns) / 1e6, "ms incl copies )")
    print("nonzero orders:", nonzero, " (multi-line nonzero:", multi_line_nonzero,
          "); orders passing but all-rows-filtered->0:", filtered_to_zero)
    print("per-segment mismatches:", mismatches)
    print("EXACT match (full per-segment revenue):", exact)

    order_pass.free()
    lorderkey.free(); ship.free(); ext.free(); disc.free()
    seg_key.free(); seg_off.free()
    cpu_seg.free(); gpu_seg.free()
