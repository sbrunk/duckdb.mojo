"""De-risk the TPC-H Q1 GPU grouped-aggregation kernel (bit-exact vs CPU int128).

DuckDB stores l_quantity/l_extendedprice/l_discount/l_tax as DECIMAL(15,2) = int64
(scale 2) and l_shipdate as DATE = int32 (days). l_returnflag/l_linestatus are a
single char each; the host pre-assigns a dense group id (0..NGROUPS-1).

The kernel accumulates, per group, 6 integer quantities (all exact):
    [0] count
    [1] sum(l_quantity)                          scale 2
    [2] sum(l_extendedprice)                     scale 2
    [3] sum(l_discount)                          scale 2
    [4] sum(ext_raw * (100 - disc_raw))          scale 4  (sum_disc_price)
    [5] sum(ext_raw*(100-disc_raw)*(100+tax_raw)) scale 6  (sum_charge)

Per-row magnitudes: sum_charge per row ~ 1e7 * 100 * 108 ~ 1.1e11; a per-block
partial over ~1500 rows ~ 1.7e14 — fits int64. Only the cross-block reduction
needs int128 (done on the host), so the kernel is pure-integer and EXACT.

Kernel design: one warp (32 lanes) per block. Each lane keeps private int64
accumulators [NGROUPS][6], lane-strided over rows; then warp.sum reduces each
(group, metric) across the 32 lanes and lane 0 writes the per-block partials to
global memory. No shared-memory atomics (portable to the Apple GPU).
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N = 6_000_000     # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096    # one warp (32 lanes) per block
comptime NGROUPS = 8       # cap; Q1 uses 4
comptime NMETRICS = 6      # count, Sqty, Sext, Sdisc, Sdisc_price(s4), Scharge(s6)


def q1_kernel(
    gid: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    qty: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    tax: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_hi: Int32,  # filter: l_shipdate <= ship_hi  (inclusive)
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * 32
    # Private per-lane accumulators [NGROUPS][NMETRICS].
    var acc = InlineArray[Int64, NGROUPS * NMETRICS](fill=0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd <= ship_hi:
            var g = Int(gid[i])
            var e = ext[i]
            var d = disc[i]
            var t = tax[i]
            var one_minus_d = Int64(100) - d
            var ed = e * one_minus_d            # scale 4
            var charge = ed * (Int64(100) + t)  # scale 6
            var b = g * NMETRICS
            acc[b + 0] += 1
            acc[b + 1] += qty[i]
            acc[b + 2] += e
            acc[b + 3] += d
            acc[b + 4] += ed
            acc[b + 5] += charge
        i += stride
    # Reduce each (group, metric) across the 32 lanes; lane 0 writes partials.
    var blk = Int(block_idx.x)
    for g in range(NGROUPS):
        for m in range(NMETRICS):
            var s = warp.sum(acc[g * NMETRICS + m])
            if lane == 0:
                # partials layout: [block][group][metric]
                partials[(blk * NGROUPS + g) * NMETRICS + m] = s


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    comptime NG = 4  # synthetic groups in use

    # ---- synthetic lineitem-like columns ----
    var gid = alloc[UInt8](N)
    var qty = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var disc = alloc[Int64](N)
    var tax = alloc[Int64](N)
    var ship = alloc[Int32](N)
    for i in range(N):
        gid[i] = UInt8((i * 2654435761) % NG)
        qty[i] = Int64(100 + (i * 22695477) % 4900)        # 1.00..50.00 scale2
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)      # up to ~1e7 scale2
        disc[i] = Int64((i * 48271) % 11)                  # 0..10 scale2 (0.00..0.10)
        tax[i] = Int64((i * 69069) % 9)                    # 0..8 scale2
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)

    var ship_hi = Int32(9131)

    # ---- CPU int128 reference ----
    var t0 = perf_counter_ns()
    var cpu = InlineArray[Int128, NG * NMETRICS](fill=Int128(0))
    for i in range(N):
        if ship[i] <= ship_hi:
            var g = Int(gid[i])
            var e = Int128(ext[i])
            var d = Int128(disc[i])
            var t = Int128(tax[i])
            var ed = e * (Int128(100) - d)
            var charge = ed * (Int128(100) + t)
            var b = g * NMETRICS
            cpu[b + 0] += Int128(1)
            cpu[b + 1] += Int128(qty[i])
            cpu[b + 2] += e
            cpu[b + 3] += d
            cpu[b + 4] += ed
            cpu[b + 5] += charge
    var cpu_ns = perf_counter_ns() - t0

    # ---- GPU ----
    var gid_d = ctx.enqueue_create_buffer[DType.uint8](N)
    var qty_d = ctx.enqueue_create_buffer[DType.int64](N)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](N)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](N)
    var tax_d = ctx.enqueue_create_buffer[DType.int64](N)
    var ship_d = ctx.enqueue_create_buffer[DType.int32](N)
    var part_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS * NGROUPS * NMETRICS)
    ctx.synchronize()
    ctx.enqueue_copy(gid_d, gid)
    ctx.enqueue_copy(qty_d, qty)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(disc_d, disc)
    ctx.enqueue_copy(tax_d, tax)
    ctx.enqueue_copy(ship_d, ship)
    ctx.synchronize()

    var part_h = alloc[Int64](NBLOCKS * NGROUPS * NMETRICS)
    var tg = perf_counter_ns()
    ctx.enqueue_function[q1_kernel](
        gid_d, qty_d, ext_d, disc_d, tax_d, ship_d, part_d, N, ship_hi,
        grid_dim=NBLOCKS, block_dim=32,
    )
    var part_sub = DeviceBuffer(
        ctx, part_d.unsafe_ptr(), NBLOCKS * NGROUPS * NMETRICS, owning=False
    )
    ctx.enqueue_copy(part_h, part_sub)
    ctx.synchronize()
    # host int128 reduction across blocks
    var gpu = InlineArray[Int128, NGROUPS * NMETRICS](fill=Int128(0))
    for b in range(NBLOCKS):
        for g in range(NGROUPS):
            for m in range(NMETRICS):
                gpu[g * NMETRICS + m] += Int128(
                    part_h[(b * NGROUPS + g) * NMETRICS + m]
                )
    var gpu_ns = perf_counter_ns() - tg

    print("rows:", N, " groups:", NG)
    print("CPU ref (", Float64(cpu_ns) / 1e6, "ms ), GPU (", Float64(gpu_ns) / 1e6, "ms incl host reduce )")
    var all_ok = True
    for g in range(NG):
        for m in range(NMETRICS):
            var c = cpu[g * NMETRICS + m]
            var v = gpu[g * NMETRICS + m]
            if c != v:
                all_ok = False
                print("  MISMATCH g", g, "metric", m, "cpu", c, "gpu", v)
    print("metrics: [0]count [1]Sqty [2]Sext [3]Sdisc [4]Sdisc_price(s4) [5]Scharge(s6)")
    print("EXACT match all groups/metrics:", all_ok)
    # Spot-print group 0
    print("group0:")
    for m in range(NMETRICS):
        print("   metric", m, "=", gpu[m])

    gid.free(); qty.free(); ext.free(); disc.free(); tax.free(); ship.free(); part_h.free()
