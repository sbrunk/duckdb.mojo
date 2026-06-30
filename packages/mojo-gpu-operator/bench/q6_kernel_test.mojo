"""De-risk the TPC-H Q6 GPU kernel: filter + exact decimal sum(ext*disc).

DuckDB stores l_extendedprice/l_discount/l_quantity as DECIMAL(15,2) = int64
(scale 2) and l_shipdate as DATE = int32 (days). The Q6 sum must be EXACT to
match DuckDB, so we do integer arithmetic, not float:

  per-row product ext*disc fits int64 (~1e7 * ~10);
  per-block partial (warp.sum of int64) fits int64;
  only the final cross-block reduction needs int128 (done on the host).

This standalone test runs the kernel over synthetic lineitem-like data and
checks the GPU result is bit-exact against a CPU int128 reference.
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N = 6_000_000     # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096    # one warp (32 lanes) per block


def q6_kernel(
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    qty: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_lo: Int32,
    ship_hi: Int32,
    disc_lo: Int64,
    disc_hi: Int64,
    qty_hi: Int64,
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * 32
    var local = Int64(0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd >= ship_lo and sd < ship_hi:
            var d = disc[i]
            if d >= disc_lo and d <= disc_hi and qty[i] < qty_hi:
                local += ext[i] * d
        i += stride
    var s = warp.sum(local)
    if lane == 0:
        partials[Int(block_idx.x)] = s


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ---- synthetic lineitem-like columns ----
    var ship = alloc[Int32](N)
    var disc = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var qty = alloc[Int64](N)
    for i in range(N):
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)  # day number
        disc[i] = Int64((i * 48271) % 11)                        # 0..10  (0.00..0.10 scale2)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)            # ~ up to 1e7 (price scale2)
        qty[i] = Int64(1 + (i * 22695477) % 50)                  # 1..50  (scale2-ish, simplified)

    # Q6-like predicate constants
    var ship_lo = Int32(8766)
    var ship_hi = Int32(9131)
    var disc_lo = Int64(5)
    var disc_hi = Int64(7)
    var qty_hi = Int64(24)

    # ---- CPU int128 reference ----
    var t0 = perf_counter_ns()
    var cpu = Int128(0)
    for i in range(N):
        if ship[i] >= ship_lo and ship[i] < ship_hi:
            if disc[i] >= disc_lo and disc[i] <= disc_hi and qty[i] < qty_hi:
                cpu += Int128(ext[i]) * Int128(disc[i])
    var cpu_ns = perf_counter_ns() - t0

    # ---- GPU ----
    var ship_d = ctx.enqueue_create_buffer[DType.int32](N)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](N)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](N)
    var qty_d = ctx.enqueue_create_buffer[DType.int64](N)
    var part_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS)
    ctx.synchronize()
    ctx.enqueue_copy(ship_d, ship)
    ctx.enqueue_copy(disc_d, disc)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(qty_d, qty)
    ctx.synchronize()

    var part_h = alloc[Int64](NBLOCKS)
    var tg = perf_counter_ns()
    ctx.enqueue_function[q6_kernel](
        ship_d, disc_d, ext_d, qty_d, part_d, N,
        ship_lo, ship_hi, disc_lo, disc_hi, qty_hi,
        grid_dim=NBLOCKS, block_dim=32,
    )
    var part_sub = DeviceBuffer(ctx, part_d.unsafe_ptr(), NBLOCKS, owning=False)
    ctx.enqueue_copy(part_h, part_sub)
    ctx.synchronize()
    # host int128 reduction of the block partials
    var gpu = Int128(0)
    for b in range(NBLOCKS):
        gpu += Int128(part_h[b])
    var gpu_ns = perf_counter_ns() - tg

    print("rows:", N)
    print("CPU int128 sum:", cpu, "  (", Float64(cpu_ns) / 1e6, "ms )")
    print("GPU int128 sum:", gpu, "  (", Float64(gpu_ns) / 1e6, "ms, incl. host reduce )")
    print("EXACT match:", cpu == gpu)

    ship.free(); disc.free(); ext.free(); qty.free(); part_h.free()
