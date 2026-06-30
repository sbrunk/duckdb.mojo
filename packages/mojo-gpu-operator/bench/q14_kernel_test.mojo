"""De-risk the TPC-H Q14 GPU hash-probe join kernel (bit-exact vs CPU int128).

Q14 is a FK join lineitem -> part (p_partkey unique) + a probe-side aggregation:
    promo_revenue = 100 * sum(CASE WHEN p_type LIKE 'PROMO%'
                                   THEN l_extendedprice*(1-l_discount) ELSE 0 END)
                        / sum(l_extendedprice*(1-l_discount))
with the probe restricted to l_shipdate in [lo, hi).

Build-small / probe-big hash join:
  * Build an open-addressing (linear-probing) hash table on the HOST keyed by
    p_partkey (int64) with a 1-byte payload is_promo (p_type starts "PROMO").
    Table size = next pow2 >= 2 * build_rows; empty slot = key 0 (TPC-H partkeys
    start at 1, so 0 is a safe sentinel).
  * Upload keys[] + promo[] to the GPU once.
  * Probe kernel: each thread strides over filtered lineitem rows; for a passing
    row (shipdate in [lo,hi)), hash l_partkey, linear-probe the resident table,
    read is_promo, compute prod = ext_raw * (100 - disc_raw)  (scale 4, int64),
    add to a local total; if promo add to a local promo. warp.sum both; per-block
    partials; host reduces in int128 -> EXACT (matches DuckDB's int128 sum).

Exactness: ext is DECIMAL(15,2)=int64 scale2; (1-disc) computed as (100-disc_raw)
with disc DECIMAL(15,2) scale2 -> (100-disc_raw) is scale2; product is scale4 int64.
Per-row product ~ 1e7 * 100 = 1e9; per-block partial over ~6M/4096 rows fits int64.
Only the cross-block reduction needs int128 (host).
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N_BUILD = 200_000     # ~ TPC-H SF1 part rows
comptime N_PROBE = 6_000_000   # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096        # one warp (32 lanes) per block


# Splitmix-ish 64-bit integer hash for the partkey.
def hash_key(k: Int64) -> UInt64:
    var x = UInt64(k)
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x


def q14_kernel(
    ht_keys: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ht_promo: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    ht_mask: UInt64,
    lpartkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_total: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_promo: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    part_miss: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_lo: Int32,
    ship_hi: Int32,
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * 32
    var total = Int64(0)
    var promo = Int64(0)
    var miss = Int64(0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var sd = ship[i]
        if sd >= ship_lo and sd < ship_hi:
            var key = lpartkey[i]
            var h = hash_key(key) & ht_mask
            var found = False
            var is_promo = UInt8(0)
            while ht_keys[Int(h)] != 0:
                if ht_keys[Int(h)] == key:
                    is_promo = ht_promo[Int(h)]
                    found = True
                    break
                h = (h + 1) & ht_mask
            if found:
                var prod = ext[i] * (Int64(100) - disc[i])  # scale 4
                total += prod
                if is_promo != 0:
                    promo += prod
            else:
                miss += 1
        i += stride
    var st = warp.sum(total)
    var sp = warp.sum(promo)
    var sm = warp.sum(miss)
    if lane == 0:
        part_total[Int(block_idx.x)] = st
        part_promo[Int(block_idx.x)] = sp
        part_miss[Int(block_idx.x)] = sm


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ---- synthetic part (build side): distinct keys 1..N_BUILD, random promo ----
    var build_keys = alloc[Int64](N_BUILD)
    var build_promo = alloc[UInt8](N_BUILD)
    for i in range(N_BUILD):
        build_keys[i] = Int64(i + 1)                       # 1..N_BUILD (dense, unique)
        build_promo[i] = UInt8(1) if (i * 2654435761) % 5 == 0 else UInt8(0)

    # ---- host open-addressing hash table (linear probing) ----
    var ht_size = 1
    while ht_size < 2 * N_BUILD:
        ht_size *= 2
    var ht_mask = UInt64(ht_size - 1)
    var ht_keys = alloc[Int64](ht_size)
    var ht_promo = alloc[UInt8](ht_size)
    for i in range(ht_size):
        ht_keys[i] = 0
        ht_promo[i] = 0
    for i in range(N_BUILD):
        var key = build_keys[i]
        var h = hash_key(key) & ht_mask
        while ht_keys[Int(h)] != 0:
            h = (h + 1) & ht_mask
        ht_keys[Int(h)] = key
        ht_promo[Int(h)] = build_promo[i]

    # ---- synthetic lineitem (probe side) ----
    var lpartkey = alloc[Int64](N_PROBE)
    var ship = alloc[Int32](N_PROBE)
    var ext = alloc[Int64](N_PROBE)
    var disc = alloc[Int64](N_PROBE)
    for i in range(N_PROBE):
        lpartkey[i] = Int64(1 + (i * 16807) % N_BUILD)     # drawn from build keys
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)      # up to ~1e7 scale2
        disc[i] = Int64((i * 48271) % 11)                  # 0..10 scale2 (0.00..0.10)

    var ship_lo = Int32(8500)
    var ship_hi = Int32(8531)

    # ---- CPU int128 reference (host hash lookup + int128 accumulate) ----
    var t0 = perf_counter_ns()
    var cpu_total = Int128(0)
    var cpu_promo = Int128(0)
    var cpu_miss = 0
    for i in range(N_PROBE):
        var sd = ship[i]
        if sd >= ship_lo and sd < ship_hi:
            var key = lpartkey[i]
            var h = hash_key(key) & ht_mask
            var found = False
            var ip = UInt8(0)
            while ht_keys[Int(h)] != 0:
                if ht_keys[Int(h)] == key:
                    ip = ht_promo[Int(h)]
                    found = True
                    break
                h = (h + 1) & ht_mask
            if found:
                var prod = Int128(ext[i]) * (Int128(100) - Int128(disc[i]))
                cpu_total += prod
                if ip != 0:
                    cpu_promo += prod
            else:
                cpu_miss += 1
    var cpu_ns = perf_counter_ns() - t0

    # ---- GPU ----
    var keys_d = ctx.enqueue_create_buffer[DType.int64](ht_size)
    var promo_d = ctx.enqueue_create_buffer[DType.uint8](ht_size)
    var lpk_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var ship_d = ctx.enqueue_create_buffer[DType.int32](N_PROBE)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var pt_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS)
    var pp_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS)
    var pm_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS)
    ctx.synchronize()
    ctx.enqueue_copy(keys_d, ht_keys)
    ctx.enqueue_copy(promo_d, ht_promo)
    ctx.enqueue_copy(lpk_d, lpartkey)
    ctx.enqueue_copy(ship_d, ship)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(disc_d, disc)
    ctx.synchronize()

    var pt_h = alloc[Int64](NBLOCKS)
    var pp_h = alloc[Int64](NBLOCKS)
    var pm_h = alloc[Int64](NBLOCKS)
    var tg = perf_counter_ns()
    ctx.enqueue_function[q14_kernel](
        keys_d, promo_d, ht_mask, lpk_d, ship_d, ext_d, disc_d,
        pt_d, pp_d, pm_d, N_PROBE, ship_lo, ship_hi,
        grid_dim=NBLOCKS, block_dim=32,
    )
    var pt_sub = DeviceBuffer(ctx, pt_d.unsafe_ptr(), NBLOCKS, owning=False)
    var pp_sub = DeviceBuffer(ctx, pp_d.unsafe_ptr(), NBLOCKS, owning=False)
    var pm_sub = DeviceBuffer(ctx, pm_d.unsafe_ptr(), NBLOCKS, owning=False)
    ctx.enqueue_copy(pt_h, pt_sub)
    ctx.enqueue_copy(pp_h, pp_sub)
    ctx.enqueue_copy(pm_h, pm_sub)
    ctx.synchronize()
    var gpu_total = Int128(0)
    var gpu_promo = Int128(0)
    var gpu_miss = Int128(0)
    for b in range(NBLOCKS):
        gpu_total += Int128(pt_h[b])
        gpu_promo += Int128(pp_h[b])
        gpu_miss += Int128(pm_h[b])
    var gpu_ns = perf_counter_ns() - tg

    print("build rows:", N_BUILD, " probe rows:", N_PROBE, " ht_size:", ht_size)
    print("CPU ref (", Float64(cpu_ns) / 1e6, "ms ), GPU (", Float64(gpu_ns) / 1e6, "ms incl host reduce )")
    print("CPU  total =", cpu_total, " promo =", cpu_promo, " miss =", cpu_miss)
    print("GPU  total =", gpu_total, " promo =", gpu_promo, " miss =", gpu_miss)
    var exact = (cpu_total == gpu_total) and (cpu_promo == gpu_promo)
    var zero_miss = (cpu_miss == 0) and (gpu_miss == Int128(0))
    print("EXACT match (total & promo):", exact)
    print("ZERO probe misses (cpu & gpu):", zero_miss)
    if cpu_total != Int128(0):
        var pct = 100.0 * Float64(Int(gpu_promo)) / Float64(Int(gpu_total))
        print("promo_revenue % =", pct)

    build_keys.free(); build_promo.free(); ht_keys.free(); ht_promo.free()
    lpartkey.free(); ship.free(); ext.free(); disc.free()
    pt_h.free(); pp_h.free(); pm_h.free()
