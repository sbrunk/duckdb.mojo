"""De-risk the TPC-H Q5 GPU multi-table-join grouped-aggregation kernel.

Q5 is a 6-table join (customer, orders, lineitem, supplier, nation, region) with
a correlated join condition (c_nationkey == s_nationkey) and a small-cardinality
group-by on the nation:

    SELECT n_name, sum(l_extendedprice*(1-l_discount)) AS revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey AND r_name = 'ASIA'
      AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'
    GROUP BY n_name ORDER BY revenue DESC;

Design (build-on-host dimension lookups + GPU probe over lineitem +
per-block-partials group-by, NO GPU atomics -- the Apple GPU lacks int64
Atomic.fetch_add, found in the Q3 work):

  * HOST collapses the 5 dimension joins into dense arrays indexed by key:
    - order_pass[o_orderkey]        : uint8  (o_orderdate in [lo, hi))
    - order_cust_nation[o_orderkey] : int32  (folds customer lookup:
                                       c_nationkey[o_custkey])
    - supp_nation[l_suppkey]        : int32  (s_nationkey)
    - nation_in_asia[nationkey]     : uint8  (region of nation == ASIA)
  * GPU probe over lineitem: for each row, if order_pass[l_orderkey]:
        cn = order_cust_nation[l_orderkey]
        sn = supp_nation[l_suppkey]
        if cn == sn  (the correlated condition)  AND nation_in_asia[sn]:
            rev = ext_raw * (100 - disc_raw)   (scale-4 int64)
            accumulate rev into group = sn  via per-block partials.
  * Each block keeps private [NGROUPS] int64 lane accumulators (group=nation),
    warp.sum reduces each group across the 32 lanes, lane 0 writes the per-block
    partial. The HOST reduces partials across blocks in int128 -> exact.

Exactness: ext is DECIMAL(15,2)=int64 scale2, (1-disc)=(100-disc_raw) scale2;
per-row product is scale-4 int64 (~1e9). At SF1 a nation's revenue ~5.5e7 (scale
4 -> ~5.5e11) fits int64, but we reduce across blocks in int128 to stay exact for
any scale. This test asserts the GPU per-nation revenue is bit-exact vs a CPU
int128 reference across all 25 groups.
"""

from std.gpu import block_idx, thread_idx
from std.gpu.primitives import warp
from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N_ORDERS = 1_500_000   # ~ TPC-H SF1 orders rows
comptime N_SUPP = 10_000        # ~ TPC-H SF1 supplier rows
comptime N_PROBE = 6_000_000    # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096         # one warp (32 lanes) per block
comptime NGROUPS = 25           # n_nationkey range 0..24


def q5_kernel(
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    order_cust_nation: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    supp_nation: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    nation_in_asia: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    lorderkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    lsuppkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    partials: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * 32
    # Private per-lane per-group (nation) revenue accumulators.
    var acc = InlineArray[Int64, NGROUPS](fill=0)
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var ok = lorderkey[i]
        if order_pass[Int(ok)] != 0:
            var cn = order_cust_nation[Int(ok)]
            var sn = supp_nation[Int(lsuppkey[i])]
            if cn == sn and nation_in_asia[Int(sn)] != 0:
                acc[Int(sn)] += ext[i] * (Int64(100) - disc[i])  # scale 4
        i += stride
    var blk = Int(block_idx.x)
    for g in range(NGROUPS):
        var s = warp.sum(acc[g])
        if lane == 0:
            partials[blk * NGROUPS + g] = s


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ===== 1. synthetic dimensions (dense by key) =====
    var max_orderkey = N_ORDERS
    var n_oslots = max_orderkey + 1
    var order_pass = alloc[UInt8](n_oslots)
    var order_cust_nation = alloc[Int32](n_oslots)
    for k in range(n_oslots):
        order_pass[k] = 0
        order_cust_nation[k] = 0
    # ~ half the orders are in the date window -> pass; assign each order's
    # customer a nation in 0..24.
    for k in range(1, n_oslots):
        var in_window = (k * 40503) % 2 == 0
        order_pass[k] = 1 if in_window else 0
        order_cust_nation[k] = Int32((k * 2654435761) % NGROUPS)
    # Force orders 1..200000 to pass with an ASIA customer nation, so the steered
    # probe rows below land on a passing+ASIA order and exercise the match branch.
    for k in range(1, 200001):
        order_pass[k] = 1
        order_cust_nation[k] = Int32(5 * ((k * 7) % 5))  # 0,5,10,15,20

    var max_suppkey = N_SUPP
    var n_sslots = max_suppkey + 1
    var supp_nation = alloc[Int32](n_sslots)
    for k in range(n_sslots):
        supp_nation[k] = Int32((k * 16807) % NGROUPS)

    # nation_in_asia: pick ~5 of the 25 nations as "ASIA" (like real Q5).
    var nation_in_asia = alloc[UInt8](NGROUPS)
    for g in range(NGROUPS):
        nation_in_asia[g] = 1 if (g % 5 == 0) else 0  # nations 0,5,10,15,20

    # ===== 2. synthetic lineitem (probe side) =====
    var lorderkey = alloc[Int64](N_PROBE)
    var lsuppkey = alloc[Int64](N_PROBE)
    var ext = alloc[Int64](N_PROBE)
    var disc = alloc[Int64](N_PROBE)
    # To exercise the correlated condition (cn == sn) + ASIA filter for a
    # meaningful fraction of rows, steer ~1/8 of rows to a supplier whose nation
    # matches their order's customer nation when that nation is ASIA. The kernel
    # still independently recomputes cn/sn and the equality test; this just makes
    # the matching branch non-empty so the comparison is real.
    for i in range(N_PROBE):
        var sk = Int64(1 + (i * 48271) % max_suppkey)
        if (i & 7) == 0:
            # steer to a passing+ASIA order (key in 1..200000) and a supplier in
            # the same nation, so the correlated condition + ASIA filter hit.
            var ok = Int64(1 + (i * 16807) % 200000)
            lorderkey[i] = ok
            var cn = order_cust_nation[Int(ok)]
            var found = Int64(sk)
            for cand in range(1, 64):
                if Int32((Int64(cand) * 16807) % NGROUPS) == cn:
                    found = Int64(cand)
                    break
            sk = found
        else:
            lorderkey[i] = Int64(1 + (i * 16807) % max_orderkey)  # 1..max_orderkey
        lsuppkey[i] = sk
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)          # up to ~1e7 scale2
        disc[i] = Int64((i * 48271) % 11)                      # 0..10 scale2

    # ===== 3. CPU int128 reference (per-nation revenue) =====
    var t0 = perf_counter_ns()
    var cpu = InlineArray[Int128, NGROUPS](fill=Int128(0))
    for i in range(N_PROBE):
        var ok = Int(lorderkey[i])
        if order_pass[ok] != 0:
            var cn = order_cust_nation[ok]
            var sn = supp_nation[Int(lsuppkey[i])]
            if cn == sn and nation_in_asia[Int(sn)] != 0:
                cpu[Int(sn)] += Int128(ext[i]) * (Int128(100) - Int128(disc[i]))
    var cpu_ns = perf_counter_ns() - t0

    # ===== 4. GPU =====
    var op_d = ctx.enqueue_create_buffer[DType.uint8](n_oslots)
    var ocn_d = ctx.enqueue_create_buffer[DType.int32](n_oslots)
    var sn_d = ctx.enqueue_create_buffer[DType.int32](n_sslots)
    var asia_d = ctx.enqueue_create_buffer[DType.uint8](NGROUPS)
    var lok_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var lsk_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var part_d = ctx.enqueue_create_buffer[DType.int64](NBLOCKS * NGROUPS)
    ctx.synchronize()
    ctx.enqueue_copy(op_d, order_pass)
    ctx.enqueue_copy(ocn_d, order_cust_nation)
    ctx.enqueue_copy(sn_d, supp_nation)
    ctx.enqueue_copy(asia_d, nation_in_asia)
    ctx.enqueue_copy(lok_d, lorderkey)
    ctx.enqueue_copy(lsk_d, lsuppkey)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(disc_d, disc)
    ctx.synchronize()

    var part_h = alloc[Int64](NBLOCKS * NGROUPS)
    var tg = perf_counter_ns()
    ctx.enqueue_function[q5_kernel](
        op_d, ocn_d, sn_d, asia_d, lok_d, lsk_d, ext_d, disc_d, part_d, N_PROBE,
        grid_dim=NBLOCKS, block_dim=32,
    )
    var part_sub = DeviceBuffer(
        ctx, part_d.unsafe_ptr(), NBLOCKS * NGROUPS, owning=False
    )
    ctx.enqueue_copy(part_h, part_sub)
    ctx.synchronize()
    # host int128 reduction across blocks, per group (nation).
    var gpu = InlineArray[Int128, NGROUPS](fill=Int128(0))
    for b in range(NBLOCKS):
        for g in range(NGROUPS):
            gpu[g] += Int128(part_h[b * NGROUPS + g])
    var gpu_ns = perf_counter_ns() - tg

    # ===== 5. compare bit-for-bit across all 25 groups =====
    print("==== Q5 multi-join grouped aggregation ====")
    print("orders:", N_ORDERS, " suppliers:", N_SUPP, " probe rows:", N_PROBE,
          " groups(nations):", NGROUPS)
    print("CPU ref (", Float64(cpu_ns) / 1e6, "ms ), GPU (",
          Float64(gpu_ns) / 1e6, "ms incl host reduce )")
    var all_ok = True
    var nonzero = 0
    for g in range(NGROUPS):
        var c = cpu[g]
        var v = gpu[g]
        if c != Int128(0):
            nonzero += 1
        if c != v:
            all_ok = False
            print("  MISMATCH nation", g, "cpu", c, "gpu", v)
    print("nations with revenue:", nonzero, " (ASIA nations: 0,5,10,15,20)")
    print("EXACT match all nations:", all_ok)
    for g in range(NGROUPS):
        if gpu[g] != Int128(0):
            print("  nation", g, "revenue(scale4)", gpu[g])

    order_pass.free(); order_cust_nation.free(); supp_nation.free()
    nation_in_asia.free()
    lorderkey.free(); lsuppkey.free(); ext.free(); disc.free(); part_h.free()
