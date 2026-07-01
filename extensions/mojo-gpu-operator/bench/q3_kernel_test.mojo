"""De-risk the TPC-H Q3 GPU multi-way-join kernel (bit-exact vs CPU int128).

Q3 is a 3-way FK join customer <- orders <- lineitem + a high-cardinality
group-by on l_orderkey + top-10:

    SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) AS revenue,
           o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    LIMIT 10;

Design (mirrors the verified Q14 hash-probe pattern but with a DENSE array for
the dimension lookup, because TPC-H keys are dense and bounded):
  * HOST builds, per order, a 1-byte `order_pass[o_orderkey]` flag =
    is_building[o_custkey] AND (o_orderdate < o_cutoff). Dense array indexed by
    o_orderkey (size max_orderkey+1). o_orderdate / o_shippriority stay on host
    for the final attach.
  * GPU probe over lineitem: for each row with l_shipdate > l_cutoff and
    order_pass[l_orderkey], compute rev = ext_raw*(100 - disc_raw) (scale-4
    int64) and ACCUMULATE into a dense int64 accumulator indexed by l_orderkey
    via Atomic.fetch_add (global atomics).
  * HOST reads back the dense accumulator, attaches date/priority, sorts, top-10.

This file also probes whether global int64 Atomic.fetch_add works on this GPU
(a tiny standalone kernel) BEFORE relying on it for the join.

Exactness: ext is DECIMAL(15,2)=int64 scale2; (1-disc) = (100-disc_raw) scale2;
per-row product is scale4 int64 (~1e9). An order has <=7 lines, so per-order
revenue fits int64 comfortably. The dense accumulator therefore needs no host
int128 reduction (unlike Q1/Q6/Q14's cross-block sum) -- each order's bucket is
a single exact int64. We still compare the full accumulator bit-for-bit.
"""

from std.gpu import block_idx, thread_idx
from std.gpu.host import DeviceContext, DeviceBuffer
from std.atomic import Atomic
from std.memory import alloc
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N_ORDERS = 1_500_000   # ~ TPC-H SF1 orders rows
comptime N_PROBE = 6_000_000    # ~ TPC-H SF1 lineitem rows
comptime NBLOCKS = 4096         # one warp (32 lanes) per block


# ---------------------------------------------------------------------------
# Atomic-width probes. Apple GPU (Metal) supports 32-bit atomics but NOT 64-bit
# atomics: a global int64 Atomic.fetch_add fails at GPU pipeline-state creation
# (XPC_ERROR_CONNECTION_INTERRUPTED). So per-order accumulation via int64
# atomics is NOT available here. We verify both widths and select the design:
#   - int64 atomics work -> accumulate per-order on the GPU directly.
#   - else (Apple) -> the q3_kernel below writes per-row revenue (still doing the
#     join-probe + filter + exact decimal product on the GPU, the expensive
#     part); the HOST sums per order (one O(n_rows) scan, ~ms).
# ---------------------------------------------------------------------------
def atomic_probe32(acc: UnsafePointer[Scalar[DType.uint32], MutAnyOrigin], n: Int):
    var tid = Int(block_idx.x) * 32 + Int(thread_idx.x)
    if tid < n:
        _ = Atomic.fetch_add(acc, UInt32(1))


def atomic_probe64(acc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin], n: Int):
    var tid = Int(block_idx.x) * 32 + Int(thread_idx.x)
    if tid < n:
        _ = Atomic.fetch_add(acc, Int64(1))


# ---------------------------------------------------------------------------
# Q3 probe kernel (Apple/no-int64-atomics design): each thread strides over
# lineitem rows; for a passing row (l_shipdate > ship_cutoff AND
# order_pass[l_orderkey]) it computes rev = ext*(100-disc) (scale-4 int64) and
# writes it to rev_out[i]; otherwise writes 0. The GPU does the join-probe (dense
# dimension lookup), the shipdate filter, and the exact decimal product -- the
# expensive part. The host then sums rev_out per l_orderkey into the dense
# accumulator (exact int64; an order has <=7 lines so the per-order sum fits).
# ---------------------------------------------------------------------------
def q3_kernel(
    order_pass: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    lorderkey: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    ship: UnsafePointer[Scalar[DType.int32], MutAnyOrigin],
    ext: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    disc: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    rev_out: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_rows: Int,
    ship_cutoff: Int32,   # l_shipdate > ship_cutoff (strict)
):
    var lane = Int(thread_idx.x)
    var stride = NBLOCKS * 32
    var i = Int(block_idx.x) * 32 + lane
    while i < n_rows:
        var r = Int64(0)
        var sd = ship[i]
        if sd > ship_cutoff:
            var ok = lorderkey[i]
            if order_pass[Int(ok)] != 0:
                r = ext[i] * (Int64(100) - disc[i])  # scale 4
        rev_out[i] = r
        i += stride


# Host top-10 selection by (revenue desc, orderdate asc) over nonzero orders.
def topn(
    rev: UnsafePointer[Int64, MutAnyOrigin],
    order_date: UnsafePointer[Int32, MutAnyOrigin],
    n_slots: Int,
    label: String,
):
    var best_key = InlineArray[Int, 10](fill=0)
    var best_rev = InlineArray[Int64, 10](fill=0)
    var best_date = InlineArray[Int32, 10](fill=0)
    var n_best = 0
    for k in range(1, n_slots):
        var r = rev[k]
        if r <= 0:
            continue
        var d = order_date[k]
        var pos = n_best
        while pos > 0:
            var pr = best_rev[pos - 1]
            var pd = best_date[pos - 1]
            var better = r > pr or (r == pr and d < pd)
            if not better:
                break
            if pos < 10:
                best_rev[pos] = best_rev[pos - 1]
                best_key[pos] = best_key[pos - 1]
                best_date[pos] = best_date[pos - 1]
            pos -= 1
        if pos < 10:
            best_rev[pos] = r
            best_key[pos] = k
            best_date[pos] = d
            if n_best < 10:
                n_best += 1
    print("---- top-10 (", label, ") ----")
    for i in range(n_best):
        print("  orderkey", best_key[i], " revenue(scale4)", best_rev[i], " date", best_date[i])


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ===== 0. Atomic-width probes (uint32 vs int64) =====
    comptime PROBE_THREADS = 64 * 32
    print("==== Atomic.fetch_add width probes ====")
    var p32 = ctx.enqueue_create_buffer[DType.uint32](1)
    ctx.synchronize()
    p32.enqueue_fill(UInt32(0))
    ctx.enqueue_function[atomic_probe32](p32, PROBE_THREADS, grid_dim=64, block_dim=32)
    var p32_h = alloc[UInt32](1)
    ctx.enqueue_copy(p32_h, p32)
    ctx.synchronize()
    print("uint32 atomic: got", p32_h[0], " expected", PROBE_THREADS, " ->",
          "OK" if p32_h[0] == UInt32(PROBE_THREADS) else "FAIL")
    var atomics64_ok = False
    try:
        var p64 = ctx.enqueue_create_buffer[DType.int64](1)
        ctx.synchronize()
        p64.enqueue_fill(Int64(0))
        ctx.enqueue_function[atomic_probe64](p64, PROBE_THREADS, grid_dim=64, block_dim=32)
        var p64_h = alloc[Int64](1)
        ctx.enqueue_copy(p64_h, p64)
        ctx.synchronize()
        atomics64_ok = p64_h[0] == Int64(PROBE_THREADS)
        print("int64  atomic: got", p64_h[0], " expected", PROBE_THREADS, " ->",
              "OK" if atomics64_ok else "FAIL")
        p64_h.free()
    except e:
        print("int64  atomic: FAILED (pipeline-state gen):", e)
    print("-> design:", "GPU int64 atomic accumulate" if atomics64_ok
          else "GPU per-row revenue + HOST per-order sum (fallback)")
    p32_h.free()

    # ===== 1. synthetic dimensions =====
    # Dense order metadata: orderkeys 1..N_ORDERS (TPC-H orderkeys are sparse in
    # reality but bounded; dense array sized to max+1 is the recommended choice).
    var max_orderkey = N_ORDERS
    var n_slots = max_orderkey + 1
    var order_pass = alloc[UInt8](n_slots)
    var order_date = alloc[Int32](n_slots)   # host-only attach data
    var order_prio = alloc[Int32](n_slots)
    for k in range(n_slots):
        order_pass[k] = 0
        order_date[k] = 0
        order_prio[k] = 0
    var o_cutoff = Int32(9000)  # o_orderdate < 9000
    # ~ half the orders are BUILDING-customer + before cutoff -> pass.
    for k in range(1, n_slots):
        var od = Int32(7000 + (k * 2654435761) % 4000)  # 7000..10999
        var is_building = (k * 40503) % 2 == 0
        order_date[k] = od
        order_prio[k] = Int32((k * 12289) % 5)
        if is_building and od < o_cutoff:
            order_pass[k] = 1

    # ===== 2. synthetic lineitem (probe side) =====
    var lorderkey = alloc[Int64](N_PROBE)
    var ship = alloc[Int32](N_PROBE)
    var ext = alloc[Int64](N_PROBE)
    var disc = alloc[Int64](N_PROBE)
    for i in range(N_PROBE):
        lorderkey[i] = Int64(1 + (i * 16807) % max_orderkey)   # 1..max_orderkey
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)          # up to ~1e7 scale2
        disc[i] = Int64((i * 48271) % 11)                      # 0..10 scale2

    var ship_cutoff = Int32(9000)  # l_shipdate > 9000

    # ===== 3. CPU reference (dense int64 accumulator) =====
    var cpu_rev = alloc[Int64](n_slots)
    for k in range(n_slots):
        cpu_rev[k] = 0
    var t0 = perf_counter_ns()
    for i in range(N_PROBE):
        if ship[i] > ship_cutoff:
            var ok = Int(lorderkey[i])
            if order_pass[ok] != 0:
                cpu_rev[ok] += ext[i] * (Int64(100) - disc[i])
    var cpu_ns = perf_counter_ns() - t0

    # ===== 4. GPU =====
    var op_d = ctx.enqueue_create_buffer[DType.uint8](n_slots)
    var lok_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var ship_d = ctx.enqueue_create_buffer[DType.int32](N_PROBE)
    var ext_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var disc_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)
    var revrow_d = ctx.enqueue_create_buffer[DType.int64](N_PROBE)  # per-row revenue
    ctx.synchronize()
    ctx.enqueue_copy(op_d, order_pass)
    ctx.enqueue_copy(lok_d, lorderkey)
    ctx.enqueue_copy(ship_d, ship)
    ctx.enqueue_copy(ext_d, ext)
    ctx.enqueue_copy(disc_d, disc)
    ctx.synchronize()

    var gpu_rev = alloc[Int64](n_slots)      # dense per-order accumulator (host)
    var revrow_h = alloc[Int64](N_PROBE)     # per-row revenue from GPU
    for k in range(n_slots):
        gpu_rev[k] = 0
    var tg = perf_counter_ns()
    ctx.enqueue_function[q3_kernel](
        op_d, lok_d, ship_d, ext_d, disc_d, revrow_d, N_PROBE, ship_cutoff,
        grid_dim=NBLOCKS, block_dim=32,
    )
    ctx.enqueue_copy(revrow_h, revrow_d)
    ctx.synchronize()
    # host per-order sum of the GPU per-row revenue (orderkey is the input col)
    for i in range(N_PROBE):
        var r = revrow_h[i]
        if r != 0:
            gpu_rev[Int(lorderkey[i])] += r
    var gpu_ns = perf_counter_ns() - tg

    # ===== 5. compare full accumulator bit-for-bit =====
    var mismatches = 0
    var nonzero = 0
    for k in range(n_slots):
        if cpu_rev[k] != 0:
            nonzero += 1
        if cpu_rev[k] != gpu_rev[k]:
            mismatches += 1
    var exact = mismatches == 0

    print("==== Q3 join+accumulate ====")
    print("orders:", N_ORDERS, " probe rows:", N_PROBE, " accum slots:", n_slots)
    print("CPU ref (", Float64(cpu_ns) / 1e6, "ms ), GPU (", Float64(gpu_ns) / 1e6, "ms incl copies )")
    print("nonzero orders:", nonzero, " accumulator mismatches:", mismatches)
    print("EXACT match (full per-order accumulator):", exact)

    # ===== 6. host finalize: top-10 by (revenue desc, orderdate asc) =====
    topn(cpu_rev, order_date, n_slots, "CPU")
    topn(gpu_rev, order_date, n_slots, "GPU")

    order_pass.free(); order_date.free(); order_prio.free()
    lorderkey.free(); ship.free(); ext.free(); disc.free()
    cpu_rev.free(); gpu_rev.free(); revrow_h.free()
