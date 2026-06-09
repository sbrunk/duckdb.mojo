"""Stage-2 execution-shuttle test for the Q3 query class (Mojo-only, needs GPU).

Q3 is a 3-way join (customer |><| orders |><| lineitem) with a SORT_SEGREDUCE
grouped aggregate:
    SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) AS revenue,
           o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey AND l_orderkey=o_orderkey
      AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority;

This exercises the NEW capabilities the generic path needed for Q3:
  * SORT_SEGREDUCE grouped output (one warp per order segment),
  * a transitive dim->dim fold: customer joins ORDERS (not the fact), folded on
    host into order_pass[o_orderkey] = (o_orderdate<cutoff) AND is_building[o_custkey],
  * DIM-CARRIED group keys (o_orderdate / o_shippriority gathered per segment),
  * multi-row emission (one row per qualifying order).

It synthesizes a small customer/orders/lineitem dataset (lineitem ORDERED BY
l_orderkey, as the materialize SQL requires), hand-builds the Q3 RawPlan tape,
drives the FULL C-ABI shuttle directly, and asserts BIT-EXACT per-segment revenue
+ that EXACTLY the orders stock would emit (>=1 passing lineitem under the folded
order_pass) appear, vs a CPU int128 reference. Prints ALL PASS.

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/q3_shuttle_test.mojo
"""

from gpu_kernels import (
    mojo_gpu_build_descriptor,
    mojo_gpu_desc_free,
    mojo_gpu_desc_kind,
    mojo_gpu_desc_n_dims,
    mojo_gpu_desc_out_arity,
    mojo_gpu_desc_materialize_count,
    mojo_gpu_desc_materialize_sql,
    mojo_gpu_pin_begin,
    mojo_gpu_feed_column,
    mojo_gpu_pin_finalize,
    mojo_gpu_result_rows,
    mojo_gpu_result_i128,
    mojo_gpu_result_i64,
)
from raw_plan_tags import (
    RP_MAGIC,
    TYPE_DATE,
    TYPE_DECIMAL,
    TYPE_INTEGER,
    TYPE_BIGINT,
    TYPE_VARCHAR,
    CMP_EQ,
    CMP_LT,
    CMP_GT,
    AGG_SUM,
    JOIN_INNER,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_SUB,
    OP_MUL,
    KIND_Q3,
    STRAT_SORT_SEGREDUCE,
    IDX_NONE,
)
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N_ORDERS = 4000  # synthetic orders (orderkeys 1..N_ORDERS)
comptime N_CUST = 800  # synthetic customers (custkeys 1..N_CUST)
comptime LINES_PER = 4  # lineitems per order (contiguous, sorted by orderkey)
comptime N = N_ORDERS * LINES_PER  # synthetic lineitem rows (fact), sorted


struct TapeBuilder(Movable):
    var tape: List[Int64]
    var blob: List[UInt8]
    var names: List[String]
    var offs: List[Int]
    var lens: List[Int]

    def __init__(out self):
        self.tape = []
        self.blob = []
        self.names = []
        self.offs = []
        self.lens = []

    def put(mut self, v: Int64):
        self.tape.append(v)

    def puti(mut self, v: Int):
        self.tape.append(Int64(v))

    def sid(mut self, s: String) -> Int:
        for i in range(len(self.names)):
            if self.names[i] == s:
                return i
        var off = len(self.blob)
        var bytes = s.as_bytes()
        for i in range(s.byte_length()):
            self.blob.append(bytes[i])
        self.names.append(s)
        self.offs.append(off)
        self.lens.append(s.byte_length())
        return len(self.names) - 1

    def emit_string_table(mut self):
        self.puti(len(self.names))
        for i in range(len(self.names)):
            self.puti(self.offs[i])
            self.puti(self.lens[i])


# Q3 tape: 3 GETs (lineitem fact w/ l_shipdate filter; orders w/ o_orderdate
# filter; customer w/ c_mktsegment filter), 2 INNER joins (l_orderkey=o_orderkey,
# o_custkey=c_custkey), 3 group keys (l_orderkey/o_orderdate/o_shippriority), 1 SUM.
def build_q3_tape(
    mut b: TapeBuilder,
    o_cutoff: Int,
    l_cutoff: Int,
    bsid: Int,  # 'BUILDING' string id (added by caller via sid())
):
    var s_li = b.sid("lineitem")
    var s_ord = b.sid("orders")
    var s_cust = b.sid("customer")
    var s_lok = b.sid("l_orderkey")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_ship = b.sid("l_shipdate")
    var s_ook = b.sid("o_orderkey")
    var s_ock = b.sid("o_custkey")
    var s_odate = b.sid("o_orderdate")
    var s_oprio = b.sid("o_shippriority")
    var s_cck = b.sid("c_custkey")
    var s_cseg = b.sid("c_mktsegment")

    # HEADER (grouped -> a real group_index).
    b.put(RP_MAGIC)
    b.puti(42)  # group_index (grouped)
    b.puti(43)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: group keys first (l_orderkey BIGINT, o_orderdate DATE,
    # o_shippriority INTEGER), then the SUM (DECIMAL scale4, width 38).
    b.puti(4)
    b.put(TYPE_BIGINT); b.puti(0); b.puti(0)
    b.put(TYPE_DATE); b.puti(0); b.puti(0)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: 4 -> l_shipdate>, o_orderdate<, 100 (for 100-disc), 'BUILDING'.
    b.puti(4)
    # c0: l_shipdate > (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(l_cutoff); b.puti(0); b.puti(-1)
    # c1: o_orderdate < (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(o_cutoff); b.puti(0); b.puti(-1)
    # c2: the constant 100 (scale 2) used for (100 - l_discount).
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(100); b.puti(0); b.puti(-1)
    # c3: 'BUILDING' (VARCHAR) for c_mktsegment = 'BUILDING'.
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0); b.puti(0); b.puti(0); b.puti(bsid)

    # GETS: 3.
    # lineitem (est N, 1 shipdate filter: l_shipdate > c0)
    b.puti(3)
    b.puti(s_li); b.puti(N); b.puti(1)
    b.puti(s_ship); b.put(CMP_GT); b.puti(0)
    # orders (est N_ORDERS, 1 orderdate filter: o_orderdate < c1)
    b.puti(s_ord); b.puti(N_ORDERS); b.puti(1)
    b.puti(s_odate); b.put(CMP_LT); b.puti(1)
    # customer (est N_CUST, 1 mktsegment filter: c_mktsegment = c3)
    b.puti(s_cust); b.puti(N_CUST); b.puti(1)
    b.puti(s_cseg); b.put(CMP_EQ); b.puti(3)

    # JOINS: 2 INNER joins.
    b.puti(2)
    # join 0: l_orderkey = o_orderkey (fact <-> orders)
    b.put(JOIN_INNER); b.puti(1)
    b.puti(s_li); b.puti(s_lok); b.puti(s_ord); b.puti(s_ook)
    # join 1: o_custkey = c_custkey (orders <-> customer, the transitive edge)
    b.put(JOIN_INNER); b.puti(1)
    b.puti(s_ord); b.puti(s_ock); b.puti(s_cust); b.puti(s_cck)

    # GROUP_KEYS: 3 (l_orderkey [fact], o_orderdate [orders], o_shippriority [orders])
    b.puti(3)
    b.puti(s_li); b.puti(s_lok)
    b.puti(s_ord); b.puti(s_odate)
    b.puti(s_ord); b.puti(s_oprio)

    # AGGREGATES: 1 SUM. prog = LOAD ext; PUSH c2(100); LOAD disc; SUB; MUL
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)


# Build a contiguous DuckDB string_t (16-byte) array for a list of strings, using
# the pointer form (works for any length). Returns (buffer, owned-byte-pointers).
def build_string_t(
    strs: List[String],
) -> Tuple[
    UnsafePointer[UInt8, MutAnyOrigin],
    List[UnsafePointer[UInt8, MutAnyOrigin]],
]:
    var n = len(strs)
    var stbuf = alloc[UInt8]((n * 16) if n > 0 else 1)
    var owned: List[UnsafePointer[UInt8, MutAnyOrigin]] = []
    for r in range(n):
        ref s = strs[r]
        var L = s.byte_length()
        var sp = alloc[UInt8](L if L > 0 else 1)
        var sb = s.as_bytes()
        for k in range(L):
            sp[k] = sb[k]
        owned.append(sp)
        var base = r * 16
        stbuf[base + 0] = UInt8(L & 0xFF)
        stbuf[base + 1] = UInt8((L >> 8) & 0xFF)
        stbuf[base + 2] = UInt8((L >> 16) & 0xFF)
        stbuf[base + 3] = UInt8((L >> 24) & 0xFF)
        if L <= 12:
            for k in range(L):
                stbuf[base + 4 + k] = sb[k]
        else:
            var addr = Int(sp)
            for kb in range(8):
                stbuf[base + 8 + kb] = UInt8((addr >> (8 * kb)) & 0xFF)
    return (stbuf, owned^)


def main() raises:
    comptime assert has_accelerator(), "q3_shuttle_test requires a GPU"

    var l_cutoff = 9204  # 1995-03-15 as DATE days (value is arbitrary here)
    var o_cutoff = 9204

    # ---- synthetic customer: c_custkey 1..N_CUST; BUILDING iff ck % 5 == 0 ----
    var is_building = alloc[Int64](N_CUST + 1)
    var cseg_strs: List[String] = []
    var cck_vals: List[Int64] = []
    for r in range(N_CUST):
        var ck = r + 1
        cck_vals.append(Int64(ck))
        if ck % 5 == 0:
            cseg_strs.append(String("BUILDING"))
            is_building[ck] = 1
        else:
            cseg_strs.append(String("MACHINERY"))
            is_building[ck] = 0

    # ---- synthetic orders: o_orderkey 1..N_ORDERS, o_custkey, date, prio ----
    var order_custkey = alloc[Int64](N_ORDERS + 1)
    var order_date = alloc[Int64](N_ORDERS + 1)
    var order_prio = alloc[Int64](N_ORDERS + 1)
    var order_pass = alloc[Int64](N_ORDERS + 1)  # CPU ref: folded pass flag
    var ook_vals = alloc[Int64](N_ORDERS)
    var ock_vals = alloc[Int64](N_ORDERS)
    var odate_vals = alloc[Int32](N_ORDERS)
    var oprio_vals = alloc[Int32](N_ORDERS)
    for r in range(N_ORDERS):
        var ok = r + 1
        var ck = Int64(1 + (r * 2246822519) % N_CUST)
        var od = Int32(9000 + (r * 16807) % 400)  # date days 9000..9399
        var sp = Int32((r * 7) % 3)  # shippriority 0..2
        order_custkey[ok] = ck
        order_date[ok] = Int64(od)
        order_prio[ok] = Int64(sp)
        # folded pass: o_orderdate < o_cutoff AND is_building[o_custkey]
        var building = is_building[Int(ck)] != 0
        order_pass[ok] = Int64(1) if (
            building and Int(od) < o_cutoff
        ) else Int64(0)
        ook_vals[r] = Int64(ok)
        ock_vals[r] = ck
        odate_vals[r] = od
        oprio_vals[r] = sp

    # ---- synthetic lineitem (fact), SORTED BY l_orderkey (contiguous segs) ----
    var lok = alloc[Int64](N)
    var ship = alloc[Int32](N)
    var ext = alloc[Int64](N)
    var disc = alloc[Int64](N)
    var i = 0
    for r in range(N_ORDERS):
        var ok = r + 1
        for j in range(LINES_PER):
            var idx = ok * LINES_PER + j  # deterministic per-line variety
            lok[i] = Int64(ok)
            ship[i] = Int32(9000 + (idx * 1103515245 + 12345) % 400)  # 9000..9399
            ext[i] = Int64(100 + (idx * 16807) % 9_999_900)  # ~1e7 scale2
            disc[i] = Int64((idx * 48271) % 11)  # 0..10 scale2
            i += 1

    # ---- CPU int128 reference: per-order revenue over passing lineitems ----
    # passing lineitem: l_shipdate > l_cutoff AND order_pass[l_orderkey].
    var cpu_rev = alloc[Int128](N_ORDERS + 1)
    for ok in range(N_ORDERS + 1):
        cpu_rev[ok] = Int128(0)
    for r in range(N):
        var ok = Int(lok[r])
        if Int(ship[r]) > l_cutoff and order_pass[ok] != 0:
            cpu_rev[ok] += Int128(ext[r]) * (Int128(100) - Int128(disc[r]))
    # CPU set of emitted orders (revenue > 0) + checksum.
    var cpu_emit = 0
    var cpu_total = Int128(0)
    for ok in range(1, N_ORDERS + 1):
        if cpu_rev[ok] > Int128(0):
            cpu_emit += 1
            cpu_total += cpu_rev[ok]

    # ---- build the Q3 RawPlan tape ----
    var b = TapeBuilder()
    var bsid = b.sid("BUILDING")
    build_q3_tape(b, o_cutoff, l_cutoff, bsid)
    var tlen = len(b.tape)
    var tptr = alloc[Int64](tlen if tlen > 0 else 1)
    for k in range(tlen):
        tptr[k] = b.tape[k]
    var blen = len(b.blob)
    var bptr = alloc[UInt8](blen if blen > 0 else 1)
    for k in range(blen):
        bptr[k] = b.blob[k]

    # ---- drive the shuttle ----
    var handle_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(handle_int != 0, "build_descriptor returned 0 (rejected)")
    var h = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=handle_int
    )

    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q3, "kind != Q3")
    assert_equal(mojo_gpu_desc_n_dims(h), 2, "n_dims != 2 (orders + customer)")
    assert_equal(mojo_gpu_desc_out_arity(h), 4, "out_arity != 4")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 3, "materialize_count != 3 (fact + 2 dims)")

    var cap = 512
    var sql_buf = alloc[UInt8](cap)

    # request 0: fact SQL (must ORDER BY l_orderkey + name fact cols).
    var sql0_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    var sql0 = String("")
    for k in range(sql0_len):
        sql0 += chr(Int(sql_buf[k]))
    print("fact SQL:", sql0)
    assert_true("ORDER BY l_orderkey" in sql0, "fact SQL missing ORDER BY l_orderkey")
    assert_true("l_orderkey" in sql0, "fact SQL missing l_orderkey")
    var fact_order = _parse_order(sql0)

    # request 1 + 2: dim SQLs.
    var sql1_len = mojo_gpu_desc_materialize_sql(h, 1, sql_buf, cap)
    var sql1 = String("")
    for k in range(sql1_len):
        sql1 += chr(Int(sql_buf[k]))
    print("dim1 SQL:", sql1)
    var dim1_order = _parse_order(sql1)
    var dim1_is_orders = "FROM orders" in sql1

    var sql2_len = mojo_gpu_desc_materialize_sql(h, 2, sql_buf, cap)
    var sql2 = String("")
    for k in range(sql2_len):
        sql2 += chr(Int(sql_buf[k]))
    print("dim2 SQL:", sql2)
    var dim2_order = _parse_order(sql2)

    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    # ---- feed fact columns (request 0) ----
    for j in range(len(fact_order)):
        var nm = fact_order[j]
        var rc: Int
        if nm == "l_orderkey":
            rc = mojo_gpu_feed_column(h, 0, j, lok.bitcast[NoneType](), N, TYPE_BIGINT)
        elif nm == "l_shipdate":
            rc = mojo_gpu_feed_column(h, 0, j, ship.bitcast[NoneType](), N, TYPE_DATE)
        elif nm == "l_extendedprice":
            rc = mojo_gpu_feed_column(h, 0, j, ext.bitcast[NoneType](), N, TYPE_DECIMAL)
        elif nm == "l_discount":
            rc = mojo_gpu_feed_column(h, 0, j, disc.bitcast[NoneType](), N, TYPE_DECIMAL)
        else:
            raise Error("unexpected fact column: " + nm)
        assert_equal(rc, 0, "fact feed rc for " + nm)

    # ---- feed dim columns. Build orders + customer buffers. ----
    var ook_buf = alloc[Int64](N_ORDERS)
    var ock_buf = alloc[Int64](N_ORDERS)
    for r in range(N_ORDERS):
        ook_buf[r] = ook_vals[r]
        ock_buf[r] = ock_vals[r]
    var cck_buf = alloc[Int64](N_CUST)
    for r in range(N_CUST):
        cck_buf[r] = cck_vals[r]
    var cstr = build_string_t(cseg_strs)

    var orders_req = 1 if dim1_is_orders else 2
    var cust_req = 2 if dim1_is_orders else 1
    var orders_order = dim1_order.copy() if dim1_is_orders else dim2_order.copy()
    var cust_order = dim2_order.copy() if dim1_is_orders else dim1_order.copy()

    # feed orders dim columns.
    for j in range(len(orders_order)):
        var nm = orders_order[j]
        var rc: Int
        if nm == "o_orderkey":
            rc = mojo_gpu_feed_column(h, orders_req, j, ook_buf.bitcast[NoneType](), N_ORDERS, TYPE_BIGINT)
        elif nm == "o_custkey":
            rc = mojo_gpu_feed_column(h, orders_req, j, ock_buf.bitcast[NoneType](), N_ORDERS, TYPE_BIGINT)
        elif nm == "o_orderdate":
            rc = mojo_gpu_feed_column(h, orders_req, j, odate_vals.bitcast[NoneType](), N_ORDERS, TYPE_DATE)
        elif nm == "o_shippriority":
            rc = mojo_gpu_feed_column(h, orders_req, j, oprio_vals.bitcast[NoneType](), N_ORDERS, TYPE_INTEGER)
        else:
            raise Error("unexpected orders column: " + nm)
        assert_equal(rc, 0, "orders feed rc for " + nm)

    # feed customer dim columns.
    for j in range(len(cust_order)):
        var nm = cust_order[j]
        var rc: Int
        if nm == "c_custkey":
            rc = mojo_gpu_feed_column(h, cust_req, j, cck_buf.bitcast[NoneType](), N_CUST, TYPE_BIGINT)
        elif nm == "c_mktsegment":
            rc = mojo_gpu_feed_column(h, cust_req, j, cstr[0].bitcast[NoneType](), N_CUST, TYPE_VARCHAR)
        else:
            raise Error("unexpected customer column: " + nm)
        assert_equal(rc, 0, "customer feed rc for " + nm)

    # pin_finalize: runs SORT_SEGREDUCE + transitive fold + int128 reduce.
    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    var rows = mojo_gpu_result_rows(h)
    print("emitted rows:", rows, " CPU emitted:", cpu_emit)
    assert_equal(rows, cpu_emit, "emitted row count != CPU reference")

    # Verify every emitted row: revenue bit-exact, carried keys correct, and the
    # orderkey is one CPU also emits (revenue>0). Out cols: 0=l_orderkey(BIGINT),
    # 1=o_orderdate(DATE), 2=o_shippriority(INTEGER), 3=revenue(DECIMAL38,4).
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    var gpu_total = Int128(0)
    var all_ok = True
    var seen = alloc[Int64](N_ORDERS + 1)
    for k in range(N_ORDERS + 1):
        seen[k] = 0
    for r in range(rows):
        var ok = Int(mojo_gpu_result_i64(h, r, 0))
        var odate = Int(mojo_gpu_result_i64(h, r, 1))
        var oprio = Int(mojo_gpu_result_i64(h, r, 2))
        _ = mojo_gpu_result_i128(h, r, 3, lo, hi)
        var rev = (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))
        gpu_total += rev
        if ok < 1 or ok > N_ORDERS:
            all_ok = False
            continue
        seen[ok] = 1
        if rev != cpu_rev[ok]:
            all_ok = False
            if r < 5:
                print("  MISMATCH ok=", ok, " gpu=", rev, " cpu=", cpu_rev[ok])
        if Int64(odate) != order_date[ok]:
            all_ok = False
            print("  DATE MISMATCH ok=", ok, " gpu=", odate, " cpu=", order_date[ok])
        if Int64(oprio) != order_prio[ok]:
            all_ok = False
            print("  PRIO MISMATCH ok=", ok, " gpu=", oprio, " cpu=", order_prio[ok])
    # Every CPU-emitted order must appear exactly once.
    for ok in range(1, N_ORDERS + 1):
        if (cpu_rev[ok] > Int128(0)) != (seen[ok] != 0):
            all_ok = False

    print("GPU total revenue =", gpu_total, "  CPU total =", cpu_total)
    assert_equal(gpu_total, cpu_total, "total revenue != CPU (not bit-exact)")
    assert_true(all_ok, "per-segment revenue / carried keys / emit set mismatch")

    for r in range(len(cstr[1])):
        cstr[1][r].free()
    mojo_gpu_desc_free(h)
    print("ALL PASS")


def _parse_order(sql: String) raises -> List[String]:
    var start = sql.find(String("SELECT ")) + 7
    var stop = sql.find(String(" FROM "))
    var bytes = sql.as_bytes()
    var cols: List[String] = []
    var cur = String("")
    for i in range(start, stop):
        var ch = chr(Int(bytes[i]))
        if ch == ",":
            cols.append(_strip(cur))
            cur = String("")
        else:
            cur += ch
    if _strip(cur).byte_length() > 0:
        cols.append(_strip(cur))
    return cols^


def _strip(s: String) -> String:
    var out = String("")
    var started = False
    var bytes = s.as_bytes()
    for i in range(s.byte_length()):
        var ch = chr(Int(bytes[i]))
        if ch == " " and not started:
            continue
        started = True
        out += ch
    var trimmed = String("")
    var ob = out.as_bytes()
    var last = out.byte_length()
    while last > 0 and chr(Int(ob[last - 1])) == " ":
        last -= 1
    for i in range(last):
        trimmed += chr(Int(ob[i]))
    return trimmed
