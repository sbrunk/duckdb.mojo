"""Stage-2 execution-shuttle test for the Q5 query class (Mojo-only, needs GPU).

Q5 is a 6-way join (customer, orders, lineitem, supplier, nation, region) with a
DENSE_GROUP aggregate over a dim-carried VARCHAR group key (n_name):

    SELECT n_name, sum(l_extendedprice*(1-l_discount)) AS revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey=o_custkey AND l_orderkey=o_orderkey AND l_suppkey=s_suppkey
      AND c_nationkey=s_nationkey AND s_nationkey=n_nationkey
      AND n_regionkey=r_regionkey AND r_name='ASIA'
      AND o_orderdate>=DATE '1994-01-01' AND o_orderdate<DATE '1995-01-01'
    GROUP BY n_name;

This exercises the new capabilities the generic path needed for Q5:
  * a correlated dim<->dim equality cust_nation==supp_nation on the same fact row
    (lowered to OP_EQ over two single-level dim gathers),
  * a VARCHAR group key carried from the nation dim (res_str),
  * DENSE_GROUP over that dim-carried key (gid = supplier's ASIA-nation dense
    rank, gathered per fact row on host into the gid column).

It synthesizes a small 6-table dataset, hand-builds the Q5 RawPlan tape, drives
the full C-ABI shuttle directly, and asserts bit-exact per-nation revenue vs a
CPU int128 reference (emitting exactly the ASIA nations with revenue>0). Prints
"ALL PASS" on success.

Run from the repo root:
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/q5_shuttle_test.mojo
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
    mojo_gpu_result_str,
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
    CMP_GE,
    AGG_SUM,
    JOIN_INNER,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_SUB,
    OP_MUL,
    KIND_Q5,
    STRAT_DENSE_GROUP,
    IDX_NONE,
)
from std.memory.alloc import unsafe_alloc
from std.os import getenv
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N_NATIONS = 10  # nation keys 0..9 (5 in ASIA region)
comptime ASIA = 2  # the ASIA region key
comptime N_REGIONS = 5
comptime N_CUST = 600  # custkeys 1..N_CUST
comptime N_SUPP = 400  # suppkeys 1..N_SUPP
comptime N_ORDERS = 3000  # orderkeys 1..N_ORDERS
comptime LINES_PER = 4
comptime N = N_ORDERS * LINES_PER  # fact rows


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


# Q5 tape: 6 GETs, 6 INNER join conds, 1 group key (n_name), 1 SUM(revenue).
def build_q5_tape(
    mut b: TapeBuilder, o_lo: Int, o_hi: Int, asia_sid: Int
):
    var s_li = b.sid("lineitem")
    var s_ord = b.sid("orders")
    var s_cust = b.sid("customer")
    var s_supp = b.sid("supplier")
    var s_nat = b.sid("nation")
    var s_reg = b.sid("region")
    var s_lok = b.sid("l_orderkey")
    var s_lsk = b.sid("l_suppkey")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_ook = b.sid("o_orderkey")
    var s_ock = b.sid("o_custkey")
    var s_odate = b.sid("o_orderdate")
    var s_cck = b.sid("c_custkey")
    var s_cnk = b.sid("c_nationkey")
    var s_ssk = b.sid("s_suppkey")
    var s_snk = b.sid("s_nationkey")
    var s_nnk = b.sid("n_nationkey")
    var s_nnm = b.sid("n_name")
    var s_nrk = b.sid("n_regionkey")
    var s_rrk = b.sid("r_regionkey")
    var s_rnm = b.sid("r_name")

    # HEADER (grouped).
    b.put(RP_MAGIC)
    b.puti(42)
    b.puti(43)
    b.emit_string_table()
    # OUT_TYPES: group key first (n_name VARCHAR), then SUM (DECIMAL scale4 w38).
    b.puti(2)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: 4 -> o_orderdate>=lo, o_orderdate<hi, 100 (for 100-disc), 'ASIA'.
    b.puti(4)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(o_lo); b.puti(0); b.puti(-1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(o_hi); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(100); b.puti(0); b.puti(-1)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0); b.puti(0); b.puti(0); b.puti(asia_sid)

    # GETS: 6.  lineitem is the fact (largest est).
    b.puti(6)
    # lineitem (no filter)
    b.puti(s_li); b.puti(N); b.puti(0)
    # orders (2 orderdate filters: >= c0, < c1)
    b.puti(s_ord); b.puti(N_ORDERS); b.puti(2)
    b.puti(s_odate); b.put(CMP_GE); b.puti(0)
    b.puti(s_odate); b.put(CMP_LT); b.puti(1)
    # customer (no filter)
    b.puti(s_cust); b.puti(N_CUST); b.puti(0)
    # supplier (no filter)
    b.puti(s_supp); b.puti(N_SUPP); b.puti(0)
    # nation (no filter)
    b.puti(s_nat); b.puti(N_NATIONS); b.puti(0)
    # region (1 r_name filter: = c3 'ASIA')
    b.puti(s_reg); b.puti(N_REGIONS); b.puti(1)
    b.puti(s_rnm); b.put(CMP_EQ); b.puti(3)

    # JOINS: 6 conds (one INNER join section).
    b.puti(1)
    b.put(JOIN_INNER); b.puti(6)
    # l_orderkey = o_orderkey
    b.puti(s_li); b.puti(s_lok); b.puti(s_ord); b.puti(s_ook)
    # l_suppkey = s_suppkey
    b.puti(s_li); b.puti(s_lsk); b.puti(s_supp); b.puti(s_ssk)
    # o_custkey = c_custkey
    b.puti(s_ord); b.puti(s_ock); b.puti(s_cust); b.puti(s_cck)
    # c_nationkey = s_nationkey  (the correlated cond)
    b.puti(s_cust); b.puti(s_cnk); b.puti(s_supp); b.puti(s_snk)
    # s_nationkey = n_nationkey
    b.puti(s_supp); b.puti(s_snk); b.puti(s_nat); b.puti(s_nnk)
    # n_regionkey = r_regionkey
    b.puti(s_nat); b.puti(s_nrk); b.puti(s_reg); b.puti(s_rrk)

    # GROUP_KEYS: 1 (n_name on nation).
    b.puti(1)
    b.puti(s_nat); b.puti(s_nnm)

    # AGGREGATES: 1 SUM. prog = LOAD ext; PUSH c2(100); LOAD disc; SUB; MUL
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    # PASS_PROGRAMS: none (NR3 trailing additive section)
    b.puti(0)


def build_string_t(
    strs: List[String],
) -> Tuple[
    Pointer[UInt8, MutUntrackedOrigin],
    List[Pointer[UInt8, MutUntrackedOrigin]],
]:
    var n = len(strs)
    var stbuf = unsafe_alloc[UInt8]((n * 16) if n > 0 else 1)
    var owned: List[Pointer[UInt8, MutUntrackedOrigin]] = []
    for r in range(n):
        ref s = strs[r]
        var L = s.byte_length()
        var sp = unsafe_alloc[UInt8](L if L > 0 else 1)
        var sb = s.as_bytes()
        for k in range(L):
            sp[unsafe_offset=k] = sb[k]
        owned.append(sp)
        var base = r * 16
        stbuf[unsafe_offset=base + 0] = UInt8(L & 0xFF)
        stbuf[unsafe_offset=base + 1] = UInt8((L >> 8) & 0xFF)
        stbuf[unsafe_offset=base + 2] = UInt8((L >> 16) & 0xFF)
        stbuf[unsafe_offset=base + 3] = UInt8((L >> 24) & 0xFF)
        if L <= 12:
            for k in range(L):
                stbuf[unsafe_offset=base + 4 + k] = sb[k]
        else:
            var addr = Int(sp)
            for kb in range(8):
                stbuf[unsafe_offset=base + 8 + kb] = UInt8((addr >> (8 * kb)) & 0xFF)
    return (stbuf, owned^)


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


def _table_of(sql: String) raises -> String:
    var start = sql.find(String(" FROM ")) + 6
    var bytes = sql.as_bytes()
    var t = String("")
    for i in range(start, sql.byte_length()):
        var ch = chr(Int(bytes[i]))
        if ch == " ":
            break
        t += ch
    return t


def main() raises:
    comptime assert has_accelerator(), "q5_shuttle_test requires a GPU"

    # Q5_WIDE_NATION_KEYS=1 spreads the nation keys out to 0, 10, ..., 90. With
    # GPU_OP_NATIVE_DECODE=1 the predicate-independent path then declines
    # (G = max key + 1 > 64) and falls back to the per-constant path.
    var key_stride = 10 if getenv("Q5_WIDE_NATION_KEYS", "") != "" else 1

    # Two queries in one process with different date ranges. The second one is
    # a warm hit if its pin signature leaves out the constants, so it checks
    # that a warm hit never returns the first query's result.
    var pb1 = run_q5(8766, 9131, key_stride)  # 1994-01-01-ish to 1995-01-01-ish
    var pb2 = run_q5(8900, 9200, key_stride)
    print("pin_begin per query:", pb1, pb2, "(0=WARM, 1=COLD)")
    print("ALL PASS")


def run_q5(o_lo: Int, o_hi: Int, key_stride: Int) raises -> Int:
    """Run Q5 through the shuttle, check it against a CPU reference, and return
    the pin_begin result (0 = warm, 1 = cold).

    Nation keys in the fed columns are `k * key_stride` for k in 0..N_NATIONS-1.
    """

    # ---- nation: keys 0..N_NATIONS-1; region = key % N_REGIONS; ASIA region=2 ----
    var nation_name: List[String] = []
    var nation_region = unsafe_alloc[Int32](N_NATIONS)
    var nation_in_asia = unsafe_alloc[Int64](N_NATIONS)
    for k in range(N_NATIONS):
        nation_name.append(String("NATION_") + String(k))
        var rk = Int32(k % N_REGIONS)
        nation_region[unsafe_offset=k] = rk
        nation_in_asia[unsafe_offset=k] = Int64(1) if Int(rk) == ASIA else Int64(0)

    # ---- region: keys 0..N_REGIONS-1; r_name "REGION_<k>", ASIA = "REGION_2" ----
    var region_name: List[String] = []
    var rrk_vals = unsafe_alloc[Int32](N_REGIONS)
    for k in range(N_REGIONS):
        region_name.append(String("REGION_") + String(k))
        rrk_vals[unsafe_offset=k] = Int32(k)
    var asia_str = String("REGION_") + String(ASIA)

    # ---- customer: c_custkey 1..N_CUST, c_nationkey ----
    var cust_nation = unsafe_alloc[Int32](N_CUST + 1)
    var cck_vals = unsafe_alloc[Int64](N_CUST)
    var cnk_vals = unsafe_alloc[Int32](N_CUST)
    for r in range(N_CUST):
        var ck = r + 1
        var nk = Int32((r * 2654435761) % N_NATIONS)
        cust_nation[unsafe_offset=ck] = nk
        cck_vals[unsafe_offset=r] = Int64(ck)
        cnk_vals[unsafe_offset=r] = nk * Int32(key_stride)

    # ---- supplier: s_suppkey 1..N_SUPP, s_nationkey. Assign nation = suppkey %
    #      N_NATIONS so each nation has a known supplier (one per residue), which
    #      lets the fact builder pick a supplier of a specific nation on demand. ----
    var supp_nation = unsafe_alloc[Int32](N_SUPP + 1)
    var ssk_vals = unsafe_alloc[Int64](N_SUPP)
    var snk_vals = unsafe_alloc[Int32](N_SUPP)
    # first_supp_of_nation[nk] = a suppkey whose s_nationkey == nk.
    var first_supp_of_nation = unsafe_alloc[Int64](N_NATIONS)
    for k in range(N_NATIONS):
        first_supp_of_nation[unsafe_offset=k] = 0
    for r in range(N_SUPP):
        var sk = r + 1
        var nk = Int32(sk % N_NATIONS)
        supp_nation[unsafe_offset=sk] = nk
        ssk_vals[unsafe_offset=r] = Int64(sk)
        snk_vals[unsafe_offset=r] = nk * Int32(key_stride)
        if first_supp_of_nation[unsafe_offset=Int(nk)] == 0:
            first_supp_of_nation[unsafe_offset=Int(nk)] = Int64(sk)

    # ---- orders: o_orderkey 1..N_ORDERS, o_custkey, o_orderdate ----
    var order_cust = unsafe_alloc[Int64](N_ORDERS + 1)
    var order_date = unsafe_alloc[Int32](N_ORDERS + 1)
    var order_pass = unsafe_alloc[Int64](N_ORDERS + 1)
    var ook_vals = unsafe_alloc[Int64](N_ORDERS)
    var ock_vals = unsafe_alloc[Int64](N_ORDERS)
    var odate_vals = unsafe_alloc[Int32](N_ORDERS)
    for r in range(N_ORDERS):
        var ok = r + 1
        var ck = Int64(1 + (r * 2246822519) % N_CUST)
        var od = Int32(8600 + (r * 16807) % 700)  # 8600..9299 (straddles range)
        order_cust[unsafe_offset=ok] = ck
        order_date[unsafe_offset=ok] = od
        order_pass[unsafe_offset=ok] = Int64(1) if (
            Int(od) >= o_lo and Int(od) < o_hi
        ) else Int64(0)
        ook_vals[unsafe_offset=r] = Int64(ok)
        ock_vals[unsafe_offset=r] = ck
        odate_vals[unsafe_offset=r] = od

    # ---- lineitem (fact): l_orderkey, l_suppkey, l_extendedprice, l_discount ----
    var lok = unsafe_alloc[Int64](N)
    var lsk = unsafe_alloc[Int64](N)
    var ext = unsafe_alloc[Int64](N)
    var disc = unsafe_alloc[Int64](N)
    var i = 0
    for r in range(N_ORDERS):
        var ok = r + 1
        var cust = Int(order_cust[unsafe_offset=ok])
        var cn = Int(cust_nation[unsafe_offset=cust])
        for j in range(LINES_PER):
            var idx = ok * LINES_PER + j
            lok[unsafe_offset=i] = Int64(ok)
            # For ~half the lines, pick a supplier whose nation == the order's
            # customer nation (so the correlated compare passes and exercises the
            # ASIA group when cn is an ASIA nation); else a pseudo-random supplier.
            if idx % 2 == 0:
                lsk[unsafe_offset=i] = first_supp_of_nation[unsafe_offset=cn]
            else:
                lsk[unsafe_offset=i] = Int64(1 + (idx * 40009) % N_SUPP)
            ext[unsafe_offset=i] = Int64(100 + (idx * 16807) % 9_999_900)  # ~1e7 scale2
            disc[unsafe_offset=i] = Int64((idx * 48271) % 11)  # 0..10 scale2
            i += 1

    # ---- CPU int128 reference: per-nation (ASIA) revenue ----
    # A fact row passes iff: order_pass[l_orderkey]
    #   AND cust_nation[order_cust[l_orderkey]] == supp_nation[l_suppkey]
    #   AND nation_in_asia[supp_nation[l_suppkey]].
    # Group = supp_nation (== cust_nation when it passes).
    var cpu_rev = unsafe_alloc[Int128](N_NATIONS)
    for k in range(N_NATIONS):
        cpu_rev[unsafe_offset=k] = Int128(0)
    for r in range(N):
        var ok = Int(lok[unsafe_offset=r])
        if order_pass[unsafe_offset=ok] == 0:
            continue
        var cust = Int(order_cust[unsafe_offset=ok])
        var cn = Int(cust_nation[unsafe_offset=cust])
        var sk = Int(lsk[unsafe_offset=r])
        var sn = Int(supp_nation[unsafe_offset=sk])
        if cn != sn:
            continue
        if nation_in_asia[unsafe_offset=sn] == 0:
            continue
        cpu_rev[unsafe_offset=sn] += Int128(ext[unsafe_offset=r]) * (Int128(100) - Int128(disc[unsafe_offset=r]))
    var cpu_emit = 0
    var cpu_total = Int128(0)
    for k in range(N_NATIONS):
        if cpu_rev[unsafe_offset=k] != Int128(0):
            cpu_emit += 1
            cpu_total += cpu_rev[unsafe_offset=k]

    # ---- build the Q5 RawPlan tape ----
    var b = TapeBuilder()
    var asia_sid = b.sid(asia_str)
    build_q5_tape(b, o_lo, o_hi, asia_sid)
    var tlen = len(b.tape)
    var tptr = unsafe_alloc[Int64](tlen if tlen > 0 else 1)
    for k in range(tlen):
        tptr[unsafe_offset=k] = b.tape[k]
    var blen = len(b.blob)
    var bptr = unsafe_alloc[UInt8](blen if blen > 0 else 1)
    for k in range(blen):
        bptr[unsafe_offset=k] = b.blob[k]

    var handle_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(handle_int != 0, "build_descriptor returned 0 (rejected)")
    var h = Pointer[NoneType, MutUntrackedOrigin](
        unsafe_from_address=handle_int
    )

    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q5, "kind != Q5")
    assert_equal(mojo_gpu_desc_n_dims(h), 5, "n_dims != 5")
    assert_equal(mojo_gpu_desc_out_arity(h), 2, "out_arity != 2")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 6, "materialize_count != 6 (fact + 5 dims)")

    var cap = 512
    var sql_buf = unsafe_alloc[UInt8](cap)

    # Build string_t buffers for nation/region names once.
    var nat_str = build_string_t(nation_name)
    var reg_str = build_string_t(region_name)

    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    # Feed each request by parsing its SQL column order + table. On a warm hit
    # nothing is fed, as on the C++ side.
    for req in range(count if pb == 1 else 0):
        var slen = mojo_gpu_desc_materialize_sql(h, req, sql_buf, cap)
        var sql = String("")
        for k in range(slen):
            sql += chr(Int(sql_buf[unsafe_offset=k]))
        var tbl = _table_of(sql)
        var order = _parse_order(sql)
        if req == 0:
            print("fact SQL:", sql)
        for j in range(len(order)):
            var nm = order[j]
            var rc: Int
            if nm == "l_orderkey":
                rc = mojo_gpu_feed_column(h, req, j, lok.unsafe_bitcast[NoneType](), N, TYPE_BIGINT)
            elif nm == "l_suppkey":
                rc = mojo_gpu_feed_column(h, req, j, lsk.unsafe_bitcast[NoneType](), N, TYPE_BIGINT)
            elif nm == "l_extendedprice":
                rc = mojo_gpu_feed_column(h, req, j, ext.unsafe_bitcast[NoneType](), N, TYPE_DECIMAL)
            elif nm == "l_discount":
                rc = mojo_gpu_feed_column(h, req, j, disc.unsafe_bitcast[NoneType](), N, TYPE_DECIMAL)
            elif nm == "o_orderkey":
                rc = mojo_gpu_feed_column(h, req, j, ook_vals.unsafe_bitcast[NoneType](), N_ORDERS, TYPE_BIGINT)
            elif nm == "o_custkey":
                rc = mojo_gpu_feed_column(h, req, j, ock_vals.unsafe_bitcast[NoneType](), N_ORDERS, TYPE_BIGINT)
            elif nm == "o_orderdate":
                rc = mojo_gpu_feed_column(h, req, j, odate_vals.unsafe_bitcast[NoneType](), N_ORDERS, TYPE_DATE)
            elif nm == "c_custkey":
                rc = mojo_gpu_feed_column(h, req, j, cck_vals.unsafe_bitcast[NoneType](), N_CUST, TYPE_BIGINT)
            elif nm == "c_nationkey":
                rc = mojo_gpu_feed_column(h, req, j, cnk_vals.unsafe_bitcast[NoneType](), N_CUST, TYPE_INTEGER)
            elif nm == "s_suppkey":
                rc = mojo_gpu_feed_column(h, req, j, ssk_vals.unsafe_bitcast[NoneType](), N_SUPP, TYPE_BIGINT)
            elif nm == "s_nationkey":
                rc = mojo_gpu_feed_column(h, req, j, snk_vals.unsafe_bitcast[NoneType](), N_SUPP, TYPE_INTEGER)
            elif nm == "n_nationkey":
                rc = _feed_nat_key(h, req, j, key_stride)
            elif nm == "n_name":
                rc = mojo_gpu_feed_column(h, req, j, nat_str[0].unsafe_bitcast[NoneType](), N_NATIONS, TYPE_VARCHAR)
            elif nm == "n_regionkey":
                rc = _feed_nat_region(h, req, j, nation_region)
            elif nm == "r_regionkey":
                rc = mojo_gpu_feed_column(h, req, j, rrk_vals.unsafe_bitcast[NoneType](), N_REGIONS, TYPE_INTEGER)
            elif nm == "r_name":
                rc = mojo_gpu_feed_column(h, req, j, reg_str[0].unsafe_bitcast[NoneType](), N_REGIONS, TYPE_VARCHAR)
            else:
                raise Error("unexpected column in req " + String(req) + ": " + nm)
            assert_equal(rc, 0, "feed rc for " + nm)
            _ = tbl  # (table name parsed for clarity / debug only)

    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    var rows = mojo_gpu_result_rows(h)
    print("emitted rows:", rows, " CPU emitted:", cpu_emit)
    assert_equal(rows, cpu_emit, "emitted row count != CPU reference")

    # Verify every emitted (n_name, revenue) row against the CPU reference.
    var lo = unsafe_alloc[Int64](1)
    var hi = unsafe_alloc[Int64](1)
    var name_buf = unsafe_alloc[UInt8](64)
    var gpu_total = Int128(0)
    var all_ok = True
    var seen = unsafe_alloc[Int64](N_NATIONS)
    for k in range(N_NATIONS):
        seen[unsafe_offset=k] = 0
    for r in range(rows):
        var nlen = mojo_gpu_result_str(h, r, 0, name_buf, 64)
        var nm = String("")
        for k in range(nlen):
            nm += chr(Int(name_buf[unsafe_offset=k]))
        _ = mojo_gpu_result_i128(h, r, 1, lo, hi)
        var rev = (Int128(hi[unsafe_offset=0]) << 64) + Int128(UInt64(lo[unsafe_offset=0]))
        gpu_total += rev
        # find the nation key whose name matches.
        var nk = -1
        for k in range(N_NATIONS):
            if nation_name[k] == nm:
                nk = k
        if nk < 0:
            all_ok = False
            print("  UNKNOWN n_name:", nm)
            continue
        seen[unsafe_offset=nk] = 1
        if rev != cpu_rev[unsafe_offset=nk]:
            all_ok = False
            print("  MISMATCH nk=", nk, " name=", nm, " gpu=", rev, " cpu=", cpu_rev[unsafe_offset=nk])
    for k in range(N_NATIONS):
        if (cpu_rev[unsafe_offset=k] != Int128(0)) != (seen[unsafe_offset=k] != 0):
            all_ok = False
            print("  EMIT-SET MISMATCH nk=", k)

    print("GPU total revenue =", gpu_total, "  CPU total =", cpu_total)
    assert_equal(gpu_total, cpu_total, "total revenue != CPU (not bit-exact)")
    assert_true(all_ok, "per-nation revenue / emit set mismatch")

    for r in range(len(nat_str[1])):
        nat_str[1][r].unsafe_free()
    for r in range(len(reg_str[1])):
        reg_str[1][r].unsafe_free()
    mojo_gpu_desc_free(h)
    return pb


# n_nationkey is an INTEGER column k * key_stride for k in 0..N_NATIONS-1.
def _feed_nat_key(
    h: Pointer[NoneType, MutUntrackedOrigin], req: Int, j: Int, key_stride: Int
) raises -> Int:
    var nk = unsafe_alloc[Int32](N_NATIONS)
    for k in range(N_NATIONS):
        nk[unsafe_offset=k] = Int32(k * key_stride)
    return mojo_gpu_feed_column(h, req, j, nk.unsafe_bitcast[NoneType](), N_NATIONS, TYPE_INTEGER)


def _feed_nat_region(
    h: Pointer[NoneType, MutUntrackedOrigin],
    req: Int,
    j: Int,
    nation_region: Pointer[Int32, MutUntrackedOrigin],
) raises -> Int:
    return mojo_gpu_feed_column(
        h, req, j, nation_region.unsafe_bitcast[NoneType](), N_NATIONS, TYPE_INTEGER
    )
