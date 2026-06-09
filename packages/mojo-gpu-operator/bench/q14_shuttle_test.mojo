"""Stage-2 execution-shuttle test for the Q14 query class (Mojo-only, needs GPU).

Q14 is an ungrouped 2-aggregate sum over a lineitem |><| part FK join:
    promo = sum(CASE WHEN p_type LIKE 'PROMO%' THEN ext*(1-disc) ELSE 0 END)
    total = sum(ext*(1-disc))
(the surrounding 100*promo/total ratio is a projection ABOVE the GPU op).

This synthesizes a small fact (lineitem-like) + dim (part-like) dataset in host
memory, hand-builds the Q14 RawPlan tape (1 fact GET with a shipdate range, 1 dim
GET, an INNER join l_partkey=p_partkey, 0 group keys, 2 SUM aggregates), then
drives the FULL C-ABI shuttle by calling the @export functions directly:

    build_descriptor -> materialize_count(=2) -> materialize_sql(0)=fact /
        materialize_sql(1)=part -> pin_begin -> feed_column(req 0)=fact cols /
        feed_column(req 1)=part cols -> pin_finalize -> result_i128 x2

It computes a CPU int128 reference for both sums (using the same promo test and
the same ext*(100-disc) revenue), and asserts BIT-EXACT equality. Prints ALL PASS.

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/q14_shuttle_test.mojo
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
)
from raw_plan_tags import (
    RP_MAGIC,
    TYPE_DATE,
    TYPE_DECIMAL,
    TYPE_BIGINT,
    TYPE_VARCHAR,
    CMP_GE,
    CMP_LT,
    AGG_SUM,
    JOIN_INNER,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_PROMO_PRED,
    KIND_Q14,
    IDX_NONE,
)
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N = 200_000  # synthetic lineitem rows (fact)
comptime P = 1000  # synthetic part rows (dim), partkeys 1..P


# ---------------------------------------------------------------------------
# Tape/blob builder (mirrors q6_shuttle_test.mojo).
# ---------------------------------------------------------------------------
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


# Q14 tape: GET lineitem (est N, 2 shipdate filters) + GET part (est P, no
# filter); INNER join l_partkey=p_partkey; 0 group keys; 2 SUM aggs.
def build_q14_tape(mut b: TapeBuilder, ship_lo: Int, ship_hi: Int):
    var s_li = b.sid("lineitem")
    var s_part = b.sid("part")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_ship = b.sid("l_shipdate")
    var s_lpk = b.sid("l_partkey")
    var s_ppk = b.sid("p_partkey")
    var s_ptype = b.sid("p_type")

    # HEADER
    b.put(RP_MAGIC)
    b.put(IDX_NONE)  # group_index (ungrouped)
    b.puti(0)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: 2 agg cols (DECIMAL scale4, width 38). Order: promo, total.
    b.puti(2)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: 4 -> shipdate>=, shipdate<, the literal 100, the promo ELSE 0.
    b.puti(4)
    # c0: l_shipdate >= (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_lo); b.puti(0); b.puti(-1)
    # c1: l_shipdate < (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_hi); b.puti(0); b.puti(-1)
    # c2: the constant 100 (scale 2) used for (100 - l_discount).
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(100); b.puti(0); b.puti(-1)
    # c3: the promo CASE ELSE literal 0.
    b.put(TYPE_BIGINT); b.puti(0); b.puti(0); b.puti(0); b.puti(0); b.puti(-1)

    # GETS: 2 gets. lineitem (est N, 2 shipdate filters), part (est P, 0 filters)
    b.puti(2)
    b.puti(s_li); b.puti(N); b.puti(2)
    b.puti(s_ship); b.put(CMP_GE); b.puti(0)
    b.puti(s_ship); b.put(CMP_LT); b.puti(1)
    b.puti(s_part); b.puti(P); b.puti(0)
    # JOINS: 1 INNER join, 1 cond l_partkey=p_partkey
    b.puti(1)
    b.put(JOIN_INNER); b.puti(1)
    b.puti(s_li); b.puti(s_lpk); b.puti(s_part); b.puti(s_ppk)
    # GROUP_KEYS: none
    b.puti(0)
    # AGGREGATES: 2 SUMs.
    b.puti(2)
    # agg0 (promo): SUM, prog =
    #   PROMO_PRED(part,p_type); LOAD ext; PUSH c2(100); LOAD disc; SUB; MUL;
    #   PUSH c3(0); SELECT
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(8)
    b.put(OP_PROMO_PRED); b.puti(s_part); b.puti(s_ptype)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    b.put(OP_PUSH_CONST); b.puti(3); b.puti(0)
    b.put(OP_SELECT); b.puti(0); b.puti(0)
    # agg1 (total): SUM, prog = LOAD ext; PUSH c2(100); LOAD disc; SUB; MUL
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)


def main() raises:
    comptime assert has_accelerator(), "q14_shuttle_test requires a GPU"

    var ship_lo = 8766
    var ship_hi = 9131

    # ---- synthetic part (dim): p_partkey 1..P, p_type promo iff pk % 3 == 1 ----
    # Build the p_type strings + promo truth on the host for the CPU reference.
    var promo_truth = alloc[Int64](P + 1)  # indexed by partkey (1..P), 0 unused
    var ptype_strs: List[String] = []
    var ppk_vals: List[Int64] = []
    for _ in range(P + 1):
        promo_truth[0] = 0
    for r in range(P):
        var pk = r + 1
        var is_promo = (pk % 3 == 1)
        ppk_vals.append(Int64(pk))
        if is_promo:
            ptype_strs.append(String("PROMO BRUSHED STEEL"))
            promo_truth[pk] = 1
        else:
            ptype_strs.append(String("STANDARD POLISHED TIN"))
            promo_truth[pk] = 0

    # ---- synthetic lineitem (fact) ----
    var ship = alloc[Int32](N)
    var disc = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var lpk = alloc[Int64](N)
    for i in range(N):
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)
        disc[i] = Int64((i * 48271) % 11)  # 0..10 (scale2)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)  # ~ up to 1e7 (scale2)
        lpk[i] = Int64(1 + (i * 2246822519) % P)  # partkey in 1..P

    # ---- CPU int128 reference (same filter + ext*(100-disc), same promo) ----
    var cpu_promo = Int128(0)
    var cpu_total = Int128(0)
    for i in range(N):
        var sd = ship[i]
        if sd >= Int32(ship_lo) and sd < Int32(ship_hi):
            var rev = Int128(ext[i]) * (Int128(100) - Int128(disc[i]))
            cpu_total += rev
            if promo_truth[Int(lpk[i])] != 0:
                cpu_promo += rev

    # ---- build the Q14 RawPlan tape ----
    var b = TapeBuilder()
    build_q14_tape(b, ship_lo, ship_hi)
    var tlen = len(b.tape)
    var tptr = alloc[Int64](tlen if tlen > 0 else 1)
    for i in range(tlen):
        tptr[i] = b.tape[i]
    var blen = len(b.blob)
    var bptr = alloc[UInt8](blen if blen > 0 else 1)
    for i in range(blen):
        bptr[i] = b.blob[i]

    # ---- drive the shuttle ----
    var handle_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(handle_int != 0, "build_descriptor returned 0 (rejected)")
    var h = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=handle_int
    )

    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q14, "kind != Q14")
    assert_equal(mojo_gpu_desc_n_dims(h), 1, "n_dims != 1")
    assert_equal(mojo_gpu_desc_out_arity(h), 2, "out_arity != 2")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 2, "materialize_count != 2 (fact + 1 dim)")

    # ---- request 0: fact SQL (must name the fact cols incl l_partkey) ----
    var cap = 512
    var sql_buf = alloc[UInt8](cap)
    var sql0_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    assert_true(sql0_len > 0, "fact materialize_sql empty")
    var sql0 = String("")
    for i in range(sql0_len):
        sql0 += chr(Int(sql_buf[i]))
    print("fact SQL:", sql0)
    assert_true("l_extendedprice" in sql0, "fact SQL missing l_extendedprice")
    assert_true("l_discount" in sql0, "fact SQL missing l_discount")
    assert_true("l_shipdate" in sql0, "fact SQL missing l_shipdate")
    assert_true("l_partkey" in sql0, "fact SQL missing l_partkey (FK)")
    assert_true("FROM lineitem" in sql0, "fact SQL missing fact table")

    # ---- request 1: dim SQL (part: p_partkey + p_type) ----
    var sql1_len = mojo_gpu_desc_materialize_sql(h, 1, sql_buf, cap)
    assert_true(sql1_len > 0, "dim materialize_sql empty")
    var sql1 = String("")
    for i in range(sql1_len):
        sql1 += chr(Int(sql_buf[i]))
    print("dim SQL:", sql1)
    assert_true("p_partkey" in sql1, "dim SQL missing p_partkey")
    assert_true("p_type" in sql1, "dim SQL missing p_type")
    assert_true("FROM part" in sql1, "dim SQL missing dim table")

    # Parse the fact feed order from the SQL (between SELECT and FROM).
    var fact_order = _parse_order(sql0)
    var dim_order = _parse_order(sql1)

    # pin_begin (COLD first call).
    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    # ---- feed fact columns (request 0) in fact_order ----
    for j in range(len(fact_order)):
        var nm = fact_order[j]
        var rc: Int
        if nm == "l_shipdate":
            rc = mojo_gpu_feed_column(
                h, 0, j, ship.bitcast[NoneType](), N, TYPE_DATE
            )
        elif nm == "l_discount":
            rc = mojo_gpu_feed_column(
                h, 0, j, disc.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif nm == "l_extendedprice":
            rc = mojo_gpu_feed_column(
                h, 0, j, ext.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif nm == "l_partkey":
            rc = mojo_gpu_feed_column(
                h, 0, j, lpk.bitcast[NoneType](), N, TYPE_BIGINT
            )
        else:
            raise Error("unexpected fact column: " + nm)
        assert_equal(rc, 0, "fact feed rc for " + nm)

    # ---- feed dim columns (request 1) in dim_order ----
    # p_partkey: BIGINT int64 array; p_type: contiguous DuckDB string_t (16 bytes).
    var ppk_buf = alloc[Int64](P)
    for r in range(P):
        ppk_buf[r] = ppk_vals[r]
    # Build a contiguous string_t array for p_type (1..>12 chars -> use pointer
    # form). Simpler: build the 16-byte string_t layout by hand with the pointer
    # variant (length > 12), pointing at owned UTF-8 buffers.
    var stbuf = alloc[UInt8](P * 16)
    var owned_strs: List[UnsafePointer[UInt8, MutAnyOrigin]] = []
    for r in range(P):
        ref s = ptype_strs[r]
        var L = s.byte_length()
        var sp = alloc[UInt8](L if L > 0 else 1)
        var sb = s.as_bytes()
        for k in range(L):
            sp[k] = sb[k]
        owned_strs.append(sp)
        var base = r * 16
        # length (little-endian uint32) in bytes 0..3
        stbuf[base + 0] = UInt8(L & 0xFF)
        stbuf[base + 1] = UInt8((L >> 8) & 0xFF)
        stbuf[base + 2] = UInt8((L >> 16) & 0xFF)
        stbuf[base + 3] = UInt8((L >> 24) & 0xFF)
        # all our p_type strings are > 12 chars -> pointer form: bytes 8..15.
        var addr = Int(sp)
        for kb in range(8):
            stbuf[base + 8 + kb] = UInt8((addr >> (8 * kb)) & 0xFF)

    for j in range(len(dim_order)):
        var nm = dim_order[j]
        var rc: Int
        if nm == "p_partkey":
            rc = mojo_gpu_feed_column(
                h, 1, j, ppk_buf.bitcast[NoneType](), P, TYPE_BIGINT
            )
        elif nm == "p_type":
            rc = mojo_gpu_feed_column(
                h, 1, j, stbuf.bitcast[NoneType](), P, TYPE_VARCHAR
            )
        else:
            raise Error("unexpected dim column: " + nm)
        assert_equal(rc, 0, "dim feed rc for " + nm)

    # pin_finalize: runs the generic FK-join kernel + int128 reduce.
    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    # results: 1 row, 2 cols (promo, total) in out_types order.
    assert_equal(mojo_gpu_result_rows(h), 1, "result_rows != 1")
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)

    var rcp = mojo_gpu_result_i128(h, 0, 0, lo, hi)
    assert_equal(rcp, 0, "result_i128 promo rc")
    var gpu_promo = (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))

    var rct = mojo_gpu_result_i128(h, 0, 1, lo, hi)
    assert_equal(rct, 0, "result_i128 total rc")
    var gpu_total = (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))

    print("CPU promo =", cpu_promo, "  GPU promo =", gpu_promo)
    print("CPU total =", cpu_total, "  GPU total =", gpu_total)
    assert_equal(gpu_promo, cpu_promo, "GPU promo sum != CPU (not bit-exact)")
    assert_equal(gpu_total, cpu_total, "GPU total sum != CPU (not bit-exact)")

    for r in range(len(owned_strs)):
        owned_strs[r].free()
    mojo_gpu_desc_free(h)
    print("ALL PASS")


# Parse the comma-separated column list between "SELECT " and " FROM " in a SQL.
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
    # trim trailing spaces
    var trimmed = String("")
    var n = out.byte_length()
    var ob = out.as_bytes()
    var last = n
    while last > 0 and chr(Int(ob[last - 1])) == " ":
        last -= 1
    for i in range(last):
        trimmed += chr(Int(ob[i]))
    return trimmed
