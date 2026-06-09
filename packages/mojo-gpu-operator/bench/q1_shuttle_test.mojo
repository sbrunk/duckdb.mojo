"""Stage-2 execution-shuttle test for the Q1 query class (Mojo-only, needs GPU).

Synthesizes a small lineitem-like dataset with a handful of distinct
(l_returnflag, l_linestatus) combos, hand-builds the Q1 RawPlan tape (2 group
keys + 8 aggregates + an `l_shipdate <= cutoff` filter), and drives the FULL
C-ABI shuttle by calling the @export functions in gpu_kernels.mojo directly:

    build_descriptor -> materialize_count/sql -> pin_begin
                     -> feed_column x7 (incl the 2 VARCHAR group keys, fed as
                        DuckDB string_t arrays) -> pin_finalize
                     -> result_rows + per-cell getters (str / i128 / f64 / i64)

It computes a CPU int128/double reference for every group x metric and asserts
bit-exact equality for the 4 int128 sums + the count, and a tight float
tolerance for the 3 avgs, with the group keys matching. Prints ALL PASS.

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/q1_shuttle_test.mojo
"""

from gpu_kernels import (
    mojo_gpu_build_descriptor,
    mojo_gpu_desc_free,
    mojo_gpu_desc_kind,
    mojo_gpu_desc_strategy,
    mojo_gpu_desc_out_arity,
    mojo_gpu_desc_materialize_count,
    mojo_gpu_desc_materialize_sql,
    mojo_gpu_pin_begin,
    mojo_gpu_feed_column,
    mojo_gpu_pin_finalize,
    mojo_gpu_result_rows,
    mojo_gpu_result_i128,
    mojo_gpu_result_i64,
    mojo_gpu_result_f64,
    mojo_gpu_result_str,
)
from raw_plan_tags import (
    RP_MAGIC,
    TYPE_DATE,
    TYPE_DECIMAL,
    TYPE_VARCHAR,
    TYPE_DOUBLE,
    TYPE_BIGINT,
    CMP_LE,
    AGG_SUM,
    AGG_AVG,
    AGG_COUNT_STAR,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_SUB,
    OP_ADD,
    OP_MUL,
    KIND_Q1,
    STRAT_DENSE_GROUP,
    IDX_NONE,
)
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true
from std.math import abs


comptime N = 100_000  # small synthetic lineitem
comptime NG = 4  # distinct (returnflag, linestatus) combos


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


# Q1 tape: 1 GET lineitem [filter l_shipdate <= cutoff], 2 group keys
# (l_returnflag, l_linestatus), 8 aggregates in TPC-H Q1 order.
def build_q1_tape(mut b: TapeBuilder, ship_cutoff: Int):
    var s_li = b.sid("lineitem")
    var s_rf = b.sid("l_returnflag")
    var s_ls = b.sid("l_linestatus")
    var s_qty = b.sid("l_quantity")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_tax = b.sid("l_tax")
    var s_ship = b.sid("l_shipdate")

    # HEADER
    b.put(RP_MAGIC)
    b.puti(0)  # group_index
    b.puti(0)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: group cols first (2 VARCHAR), then 8 agg cols.
    b.puti(10)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)  # l_returnflag
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)  # l_linestatus
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(38)  # sum_qty
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(38)  # sum_base_price
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)  # sum_disc_price
    b.put(TYPE_DECIMAL); b.puti(6); b.puti(38)  # sum_charge
    b.put(TYPE_DOUBLE); b.puti(0); b.puti(0)  # avg_qty
    b.put(TYPE_DOUBLE); b.puti(0); b.puti(0)  # avg_price
    b.put(TYPE_DOUBLE); b.puti(0); b.puti(0)  # avg_disc
    b.put(TYPE_BIGINT); b.puti(0); b.puti(0)  # count_order
    # CONSTS: 1 -> l_shipdate cutoff (DATE days). Plus a "1@scale2" const used
    # by the disc_price/charge programs.
    b.puti(2)
    # c0: shipdate cutoff (days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_cutoff); b.puti(0); b.puti(-1)
    # c1: 1.00 at scale 2 (== 100)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(100); b.puti(0); b.puti(-1)
    # GETS: 1 get, 1 filter (l_shipdate <= cutoff)
    b.puti(1)
    b.puti(s_li); b.puti(N); b.puti(1)
    b.puti(s_ship); b.put(CMP_LE); b.puti(0)
    # JOINS: none
    b.puti(0)
    # GROUP_KEYS: 2 (l_returnflag, l_linestatus)
    b.puti(2)
    b.puti(s_li); b.puti(s_rf)
    b.puti(s_li); b.puti(s_ls)
    # AGGREGATES: 8
    b.puti(8)
    # a0: sum(l_quantity) scale2
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(2); b.puti(38); b.puti(1)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_qty)
    # a1: sum(l_extendedprice) scale2
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(2); b.puti(38); b.puti(1)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    # a2: sum(l_extendedprice * (1 - l_discount)) scale4
    #     program: LOAD ext; PUSH 1; LOAD disc; SUB; MUL
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    # a3: sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) scale6
    #     program: LOAD ext; PUSH 1; LOAD disc; SUB; MUL; PUSH 1; LOAD tax; ADD; MUL
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(6); b.puti(38); b.puti(1)
    b.puti(9)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_tax)
    b.put(OP_ADD); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    # a4: avg(l_quantity) DOUBLE
    b.put(AGG_AVG); b.put(TYPE_DOUBLE); b.puti(0); b.puti(0); b.puti(0)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_qty)
    # a5: avg(l_extendedprice) DOUBLE
    b.put(AGG_AVG); b.put(TYPE_DOUBLE); b.puti(0); b.puti(0); b.puti(0)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    # a6: avg(l_discount) DOUBLE
    b.put(AGG_AVG); b.put(TYPE_DOUBLE); b.puti(0); b.puti(0); b.puti(0)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    # a7: count_star() BIGINT
    b.put(AGG_COUNT_STAR); b.put(TYPE_BIGINT); b.puti(0); b.puti(0); b.puti(0)
    b.puti(0)


# Build a synthetic DuckDB string_t (16 bytes) for a short ASCII string into the
# 16-byte slot at `slot` (already pointing at row i). Short strings (<=12 bytes,
# always the case here -> single chars) are inlined.
def write_string_t(slot: UnsafePointer[UInt8, MutAnyOrigin], s: String):
    var n = s.byte_length()
    # length (little-endian uint32)
    slot[0] = UInt8(n & 0xFF)
    slot[1] = UInt8((n >> 8) & 0xFF)
    slot[2] = UInt8((n >> 16) & 0xFF)
    slot[3] = UInt8((n >> 24) & 0xFF)
    # zero the 12 inline bytes, then copy (n <= 12 here).
    for k in range(12):
        slot[4 + k] = 0
    var bytes = s.as_bytes()
    for k in range(n):
        slot[4 + k] = bytes[k]


def main() raises:
    comptime assert has_accelerator(), "q1_shuttle_test requires a GPU"

    var ship_cutoff = 9000  # l_shipdate <= 9000 (days)

    # ---- distinct group-key tuples (returnflag, linestatus) ----
    var rf_tab: List[String] = [
        String("N"),
        String("R"),
        String("A"),
        String("N"),
    ]
    var ls_tab: List[String] = [
        String("O"),
        String("F"),
        String("F"),
        String("F"),
    ]

    # ---- synthetic lineitem-like columns ----
    var qty = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var disc = alloc[Int64](N)
    var tax = alloc[Int64](N)
    var ship = alloc[Int32](N)
    # group keys as DuckDB string_t arrays (16 bytes each).
    var rf = alloc[UInt8](N * 16)
    var ls = alloc[UInt8](N * 16)
    var grp = alloc[Int32](N)  # which (rf,ls) tuple index per row
    for i in range(N):
        var g = (i * 2654435761) % NG
        grp[i] = Int32(g)
        write_string_t(rf + i * 16, rf_tab[g])
        write_string_t(ls + i * 16, ls_tab[g])
        qty[i] = Int64(1 + (i * 22695477) % 5000)  # 1..5000 (scale2)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)  # up to ~1e7 (scale2)
        disc[i] = Int64((i * 48271) % 11)  # 0..10 (scale2)
        tax[i] = Int64((i * 69069) % 9)  # 0..8 (scale2)
        ship[i] = Int32(7000 + (i * 1103515245 + 12345) % 3000)  # 7000..9999

    # ---- CPU int128/double reference, per distinct sorted (rf,ls) tuple ----
    # Build sorted distinct tuple list to define the expected dense gid order.
    var seen: List[String] = []
    var seen_rf: List[String] = []
    var seen_ls: List[String] = []
    for g in range(NG):
        var tk = rf_tab[g] + String("\x01") + ls_tab[g]
        var present = False
        for j in range(len(seen)):
            if seen[j] == tk:
                present = True
                break
        if not present:
            seen.append(tk)
            seen_rf.append(rf_tab[g])
            seen_ls.append(ls_tab[g])
    var ng_distinct = len(seen)
    # sort indices by tuple key
    var ord: List[Int] = []
    for j in range(ng_distinct):
        ord.append(j)
    for a in range(ng_distinct):
        for b in range(a + 1, ng_distinct):
            if seen[ord[b]] < seen[ord[a]]:
                var t = ord[a]
                ord[a] = ord[b]
                ord[b] = t
    # tuple-distinct-idx -> dense gid (sorted rank)
    var didx_gid = alloc[Int32](ng_distinct)
    for g in range(ng_distinct):
        didx_gid[ord[g]] = Int32(g)
    # map original tuple index g(0..NG) -> distinct idx
    def distinct_idx_of(g: Int, seen: List[String], rf_tab: List[String], ls_tab: List[String]) raises -> Int:
        var tk = rf_tab[g] + String("\x01") + ls_tab[g]
        for j in range(len(seen)):
            if seen[j] == tk:
                return j
        raise Error("tuple not found")

    var ref_cnt = alloc[Int64](ng_distinct)
    var ref_sqty = alloc[Int128](ng_distinct)
    var ref_sext = alloc[Int128](ng_distinct)
    var ref_sdp = alloc[Int128](ng_distinct)
    var ref_sch = alloc[Int128](ng_distinct)
    var ref_sdisc = alloc[Int128](ng_distinct)
    for g in range(ng_distinct):
        ref_cnt[g] = 0
        ref_sqty[g] = Int128(0)
        ref_sext[g] = Int128(0)
        ref_sdp[g] = Int128(0)
        ref_sch[g] = Int128(0)
        ref_sdisc[g] = Int128(0)
    for i in range(N):
        if ship[i] <= Int32(ship_cutoff):
            var didx = distinct_idx_of(Int(grp[i]), seen, rf_tab, ls_tab)
            var gid = Int(didx_gid[didx])
            var e = Int128(ext[i])
            var d = Int128(disc[i])
            var t = Int128(tax[i])
            var ed = e * (Int128(100) - d)  # scale4
            var charge = ed * (Int128(100) + t)  # scale6
            ref_cnt[gid] += 1
            ref_sqty[gid] += Int128(qty[i])
            ref_sext[gid] += e
            ref_sdisc[gid] += d
            ref_sdp[gid] += ed
            ref_sch[gid] += charge

    # ---- build the Q1 RawPlan tape ----
    var b = TapeBuilder()
    build_q1_tape(b, ship_cutoff)
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

    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q1, "kind != Q1")
    assert_equal(
        Int64(mojo_gpu_desc_strategy(h)),
        STRAT_DENSE_GROUP,
        "strategy != DENSE_GROUP",
    )
    assert_equal(mojo_gpu_desc_out_arity(h), 10, "out_arity != 10")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 1, "materialize_count != 1")

    var cap = 1024
    var sql_buf = alloc[UInt8](cap)
    var sql_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    assert_true(sql_len > 0, "materialize_sql returned empty")
    var sql = String("")
    for i in range(sql_len):
        sql += chr(Int(sql_buf[i]))
    print("materialize SQL:", sql)

    # Expected order: group keys first (l_returnflag, l_linestatus), then fact
    # filter cols (l_shipdate), then agg-program LOAD_COLs
    # (l_quantity, l_extendedprice, l_discount, l_tax).
    var order: List[String] = [
        String("l_returnflag"),
        String("l_linestatus"),
        String("l_shipdate"),
        String("l_quantity"),
        String("l_extendedprice"),
        String("l_discount"),
        String("l_tax"),
    ]
    var expect_sql = String("SELECT ")
    for j in range(len(order)):
        if j > 0:
            expect_sql += ", "
        expect_sql += order[j]
    expect_sql += " FROM lineitem"
    assert_equal(sql, expect_sql, "materialize_sql order mismatch")

    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    # feed each column in mat_cols order.
    for j in range(len(order)):
        var name = order[j]
        var rc: Int
        if name == "l_returnflag":
            rc = mojo_gpu_feed_column(
                h, 0, j, rf.bitcast[NoneType](), N, TYPE_VARCHAR
            )
        elif name == "l_linestatus":
            rc = mojo_gpu_feed_column(
                h, 0, j, ls.bitcast[NoneType](), N, TYPE_VARCHAR
            )
        elif name == "l_shipdate":
            rc = mojo_gpu_feed_column(
                h, 0, j, ship.bitcast[NoneType](), N, TYPE_DATE
            )
        elif name == "l_quantity":
            rc = mojo_gpu_feed_column(
                h, 0, j, qty.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif name == "l_extendedprice":
            rc = mojo_gpu_feed_column(
                h, 0, j, ext.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif name == "l_discount":
            rc = mojo_gpu_feed_column(
                h, 0, j, disc.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif name == "l_tax":
            rc = mojo_gpu_feed_column(
                h, 0, j, tax.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        else:
            raise Error("unexpected column: " + name)
        assert_equal(rc, 0, "feed_column rc for " + name)

    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    var rows = mojo_gpu_result_rows(h)
    assert_equal(rows, ng_distinct, "result_rows != n_groups")

    # ---- verify each group x metric ----
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    var sbuf = alloc[UInt8](64)

    def read_i128(
        h: UnsafePointer[NoneType, MutAnyOrigin],
        row: Int,
        col: Int,
        lo: UnsafePointer[Int64, MutAnyOrigin],
        hi: UnsafePointer[Int64, MutAnyOrigin],
    ) raises -> Int128:
        var rc = mojo_gpu_result_i128(h, row, col, lo, hi)
        assert_equal(rc, 0, "result_i128 rc")
        return (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))

    def read_str(
        h: UnsafePointer[NoneType, MutAnyOrigin],
        row: Int,
        col: Int,
        sbuf: UnsafePointer[UInt8, MutAnyOrigin],
    ) raises -> String:
        var n = mojo_gpu_result_str(h, row, col, sbuf, 64)
        var s = String("")
        for k in range(n):
            s += chr(Int(sbuf[k]))
        return s

    for g in range(ng_distinct):
        # group keys (sorted-rank g -> distinct idx ord[g])
        var didx = ord[g]
        var exp_rf = seen_rf[didx]
        var exp_ls = seen_ls[didx]
        var got_rf = read_str(h, g, 0, sbuf)
        var got_ls = read_str(h, g, 1, sbuf)
        assert_equal(got_rf, exp_rf, "group rf mismatch row " + String(g))
        assert_equal(got_ls, exp_ls, "group ls mismatch row " + String(g))

        var sum_qty = read_i128(h, g, 2, lo, hi)
        var sum_ext = read_i128(h, g, 3, lo, hi)
        var sum_dp = read_i128(h, g, 4, lo, hi)
        var sum_ch = read_i128(h, g, 5, lo, hi)
        assert_equal(sum_qty, ref_sqty[g], "sum_qty mismatch row " + String(g))
        assert_equal(sum_ext, ref_sext[g], "sum_ext mismatch row " + String(g))
        assert_equal(sum_dp, ref_sdp[g], "sum_disc_price mismatch row " + String(g))
        assert_equal(sum_ch, ref_sch[g], "sum_charge mismatch row " + String(g))

        var cnt = mojo_gpu_result_i64(h, g, 9)
        assert_equal(cnt, ref_cnt[g], "count mismatch row " + String(g))

        var dcnt = Float64(ref_cnt[g])
        var exp_aqty = (
            Float64(ref_sqty[g].cast[DType.int64]()) / 100.0 / dcnt
        ) if ref_cnt[g] != 0 else 0.0
        var exp_aprice = (
            Float64(ref_sext[g].cast[DType.int64]()) / 100.0 / dcnt
        ) if ref_cnt[g] != 0 else 0.0
        var exp_adisc = (
            Float64(ref_sdisc[g].cast[DType.int64]()) / 100.0 / dcnt
        ) if ref_cnt[g] != 0 else 0.0
        var got_aqty = mojo_gpu_result_f64(h, g, 6)
        var got_aprice = mojo_gpu_result_f64(h, g, 7)
        var got_adisc = mojo_gpu_result_f64(h, g, 8)
        assert_true(abs(got_aqty - exp_aqty) < 1e-6, "avg_qty mismatch row " + String(g))
        assert_true(abs(got_aprice - exp_aprice) < 1e-6, "avg_price mismatch row " + String(g))
        assert_true(abs(got_adisc - exp_adisc) < 1e-9, "avg_disc mismatch row " + String(g))

        print(
            "group", g, exp_rf, exp_ls,
            " cnt=", cnt,
            " sum_qty=", sum_qty,
            " avg_disc=", got_adisc,
        )

    mojo_gpu_desc_free(h)
    print("ALL PASS")
