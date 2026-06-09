"""Round-trip test for the RawPlan -> descriptor boundary (pure Mojo, no GPU).

Hand-builds 5 TPC-H query tapes following RAW_PLAN_CONTRACT.md, then asserts the
matcher brain (`build_descriptor_impl`) classifies each correctly. Imports
`descriptor` directly -- needs neither the GPU nor the built dylib.

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/raw_plan_roundtrip_test.mojo
"""

from descriptor import RawPlanReader, build_descriptor_impl
from raw_plan_tags import (
    RP_MAGIC,
    TYPE_INTEGER,
    TYPE_BIGINT,
    TYPE_HUGEINT,
    TYPE_DECIMAL,
    TYPE_DATE,
    TYPE_VARCHAR,
    CMP_EQ,
    CMP_LT,
    CMP_LE,
    CMP_GT,
    CMP_GE,
    AGG_SUM,
    AGG_AVG,
    AGG_COUNT_STAR,
    JOIN_INNER,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_MUL,
    OP_SUB,
    OP_SELECT,
    OP_PROMO_PRED,
    KIND_Q6,
    KIND_Q1,
    KIND_Q14,
    KIND_Q3,
    KIND_Q5,
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
    IDX_NONE,
)
from std.memory import alloc
from std.testing import assert_equal, assert_true


# ---------------------------------------------------------------------------
# Tape/blob builder. Strings are interned; sid() returns the string id.
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

    # Intern a string; dedups. Returns its string id.
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

    # Emit the STRING_TABLE section (call AFTER all strings interned upstream is
    # not required because we build the tape section-by-section; instead the
    # caller emits the table at the right spot by reading offs/lens).
    def emit_string_table(mut self):
        self.puti(len(self.names))
        for i in range(len(self.names)):
            self.puti(self.offs[i])
            self.puti(self.lens[i])


# Build a RawPlanReader from a finished builder. Copies tape+blob to heap so the
# reader's raw pointers stay valid.
def reader_from(b: TapeBuilder) -> RawPlanReader:
    var tlen = len(b.tape)
    var tptr = alloc[Int64](tlen if tlen > 0 else 1)
    for i in range(tlen):
        tptr[i] = b.tape[i]
    var blen = len(b.blob)
    var bptr = alloc[UInt8](blen if blen > 0 else 1)
    for i in range(blen):
        bptr[i] = b.blob[i]
    return RawPlanReader(tptr, tlen, bptr, blen)


# ---------------------------------------------------------------------------
# Q6: 1 GET lineitem, 5 filters, 0 group keys, 1 SUM agg (ext*disc).
# ---------------------------------------------------------------------------
def build_q6() -> RawPlanReader:
    var b = TapeBuilder()
    # Pre-intern the strings we need (order doesn't matter; ids are stable).
    var s_li = b.sid("lineitem")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_ship = b.sid("l_shipdate")
    var s_qty = b.sid("l_quantity")

    # HEADER
    b.put(RP_MAGIC)
    b.put(IDX_NONE)  # group_index (ungrouped)
    b.puti(0)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: 0 group cols + 1 agg col (the sum, DECIMAL scale4 int128)
    b.puti(1)
    b.put(TYPE_DECIMAL)
    b.puti(4)
    b.puti(38)
    # CONSTS: 5 filter constants (2 dates, disc lo/hi scale2, qty scale2)
    b.puti(5)
    # c0: l_shipdate >= date
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9131); b.puti(0); b.puti(-1)
    # c1: l_shipdate < date
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9496); b.puti(0); b.puti(-1)
    # c2: l_discount >= 0.05 (scale2 -> 5)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(5); b.puti(0); b.puti(-1)
    # c3: l_discount <= 0.07 (scale2 -> 7)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(7); b.puti(0); b.puti(-1)
    # c4: l_quantity < 24 (scale2 -> 2400)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(2400); b.puti(0); b.puti(-1)
    # GETS: 1 get, 5 filters
    b.puti(1)
    b.puti(s_li); b.puti(6_000_000); b.puti(5)
    b.puti(s_ship); b.put(CMP_GE); b.puti(0)
    b.puti(s_ship); b.put(CMP_LT); b.puti(1)
    b.puti(s_disc); b.put(CMP_GE); b.puti(2)
    b.puti(s_disc); b.put(CMP_LE); b.puti(3)
    b.puti(s_qty); b.put(CMP_LT); b.puti(4)
    # JOINS: none
    b.puti(0)
    # GROUP_KEYS: none
    b.puti(0)
    # AGGREGATES: 1 SUM, program = LOAD ext, LOAD disc, MUL
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(3)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    return reader_from(b)


# ---------------------------------------------------------------------------
# Q1: 1 GET lineitem, 1 filter, 2 VARCHAR group keys, 8 aggregates.
# ---------------------------------------------------------------------------
def build_q1() -> RawPlanReader:
    var b = TapeBuilder()
    var s_li = b.sid("lineitem")
    var s_rf = b.sid("l_returnflag")
    var s_ls = b.sid("l_linestatus")
    var s_ship = b.sid("l_shipdate")
    var s_qty = b.sid("l_quantity")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")

    b.put(RP_MAGIC)
    b.puti(0)  # group_index (grouped)
    b.puti(1)  # aggregate_index
    b.emit_string_table()
    # OUT_TYPES: 2 group cols (VARCHAR) + 8 agg cols (types not load-bearing here)
    b.puti(10)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)
    for _ in range(8):
        b.put(TYPE_DECIMAL); b.puti(2); b.puti(38)
    # CONSTS: 1 (l_shipdate <= date)
    b.puti(1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(10500); b.puti(0); b.puti(-1)
    # GETS: 1 get, 1 filter
    b.puti(1)
    b.puti(s_li); b.puti(6_000_000); b.puti(1)
    b.puti(s_ship); b.put(CMP_LE); b.puti(0)
    # JOINS: none
    b.puti(0)
    # GROUP_KEYS: 2 (lineitem.l_returnflag, lineitem.l_linestatus)
    b.puti(2)
    b.puti(s_li); b.puti(s_rf)
    b.puti(s_li); b.puti(s_ls)
    # AGGREGATES: 8 (4 SUM, 3 AVG, 1 COUNT_STAR). Trivial 1-op programs.
    b.puti(8)
    var sumcols = [s_qty, s_ext, s_disc, s_ext]
    for ci in range(4):
        b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(2); b.puti(38); b.puti(1)
        b.puti(1); b.put(OP_LOAD_COL); b.puti(s_li); b.puti(sumcols[ci])
    var avgcols = [s_qty, s_ext, s_disc]
    for ci in range(3):
        b.put(AGG_AVG); b.put(TYPE_DECIMAL); b.puti(6); b.puti(38); b.puti(1)
        b.puti(1); b.put(OP_LOAD_COL); b.puti(s_li); b.puti(avgcols[ci])
    b.put(AGG_COUNT_STAR); b.put(TYPE_BIGINT); b.puti(0); b.puti(0); b.puti(0)
    b.puti(0)
    return reader_from(b)


# ---------------------------------------------------------------------------
# Q14: 2 GETs lineitem(huge)+part(small), 1 INNER join, 2 lineitem filters,
# 0 group keys, 2 aggregates.
# ---------------------------------------------------------------------------
def build_q14() -> RawPlanReader:
    var b = TapeBuilder()
    var s_li = b.sid("lineitem")
    var s_part = b.sid("part")
    var s_lpk = b.sid("l_partkey")
    var s_ppk = b.sid("p_partkey")
    var s_ptype = b.sid("p_type")
    var s_ship = b.sid("l_shipdate")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")

    b.put(RP_MAGIC)
    b.put(IDX_NONE)
    b.puti(0)
    b.emit_string_table()
    # OUT_TYPES: 2 agg cols
    b.puti(2)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: 2 dates + a literal 1 + a literal 0
    b.puti(4)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9587); b.puti(0); b.puti(-1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9617); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(100); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(0); b.puti(0); b.puti(-1)
    # GETS: lineitem (huge, 2 filters) + part (small, 0 filters)
    b.puti(2)
    b.puti(s_li); b.puti(6_000_000); b.puti(2)
    b.puti(s_ship); b.put(CMP_GE); b.puti(0)
    b.puti(s_ship); b.put(CMP_LT); b.puti(1)
    b.puti(s_part); b.puti(200_000); b.puti(0)
    # JOINS: 1 INNER, 1 cond (lineitem.l_partkey = part.p_partkey)
    b.puti(1)
    b.put(JOIN_INNER); b.puti(1)
    b.puti(s_li); b.puti(s_lpk); b.puti(s_part); b.puti(s_ppk)
    # GROUP_KEYS: none
    b.puti(0)
    # AGGREGATES: 2.
    # agg0 (promo): PROMO_PRED(part,p_type); ext; PUSH 1; disc; SUB; MUL; PUSH 0; SELECT
    # agg1 (revenue): ext; PUSH 1; disc; SUB; MUL
    b.puti(2)
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
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    return reader_from(b)


# ---------------------------------------------------------------------------
# Q3: 3 GETs lineitem(huge)+orders+customer, 2 INNER conds, 3 filters,
# 3 group keys (l_orderkey BIGINT [fact], o_orderdate DATE, o_shippriority INT),
# 1 SUM revenue agg.
# ---------------------------------------------------------------------------
def build_q3() -> RawPlanReader:
    var b = TapeBuilder()
    var s_li = b.sid("lineitem")
    var s_ord = b.sid("orders")
    var s_cust = b.sid("customer")
    var s_lok = b.sid("l_orderkey")
    var s_ook = b.sid("o_orderkey")
    var s_cck = b.sid("c_custkey")
    var s_ock = b.sid("o_custkey")
    var s_mkt = b.sid("c_mktsegment")
    var s_odate = b.sid("o_orderdate")
    var s_ship = b.sid("l_shipdate")
    var s_sprio = b.sid("o_shippriority")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_seg = b.sid("BUILDING")

    b.put(RP_MAGIC)
    b.puti(0)
    b.puti(1)
    b.emit_string_table()
    # OUT_TYPES: 3 group cols (BIGINT, DATE, INTEGER) + 1 agg col
    b.puti(4)
    b.put(TYPE_BIGINT); b.puti(0); b.puti(0)
    b.put(TYPE_DATE); b.puti(0); b.puti(0)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: c_mktsegment EQ varchar, o_orderdate LT, l_shipdate GT
    b.puti(3)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0); b.puti(0); b.puti(0); b.puti(s_seg)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9204); b.puti(0); b.puti(-1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9204); b.puti(0); b.puti(-1)
    # GETS: lineitem(huge,1 filter), orders(1 filter), customer(1 filter)
    b.puti(3)
    b.puti(s_li); b.puti(6_000_000); b.puti(1)
    b.puti(s_ship); b.put(CMP_GT); b.puti(2)
    b.puti(s_ord); b.puti(1_500_000); b.puti(1)
    b.puti(s_odate); b.put(CMP_LT); b.puti(1)
    b.puti(s_cust); b.puti(150_000); b.puti(1)
    b.puti(s_mkt); b.put(CMP_EQ); b.puti(0)
    # JOINS: 1 INNER with 2 conds (l_orderkey=o_orderkey, c_custkey=o_custkey)
    b.puti(1)
    b.put(JOIN_INNER); b.puti(2)
    b.puti(s_li); b.puti(s_lok); b.puti(s_ord); b.puti(s_ook)
    b.puti(s_cust); b.puti(s_cck); b.puti(s_ord); b.puti(s_ock)
    # GROUP_KEYS: 3 (lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority)
    b.puti(3)
    b.puti(s_li); b.puti(s_lok)
    b.puti(s_ord); b.puti(s_odate)
    b.puti(s_ord); b.puti(s_sprio)
    # AGGREGATES: 1 SUM revenue
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(0); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    return reader_from(b)


# ---------------------------------------------------------------------------
# Q5: 6 GETs (snowflake), 6 INNER conds, filters, 1 VARCHAR dim group key,
# 1 SUM revenue agg.
# ---------------------------------------------------------------------------
def build_q5() -> RawPlanReader:
    var b = TapeBuilder()
    var s_li = b.sid("lineitem")
    var s_ord = b.sid("orders")
    var s_cust = b.sid("customer")
    var s_supp = b.sid("supplier")
    var s_nation = b.sid("nation")
    var s_region = b.sid("region")
    var s_cck = b.sid("c_custkey")
    var s_ock = b.sid("o_custkey")
    var s_lok = b.sid("l_orderkey")
    var s_ook = b.sid("o_orderkey")
    var s_lsk = b.sid("l_suppkey")
    var s_ssk = b.sid("s_suppkey")
    var s_cnk = b.sid("c_nationkey")
    var s_snk = b.sid("s_nationkey")
    var s_nnk = b.sid("n_nationkey")
    var s_nrk = b.sid("n_regionkey")
    var s_rrk = b.sid("r_regionkey")
    var s_rname = b.sid("r_name")
    var s_nname = b.sid("n_name")
    var s_odate = b.sid("o_orderdate")
    var s_ext = b.sid("l_extendedprice")
    var s_disc = b.sid("l_discount")
    var s_asia = b.sid("ASIA")

    b.put(RP_MAGIC)
    b.puti(0)
    b.puti(1)
    b.emit_string_table()
    # OUT_TYPES: 1 group col (VARCHAR) + 1 agg col
    b.puti(2)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    # CONSTS: r_name EQ varchar, o_orderdate GE, o_orderdate LT
    b.puti(3)
    b.put(TYPE_VARCHAR); b.puti(0); b.puti(0); b.puti(0); b.puti(0); b.puti(s_asia)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(8766); b.puti(0); b.puti(-1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(9131); b.puti(0); b.puti(-1)
    # GETS: lineitem(huge), orders(2 date filters), customer, supplier, nation, region(1 filter)
    b.puti(6)
    b.puti(s_li); b.puti(6_000_000); b.puti(0)
    b.puti(s_ord); b.puti(1_500_000); b.puti(2)
    b.puti(s_odate); b.put(CMP_GE); b.puti(1)
    b.puti(s_odate); b.put(CMP_LT); b.puti(2)
    b.puti(s_cust); b.puti(150_000); b.puti(0)
    b.puti(s_supp); b.puti(10_000); b.puti(0)
    b.puti(s_nation); b.puti(25); b.puti(0)
    b.puti(s_region); b.puti(5); b.puti(1)
    b.puti(s_rname); b.put(CMP_EQ); b.puti(0)
    # JOINS: 1 INNER with 6 conds (snowflake)
    b.puti(1)
    b.put(JOIN_INNER); b.puti(6)
    b.puti(s_cust); b.puti(s_cck); b.puti(s_ord); b.puti(s_ock)
    b.puti(s_li); b.puti(s_lok); b.puti(s_ord); b.puti(s_ook)
    b.puti(s_li); b.puti(s_lsk); b.puti(s_supp); b.puti(s_ssk)
    b.puti(s_cust); b.puti(s_cnk); b.puti(s_supp); b.puti(s_snk)
    b.puti(s_supp); b.puti(s_snk); b.puti(s_nation); b.puti(s_nnk)
    b.puti(s_nation); b.puti(s_nrk); b.puti(s_region); b.puti(s_rrk)
    # GROUP_KEYS: 1 (nation.n_name VARCHAR)
    b.puti(1)
    b.puti(s_nation); b.puti(s_nname)
    # AGGREGATES: 1 SUM revenue
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(5)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_SUB); b.puti(0); b.puti(0)
    b.put(OP_MUL); b.puti(0); b.puti(0)
    return reader_from(b)


def check(
    name: String,
    mut r: RawPlanReader,
    exp_kind: Int64,
    exp_strat: Int64,
    exp_dims: Int,
    exp_aggs: Int,
) raises:
    var maybe = build_descriptor_impl(r)
    assert_true(Bool(maybe), name + ": expected a descriptor, got None")
    ref d = maybe.value()
    assert_equal(d.kind, exp_kind, name + ": kind")
    assert_equal(d.strategy, exp_strat, name + ": strategy")
    assert_equal(len(d.dim_edges), exp_dims, name + ": n_dims")
    assert_equal(len(d.aggregates), exp_aggs, name + ": n_aggs")
    assert_equal(d.fact_table, String("lineitem"), name + ": fact_table")
    print(
        name,
        "OK  kind=",
        d.kind,
        " strat=",
        d.strategy,
        " dims=",
        len(d.dim_edges),
        " aggs=",
        len(d.aggregates),
        " fact=",
        d.fact_table,
    )


def main() raises:
    var q6 = build_q6()
    check("Q6", q6, KIND_Q6, STRAT_UNGROUPED, 0, 1)

    var q1 = build_q1()
    check("Q1", q1, KIND_Q1, STRAT_DENSE_GROUP, 0, 8)

    var q14 = build_q14()
    check("Q14", q14, KIND_Q14, STRAT_UNGROUPED, 1, 2)

    var q3 = build_q3()
    check("Q3", q3, KIND_Q3, STRAT_SORT_SEGREDUCE, 2, 1)

    var q5 = build_q5()
    check("Q5", q5, KIND_Q5, STRAT_DENSE_GROUP, 5, 1)

    print("ALL PASS")
