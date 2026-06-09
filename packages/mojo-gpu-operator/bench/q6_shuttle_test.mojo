"""Stage-2 execution-shuttle test for the Q6 query class (Mojo-only, needs GPU).

Synthesizes a small lineitem-like dataset in host memory, hand-builds the Q6
RawPlan tape (same style as raw_plan_roundtrip_test.mojo), then drives the FULL
C-ABI shuttle by calling the @export functions in gpu_kernels.mojo directly:

    build_descriptor -> materialize_count/sql -> pin_begin -> feed_column x4
                     -> pin_finalize -> result_i128

It computes a CPU int128 reference Q6 sum over the synthetic data with the same
filter and asserts bit-exact equality with the GPU result. Prints ALL PASS.

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/q6_shuttle_test.mojo
"""

from gpu_kernels import (
    mojo_gpu_build_descriptor,
    mojo_gpu_desc_free,
    mojo_gpu_desc_kind,
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
    CMP_GE,
    CMP_LT,
    CMP_LE,
    AGG_SUM,
    OP_LOAD_COL,
    OP_MUL,
    KIND_Q6,
    IDX_NONE,
)
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N = 200_000  # small synthetic lineitem


# ---------------------------------------------------------------------------
# Tape/blob builder (mirrors raw_plan_roundtrip_test.mojo).
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


# Q6 tape: 1 GET lineitem, 5 filters, 0 group keys, 1 SUM agg (ext*disc).
# Filter constants are passed in (so the test's CPU reference uses the same).
def build_q6_tape(
    mut b: TapeBuilder,
    ship_lo: Int,
    ship_hi: Int,
    disc_lo: Int,
    disc_hi: Int,
    qty_hi: Int,
):
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
    # OUT_TYPES: 1 agg col (DECIMAL scale4)
    b.puti(1)
    b.put(TYPE_DECIMAL)
    b.puti(4)
    b.puti(38)
    # CONSTS: 5 filter constants
    b.puti(5)
    # c0: l_shipdate >= (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_lo); b.puti(0); b.puti(-1)
    # c1: l_shipdate < (date days)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_hi); b.puti(0); b.puti(-1)
    # c2: l_discount >= (scale2)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(disc_lo); b.puti(0); b.puti(-1)
    # c3: l_discount <= (scale2)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(disc_hi); b.puti(0); b.puti(-1)
    # c4: l_quantity < (scale2)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(qty_hi); b.puti(0); b.puti(-1)
    # GETS: 1 get, 5 filters
    b.puti(1)
    b.puti(s_li); b.puti(N); b.puti(5)
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


def main() raises:
    comptime assert has_accelerator(), "q6_shuttle_test requires a GPU"

    # ---- Q6 filter constants (mirror the kernel's expected scalings) ----
    var ship_lo = 8766
    var ship_hi = 9131
    var disc_lo = Int64(5)  # 0.05 scale2
    var disc_hi = Int64(7)  # 0.07 scale2
    var qty_hi = Int64(2400)  # 24.00 scale2

    # ---- synthetic lineitem-like columns ----
    var ship = alloc[Int32](N)
    var disc = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var qty = alloc[Int64](N)
    for i in range(N):
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)  # day number
        disc[i] = Int64((i * 48271) % 11)  # 0..10 (scale2)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)  # ~ up to 1e7 (scale2)
        qty[i] = Int64(1 + (i * 22695477) % 5000)  # 1..5000 (scale2)

    # ---- CPU int128 reference (same filter + ext*disc product) ----
    var cpu = Int128(0)
    for i in range(N):
        var sd = ship[i]
        if sd >= Int32(ship_lo) and sd < Int32(ship_hi):
            var d = disc[i]
            if d >= disc_lo and d <= disc_hi and qty[i] < qty_hi:
                cpu += Int128(ext[i]) * Int128(d)

    # ---- build the Q6 RawPlan tape ----
    var b = TapeBuilder()
    build_q6_tape(
        b, ship_lo, ship_hi, Int(disc_lo), Int(disc_hi), Int(qty_hi)
    )
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

    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q6, "kind != Q6")
    assert_equal(mojo_gpu_desc_out_arity(h), 1, "out_arity != 1")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 1, "materialize_count != 1")

    # materialize SQL: must name the 4 fact columns.
    var cap = 512
    var sql_buf = alloc[UInt8](cap)
    var sql_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    assert_true(sql_len > 0, "materialize_sql returned empty")
    var sql = String("")
    for i in range(sql_len):
        sql += chr(Int(sql_buf[i]))
    print("materialize SQL:", sql)
    assert_true("l_extendedprice" in sql, "SQL missing l_extendedprice")
    assert_true("l_discount" in sql, "SQL missing l_discount")
    assert_true("l_shipdate" in sql, "SQL missing l_shipdate")
    assert_true("l_quantity" in sql, "SQL missing l_quantity")
    assert_true("lineitem" in sql, "SQL missing fact table")

    # The materialize column order C++ must feed in is deterministic
    # (fact_projected_columns): fact filters first in filter order
    # (l_shipdate, l_discount, l_quantity), then agg-program LOAD_COLs
    # (l_extendedprice). Verify the SQL emits exactly this order, then feed in it.
    var order: List[String] = [
        String("l_shipdate"),
        String("l_discount"),
        String("l_quantity"),
        String("l_extendedprice"),
    ]
    var expect_sql = String("SELECT ")
    for j in range(len(order)):
        if j > 0:
            expect_sql += ", "
        expect_sql += order[j]
    expect_sql += " FROM lineitem"
    assert_equal(sql, expect_sql, "materialize_sql order mismatch")
    var order_str = String("")
    for j in range(len(order)):
        if j > 0:
            order_str += ", "
        order_str += order[j]
    print("feed order:", order_str)

    # pin_begin (COLD on first call).
    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    # feed each column in the SQL/order indexing.
    def feed(
        h: UnsafePointer[NoneType, MutAnyOrigin],
        col_j: Int,
        name: String,
        ship: UnsafePointer[Int32, MutAnyOrigin],
        disc: UnsafePointer[Int64, MutAnyOrigin],
        ext: UnsafePointer[Int64, MutAnyOrigin],
        qty: UnsafePointer[Int64, MutAnyOrigin],
    ) raises:
        var rc: Int
        if name == "l_shipdate":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, ship.bitcast[NoneType](), N, TYPE_DATE
            )
        elif name == "l_discount":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, disc.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif name == "l_extendedprice":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, ext.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        elif name == "l_quantity":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, qty.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        else:
            raise Error("unexpected column in feed order: " + name)
        assert_equal(rc, 0, "feed_column rc for " + name)

    for j in range(len(order)):
        feed(h, j, order[j], ship, disc, ext, qty)

    # pin_finalize: runs the existing Q6 kernel + int128 reduce.
    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    # results.
    assert_equal(mojo_gpu_result_rows(h), 1, "result_rows != 1")
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    var rr = mojo_gpu_result_i128(h, 0, 0, lo, hi)
    assert_equal(rr, 0, "result_i128 rc")
    # Reassemble the int128 from the two limbs (low is unsigned).
    var gpu = Int128(hi[0]) << 64
    gpu += Int128(UInt64(lo[0]))
    print("CPU ref =", cpu, "  GPU =", gpu)
    assert_equal(gpu, cpu, "GPU Q6 sum != CPU reference (not bit-exact)")

    # ---- WARM-path check: a second identical run should hit the pin cache. ----
    var handle2_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    var h2 = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=handle2_int
    )
    _ = mojo_gpu_desc_materialize_sql(h2, 0, sql_buf, cap)
    var pb2 = mojo_gpu_pin_begin(h2)
    print("second pin_begin:", pb2, "(expect 0=WARM)")
    assert_equal(pb2, 0, "second identical query should be WARM")
    # feed is skipped on WARM; finalize reuses the cached Q6State.
    var fr2 = mojo_gpu_pin_finalize(h2)
    assert_equal(fr2, 0, "warm pin_finalize rc")
    var rr2 = mojo_gpu_result_i128(h2, 0, 0, lo, hi)
    assert_equal(rr2, 0, "warm result_i128 rc")
    var gpu_warm = Int128(lo[0]) + (Int128(hi[0]) << 64)
    assert_equal(gpu_warm, cpu, "WARM GPU Q6 sum != CPU reference")

    mojo_gpu_desc_free(h2)
    mojo_gpu_desc_free(h)
    print("ALL PASS")
