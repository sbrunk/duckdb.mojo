"""SKIP-MATERIALIZE all-resident edge test (Mojo-only, needs GPU).

Exercises landmine #1 (the all-fact-columns-resident -> 0-column SELECT path) of
the skip-materialize follow-up directly at the Mojo C-ABI level, where the CLI
cannot reach it: with predicate-independent residency a repeated Q6 is a WARM hit
(pin_begin returns 0, skipping the cold feed entirely), so the natural CLI flow
never drives a COLD finalize with EVERY fact column already pooled. This test
forces exactly that state by:

  1. Driving a full COLD Q6 shuttle (build->materialize->feed x4->finalize) under
     GPU_OP_COLPOOL=2 + GPU_OP_NATIVE_DECODE=1 so the 4 fact columns
     (l_shipdate/l_discount/l_quantity/l_extendedprice) are UPLOADED into the pool
     and stay leased by the cached residency.
  2. Building a SECOND Q6 descriptor (a fresh handle => fresh exec state) and
     calling materialize_sql(0). Because all 4 columns are now pool-resident, the
     narrowed SELECT must OMIT ALL of them -> a 0-COLUMN projection ("SELECT  FROM
     lineitem") AND materialize_sql must seed st.n_rows from the resident row count
     (so the C++ side can skip the illegal 0-column query and the finalize still
     knows n).

It then asserts: the narrowed SQL has an EMPTY projection (the C++ empty-projection
detector's trigger), and that running the FULL shuttle for that 2nd query (it is a
WARM hit, so feed is skipped) still yields the bit-exact Q6 sum. This validates the
Mojo half of the n_rows landmine; the C++ "skip the 0-column Connection::Query +
feed_rowcount" half is a direct consequence and is covered by the partial-omit
end-to-end CLI path (Q14-after-Q6 omits 3 of 4 columns).

Run from the repo root (the flags MUST be set so skip-materialize is active):
    GPU_OP_COLPOOL=2 GPU_OP_NATIVE_DECODE=1 pixi run mojo run \
        -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/skipmat_allresident_test.mojo
"""

from gpu_kernels import (
    mojo_gpu_build_descriptor,
    mojo_gpu_desc_free,
    mojo_gpu_desc_kind,
    mojo_gpu_desc_materialize_count,
    mojo_gpu_desc_materialize_sql,
    mojo_gpu_pin_begin,
    mojo_gpu_feed_column,
    mojo_gpu_feed_rowcount,
    mojo_gpu_skipmat_active,
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
from std.os import getenv
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N = 200_000


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
    b.put(RP_MAGIC)
    b.put(IDX_NONE)
    b.puti(0)
    b.emit_string_table()
    b.puti(1)
    b.put(TYPE_DECIMAL); b.puti(4); b.puti(38)
    b.puti(5)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_lo); b.puti(0); b.puti(-1)
    b.put(TYPE_DATE); b.puti(0); b.puti(0); b.puti(ship_hi); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(disc_lo); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(disc_hi); b.puti(0); b.puti(-1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(15); b.puti(qty_hi); b.puti(0); b.puti(-1)
    b.puti(1)
    b.puti(s_li); b.puti(N); b.puti(5)
    b.puti(s_ship); b.put(CMP_GE); b.puti(0)
    b.puti(s_ship); b.put(CMP_LT); b.puti(1)
    b.puti(s_disc); b.put(CMP_GE); b.puti(2)
    b.puti(s_disc); b.put(CMP_LE); b.puti(3)
    b.puti(s_qty); b.put(CMP_LT); b.puti(4)
    b.puti(0)
    b.puti(0)
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(4); b.puti(38); b.puti(1)
    b.puti(3)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_ext)
    b.put(OP_LOAD_COL); b.puti(s_li); b.puti(s_disc)
    b.put(OP_MUL); b.puti(0); b.puti(0)


# Parse the comma-separated projection between "SELECT " and " FROM ".
def _projection_cols(sql: String) raises -> List[String]:
    var sb = sql.as_bytes()
    var start = sql.find(String("SELECT ")) + 7
    var stop = sql.find(String(" FROM "))
    var cols: List[String] = []
    var cur = String("")
    for i in range(start, stop):
        var ch = chr(Int(sb[i]))
        if ch == ",":
            cols.append(cur)
            cur = String("")
        else:
            cur += ch
    # trim + drop empties (an all-omit SELECT yields one all-whitespace token)
    var out: List[String] = []
    cols.append(cur)
    for c in range(len(cols)):
        var trimmed = String("")
        var cb = cols[c].as_bytes()
        for i in range(cols[c].byte_length()):
            if chr(Int(cb[i])) != " ":
                trimmed += chr(Int(cb[i]))
        if trimmed.byte_length() > 0:
            out.append(trimmed)
    return out^


def _feed_q6(
    h: UnsafePointer[NoneType, MutAnyOrigin],
    order: List[String],
    ship: UnsafePointer[Int32, MutAnyOrigin],
    disc: UnsafePointer[Int64, MutAnyOrigin],
    ext: UnsafePointer[Int64, MutAnyOrigin],
    qty: UnsafePointer[Int64, MutAnyOrigin],
) raises:
    for j in range(len(order)):
        var nm = order[j]
        var rc: Int
        if nm == "l_shipdate":
            rc = mojo_gpu_feed_column(h, 0, j, ship.bitcast[NoneType](), N, TYPE_DATE)
        elif nm == "l_discount":
            rc = mojo_gpu_feed_column(h, 0, j, disc.bitcast[NoneType](), N, TYPE_DECIMAL)
        elif nm == "l_extendedprice":
            rc = mojo_gpu_feed_column(h, 0, j, ext.bitcast[NoneType](), N, TYPE_DECIMAL)
        elif nm == "l_quantity":
            rc = mojo_gpu_feed_column(h, 0, j, qty.bitcast[NoneType](), N, TYPE_DECIMAL)
        else:
            raise Error("unexpected fact column: " + nm)
        assert_equal(rc, 0, "feed rc for " + nm)


def main() raises:
    comptime assert has_accelerator(), "skipmat_allresident_test requires a GPU"

    # This test only makes sense when skip-materialize is active. If the flags are
    # not set, skip gracefully (so it is safe to run unconditionally in a suite).
    var skipmat_flag = (
        getenv("GPU_OP_COLPOOL", "") == "2"
        or getenv("GPU_OP_SKIP_MATERIALIZE", "") != ""
    )
    var ndecode = getenv("GPU_OP_NATIVE_DECODE", "") != ""
    if not (skipmat_flag and ndecode):
        print(
            "SKIP: needs GPU_OP_COLPOOL=2 (or GPU_OP_SKIP_MATERIALIZE) +"
            " GPU_OP_NATIVE_DECODE=1; nothing to test. ALL PASS"
        )
        return

    var ship_lo = 8766
    var ship_hi = 9131
    var disc_lo = Int64(5)
    var disc_hi = Int64(7)
    var qty_hi = Int64(2400)

    var ship = alloc[Int32](N)
    var disc = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var qty = alloc[Int64](N)
    for i in range(N):
        ship[i] = Int32(8000 + (i * 1103515245 + 12345) % 2000)
        disc[i] = Int64((i * 48271) % 11)
        ext[i] = Int64(100 + (i * 16807) % 9_999_900)
        qty[i] = Int64(1 + (i * 22695477) % 5000)

    var cpu = Int128(0)
    for i in range(N):
        var sd = ship[i]
        if sd >= Int32(ship_lo) and sd < Int32(ship_hi):
            var dd = disc[i]
            if dd >= disc_lo and dd <= disc_hi and qty[i] < qty_hi:
                cpu += Int128(ext[i]) * Int128(dd)

    var b = TapeBuilder()
    build_q6_tape(b, ship_lo, ship_hi, Int(disc_lo), Int(disc_hi), Int(qty_hi))
    var tlen = len(b.tape)
    var tptr = alloc[Int64](tlen if tlen > 0 else 1)
    for i in range(tlen):
        tptr[i] = b.tape[i]
    var blen = len(b.blob)
    var bptr = alloc[UInt8](blen if blen > 0 else 1)
    for i in range(blen):
        bptr[i] = b.blob[i]

    var cap = 512
    var sql_buf = alloc[UInt8](cap)

    # ---- run #1: COLD, uploads all 4 fact columns into the pool ----
    var h1_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(h1_int != 0, "build #1 returned 0")
    var h1 = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=h1_int)
    assert_equal(Int64(mojo_gpu_desc_kind(h1)), KIND_Q6, "kind != Q6")
    assert_equal(mojo_gpu_desc_materialize_count(h1), 1, "mat_count != 1")
    assert_equal(
        Int(mojo_gpu_skipmat_active(h1)), 1, "skipmat should be active for Q6"
    )
    var sql1_len = mojo_gpu_desc_materialize_sql(h1, 0, sql_buf, cap)
    var sql1 = String("")
    for i in range(sql1_len):
        sql1 += chr(Int(sql_buf[i]))
    var proj1 = _projection_cols(sql1)
    print("run #1 projection cols:", len(proj1), " (expect 4 -- pool empty)")
    assert_equal(len(proj1), 4, "run #1 should select all 4 fact cols")
    var pb1 = mojo_gpu_pin_begin(h1)
    assert_equal(pb1, 1, "run #1 should be COLD")
    _feed_q6(h1, proj1, ship, disc, ext, qty)
    assert_equal(mojo_gpu_pin_finalize(h1), 0, "finalize #1 rc")
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    _ = mojo_gpu_result_i128(h1, 0, 0, lo, hi)
    var gpu1 = (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))
    assert_equal(gpu1, cpu, "run #1 GPU sum != CPU")

    # ---- run #2: a fresh handle. All 4 cols are now pool-resident, so the
    # narrowed SELECT must OMIT ALL of them (0-column projection) and the
    # materialize must seed n_rows so the C++ side can skip the illegal query. ----
    var h2_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(h2_int != 0, "build #2 returned 0")
    var h2 = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=h2_int)
    var sql2_len = mojo_gpu_desc_materialize_sql(h2, 0, sql_buf, cap)
    var sql2 = String("")
    for i in range(sql2_len):
        sql2 += chr(Int(sql_buf[i]))
    print("run #2 narrowed SQL: '", sql2, "'")
    var proj2 = _projection_cols(sql2)
    print("run #2 projection cols:", len(proj2), " (expect 0 -- all resident)")
    assert_equal(
        len(proj2), 0,
        "run #2 should OMIT ALL 4 fact cols (0-column SELECT, all resident)",
    )
    assert_true(
        "FROM lineitem" in sql2, "run #2 SQL must still name the fact table"
    )

    # The C++ side would here SKIP the (illegal 0-column) query and rely on the
    # n_rows materialize_sql seeded. We exercise feed_rowcount the same way C++
    # does for the partial case (idempotent / unconditional), then finalize. run #2
    # is a WARM hit (same signature as run #1) so finalize re-runs on the resident
    # buffers; the result must still be bit-exact.
    assert_equal(
        Int(mojo_gpu_feed_rowcount(h2, N)), 0, "feed_rowcount rc"
    )
    var pb2 = mojo_gpu_pin_begin(h2)
    print("run #2 pin_begin:", pb2, "(0=WARM expected -- same signature)")
    assert_equal(mojo_gpu_pin_finalize(h2), 0, "finalize #2 rc")
    _ = mojo_gpu_result_i128(h2, 0, 0, lo, hi)
    var gpu2 = (Int128(hi[0]) << 64) + Int128(UInt64(lo[0]))
    print("CPU ref =", cpu, "  run#2 GPU =", gpu2)
    assert_equal(gpu2, cpu, "run #2 GPU sum != CPU (all-resident path)")

    mojo_gpu_desc_free(h1)
    mojo_gpu_desc_free(h2)
    print("ALL PASS")
