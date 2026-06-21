"""NR3 (GPU_OP_FILTER_OR) execution-shuttle test (Mojo-only, needs GPU).

Clone of q6_shuttle_test.mojo for the OR-of-equalities residual-filter slice:
synthesizes a small single-fact dataset, hand-builds an UNGROUPED `sum(x)` tape
with a PASS_PROGRAMS section encoding `WHERE a = 2 OR a = 9 OR a = 40` (== `a IN
(2,9,40)`, which DuckDB lowers to an OR-of-equalities residual filter), then drives
the FULL C-ABI shuttle:

    build_descriptor -> materialize_count/sql -> pin_begin -> feed_column x2
                     -> pin_finalize -> result_i128

It computes a CPU int128 reference applying the SAME OR predicate and asserts
bit-exact equality with the GPU result. Also checks the WARM path. Prints ALL PASS.

The OR program is (postfix, EXISTING opcodes only):
    LOAD a; PUSH 2; EQ; LOAD a; PUSH 9; EQ; ADD; LOAD a; PUSH 40; EQ; ADD
A row passes iff eval_program(...) != 0 (every EQ leaf is 0/1; ADD chains the OR).
There are NO pushed range filters, so the host pass column is all-1 and the OR
program alone gates (AND-composed via OP_MUL inside _pin_finalize_generic).

It then drives a SECOND tape exercising the GPU_OP_FILTER_OR *widening* to
OR-of-RANGE / inequality filters: ungrouped sum(x) WHERE a < LO OR a > HI, whose
PASS_PROGRAM uses the new comparison opcodes:
    LOAD a; PUSH LO; OP_LT; LOAD a; PUSH HI; OP_GT; OP_ADD
again asserting GPU == CPU int128 reference (COLD + WARM) with the SAME predicate.

Run from the repo root:
    GPU_OP_FILTER_OR=1 pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/or_filter_shuttle_test.mojo
(or: pixi run gpu-op-orfilter-test -- the flag is read C++-side at serialize time,
 but this test hand-builds the tape, so it does not depend on the env flag.)
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
    TYPE_INTEGER,
    TYPE_DECIMAL,
    CMP_EQ,
    AGG_SUM,
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_EQ,
    OP_LT,
    OP_GT,
    OP_ADD,
    KIND_Q6,
    IDX_NONE,
)
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N = 200_000  # small synthetic fact table


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


# OR-filter tape: 1 GET facts, 0 pushed filters, 0 group keys, 1 SUM agg (sum(x)),
# 1 PASS_PROGRAM encoding (a = k0 OR a = k1 OR a = k2). The 3 OR constants are
# passed in so the test's CPU reference uses the same values.
def build_or_tape(mut b: TapeBuilder, k0: Int, k1: Int, k2: Int):
    var s_facts = b.sid("facts")
    var s_a = b.sid("a")
    var s_x = b.sid("x")

    # HEADER
    b.put(RP_MAGIC)
    b.put(IDX_NONE)  # group_index (ungrouped)
    b.puti(0)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: 1 agg col (DECIMAL scale2)
    b.puti(1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(38)
    # CONSTS: 3 INTEGER OR constants
    b.puti(3)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0); b.puti(k0); b.puti(0); b.puti(-1)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0); b.puti(k1); b.puti(0); b.puti(-1)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0); b.puti(k2); b.puti(0); b.puti(-1)
    # GETS: 1 get, 0 pushed filters (the OR is residual -> PASS_PROGRAMS)
    b.puti(1)
    b.puti(s_facts); b.puti(N); b.puti(0)
    # JOINS: none
    b.puti(0)
    # GROUP_KEYS: none
    b.puti(0)
    # AGGREGATES: 1 SUM, program = LOAD x
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(2); b.puti(38); b.puti(1)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_x)
    # PASS_PROGRAMS: 1 entry, get_ordinal 0, OR-of-equalities over column a.
    #   LOAD a; PUSH c0; EQ; LOAD a; PUSH c1; EQ; ADD; LOAD a; PUSH c2; EQ; ADD
    b.puti(1)  # n_pass
    b.puti(0)  # get_ordinal
    b.puti(11)  # n_ops
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_a)
    b.put(OP_PUSH_CONST); b.puti(0); b.puti(0)
    b.put(OP_EQ); b.puti(0); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_a)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_EQ); b.puti(0); b.puti(0)
    b.put(OP_ADD); b.puti(0); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_a)
    b.put(OP_PUSH_CONST); b.puti(2); b.puti(0)
    b.put(OP_EQ); b.puti(0); b.puti(0)
    b.put(OP_ADD); b.puti(0); b.puti(0)


# OR-of-RANGE tape (GPU_OP_FILTER_OR widening): identical structure to build_or_tape
# but the PASS_PROGRAM encodes `WHERE a < LO OR a > HI` using the new comparison
# opcodes OP_LT / OP_GT (each a 0/1 leaf, ORed via OP_ADD):
#   LOAD a; PUSH LO; OP_LT; LOAD a; PUSH HI; OP_GT; OP_ADD
# Only 2 INTEGER constants (LO, HI) live in the CONSTS pool.
def build_or_range_tape(mut b: TapeBuilder, lo: Int, hi: Int):
    var s_facts = b.sid("facts")
    var s_a = b.sid("a")
    var s_x = b.sid("x")

    # HEADER
    b.put(RP_MAGIC)
    b.put(IDX_NONE)  # group_index (ungrouped)
    b.puti(0)  # aggregate_index
    # STRING_TABLE
    b.emit_string_table()
    # OUT_TYPES: 1 agg col (DECIMAL scale2)
    b.puti(1)
    b.put(TYPE_DECIMAL); b.puti(2); b.puti(38)
    # CONSTS: 2 INTEGER range constants (LO, HI)
    b.puti(2)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0); b.puti(lo); b.puti(0); b.puti(-1)
    b.put(TYPE_INTEGER); b.puti(0); b.puti(0); b.puti(hi); b.puti(0); b.puti(-1)
    # GETS: 1 get, 0 pushed filters (the OR-of-range is residual -> PASS_PROGRAMS)
    b.puti(1)
    b.puti(s_facts); b.puti(N); b.puti(0)
    # JOINS: none
    b.puti(0)
    # GROUP_KEYS: none
    b.puti(0)
    # AGGREGATES: 1 SUM, program = LOAD x
    b.puti(1)
    b.put(AGG_SUM); b.put(TYPE_DECIMAL); b.puti(2); b.puti(38); b.puti(1)
    b.puti(1)
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_x)
    # PASS_PROGRAMS: 1 entry, get_ordinal 0, OR-of-range over column a.
    #   LOAD a; PUSH LO; OP_LT; LOAD a; PUSH HI; OP_GT; OP_ADD
    b.puti(1)  # n_pass
    b.puti(0)  # get_ordinal
    b.puti(7)  # n_ops
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_a)
    b.put(OP_PUSH_CONST); b.puti(0); b.puti(0)
    b.put(OP_LT); b.puti(0); b.puti(0)
    b.put(OP_LOAD_COL); b.puti(s_facts); b.puti(s_a)
    b.put(OP_PUSH_CONST); b.puti(1); b.puti(0)
    b.put(OP_GT); b.puti(0); b.puti(0)
    b.put(OP_ADD); b.puti(0); b.puti(0)


# Drive the FULL C-ABI shuttle for ONE hand-built tape against a precomputed CPU
# int128 reference, checking COLD + WARM paths. `expect_kind` lets both the OR-of-eq
# and OR-of-range tapes (both KIND_Q6 UNGROUPED) reuse this. Asserts bit-exact.
def run_shuttle(
    label: String,
    tptr: UnsafePointer[Int64, MutAnyOrigin],
    tlen: Int,
    bptr: UnsafePointer[UInt8, MutAnyOrigin],
    blen: Int,
    a: UnsafePointer[Int32, MutAnyOrigin],
    x: UnsafePointer[Int64, MutAnyOrigin],
    cpu: Int128,
) raises:
    print("--- shuttle:", label, "---")
    var handle_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    assert_true(handle_int != 0, label + ": build_descriptor returned 0 (rejected)")
    var h = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=handle_int
    )
    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q6, label + ": kind != Q6")
    assert_equal(mojo_gpu_desc_out_arity(h), 1, label + ": out_arity != 1")
    assert_equal(
        mojo_gpu_desc_materialize_count(h), 1, label + ": materialize_count != 1"
    )

    var cap = 512
    var sql_buf = alloc[UInt8](cap)
    var sql_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    assert_true(sql_len > 0, label + ": materialize_sql returned empty")
    var sql = String("")
    for i in range(sql_len):
        sql += chr(Int(sql_buf[i]))
    print(label, "materialize SQL:", sql)
    # column order: pass-program columns (a) -> agg-program LOAD_COLs (x) => [a, x]
    assert_equal(sql, String("SELECT a, x FROM facts"), label + ": SQL mismatch")

    var pb = mojo_gpu_pin_begin(h)
    print(label, "pin_begin:", pb, "(0=WARM, 1=COLD)")

    # feed order: [a, x]
    var rc0 = mojo_gpu_feed_column(
        h, 0, 0, a.bitcast[NoneType](), N, TYPE_INTEGER
    )
    assert_equal(rc0, 0, label + ": feed_column rc for a")
    var rc1 = mojo_gpu_feed_column(
        h, 0, 1, x.bitcast[NoneType](), N, TYPE_DECIMAL
    )
    assert_equal(rc1, 0, label + ": feed_column rc for x")

    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, label + ": pin_finalize rc")
    assert_equal(mojo_gpu_result_rows(h), 1, label + ": result_rows != 1")
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    var rr = mojo_gpu_result_i128(h, 0, 0, lo, hi)
    assert_equal(rr, 0, label + ": result_i128 rc")
    var gpu = Int128(hi[0]) << 64
    gpu += Int128(UInt64(lo[0]))
    print(label, "CPU ref =", cpu, "  GPU =", gpu)
    assert_equal(gpu, cpu, label + ": GPU sum != CPU reference (not bit-exact)")
    mojo_gpu_desc_free(h)


def main() raises:
    comptime assert has_accelerator(), "or_filter_shuttle_test requires a GPU"

    # ---- OR constants (a IN (2, 9, 40)) ----
    var k0 = 2
    var k1 = 9
    var k2 = 40

    # ---- synthetic single-fact columns ----
    var a = alloc[Int32](N)
    var x = alloc[Int64](N)
    for i in range(N):
        # `a` in 0..49 so the three OR values are hit by a reasonable fraction.
        a[i] = Int32((i * 1103515245 + 12345) % 50)
        x[i] = Int64(100 + (i * 16807) % 9_999_900)  # ~ up to 1e7 (scale2)

    # ---- CPU int128 reference (same OR predicate over `a`) ----
    var cpu = Int128(0)
    for i in range(N):
        var av = Int(a[i])
        if av == k0 or av == k1 or av == k2:
            cpu += Int128(x[i])

    # ---- build the OR-filter RawPlan tape ----
    var b = TapeBuilder()
    build_or_tape(b, k0, k1, k2)
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

    # 1 agg, 0 dims, 0 group keys -> KIND_Q6 UNGROUPED (same classification as Q6).
    assert_equal(Int64(mojo_gpu_desc_kind(h)), KIND_Q6, "kind != Q6")
    assert_equal(mojo_gpu_desc_out_arity(h), 1, "out_arity != 1")

    var count = mojo_gpu_desc_materialize_count(h)
    assert_equal(count, 1, "materialize_count != 1")

    # materialize SQL: must name both fact columns. Order from
    # fact_projected_columns: pushed fact filters (none) -> pass-program columns
    # (a) -> agg-program LOAD_COLs (x). So the order is exactly [a, x].
    var cap = 512
    var sql_buf = alloc[UInt8](cap)
    var sql_len = mojo_gpu_desc_materialize_sql(h, 0, sql_buf, cap)
    assert_true(sql_len > 0, "materialize_sql returned empty")
    var sql = String("")
    for i in range(sql_len):
        sql += chr(Int(sql_buf[i]))
    print("materialize SQL:", sql)
    assert_true("facts" in sql, "SQL missing fact table")
    assert_true(" a" in sql or "a," in sql, "SQL missing OR column a")
    assert_true(" x" in sql, "SQL missing agg column x")

    var order: List[String] = [String("a"), String("x")]
    var expect_sql = String("SELECT ")
    for j in range(len(order)):
        if j > 0:
            expect_sql += ", "
        expect_sql += order[j]
    expect_sql += " FROM facts"
    assert_equal(sql, expect_sql, "materialize_sql order mismatch")

    # pin_begin (COLD on first call).
    var pb = mojo_gpu_pin_begin(h)
    print("pin_begin:", pb, "(0=WARM, 1=COLD)")

    def feed(
        h: UnsafePointer[NoneType, MutAnyOrigin],
        col_j: Int,
        name: String,
        a: UnsafePointer[Int32, MutAnyOrigin],
        x: UnsafePointer[Int64, MutAnyOrigin],
    ) raises:
        var rc: Int
        if name == "a":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, a.bitcast[NoneType](), N, TYPE_INTEGER
            )
        elif name == "x":
            rc = mojo_gpu_feed_column(
                h, 0, col_j, x.bitcast[NoneType](), N, TYPE_DECIMAL
            )
        else:
            raise Error("unexpected column in feed order: " + name)
        assert_equal(rc, 0, "feed_column rc for " + name)

    for j in range(len(order)):
        feed(h, j, order[j], a, x)

    # pin_finalize: runs the kernel with the AND-composed OR pass program.
    var fr = mojo_gpu_pin_finalize(h)
    assert_equal(fr, 0, "pin_finalize rc")

    # results.
    assert_equal(mojo_gpu_result_rows(h), 1, "result_rows != 1")
    var lo = alloc[Int64](1)
    var hi = alloc[Int64](1)
    var rr = mojo_gpu_result_i128(h, 0, 0, lo, hi)
    assert_equal(rr, 0, "result_i128 rc")
    var gpu = Int128(hi[0]) << 64
    gpu += Int128(UInt64(lo[0]))
    print("CPU ref =", cpu, "  GPU =", gpu)
    assert_equal(gpu, cpu, "GPU OR-filter sum != CPU reference (not bit-exact)")

    # ---- WARM-path check: a second identical run should hit the pin cache. ----
    var handle2_int = mojo_gpu_build_descriptor(tptr, tlen, bptr, blen)
    var h2 = UnsafePointer[NoneType, MutAnyOrigin](
        unsafe_from_address=handle2_int
    )
    _ = mojo_gpu_desc_materialize_sql(h2, 0, sql_buf, cap)
    var pb2 = mojo_gpu_pin_begin(h2)
    print("second pin_begin:", pb2, "(expect 0=WARM)")
    assert_equal(pb2, 0, "second identical query should be WARM")
    var fr2 = mojo_gpu_pin_finalize(h2)
    assert_equal(fr2, 0, "warm pin_finalize rc")
    var rr2 = mojo_gpu_result_i128(h2, 0, 0, lo, hi)
    assert_equal(rr2, 0, "warm result_i128 rc")
    var gpu_warm = Int128(hi[0]) << 64
    gpu_warm += Int128(UInt64(lo[0]))
    assert_equal(gpu_warm, cpu, "WARM GPU OR-filter sum != CPU reference")

    mojo_gpu_desc_free(h2)
    mojo_gpu_desc_free(h)

    # ====================================================================
    # OR-of-RANGE slice (GPU_OP_FILTER_OR widening): sum(x) WHERE a < LO OR a > HI
    # over the SAME synthetic columns, using the new OP_LT / OP_GT comparison
    # opcodes. Compute the CPU int128 reference with the SAME predicate and assert
    # the GPU result is bit-exact (COLD + WARM).
    # ====================================================================
    var lo_k = 8   # a < 8
    var hi_k = 42  # a > 42  (a is in 0..49)
    var cpu_range = Int128(0)
    for i in range(N):
        var av = Int(a[i])
        if av < lo_k or av > hi_k:
            cpu_range += Int128(x[i])

    var br = TapeBuilder()
    build_or_range_tape(br, lo_k, hi_k)
    var rtlen = len(br.tape)
    var rtptr = alloc[Int64](rtlen if rtlen > 0 else 1)
    for i in range(rtlen):
        rtptr[i] = br.tape[i]
    var rblen = len(br.blob)
    var rbptr = alloc[UInt8](rblen if rblen > 0 else 1)
    for i in range(rblen):
        rbptr[i] = br.blob[i]

    # COLD run.
    run_shuttle(
        "OR-range COLD", rtptr, rtlen, rbptr, rblen, a, x, cpu_range
    )
    # WARM run (rebuild identical descriptor -> should hit the pin cache).
    run_shuttle(
        "OR-range WARM", rtptr, rtlen, rbptr, rblen, a, x, cpu_range
    )

    print("ALL PASS")
