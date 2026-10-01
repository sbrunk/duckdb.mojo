"""GPU_OP_NULLABLE live-SQL end-to-end test (needs the GPU + the built extension).

Unlike the q*_shuttle tests (which hand-build a tape and bypass the C++ matcher),
this drives the full operator path through real SQL:

    LOAD extension -> optimizer match (SerializeMatchedPlan safe-slice gate) ->
    materialize -> feed_column + feed_validity -> _pin_finalize_generic validity
    fold into the host pass column -> result

It builds a single fact table with nullable columns (CTAS columns carry no NOT NULL
constraint) holding a deterministic NULL pattern, then asserts that every ungrouped
SUM/AVG query matches a CPU reference computed in Mojo with SQL NULL semantics
(rows with a NULL agg input or filter value are excluded; a sum over zero non-NULL
rows gives SQL NULL). The reference is the ground truth: if the operator read a
NULL row's garbage data the assertion fails, and if it correctly folds validity it
passes. If it declines, stock DuckDB runs and is also correct, so run with
GPU_OP_SHADOW=1 / GPU_OP_PIN_LOG=1 to confirm that the operator actually handled
these queries (see bench harness).

Run from the repo root with the extension built (`pixi run gpu-op-build`):
    GPU_OP_NULLABLE=1 pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/nullable_sql_test.mojo
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.os import getenv
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true


comptime N = 120_000  # fact rows


# --- the generation rule, mirrored on the CPU side for the reference ---------
# b (BIGINT, nullable):    NULL if i % 7 == 0 else (i % 1000)
# f (INTEGER, nullable):   NULL if i % 5 == 0 else (i % 100)
# g (BIGINT, nullable):    NULL for every row (all-NULL column)
def _b_null(i: Int) -> Bool:
    return i % 7 == 0


def _b_val(i: Int) -> Int:
    return i % 1000


def _f_null(i: Int) -> Bool:
    return i % 5 == 0


def _f_val(i: Int) -> Int:
    return i % 100


def main() raises:
    if not has_accelerator():
        print("no GPU accelerator -> skipping (the operator declines, stock is correct)")
        return

    var ext = getenv(
        "GPU_OP_EXT",
        "extensions/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )

    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")

    # With CTAS, columns b/f/g are nullable (no NOT NULL constraint), so the
    # operator declines all NULL-capable input unless GPU_OP_NULLABLE relaxes that
    # for this slice.
    _ = con.execute(
        String(
            "CREATE TABLE t AS SELECT"
            " CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 1000) END::BIGINT AS b,"
            " CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 100) END::INTEGER AS f,"
            " NULL::BIGINT AS g"
            " FROM range("
        )
        + String(N)
        + ") r(i)"
    )

    # ---- CPU references (SQL NULL semantics) --------------------------------
    var ref_sum_b = Int128(0)
    var ref_cnt_b = 0
    var ref_sum_b_f50 = Int128(0)  # WHERE f > 50
    var ref_sum_b_fbetween = Int128(0)  # WHERE f BETWEEN 20 AND 80
    var ref_sum_b_bgt500 = Int128(0)  # WHERE b > 500 (b is filter AND agg)
    var ref_cnt_b_f50 = 0  # count(non-null b) WHERE f > 50, for avg
    var ref_cnt_star = 0
    for i in range(N):
        ref_cnt_star += 1
        var b_ok = not _b_null(i)
        var f_ok = not _f_null(i)
        if b_ok:
            ref_sum_b += Int128(_b_val(i))
            ref_cnt_b += 1
            if f_ok and _f_val(i) > 50:
                ref_sum_b_f50 += Int128(_b_val(i))
                ref_cnt_b_f50 += 1
            if f_ok and _f_val(i) >= 20 and _f_val(i) <= 80:
                ref_sum_b_fbetween += Int128(_b_val(i))
            if _b_val(i) > 500:
                ref_sum_b_bgt500 += Int128(_b_val(i))

    # avg over the filtered set (f > 50). The nullable slice only handles SUM with
    # an INT128 result. AVG is not in the slice (DuckDB rewrites a nullable avg into
    # a `sum` aggregate with a DOUBLE output plus a division projection, so
    # ret_is_int128==0 and the gate hands it to stock). So avg here must equal
    # stock's correct answer (it is not folded on the GPU). See
    # avg_ungrouped_probe.mojo for the separate, older bug (ungrouped avg rewritten
    # as a DOUBLE sum, on by default) that this keeps out.
    var ref_avg_b_f50 = Float64(ref_sum_b_f50) / Float64(ref_cnt_b_f50)
    var ref_avg_b = Float64(ref_sum_b) / Float64(ref_cnt_b)  # bare avg, non-NULL b

    # ---- the SUM/AVG safe-slice suite (each must equal the CPU reference) ----
    print("N =", N, "  non-null b =", ref_cnt_b)

    var s0 = con.execute("SELECT sum(b) FROM t").fetch_chunk().get[
        Optional[Int128]
    ](col=0, row=0)
    assert_true(Bool(s0), "sum(b) should be non-NULL")
    assert_equal(s0.value(), ref_sum_b)
    print("[ok] sum(b)                       =", String(s0.value()))

    var s1 = con.execute("SELECT sum(b) FROM t WHERE f > 50").fetch_chunk().get[
        Optional[Int128]
    ](col=0, row=0)
    assert_true(Bool(s1), "filtered sum(b) should be non-NULL")
    assert_equal(s1.value(), ref_sum_b_f50)
    print("[ok] sum(b) WHERE f>50            =", String(s1.value()))

    var s2 = con.execute(
        "SELECT sum(b) FROM t WHERE f BETWEEN 20 AND 80"
    ).fetch_chunk().get[Optional[Int128]](col=0, row=0)
    assert_true(Bool(s2), "between sum(b) should be non-NULL")
    assert_equal(s2.value(), ref_sum_b_fbetween)
    print("[ok] sum(b) WHERE f BETWEEN 20,80 =", String(s2.value()))

    var s3 = con.execute("SELECT sum(b) FROM t WHERE b > 500").fetch_chunk().get[
        Optional[Int128]
    ](col=0, row=0)
    assert_true(Bool(s3), "b>500 sum(b) should be non-NULL")
    assert_equal(s3.value(), ref_sum_b_bgt500)
    print("[ok] sum(b) WHERE b>500           =", String(s3.value()))

    # AVG is now handled (sum m0 and count m1 are both pass-gated, so NULL-x is
    # excluded from both).
    var a0 = con.execute("SELECT avg(b) FROM t WHERE f > 50").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    assert_true(Bool(a0), "avg(b) WHERE f>50 should be non-NULL")
    var adiff = a0.value() - ref_avg_b_f50
    var aerr = adiff if adiff >= 0 else -adiff
    assert_true(aerr < 1e-6, "avg(b) WHERE f>50 routes + correct")
    print("[ok] avg(b) WHERE f>50            =", a0.value())

    var a1 = con.execute("SELECT avg(b) FROM t").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    assert_true(Bool(a1), "bare avg(b) should be non-NULL")
    var a1d = a1.value() - ref_avg_b
    assert_true((a1d if a1d >= 0 else -a1d) < 1e-6, "bare avg(b) routes + correct")
    print("[ok] avg(b)                       =", a1.value())

    # avg over an all-NULL column gives SQL NULL (count 0, so ungrouped_count_m marks NULL)
    var ag = con.execute("SELECT avg(g) FROM t").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    assert_true(not Bool(ag), "avg(all-NULL) must be SQL NULL")
    print("[ok] avg(g all-NULL)             = NULL")

    # ---- SQL NULL edge cases ------------------------------------------------
    # all-NULL column: a sum over zero non-NULL rows gives SQL NULL
    var sg = con.execute("SELECT sum(g) FROM t").fetch_chunk().get[
        Optional[Int128]
    ](col=0, row=0)
    assert_true(not Bool(sg), "sum(all-NULL) must be SQL NULL")
    print("[ok] sum(g all-NULL)             = NULL")

    # empty filter match gives SQL NULL (works together with the empty-sum NULL fix)
    var se = con.execute(
        "SELECT sum(b) FROM t WHERE f > 1000000"
    ).fetch_chunk().get[Optional[Int128]](col=0, row=0)
    assert_true(not Bool(se), "empty-match sum must be SQL NULL")
    print("[ok] sum(b) WHERE f>1e6 (empty)  = NULL")

    # ---- count(*) and multi-agg are now handled correctly (A1 unified pass model) ----
    # count(*) counts all rows (incl. NULL-b); routes as KIND_Q6 (single PUSH_CONST(1)).
    var c0 = con.execute("SELECT count(*) FROM t").fetch_chunk().get[Int64](
        col=0, row=0
    )
    assert_equal(Int(c0), ref_cnt_star)
    print("[ok] count(*)                     =", Int(c0))

    # count(*), sum(b): count(*) counts all rows incl. NULL-b (unmultiplied metric),
    # sum(b) excludes NULL-b (validity-multiplied), and A1 routes both correctly.
    var chunk = con.execute("SELECT count(*), sum(b) FROM t").fetch_chunk()
    var c1 = chunk.get[Int64](col=0, row=0)
    var s_mix = chunk.get[Optional[Int128]](col=1, row=0)
    assert_equal(Int(c1), ref_cnt_star)
    assert_true(s_mix.__bool__() and s_mix.value() == ref_sum_b, "mixed sum ok")
    print("[ok] count(*),sum(b)              =", Int(c1), String(s_mix.value()))

    print("ALL PASS")
