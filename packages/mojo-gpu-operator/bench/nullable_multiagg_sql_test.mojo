"""A1 unified pass model: count(*) + multi-aggregate over nullable columns, live SQL.

The shipped single-aggregate nullable path folded validity into the host pass column,
which CANNOT serve count(*) (must count NULL-input rows) or two aggregates over DIFFERENT
nullable columns (each must exclude only its own NULLs). A1 moves agg-input-column
validity OUT of the pass into a per-metric multiply (count(*) unmultiplied), with a
per-aggregate validity-product count for NULL-on-empty.

Each probe wraps its aggregates in a COALESCE(<agg>::VARCHAR,'NULL') '|'-concat so a
single VARCHAR column captures every aggregate cell (incl. NULLs); the operator still
routes the underlying aggregate (the concat is a CPU projection above it). We compare the
operator (GPU_OP_NULLABLE=1) string to stock DuckDB's (GPU_OP_GENERIC=off) for the SAME
connection (toggled via setenv). Any divergence in per-metric validity (count(*)
undercounting, or sum(b) leaking sum(c)'s NULL set) shows as a string mismatch.

Run: GPU_OP_NULLABLE=1 pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/nullable_multiagg_sql_test.mojo
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.os import getenv, setenv
from std.sys import has_accelerator
from std.testing import assert_true


comptime N = 120_000


def main() raises:
    if not has_accelerator():
        print("no GPU -> skip")
        return
    var ext = getenv(
        "GPU_OP_EXT",
        "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    _ = setenv("GPU_OP_NULLABLE", "1", True)
    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    _ = con.execute(
        String(
            "CREATE TABLE t AS SELECT"
            " CASE WHEN i % 7  = 0 THEN NULL ELSE (i % 1000) END::BIGINT  AS b,"
            " CASE WHEN i % 11 = 0 THEN NULL ELSE (i % 500)  END::BIGINT  AS c,"
            " CASE WHEN i % 5  = 0 THEN NULL ELSE (i % 100)  END::INTEGER AS f,"
            " NULL::BIGINT AS g"
            " FROM range("
        )
        + String(N)
        + ") r(i)"
    )

    # Each entry: a SELECT whose single VARCHAR column is the '|'-joined aggregate cells.
    var sqls = List[String]()
    # count(*) counts ALL rows; sum(b) excludes only NULL-b.
    sqls.append(
        "SELECT count(*)::VARCHAR || '|' || COALESCE(sum(b)::VARCHAR,'NULL') FROM t"
    )
    # two aggregates over DIFFERENT nullable columns -- each excludes only its own NULLs.
    sqls.append(
        "SELECT COALESCE(sum(b)::VARCHAR,'NULL') || '|' ||"
        " COALESCE(sum(c)::VARCHAR,'NULL') FROM t"
    )
    # count(*) with a nullable filter (NULL f -> excluded).
    sqls.append("SELECT count(*)::VARCHAR FROM t WHERE f > 50")
    # count(*) + sum + avg together.
    sqls.append(
        "SELECT count(*)::VARCHAR || '|' || COALESCE(sum(b)::VARCHAR,'NULL') || '|' ||"
        " COALESCE(round(avg(b),6)::VARCHAR,'NULL') FROM t"
    )
    # three aggregates + filter.
    sqls.append(
        "SELECT COALESCE(sum(b)::VARCHAR,'NULL') || '|' ||"
        " COALESCE(sum(c)::VARCHAR,'NULL') || '|' || count(*)::VARCHAR"
        " FROM t WHERE f > 30"
    )
    # all-NULL sum -> NULL alongside a live count(*).
    sqls.append(
        "SELECT COALESCE(sum(g)::VARCHAR,'NULL') || '|' || count(*)::VARCHAR FROM t"
    )

    var allok = True
    for k in range(len(sqls)):
        _ = setenv("GPU_OP_GENERIC", "off", True)
        var s = con.execute(sqls[k]).fetch_chunk().get[Optional[String]](
            col=0, row=0
        )
        _ = setenv("GPU_OP_GENERIC", "", True)
        var o = con.execute(sqls[k]).fetch_chunk().get[Optional[String]](
            col=0, row=0
        )
        var sv = s.value() if Bool(s) else String("<NULL>")
        var ov = o.value() if Bool(o) else String("<NULL>")
        var ok = sv == ov
        if not ok:
            allok = False
        print("[" + ("ok" if ok else "FAIL") + "] stock=", sv, " oper=", ov)

    assert_true(allok, "all count(*)/multi-agg nullable queries match stock")
    print("ALL PASS")
