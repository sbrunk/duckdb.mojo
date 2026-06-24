"""GPU_OP_NULLABLE adversarial correctness sweep (operator vs stock, live SQL).

Hardening due-diligence for the full nullable feature (int SUM/AVG/count(*)/multi-agg,
f64 transcendental/stats single-agg, DENSE GROUP BY) before any default-on flip. Builds
a fact table with nullable columns of varied TYPE / SCALE / sign / NULL-density and runs
a broad suite, comparing the operator (GPU_OP_NULLABLE[_GROUPED]=1) to stock DuckDB
(GPU_OP_GENERIC=off) for the SAME connection (toggled via setenv). Each query is reduced
to ONE VARCHAR (single cell, or string_agg over an ORDER BY subquery for grouped) so all
output cells incl. NULLs are captured; a mismatch is a wrong result.

Run: GPU_OP_NULLABLE=1 GPU_OP_NULLABLE_GROUPED=1 pixi run mojo run \
        -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/nullable_adversarial_sweep.mojo
(transcendental/stats route only on NVIDIA; on Apple they decline -> op==stock anyway.)
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.os import getenv, setenv
from std.sys import has_accelerator
from std.testing import assert_true


comptime N = 150_000


def main() raises:
    if not has_accelerator():
        print("no GPU -> skip")
        return
    var ext = getenv(
        "GPU_OP_EXT",
        "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    # Rely on the DEFAULTS (GPU_OP_NULLABLE / _GROUPED / TRANSCENDENTAL / STATS are all
    # default-ON) -- this also validates the default-on posture. Only GPU_OP_GENERIC is
    # toggled below for the stock comparison.
    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    # Varied columns:
    #  d2  DECIMAL(15,2) nullable, dense NULLs (i%3)            -- scale 2
    #  d0  DECIMAL(15,0) nullable, sparse NULLs (i%101)         -- scale 0
    #  d4  DECIMAL(18,4) nullable, signed (negative half)       -- scale 4, negatives
    #  bg  BIGINT nullable (i%7), can be 0                      -- int, zeros
    #  pn  DECIMAL(12,2) nullable, strictly POSITIVE (for ln/sqrt)
    #  alln BIGINT, ALL NULL
    #  fi  INTEGER nullable filter (i%5)
    #  k   VARCHAR NOT NULL group key (5 groups)
    _ = con.execute("CREATE TABLE t(d2 DECIMAL(15,2), d0 DECIMAL(15,0),"
                    " d4 DECIMAL(18,4), bg BIGINT, pn DECIMAL(12,2),"
                    " alln BIGINT, fi INTEGER, k VARCHAR NOT NULL)")
    _ = con.execute(
        String(
            "INSERT INTO t SELECT"
            " CASE WHEN i%3=0 THEN NULL ELSE ((i%1000)+0.25) END,"
            " CASE WHEN i%101=0 THEN NULL ELSE (i%5000) END,"
            " CASE WHEN i%13=0 THEN NULL ELSE ((i%800)-400 + 0.5) END,"
            " CASE WHEN i%7=0 THEN NULL ELSE (i%100) END,"
            " CASE WHEN i%9=0 THEN NULL ELSE ((i%500)+1.5) END,"
            " NULL,"
            " CASE WHEN i%5=0 THEN NULL ELSE (i%100) END,"
            " ('g' || (i%5))"
            " FROM range("
        )
        + String(N)
        + ") r(i)"
    )

    var q = List[String]()
    # --- single int SUM, varied scale/sign/density ---
    q.append("SELECT COALESCE(sum(d2)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(sum(d0)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(sum(d4)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(sum(bg)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(sum(alln)::VARCHAR,'NULL') FROM t")  # all-NULL -> NULL
    # --- AVG ---
    q.append("SELECT COALESCE(round(avg(d2),6)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(round(avg(d4),6)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(round(avg(alln),6)::VARCHAR,'NULL') FROM t")  # NULL
    # --- count(*) + multi-agg over DIFFERENT nullable columns ---
    q.append("SELECT count(*)::VARCHAR||'|'||COALESCE(sum(d2)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(sum(d2)::VARCHAR,'NULL')||'|'||"
             "COALESCE(sum(d4)::VARCHAR,'NULL')||'|'||"
             "COALESCE(sum(bg)::VARCHAR,'NULL')||'|'||count(*)::VARCHAR FROM t")
    q.append("SELECT count(*)::VARCHAR||'|'||COALESCE(sum(alln)::VARCHAR,'NULL') FROM t")
    # --- filters: range / eq / between / on nullable + non-nullable cols ---
    q.append("SELECT COALESCE(sum(d2)::VARCHAR,'NULL') FROM t WHERE fi > 50")
    q.append("SELECT COALESCE(sum(d2)::VARCHAR,'NULL') FROM t WHERE fi BETWEEN 10 AND 40")
    q.append("SELECT COALESCE(sum(d4)::VARCHAR,'NULL') FROM t WHERE d4 > 0")  # filter+agg same nullable col
    q.append("SELECT count(*)::VARCHAR FROM t WHERE fi = 7")
    q.append("SELECT COALESCE(sum(bg)::VARCHAR,'NULL') FROM t WHERE fi > 100000")  # empty -> NULL
    q.append("SELECT count(*)::VARCHAR||'|'||COALESCE(sum(d2)::VARCHAR,'NULL') FROM t WHERE fi > 50")
    # --- grouped (DENSE, NOT-NULL key) ---
    q.append("SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL')||':'||c::VARCHAR,'|' ORDER BY k)"
             " FROM (SELECT k, sum(d2) s, count(*) c FROM t GROUP BY k)")
    q.append("SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL'),'|' ORDER BY k)"
             " FROM (SELECT k, sum(d4) s FROM t WHERE fi > 30 GROUP BY k)")
    q.append("SELECT string_agg(k||':'||COALESCE(round(a,4)::VARCHAR,'NULL'),'|' ORDER BY k)"
             " FROM (SELECT k, avg(bg) a FROM t GROUP BY k)")
    q.append("SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL'),'|' ORDER BY k)"
             " FROM (SELECT k, sum(alln) s FROM t GROUP BY k)")  # every group all-NULL -> NULL
    # --- f64 (NVIDIA routes; Apple declines -> op==stock) ---
    q.append("SELECT COALESCE(round(sum(sqrt(pn)),4)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(round(sum(ln(pn)),4)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(round(stddev_samp(d2),6)::VARCHAR,'NULL') FROM t")
    q.append("SELECT COALESCE(round(corr(d2,d4),8)::VARCHAR,'NULL') FROM t")

    var fails = 0
    for i in range(len(q)):
        _ = setenv("GPU_OP_GENERIC", "off", True)
        var s = con.execute(q[i]).fetch_chunk().get[Optional[String]](col=0, row=0)
        _ = setenv("GPU_OP_GENERIC", "", True)
        var o = con.execute(q[i]).fetch_chunk().get[Optional[String]](col=0, row=0)
        var sv = s.value() if Bool(s) else String("<NULL>")
        var ov = o.value() if Bool(o) else String("<NULL>")
        if sv != ov:
            fails += 1
            print("[FAIL] q", i, " stock=", sv, " oper=", ov)
            print("       sql:", q[i])

    print("sweep:", len(q), "queries,", fails, "mismatch")
    assert_true(fails == 0, "adversarial nullable sweep matches stock")
    print("ALL PASS")
