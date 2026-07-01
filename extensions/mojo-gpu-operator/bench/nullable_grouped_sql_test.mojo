"""GPU_OP_NULLABLE_GROUPED: DENSE GROUP BY over nullable agg columns, live SQL.

A DENSE grouped int aggregate (count(*) + sum/avg, NOT-NULL group key) over NULLABLE
agg columns. The A1 per-metric validity multiply + per-group validity-count NULL
marking + the dense filter-count existence gate are all per-group, so:
  * each group's sum(x) excludes its own NULL-x rows,
  * a group with filter-passing rows but all-NULL x emits SQL NULL,
  * a group with ZERO filter-passing rows is OMITTED (matches stock),
  * count(*) per group counts all filter-passing rows.
A NULLABLE group key must DECLINE (it would form its own SQL NULL group).

Each probe wraps the grouped result in string_agg(... ORDER BY k) so a single VARCHAR
captures all groups; the inner GROUP BY routes (operator), the outer string_agg is a CPU
projection. We compare operator (GPU_OP_NULLABLE_GROUPED=1) vs stock (GPU_OP_GENERIC=off).

Run: GPU_OP_NULLABLE=1 GPU_OP_NULLABLE_GROUPED=1 pixi run mojo run \
        -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/nullable_grouped_sql_test.mojo
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
        "extensions/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    _ = setenv("GPU_OP_NULLABLE", "1", True)
    _ = setenv("GPU_OP_NULLABLE_GROUPED", "1", True)
    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    # k: NOT-NULL VARCHAR group key (4 groups -> DENSE). gid: NOT-NULL int (==group).
    # x: nullable BIGINT (NULL on i%7). xg: nullable BIGINT where group 'D' (i%4==3) is
    # ALL NULL. knull: nullable VARCHAR (group 'A' key is NULL) for the decline test.
    _ = con.execute("CREATE TABLE t(k VARCHAR NOT NULL, gid INTEGER NOT NULL,"
                    " x BIGINT, xg BIGINT, knull VARCHAR)")
    _ = con.execute(
        String(
            "INSERT INTO t SELECT"
            " CASE WHEN i%4=0 THEN 'A' WHEN i%4=1 THEN 'B'"
            "      WHEN i%4=2 THEN 'C' ELSE 'D' END,"
            " (i%4),"
            " CASE WHEN i%7=0 THEN NULL ELSE (i%1000) END,"
            " CASE WHEN i%4=3 THEN NULL ELSE (i%1000) END,"
            " CASE WHEN i%4=0 THEN NULL ELSE 'g'||(i%4) END"
            " FROM range("
        )
        + String(N)
        + ") r(i)"
    )

    var sqls = List[String]()
    # per-group sum(x) (excludes NULL-x) + count(*) (all rows).
    sqls.append(
        "SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL')||':'||c::VARCHAR,"
        " '|' ORDER BY k) FROM (SELECT k, sum(x) s, count(*) c FROM t GROUP BY k)"
    )
    # fully-filtered group: gid<3 excludes ALL of group 'D' -> 'D' must be OMITTED.
    sqls.append(
        "SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL')||':'||c::VARCHAR,"
        " '|' ORDER BY k) FROM (SELECT k, sum(x) s, count(*) c FROM t"
        " WHERE gid < 3 GROUP BY k)"
    )
    # all-NULL-x group: sum(xg) for group 'D' is SQL NULL (every 'D' row has xg NULL).
    sqls.append(
        "SELECT string_agg(k||':'||COALESCE(s::VARCHAR,'NULL'),"
        " '|' ORDER BY k) FROM (SELECT k, sum(xg) s FROM t GROUP BY k)"
    )
    # avg per group + filter on a nullable agg column's sibling.
    sqls.append(
        "SELECT string_agg(k||':'||COALESCE(round(a,4)::VARCHAR,'NULL'),"
        " '|' ORDER BY k) FROM (SELECT k, avg(x) AS a FROM t WHERE x > 100 GROUP BY k)"
    )
    # NULLABLE GROUP KEY -> must DECLINE (stock). (knull group 'A' key is NULL.)
    sqls.append(
        "SELECT string_agg(COALESCE(knull,'<NULL>')||':'||COALESCE(s::VARCHAR,'NULL'),"
        " '|' ORDER BY knull) FROM (SELECT knull, sum(x) s FROM t GROUP BY knull)"
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
        print("[" + ("ok" if ok else "FAIL") + "] q", k)
        print("        stock=", sv)
        print("        oper =", ov)

    assert_true(allok, "all grouped-nullable queries match stock")
    print("ALL PASS")
