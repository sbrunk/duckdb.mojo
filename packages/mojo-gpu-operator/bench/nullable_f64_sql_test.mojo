"""GPU_OP_NULLABLE on the f64 path (transcendental SUM + statistical aggregates),
live-SQL, NVIDIA-only (on Apple/AMD the f64 scope guard declines -> operator == stock,
so this passes trivially; the real validation is on the RTX 4090 / frederick).

Builds a single fact table with NULLABLE DECIMAL columns + a deterministic NULL pattern
and asserts every routed query equals stock DuckDB (operator OFF) within tight rel-err.
Operator-vs-stock is toggled in-process via GPU_OP_GENERIC so the SAME data/connection
is compared (no need to hand-compute transcendentals). If the validity fold failed, a
NULL row's garbage would be sqrt'd/summed and the operator would diverge from stock.

Run from the repo root (extension built):
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/nullable_f64_sql_test.mojo
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.math import sqrt
from std.os import getenv, setenv
from std.sys import has_accelerator
from std.testing import assert_true


def main() raises:
    if not has_accelerator():
        print("no GPU accelerator -> skip")
        return
    var ext = getenv(
        "GPU_OP_EXT",
        "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    _ = setenv("GPU_OP_NULLABLE", "1", True)
    _ = setenv("GPU_OP_TRANSCENDENTAL", "1", True)
    _ = setenv("GPU_OP_STATS", "1", True)

    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    # x: positive (sqrt/ln safe) nullable DECIMAL; y: nullable DECIMAL; f: nullable INT.
    # f64 paths decline on raw DOUBLE -> use DECIMAL.
    _ = con.execute(
        String(
            "CREATE TABLE t AS SELECT"
            " CASE WHEN i % 7  = 0 THEN NULL ELSE ((i % 500) + 0.25) END::DECIMAL(15,2) AS x,"
            " CASE WHEN i % 11 = 0 THEN NULL ELSE ((i % 300) + 0.50) END::DECIMAL(15,2) AS y,"
            " CASE WHEN i % 5  = 0 THEN NULL ELSE (i % 100) END::INTEGER AS f"
            " FROM range(120000) r(i)"
        )
    )

    var sqls = List[String]()
    sqls.append("SELECT sum(sqrt(x)) FROM t")
    sqls.append("SELECT sum(sqrt(x)) FROM t WHERE f > 50")
    sqls.append("SELECT sum(ln(x)) FROM t")
    sqls.append("SELECT stddev_samp(x) FROM t")
    sqls.append("SELECT var_samp(x) FROM t")
    sqls.append("SELECT corr(x, y) FROM t")
    sqls.append("SELECT regr_slope(y, x) FROM t")

    var routed_count = 0
    var allok = True
    for k in range(len(sqls)):
        # stock (operator disabled)
        _ = setenv("GPU_OP_GENERIC", "off", True)
        var rs = con.execute(sqls[k]).fetch_chunk().get[Optional[Float64]](
            col=0, row=0
        )
        # operator on (+ shadow to see routing)
        _ = setenv("GPU_OP_GENERIC", "", True)
        _ = setenv("GPU_OP_SHADOW", "1", True)
        var ro = con.execute(sqls[k]).fetch_chunk().get[Optional[Float64]](
            col=0, row=0
        )
        _ = setenv("GPU_OP_SHADOW", "", True)

        var sv = rs.value() if Bool(rs) else Float64(0)
        var ov = ro.value() if Bool(ro) else Float64(0)
        var d = ov - sv
        var ad = d if d >= 0 else -d
        var asv = sv if sv >= 0 else -sv
        var denom = asv if asv > 1e-12 else Float64(1)
        var rel = ad / denom
        var ok = (Bool(rs) == Bool(ro)) and rel < 1e-9
        if not ok:
            allok = False
        print(
            ("[ok] " if ok else "[FAIL] "),
            sqls[k],
            " stock=",
            sv,
            " op=",
            ov,
            " rel=",
            rel,
        )

    assert_true(allok, "all f64-nullable queries match stock within rel-err")
    print("ALL PASS")
