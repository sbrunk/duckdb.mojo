"""DEFAULT-ON bug repro: ungrouped avg(x) on a SINGLE-column table returns 0.0 via
the operator (NO flags -- pure default config), while stock + multi-column tables
return the correct value. DuckDB rewrites the single-column avg into a `sum`
aggregate with a DOUBLE output; the operator's int128 assembly writes res_lo while
the DOUBLE extraction reads res_f64 (=> 0.0). NOT a nullable-feature bug (NOT NULL
column, no GPU_OP_NULLABLE) -- a pre-existing default-on silent-wrong-result that
TPC-H never hits (Q1's avg is grouped, Q6 is sum). Run with NO env flags:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/avg_ungrouped_probe.mojo
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.os import getenv
from std.sys import has_accelerator


def main() raises:
    if not has_accelerator():
        print("no GPU")
        return
    var ext = getenv(
        "GPU_OP_EXT",
        "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    # SINGLE-column NOT-NULL table; c = i % 1000 -> avg = 499.5.
    _ = con.execute("CREATE TABLE one(c BIGINT NOT NULL)")
    _ = con.execute("INSERT INTO one SELECT (i%1000)::BIGINT FROM range(120000) r(i)")
    # TWO-column NOT-NULL table (same data) -> avg works.
    _ = con.execute("CREATE TABLE two(c BIGINT NOT NULL, d BIGINT NOT NULL)")
    _ = con.execute(
        "INSERT INTO two SELECT (i%1000)::BIGINT, 0::BIGINT FROM range(120000) r(i)"
    )

    # Repro the original failure: sum(c) FIRST, then avg(c), same table+columns.
    var s_first = con.execute("SELECT sum(c) FROM one").fetch_chunk().get[
        Optional[Int128]
    ](col=0, row=0)
    var a_after = con.execute("SELECT avg(c) FROM one").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    print("sum(c) then avg(c) FROM one -> sum=",
          String(s_first.value()) if Bool(s_first) else "NULL",
          " avg=", a_after.value() if Bool(a_after) else Float64(-1),
          "  (avg CPU truth 499.5)")

    var a1 = con.execute("SELECT avg(c) FROM one").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    var a2 = con.execute("SELECT avg(c) FROM two").fetch_chunk().get[
        Optional[Float64]
    ](col=0, row=0)
    print("avg(c) FROM one(single col) =", a1.value() if Bool(a1) else Float64(-1),
          "  (CPU truth 499.5)")
    print("avg(c) FROM two(2 cols)     =", a2.value() if Bool(a2) else Float64(-1),
          "  (CPU truth 499.5)")
    if Bool(a1) and a1.value() == 0.0:
        print(">>> CONFIRMED default-on wrong-result: single-col ungrouped avg = 0.0")
