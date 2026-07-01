"""PROBE (throwaway): semi-join pushdown feasibility for TPC-H Q5 fact materialize.

Measures whether pushing Q5's dimension semi-join (ASIA region + 1994 orders)
into the lineitem fact-scan SQL meaningfully reduces the rows/time DuckDB must
materialize, vs. scanning the FULL lineitem table (the current GPU-operator
cold path that uploads all fact rows and filters on-GPU).

Both A and B use the SAME forcing -- wrap in `SELECT count(*), sum(...)` over a
subquery -- so DuckDB cannot optimize the scan away and the comparison is fair.

Run: pixi run mojo run extensions/mojo-gpu-operator/bench/semijoin_pushdown_probe.mojo
"""

from duckdb import *
from std.time import perf_counter_ns


# The fact-scan body, with the 4 columns the operator uploads. A scans all of
# lineitem; B pushes Q5's dimension semi-join into the scan.
comptime BODY_A = String(
    "SELECT l_orderkey, l_suppkey, l_extendedprice, l_discount FROM lineitem"
)
comptime BODY_B = String(
    "SELECT l_orderkey, l_suppkey, l_extendedprice, l_discount FROM lineitem"
    "  WHERE l_suppkey IN ("
    "    SELECT s_suppkey FROM supplier"
    "      JOIN nation ON s_nationkey = n_nationkey"
    "      JOIN region ON n_regionkey = r_regionkey"
    "     WHERE r_name = 'ASIA')"
    "  AND l_orderkey IN ("
    "    SELECT o_orderkey FROM orders"
    "     WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01')"
)


# Forcing 1: aggregate that TOUCHES ALL 4 COLUMNS so DuckDB cannot reduce the
# scan to a metadata-only count(*). Models "read all 4 cols through the filter".
def agg4(body: String) -> String:
    return String(
        "SELECT CAST(sum(l_orderkey) + sum(l_suppkey)"
        " + sum(l_extendedprice) + sum(l_discount) AS DOUBLE)"
        " FROM (" + body + ") t"
    )


# Plain row-count of each shape, to report materialized row counts cleanly.
comptime COUNT_A = String("SELECT count(*) FROM lineitem")
comptime COUNT_B = String(
    "SELECT count(*) FROM lineitem"
    "  WHERE l_suppkey IN ("
    "    SELECT s_suppkey FROM supplier"
    "      JOIN nation ON s_nationkey = n_nationkey"
    "      JOIN region ON n_regionkey = r_regionkey"
    "     WHERE r_name = 'ASIA')"
    "  AND l_orderkey IN ("
    "    SELECT o_orderkey FROM orders"
    "     WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01')"
)


def scalar_count(con: Connection, sql: String) raises -> Int64:
    var out = Int64(0)
    for row in con.execute(sql):
        out = row.get[Int64](col=0)
    return out


def median_of(mut times: List[Float64]) raises -> Float64:
    # simple insertion sort (tiny list)
    for i in range(1, len(times)):
        var key = times[i]
        var j = i - 1
        while j >= 0 and times[j] > key:
            times[j + 1] = times[j]
            j -= 1
        times[j + 1] = key
    var n = len(times)
    if n % 2 == 1:
        return times[n // 2]
    return (times[n // 2 - 1] + times[n // 2]) / 2.0


# Forcing 1: aggregate touching all 4 cols. Read the single Float64 result row.
def time_agg4(con: Connection, body: String, runs: Int) raises -> Float64:
    var sql = agg4(body)
    var times = List[Float64]()
    for _ in range(runs):
        var t0 = perf_counter_ns()
        for row in con.execute(sql):
            _ = row.get[Float64](col=0)
        times.append(Float64(perf_counter_ns() - t0) / 1e6)
    return median_of(times)


# Forcing 2: CREATE TABLE AS -- physically materialize all 4 cols to a new
# table. Closest analog to "materialize 4 cols into a host buffer for upload".
def time_ctas(con: Connection, body: String, runs: Int) raises -> Float64:
    var times = List[Float64]()
    for _ in range(runs):
        _ = con.execute("DROP TABLE IF EXISTS _probe_mat")
        var t0 = perf_counter_ns()
        _ = con.execute("CREATE TABLE _probe_mat AS " + body)
        times.append(Float64(perf_counter_ns() - t0) / 1e6)
    _ = con.execute("DROP TABLE IF EXISTS _probe_mat")
    return median_of(times)


def run_scale(sf: Int, runs: Int) raises:
    print("================ SF", sf, "================")
    var con = DuckDB.connect(":memory:")
    _ = con.execute(
        "SET autoinstall_known_extensions=1;"
        " SET autoload_known_extensions=1;"
        " INSTALL tpch; LOAD tpch;"
    )
    var tgen = perf_counter_ns()
    _ = con.execute("CALL dbgen(sf=" + String(sf) + ")")
    print("dbgen done in", Float64(perf_counter_ns() - tgen) / 1e6, "ms")

    var rows_a = scalar_count(con, COUNT_A)
    var rows_b = scalar_count(con, COUNT_B)
    print("rows(A) full lineitem      :", rows_a)
    print("rows(B) semi-join pushed   :", rows_b)
    print("row ratio B/A              :", Float64(rows_b) / Float64(rows_a))

    # warm up once each (extension loaded, plan/stats cached) before timing
    _ = time_agg4(con, BODY_A, 1)
    _ = time_agg4(con, BODY_B, 1)
    _ = time_ctas(con, BODY_A, 1)
    _ = time_ctas(con, BODY_B, 1)

    print("--- Forcing 1: agg over all 4 cols (read-through-filter cost) ---")
    var a1 = time_agg4(con, BODY_A, runs)
    var b1 = time_agg4(con, BODY_B, runs)
    print("median time(A) ms          :", a1)
    print("median time(B) ms          :", b1)
    print("time ratio B/A             :", b1 / a1)

    print("--- Forcing 2: CREATE TABLE AS (materialize 4 cols to memory) ---")
    var a2 = time_ctas(con, BODY_A, runs)
    var b2 = time_ctas(con, BODY_B, runs)
    print("median time(A) ms          :", a2)
    print("median time(B) ms          :", b2)
    print("time ratio B/A             :", b2 / a2)


def main() raises:
    var runs = 7
    run_scale(1, runs)
    run_scale(10, runs)
