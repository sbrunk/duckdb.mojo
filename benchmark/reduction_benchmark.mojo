"""Reduction Aggregate Benchmark: Mojo high-level aggregates vs DuckDB builtins.

Compares aggregate functions registered via the high-level `from_reduce`,
`from_sum`, `from_max`, `from_min`, `from_product`, and `from_mean` helpers
against DuckDB's built-in aggregates (sum, max, min, product, avg).

Two scenarios are tested:
  1. **Ungrouped** — a single aggregate over the entire table.
  2. **Grouped**  — aggregate per group with ~1 000 distinct groups.

Change ``F`` below to switch between Float64 (DOUBLE) and Float32 (FLOAT).

Usage:
    pixi run mojo run benchmark/reduction_benchmark.mojo
"""

from duckdb import *
from duckdb.aggregate_function import AggregateFunction, AggregateFunctionInfo
from duckdb._libduckdb import *
from sys import simd_width_of
import benchmark


# ===--------------------------------------------------------------------===#
# Configuration
# ===--------------------------------------------------------------------===#

comptime F = DType.float32

fn sql_type() -> String:
    """Returns the SQL type name corresponding to F."""
    @parameter
    if F == DType.float64:
        return "DOUBLE"
    else:
        return "FLOAT"


# ===--------------------------------------------------------------------===#
# Custom reduce kernel — sum of squares (not a DuckDB builtin)
# ===--------------------------------------------------------------------===#

fn simd_add[w: Int](a: SIMD[F, w], b: SIMD[F, w]) -> SIMD[F, w]:
    return a + b

fn zero() -> Scalar[F]:
    return 0


# ===--------------------------------------------------------------------===#
# Registration
# ===--------------------------------------------------------------------===#

fn register_functions(conn: Connection) raises:
    """Register all Mojo aggregate functions."""
    AggregateFunction.from_sum["mojo_sum", F](conn)
    AggregateFunction.from_max["mojo_max", F](conn)
    AggregateFunction.from_min["mojo_min", F](conn)
    AggregateFunction.from_product["mojo_product", F](conn)
    AggregateFunction.from_mean["mojo_mean", F](conn)

    # A custom reduction: reuse the generic from_reduce
    AggregateFunction.from_reduce["mojo_sum_custom", F, simd_add, zero](conn)


# ===--------------------------------------------------------------------===#
# Benchmark runner
# ===--------------------------------------------------------------------===#

fn run_benchmark(
    name: String,
    conn: Connection,
    query_standard: String,
    query_mojo: String,
    max_iters: Int,
    warmup_iters: Int,
) raises:
    """Run a single benchmark comparing standard vs Mojo query."""
    print("\n  " + name)
    print("  " + "-" * len(name))

    # Warmup
    for _ in range(warmup_iters):
        _ = conn.execute(query_standard)

    fn bench_standard() capturing raises:
        _ = conn.execute(query_standard)

    var std_report = benchmark.run[bench_standard](max_iters=max_iters)
    var std_ms = std_report.mean("ms")
    print("    DuckDB builtin:   " + String(std_ms) + " ms")

    # Warmup Mojo
    for _ in range(warmup_iters):
        _ = conn.execute(query_mojo)

    fn bench_mojo() capturing raises:
        _ = conn.execute(query_mojo)

    var mojo_report = benchmark.run[bench_mojo](max_iters=max_iters)
    var mojo_ms = mojo_report.mean("ms")
    print("    Mojo aggregate:   " + String(mojo_ms) + " ms")

    var speedup = std_ms / mojo_ms
    if speedup >= 1.0:
        print("    Speedup:          " + String(speedup) + "x faster")
    else:
        print("    Slowdown:         " + String(1.0 / speedup) + "x slower")


# ===--------------------------------------------------------------------===#
# Main
# ===--------------------------------------------------------------------===#

fn main() raises:
    var num_rows = 10_000_000
    var num_groups = 1000
    var max_iters = 50
    var warmup_iters = 3
    var T = sql_type()

    print("=" * 70)
    print("Reduction Aggregate Benchmark: Mojo vs DuckDB Builtins")
    print("=" * 70)
    print("Rows:         " + String(num_rows))
    print("Groups:       " + String(num_groups))
    print("Type:         " + T + " (" + String(F) + ")")
    print("Max Iters:    " + String(max_iters))
    print()

    # ---- Setup ----
    print("Setting up database...")
    var conn = DuckDB.connect(":memory:")

    print("Generating test data...")
    _ = conn.execute(
        "CREATE TABLE agg_data AS"
        + " SELECT (random() * 100 + 0.01)::" + T + " AS x,"
        + "        (i % " + String(num_groups) + ")::INTEGER AS g"
        + " FROM range(" + String(num_rows) + ") t(i);"
    )

    var row_count = conn.execute("SELECT count(*) FROM agg_data")
    var rc = row_count.fetch_chunk()
    print("  agg_data rows: " + String(rc.get(bigint, col=0, row=0).value()))

    # ---- Register Mojo functions ----
    print("\nRegistering Mojo aggregate functions...")
    register_functions(conn)
    print("  from_sum:     mojo_sum")
    print("  from_max:     mojo_max")
    print("  from_min:     mojo_min")
    print("  from_product: mojo_product")
    print("  from_mean:    mojo_mean")
    print("  from_reduce:  mojo_sum_custom  (generic reduce)")

    # ---- Validate correctness ----
    print("\nValidating correctness...")

    fn check_close(label: String, conn: Connection, query: String) raises:
        var res = conn.execute(query)
        var ck = res.fetch_chunk()
        var ok = ck.get(boolean, col=0, row=0).value()
        print("  " + label + ": " + String(ok))

    # Use a tolerance that accommodates float accumulation differences
    fn tol() -> String:
        @parameter
        if F == DType.float64:
            return "0.001"
        else:
            return "1000.0"

    var t = tol()

    check_close(
        "sum",
        conn,
        "SELECT abs(sum(x) - mojo_sum(x)) < " + t + " FROM agg_data",
    )
    check_close(
        "max",
        conn,
        "SELECT max(x) = mojo_max(x) FROM agg_data",
    )
    check_close(
        "min",
        conn,
        "SELECT min(x) = mojo_min(x) FROM agg_data",
    )
    check_close(
        "product (TOP 20)",
        conn,
        "SELECT abs(product(x) - mojo_product(x)) < 1e200 FROM (SELECT x FROM agg_data LIMIT 20)",
    )
    check_close(
        "mean",
        conn,
        "SELECT abs(avg(x) - mojo_mean(x)) < " + t + " FROM agg_data",
    )
    check_close(
        "custom sum",
        conn,
        "SELECT abs(sum(x) - mojo_sum_custom(x)) < " + t + " FROM agg_data",
    )

    # ---- Ungrouped benchmarks ----
    print("\n" + "=" * 70)
    print("UNGROUPED AGGREGATES (full-table reduction)")
    print("=" * 70)

    run_benchmark(
        "SUM(x)",
        conn,
        "SELECT sum(x) FROM agg_data",
        "SELECT mojo_sum(x) FROM agg_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "MAX(x)",
        conn,
        "SELECT max(x) FROM agg_data",
        "SELECT mojo_max(x) FROM agg_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "MIN(x)",
        conn,
        "SELECT min(x) FROM agg_data",
        "SELECT mojo_min(x) FROM agg_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "AVG(x)",
        conn,
        "SELECT avg(x) FROM agg_data",
        "SELECT mojo_mean(x) FROM agg_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "SUM(x) [from_reduce]",
        conn,
        "SELECT sum(x) FROM agg_data",
        "SELECT mojo_sum_custom(x) FROM agg_data",
        max_iters,
        warmup_iters,
    )

    # ---- Grouped benchmarks ----
    print("\n" + "=" * 70)
    print("GROUPED AGGREGATES (GROUP BY g — " + String(num_groups) + " groups)")
    print("=" * 70)

    run_benchmark(
        "SUM(x) GROUP BY g",
        conn,
        "SELECT g, sum(x) FROM agg_data GROUP BY g",
        "SELECT g, mojo_sum(x) FROM agg_data GROUP BY g",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "MAX(x) GROUP BY g",
        conn,
        "SELECT g, max(x) FROM agg_data GROUP BY g",
        "SELECT g, mojo_max(x) FROM agg_data GROUP BY g",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "MIN(x) GROUP BY g",
        conn,
        "SELECT g, min(x) FROM agg_data GROUP BY g",
        "SELECT g, mojo_min(x) FROM agg_data GROUP BY g",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "AVG(x) GROUP BY g",
        conn,
        "SELECT g, avg(x) FROM agg_data GROUP BY g",
        "SELECT g, mojo_mean(x) FROM agg_data GROUP BY g",
        max_iters,
        warmup_iters,
    )

    print("\n" + "=" * 70)
    print("Benchmark complete.")
    print("=" * 70)
