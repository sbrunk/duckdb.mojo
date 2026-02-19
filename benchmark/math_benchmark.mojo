"""Math Function Benchmark: Mojo SIMD scalar functions vs DuckDB builtins.

Compares Mojo's SIMD-vectorized math functions (registered via from_simd_function
/ set_simd_function) against DuckDB's built-in math operators on a large table.

Change ``F`` below to switch between Float64 (DOUBLE) and Float32 (FLOAT).

Benchmarks:
  - Unary:  sqrt(x), sin(x), cos(x), exp(x), log(x), abs(x)
  - Fused:  sin(x) + cos(x), sqrt(x*x + 1), exp(-x*x)

Usage:
    pixi run mojo run benchmark/math_benchmark.mojo
"""

from duckdb import *
from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet, FunctionInfo
from duckdb.logical_type import LogicalType
from duckdb._libduckdb import *
from sys import simd_width_of
import math
import benchmark


# ===--------------------------------------------------------------------===#
# Configuration — change F to DType.float32 for single-precision benchmark
# ===--------------------------------------------------------------------===#

comptime F = DType.float32


# Derive SQL type name from F at comptime.
fn sql_type() -> String:
    """Returns the SQL type name corresponding to F."""
    @parameter
    if F == DType.float64:
        return "DOUBLE"
    else:
        return "FLOAT"


# ===--------------------------------------------------------------------===#
# SIMD math kernels
# ===--------------------------------------------------------------------===#
# For standard math functions (sqrt, sin, cos, exp, log) we pass the stdlib
# functions directly — no wrappers needed thanks to the stdlib-compatible
# from_simd_function overload.
#
# Only fused/compound kernels need custom definitions.


# --- Fused (compound) kernels ---

fn simd_sin_plus_cos[w: Int](x: SIMD[F, w]) -> SIMD[F, w]:
    """Computes sin(x) + cos(x) — a common fused trig computation."""
    return math.sin(x) + math.cos(x)


fn simd_hypot1[w: Int](x: SIMD[F, w]) -> SIMD[F, w]:
    """Computes sqrt(x*x + 1) — distance from origin to (x, 1)."""
    return math.sqrt(x * x + 1.0)


fn simd_gauss[w: Int](x: SIMD[F, w]) -> SIMD[F, w]:
    """Computes exp(-x*x) — unnormalized Gaussian kernel."""
    return math.exp(-(x * x))


# --- Binary kernels (compound only) ---

fn simd_hypot[w: Int](a: SIMD[F, w], b: SIMD[F, w]) -> SIMD[F, w]:
    """Computes sqrt(a*a + b*b) — Euclidean distance."""
    return math.sqrt(a * a + b * b)


# ===--------------------------------------------------------------------===#
# Registration
# ===--------------------------------------------------------------------===#

fn register_functions(conn: Connection) raises:
    """Register all Mojo math scalar functions."""

    # Unary functions: pass stdlib math functions directly
    ScalarFunction.from_simd_function["mojo_sqrt", F, math.sqrt](conn)
    ScalarFunction.from_simd_function["mojo_sin", F, math.sin](conn)
    ScalarFunction.from_simd_function["mojo_cos", F, math.cos](conn)
    ScalarFunction.from_simd_function["mojo_exp", F, math.exp](conn)
    ScalarFunction.from_simd_function["mojo_log", F, math.log](conn)

    # abs uses a trait-based signature, so it still needs the original overload
    fn simd_abs[w: Int](x: SIMD[F, w]) -> SIMD[F, w]:
        return math.abs(x)

    ScalarFunction.from_simd_function["mojo_abs", F, F, simd_abs](conn)

    # Fused unary functions: custom kernels (DOUBLE) -> DOUBLE
    ScalarFunction.from_simd_function[
        "mojo_sin_plus_cos", F, F, simd_sin_plus_cos
    ](conn)
    ScalarFunction.from_simd_function["mojo_hypot1", F, F, simd_hypot1](conn)
    ScalarFunction.from_simd_function["mojo_gauss", F, F, simd_gauss](conn)

    # Binary functions:
    # atan2 matches the stdlib signature — pass directly
    ScalarFunction.from_simd_function["mojo_atan2", F, math.atan2](conn)
    # hypot is a custom compound kernel
    ScalarFunction.from_simd_function["mojo_hypot", F, F, F, simd_hypot](conn)


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
    print("    Standard DuckDB:  " + String(std_ms) + " ms")

    # Warmup Mojo
    for _ in range(warmup_iters):
        _ = conn.execute(query_mojo)

    fn bench_mojo() capturing raises:
        _ = conn.execute(query_mojo)

    var mojo_report = benchmark.run[bench_mojo](max_iters=max_iters)
    var mojo_ms = mojo_report.mean("ms")
    print("    Mojo SIMD:        " + String(mojo_ms) + " ms")

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
    var max_iters = 100
    var warmup_iters = 3
    var T = sql_type()

    print("=" * 70)
    print("Math Function Benchmark: Mojo SIMD vs DuckDB Builtins")
    print("=" * 70)
    print("Rows:         " + String(num_rows))
    print("Type:         " + T + " (" + String(F) + ")")
    print("SIMD Width:   " + String(simd_width_of[F]()) + " (auto-detected)")
    print("Max Iters:    " + String(max_iters))
    print()

    # ---- Setup ----
    print("Setting up database...")
    var conn = DuckDB.connect(":memory:")

    print("Generating test data...")
    _ = conn.execute(
        "CREATE TABLE math_data AS"
        + " SELECT (random() * 100 + 0.01)::" + T + " AS x,"
        + "        (random() * 100 + 0.01)::" + T + " AS y"
        + " FROM range(" + String(num_rows) + ") t(i);"
    )

    var row_count = conn.execute("SELECT count(*) FROM math_data")
    var rc = row_count.fetch_chunk()
    print("  math_data rows: " + String(rc.get(bigint, col=0, row=0).value()))

    # ---- Register Mojo functions ----
    print("\nRegistering Mojo scalar functions...")
    register_functions(conn)
    print("  Unary:  mojo_sqrt, mojo_sin, mojo_cos, mojo_exp, mojo_log, mojo_abs")
    print("  Fused:  mojo_sin_plus_cos, mojo_hypot1, mojo_gauss")
    print("  Binary: mojo_hypot, mojo_atan2")

    # ---- Validate correctness ----
    print("\nValidating correctness (first row)...")
    # Quick spot-check: compare first row (cast builtins to T since DuckDB
    # math builtins always return DOUBLE even for FLOAT inputs)
    var check = conn.execute(
        "SELECT sqrt(x)::" + T + " AS std_sqrt, mojo_sqrt(x) AS mojo_sqrt_val,"
        + " sin(x)::" + T + " AS std_sin, mojo_sin(x) AS mojo_sin_val"
        + " FROM math_data LIMIT 1"
    )
    var ck = check.fetch_chunk()

    @parameter
    if F == DType.float64:
        print(
            "  sqrt: std="
            + String(ck.get(double, col=0, row=0).value())
            + " mojo="
            + String(ck.get(double, col=1, row=0).value())
        )
        print(
            "  sin:  std="
            + String(ck.get(double, col=2, row=0).value())
            + " mojo="
            + String(ck.get(double, col=3, row=0).value())
        )
    else:
        print(
            "  sqrt: std="
            + String(ck.get(float, col=0, row=0).value())
            + " mojo="
            + String(ck.get(float, col=1, row=0).value())
        )
        print(
            "  sin:  std="
            + String(ck.get(float, col=2, row=0).value())
            + " mojo="
            + String(ck.get(float, col=3, row=0).value())
        )

    # Quick numeric check on aggregate — use wider tolerance for float32
    fn agg_tol() -> String:
        @parameter
        if F == DType.float64:
            return "0.001"
        else:
            return "100.0"

    var tol = agg_tol()
    var agg_check = conn.execute(
        "SELECT abs(sum(sqrt(x)) - sum(mojo_sqrt(x))) < " + tol + " AS sqrt_ok,"
        + " abs(sum(sin(x)) - sum(mojo_sin(x))) < " + tol + " AS sin_ok,"
        + " abs(sum(cos(x)) - sum(mojo_cos(x))) < " + tol + " AS cos_ok"
        + " FROM math_data"
    )
    var ac = agg_check.fetch_chunk()
    print(
        "  Aggregate check — sqrt: "
        + String(ac.get(boolean, col=0, row=0).value())
        + ", sin: "
        + String(ac.get(boolean, col=1, row=0).value())
        + ", cos: "
        + String(ac.get(boolean, col=2, row=0).value())
    )

    # ---- Unary benchmarks ----
    print("\n" + "=" * 70)
    print("UNARY FUNCTIONS")
    print("=" * 70)

    run_benchmark(
        "sqrt(x)",
        conn,
        "SELECT sum(sqrt(x)) FROM math_data",
        "SELECT sum(mojo_sqrt(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "sin(x)",
        conn,
        "SELECT sum(sin(x)) FROM math_data",
        "SELECT sum(mojo_sin(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "cos(x)",
        conn,
        "SELECT sum(cos(x)) FROM math_data",
        "SELECT sum(mojo_cos(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "exp(x) [clamped]",
        conn,
        "SELECT sum(exp(x / 100)) FROM math_data",
        "SELECT sum(mojo_exp(x / 100)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "log(x)",
        conn,
        "SELECT sum(ln(x)) FROM math_data",
        "SELECT sum(mojo_log(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "abs(x)",
        conn,
        "SELECT sum(abs(x - 50)) FROM math_data",
        "SELECT sum(mojo_abs(x - 50)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    # ---- Fused benchmarks ----
    print("\n" + "=" * 70)
    print("FUSED COMPUTATIONS (single Mojo function vs DuckDB expression)")
    print("=" * 70)

    run_benchmark(
        "sin(x) + cos(x)",
        conn,
        "SELECT sum(sin(x) + cos(x)) FROM math_data",
        "SELECT sum(mojo_sin_plus_cos(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "sqrt(x*x + 1)",
        conn,
        "SELECT sum(sqrt(x * x + 1)) FROM math_data",
        "SELECT sum(mojo_hypot1(x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "exp(-x*x) [Gaussian]",
        conn,
        "SELECT sum(exp(-(x / 100) * (x / 100))) FROM math_data",
        "SELECT sum(mojo_gauss(x / 100)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    # ---- Binary benchmarks ----
    print("\n" + "=" * 70)
    print("BINARY FUNCTIONS")
    print("=" * 70)

    run_benchmark(
        "sqrt(x*x + y*y) [hypot]",
        conn,
        "SELECT sum(sqrt(x * x + y * y)) FROM math_data",
        "SELECT sum(mojo_hypot(x, y)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "atan2(y, x)",
        conn,
        "SELECT sum(atan2(y, x)) FROM math_data",
        "SELECT sum(mojo_atan2(y, x)) FROM math_data",
        max_iters,
        warmup_iters,
    )

    print("\n" + "=" * 70)
    print("Benchmark complete.")
    print("=" * 70)
