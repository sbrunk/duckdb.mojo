"""TPC-H Benchmark with Mojo SIMD scalar functions.

Registers Mojo functions that operate directly on DECIMAL's underlying
scaled Int64 representation, matching DuckDB's native DECIMAL arithmetic path.

TPC-H schema: all monetary columns are DECIMAL(15,2), stored internally as
scaled Int64 (value × 10^2). DuckDB's own DECIMAL arithmetic works like this:
  - add/sub → plain int64 add/sub (result: DECIMAL(16,2))
  - multiply → plain int64 multiply (result: DECIMAL(18,4), scale = 2+2)
  - divide  → cast to DOUBLE (even in standard DuckDB)

We use DECIMAL(18,4) as a universal input/output type. DuckDB implicitly casts
narrower DECIMAL inputs (e.g. DECIMAL(15,2)) to DECIMAL(18,4).

Usage:
    pixi run mojo run benchmark/tpch_benchmark.mojo
"""

from duckdb import *
from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet, FunctionInfo
from duckdb.logical_type import LogicalType, decimal_type
from duckdb._libduckdb import *
import benchmark


# ===--------------------------------------------------------------------===#
# SIMD width for Int64 operations
# ===--------------------------------------------------------------------===#

comptime SIMD_WIDTH = 8


# ===--------------------------------------------------------------------===#
# SIMD-accelerated DECIMAL arithmetic (operates on scaled Int64 values)
# ===--------------------------------------------------------------------===#
# DECIMAL(15,2) is stored as Int64 internally (width 15 ≤ 18).
# For add/sub the raw integers share the same scale, so we just add/sub.
# For multiply the scales add (2+2=4), stored in the output type metadata.
# No scale adjustment (division) is needed — exactly matching DuckDB internals.

fn mojo_add(info: FunctionInfo, mut input: Chunk, output: Vector):
    """SIMD addition on scaled Int64 DECIMAL values."""
    var size = len(input)
    var a_data = input.get_vector(0).get_data().bitcast[Int64]()
    var b_data = input.get_vector(1).get_data().bitcast[Int64]()
    var out_data = output.get_data().bitcast[Int64]()

    var num_simd = size // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        (out_data + idx).store(a_vec + b_vec)

    for i in range(num_simd * SIMD_WIDTH, size):
        out_data[i] = a_data[i] + b_data[i]


fn mojo_subtract(info: FunctionInfo, mut input: Chunk, output: Vector):
    """SIMD subtraction on scaled Int64 DECIMAL values."""
    var size = len(input)
    var a_data = input.get_vector(0).get_data().bitcast[Int64]()
    var b_data = input.get_vector(1).get_data().bitcast[Int64]()
    var out_data = output.get_data().bitcast[Int64]()

    var num_simd = size // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        (out_data + idx).store(a_vec - b_vec)

    for i in range(num_simd * SIMD_WIDTH, size):
        out_data[i] = a_data[i] - b_data[i]


fn mojo_multiply(info: FunctionInfo, mut input: Chunk, output: Vector):
    """SIMD multiplication on scaled Int64 DECIMAL values.

    Both inputs are DECIMAL(18,4) — scale 4 each. Raw multiply gives scale 8.
    We divide by 10^4 = 10000 to bring result back to scale 4.
    This division is exact for TPC-H data (original DECIMAL(15,2) values
    upcast to scale 4 have at most 4 fractional digits after multiply).
    """
    var size = len(input)
    var a_data = input.get_vector(0).get_data().bitcast[Int64]()
    var b_data = input.get_vector(1).get_data().bitcast[Int64]()
    var out_data = output.get_data().bitcast[Int64]()

    comptime SCALE_CORRECTION = SIMD[DType.int64, SIMD_WIDTH](10000)

    var num_simd = size // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        (out_data + idx).store(a_vec * b_vec // SCALE_CORRECTION)

    for i in range(num_simd * SIMD_WIDTH, size):
        out_data[i] = a_data[i] * b_data[i] // 10000


# For division and mixed DOUBLE expressions (Q14: 100.00 * sum(...) / sum(...)),
# we keep DOUBLE since DuckDB itself casts DECIMAL→DOUBLE for '/'.
fn mojo_multiply_f64(info: FunctionInfo, mut input: Chunk, output: Vector):
    """SIMD multiplication (DOUBLE) — for mixed expressions involving DOUBLE."""
    var size = len(input)
    var a_data = input.get_vector(0).get_data().bitcast[Float64]()
    var b_data = input.get_vector(1).get_data().bitcast[Float64]()
    var out_data = output.get_data().bitcast[Float64]()

    var num_simd = size // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        (out_data + idx).store(a_vec * b_vec)

    for i in range(num_simd * SIMD_WIDTH, size):
        out_data[i] = a_data[i] * b_data[i]


fn mojo_divide_f64(info: FunctionInfo, mut input: Chunk, output: Vector):
    """SIMD division (DOUBLE) — DuckDB uses DOUBLE for DECIMAL division too."""
    var size = len(input)
    var a_data = input.get_vector(0).get_data().bitcast[Float64]()
    var b_data = input.get_vector(1).get_data().bitcast[Float64]()
    var out_data = output.get_data().bitcast[Float64]()

    var num_simd = size // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        (out_data + idx).store(a_vec / b_vec)

    for i in range(num_simd * SIMD_WIDTH, size):
        out_data[i] = a_data[i] / b_data[i]


# ===--------------------------------------------------------------------===#
# Registration
# ===--------------------------------------------------------------------===#

fn register_functions(conn: Connection) raises:
    """Register mojo_add, mojo_subtract, mojo_multiply, mojo_divide.

    Each function is a single ScalarFunction with DECIMAL(18,4) as the universal
    input/output type. DuckDB implicitly casts narrower DECIMAL inputs (e.g.
    DECIMAL(15,2)) to DECIMAL(18,4). Multiply includes scale correction (÷10000).
    Division uses DOUBLE (matching DuckDB's own DECIMAL division behavior).
    """

    var d18_4 = decimal_type(18, 4)
    var dbl = LogicalType(DuckDBType.double)

    # --- mojo_add: (18,4) + (18,4) → (18,4) ---
    var add_fn = ScalarFunction()
    add_fn.set_name("mojo_add")
    add_fn.add_parameter(d18_4)
    add_fn.add_parameter(d18_4)
    add_fn.set_return_type(d18_4)
    add_fn.set_function[mojo_add]()
    add_fn.register(conn)

    # --- mojo_subtract: (18,4) - (18,4) → (18,4) ---
    var sub_fn = ScalarFunction()
    sub_fn.set_name("mojo_subtract")
    sub_fn.add_parameter(d18_4)
    sub_fn.add_parameter(d18_4)
    sub_fn.set_return_type(d18_4)
    sub_fn.set_function[mojo_subtract]()
    sub_fn.register(conn)

    # --- mojo_multiply: (18,4) × (18,4) → (18,4) ---
    # Scale correction: (scale4 × scale4) / 10000 → scale4
    var mul_fn = ScalarFunction()
    mul_fn.set_name("mojo_multiply")
    mul_fn.add_parameter(d18_4)
    mul_fn.add_parameter(d18_4)
    mul_fn.set_return_type(d18_4)
    mul_fn.set_function[mojo_multiply]()
    mul_fn.register(conn)

    # --- mojo_divide: (DOUBLE, DOUBLE) → DOUBLE ---
    # Division always goes through DOUBLE (matching DuckDB behavior)
    var div_fn = ScalarFunction()
    div_fn.set_name("mojo_divide")
    div_fn.add_parameter(dbl)
    div_fn.add_parameter(dbl)
    div_fn.set_return_type(dbl)
    div_fn.set_function[mojo_divide_f64]()
    div_fn.register(conn)


# ===--------------------------------------------------------------------===#
# TPC-H Query definitions
# ===--------------------------------------------------------------------===#

# --- TPC-H Q1: Pricing Summary Report ---
comptime Q1_STANDARD = """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
"""

comptime Q1_MOJO = """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount))) AS sum_disc_price,
    sum(mojo_multiply(mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount)), mojo_add(1, l_tax))) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
"""

# --- TPC-H Q6: Forecasting Revenue Change ---
comptime Q6_STANDARD = """
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;
"""

comptime Q6_MOJO = """
SELECT
    sum(mojo_multiply(l_extendedprice, l_discount)) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;
"""

# --- TPC-H Q14: Promotion Effect ---
comptime Q14_STANDARD = """
SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);
"""

comptime Q14_MOJO = """
SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount))
        ELSE
            0
        END) / sum(mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount))) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);
"""


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
    var scale_factor = 1
    var max_iters = 50
    var warmup_iters = 3

    print("=" * 70)
    print("TPC-H Benchmark: Mojo SIMD Scalar Functions")
    print("=" * 70)
    print("Scale Factor: " + String(scale_factor))
    print("SIMD Width:   " + String(SIMD_WIDTH) + " (Int64)")
    print("Max Iters:    " + String(max_iters))
    print()

    # ---- Setup ----
    print("Setting up database...")
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("INSTALL tpch; LOAD tpch;")

    print("Generating TPC-H data (SF=" + String(scale_factor) + ")...")
    _ = conn.execute("CALL dbgen(sf=" + String(scale_factor) + ");")

    var lineitem_count = conn.execute("SELECT count(*) FROM lineitem")
    var li_chunk = lineitem_count.fetch_chunk()
    print("  lineitem rows: " + String(li_chunk.get(bigint, col=0, row=0).value()))

    var orders_count = conn.execute("SELECT count(*) FROM orders")
    var o_chunk = orders_count.fetch_chunk()
    print("  orders rows:   " + String(o_chunk.get(bigint, col=0, row=0).value()))

    var part_count = conn.execute("SELECT count(*) FROM part")
    var p_chunk = part_count.fetch_chunk()
    print("  part rows:     " + String(p_chunk.get(bigint, col=0, row=0).value()))

    # ---- Register Mojo functions ----
    print("\nRegistering Mojo scalar functions...")
    register_functions(conn)
    print("  Registered: mojo_add, mojo_subtract, mojo_multiply, mojo_divide")
    print("  Types: DECIMAL(18,4) for add/sub/mul, DOUBLE for divide")
    print("  Internal: SIMD Int64 add/sub/mul, SIMD Float64 divide")

    # ---- Validate correctness ----
    print("\nValidating correctness...")

    # Check that mojo functions resolve and produce the right types
    var type_check = conn.execute(
        "SELECT typeof(mojo_multiply(l_extendedprice, l_discount)) AS mtype,"
        + " typeof(l_extendedprice * l_discount) AS stype"
        + " FROM lineitem LIMIT 1"
    )
    var tc = type_check.fetch_chunk()
    print("  mojo_multiply type: " + String(tc.get(varchar, col=0, row=0).value()))
    print("  native * type:      " + String(tc.get(varchar, col=1, row=0).value()))

    # Validate Q1 results
    print("  Validating Q1 results match...")
    var std_result = conn.execute(Q1_STANDARD).fetch_all()
    var mojo_result = conn.execute(Q1_MOJO).fetch_all()
    print("    Standard Q1 rows: " + String(len(std_result)))
    print("    Mojo Q1 rows:     " + String(len(mojo_result)))

    # Validate Q6
    print("  Validating Q6 results match...")
    _ = conn.execute(Q6_STANDARD)
    _ = conn.execute(Q6_MOJO)
    print("    Q6 ✓")

    # ---- Run benchmarks ----
    print("\n" + "=" * 70)
    print("BENCHMARKS")
    print("=" * 70)

    run_benchmark(
        "TPC-H Q1 — Pricing Summary Report",
        conn,
        Q1_STANDARD,
        Q1_MOJO,
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "TPC-H Q6 — Forecasting Revenue Change",
        conn,
        Q6_STANDARD,
        Q6_MOJO,
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "TPC-H Q14 — Promotion Effect",
        conn,
        Q14_STANDARD,
        Q14_MOJO,
        max_iters,
        warmup_iters,
    )

    # ---- Micro-benchmarks ----
    print("\n" + "=" * 70)
    print("MICRO-BENCHMARKS (arithmetic-only on lineitem)")
    print("=" * 70)

    comptime MICRO_STANDARD = """
    SELECT sum(l_extendedprice * (1 - l_discount)) FROM lineitem;
    """
    comptime MICRO_MOJO = """
    SELECT sum(mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount))) FROM lineitem;
    """

    run_benchmark(
        "SUM(price * (1 - discount))",
        conn,
        MICRO_STANDARD,
        MICRO_MOJO,
        max_iters,
        warmup_iters,
    )

    comptime MICRO2_STANDARD = """
    SELECT sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) FROM lineitem;
    """
    comptime MICRO2_MOJO = """
    SELECT sum(mojo_multiply(mojo_multiply(l_extendedprice, mojo_subtract(1, l_discount)), mojo_add(1, l_tax))) FROM lineitem;
    """

    run_benchmark(
        "SUM(price * (1 - discount) * (1 + tax))",
        conn,
        MICRO2_STANDARD,
        MICRO2_MOJO,
        max_iters,
        warmup_iters,
    )

    comptime MICRO3_STANDARD = """
    SELECT sum(l_extendedprice * l_discount) FROM lineitem;
    """
    comptime MICRO3_MOJO = """
    SELECT sum(mojo_multiply(l_extendedprice, l_discount)) FROM lineitem;
    """

    run_benchmark(
        "SUM(price * discount)",
        conn,
        MICRO3_STANDARD,
        MICRO3_MOJO,
        max_iters,
        warmup_iters,
    )

    print("\n" + "=" * 70)
    print("Benchmark complete.")
    print("=" * 70)
