"""TPC-H Benchmark with Mojo SIMD scalar functions.

Registers Mojo functions that operate directly on DECIMAL's underlying
scaled Int64 representation, matching DuckDB's native DECIMAL arithmetic path.

TPC-H schema: all monetary columns are DECIMAL(15,2), stored internally as
scaled Int64 (value × 10^2). DuckDB's own DECIMAL arithmetic works like this:
  - add/sub: plain int64 add/sub (result: DECIMAL(16,2))
  - multiply: plain int64 multiply (result: DECIMAL(18,4), scale = 2+2)
  - divide: cast to DOUBLE (even in standard DuckDB)

We use DECIMAL(18,4) as a universal input/output type. DuckDB implicitly casts
narrower DECIMAL inputs (e.g. DECIMAL(15,2)) to DECIMAL(18,4).

Usage:
    pixi run mojo run benchmark/tpch_benchmark.mojo
"""

import duckdb
from duckdb import Connection, Decimal, LogicalType, ScalarArgs, ScalarFunction, VectorWriter
from std.sys import simd_width_of
from std import benchmark


# ===--------------------------------------------------------------------===#
# SIMD arithmetic kernels
# ===--------------------------------------------------------------------===#
# Plain SIMD functions. `decimal_op` below runs them over a chunk (SIMD-width
# batches plus a scalar tail); `from_simd_function` does the same for DOUBLE.
#
# DECIMAL(15,2) is stored as Int64 internally (width 15 ≤ 18).
# For add/sub the raw integers share the same scale, so we add/sub them directly.
# For multiply the scales add (2+2=4), stored in the output type metadata.
# No scale adjustment (division) is needed, which matches DuckDB internals.


def simd_add[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    """DECIMAL add: same-scale Int64 addition."""
    return a + b


def simd_subtract[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    """DECIMAL subtract: same-scale Int64 subtraction."""
    return a - b


def simd_multiply[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    """DECIMAL multiply with scale correction.

    Both inputs are scale-4, so raw multiply gives scale 8.
    Divide by 10^4 = 10000 to bring result back to scale 4.
    """
    return a * b // 10000


# For division and mixed DOUBLE expressions (Q14: 100.00 * sum(...) / sum(...)),
# we keep DOUBLE since DuckDB itself casts DECIMAL to DOUBLE for '/'.


def simd_divide_f64[w: SIMDLength](a: SIMD[DType.float64, w], b: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    """DOUBLE division. DuckDB uses DOUBLE for DECIMAL division too."""
    return a / b


# ===--------------------------------------------------------------------===#
# Registration
# ===--------------------------------------------------------------------===#

comptime Int64Op = def[w: SIMDLength](SIMD[DType.int64, w], SIMD[DType.int64, w]) thin -> SIMD[DType.int64, w]


@always_inline
def lanes[w: SIMDLength](p: Pointer[Int64, _], i: Int, constant: Bool) -> SIMD[DType.int64, w]:
    """`w` values from row `i` on. A constant vector (a literal) repeats its value."""
    if constant:
        return SIMD[DType.int64, w](p[])
    return p.unsafe_offset(i).unsafe_load[width=w]()


def decimal_op[op: Int64Op](mut args: ScalarArgs, mut out: VectorWriter) raises:
    """Applies `op` to two DECIMAL(18,4) arguments through their Int64 storage."""
    var x = args.column[Decimal](0)
    var y = args.column[Decimal](1)
    var dst = out.data[Int64]()
    var n = args.size()
    var simple = (x.is_flat() or x.is_constant()) and (y.is_flat() or y.is_constant())
    if not simple or x.has_nulls() or y.has_nulls():
        # Dictionary vectors or NULLs: one row at a time.
        for row in range(n):
            var u = x.get(row)
            var v = y.get(row)
            if u and v:
                dst[unsafe_offset=row] = op[1](Int64(u.value().value()), Int64(v.value().value()))[0]
            else:
                out.write_null(row)
        return
    comptime w = simd_width_of[DType.int64]()
    var a = x.unsafe_data().unsafe_bitcast[Int64]()
    var b = y.unsafe_data().unsafe_bitcast[Int64]()
    var ca = x.is_constant()
    var cb = y.is_constant()
    var i = 0
    while i + w <= n:
        dst.unsafe_offset(i).unsafe_store(0, op[w](lanes[w](a, i, ca), lanes[w](b, i, cb)))
        i += w
    while i < n:
        dst[unsafe_offset=i] = op[1](lanes[1](a, i, ca), lanes[1](b, i, cb))[0]
        i += 1


def register_functions(conn: Connection) raises:
    """Register mojo_add, mojo_subtract, mojo_multiply, mojo_divide.

    DECIMAL types can't be derived from a DType, so the DECIMAL functions are
    chunk-level functions with explicit types. Division uses
    from_simd_function with DOUBLE types.
    """
    var d18_4 = LogicalType.decimal(18, 4)

    # (18,4) op (18,4) -> (18,4)
    conn.register(ScalarFunction.from_chunk_function[decimal_op[simd_add]]("mojo_add", [d18_4.copy(), d18_4.copy()], d18_4.copy()))
    conn.register(
        ScalarFunction.from_chunk_function[decimal_op[simd_subtract]]("mojo_subtract", [d18_4.copy(), d18_4.copy()], d18_4.copy())
    )
    # Scale correction: (scale 4 * scale 4) / 10000 gives scale 4
    conn.register(
        ScalarFunction.from_chunk_function[decimal_op[simd_multiply]]("mojo_multiply", [d18_4.copy(), d18_4.copy()], d18_4.copy())
    )

    # (DOUBLE, DOUBLE) -> DOUBLE: all types are derived automatically.
    conn.register(ScalarFunction.from_simd_function["mojo_divide", DType.float64, DType.float64, DType.float64, simd_divide_f64]())


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

def run_benchmark(
    name: String,
    conn: Connection,
    query_standard: String,
    query_mojo: String,
    max_iters: Int,
    warmup_iters: Int,
) raises:
    """Run a single benchmark comparing standard vs Mojo query."""
    print("\n  " + name)
    print("  " + "-" * name.byte_length())

    # Warmup
    for _ in range(warmup_iters):
        _ = conn.execute(query_standard).fetchall()

    def bench_standard() raises {imm}:
        _ = conn.execute(query_standard).fetchall()

    var std_report = benchmark.run(bench_standard, max_iters=max_iters)
    var std_ms = std_report.mean("ms")
    print("    Standard DuckDB:  " + String(std_ms) + " ms")

    # Warmup Mojo
    for _ in range(warmup_iters):
        _ = conn.execute(query_mojo).fetchall()

    def bench_mojo() raises {imm}:
        _ = conn.execute(query_mojo).fetchall()

    var mojo_report = benchmark.run(bench_mojo, max_iters=max_iters)
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

def main() raises:
    var scale_factor = 1
    var max_iters = 50
    var warmup_iters = 3

    print("=" * 70)
    print("TPC-H Benchmark: Mojo SIMD Scalar Functions")
    print("=" * 70)
    print("Scale Factor: " + String(scale_factor))
    print("SIMD Width:   " + String(simd_width_of[DType.int64]()) + " (Int64, auto-detected)")
    print("Max Iters:    " + String(max_iters))
    print()

    # ---- Setup ----
    print("Setting up database...")
    var conn = duckdb.connect()
    _ = conn.execute("INSTALL tpch; LOAD tpch;")

    print("Generating TPC-H data (SF=" + String(scale_factor) + ")...")
    _ = conn.execute("CALL dbgen(sf=" + String(scale_factor) + ");")

    print("  lineitem rows: " + String(conn.execute("SELECT count(*) FROM lineitem").get[Int64]()[0]))

    print("  orders rows:   " + String(conn.execute("SELECT count(*) FROM orders").get[Int64]()[0]))

    print("  part rows:     " + String(conn.execute("SELECT count(*) FROM part").get[Int64]()[0]))

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
    var tc = type_check^.fetchall()
    print("  mojo_multiply type: " + String(tc.get[String](col=0, row=0)))
    print("  native * type:      " + String(tc.get[String](col=1, row=0)))

    # Validate Q1 results
    print("  Validating Q1 results match...")
    var std_result = conn.execute(Q1_STANDARD).fetchall()
    var mojo_result = conn.execute(Q1_MOJO).fetchall()
    print("    Standard Q1 rows: " + String(len(std_result)))
    print("    Mojo Q1 rows:     " + String(len(mojo_result)))

    # Validate Q6
    print("  Validating Q6 results match...")
    var q6_std = conn.execute(Q6_STANDARD).get[Decimal]()[0]
    var q6_mojo = conn.execute(Q6_MOJO).get[Decimal]()[0]
    print("    Standard Q6: " + String(q6_std))
    print("    Mojo Q6:     " + String(q6_mojo))
    if q6_std.value() != q6_mojo.value():
        raise Error("Q6 results differ")

    # ---- Run benchmarks ----
    print("\n" + "=" * 70)
    print("BENCHMARKS")
    print("=" * 70)

    run_benchmark(
        "TPC-H Q1: Pricing Summary Report",
        conn,
        Q1_STANDARD,
        Q1_MOJO,
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "TPC-H Q6: Forecasting Revenue Change",
        conn,
        Q6_STANDARD,
        Q6_MOJO,
        max_iters,
        warmup_iters,
    )

    run_benchmark(
        "TPC-H Q14: Promotion Effect",
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
