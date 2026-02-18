"""TPC-H Benchmark with Mojo operator replacement.

Uses DuckDB's operator replacement mechanism to transparently swap standard
arithmetic operators with Mojo SIMD implementations. Registers DECIMAL(18,4)-typed
functions that operate on the underlying scaled Int64 representation.

This gives transparent operator replacement (unchanged PRAGMA tpch(N) queries)
with native DECIMAL arithmetic.

The trade-off: DuckDB upcasts DECIMAL(15,2) inputs to DECIMAL(18,4) before calling
our functions, and multiply needs scale correction (÷10000). Both are cheap integer
operations and numerically exact for TPC-H data.

Requires the 'full' environment (source-built DuckDB with operator replacement):
    pixi run -e full mojo run benchmark/tpch_benchmark_op_replacement.mojo
"""

from duckdb import *
from duckdb.api import DuckDB
from duckdb._libduckdb import *
from operator_replacement import OperatorReplacementLib
import benchmark


# ===--------------------------------------------------------------------===#
# SIMD width for Int64 operations
# ===--------------------------------------------------------------------===#

comptime SIMD_WIDTH = 8


# ===--------------------------------------------------------------------===#
# SIMD-accelerated DECIMAL arithmetic (low-level FFI, scaled Int64)
# ===--------------------------------------------------------------------===#
# These use the low-level FFI signature required by operator replacement.
# DECIMAL(18,4) is stored as Int64 internally (width 18 ≤ 18).
# For add/sub the raw integers share the same scale, so we just add/sub.
# For multiply we need scale correction: (scale4 × scale4) / 10000 → scale4.

fn mojo_add(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """SIMD addition on scaled Int64 DECIMAL values."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)

    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Int64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Int64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Int64]()

    var num_simd = Int(size) // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        result_data.store(idx, a_vec + b_vec)

    for i in range(num_simd * SIMD_WIDTH, Int(size)):
        result_data[i] = a_data[i] + b_data[i]


fn mojo_subtract(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """SIMD subtraction on scaled Int64 DECIMAL values."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)

    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Int64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Int64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Int64]()

    var num_simd = Int(size) // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        result_data.store(idx, a_vec - b_vec)

    for i in range(num_simd * SIMD_WIDTH, Int(size)):
        result_data[i] = a_data[i] - b_data[i]


fn mojo_multiply(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """SIMD multiplication on scaled Int64 DECIMAL values.

    Both inputs are DECIMAL(18,4) — scale 4 each. Raw multiply gives scale 8.
    We divide by 10^4 = 10000 to bring result back to scale 4.
    """
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)

    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Int64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Int64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Int64]()

    comptime SCALE_CORRECTION = SIMD[DType.int64, SIMD_WIDTH](10000)

    var num_simd = Int(size) // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        result_data.store(idx, a_vec * b_vec // SCALE_CORRECTION)

    for i in range(num_simd * SIMD_WIDTH, Int(size)):
        result_data[i] = a_data[i] * b_data[i] // 10000


fn mojo_divide(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """SIMD division (DOUBLE) — DuckDB uses DOUBLE for DECIMAL division too."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)

    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()

    var num_simd = Int(size) // SIMD_WIDTH
    for i in range(num_simd):
        var idx = i * SIMD_WIDTH
        var a_vec = (a_data + idx).load[width=SIMD_WIDTH]()
        var b_vec = (b_data + idx).load[width=SIMD_WIDTH]()
        result_data.store(idx, a_vec / b_vec)

    for i in range(num_simd * SIMD_WIDTH, Int(size)):
        result_data[i] = a_data[i] / b_data[i]


# ===--------------------------------------------------------------------===#
# Registration helpers (low-level FFI)
# ===--------------------------------------------------------------------===#

fn register_decimal_op[
    func: fn (duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> None
](name: String, conn: duckdb_connection) raises:
    """Register a binary operator function as (DECIMAL(18,4), DECIMAL(18,4)) -> DECIMAL(18,4)."""
    ref lib = DuckDB().libduckdb()
    var function = lib.duckdb_create_scalar_function()
    var name_copy = name
    lib.duckdb_scalar_function_set_name(function, name_copy.as_c_string_slice().unsafe_ptr())

    var type = lib.duckdb_create_decimal_type(18, 4)
    lib.duckdb_scalar_function_add_parameter(function, type)
    lib.duckdb_scalar_function_add_parameter(function, type)
    lib.duckdb_scalar_function_set_return_type(function, type)
    lib.duckdb_destroy_logical_type(UnsafePointer(to=type))

    lib.duckdb_scalar_function_set_function(function, func)

    var status = lib.duckdb_register_scalar_function(conn, function)
    if status != DuckDBSuccess:
        raise Error("Failed to register function: " + name)

    lib.duckdb_destroy_scalar_function(UnsafePointer(to=function))


fn register_double_op[
    func: fn (duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> None
](name: String, conn: duckdb_connection) raises:
    """Register a binary operator function as (DOUBLE, DOUBLE) -> DOUBLE."""
    ref lib = DuckDB().libduckdb()
    var function = lib.duckdb_create_scalar_function()
    var name_copy = name
    lib.duckdb_scalar_function_set_name(function, name_copy.as_c_string_slice().unsafe_ptr())

    var type = lib.duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE)
    lib.duckdb_scalar_function_add_parameter(function, type)
    lib.duckdb_scalar_function_add_parameter(function, type)
    lib.duckdb_scalar_function_set_return_type(function, type)
    lib.duckdb_destroy_logical_type(UnsafePointer(to=type))

    lib.duckdb_scalar_function_set_function(function, func)

    var status = lib.duckdb_register_scalar_function(conn, function)
    if status != DuckDBSuccess:
        raise Error("Failed to register function: " + name)

    lib.duckdb_destroy_scalar_function(UnsafePointer(to=function))


# ===--------------------------------------------------------------------===#
# Benchmark helpers
# ===--------------------------------------------------------------------===#

fn run_tpch_query(conn: Connection, query_nr: Int) raises:
    """Run a TPC-H query by number using PRAGMA tpch(N)."""
    _ = conn.execute("PRAGMA tpch(" + String(query_nr) + ");")


fn bench_single_query(
    name: String,
    conn: Connection,
    query_nr: Int,
    max_iters: Int,
    warmup_iters: Int,
) raises -> benchmark.Report:
    """Benchmark a single TPC-H query, returns the report."""
    for _ in range(warmup_iters):
        run_tpch_query(conn, query_nr)

    fn bench_fn() capturing raises:
        run_tpch_query(conn, query_nr)

    return benchmark.run[bench_fn](max_iters=max_iters)


# ===--------------------------------------------------------------------===#
# Main
# ===--------------------------------------------------------------------===#

fn main() raises:
    var scale_factor = 0.1
    var max_iters = 50
    var warmup_iters = 3

    print("=" * 70)
    print("TPC-H Benchmark: Operator Replacement with Mojo")
    print("=" * 70)
    print("Scale Factor: " + String(scale_factor))
    print("SIMD Width:   " + String(SIMD_WIDTH) + " (Int64)")
    print("Max Iters:    " + String(max_iters))
    print()
    print("This benchmark uses transparent operator replacement. Add/sub/mul")
    print("operate on scaled Int64 (DECIMAL), division uses DOUBLE.")
    print()

    # ---- Setup ----
    var db = DuckDB()
    var conn = db.connect(":memory:")

    print("Loading TPC-H extension...")
    _ = conn.execute("INSTALL tpch; LOAD tpch;")

    print("Generating TPC-H data (SF=" + String(scale_factor) + ")...")
    _ = conn.execute("CALL dbgen(sf=" + String(scale_factor) + ");")

    var lineitem_count = conn.execute("SELECT count(*) FROM lineitem")
    var li_chunk = lineitem_count.fetch_chunk()
    print("  lineitem rows: " + String(li_chunk.get(bigint, col=0, row=0).value()))

    var orders_count = conn.execute("SELECT count(*) FROM orders")
    var o_chunk = orders_count.fetch_chunk()
    print("  orders rows:   " + String(o_chunk.get(bigint, col=0, row=0).value()))

    # TPC-H queries to benchmark (arithmetic-heavy)
    var tpch_queries = List[Int]()
    tpch_queries.append(1)
    tpch_queries.append(6)
    tpch_queries.append(14)

    # =========================================================================
    # PHASE 1: Benchmark standard DuckDB operators (baseline)
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: Standard DuckDB operators (baseline)")
    print("=" * 70)

    var baseline_times = List[Float64]()
    for i in range(len(tpch_queries)):
        var qn = tpch_queries[i]
        print("\n  TPC-H Q" + String(qn) + ":", end="  ")
        var report = bench_single_query(
            "Q" + String(qn), conn, qn, max_iters, warmup_iters
        )
        report.print(unit="ms")
        baseline_times.append(report.mean("ms"))

    # =========================================================================
    # PHASE 2: Register Mojo DECIMAL functions and activate op replacement
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 2: Registering Mojo functions & activating op replacement")
    print("=" * 70)

    # Register custom Mojo implementations
    print("\n  Registering Mojo scalar functions...")
    register_decimal_op[mojo_add]("mojo_add", conn._conn)
    register_decimal_op[mojo_subtract]("mojo_subtract", conn._conn)
    register_decimal_op[mojo_multiply]("mojo_multiply", conn._conn)
    # Division stays DOUBLE (matching DuckDB's own behavior)
    register_double_op[mojo_divide]("mojo_divide", conn._conn)
    print("  Registered: mojo_add, mojo_subtract, mojo_multiply, mojo_divide")

    # Map operators to Mojo implementations
    print("\n  Mapping operators to Mojo implementations...")
    var oplib = OperatorReplacementLib()
    oplib.register_function_replacement("+", "mojo_add")
    oplib.register_function_replacement("-", "mojo_subtract")
    oplib.register_function_replacement("*", "mojo_multiply")
    oplib.register_function_replacement("/", "mojo_divide")
    print("  Registered 4 operator replacements: +, -, *, /")

    # Activate operator replacement
    print("\n  Activating optimizer extension...")
    oplib.register_operator_replacement(conn._conn[].__conn)
    print("  Optimizer extension activated ✓")

    # =========================================================================
    # PHASE 3: Benchmark with DECIMAL operator replacement active
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 3: Same TPC-H queries with Mojo operator replacement")
    print("=" * 70)

    var mojo_times = List[Float64]()
    for i in range(len(tpch_queries)):
        var qn = tpch_queries[i]
        print("\n  TPC-H Q" + String(qn) + ":", end="  ")
        var report = bench_single_query(
            "Q" + String(qn), conn, qn, max_iters, warmup_iters
        )
        report.print(unit="ms")
        mojo_times.append(report.mean("ms"))

    # =========================================================================
    # PHASE 4: Performance comparison
    # =========================================================================
    print("\n" + "=" * 70)
    print("PERFORMANCE COMPARISON")
    print("=" * 70)
    print()

    for i in range(len(tpch_queries)):
        var qn = tpch_queries[i]
        var std_ms = baseline_times[i]
        var mojo_ms = mojo_times[i]
        var speedup = std_ms / mojo_ms

        print("  TPC-H Q" + String(qn) + ":")
        print("    Standard DuckDB:            " + String(std_ms) + " ms")
        print("    Mojo (op replace):          " + String(mojo_ms) + " ms")
        if speedup >= 1.0:
            print("    Result:                     " + String(speedup) + "x faster")
        else:
            print("    Result:                     " + String(1.0 / speedup) + "x slower")
        print()

    print("=" * 70)
    print("Benchmark complete.")
    print("=" * 70)
