"""Example: Using DuckDB Operator Replacement from Mojo.

This example demonstrates how to:
1. Register custom math operators implemented in Mojo
2. Map standard operators to custom implementations
3. Activate the optimizer extension
4. Benchmark performance differences
"""

from duckdb.api import DuckDB
from duckdb._libduckdb import *
from operator_replacement import OperatorReplacementLib
import math
import benchmark


# ===--------------------------------------------------------------------===#
# SIMD-accelerated operator implementations
# ===--------------------------------------------------------------------===#

fn mojo_add(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD addition."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16  # Process 16 Float64s at once
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        var b_vec = (b_data + idx).load[width=simd_width]()
        result_data.store(idx, a_vec + b_vec)
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = a_data[i] + b_data[i]

fn mojo_subtract(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD subtraction."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        var b_vec = (b_data + idx).load[width=simd_width]()
        result_data.store(idx, a_vec - b_vec)
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = a_data[i] - b_data[i]

fn mojo_multiply(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD multiplication."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        var b_vec = (b_data + idx).load[width=simd_width]()
        result_data.store(idx, a_vec * b_vec)
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = a_data[i] * b_data[i]

fn mojo_divide(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD division."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        var b_vec = (b_data + idx).load[width=simd_width]()
        result_data.store(idx, a_vec / b_vec)
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = a_data[i] / b_data[i]

fn mojo_sqrt(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD square root."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        result_data.store(idx, math.sqrt(a_vec))
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.sqrt(a_data[i])

fn mojo_log(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD natural logarithm."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float64]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float64]()
    
    comptime simd_width = 16
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        result_data.store(idx, math.log(a_vec))
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.log(a_data[i])


# ===--------------------------------------------------------------------===#
# Registration helpers
# ===--------------------------------------------------------------------===#

fn register_binary_op[func: fn(duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> None](name: String, conn: duckdb_connection) raises:
    """Register a binary operator function (DOUBLE, DOUBLE -> DOUBLE)."""
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

fn register_unary_op[func: fn(duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> None](name: String, conn: duckdb_connection) raises:
    """Register a unary operator function (DOUBLE -> DOUBLE)."""
    ref lib = DuckDB().libduckdb()
    var function = lib.duckdb_create_scalar_function()
    var name_copy = name
    lib.duckdb_scalar_function_set_name(function, name_copy.as_c_string_slice().unsafe_ptr())
    
    var type = lib.duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE)
    lib.duckdb_scalar_function_add_parameter(function, type)
    lib.duckdb_scalar_function_set_return_type(function, type)
    lib.duckdb_destroy_logical_type(UnsafePointer(to=type))
    
    lib.duckdb_scalar_function_set_function(function, func)
    
    var status = lib.duckdb_register_scalar_function(conn, function)
    if status != DuckDBSuccess:
        raise Error("Failed to register function: " + name)
    
    lib.duckdb_destroy_scalar_function(UnsafePointer(to=function))


# ===--------------------------------------------------------------------===#
# Main example
# ===--------------------------------------------------------------------===#

fn main() raises:
    print("=== DuckDB Operator Replacement with Mojo ===\n")
    
    # Benchmark configuration
    var max_iters = 100
    
    var db = DuckDB()
    var conn = db.connect(":memory:")
    
    # IMPORTANT: Create test table BEFORE activating operator replacement
    # This ensures table creation uses standard DuckDB operators
    print("Creating test table with 100M rows...")
    _ = conn.execute("CREATE TABLE numbers AS SELECT (random() * 100)::DOUBLE AS x, (random() * 100)::DOUBLE AS y FROM range(100_000_000)")
    print("✓ Table created\n")
    
    # Define benchmarks that can be used both before and after replacement
    fn bench_add() capturing raises:
        _ = conn.execute("SELECT SUM(x + y) FROM numbers")
    
    fn bench_multiply() capturing raises:
        _ = conn.execute("SELECT SUM(x * y) FROM numbers")
    
    fn bench_complex() capturing raises:
        _ = conn.execute("SELECT SUM(x * y + x - y / 2.0) FROM numbers")
    
    fn bench_sqrt() capturing raises:
        _ = conn.execute("SELECT SUM(sqrt(x)) FROM numbers")
    
    fn bench_log() capturing raises:
        _ = conn.execute("SELECT SUM(ln(x + 1.0)) FROM numbers")
    
    # =========================================================================
    # PHASE 1: Benchmark standard DuckDB operators (baseline)
    # =========================================================================
    print("=" * 70)
    print("PHASE 1: Benchmarking standard DuckDB operators (baseline)")
    print("=" * 70)
    
    print("\n[Standard DuckDB Operators]")
    print("  Addition (x + y):    ", end="")
    var std_add = benchmark.run[bench_add](max_iters=max_iters)
    std_add.print(unit="ms")
    
    print("  Multiplication:      ", end="")
    var std_mul = benchmark.run[bench_multiply](max_iters=max_iters)
    std_mul.print(unit="ms")
    
    print("  Complex expr:        ", end="")
    var std_complex = benchmark.run[bench_complex](max_iters=max_iters)
    std_complex.print(unit="ms")
    
    print("  sqrt(x):             ", end="")
    var std_sqrt = benchmark.run[bench_sqrt](max_iters=max_iters)
    std_sqrt.print(unit="ms")
    
    print("  ln(x+1):             ", end="")
    var std_log = benchmark.run[bench_log](max_iters=max_iters)
    std_log.print(unit="ms")
    
    print()
    
    # =========================================================================
    # PHASE 2: Set up and activate Mojo operator replacement
    # =========================================================================
    print("=" * 70)
    print("PHASE 2: Setting up Mojo operator replacement")
    print("=" * 70)
    
    # Step 1: Register custom Mojo implementations
    print("\nStep 1: Registering Mojo operator implementations...")
    register_binary_op[mojo_add]("mojo_add", conn._conn)
    register_binary_op[mojo_subtract]("mojo_subtract", conn._conn)
    register_binary_op[mojo_multiply]("mojo_multiply", conn._conn)
    register_binary_op[mojo_divide]("mojo_divide", conn._conn)
    register_unary_op[mojo_sqrt]("mojo_sqrt", conn._conn)
    register_unary_op[mojo_log]("mojo_log", conn._conn)
    print("✓ Registered 6 custom functions\n")
    
    # Step 2: Map operators to Mojo implementations
    print("Step 2: Mapping operators to Mojo implementations...")
    var oplib = OperatorReplacementLib()
    oplib.register_function_replacement("+", "mojo_add")
    oplib.register_function_replacement("-", "mojo_subtract")
    oplib.register_function_replacement("*", "mojo_multiply")
    oplib.register_function_replacement("/", "mojo_divide")
    oplib.register_function_replacement("sqrt", "mojo_sqrt")
    oplib.register_function_replacement("ln", "mojo_log")
    print("✓ Registered 6 operator replacements\n")
    
    # Step 3: Activate operator replacement
    print("Step 3: Activating optimizer extension...")
    oplib.register_operator_replacement(conn._conn[].__conn)
    print("✓ Optimizer extension activated\n")
    
    # =========================================================================
    # PHASE 3: Benchmark with Mojo-replaced operators
    # =========================================================================
    print("=" * 70)
    print("PHASE 3: Benchmarking Mojo SIMD operators")
    print("=" * 70)
    
    print("\n[Mojo SIMD Operators]")
    print("  Addition (x + y):    ", end="")
    var mojo_add_time = benchmark.run[bench_add](max_iters=max_iters)
    mojo_add_time.print(unit="ms")
    
    print("  Multiplication:      ", end="")
    var mojo_mul_time = benchmark.run[bench_multiply](max_iters=max_iters)
    mojo_mul_time.print(unit="ms")
    
    print("  Complex expr:        ", end="")
    var mojo_complex_time = benchmark.run[bench_complex](max_iters=max_iters)
    mojo_complex_time.print(unit="ms")
    
    print("  sqrt(x):             ", end="")
    var mojo_sqrt_time = benchmark.run[bench_sqrt](max_iters=max_iters)
    mojo_sqrt_time.print(unit="ms")
    
    print("  ln(x+1):             ", end="")
    var mojo_log_time = benchmark.run[bench_log](max_iters=max_iters)
    mojo_log_time.print(unit="ms")
    
    # =========================================================================
    # PHASE 4: Performance comparison
    # =========================================================================
    print("\n" + "=" * 70)
    print("PERFORMANCE COMPARISON")
    print("=" * 70)
    print()
    
    fn show_speedup(name: String, std_time: benchmark.Report, mojo_time: benchmark.Report):
        # Get times in milliseconds by passing "ms" unit to mean()
        var std_ms = std_time.mean("ms")
        var mojo_ms = mojo_time.mean("ms")
        var speedup = std_ms / mojo_ms
        var improvement = (1.0 - mojo_ms / std_ms) * 100
        print("  " + name)
        print("    Standard: " + String(std_ms) + " ms | Mojo: " + String(mojo_ms) + " ms")
        print("    Speedup: " + String(speedup) + "x | Improvement: " + String(improvement) + "%")
        print()
    
    show_speedup("Addition (x + y):", std_add, mojo_add_time)
    show_speedup("Multiplication (x * y):", std_mul, mojo_mul_time)
    show_speedup("Complex (x*y + x - y/2):", std_complex, mojo_complex_time)
    show_speedup("Square root sqrt(x):", std_sqrt, mojo_sqrt_time)
    show_speedup("Natural log ln(x+1):", std_log, mojo_log_time)
    
    print("=" * 70)
    print("\n✓ All operators successfully replaced with Mojo implementations")
    print("✓ Benchmark comparison complete")
    print("\nNote: Operator replacement works on column expressions.")
    print("Constants like '3 * 4' are folded before optimization.")
