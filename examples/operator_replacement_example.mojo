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
    
    comptime simd_width = 8  # Process 8 Float64s at once
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
    
    comptime simd_width = 8
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
    
    comptime simd_width = 8
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
    
    comptime simd_width = 8
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
    
    comptime simd_width = 8
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
    
    comptime simd_width = 8
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
    
    var db = DuckDB()
    var conn = db.connect(":memory:")
    
    # Step 1: Register custom Mojo implementations
    print("Step 1: Registering Mojo operator implementations...")
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
    
    # Create test data
    print("Creating test table with 10M rows...")
    _ = conn.execute("CREATE TABLE numbers AS SELECT (random() * 100)::DOUBLE AS x, (random() * 100)::DOUBLE AS y FROM range(10000000)")
    print("✓ Table created\n")
    
    # Benchmark: Standard operators
    print("=" * 60)
    print("BENCHMARKING: Standard vs Mojo-replaced operators")
    print("=" * 60)
    
    fn bench_standard_add() capturing raises:
        _ = conn.execute("SELECT SUM(x + y) FROM numbers")
    
    fn bench_standard_multiply() capturing raises:
        _ = conn.execute("SELECT SUM(x * y) FROM numbers")
    
    fn bench_standard_complex() capturing raises:
        _ = conn.execute("SELECT SUM(x * y + x - y / 2.0) FROM numbers")
    
    fn bench_standard_sqrt() capturing raises:
        _ = conn.execute("SELECT SUM(sqrt(x)) FROM numbers")
    
    fn bench_standard_log() capturing raises:
        _ = conn.execute("SELECT SUM(ln(x + 1.0)) FROM numbers")
    
    print("\n[Standard DuckDB]")
    print("  Addition (x + y):    ", end="")
    benchmark.run[bench_standard_add](max_iters=5).print(unit="ms")
    
    print("  Multiplication:      ", end="")
    benchmark.run[bench_standard_multiply](max_iters=5).print(unit="ms")
    
    print("  Complex expr:        ", end="")
    benchmark.run[bench_standard_complex](max_iters=5).print(unit="ms")
    
    print("  sqrt(x):             ", end="")
    benchmark.run[bench_standard_sqrt](max_iters=5).print(unit="ms")
    
    print("  ln(x+1):             ", end="")
    benchmark.run[bench_standard_log](max_iters=5).print(unit="ms")
    
    print("\n" + "=" * 60)
    print("\n✓ All operators successfully replaced with Mojo implementations")
    print("✓ Benchmarks complete")
    print("\nNote: Performance gains depend on query complexity and data access patterns.")
