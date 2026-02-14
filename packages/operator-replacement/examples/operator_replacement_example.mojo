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

# NOTE: SIMD width can be tuned - benchmarks show optimal width varies by operation
comptime simd_width = 1

fn mojo_add(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD addition."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
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
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
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
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
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
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
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
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
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
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        result_data.store(idx, math.log(a_vec))
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.log(a_data[i])

fn mojo_cos(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD cosine."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        result_data.store(idx, math.cos(a_vec))
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.cos(a_data[i])

fn mojo_sin(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD sine."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        result_data.store(idx, math.sin(a_vec))
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.sin(a_data[i])

fn mojo_cos_sin(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Fast SIMD fused cosine + sine (computes cos(x) + sin(x))."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var result_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    var num_simd = Int(size) // simd_width
    
    for i in range(num_simd):
        var idx = i * simd_width
        var a_vec = (a_data + idx).load[width=simd_width]()
        var cos_vec = math.cos(a_vec)
        var sin_vec = math.sin(a_vec)
        result_data.store(idx, cos_vec + sin_vec)
    
    for i in range(num_simd * simd_width, Int(size)):
        result_data[i] = math.cos(a_data[i]) + math.sin(a_data[i])


# ===--------------------------------------------------------------------===#
# Registration helpers
# ===--------------------------------------------------------------------===#

fn register_binary_op[func: fn(duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> None](name: String, conn: duckdb_connection) raises:
    """Register a binary operator function (FLOAT, FLOAT -> FLOAT)."""
    ref lib = DuckDB().libduckdb()
    var function = lib.duckdb_create_scalar_function()
    var name_copy = name
    lib.duckdb_scalar_function_set_name(function, name_copy.as_c_string_slice().unsafe_ptr())
    
    var type = lib.duckdb_create_logical_type(DUCKDB_TYPE_FLOAT)
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
    """Register a unary operator function (FLOAT -> FLOAT)."""
    ref lib = DuckDB().libduckdb()
    var function = lib.duckdb_create_scalar_function()
    var name_copy = name
    lib.duckdb_scalar_function_set_name(function, name_copy.as_c_string_slice().unsafe_ptr())
    
    var type = lib.duckdb_create_logical_type(DUCKDB_TYPE_FLOAT)
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
    print("\n" + "=" * 70)
    print("=== DuckDB Operator Replacement with Mojo ===")
    print("=" * 70)
    print("SIMD WIDTH: " + String(simd_width))
    print("=" * 70 + "\n")
    
    # Benchmark configuration
    var max_iters = 200
    
    var db = DuckDB()
    var conn = db.connect(":memory:")
    
    # IMPORTANT: Create test table BEFORE activating operator replacement
    # This ensures table creation uses standard DuckDB operators
    print("Creating test table with 100M rows...")
    _ = conn.execute("CREATE TABLE numbers AS SELECT (random() * 100)::FLOAT AS x, (random() * 100)::FLOAT AS y FROM range(100_000_000)")
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
    
    fn bench_cos() capturing raises:
        _ = conn.execute("SELECT SUM(cos(x)) FROM numbers")
    
    fn bench_sin() capturing raises:
        _ = conn.execute("SELECT SUM(sin(x)) FROM numbers")
    
    fn bench_cos_sin() capturing raises:
        _ = conn.execute("SELECT SUM(cos(x) + sin(x)) FROM numbers")
    
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
    
    print("  cos(x):              ", end="")
    var std_cos = benchmark.run[bench_cos](max_iters=max_iters)
    std_cos.print(unit="ms")
    
    print("  sin(x):              ", end="")
    var std_sin = benchmark.run[bench_sin](max_iters=max_iters)
    std_sin.print(unit="ms")
    
    print("  cos(x)+sin(x):       ", end="")
    var std_cos_sin = benchmark.run[bench_cos_sin](max_iters=max_iters)
    std_cos_sin.print(unit="ms")
    
    print()
    
    # =========================================================================
    # PHASE 2: Register Mojo functions and benchmark explicit calls (NO operator replacement)
    # =========================================================================
    print("=" * 70)
    print("PHASE 2: Benchmarking Mojo functions (explicit calls, no replacement)")
    print("=" * 70)
    
    # Register custom Mojo implementations
    print("\nRegistering Mojo functions (NOT activating operator replacement yet)...")
    register_binary_op[mojo_add]("mojo_add", conn._conn)
    register_binary_op[mojo_subtract]("mojo_subtract", conn._conn)
    register_binary_op[mojo_multiply]("mojo_multiply", conn._conn)
    register_binary_op[mojo_divide]("mojo_divide", conn._conn)
    register_unary_op[mojo_sqrt]("mojo_sqrt", conn._conn)
    register_unary_op[mojo_log]("mojo_log", conn._conn)
    register_unary_op[mojo_cos]("mojo_cos", conn._conn)
    register_unary_op[mojo_sin]("mojo_sin", conn._conn)
    register_unary_op[mojo_cos_sin]("mojo_cos_sin", conn._conn)
    print("✓ Registered 9 custom functions\n")
    
    # Define benchmarks that explicitly call Mojo functions by name
    fn bench_mojo_add_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_add(x, y)) FROM numbers")
    
    fn bench_mojo_multiply_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_multiply(x, y)) FROM numbers")
    
    fn bench_mojo_sqrt_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_sqrt(x)) FROM numbers")
    
    fn bench_mojo_log_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_log(x + 1.0)) FROM numbers")
    
    fn bench_mojo_cos_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_cos(x)) FROM numbers")
    
    fn bench_mojo_sin_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_sin(x)) FROM numbers")
    
    fn bench_mojo_cos_sin_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_add(mojo_cos(x), mojo_sin(x))) FROM numbers")
    
    fn bench_mojo_cos_sin_fused_explicit() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_cos_sin(x)) FROM numbers")
    
    print("[Mojo Functions - Explicit Calls]")
    print("  mojo_add(x, y):      ", end="")
    var explicit_add = benchmark.run[bench_mojo_add_explicit](max_iters=max_iters)
    explicit_add.print(unit="ms")
    
    print("  mojo_multiply(x, y): ", end="")
    var explicit_mul = benchmark.run[bench_mojo_multiply_explicit](max_iters=max_iters)
    explicit_mul.print(unit="ms")
    
    print("  mojo_sqrt(x):        ", end="")
    var explicit_sqrt = benchmark.run[bench_mojo_sqrt_explicit](max_iters=max_iters)
    explicit_sqrt.print(unit="ms")
    
    print("  mojo_log(x+1):       ", end="")
    var explicit_log = benchmark.run[bench_mojo_log_explicit](max_iters=max_iters)
    explicit_log.print(unit="ms")
    
    print("  mojo_cos(x):         ", end="")
    var explicit_cos = benchmark.run[bench_mojo_cos_explicit](max_iters=max_iters)
    explicit_cos.print(unit="ms")
    
    print("  mojo_sin(x):         ", end="")
    var explicit_sin = benchmark.run[bench_mojo_sin_explicit](max_iters=max_iters)
    explicit_sin.print(unit="ms")
    
    print("  mojo_add(cos, sin):  ", end="")
    var explicit_cos_sin = benchmark.run[bench_mojo_cos_sin_explicit](max_iters=max_iters)
    explicit_cos_sin.print(unit="ms")
    
    print("  mojo_cos_sin(x):     ", end="")
    var explicit_fused = benchmark.run[bench_mojo_cos_sin_fused_explicit](max_iters=max_iters)
    explicit_fused.print(unit="ms")
    
    print()
    
    # =========================================================================
    # PHASE 3: Activate operator replacement
    # =========================================================================
    print("=" * 70)
    print("PHASE 3: Activating operator replacement")
    print("=" * 70)
    
    # Map operators to Mojo implementations
    print("\nMapping operators to Mojo implementations...")
    var oplib = OperatorReplacementLib()
    oplib.register_function_replacement("+", "mojo_add")
    oplib.register_function_replacement("-", "mojo_subtract")
    oplib.register_function_replacement("*", "mojo_multiply")
    oplib.register_function_replacement("/", "mojo_divide")
    oplib.register_function_replacement("sqrt", "mojo_sqrt")
    oplib.register_function_replacement("ln", "mojo_log")
    oplib.register_function_replacement("cos", "mojo_cos")
    oplib.register_function_replacement("sin", "mojo_sin")
    print("✓ Registered 8 operator replacements\n")
    
    # Activate operator replacement
    print("Activating optimizer extension...")
    oplib.register_operator_replacement(conn._conn[].__conn)
    print("✓ Optimizer extension activated\n")
    
    # Define fused benchmark (uses replacement)
    fn bench_cos_sin_fused() capturing raises:
        _ = conn.execute("SELECT SUM(mojo_cos_sin(x)) FROM numbers")
    
    # =========================================================================
    # PHASE 4: Benchmark with operator replacement active
    # =========================================================================
    print("=" * 70)
    print("PHASE 4: Benchmarking with operator replacement active")
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
    
    print("  cos(x):              ", end="")
    var mojo_cos_time = benchmark.run[bench_cos](max_iters=max_iters)
    mojo_cos_time.print(unit="ms")
    
    print("  sin(x):              ", end="")
    var mojo_sin_time = benchmark.run[bench_sin](max_iters=max_iters)
    mojo_sin_time.print(unit="ms")
    
    print("  cos(x)+sin(x):       ", end="")
    var mojo_cos_sin_time = benchmark.run[bench_cos_sin](max_iters=max_iters)
    mojo_cos_sin_time.print(unit="ms")
    
    print("  mojo_cos_sin(x):     ", end="")
    var mojo_cos_sin_fused_time = benchmark.run[bench_cos_sin_fused](max_iters=max_iters)
    mojo_cos_sin_fused_time.print(unit="ms")
    
    # =========================================================================
    # PHASE 5: Performance comparison and overhead analysis
    # =========================================================================
    print("\n" + "=" * 70)
    print("PERFORMANCE COMPARISON & OPERATOR REPLACEMENT OVERHEAD ANALYSIS")
    print("=" * 70)
    print()
    
    fn show_speedup_with_overhead(name: String, std_time: benchmark.Report, explicit_time: benchmark.Report, replacement_time: benchmark.Report):
        var std_ms = std_time.mean("ms")
        var explicit_ms = explicit_time.mean("ms")
        var replacement_ms = replacement_time.mean("ms")
        var explicit_speedup = std_ms / explicit_ms
        var replacement_speedup = std_ms / replacement_ms
        var overhead = ((replacement_ms - explicit_ms) / explicit_ms) * 100
        
        print("  " + name)
        print("    DuckDB Standard:      " + String(std_ms) + " ms")
        print("    Mojo Explicit:        " + String(explicit_ms) + " ms (" + String(explicit_speedup) + "x vs std)")
        print("    Mojo w/ Replacement:  " + String(replacement_ms) + " ms (" + String(replacement_speedup) + "x vs std)")
        print("    Replacement Overhead: " + String(overhead) + "%")
        print()
    
    show_speedup_with_overhead("Addition (x + y):", std_add, explicit_add, mojo_add_time)
    show_speedup_with_overhead("Multiplication (x * y):", std_mul, explicit_mul, mojo_mul_time)
    show_speedup_with_overhead("Square root sqrt(x):", std_sqrt, explicit_sqrt, mojo_sqrt_time)
    show_speedup_with_overhead("Natural log ln(x+1):", std_log, explicit_log, mojo_log_time)
    show_speedup_with_overhead("Cosine cos(x):", std_cos, explicit_cos, mojo_cos_time)
    show_speedup_with_overhead("Sine sin(x):", std_sin, explicit_sin, mojo_sin_time)
    
    print("  Separated cos(x)+sin(x):")
    print("    DuckDB Standard:      " + String(std_cos_sin.mean("ms")) + " ms")
    print("    Mojo Explicit:        " + String(explicit_cos_sin.mean("ms")) + " ms")
    print("    Mojo w/ Replacement:  " + String(mojo_cos_sin_time.mean("ms")) + " ms")
    var sep_overhead = ((mojo_cos_sin_time.mean("ms") - explicit_cos_sin.mean("ms")) / explicit_cos_sin.mean("ms")) * 100
    print("    Replacement Overhead: " + String(sep_overhead) + "%")
    print()
    
    print("  Fused mojo_cos_sin(x):")
    print("    DuckDB Standard:      " + String(std_cos_sin.mean("ms")) + " ms")
    print("    Mojo Explicit:        " + String(explicit_fused.mean("ms")) + " ms (" + String(std_cos_sin.mean("ms") / explicit_fused.mean("ms")) + "x vs std)")
    print("    Mojo w/ Replacement:  " + String(mojo_cos_sin_fused_time.mean("ms")) + " ms (" + String(std_cos_sin.mean("ms") / mojo_cos_sin_fused_time.mean("ms")) + "x vs std)")
    var fused_overhead = ((mojo_cos_sin_fused_time.mean("ms") - explicit_fused.mean("ms")) / explicit_fused.mean("ms")) * 100
    print("    Replacement Overhead: " + String(fused_overhead) + "%")
    print()
    
    print("=" * 70)
    print("FUSED vs SEPARATED COMPARISON")
    print("=" * 70)
    var separated_ms = mojo_cos_sin_time.mean("ms")
    var fused_ms = mojo_cos_sin_fused_time.mean("ms")
    var separated_explicit_ms = explicit_cos_sin.mean("ms")
    var fused_explicit_ms = explicit_fused.mean("ms")
    
    print("  Explicit Calls (no replacement overhead):")
    print("    Separated mojo_add(cos, sin): " + String(separated_explicit_ms) + " ms")
    print("    Fused mojo_cos_sin:           " + String(fused_explicit_ms) + " ms")
    print("    Fused speedup:                " + String(separated_explicit_ms / fused_explicit_ms) + "x")
    print()
    print("  With Operator Replacement:")
    print("    Separated via replacement:    " + String(separated_ms) + " ms")
    print("    Fused function call:          " + String(fused_ms) + " ms")
    print("    Fused speedup:                " + String(separated_ms / fused_ms) + "x")
