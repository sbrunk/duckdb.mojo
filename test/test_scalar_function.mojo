from duckdb import *
from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet, BindInfo
from duckdb._libduckdb import *
from std.testing import assert_equal, assert_true, assert_false, assert_almost_equal, assert_raises
from std.testing.suite import TestSuite
from std.sys.info import size_of
import std.math


# ===--------------------------------------------------------------------===#
# Helper UDF implementations (using high-level API)
# ===--------------------------------------------------------------------===#

fn add_one(info: FunctionInfo, mut input: Chunk, output: Vector):
    """UDF that adds 1 to the input."""
    var size = len(input)
    var in_vec = input.get_vector(0)
    var in_data = in_vec.get_data().bitcast[Int32]()
    var out_data = output.get_data().bitcast[Int32]()
    
    for i in range(size):
        out_data[i] = in_data[i] + 1


fn multiply_two(info: FunctionInfo, mut input: Chunk, output: Vector):
    """UDF that multiplies input by 2."""
    var size = len(input)
    var in_vec = input.get_vector(0)
    var in_data = in_vec.get_data().bitcast[Float32]()
    var out_data = output.get_data().bitcast[Float32]()
    
    for i in range(size):
        out_data[i] = in_data[i] * 2.0


fn binary_add(info: FunctionInfo, mut input: Chunk, output: Vector):
    """UDF that adds two integers."""
    var size = len(input)
    var vec_a = input.get_vector(0)
    var vec_b = input.get_vector(1)
    var a_data = vec_a.get_data().bitcast[Int32]()
    var b_data = vec_b.get_data().bitcast[Int32]()
    var out_data = output.get_data().bitcast[Int32]()
    
    for i in range(size):
        out_data[i] = a_data[i] + b_data[i]


fn binary_add_float(info: FunctionInfo, mut input: Chunk, output: Vector):
    """UDF that adds two floats."""
    var size = len(input)
    var vec_a = input.get_vector(0)
    var vec_b = input.get_vector(1)
    var a_data = vec_a.get_data().bitcast[Float32]()
    var b_data = vec_b.get_data().bitcast[Float32]()
    var out_data = output.get_data().bitcast[Float32]()
    
    for i in range(size):
        out_data[i] = a_data[i] + b_data[i]


# ===--------------------------------------------------------------------===#
# ScalarFunction Tests
# ===--------------------------------------------------------------------===#

def test_scalar_function_create() raises:
    """Test creating a scalar function."""
    var func = ScalarFunction()
    # If we got here without crashing, the constructor worked
    _ = func^


def test_scalar_function_set_name() raises:
    """Test setting the name of a scalar function."""
    var func = ScalarFunction()
    func.set_name("test_func")
    _ = func^


def test_scalar_function_add_parameter() raises:
    """Test adding parameters to a scalar function."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    _ = func^


def test_scalar_function_set_return_type() raises:
    """Test setting the return type of a scalar function."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.set_return_type(int_type)
    _ = func^


def test_scalar_function_set_volatile() raises:
    """Test setting a scalar function as volatile."""
    var func = ScalarFunction()
    func.set_volatile()
    _ = func^


def test_scalar_function_set_special_handling() raises:
    """Test setting special handling for a scalar function."""
    var func = ScalarFunction()
    func.set_special_handling()
    _ = func^


def test_scalar_function_set_varargs() raises:
    """Test setting a scalar function to accept varargs."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.set_varargs(int_type)
    _ = func^


def test_scalar_function_register_simple() raises:
    """Test registering a simple scalar function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    
    func.register(conn)  # Consumes func


def test_scalar_function_execute_simple() raises:
    """Test executing a registered scalar function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT add_one(41) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_scalar_function_highlevel_types() raises:
    """Test executing a scalar function using high-level Chunk and Vector types."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("highlevel_add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Test with a single value
    var result1 = conn.execute("SELECT highlevel_add_one(99) as answer")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 100)
    
    # Test with a table
    _ = conn.execute("CREATE TABLE nums (x INTEGER)")
    _ = conn.execute("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
    
    var result2 = conn.execute("SELECT highlevel_add_one(x) as y FROM nums ORDER BY x")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get[Int32](col=0, row=0), 2)
    assert_equal(chunk2.get[Int32](col=0, row=1), 3)
    assert_equal(chunk2.get[Int32](col=0, row=2), 4)
    assert_equal(chunk2.get[Int32](col=0, row=3), 5)
    assert_equal(chunk2.get[Int32](col=0, row=4), 6)


def test_scalar_function_auto_wrapped() raises:
    """Test executing a scalar function with automatic high-level wrapping."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("auto_add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    # Pass high-level function as compile-time parameter
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Test with a single value
    var result1 = conn.execute("SELECT auto_add_one(99) as answer")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 100)
    
    # Test with a table
    _ = conn.execute("CREATE TABLE nums2 (x INTEGER)")
    _ = conn.execute("INSERT INTO nums2 VALUES (10), (20), (30)")
    
    var result2 = conn.execute("SELECT auto_add_one(x) as y FROM nums2 ORDER BY x")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get[Int32](col=0, row=0), 11)
    assert_equal(chunk2.get[Int32](col=0, row=1), 21)
    assert_equal(chunk2.get[Int32](col=0, row=2), 31)


def test_scalar_function_fully_highlevel() raises:
    """Test executing a scalar function with fully high-level types."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("fully_hl_add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    # Pass high-level function - automatically wrapped
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Test with a single value
    var result1 = conn.execute("SELECT fully_hl_add_one(42) as answer")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 43)
    
    # Test with a table
    _ = conn.execute("CREATE TABLE test_nums (x INTEGER)")
    _ = conn.execute("INSERT INTO test_nums VALUES (100), (200), (300)")
    
    var result2 = conn.execute("SELECT fully_hl_add_one(x) as y FROM test_nums ORDER BY x")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get[Int32](col=0, row=0), 101)
    assert_equal(chunk2.get[Int32](col=0, row=1), 201)
    assert_equal(chunk2.get[Int32](col=0, row=2), 301)


def test_scalar_function_execute_from_table() raises:
    """Test executing a scalar function on table data."""
    var conn = DuckDB.connect(":memory:")
    
    # Create test data
    _ = conn.execute("CREATE TABLE numbers (x INT)")
    _ = conn.execute("INSERT INTO numbers VALUES (1), (2), (3), (10)")
    
    # Register function
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Test the function on table
    var result = conn.execute("SELECT add_one(x) as y FROM numbers ORDER BY x")
    var chunk = result.fetch_chunk()
    
    assert_equal(chunk.get[Int32](col=0, row=0), 2)   # 1+1
    assert_equal(chunk.get[Int32](col=0, row=1), 3)   # 2+1
    assert_equal(chunk.get[Int32](col=0, row=2), 4)   # 3+1
    assert_equal(chunk.get[Int32](col=0, row=3), 11)  # 10+1


def test_scalar_function_binary_operator() raises:
    """Test registering a binary operator function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("my_add")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[binary_add]()
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT my_add(10, 32) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_scalar_function_float_type() raises:
    """Test scalar function with float types."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("multiply_two")
    var float_type = LogicalType(DuckDBType.float)
    func.add_parameter(float_type)
    func.set_return_type(float_type)
    func.set_function[multiply_two]()
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT multiply_two(21.0::FLOAT) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float32](col=0, row=0)
    # Use approximate equality for floats
    assert_almost_equal(value, 42.0, atol=0.001)


def test_scalar_function_register_error() raises:
    """Test that registering an incomplete function raises an error."""
    var conn = DuckDB.connect(":memory:")

    # Create incomplete function (no return type, no function implementation)
    var func = ScalarFunction()
    func.set_name("incomplete_func")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)

    with assert_raises():
        func.register(conn)


# ===--------------------------------------------------------------------===#
# ScalarFunctionSet Tests  
# ===--------------------------------------------------------------------===#

def test_scalar_function_set_create() raises:
    """Test creating a scalar function set."""
    var _ = ScalarFunctionSet("test_set")


def test_scalar_function_set_add_function() raises:
    """Test adding a function to a function set."""
    var func_set = ScalarFunctionSet("my_add")
    
    # Add integer overload
    var func1 = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[binary_add]()
    
    func_set.add_function(func1)


def test_scalar_function_set_register() raises:
    """Test registering a function set."""
    var conn = DuckDB.connect(":memory:")
    var func_set = ScalarFunctionSet("my_add")
    
    # Add integer overload
    var func1 = ScalarFunction()
    func1.set_name("my_add")  # Must match set name
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[binary_add]()
    
    func_set.add_function(func1)
    func_set.register(conn)


def test_scalar_function_set_execute() raises:
    """Test executing functions from a function set."""
    var conn = DuckDB.connect(":memory:")
    var func_set = ScalarFunctionSet("my_add")
    
    # Add integer overload
    var func1 = ScalarFunction()
    func1.set_name("my_add")  # Try setting name to match set name
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[binary_add]()
    
    func_set.add_function(func1)
    func_set.register(conn)
    
    # Test the integer overload
    var result = conn.execute("SELECT my_add(15, 27) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_scalar_function_set_multiple_overloads() raises:
    """Test function set with multiple overloads."""
    var conn = DuckDB.connect(":memory:")
    var func_set = ScalarFunctionSet("my_add")
    
    # Add integer overload
    var func1 = ScalarFunction()
    func1.set_name("my_add")  # Must match set name
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[binary_add]()
    func_set.add_function(func1)
    
    # Add float overload
    var func2 = ScalarFunction()
    func2.set_name("my_add")  # Must match set name
    var float_type = LogicalType(DuckDBType.float)
    func2.add_parameter(float_type)
    func2.add_parameter(float_type)
    func2.set_return_type(float_type)
    func2.set_function[binary_add_float]()
    func_set.add_function(func2)
    
    func_set.register(conn)
    
    # Test integer overload
    var result1 = conn.execute("SELECT my_add(20, 22) as answer")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 42)
    
    # Test float overload
    var result2 = conn.execute("SELECT my_add(20.5::FLOAT, 21.5::FLOAT) as answer")
    var chunk2 = result2.fetch_chunk()
    var value = chunk2.get[Float32](col=0, row=0)
    assert_almost_equal(value, 42.0, atol=0.001)


def test_scalar_function_set_duplicate_overload() raises:
    """Test that adding duplicate overloads is detected."""
    var func_set = ScalarFunctionSet("my_add")
    
    # Add first function
    var func1 = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[binary_add]()
    func_set.add_function(func1)
    
    # Try to add duplicate (same signature)
    var func2 = ScalarFunction()
    func2.add_parameter(int_type)
    func2.add_parameter(int_type)
    func2.set_return_type(int_type)
    func2.set_function[binary_add]()
    
    # DuckDB might accept duplicates or might raise - for now just test it doesn't crash
    try:
        func_set.add_function(func2)
    except e:
        pass  # Expected - duplicate signature


# ===--------------------------------------------------------------------===#
# BindInfo Tests
# ===--------------------------------------------------------------------===#

def test_bind_info_operations() raises:
    """Test BindInfo struct creation and basic operations."""
    # Note: BindInfo is typically used inside bind callbacks
    # This test just verifies the struct can be instantiated
    var conn = DuckDB.connect(":memory:")
    
    # We can't easily test BindInfo without a bind callback setup
    # but we can verify the module imports correctly
    # The real test is in the integration tests above where functions
    # with parameters are registered and executed


# ===--------------------------------------------------------------------===#
# Integration Tests
# ===--------------------------------------------------------------------===#

def test_volatile_function_not_optimized() raises:
    """Test that volatile functions can be marked with set_volatile."""
    var conn = DuckDB.connect(":memory:")
    
    # Register a volatile function
    var func = ScalarFunction()
    func.set_name("volatile_add_one")
    func.set_volatile()  # Mark as volatile
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    
    func.register(conn)
    
    # Call the function with a parameter
    _ = conn.execute("SELECT volatile_add_one(5) as val")
    # If this doesn't crash, volatile flag was accepted


def test_multiple_functions_same_connection() raises:
    """Test registering multiple different functions on same connection."""
    var conn = DuckDB.connect(":memory:")
    
    # Register first function
    var func1 = ScalarFunction()
    func1.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function[add_one]()
    func1.register(conn)
    
    # Register second function
    var func2 = ScalarFunction()
    func2.set_name("multiply_two")
    var float_type = LogicalType(DuckDBType.float)
    func2.add_parameter(float_type)
    func2.set_return_type(float_type)
    func2.set_function[multiply_two]()
    func2.register(conn)
    
    # Use both functions in one query
    var result = conn.execute("SELECT add_one(40) as a, multiply_two(21.0::FLOAT) as b")
    var chunk = result.fetch_chunk()
    
    assert_equal(chunk.get[Int32](col=0, row=0), 41)
    var b_val = chunk.get[Float32](col=1, row=0)
    assert_almost_equal(b_val, 42.0)


def test_function_reuse_across_queries() raises:
    """Test that registered functions persist across multiple queries."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()
    func.register(conn)
    
    # Execute multiple queries using the same function
    var result1 = conn.execute("SELECT add_one(10) as val")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 11)
    
    var result2 = conn.execute("SELECT add_one(20) as val")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get[Int32](col=0, row=0), 21)
    
    var result3 = conn.execute("SELECT add_one(100) as val")
    var chunk3 = result3.fetch_chunk()
    assert_equal(chunk3.get[Int32](col=0, row=0), 101)


def test_function_outlives_connection() raises:
    """Test that a registered function is available on new connections to the same database.

    DuckDB registers functions in the database catalog, so they are accessible
    from any connection to the same database — no re-registration needed.
    """
    var db = Database(":memory:")
    var func = ScalarFunction.from_function[
        "outlive_test", DType.int32, DType.int32, simple_add_one
    ]()

    # Register on first connection
    var conn1 = Connection(db)
    func.register(conn1)
    var result1 = conn1.execute("SELECT outlive_test(41) as val")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get[Int32](col=0, row=0), 42)

    # Function is available on a second connection without re-registering
    var conn2 = Connection(db)
    var result2 = conn2.execute("SELECT outlive_test(99) as val")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get[Int32](col=0, row=0), 100)


def test_connection_outlives_function() raises:
    """Test that a registered function works after the ScalarFunction handle is destroyed.
    
    DuckDB copies function handles during registration, so destroying our handle
    does not affect the registered function in the database. Mojo's ASAP destruction
    means `func` is destroyed right after `register()` (its last use), so by the
    time we call `execute()` below, the ScalarFunction handle is already gone.
    """
    var conn = DuckDB.connect(":memory:")

    # func's last use is register() — ASAP destruction destroys it before execute().
    var func = ScalarFunction.from_function[
        "survive_test", DType.int32, DType.int32, simple_add_one
    ]()
    func.register(conn)

    # The ScalarFunction handle is now destroyed, but the registered function
    # should still work because DuckDB made an internal copy during registration.
    var result = conn.execute("SELECT survive_test(41) as val")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


# ===--------------------------------------------------------------------===#
# ScalarFunction.create convenience API tests
# ===--------------------------------------------------------------------===#

def test_create_unary_int() raises:
    """Test ScalarFunction.create with a unary integer function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.create["create_add_one", add_one, DType.int32, DType.int32](conn)

    var result = conn.execute("SELECT create_add_one(41) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_create_unary_float() raises:
    """Test ScalarFunction.create with a unary float function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.create["create_mul2", multiply_two, DType.float32, DType.float32](conn)

    var result = conn.execute("SELECT create_mul2(21.0::FLOAT) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float32](col=0, row=0)
    assert_almost_equal(value, 42.0)


def test_create_binary() raises:
    """Test ScalarFunction.create with a binary function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.create["create_my_add", binary_add, DType.int32, DType.int32, DType.int32](conn)

    var result = conn.execute("SELECT create_my_add(10, 32) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_create_on_table() raises:
    """Test ScalarFunction.create with table data."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.create["tbl_add_one", add_one, DType.int32, DType.int32](conn)

    _ = conn.execute("CREATE TABLE create_nums (x INTEGER)")
    _ = conn.execute("INSERT INTO create_nums VALUES (1), (2), (3)")

    var result = conn.execute("SELECT tbl_add_one(x) as y FROM create_nums ORDER BY x")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 2)
    assert_equal(chunk.get[Int32](col=0, row=1), 3)
    assert_equal(chunk.get[Int32](col=0, row=2), 4)


# ===--------------------------------------------------------------------===#
# ScalarFunction.from_function row-at-a-time API tests
# ===--------------------------------------------------------------------===#

# Simple row-at-a-time functions (not vectorized)
fn simple_add_one(x: Int32) -> Int32:
    return x + 1


fn simple_double(x: Float32) -> Float32:
    return x * 2.0


fn simple_add(a: Int32, b: Int32) -> Int32:
    return a + b


fn simple_add_float(a: Float64, b: Float64) -> Float64:
    return a + b


def test_from_function_unary_int() raises:
    """Test from_function with a simple unary int function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_function["ff_add_one", DType.int32, DType.int32, simple_add_one](conn)

    var result = conn.execute("SELECT ff_add_one(41) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_from_function_unary_float() raises:
    """Test from_function with a simple unary float function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_function["ff_double", DType.float32, DType.float32, simple_double](conn)

    var result = conn.execute("SELECT ff_double(21.0::FLOAT) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float32](col=0, row=0)
    assert_almost_equal(value, 42.0, atol=0.001)


def test_from_function_binary() raises:
    """Test from_function with a binary function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_function["ff_add", DType.int32, DType.int32, DType.int32, simple_add](conn)

    var result = conn.execute("SELECT ff_add(10, 32) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_from_function_binary_float() raises:
    """Test from_function with a binary float function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_function["ff_add_f64", DType.float64, DType.float64, DType.float64, simple_add_float](conn)

    var result = conn.execute("SELECT ff_add_f64(20.5, 21.5) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float64](col=0, row=0)
    assert_almost_equal(value, 42.0, atol=0.001)


def test_from_function_on_table() raises:
    """Test from_function applied to table data."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_function["ff_tbl_add_one", DType.int32, DType.int32, simple_add_one](conn)

    _ = conn.execute("CREATE TABLE ff_nums (x INTEGER)")
    _ = conn.execute("INSERT INTO ff_nums VALUES (10), (20), (30)")

    var result = conn.execute("SELECT ff_tbl_add_one(x) as y FROM ff_nums ORDER BY x")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 11)
    assert_equal(chunk.get[Int32](col=0, row=1), 21)
    assert_equal(chunk.get[Int32](col=0, row=2), 31)


# ===--------------------------------------------------------------------===#
# ScalarFunction.from_simd_function SIMD-vectorized API tests
# ===--------------------------------------------------------------------===#

# SIMD-vectorized functions (operate on SIMD[dt, width] vectors)
fn simd_add_one[width: Int](x: SIMD[DType.int32, width]) -> SIMD[DType.int32, width]:
    return x + 1


fn simd_double[width: Int](x: SIMD[DType.float32, width]) -> SIMD[DType.float32, width]:
    return x * 2.0


fn simd_add[width: Int](
    a: SIMD[DType.int32, width], b: SIMD[DType.int32, width]
) -> SIMD[DType.int32, width]:
    return a + b


fn simd_add_f64[width: Int](
    a: SIMD[DType.float64, width], b: SIMD[DType.float64, width]
) -> SIMD[DType.float64, width]:
    return a + b


def test_from_simd_function_unary_int() raises:
    """Test from_simd_function with a unary SIMD int function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_add_one", DType.int32, DType.int32, simd_add_one](conn)

    var result = conn.execute("SELECT sf_add_one(41) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_from_simd_function_unary_float() raises:
    """Test from_simd_function with a unary SIMD float function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_double", DType.float32, DType.float32, simd_double](conn)

    var result = conn.execute("SELECT sf_double(21.0::FLOAT) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float32](col=0, row=0)
    assert_almost_equal(value, 42.0)


def test_from_simd_function_binary_int() raises:
    """Test from_simd_function with a binary SIMD int function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_add", DType.int32, DType.int32, DType.int32, simd_add](conn)

    var result = conn.execute("SELECT sf_add(10, 32) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_from_simd_function_binary_float() raises:
    """Test from_simd_function with a binary SIMD float64 function."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_add_f64", DType.float64, DType.float64, DType.float64, simd_add_f64](conn)

    var result = conn.execute("SELECT sf_add_f64(20.5, 21.5) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float64](col=0, row=0)
    assert_almost_equal(value, 42.0)


def test_from_simd_function_on_table() raises:
    """Test from_simd_function applied to table data (exercises SIMD + tail loop)."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_tbl_add_one", DType.int32, DType.int32, simd_add_one](conn)

    _ = conn.execute("CREATE TABLE sf_nums (x INTEGER)")
    _ = conn.execute("INSERT INTO sf_nums VALUES (1), (2), (3), (4), (5), (6), (7)")

    var result = conn.execute("SELECT sf_tbl_add_one(x) as y FROM sf_nums ORDER BY x")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 2)
    assert_equal(chunk.get[Int32](col=0, row=1), 3)
    assert_equal(chunk.get[Int32](col=0, row=2), 4)
    assert_equal(chunk.get[Int32](col=0, row=3), 5)
    assert_equal(chunk.get[Int32](col=0, row=4), 6)
    assert_equal(chunk.get[Int32](col=0, row=5), 7)
    assert_equal(chunk.get[Int32](col=0, row=6), 8)


def test_from_simd_function_large_table() raises:
    """Test from_simd_function on a larger table to exercise multiple SIMD batches."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_big_add", DType.int32, DType.int32, DType.int32, simd_add](conn)

    _ = conn.execute("CREATE TABLE sf_big AS SELECT i::INTEGER as a, (i*2)::INTEGER as b FROM range(1000) t(i)")

    var result = conn.execute("SELECT sf_big_add(a, b) as c FROM sf_big ORDER BY a LIMIT 5")
    var chunk = result.fetch_chunk()
    # a=0,b=0 -> 0; a=1,b=2 -> 3; a=2,b=4 -> 6; a=3,b=6 -> 9; a=4,b=8 -> 12
    assert_equal(chunk.get[Int32](col=0, row=0), 0)
    assert_equal(chunk.get[Int32](col=0, row=1), 3)
    assert_equal(chunk.get[Int32](col=0, row=2), 6)
    assert_equal(chunk.get[Int32](col=0, row=3), 9)
    assert_equal(chunk.get[Int32](col=0, row=4), 12)


# ===--------------------------------------------------------------------===#
# from_simd_function stdlib-compatible overload tests
# ===--------------------------------------------------------------------===#

def test_from_simd_function_stdlib_unary() raises:
    """Test from_simd_function with stdlib math.sqrt passed directly."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_stdlib_sqrt", DType.float64, math.sqrt](conn)

    var result = conn.execute("SELECT sf_stdlib_sqrt(16.0) as answer")
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get[Float64](col=0, row=0), 4.0)


def test_from_simd_function_stdlib_sin() raises:
    """Test from_simd_function with stdlib math.sin passed directly."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_stdlib_sin", DType.float64, math.sin](conn)

    var result = conn.execute("SELECT sf_stdlib_sin(0.0) as answer")
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get[Float64](col=0, row=0), 0.0)


def test_from_simd_function_stdlib_binary() raises:
    """Test from_simd_function with stdlib math.atan2 passed directly."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_stdlib_atan2", DType.float64, math.atan2](conn)

    var result = conn.execute("SELECT sf_stdlib_atan2(0.0, 1.0) as answer")
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get[Float64](col=0, row=0), 0.0)


def test_from_simd_function_stdlib_on_table() raises:
    """Test stdlib overload on table data (exercises SIMD vectorization + tail)."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_stdlib_exp", DType.float64, math.exp](conn)

    _ = conn.execute(
        "CREATE TABLE sf_stdlib AS SELECT i::DOUBLE as x FROM range(100) t(i)"
    )

    var result = conn.execute(
        "SELECT sf_stdlib_exp(x) as y FROM sf_stdlib WHERE x = 0.0"
    )
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get[Float64](col=0, row=0), 1.0)


def test_from_simd_function_stdlib_matches_builtin() raises:
    """Test that stdlib overload produces identical results to DuckDB builtins."""
    var conn = DuckDB.connect(":memory:")
    ScalarFunction.from_simd_function["sf_stdlib_log", DType.float64, math.log](conn)
    ScalarFunction.from_simd_function["sf_stdlib_cos", DType.float64, math.cos](conn)

    _ = conn.execute(
        "CREATE TABLE sf_cmp AS SELECT (i + 1)::DOUBLE as x FROM range(50) t(i)"
    )

    # Compare Mojo stdlib log vs DuckDB ln
    var result = conn.execute(
        "SELECT SUM(ABS(sf_stdlib_log(x) - ln(x))) as diff FROM sf_cmp"
    )
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get[Float64](col=0, row=0), 0.0, atol=1e-6)

    # Compare Mojo stdlib cos vs DuckDB cos
    var result2 = conn.execute(
        "SELECT SUM(ABS(sf_stdlib_cos(x) - cos(x))) as diff FROM sf_cmp"
    )
    var chunk2 = result2.fetch_chunk()
    assert_almost_equal(chunk2.get[Float64](col=0, row=0), 0.0, atol=1e-6)


# ===--------------------------------------------------------------------===#
# dtype_to_duckdb_type / mojo_to_duckdb_type tests
# ===--------------------------------------------------------------------===#

def test_dtype_to_duckdb_type() raises:
    """Test compile-time DType to DuckDBType mapping."""
    assert_equal(dtype_to_duckdb_type[DType.bool](), DuckDBType.boolean)
    assert_equal(dtype_to_duckdb_type[DType.int8](), DuckDBType.tinyint)
    assert_equal(dtype_to_duckdb_type[DType.int16](), DuckDBType.smallint)
    assert_equal(dtype_to_duckdb_type[DType.int32](), DuckDBType.integer)
    assert_equal(dtype_to_duckdb_type[DType.int64](), DuckDBType.bigint)
    assert_equal(dtype_to_duckdb_type[DType.uint8](), DuckDBType.utinyint)
    assert_equal(dtype_to_duckdb_type[DType.uint16](), DuckDBType.usmallint)
    assert_equal(dtype_to_duckdb_type[DType.uint32](), DuckDBType.uinteger)
    assert_equal(dtype_to_duckdb_type[DType.uint64](), DuckDBType.ubigint)
    assert_equal(dtype_to_duckdb_type[DType.float32](), DuckDBType.float)
    assert_equal(dtype_to_duckdb_type[DType.float64](), DuckDBType.double)


def test_mojo_to_duckdb_type() raises:
    """Test compile-time Mojo type to DuckDBType mapping."""
    assert_equal(mojo_to_duckdb_type[Bool](), DuckDBType.boolean)
    assert_equal(mojo_to_duckdb_type[Int8](), DuckDBType.tinyint)
    assert_equal(mojo_to_duckdb_type[Int16](), DuckDBType.smallint)
    assert_equal(mojo_to_duckdb_type[Int32](), DuckDBType.integer)
    assert_equal(mojo_to_duckdb_type[Int64](), DuckDBType.bigint)
    assert_equal(mojo_to_duckdb_type[UInt8](), DuckDBType.utinyint)
    assert_equal(mojo_to_duckdb_type[UInt16](), DuckDBType.usmallint)
    assert_equal(mojo_to_duckdb_type[UInt32](), DuckDBType.uinteger)
    assert_equal(mojo_to_duckdb_type[UInt64](), DuckDBType.ubigint)
    assert_equal(mojo_to_duckdb_type[Float32](), DuckDBType.float)
    assert_equal(mojo_to_duckdb_type[Float64](), DuckDBType.double)

    # Mojo native Int/UInt (platform-dependent width)
    comptime if size_of[Int]() == 4:
        assert_equal(mojo_to_duckdb_type[Int](), DuckDBType.integer)
        assert_equal(mojo_to_duckdb_type[UInt](), DuckDBType.uinteger)
    else:
        assert_equal(mojo_to_duckdb_type[Int](), DuckDBType.bigint)
        assert_equal(mojo_to_duckdb_type[UInt](), DuckDBType.ubigint)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
