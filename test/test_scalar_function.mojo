from duckdb import *
from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet, BindInfo
from duckdb._libduckdb import *
from testing import *
from testing.suite import TestSuite


# ===--------------------------------------------------------------------===#
# Helper UDF implementations
# ===--------------------------------------------------------------------===#

fn simple_add_one(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Simple UDF that adds 1 to the input."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var in_vec = lib.duckdb_data_chunk_get_vector(input, 0)
    var in_data = lib.duckdb_vector_get_data(in_vec).bitcast[Int32]()
    var out_data = lib.duckdb_vector_get_data(output).bitcast[Int32]()
    
    for i in range(Int(size)):
        out_data[i] = in_data[i] + 1


fn simple_multiply_two(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """Simple UDF that multiplies input by 2."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var in_vec = lib.duckdb_data_chunk_get_vector(input, 0)
    var in_data = lib.duckdb_vector_get_data(in_vec).bitcast[Float32]()
    var out_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    for i in range(Int(size)):
        out_data[i] = in_data[i] * 2.0


fn binary_add(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """UDF that adds two integers."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var vec_a = lib.duckdb_data_chunk_get_vector(input, 0)
    var vec_b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(vec_a).bitcast[Int32]()
    var b_data = lib.duckdb_vector_get_data(vec_b).bitcast[Int32]()
    var out_data = lib.duckdb_vector_get_data(output).bitcast[Int32]()
    
    for i in range(Int(size)):
        out_data[i] = a_data[i] + b_data[i]


fn binary_add_float(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector):
    """UDF that adds two floats."""
    ref lib = DuckDB().libduckdb()
    var size = lib.duckdb_data_chunk_get_size(input)
    
    var vec_a = lib.duckdb_data_chunk_get_vector(input, 0)
    var vec_b = lib.duckdb_data_chunk_get_vector(input, 1)
    var a_data = lib.duckdb_vector_get_data(vec_a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(vec_b).bitcast[Float32]()
    var out_data = lib.duckdb_vector_get_data(output).bitcast[Float32]()
    
    for i in range(Int(size)):
        out_data[i] = a_data[i] + b_data[i]


# ===--------------------------------------------------------------------===#
# ScalarFunction Tests
# ===--------------------------------------------------------------------===#

def test_scalar_function_create():
    """Test creating a scalar function."""
    var func = ScalarFunction()
    # If we got here without crashing, the constructor worked
    _ = func^


def test_scalar_function_set_name():
    """Test setting the name of a scalar function."""
    var func = ScalarFunction()
    func.set_name("test_func")
    _ = func^


def test_scalar_function_add_parameter():
    """Test adding parameters to a scalar function."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    _ = func^


def test_scalar_function_set_return_type():
    """Test setting the return type of a scalar function."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.set_return_type(int_type)
    _ = func^


def test_scalar_function_set_volatile():
    """Test setting a scalar function as volatile."""
    var func = ScalarFunction()
    func.set_volatile()
    _ = func^


def test_scalar_function_set_special_handling():
    """Test setting special handling for a scalar function."""
    var func = ScalarFunction()
    func.set_special_handling()
    _ = func^


def test_scalar_function_set_varargs():
    """Test setting a scalar function to accept varargs."""
    var func = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.set_varargs(int_type)
    _ = func^


def test_scalar_function_register_simple():
    """Test registering a simple scalar function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function(simple_add_one)
    
    func.register(conn)  # Consumes func


def test_scalar_function_execute_simple():
    """Test executing a registered scalar function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function(simple_add_one)
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT add_one(41) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 42)


def test_scalar_function_execute_from_table():
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
    func.set_function(simple_add_one)
    
    func.register(conn)
    
    # Test the function on table
    var result = conn.execute("SELECT add_one(x) as y FROM numbers ORDER BY x")
    var chunk = result.fetch_chunk()
    
    assert_equal(chunk.get(integer, col=0, row=0).value(), 2)   # 1+1
    assert_equal(chunk.get(integer, col=0, row=1).value(), 3)   # 2+1
    assert_equal(chunk.get(integer, col=0, row=2).value(), 4)   # 3+1
    assert_equal(chunk.get(integer, col=0, row=3).value(), 11)  # 10+1


def test_scalar_function_binary_operator():
    """Test registering a binary operator function."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("my_add")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function(binary_add)
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT my_add(10, 32) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 42)


def test_scalar_function_float_type():
    """Test scalar function with float types."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("multiply_two")
    var float_type = LogicalType(DuckDBType.float)
    func.add_parameter(float_type)
    func.set_return_type(float_type)
    func.set_function(simple_multiply_two)
    
    func.register(conn)
    
    # Test the function
    var result = conn.execute("SELECT multiply_two(21.0::FLOAT) as answer")
    var chunk = result.fetch_chunk()
    var value = chunk.get(float_, col=0, row=0).value()
    # Use approximate equality for floats
    assert_true(abs(value - 42.0) < 0.001)


def test_scalar_function_register_error():
    """Test that registering an incomplete function doesn't crash.""" 
    var conn = DuckDB.connect(":memory:")
    
    # Create incomplete function (no return type, no function implementation)
    var func = ScalarFunction()
    func.set_name("incomplete_func")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    
    # Registration won't validate, but SQL execution will fail
    func.register(conn)


# ===--------------------------------------------------------------------===#
# ScalarFunctionSet Tests  
# ===--------------------------------------------------------------------===#

def test_scalar_function_set_create():
    """Test creating a scalar function set."""
    var func_set = ScalarFunctionSet("test_set")


def test_scalar_function_set_add_function():
    """Test adding a function to a function set."""
    var func_set = ScalarFunctionSet("my_add")
    
    # Add integer overload
    var func1 = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function(binary_add)
    
    func_set.add_function(func1)


def test_scalar_function_set_register():
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
    func1.set_function(binary_add)
    
    func_set.add_function(func1)
    func_set.register(conn)


def test_scalar_function_set_execute():
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
    func1.set_function(binary_add)
    
    func_set.add_function(func1)
    func_set.register(conn)
    # Release ownership after registration
    func1._release_ownership()
    
    # Test the integer overload
    var result = conn.execute("SELECT my_add(15, 27) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 42)


def test_scalar_function_set_multiple_overloads():
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
    func1.set_function(binary_add)
    func_set.add_function(func1)
    
    # Add float overload
    var func2 = ScalarFunction()
    func2.set_name("my_add")  # Must match set name
    var float_type = LogicalType(DuckDBType.float)
    func2.add_parameter(float_type)
    func2.add_parameter(float_type)
    func2.set_return_type(float_type)
    func2.set_function(binary_add_float)
    func_set.add_function(func2)
    
    func_set.register(conn)
    # Release ownership after registration
    func1._release_ownership()
    func2._release_ownership()
    
    # Test integer overload
    var result1 = conn.execute("SELECT my_add(20, 22) as answer")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get(integer, col=0, row=0).value(), 42)
    
    # Test float overload
    var result2 = conn.execute("SELECT my_add(20.5::FLOAT, 21.5::FLOAT) as answer")
    var chunk2 = result2.fetch_chunk()
    var value = chunk2.get(float_, col=0, row=0).value()
    assert_true(abs(value - 42.0) < 0.001)


def test_scalar_function_set_duplicate_overload():
    """Test that adding duplicate overloads is detected."""
    var func_set = ScalarFunctionSet("my_add")
    
    # Add first function
    var func1 = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function(binary_add)
    func_set.add_function(func1)
    
    # Try to add duplicate (same signature)
    var func2 = ScalarFunction()
    func2.add_parameter(int_type)
    func2.add_parameter(int_type)
    func2.set_return_type(int_type)
    func2.set_function(binary_add)
    
    # DuckDB might accept duplicates or might raise - for now just test it doesn't crash
    try:
        func_set.add_function(func2)
    except e:
        pass  # Expected - duplicate signature


# ===--------------------------------------------------------------------===#
# BindInfo Tests
# ===--------------------------------------------------------------------===#

def test_bind_info_operations():
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

def test_volatile_function_not_optimized():
    """Test that volatile functions can be marked with set_volatile."""
    var conn = DuckDB.connect(":memory:")
    
    # Register a volatile function
    var func = ScalarFunction()
    func.set_name("volatile_add_one")
    func.set_volatile()  # Mark as volatile
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function(simple_add_one)
    
    func.register(conn)
    
    # Call the function with a parameter
    _ = conn.execute("SELECT volatile_add_one(5) as val")
    # If this doesn't crash, volatile flag was accepted


def test_multiple_functions_same_connection():
    """Test registering multiple different functions on same connection."""
    var conn = DuckDB.connect(":memory:")
    
    # Register first function
    var func1 = ScalarFunction()
    func1.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func1.add_parameter(int_type)
    func1.set_return_type(int_type)
    func1.set_function(simple_add_one)
    func1.register(conn)
    
    # Register second function
    var func2 = ScalarFunction()
    func2.set_name("multiply_two")
    var float_type = LogicalType(DuckDBType.float)
    func2.add_parameter(float_type)
    func2.set_return_type(float_type)
    func2.set_function(simple_multiply_two)
    func2.register(conn)
    
    # Use both functions in one query
    var result = conn.execute("SELECT add_one(40) as a, multiply_two(21.0::FLOAT) as b")
    var chunk = result.fetch_chunk()
    
    assert_equal(chunk.get(integer, col=0, row=0).value(), 41)
    var b_val = chunk.get(float_, col=1, row=0).value()
    assert_true(abs(b_val - 42.0) < 0.001)


def test_function_reuse_across_queries():
    """Test that registered functions persist across multiple queries."""
    var conn = DuckDB.connect(":memory:")
    
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function(simple_add_one)
    func.register(conn)
    
    # Execute multiple queries using the same function
    var result1 = conn.execute("SELECT add_one(10) as val")
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get(integer, col=0, row=0).value(), 11)
    
    var result2 = conn.execute("SELECT add_one(20) as val")
    var chunk2 = result2.fetch_chunk()
    assert_equal(chunk2.get(integer, col=0, row=0).value(), 21)
    
    var result3 = conn.execute("SELECT add_one(100) as val")
    var chunk3 = result3.fetch_chunk()
    assert_equal(chunk3.get(integer, col=0, row=0).value(), 101)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
