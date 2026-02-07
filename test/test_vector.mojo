from duckdb import *
from testing import *
from testing.suite import TestSuite


def test_vector_get_column_type():
    """Test getting the column type from a vector."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42 as num, 'hello' as text, 3.14 as pi")

    var chunk = result.fetch_chunk()
    var vec_int = chunk.get_vector(0)
    var vec_str = chunk.get_vector(1)
    var vec_dbl = chunk.get_vector(2)

    assert_equal(vec_int.get_column_type().get_type_id(), DuckDBType.integer)
    assert_equal(vec_str.get_column_type().get_type_id(), DuckDBType.varchar)
    assert_equal(vec_dbl.get_column_type().get_type_id(), DuckDBType.decimal)


def test_vector_create_standalone():
    """Test creating a standalone vector."""
    var int_type = LogicalType(DuckDBType.integer)
    var vec = Vector[origin=MutAnyOrigin](int_type, 10)

    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.integer)


def test_vector_create_varchar_standalone():
    """Test creating a standalone VARCHAR vector."""
    var varchar_type = LogicalType(DuckDBType.varchar)
    var vec = Vector[origin=MutAnyOrigin](varchar_type, 5)

    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.varchar)


def test_vector_assign_string_element():
    """Test assigning string elements to a vector."""
    var varchar_type = LogicalType(DuckDBType.varchar)
    var vec = Vector[origin=MutAnyOrigin](varchar_type, 3)

    # Assign strings to the vector
    vec.assign_string_element(0, "hello")
    vec.assign_string_element(1, "world")
    vec.assign_string_element(2, "test")

    # Vector should still have correct type
    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.varchar)


def test_vector_assign_string_element_len():
    """Test assigning string elements with explicit length."""
    var varchar_type = LogicalType(DuckDBType.varchar)
    var vec = Vector[origin=MutAnyOrigin](varchar_type, 2)

    # Assign strings with explicit length
    vec.assign_string_element_len(0, "hello", 5)
    vec.assign_string_element_len(1, "partial", 4)  # Only first 4 chars

    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.varchar)


def test_vector_get_data():
    """Test getting the data pointer from a vector."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 1, 2, 3")

    var chunk = result.fetch_chunk()
    var vec = chunk.get_vector(0)

    var data_ptr = vec.get_data()
    assert_not_equal(data_ptr, UnsafePointer[NoneType, MutAnyOrigin]())


def test_vector_get_validity():
    """Test getting the validity mask from a vector."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 1, NULL, 3")

    var chunk = result.fetch_chunk()
    var vec = chunk.get_vector(0)

    # Get validity mask - might be NULL if all values are valid
    _ = vec.get_validity()
    # Just verify we can call it without error


def test_vector_ensure_validity_writable():
    """Test ensuring validity mask is writable."""
    var int_type = LogicalType(DuckDBType.integer)
    var vec = Vector[origin=MutAnyOrigin](int_type, 5)

    # Ensure validity is writable
    vec.ensure_validity_writable()

    # After ensuring, get_validity should always return non-NULL
    var validity = vec.get_validity()
    assert_not_equal(validity, UnsafePointer[UInt64, MutAnyOrigin]())


def test_vector_list_operations():
    """Test list vector operations."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT [1, 2, 3] as list_col")

    var chunk = result.fetch_chunk()
    var list_vec = chunk.get_vector(0)

    # Check it's a list type
    assert_equal(list_vec.get_column_type().get_type_id(), DuckDBType.list)

    # Get child vector
    var child_vec = list_vec.list_get_child()
    assert_equal(child_vec.get_column_type().get_type_id(), DuckDBType.integer)

    # Get list size
    var size = list_vec.list_get_size()
    assert_equal(size, 3)


def test_vector_list_nested():
    """Test nested list vector operations."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT [[1, 2], [3, 4, 5]] as nested_list")

    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 1)  # 1 row in chunk

    var outer_list = chunk.get_vector(0)

    # Check it's a list type
    assert_equal(outer_list.get_column_type().get_type_id(), DuckDBType.list)

    # Get the list_entry data to see how many inner lists there are
    var outer_data = outer_list.get_data().bitcast[duckdb_list_entry]()
    var outer_entry = outer_data[0]  # First (and only) row
    assert_equal(outer_entry.length, 2)  # 2 inner lists: [1,2] and [3,4,5]

    # Get outer child - contains the inner lists flattened
    var inner_list = outer_list.list_get_child()
    assert_equal(inner_list.get_column_type().get_type_id(), DuckDBType.list)
    assert_equal(inner_list.list_get_size(), 5)  # 2 + 3 = 5 total integers

    # Get innermost child - should be integers with 5 total values
    var values = inner_list.list_get_child()
    assert_equal(values.get_column_type().get_type_id(), DuckDBType.integer)

    # Validate the actual integer values
    var int_data = values.get_data().bitcast[Int32]()
    assert_equal(int_data[0], 1)
    assert_equal(int_data[1], 2)
    assert_equal(int_data[2], 3)
    assert_equal(int_data[3], 4)
    assert_equal(int_data[4], 5)


def test_vector_struct_operations():
    """Test struct vector operations."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT {'a': 1, 'b': 'hello'} as struct_col")

    var chunk = result.fetch_chunk()
    var struct_vec = chunk.get_vector(0)

    # Check it's a struct type
    assert_equal(
        struct_vec.get_column_type().get_type_id(), DuckDBType.struct_t
    )

    # Get child vectors (struct fields)
    var field0 = struct_vec.struct_get_child(0)
    var field1 = struct_vec.struct_get_child(1)

    assert_equal(field0.get_column_type().get_type_id(), DuckDBType.integer)
    assert_equal(field1.get_column_type().get_type_id(), DuckDBType.varchar)


def test_vector_array_operations():
    """Test array vector operations."""
    con = DuckDB.connect(":memory:")
    # Create an array with fixed size
    result = con.execute("SELECT [1, 2, 3]::INT[3] as array_col")

    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 1)  # 1 row

    var array_vec = chunk.get_vector(0)

    # Check it's an array type
    assert_equal(array_vec.get_column_type().get_type_id(), DuckDBType.array)

    # Get child vector and verify it contains the data
    var child_vec = array_vec.array_get_child()
    assert_equal(child_vec.get_column_type().get_type_id(), DuckDBType.integer)

    # Read the actual integer data from the child vector
    var data_ptr = child_vec.get_data().bitcast[Int32]()
    assert_equal(data_ptr[0], 1)
    assert_equal(data_ptr[1], 2)
    assert_equal(data_ptr[2], 3)


def test_vector_array_multiple_rows():
    """Test array vector with multiple rows."""
    con = DuckDB.connect(":memory:")
    # Create multiple arrays with fixed size
    result = con.execute(
        "SELECT * FROM (VALUES ([1, 2]::INT[2]), ([3, 4]::INT[2])) AS"
        " t(array_col)"
    )

    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)  # 2 rows

    var array_vec = chunk.get_vector(0)
    assert_equal(array_vec.get_column_type().get_type_id(), DuckDBType.array)

    # Get child vector - contains 2 rows * 2 array_size = 4 total integers
    var child_vec = array_vec.array_get_child()
    assert_equal(child_vec.get_column_type().get_type_id(), DuckDBType.integer)

    # Validate the actual data: [1, 2, 3, 4]
    var data_ptr = child_vec.get_data().bitcast[Int32]()
    assert_equal(data_ptr[0], 1)
    assert_equal(data_ptr[1], 2)
    assert_equal(data_ptr[2], 3)
    assert_equal(data_ptr[3], 4)


def test_vector_types_boolean():
    """Test vector with boolean type."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT TRUE, FALSE, TRUE")

    var chunk = result.fetch_chunk()
    var vec = chunk.get_vector(0)

    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.boolean)


def test_vector_types_integers():
    """Test vector with various integer types."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT 1::TINYINT as ti, 2::SMALLINT as si, 3::INTEGER as i, 4::BIGINT"
        " as bi"
    )

    var chunk = result.fetch_chunk()

    var vec_ti = chunk.get_vector(0)
    var vec_si = chunk.get_vector(1)
    var vec_i = chunk.get_vector(2)
    var vec_bi = chunk.get_vector(3)

    assert_equal(vec_ti.get_column_type().get_type_id(), DuckDBType.tinyint)
    assert_equal(vec_si.get_column_type().get_type_id(), DuckDBType.smallint)
    assert_equal(vec_i.get_column_type().get_type_id(), DuckDBType.integer)
    assert_equal(vec_bi.get_column_type().get_type_id(), DuckDBType.bigint)


def test_vector_types_unsigned_integers():
    """Test vector with unsigned integer types."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT 1::UTINYINT as uti, 2::USMALLINT as usi, 3::UINTEGER as ui,"
        " 4::UBIGINT as ubi"
    )

    var chunk = result.fetch_chunk()

    var vec_uti = chunk.get_vector(0)
    var vec_usi = chunk.get_vector(1)
    var vec_ui = chunk.get_vector(2)
    var vec_ubi = chunk.get_vector(3)

    assert_equal(vec_uti.get_column_type().get_type_id(), DuckDBType.utinyint)
    assert_equal(vec_usi.get_column_type().get_type_id(), DuckDBType.usmallint)
    assert_equal(vec_ui.get_column_type().get_type_id(), DuckDBType.uinteger)
    assert_equal(vec_ubi.get_column_type().get_type_id(), DuckDBType.ubigint)


def test_vector_types_floats():
    """Test vector with float and double types."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 1.5::FLOAT as f, 2.5::DOUBLE as d")

    var chunk = result.fetch_chunk()

    var vec_f = chunk.get_vector(0)
    var vec_d = chunk.get_vector(1)

    assert_equal(vec_f.get_column_type().get_type_id(), DuckDBType.float)
    assert_equal(vec_d.get_column_type().get_type_id(), DuckDBType.double)


def test_vector_types_temporal():
    """Test vector with temporal types."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT DATE '2024-01-01' as d, TIME '12:00:00' as t, TIMESTAMP"
        " '2024-01-01 12:00:00' as ts"
    )

    var chunk = result.fetch_chunk()

    var vec_date = chunk.get_vector(0)
    var vec_time = chunk.get_vector(1)
    var vec_ts = chunk.get_vector(2)

    assert_equal(vec_date.get_column_type().get_type_id(), DuckDBType.date)
    assert_equal(vec_time.get_column_type().get_type_id(), DuckDBType.time)
    assert_equal(vec_ts.get_column_type().get_type_id(), DuckDBType.timestamp)


def test_vector_null_values():
    """Test vector with NULL values."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT NULL::INTEGER as null_int, NULL::VARCHAR as null_str"
    )

    var chunk = result.fetch_chunk()

    var vec_int = chunk.get_vector(0)
    var vec_str = chunk.get_vector(1)

    # Vectors should still have correct types
    assert_equal(vec_int.get_column_type().get_type_id(), DuckDBType.integer)
    assert_equal(vec_str.get_column_type().get_type_id(), DuckDBType.varchar)


def test_vector_mixed_nulls():
    """Test vector with mixed NULL and non-NULL values."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT * FROM (VALUES (1), (NULL), (3)) AS t(col)")

    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)  # 3 rows

    var vec = chunk.get_vector(0)
    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.integer)

    # Validate the actual data and validity mask
    var data_ptr = vec.get_data().bitcast[Int32]()
    var validity = vec.get_validity()

    # Row 0: value = 1, valid
    assert_true(Bool((validity[0] >> 0) & 1))
    assert_equal(data_ptr[0], 1)

    # Row 1: NULL
    assert_false(Bool((validity[0] >> 1) & 1))

    # Row 2: value = 3, valid
    assert_true(Bool((validity[0] >> 2) & 1))
    assert_equal(data_ptr[2], 3)


def test_vector_empty_list():
    """Test vector with empty list."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT []::INT[] as empty_list")

    var chunk = result.fetch_chunk()
    var list_vec = chunk.get_vector(0)

    assert_equal(list_vec.get_column_type().get_type_id(), DuckDBType.list)

    var size = list_vec.list_get_size()
    assert_equal(size, 0)


def test_vector_map_type():
    """Test vector with map type."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP([1, 2], ['a', 'b']) as map_col")

    var chunk = result.fetch_chunk()
    var map_vec = chunk.get_vector(0)

    # Check it's a map type
    assert_equal(map_vec.get_column_type().get_type_id(), DuckDBType.map)


def test_vector_chunk_size():
    """Test that chunk size is correct for vectors."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT * FROM range(100)")

    var chunk = result.fetch_chunk()
    # Chunk should have all 100 rows
    assert_equal(len(chunk), 100)

    var vec = chunk.get_vector(0)
    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.bigint)


def test_vector_multiple_chunks():
    """Test vectors across multiple chunks."""
    con = DuckDB.connect(":memory:")
    # Generate more rows than fit in one chunk (typically 2048)
    result = con.execute("SELECT * FROM range(5000)")

    var chunk1 = result.fetch_chunk()
    assert_true(len(chunk1) > 0)
    var vec1 = chunk1.get_vector(0)
    assert_equal(vec1.get_column_type().get_type_id(), DuckDBType.bigint)

    # Try to fetch second chunk - should succeed since we have 5000 rows
    var chunk2 = result.fetch_chunk()
    assert_true(len(chunk2) > 0)
    var vec2 = chunk2.get_vector(0)
    assert_equal(vec2.get_column_type().get_type_id(), DuckDBType.bigint)

    # Fetch remaining chunks until we get an error
    var chunk_count = 2
    var total_rows = len(chunk1) + len(chunk2)
    while True:
        try:
            var chunk = result.fetch_chunk()
            assert_true(len(chunk) > 0)
            total_rows += len(chunk)
            chunk_count += 1
        except:
            # No more chunks - this is expected
            break

    # Should have fetched multiple chunks from 5000 rows
    assert_true(chunk_count >= 2)
    assert_equal(total_rows, 5000)


def test_vector_varchar_strings():
    """Test vector with various string values."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT * FROM (VALUES ('short'), ('a much longer string value'), (''))"
        " AS t(str)"
    )

    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)  # 3 rows

    var vec = chunk.get_vector(0)
    assert_equal(vec.get_column_type().get_type_id(), DuckDBType.varchar)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
