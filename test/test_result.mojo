from duckdb import *
from testing import *
from testing.suite import TestSuite


def test_result_statement_type_select():
    """Test result_statement_type returns 1 for SELECT statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1")
    # DUCKDB_STATEMENT_TYPE_SELECT = 1 (not 0)
    assert_equal(result.result_statement_type(), 1)


def test_result_statement_type_insert():
    """Test result_statement_type returns 2 for INSERT statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1)")
    # DUCKDB_STATEMENT_TYPE_INSERT = 2
    assert_equal(result.result_statement_type(), 2)


def test_result_statement_type_update():
    """Test result_statement_type returns 3 for UPDATE statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1)")
    var result = conn.execute("UPDATE test SET id = 2")
    # DUCKDB_STATEMENT_TYPE_UPDATE = 3
    assert_equal(result.result_statement_type(), 3)


def test_result_statement_type_delete():
    """Test result_statement_type returns 5 for DELETE statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1)")
    var result = conn.execute("DELETE FROM test WHERE id = 1")
    # DUCKDB_STATEMENT_TYPE_DELETE = 5 (not 3, because 4 is not used)
    assert_equal(result.result_statement_type(), 5)


def test_result_statement_type_create():
    """Test result_statement_type for CREATE statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("CREATE TABLE test (id INT)")
    # DUCKDB_STATEMENT_TYPE_CREATE = 7
    assert_equal(result.result_statement_type(), 7)


def test_result_statement_type_drop():
    """Test result_statement_type for DROP statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("DROP TABLE test")
    # DUCKDB_STATEMENT_TYPE_DROP = 15
    assert_equal(result.result_statement_type(), 15)


def test_result_statement_type_alter():
    """Test result_statement_type for ALTER statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("ALTER TABLE test ADD COLUMN name VARCHAR")
    # DUCKDB_STATEMENT_TYPE_ALTER = 9
    assert_equal(result.result_statement_type(), 9)


def test_rows_changed_insert_single():
    """Test rows_changed returns 1 for single row INSERT."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1)")
    assert_equal(result.rows_changed(), 1)


def test_rows_changed_insert_multiple():
    """Test rows_changed returns correct count for multiple row INSERT."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    assert_equal(result.rows_changed(), 5)


def test_rows_changed_update_all():
    """Test rows_changed returns correct count for UPDATE affecting all rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("UPDATE test SET id = id + 10")
    assert_equal(result.rows_changed(), 3)


def test_rows_changed_update_partial():
    """Test rows_changed returns correct count for UPDATE affecting some rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    var result = conn.execute("UPDATE test SET id = 99 WHERE id <= 2")
    assert_equal(result.rows_changed(), 2)


def test_rows_changed_update_none():
    """Test rows_changed returns 0 for UPDATE matching no rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("UPDATE test SET id = 99 WHERE id > 100")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_delete_all():
    """Test rows_changed returns correct count for DELETE removing all rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4)")
    var result = conn.execute("DELETE FROM test")
    assert_equal(result.rows_changed(), 4)


def test_rows_changed_delete_partial():
    """Test rows_changed returns correct count for DELETE removing some rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    var result = conn.execute("DELETE FROM test WHERE id > 3")
    assert_equal(result.rows_changed(), 2)


def test_rows_changed_delete_none():
    """Test rows_changed returns 0 for DELETE matching no rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("DELETE FROM test WHERE id > 100")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_select():
    """Test rows_changed returns 0 for SELECT statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2, 3")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_create():
    """Test rows_changed returns 0 for CREATE statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("CREATE TABLE test (id INT)")
    assert_equal(result.rows_changed(), 0)

def test_result_materialized_length():
    """Test len() on materialized result returns correct number of rows."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(5))").fetch_all()
    assert_equal(len(result), 5)


def test_result_materialized_length_empty():
    """Test len() on materialized result returns 0 for empty result."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("SELECT * FROM test").fetch_all()
    assert_equal(len(result), 0)


def test_result_column_count():
    """Test column_count returns correct number of columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2, 3, 4, 5")
    assert_equal(result.column_count(), 5)


def test_result_column_count_single():
    """Test column_count returns 1 for single column."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42")
    assert_equal(result.column_count(), 1)


def test_result_column_name():
    """Test column_name returns correct column name."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42 AS answer")
    assert_equal(result.column_name(0), "answer")


def test_result_column_name_multiple():
    """Test column_name for multiple columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
    assert_equal(result.column_name(0), "a")
    assert_equal(result.column_name(1), "b")
    assert_equal(result.column_name(2), "c")


def test_result_column_name_unnamed():
    """Test column_name for unnamed columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2")
    # DuckDB generates default names for unnamed columns
    var name0 = result.column_name(0)
    var name1 = result.column_name(1)
    assert_true(len(name0) > 0)
    assert_true(len(name1) > 0)


def test_result_fetch_chunk():
    """Test fetch_chunk returns data correctly."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 42)


def test_result_fetch_all():
    """Test fetch_all materializes all results."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(10))").fetch_all()
    for i in range(10):
        assert_equal(result.get(bigint, col=0, row=i).value(), i)


def test_result_iteration():
    """Test iterating over result chunks."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(5))")
    var count = 0
    var iter = result.chunk_iterator()
    while iter.__has_next__():
        var chunk = iter.__next__()
        for i in range(len(chunk)):
            assert_equal(chunk.get(bigint, col=0, row=i).value(), count)
            count += 1
    assert_equal(count, 5)


def test_result_multiple_chunks():
    """Test result with multiple chunks (large dataset)."""
    var conn = DuckDB.connect(":memory:")
    # Create a result that will span multiple chunks (DuckDB uses 2048 rows per chunk by default)
    var result = conn.execute("SELECT unnest(range(5000))")
    var total_rows = 0
    var iter = result.chunk_iterator()
    while iter.__has_next__():
        var chunk = iter.__next__()
        total_rows += len(chunk)
    assert_equal(total_rows, 5000)


def test_result_column_types():
    """Test column_types returns correct types."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1::INTEGER, 2.0::DOUBLE, 'test'::VARCHAR")
    var types = result.column_types()
    assert_equal(len(types), 3)


def test_result_column_type():
    """Test column_type returns correct type for specific column."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42::INTEGER")
    var col_type = result.column_type(0)
    assert_equal(col_type.get_type_id().value, duckdb_type.DUCKDB_TYPE_INTEGER)


def test_result_with_null_values():
    """Test result handling NULL values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT * FROM (VALUES (1), (NULL), (3)) AS t(v)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 1)
    assert_false(chunk.get(integer, col=0, row=1))
    assert_equal(chunk.get(integer, col=0, row=2).value(), 3)


def test_result_mixed_types():
    """Test result with mixed column types."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42::INTEGER, 3.14::DOUBLE, 'hello'::VARCHAR, TRUE::BOOLEAN")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), 42)
    assert_equal(chunk.get(double, col=1, row=0).value(), 3.14)
    assert_equal(chunk.get(varchar, col=2, row=0).value(), "hello")
    assert_equal(chunk.get(boolean, col=3, row=0).value(), True)


def test_result_empty_table():
    """Test result from empty table."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
    var result = conn.execute("SELECT * FROM test").fetch_all()
    assert_equal(len(result), 0)
    assert_equal(result.column_count(), 2)


def test_result_large_integers():
    """Test result with large integer values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 9223372036854775807::BIGINT")  # Max BIGINT
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 9223372036854775807)


def test_result_negative_integers():
    """Test result with negative integer values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT -42::INTEGER, -123456789::BIGINT")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), -42)
    assert_equal(chunk.get(bigint, col=1, row=0).value(), -123456789)


def test_result_floating_point_precision():
    """Test result with floating point values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 3.14159265359::DOUBLE")
    var chunk = result.fetch_chunk()
    var value = chunk.get(double, col=0, row=0).value()
    assert_almost_equal(value, 3.14159265359)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
