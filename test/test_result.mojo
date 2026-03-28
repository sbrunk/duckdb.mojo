from duckdb import *
from std.testing import *
from std.testing.suite import TestSuite


def test_result_statement_type_select() raises:
    """Test statement_type returns SELECT for SELECT statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1")
    assert_equal(result.statement_type(), StatementType.SELECT)


def test_result_statement_type_insert() raises:
    """Test statement_type returns INSERT for INSERT statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1)")
    assert_equal(result.statement_type(), StatementType.INSERT)


def test_result_statement_type_update() raises:
    """Test statement_type returns UPDATE for UPDATE statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1)")
    var result = conn.execute("UPDATE test SET id = 2")
    assert_equal(result.statement_type(), StatementType.UPDATE)


def test_result_statement_type_delete() raises:
    """Test statement_type returns DELETE for DELETE statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1)")
    var result = conn.execute("DELETE FROM test")
    assert_equal(result.statement_type(), StatementType.DELETE)


def test_result_statement_type_create() raises:
    """Test statement_type returns CREATE for CREATE statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("CREATE TABLE test (id INT)")
    assert_equal(result.statement_type(), StatementType.CREATE)


def test_result_statement_type_drop() raises:
    """Test statement_type returns DROP for DROP statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("DROP TABLE test")
    assert_equal(result.statement_type(), StatementType.DROP)


def test_result_statement_type_alter() raises:
    """Test statement_type returns ALTER for ALTER statements."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("ALTER TABLE test ADD COLUMN name VARCHAR")
    assert_equal(result.statement_type(), StatementType.ALTER)


def test_rows_changed_insert_single() raises:
    """Test rows_changed returns 1 for single row INSERT."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1)")
    assert_equal(result.rows_changed(), 1)


def test_rows_changed_insert_multiple() raises:
    """Test rows_changed returns correct count for multiple row INSERT."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    assert_equal(result.rows_changed(), 5)


def test_rows_changed_update_all() raises:
    """Test rows_changed returns correct count for UPDATE affecting all rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("UPDATE test SET id = id + 10")
    assert_equal(result.rows_changed(), 3)


def test_rows_changed_update_partial() raises:
    """Test rows_changed returns correct count for UPDATE affecting some rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    var result = conn.execute("UPDATE test SET id = 99 WHERE id <= 2")
    assert_equal(result.rows_changed(), 2)


def test_rows_changed_update_none() raises:
    """Test rows_changed returns 0 for UPDATE matching no rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("UPDATE test SET id = 99 WHERE id > 100")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_delete_all() raises:
    """Test rows_changed returns correct count for DELETE removing all rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4)")
    var result = conn.execute("DELETE FROM test")
    assert_equal(result.rows_changed(), 4)


def test_rows_changed_delete_partial() raises:
    """Test rows_changed returns correct count for DELETE removing some rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)")
    var result = conn.execute("DELETE FROM test WHERE id > 3")
    assert_equal(result.rows_changed(), 2)


def test_rows_changed_delete_none() raises:
    """Test rows_changed returns 0 for DELETE matching no rows."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    _ = conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    var result = conn.execute("DELETE FROM test WHERE id > 100")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_select() raises:
    """Test rows_changed returns 0 for SELECT statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2, 3")
    assert_equal(result.rows_changed(), 0)


def test_rows_changed_create() raises:
    """Test rows_changed returns 0 for CREATE statements."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("CREATE TABLE test (id INT)")
    assert_equal(result.rows_changed(), 0)

def test_result_materialized_length() raises:
    """Test len() on materialized result returns correct number of rows."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(5))").fetchall()
    assert_equal(len(result), 5)


def test_result_materialized_length_empty() raises:
    """Test len() on materialized result returns 0 for empty result."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT)")
    var result = conn.execute("SELECT * FROM test").fetchall()
    assert_equal(len(result), 0)


def test_result_column_count() raises:
    """Test column_count returns correct number of columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2, 3, 4, 5")
    assert_equal(result.column_count(), 5)


def test_result_column_count_single() raises:
    """Test column_count returns 1 for single column."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42")
    assert_equal(result.column_count(), 1)


def test_result_column_name() raises:
    """Test column_name returns correct column name."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42 AS answer")
    assert_equal(result.column_name(0), "answer")


def test_result_column_name_multiple() raises:
    """Test column_name for multiple columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
    assert_equal(result.column_name(0), "a")
    assert_equal(result.column_name(1), "b")
    assert_equal(result.column_name(2), "c")


def test_result_column_name_unnamed() raises:
    """Test column_name for unnamed columns."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1, 2")
    # DuckDB generates default names for unnamed columns
    var name0 = result.column_name(0)
    var name1 = result.column_name(1)
    assert_true(len(name0) > 0)
    assert_true(len(name1) > 0)


def test_result_fetch_chunk() raises:
    """Test fetch_chunk returns data correctly."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_result_fetchall() raises:
    """Test fetchall materializes all results."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(10))").fetchall()
    for i in range(10):
        assert_equal(result.get[Int64](col=0, row=i), Int64(i))


def test_result_iteration() raises:
    """Test iterating over result chunks."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT unnest(range(5))")
    var count = 0
    for chunk in result.chunks():
        for i in range(len(chunk)):
            assert_equal(chunk.get[Int64](col=0, row=i), Int64(count))
            count += 1
    assert_equal(count, 5)


def test_result_multiple_chunks() raises:
    """Test result with multiple chunks (large dataset)."""
    var conn = DuckDB.connect(":memory:")
    # Create a result that will span multiple chunks (DuckDB uses 2048 rows per chunk by default)
    var result = conn.execute("SELECT unnest(range(5000))")
    var total_rows = 0
    for chunk in result.chunks():
        total_rows += len(chunk)
    assert_equal(total_rows, 5000)


def test_result_column_types() raises:
    """Test column_types returns correct types."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1::INTEGER, 2.0::DOUBLE, 'test'::VARCHAR")
    var types = result.column_types()
    assert_equal(len(types), 3)


def test_result_column_type() raises:
    """Test column_type returns correct type for specific column."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42::INTEGER")
    var col_type = result.column_type(0)
    assert_equal(col_type.get_type_id(), DuckDBType.integer)


def test_result_with_null_values() raises:
    """Test result handling NULL values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT * FROM (VALUES (1), (NULL), (3)) AS t(v)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 1)
    assert_false(chunk.get[Optional[Int32]](col=0, row=1))
    assert_equal(chunk.get[Int32](col=0, row=2), 3)


def test_result_mixed_types() raises:
    """Test result with mixed column types."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 42::INTEGER, 3.14::DOUBLE, 'hello'::VARCHAR, TRUE::BOOLEAN")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)
    assert_equal(chunk.get[Float64](col=1, row=0), 3.14)
    assert_equal(chunk.get[String](col=2, row=0), "hello")
    assert_equal(chunk.get[Bool](col=3, row=0), True)


def test_result_empty_table() raises:
    """Test result from empty table."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
    var result = conn.execute("SELECT * FROM test").fetchall()
    assert_equal(len(result), 0)
    assert_equal(result.column_count(), 2)


def test_result_large_integers() raises:
    """Test result with large integer values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 9223372036854775807::BIGINT")  # Max BIGINT
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), 9223372036854775807)


def test_result_negative_integers() raises:
    """Test result with negative integer values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT -42::INTEGER, -123456789::BIGINT")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), -42)
    assert_equal(chunk.get[Int64](col=1, row=0), -123456789)


def test_result_floating_point_precision() raises:
    """Test result with floating point values."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 3.14159265359::DOUBLE")
    var chunk = result.fetch_chunk()
    var value = chunk.get[Float64](col=0, row=0)
    assert_almost_equal(value, 3.14159265359)


# ──────────────────────────────────────────────────────────────────
# For-loop iteration tests
# ──────────────────────────────────────────────────────────────────


def test_for_chunks_iteration() raises:
    """Test iterating over chunks with a for loop."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT unnest(range(10)) AS id"
    var count = 0
    for chunk in conn.execute(query).chunks():
        for i in range(len(chunk)):
            assert_equal(chunk.get[Int64](col=0, row=i), Int64(count))
            count += 1
    assert_equal(count, 10)


def test_for_rows_iteration() raises:
    """Test iterating over rows directly from a result (via __iter__)."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT unnest(range(10)) AS id"
    var count = 0
    for row in conn.execute(query):
        assert_equal(row.get[Int64](col=0), Int64(count))
        count += 1
    assert_equal(count, 10)


def test_for_rows_in_chunk() raises:
    """Test iterating over rows within a chunk using for-in."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT * FROM (VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'e')) AS t(id, name)"
    var result = conn.execute(query)
    var chunk = result.fetch_chunk()
    var idx = 0
    for row in chunk:
        assert_equal(row.get[Int32](col=0), Int32(idx))
        idx += 1
    assert_equal(idx, 5)


def test_for_rows_multi_column() raises:
    """Test row iteration with multiple typed columns."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 42::INTEGER AS i, 3.14::DOUBLE AS d, 'hello'::VARCHAR AS s, TRUE::BOOLEAN AS b"
    for row in conn.execute(query):
        assert_equal(row.get[Int32](col=0), 42)
        assert_equal(row.get[Float64](col=1), 3.14)
        assert_equal(row.get[String](col=2), "hello")
        assert_equal(row.get[Bool](col=3), True)


def test_for_rows_with_nulls() raises:
    """Test row iteration with NULL values using Optional."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT * FROM (VALUES (1, 'a'), (NULL, 'b'), (3, NULL)) AS t(id, name)"
    var idx = 0
    for row in conn.execute(query).rows():
        if idx == 0:
            assert_equal(row.get[Optional[Int32]](col=0).value(), 1)
            assert_equal(row.get[String](col=1), "a")
        elif idx == 1:
            assert_false(row.get[Optional[Int32]](col=0))
            assert_equal(row.get[String](col=1), "b")
        else:
            assert_equal(row.get[Optional[Int32]](col=0).value(), 3)
            assert_false(row.get[Optional[String]](col=1))
        idx += 1
    assert_equal(idx, 3)


def test_for_rows_multi_chunk() raises:
    """Test row iteration spanning multiple chunks (>2048 rows)."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT unnest(range(5000)) AS id"
    var count = 0
    for row in conn.execute(query):
        assert_equal(row.get[Int64](col=0), Int64(count))
        count += 1
    assert_equal(count, 5000)


def test_for_chunks_multi_chunk() raises:
    """Test chunk iteration over large dataset, verify chunk sizes."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT unnest(range(5000)) AS id"
    var chunk_count = 0
    var total_rows = 0
    for chunk in conn.execute(query).chunks():
        chunk_count += 1
        total_rows += len(chunk)
        assert_true(len(chunk) > 0, "chunk should not be empty")
    assert_equal(total_rows, 5000)
    assert_true(chunk_count > 1, "expected multiple chunks for 5000 rows")


def test_for_rows_empty_result() raises:
    """Test row iteration over empty result produces no iterations."""
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE empty_tbl (id INT)")
    var query = "SELECT * FROM empty_tbl"
    var count = 0
    for _ in conn.execute(query):
        count += 1
    assert_equal(count, 0)


def test_rows_explicit_spelling() raises:
    """Test .rows() as explicit spelling of __iter__."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT unnest(range(5)) AS id"
    var count = 0
    for row in conn.execute(query).rows():
        assert_equal(row.get[Int64](col=0), Int64(count))
        count += 1
    assert_equal(count, 5)


def test_row_column_count() raises:
    """Test Row.column_count() inside iteration."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 1::INT, 2::INT, 3::INT"
    for row in conn.execute(query).rows():
        assert_equal(row.column_count(), 3)


def test_row_is_null_in_loop() raises:
    """Test Row.is_null() inside a for loop."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT * FROM (VALUES (1, NULL), (NULL, 'b')) AS t(id, name)"
    var idx = 0
    for row in conn.execute(query).rows():
        if idx == 0:
            assert_false(row.is_null(col=0))
            assert_true(row.is_null(col=1))
        else:
            assert_true(row.is_null(col=0))
            assert_false(row.is_null(col=1))
        idx += 1


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
