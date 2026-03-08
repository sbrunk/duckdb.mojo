"""Tests for tuple deserialization from DuckDB results."""

from duckdb import *
from std.collections import Optional
from std.testing import assert_equal, assert_false, assert_raises, assert_true
from std.testing.suite import TestSuite


def test_tuple_basic() raises:
    """Deserialize a row into a Tuple of basic types."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 42::INTEGER, 3.14::DOUBLE, 'hello'::VARCHAR"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Int32, Float64, String](row=0)
    assert_equal(t[0], 42)
    assert_equal(t[1], 3.14)
    assert_equal(t[2], "hello")


def test_tuple_all_rows() raises:
    """Deserialize all rows into a List of Tuples."""
    var conn = DuckDB.connect(":memory:")
    var query = (
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    var chunk = conn.execute(query).fetch_chunk()
    var rows = chunk.get_tuple[Int32, String]()
    assert_equal(len(rows), 3)
    assert_equal(rows[0][0], 1)
    assert_equal(rows[0][1], "a")
    assert_equal(rows[1][0], 2)
    assert_equal(rows[1][1], "b")
    assert_equal(rows[2][0], 3)
    assert_equal(rows[2][1], "c")


def test_tuple_nullable() raises:
    """Deserialize a Tuple with Optional elements for NULL handling."""
    var conn = DuckDB.connect(":memory:")
    var query = (
        "SELECT * FROM (VALUES (1, 'a'), (NULL, 'b'), (3, NULL))"
        " AS t(id, name)"
    )
    var chunk = conn.execute(query).fetch_chunk()

    var t0 = chunk.get_tuple[Optional[Int32], Optional[String]](row=0)
    assert_equal(t0[0].value(), 1)
    assert_equal(t0[1].value(), "a")

    var t1 = chunk.get_tuple[Optional[Int32], Optional[String]](row=1)
    assert_false(t1[0])
    assert_equal(t1[1].value(), "b")

    var t2 = chunk.get_tuple[Optional[Int32], Optional[String]](row=2)
    assert_equal(t2[0].value(), 3)
    assert_false(t2[1])


def test_tuple_from_row() raises:
    """Deserialize a Tuple via Row.get_tuple inside a for loop."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT * FROM (VALUES (0, 'x'), (1, 'y'), (2, 'z')) AS t(id, label)"
    var expected_labels = ["x", "y", "z"]
    var idx = 0
    for row in conn.execute(query):
        var t = row.get_tuple[Int32, String]()
        assert_equal(t[0], Int32(idx))
        assert_equal(t[1], expected_labels[idx])
        idx += 1
    assert_equal(idx, 3)


def test_tuple_bigint_types() raises:
    """Deserialize a Tuple with various integer widths."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 1::TINYINT, 2::SMALLINT, 3::INTEGER, 4::BIGINT"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Int8, Int16, Int32, Int64](row=0)
    assert_equal(t[0], Int8(1))
    assert_equal(t[1], Int16(2))
    assert_equal(t[2], Int32(3))
    assert_equal(t[3], Int64(4))


def test_tuple_boolean() raises:
    """Deserialize a Tuple containing Bool values."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT TRUE::BOOLEAN, FALSE::BOOLEAN"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Bool, Bool](row=0)
    assert_true(t[0])
    assert_false(t[1])


def test_tuple_column_count_mismatch() raises:
    """Error when Tuple element count doesn't match column count."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 1::INT, 2::INT, 3::INT"
    var chunk = conn.execute(query).fetch_chunk()

    # 2 elements for 3 columns
    with assert_raises():
        _ = chunk.get_tuple[Int32, Int32](row=0)


def test_tuple_null_non_optional_raises() raises:
    """Error when NULL encountered for non-Optional Tuple element."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT NULL::INTEGER"
    var chunk = conn.execute(query).fetch_chunk()

    with assert_raises():
        _ = chunk.get_tuple[Int32](row=0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
