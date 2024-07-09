from duckdb import DuckDB
from testing import assert_equal, assert_false, assert_raises


def test_types():
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT true")
    assert_equal(result.fetch_chunk().get[DType.bool](0, 0).value(), True)

    with assert_raises(contains="Column 0 has type boolean. Expected tinyint."):
        result = con.execute("SELECT true")
        _ = result.fetch_chunk().get[DType.int8](0, 0).value()

    with assert_raises(contains="Column 0 has type boolean. Expected varchar."):
        result = con.execute("SELECT true")
        _ = result.fetch_chunk().get[String](0, 0).value()

    result = con.execute("SELECT -42::TINYINT")
    assert_equal(result.fetch_chunk().get[DType.int8](0, 0).value(), -42)

    result = con.execute("SELECT 42::UTINYINT")
    assert_equal(result.fetch_chunk().get[DType.uint8](0, 0).value(), 42)

    result = con.execute("SELECT -42::SMALLINT")
    assert_equal(result.fetch_chunk().get[DType.int16](0, 0).value(), -42)

    result = con.execute("SELECT 42::USMALLINT")
    assert_equal(result.fetch_chunk().get[DType.uint16](0, 0).value(), 42)

    result = con.execute("SELECT -42::INTEGER")
    assert_equal(result.fetch_chunk().get[DType.int32](0, 0).value(), -42)

    result = con.execute("SELECT 42::UINTEGER")
    assert_equal(result.fetch_chunk().get[DType.uint32](0, 0).value(), 42)

    result = con.execute("SELECT -42::BIGINT")
    assert_equal(result.fetch_chunk().get[DType.int64](0, 0).value(), -42)

    result = con.execute("SELECT 42::UBIGINT")
    assert_equal(result.fetch_chunk().get[DType.uint64](0, 0).value(), 42)

    result = con.execute("SELECT 42.0::FLOAT")
    assert_equal(result.fetch_chunk().get[DType.float32](0, 0).value(), 42.0)

    result = con.execute("SELECT 42.0::DOUBLE")
    assert_equal(result.fetch_chunk().get[DType.float64](0, 0).value(), 42.0)

    result = con.execute("SELECT 'hello'")
    assert_equal(result.fetch_chunk().get_string(0, 0).value(), "hello")

    result = con.execute("SELECT 'hello longer string'")
    assert_equal(
        result.fetch_chunk().get_string(0, 0).value(), "hello longer string"
    )

    ## TODO test remaining types

def test_null():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT null")
    assert_false(result.fetch_chunk().get[DType.int32](0, 0))
