from duckdb import DuckDB
from testing import assert_equal

def test_types():
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT true")
    assert_equal(result.fetch_chunk().get_bool(0, 0), True)

    result = con.execute("SELECT -42::TINYINT")
    assert_equal(result.fetch_chunk().get_int8(0, 0), -42)

    result = con.execute("SELECT 42::UTINYINT")
    assert_equal(result.fetch_chunk().get_uint8(0, 0), 42)

    result = con.execute("SELECT -42::SMALLINT")
    assert_equal(result.fetch_chunk().get_int16(0, 0), -42)

    result = con.execute("SELECT 42::USMALLINT")
    assert_equal(result.fetch_chunk().get_uint16(0, 0), 42)

    result = con.execute("SELECT -42::INTEGER")
    assert_equal(result.fetch_chunk().get_int32(0, 0), -42)

    result = con.execute("SELECT 42::UINTEGER")
    assert_equal(result.fetch_chunk().get_uint32(0, 0), 42)

    result = con.execute("SELECT -42::BIGINT")
    assert_equal(result.fetch_chunk().get_int64(0, 0), -42)

    result = con.execute("SELECT 42::UBIGINT")
    assert_equal(result.fetch_chunk().get_uint64(0, 0), 42)

    result = con.execute("SELECT 42.0::FLOAT")
    assert_equal(result.fetch_chunk().get_float32(0, 0), 42.0)

    result = con.execute("SELECT 42.0::DOUBLE")
    assert_equal(result.fetch_chunk().get_float64(0, 0), 42.0)

    result = con.execute("SELECT 'hello'")    
    assert_equal(result.fetch_chunk().get_string(0, 0), "hello")
    