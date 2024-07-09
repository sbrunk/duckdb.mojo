from duckdb import DuckDB
from testing import assert_equal

def test_connection():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42")
    assert_equal(result.fetch_chunk().get[DType.int32](0, 0).value(), 42)
