from duckdb import DuckDB
from testing import assert_equal

def test_range():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT range(10)")
    chunk = result.fetch_chunk()
    for i in range(0):
        assert_equal(chunk.get_int64(col=0, row=i), i)