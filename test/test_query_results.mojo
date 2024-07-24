from duckdb import *
from testing import assert_equal, assert_true

def test_range():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))")
    chunk = result.fetch_chunk()
    for i in range(10):
        assert_equal(chunk.get(int64, col=0, row=i).value(), i)

    var obtained = chunk.get(int64, col=0)
    for i in range(10):
        assert_equal(obtained[i].value(), i)
