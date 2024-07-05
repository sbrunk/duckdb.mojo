from duckdb import DuckDB
from testing import assert_equal, assert_true

def test_range():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))")
    chunk = result.fetch_chunk()
    for i in range(10):
        assert_equal(chunk.get_int64(col=0, row=i), i)

    var obtained = chunk.get_int64(col=0)
    var expected = List[Int64](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    for i in range(10):
        assert_equal(obtained[i], expected[i])