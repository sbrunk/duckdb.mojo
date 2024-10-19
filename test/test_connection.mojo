from duckdb import *
from testing import *


def test_connection():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42")
    assert_equal(result.fetch_chunk().get(integer, col=0, row=0).value(), 42)


def test_failure():
    con = DuckDB.connect(":memory:")
    with assert_raises():
        _ = con.execute("invalid statement")
