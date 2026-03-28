from duckdb import *
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


def test_range() raises:
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))")
    chunk = result.fetch_chunk()
    for i in range(10):
        assert_equal(chunk.get[Int64](col=0, row=i), Int64(i))

    var obtained = chunk.get[Int64](col=0)
    for i in range(10):
        assert_equal(obtained[i], Int64(i))


def test_materialized_result() raises:
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))").fetchall()
    for i in range(10):
        assert_equal(result.get[Int64](col=0, row=i), Int64(i))

    var obtained = result.get[Int64](col=0)
    for i in range(10):
        assert_equal(obtained[i], Int64(i))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
