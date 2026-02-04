from duckdb import *
from testing import assert_equal, assert_true
from testing.suite import TestSuite


def test_range():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))")
    chunk = result.fetch_chunk()
    for i in range(10):
        assert_equal(chunk.get(bigint, col=0, row=i).value(), i)

    var obtained = chunk.get(bigint, col=0)
    for i in range(10):
        assert_equal(obtained[i].value(), i)


def test_materialized_result():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT unnest(range(10))").fetch_all()
    for i in range(10):
        assert_equal(result.get(bigint, col=0, row=i).value(), i)

    var obtained = result.get(bigint, col=0)
    for i in range(10):
        assert_equal(obtained[i].value(), i)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
