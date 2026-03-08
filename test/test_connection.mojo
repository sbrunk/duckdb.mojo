from duckdb import *
from std.testing import *
from std.testing.suite import TestSuite


def test_connection() raises:
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42")
    assert_equal(result.fetch_chunk().get[Int32](col=0, row=0), 42)


def test_failure() raises:
    con = DuckDB.connect(":memory:")
    with assert_raises(contains="Parser Error"):
        _ = con.execute("invalid statement")
    try:
        _ = con.execute("invalid statement")
    except e:
        assert_equal(e.type, ErrorType.PARSER)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
