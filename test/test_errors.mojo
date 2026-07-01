from duckdb import *
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


def test_programming_error_is_classified() raises:
    var con = DuckDB.connect(":memory:")
    var caught = False
    try:
        _ = con.execute("SELECT * FROM no_such_table")
    except e:
        caught = True
        assert_equal(e.type, ErrorType.CATALOG)
        assert_true(e.type.is_programming_error())
        assert_true(not e.type.is_data_error())
    assert_true(caught)


def test_syntax_error_is_programming() raises:
    var con = DuckDB.connect(":memory:")
    var caught = False
    try:
        _ = con.execute("SELEKT 1")
    except e:
        caught = True
        assert_true(e.type.is_programming_error())
    assert_true(caught)


def test_data_error_is_classified() raises:
    var con = DuckDB.connect(":memory:")
    var caught = False
    try:
        _ = con.execute("SELECT CAST('abc' AS INTEGER)")
    except e:
        caught = True
        assert_true(e.type.is_data_error())
        assert_true(not e.type.is_programming_error())
    assert_true(caught)


def test_integrity_error_is_classified() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (i INTEGER PRIMARY KEY)")
    _ = con.execute("INSERT INTO t VALUES (1)")
    var caught = False
    try:
        _ = con.execute("INSERT INTO t VALUES (1)")
    except e:
        caught = True
        assert_true(e.type.is_integrity_error())
    assert_true(caught)


def test_error_bucket_membership() raises:
    # Runtime/internal codes land in is_operational_error (not "no bucket").
    assert_true(ErrorType.EXECUTOR.is_operational_error())
    assert_true(ErrorType.SCHEDULER.is_operational_error())
    assert_true(ErrorType.STAT.is_operational_error())
    assert_true(ErrorType.NULL_POINTER.is_operational_error())
    assert_true(ErrorType.INVALID_CONFIGURATION.is_programming_error())


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
