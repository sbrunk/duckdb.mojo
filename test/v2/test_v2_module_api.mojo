"""Tests for the default-connection functions and the SQL fragment helpers on V2."""

from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite
from duckdb.v2 import *
from duckdb.v2 import default


def test_default_connection() raises:
    _ = default.execute("CREATE OR REPLACE TABLE people (name VARCHAR, age INTEGER)")
    _ = default.execute("INSERT INTO people VALUES (?, ?), (?, ?)", String("O'Brien"), Int32(40), String("Ada"), Int32(36))
    assert_equal(default.execute("SELECT count(*) FROM people").get[Int64]()[0], 2)
    # The same connection every time: the table is still there.
    assert_equal(default.table("people").order("age").select("name").get[String]()[0], "Ada")
    assert_equal(default.sql("SELECT 42").get[Int32]()[0], 42)


def test_connect() raises:
    var con = default.connect()
    _ = con.execute("CREATE TABLE t AS SELECT 1 AS x")
    # A separate database from the default one.
    var failed = False
    try:
        _ = default.execute("SELECT * FROM t")
    except e:
        failed = True
    assert_true(failed)


def test_fragment_helpers() raises:
    _ = default.execute("CREATE OR REPLACE TABLE people (name VARCHAR, age INTEGER)")
    _ = default.execute("INSERT INTO people VALUES ('O''Brien', 40), ('Ada', 36)")
    var r = default.table("people").filter(col("name") + " = " + lit("O'Brien")).select(fn_("upper", col("name")))
    assert_equal(r.get[String]()[0], "O'BRIEN")
    var s = default.table("people").select(star(exclude=["age"])).order("name")
    assert_equal(s.get[String]()[1], "O'Brien")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
