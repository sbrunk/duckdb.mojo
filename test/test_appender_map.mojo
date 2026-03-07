"""Tests for the Appender API with Dict/MAP types."""

from duckdb import *
from std.collections import Dict
from std.testing import assert_equal
from std.testing.suite import TestSuite


def test_append_dict_map():
    """Append a Dict to a MAP column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    var appender = Appender(con, "t")
    var d: Dict[String, Int32] = {'a': 1, 'b': 2}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, Int32]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["a"], 1)
    assert_equal(m["b"], 2)


def test_append_dict_map_multiple_rows():
    """Append multiple Dict rows to a MAP column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    var appender = Appender(con, "t")

    var d1: Dict[String, Int32] = {'x': 10}
    appender.append_value(d1)
    appender.end_row()

    var d2: Dict[String, Int32] = {'y': 20, 'z': 30}
    appender.append_value(d2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var dicts = chunk.get[Dict[String, Int32]](col=0)
    assert_equal(len(dicts), 2)

    var m0 = dicts[0].copy()
    assert_equal(len(m0), 1)
    assert_equal(m0["x"], 10)

    var m1 = dicts[1].copy()
    assert_equal(len(m1), 2)
    assert_equal(m1["y"], 20)
    assert_equal(m1["z"], 30)


def test_append_dict_map_with_id():
    """Append Dict to table with regular column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (id INTEGER, m MAP(VARCHAR, INTEGER))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    var d: Dict[String, Int32] = {'key': 42}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id, m FROM t")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    var m = chunk.get[Dict[String, Int32]](col=1, row=0)
    assert_equal(m["key"], 42)


def test_append_dict_map_roundtrip():
    """Append Dict values and read back as Dict — full roundtrip."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (m MAP(INTEGER, VARCHAR))"
    )
    var appender = Appender(con, "t")
    var d = Dict[Int32, String]()
    d[Int32(1)] = "one"
    d[Int32(2)] = "two"
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[Int32, String]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m[Int32(1)], "one")
    assert_equal(m[Int32(2)], "two")


def test_append_dict_map_mojo_int():
    """Append Dict[String, Int] using Mojo's native Int type."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, BIGINT))")
    var appender = Appender(con, "t")
    var d: Dict[String, Int] = {'a': 1, 'b': 2}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, Int]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["a"], 1)
    assert_equal(m["b"], 2)


def test_append_dict_map_mojo_uint():
    """Append Dict[String, UInt] using Mojo's native UInt type."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, UBIGINT))")
    var appender = Appender(con, "t")
    var d: Dict[String, UInt] = {'x': 10, 'y': 20}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, UInt]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["x"], 10)
    assert_equal(m["y"], 20)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
