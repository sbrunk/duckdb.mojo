"""Tests for the Appender API with Variant/UNION types."""

from duckdb import *
from std.utils import Variant
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


def test_append_variant_int_member():
    """Append a Variant whose active member is an integer."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(42)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v.isa[Int32]())
    assert_equal(v[Int32], 42)


def test_append_variant_str_member():
    """Append a Variant whose active member is a string."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v.isa[String]())
    assert_equal(v[String], "hello")


def test_append_variant_multiple_rows():
    """Append several rows with different active Variant members."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(1)))
    appender.end_row()
    appender.append_value(Variant[Int32, String](String("two")))
    appender.end_row()
    appender.append_value(Variant[Int32, String](Int32(3)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Variant[Int32, String]](col=0)
    assert_equal(len(vals), 3)

    assert_true(vals[0].isa[Int32]())
    assert_equal(vals[0][Int32], 1)

    assert_true(vals[1].isa[String]())
    assert_equal(vals[1][String], "two")

    assert_true(vals[2].isa[Int32]())
    assert_equal(vals[2][Int32], 3)


def test_append_variant_three_members():
    """Append to a UNION with three member types."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(i INTEGER, f FLOAT, s VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, Float32, String](Float32(3.14)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, Float32, String]](col=0, row=0)
    assert_true(v.isa[Float32]())


def test_append_variant_with_id_column():
    """Append Variant to a table that also has a regular column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (id INTEGER, u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.append_value(Int32(2))
    appender.append_value(Variant[Int32, String](Int32(99)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id, u FROM t ORDER BY id")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    var v0 = chunk.get[Variant[Int32, String]](col=1, row=0)
    assert_true(v0.isa[String]())
    assert_equal(v0[String], "hello")

    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    var v1 = chunk.get[Variant[Int32, String]](col=1, row=1)
    assert_true(v1.isa[Int32]())
    assert_equal(v1[Int32], 99)


def test_append_variant_roundtrip():
    """Append Variant values and read them back via Variant deserialization."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(42)))
    appender.end_row()
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var v0 = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v0.isa[Int32]())
    assert_equal(v0[Int32], 42)

    var v1 = chunk.get[Variant[Int32, String]](col=0, row=1)
    assert_true(v1.isa[String]())
    assert_equal(v1[String], "hello")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
