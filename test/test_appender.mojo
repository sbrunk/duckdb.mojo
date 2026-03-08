"""Tests for the Appender API (core functionality)."""

from duckdb import *
from std.collections import Optional
from std.testing import assert_equal, assert_raises
from std.testing.suite import TestSuite


# ─── Helper structs ──────────────────────────────────────────────

@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String


@fieldwise_init
struct PersonOpt(Copyable, Movable):
    var id: Int32
    var name: Optional[String]


# ─── Tests ────────────────────────────────────────────────────────

def test_appender_scalar_values():
    """Test appending individual scalar values and then reading them back."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    appender.append_value(String("Alice"))
    appender.end_row()
    appender.append_value(Int32(2))
    appender.append_value(String("Bob"))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id, name FROM t ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    assert_equal(chunk.get[String](col=1, row=0), "Alice")
    assert_equal(chunk.get[String](col=1, row=1), "Bob")


def test_appender_struct_row():
    """Test appending a struct as a complete row."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")
    var appender = Appender(con, "people")
    appender.append_row(Person(1, "Mark"))
    appender.append_row(Person(2, "Hannes"))
    appender.close()

    result = con.execute("SELECT id, name FROM people ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    assert_equal(chunk.get[String](col=1, row=0), "Mark")
    assert_equal(chunk.get[String](col=1, row=1), "Hannes")


def test_appender_tuple_row():
    """Test appending a tuple as a complete row."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    var appender = Appender(con, "t")
    appender.append_tuple_row(Tuple(Int32(1), String("Alpha")))
    appender.append_tuple_row(Tuple(Int32(2), String("Beta")))
    appender.close()

    result = con.execute("SELECT id, name FROM t ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    assert_equal(chunk.get[String](col=1, row=0), "Alpha")
    assert_equal(chunk.get[String](col=1, row=1), "Beta")


def test_appender_bulk_rows():
    """Test bulk-appending from a List of structs."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")
    var people = List[Person]()
    people.append(Person(10, "Ann"))
    people.append(Person(20, "Ben"))
    people.append(Person(30, "Cal"))
    var appender = Appender(con, "people")
    appender.append_rows(people)
    appender.close()

    result = con.execute("SELECT COUNT(*) FROM people")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(3))

    result = con.execute("SELECT name FROM people ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "Ann")
    assert_equal(chunk.get[String](col=0, row=1), "Ben")
    assert_equal(chunk.get[String](col=0, row=2), "Cal")


def test_appender_optional_null():
    """Test that Optional[None] maps to SQL NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    var appender = Appender(con, "t")
    appender.append_row(PersonOpt(1, String("Present")))
    appender.append_row(PersonOpt(2, None))
    appender.close()

    result = con.execute("SELECT id, name FROM t ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    assert_equal(chunk.get[String](col=1, row=0), "Present")

    result = con.execute("SELECT COUNT(*) FROM t WHERE name IS NULL")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1))


def test_appender_append_null():
    """Test low-level append_null method."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    appender.append_null()
    appender.end_row()
    appender.close()

    result = con.execute("SELECT COUNT(*) FROM t WHERE name IS NULL")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1))


def test_appender_flush():
    """Test that flush works without error and data is visible."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER)")
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    appender.end_row()
    appender.flush()

    result = con.execute("SELECT COUNT(*) FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1))

    appender.append_value(Int32(2))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT COUNT(*) FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(2))


def test_appender_column_count():
    """Test column_count metadata method."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE)")
    var appender = Appender(con, "t")
    assert_equal(appender.column_count(), 3)
    appender.close()


def test_appender_many_rows():
    """Test appending a larger number of rows."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, val DOUBLE)")

    var appender = Appender(con, "t")
    for i in range(1000):
        appender.append_value(Int32(i))
        appender.append_value(Float64(i) * 1.5)
        appender.end_row()
    appender.close()

    result = con.execute("SELECT COUNT(*) FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1000))

    result = con.execute("SELECT SUM(id)::BIGINT FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(499500))


def test_appender_auto_destroy():
    """Test that appender auto-flushes on destruction (scope exit)."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER)")

    var appender = Appender(con, "t")
    appender.append_value(Int32(42))
    appender.end_row()
    appender^.__del__()

    result = con.execute("SELECT id FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(42))


def test_appender_invalid_table():
    """Test that creating an appender for a non-existent table raises."""
    con = DuckDB.connect(":memory:")
    with assert_raises():
        var appender = Appender(con, "nonexistent_table")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
