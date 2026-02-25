"""Tests for the Appender API."""

from duckdb import *
from duckdb.duckdb_type import Date, Time, Timestamp, Interval
from collections import Optional
from testing import assert_equal, assert_true, assert_raises
from testing.suite import TestSuite


# ─── Helper structs ──────────────────────────────────────────────

@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String


@fieldwise_init
struct AllInts(Copyable, Movable):
    var a: Int8
    var b: Int16
    var c: Int32
    var d: Int64


@fieldwise_init
struct AllUInts(Copyable, Movable):
    var a: UInt8
    var b: UInt16
    var c: UInt32
    var d: UInt64


@fieldwise_init
struct Floats(Copyable, Movable):
    var a: Float32
    var b: Float64


@fieldwise_init
struct PersonOpt(Copyable, Movable):
    var id: Int32
    var name: Optional[String]


@fieldwise_init
struct WithBool(Copyable, Movable):
    var flag: Bool
    var value: Int32


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

    # The row with None should have NULL in the name column
    result = con.execute("SELECT id, name FROM t ORDER BY id")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    assert_equal(chunk.get[String](col=1, row=0), "Present")

    # Check NULL via SQL
    result = con.execute("SELECT COUNT(*) FROM t WHERE name IS NULL")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1))


def test_appender_all_int_types():
    """Test signed integer types: Int8, Int16, Int32, Int64."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (a TINYINT, b SMALLINT, c INTEGER, d BIGINT)"
    )
    var appender = Appender(con, "t")
    appender.append_row(AllInts(Int8(1), Int16(2), Int32(3), Int64(4)))
    appender.close()

    result = con.execute("SELECT a, b, c, d FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int8](col=0, row=0), Int8(1))
    assert_equal(chunk.get[Int16](col=1, row=0), Int16(2))
    assert_equal(chunk.get[Int32](col=2, row=0), Int32(3))
    assert_equal(chunk.get[Int64](col=3, row=0), Int64(4))


def test_appender_all_uint_types():
    """Test unsigned integer types: UInt8, UInt16, UInt32, UInt64."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (a UTINYINT, b USMALLINT, c UINTEGER, d UBIGINT)"
    )
    var appender = Appender(con, "t")
    appender.append_row(AllUInts(UInt8(10), UInt16(20), UInt32(30), UInt64(40)))
    appender.close()

    result = con.execute("SELECT a, b, c, d FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt8](col=0, row=0), UInt8(10))
    assert_equal(chunk.get[UInt16](col=1, row=0), UInt16(20))
    assert_equal(chunk.get[UInt32](col=2, row=0), UInt32(30))
    assert_equal(chunk.get[UInt64](col=3, row=0), UInt64(40))


def test_appender_float_types():
    """Test Float32 and Float64 types."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (a FLOAT, b DOUBLE)")
    var appender = Appender(con, "t")
    appender.append_row(Floats(Float32(3.14), Float64(2.71828)))
    appender.close()

    result = con.execute("SELECT a, b FROM t")
    chunk = result.fetch_chunk()
    # Float comparison with tolerance
    var a = chunk.get[Float32](col=0, row=0)
    var b = chunk.get[Float64](col=1, row=0)
    assert_true(abs(Float64(a) - 3.14) < 0.001, "Float32 value mismatch")
    assert_true(abs(b - 2.71828) < 0.00001, "Float64 value mismatch")


def test_appender_bool_type():
    """Test Bool type."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (flag BOOLEAN, value INTEGER)")
    var appender = Appender(con, "t")
    appender.append_row(WithBool(True, Int32(1)))
    appender.append_row(WithBool(False, Int32(0)))
    appender.close()

    result = con.execute("SELECT flag, value FROM t ORDER BY value")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Bool](col=0, row=0), False)
    assert_equal(chunk.get[Bool](col=0, row=1), True)


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

    # After flush, data should be visible
    result = con.execute("SELECT COUNT(*) FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(1))

    # Can still append after flush
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

    # Sum of 0..999 = 999 * 1000 / 2 = 499500
    result = con.execute("SELECT SUM(id)::BIGINT FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(499500))


def test_appender_auto_destroy():
    """Test that appender auto-flushes on destruction (scope exit)."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER)")

    # Create appender, append data, then let it drop without explicit close
    var appender = Appender(con, "t")
    appender.append_value(Int32(42))
    appender.end_row()
    # Explicitly drop the appender (simulates going out of scope)
    appender^.__del__()

    result = con.execute("SELECT id FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(42))


def test_appender_date_type():
    """Test appending Date values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (d DATE)")
    var appender = Appender(con, "t")
    # days since 1970-01-01 — e.g. 18628 = 2021-01-01
    appender.append_value(Date(18628))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT d::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "2021-01-01")


def test_appender_timestamp_type():
    """Test appending Timestamp values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP)")
    var appender = Appender(con, "t")
    # micros since epoch — 0 = 1970-01-01 00:00:00
    appender.append_value(Timestamp(0))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1970-01-01 00:00:00")


def test_appender_invalid_table():
    """Test that creating an appender for a non-existent table raises."""
    con = DuckDB.connect(":memory:")
    with assert_raises():
        var appender = Appender(con, "nonexistent_table")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
