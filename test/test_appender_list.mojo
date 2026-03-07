"""Tests for the Appender API with List and Array types."""

from duckdb import *
from duckdb.duckdb_type import Bit, TimeNS
from std.collections import Optional
from std.testing import assert_equal
from std.testing.suite import TestSuite


def test_append_list_int32():
    """Append a List[Int32] into a LIST(INTEGER) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    var appender = Appender(con, "t")

    var l1 = List[Int32]()
    l1.append(1)
    l1.append(2)
    l1.append(3)
    appender.append_value(l1)
    appender.end_row()

    var l2 = List[Int32]()
    l2.append(4)
    l2.append(5)
    appender.append_value(l2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Int32]]](col=0)
    assert_equal(len(lists), 2)

    var row0 = lists[0].copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)

    var row1 = lists[1].copy()
    assert_equal(len(row1), 2)
    assert_equal(row1[0].value(), 4)
    assert_equal(row1[1].value(), 5)


def test_append_list_int64():
    """Append a List[Int64] into a LIST(BIGINT) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums BIGINT[])")
    var appender = Appender(con, "t")

    var l = List[Int64]()
    l.append(100)
    l.append(200)
    l.append(300)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Int64]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), 100)
    assert_equal(row[1].value(), 200)
    assert_equal(row[2].value(), 300)


def test_append_list_float64():
    """Append a List[Float64] into a LIST(DOUBLE) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (vals DOUBLE[])")
    var appender = Appender(con, "t")

    var l = List[Float64]()
    l.append(1.5)
    l.append(2.5)
    l.append(3.5)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT vals FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Float64]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), 1.5)
    assert_equal(row[1].value(), 2.5)
    assert_equal(row[2].value(), 3.5)


def test_append_list_bool():
    """Append a List[Bool] into a LIST(BOOLEAN) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (flags BOOLEAN[])")
    var appender = Appender(con, "t")

    var l = List[Bool]()
    l.append(True)
    l.append(False)
    l.append(True)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT flags FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Bool]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), True)
    assert_equal(row[1].value(), False)
    assert_equal(row[2].value(), True)


def test_append_empty_list():
    """Append an empty list."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    var appender = Appender(con, "t")
    appender.append_value(List[Int32]())
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(row), 0)


def test_appender_mojo_int():
    """Append and read back Mojo's native Int type as a scalar."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (v BIGINT)")
    var appender = Appender(con, "t")
    appender.append_value(Int(42))
    appender.end_row()
    appender.append_value(Int(-7))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT v FROM t ORDER BY v")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int](col=0, row=0), -7)
    assert_equal(chunk.get[Int](col=0, row=1), 42)


def test_appender_mojo_uint():
    """Append and read back Mojo's native UInt type as a scalar."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (v UBIGINT)")
    var appender = Appender(con, "t")
    appender.append_value(UInt(100))
    appender.end_row()
    appender.append_value(UInt(200))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT v FROM t ORDER BY v")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt](col=0, row=0), 100)
    assert_equal(chunk.get[UInt](col=0, row=1), 200)


def test_appender_time_ns():
    """Append and read back TIME_NS values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (t TIME_NS)")
    var appender = Appender(con, "t")
    appender.append_value(TimeNS(0))
    appender.end_row()
    appender.append_value(TimeNS(3_600_000_000_000))  # 1 hour
    appender.end_row()
    appender.close()

    result = con.execute("SELECT t::VARCHAR FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "00:00:00")
    assert_equal(chunk.get[String](col=0, row=1), "01:00:00")


def test_appender_array_int32():
    """Append List[Int32] to an ARRAY column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr INTEGER[3])")
    var appender = Appender(con, "t")
    var l1: List[Int32] = [1, 2, 3]
    appender.append_value(l1)
    appender.end_row()
    var l2: List[Int32] = [4, 5, 6]
    appender.append_value(l2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT arr FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)
    var row1 = chunk.get[List[Optional[Int32]]](col=0, row=1)
    assert_equal(row1[0].value(), 4)


def test_appender_array_varchar():
    """Append List[String] to an ARRAY(VARCHAR) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr VARCHAR[2])")
    var appender = Appender(con, "t")
    var l: List[String] = ["hello", "world"]
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT arr FROM t")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[Optional[String]]](col=0, row=0)
    assert_equal(len(row0), 2)
    assert_equal(row0[0].value(), "hello")
    assert_equal(row0[1].value(), "world")


def test_appender_bit():
    """Test appending Bit values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit("10110"))
    appender.end_row()
    appender.append_value(Bit("0"))
    appender.end_row()
    appender.close()

    # Read back as VARCHAR to verify
    result = con.execute("SELECT b::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "10110")
    assert_equal(chunk.get[String](col=0, row=1), "0")


def test_appender_bit_round_trip():
    """Test round-trip of BIT through appender and typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit("10110"))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT b FROM t")
    chunk = result.fetch_chunk()
    var val = chunk.get[Bit](col=0, row=0)
    assert_equal(String(val), "10110")
    assert_equal(len(val), 5)


def test_appender_bit_from_int32():
    """Test appending Bit constructed from Int32 matches DuckDB cast."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit(Int32(123)))
    appender.end_row()
    appender.close()

    # Read back the appended value
    result = con.execute("SELECT b::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    var appended = chunk.get[String](col=0, row=0)

    # Compare with DuckDB's own cast
    result2 = con.execute("SELECT (123::INTEGER::BITSTRING)::VARCHAR")
    chunk2 = result2.fetch_chunk()
    var duckdb_cast = chunk2.get[String](col=0, row=0)

    assert_equal(appended, duckdb_cast)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
