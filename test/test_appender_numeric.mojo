"""Tests for the Appender API with numeric types (integers, floats, hugeint, decimal)."""

from duckdb import *
from duckdb.duckdb_type import Decimal
from std.testing import assert_equal, assert_almost_equal
from std.testing.suite import TestSuite


# ─── Helper structs ──────────────────────────────────────────────

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
struct WithBool(Copyable, Movable):
    var flag: Bool
    var value: Int32


# ─── Tests ────────────────────────────────────────────────────────

def test_appender_all_int_types():
    """Test signed integer types: Int8, Int16, Int32, Int64."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (a TINYINT, b SMALLINT, c INTEGER, d BIGINT)")
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
    var a = chunk.get[Float32](col=0, row=0)
    var b = chunk.get[Float64](col=1, row=0)
    assert_almost_equal(a, 3.14, atol=0.001, msg="Float32 value mismatch")
    assert_almost_equal(b, 2.71828, atol=0.00001, msg="Float64 value mismatch")


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


def test_appender_hugeint():
    """Test appending HUGEINT (Int128) values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val HUGEINT)")
    var appender = Appender(con, "t")
    appender.append_value(Int128(0))
    appender.end_row()
    appender.append_value(Int128(123456789012345))
    appender.end_row()
    appender.append_value(Int128(-42))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "0")
    assert_equal(chunk.get[String](col=0, row=1), "123456789012345")
    assert_equal(chunk.get[String](col=0, row=2), "-42")


def test_appender_uhugeint():
    """Test appending UHUGEINT (UInt128) values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val UHUGEINT)")
    var appender = Appender(con, "t")
    appender.append_value(UInt128(0))
    appender.end_row()
    appender.append_value(UInt128(999999999999999))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "0")
    assert_equal(chunk.get[String](col=0, row=1), "999999999999999")


def test_appender_hugeint_typed_api():
    """Test round-trip of HUGEINT through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val HUGEINT)")
    var appender = Appender(con, "t")
    appender.append_value(Int128(123456789012345))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int128](col=0, row=0), Int128(123456789012345))


def test_appender_uhugeint_typed_api():
    """Test round-trip of UHUGEINT through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val UHUGEINT)")
    var appender = Appender(con, "t")
    appender.append_value(UInt128(999999999999999))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt128](col=0, row=0), UInt128(999999999999999))


def test_appender_decimal():
    """Test appending Decimal values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val DECIMAL(10, 2))")
    var appender = Appender(con, "t")
    appender.append_value(Decimal(UInt8(10), UInt8(2), Int128(12345)))
    appender.end_row()
    appender.append_value(Decimal(UInt8(10), UInt8(2), Int128(-9999)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "123.45")
    assert_equal(chunk.get[String](col=0, row=1), "-99.99")


def test_appender_decimal_typed_api():
    """Test round-trip of DECIMAL through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (val DECIMAL(10, 2))")
    var appender = Appender(con, "t")
    appender.append_value(Decimal(UInt8(10), UInt8(2), Int128(12345)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT val FROM t")
    chunk = result.fetch_chunk()
    var dec = chunk.get[Decimal](col=0, row=0)
    assert_equal(dec.width, UInt8(10))
    assert_equal(dec.scale, UInt8(2))
    assert_equal(dec.value(), Int128(12345))


def test_appender_blob():
    """Test appending BLOB (List[UInt8]) values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (data BLOB)")
    var appender = Appender(con, "t")
    var blob = List[UInt8](capacity=5)
    blob.append(0x48)  # H
    blob.append(0x65)  # e
    blob.append(0x6C)  # l
    blob.append(0x6C)  # l
    blob.append(0x6F)  # o
    appender.append_value(blob)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT data::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "Hello")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
