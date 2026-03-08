"""Tests for the Appender API."""

from duckdb import *
from duckdb.duckdb_type import Bit, Date, Decimal, Time, TimeNS, Timestamp, TimestampS, TimestampMS, TimestampNS, TimestampTZ, TimeTZ, UUID
from std.collections import Dict, Optional
from std.testing import assert_almost_equal, assert_equal, assert_raises, assert_true
from std.testing.suite import TestSuite
from std.utils import Variant


# ─── Helper structs ──────────────────────────────────────────────

@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String


@fieldwise_init
struct PersonOpt(Copyable, Movable):
    var id: Int32
    var name: Optional[String]


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


# ─── Core appender tests ────────────────────────────────────────

def test_appender_scalar_values() raises:
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


def test_appender_struct_row() raises:
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


def test_appender_tuple_row() raises:
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


def test_appender_bulk_rows() raises:
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


def test_appender_optional_null() raises:
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


def test_appender_append_null() raises:
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


def test_appender_flush() raises:
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


def test_appender_column_count() raises:
    """Test column_count metadata method."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE)")
    var appender = Appender(con, "t")
    assert_equal(appender.column_count(), 3)
    appender.close()


def test_appender_many_rows() raises:
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


def test_appender_auto_destroy() raises:
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


def test_appender_invalid_table() raises:
    """Test that creating an appender for a non-existent table raises."""
    con = DuckDB.connect(":memory:")
    with assert_raises():
        var appender = Appender(con, "nonexistent_table")


# ─── Numeric type tests ─────────────────────────────────────────

def test_appender_all_int_types() raises:
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


def test_appender_all_uint_types() raises:
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


def test_appender_float_types() raises:
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


def test_appender_bool_type() raises:
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


def test_appender_hugeint() raises:
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


def test_appender_uhugeint() raises:
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


def test_appender_hugeint_typed_api() raises:
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


def test_appender_uhugeint_typed_api() raises:
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


def test_appender_decimal() raises:
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


def test_appender_decimal_typed_api() raises:
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


def test_appender_blob() raises:
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


# ─── Temporal type tests ────────────────────────────────────────

def test_appender_date_type() raises:
    """Test appending Date values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (d DATE)")
    var appender = Appender(con, "t")
    appender.append_value(Date(18628))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT d::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "2021-01-01")


def test_appender_timestamp_type() raises:
    """Test appending Timestamp values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP)")
    var appender = Appender(con, "t")
    appender.append_value(Timestamp(0))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1970-01-01 00:00:00")


def test_appender_timestamp_s() raises:
    """Test appending TIMESTAMP_S values and round-trip via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_S)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampS(0))
    appender.end_row()
    appender.append_value(TimestampS(1609459200))  # 2021-01-01 00:00:00
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1970-01-01 00:00:00")
    assert_equal(chunk.get[String](col=0, row=1), "2021-01-01 00:00:00")


def test_appender_timestamp_s_typed_api() raises:
    """Test round-trip of TIMESTAMP_S through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_S)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampS(1609459200))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[TimestampS](col=0, row=0), TimestampS(1609459200))


def test_appender_timestamp_ms() raises:
    """Test appending TIMESTAMP_MS values and round-trip via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_MS)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampMS(0))
    appender.end_row()
    appender.append_value(TimestampMS(1609459200000))  # 2021-01-01 in ms
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1970-01-01 00:00:00")
    assert_equal(chunk.get[String](col=0, row=1), "2021-01-01 00:00:00")


def test_appender_timestamp_ms_typed_api() raises:
    """Test round-trip of TIMESTAMP_MS through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_MS)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampMS(1609459200000))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[TimestampMS](col=0, row=0), TimestampMS(1609459200000))


def test_appender_timestamp_ns() raises:
    """Test appending TIMESTAMP_NS values and round-trip via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_NS)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampNS(0))
    appender.end_row()
    appender.append_value(TimestampNS(1609459200000000000))  # 2021-01-01 in ns
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1970-01-01 00:00:00")
    assert_equal(chunk.get[String](col=0, row=1), "2021-01-01 00:00:00")


def test_appender_timestamp_ns_typed_api() raises:
    """Test round-trip of TIMESTAMP_NS through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMP_NS)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampNS(1609459200000000000))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[TimestampNS](col=0, row=0), TimestampNS(1609459200000000000))


def test_appender_timestamp_tz() raises:
    """Test appending TIMESTAMPTZ values and reading back as text."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampTZ(0))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    var ts_str = chunk.get[String](col=0, row=0)
    assert_true(ts_str.startswith("1970-01-01"), "Expected 1970-01-01, got: " + ts_str)


def test_appender_timestamp_tz_typed_api() raises:
    """Test round-trip of TIMESTAMPTZ through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("SET TimeZone = 'UTC'")
    _ = con.execute("CREATE TABLE t (ts TIMESTAMPTZ)")
    var appender = Appender(con, "t")
    appender.append_value(TimestampTZ(1609459200000000))  # 2021-01-01 in micros
    appender.end_row()
    appender.close()

    result = con.execute("SELECT ts FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[TimestampTZ](col=0, row=0), TimestampTZ(1609459200000000))


def test_appender_time_tz() raises:
    """Test appending TIMETZ values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (t TIMETZ)")
    var appender = Appender(con, "t")
    var ttz = TimeTZ(
        micros=Int64(12) * 3600 * 1_000_000 + Int64(30) * 60 * 1_000_000,
        offset=Int32(7200),
    )
    appender.append_value(ttz)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT t::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    var val = chunk.get[String](col=0, row=0)
    assert_true(val.startswith("12:30:00"), "Expected 12:30:00, got: " + val)


def test_appender_time_tz_typed_api() raises:
    """Test round-trip of TIMETZ through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (t TIMETZ)")
    var appender = Appender(con, "t")
    var ttz = TimeTZ(
        micros=Int64(12) * 3600 * 1_000_000 + Int64(30) * 60 * 1_000_000,
        offset=Int32(7200),
    )
    appender.append_value(ttz)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT t FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[TimeTZ](col=0, row=0), ttz)


def test_appender_uuid() raises:
    """Test appending UUID values and reading back as text."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id UUID)")
    var appender = Appender(con, "t")
    appender.append_value(UUID(UInt128(0)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(
        chunk.get[String](col=0, row=0),
        "00000000-0000-0000-0000-000000000000",
    )


def test_appender_uuid_typed_api() raises:
    """Test round-trip of UUID through appender and typed API fetch."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id UUID)")
    var appender = Appender(con, "t")
    appender.append_value(UUID(UInt128(42)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id FROM t")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[UUID](col=0, row=0), UUID(UInt128(42)))


def test_appender_uuid_from_sql() raises:
    """Test reading a SQL-inserted UUID via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id UUID)")
    _ = con.execute("INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000')")

    result = con.execute("SELECT id FROM t")
    chunk = result.fetch_chunk()
    var uuid = chunk.get[UUID](col=0, row=0)

    _ = con.execute("CREATE TABLE t2 (id UUID)")
    var appender = Appender(con, "t2")
    appender.append_value(uuid)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id::VARCHAR FROM t2")
    chunk = result.fetch_chunk()
    assert_equal(
        chunk.get[String](col=0, row=0),
        "550e8400-e29b-41d4-a716-446655440000",
    )


def test_appender_mojo_int() raises:
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


def test_appender_mojo_uint() raises:
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


def test_appender_time_ns() raises:
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


def test_appender_bit() raises:
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


def test_appender_bit_round_trip() raises:
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


def test_appender_bit_from_int32() raises:
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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
