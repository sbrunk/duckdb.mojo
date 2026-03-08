"""Tests for the Appender API with temporal types and UUID."""

from duckdb import *
from duckdb.duckdb_type import Date, Time, Timestamp, TimestampS, TimestampMS, TimestampNS, TimestampTZ, TimeTZ, UUID
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


def test_appender_date_type():
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


def test_appender_timestamp_type():
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


def test_appender_timestamp_s():
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


def test_appender_timestamp_s_typed_api():
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


def test_appender_timestamp_ms():
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


def test_appender_timestamp_ms_typed_api():
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


def test_appender_timestamp_ns():
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


def test_appender_timestamp_ns_typed_api():
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


def test_appender_timestamp_tz():
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


def test_appender_timestamp_tz_typed_api():
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


def test_appender_time_tz():
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


def test_appender_time_tz_typed_api():
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


def test_appender_uuid():
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


def test_appender_uuid_typed_api():
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


def test_appender_uuid_from_sql():
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


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
