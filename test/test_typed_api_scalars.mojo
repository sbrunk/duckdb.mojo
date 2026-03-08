"""Tests for the typed API — scalar types, date/time, timestamps, UUID, hugeint, decimal, null handling, column retrieval, multiple columns, type mismatch, blob, enum, bit."""

from duckdb import *
from duckdb.duckdb_type import Bit
from std.collections import Optional
from std.testing import assert_equal, assert_false, assert_raises, assert_true
from std.testing.suite import TestSuite


def test_scalar_types_new_api() raises:
    """Test scalar types with the new get[T] API."""
    con = DuckDB.connect(":memory:")

    # Boolean
    result = con.execute("SELECT true")
    var chunk = result.fetch_chunk()
    var bool_val = chunk.get[Bool](col=0, row=0)
    assert_true(bool_val)
    assert_equal(bool_val, True)

    # Int types
    result = con.execute("SELECT -42::TINYINT")
    chunk = result.fetch_chunk()
    var tinyint_val = chunk.get[Int8](col=0, row=0)
    assert_equal(tinyint_val, -42)

    result = con.execute("SELECT 42::UTINYINT")
    chunk = result.fetch_chunk()
    var utinyint_val = chunk.get[UInt8](col=0, row=0)
    assert_equal(utinyint_val, 42)

    result = con.execute("SELECT -42::SMALLINT")
    chunk = result.fetch_chunk()
    var smallint_val = chunk.get[Int16](col=0, row=0)
    assert_equal(smallint_val, -42)

    result = con.execute("SELECT 42::USMALLINT")
    chunk = result.fetch_chunk()
    var usmallint_val = chunk.get[UInt16](col=0, row=0)
    assert_equal(usmallint_val, 42)

    result = con.execute("SELECT -42::INTEGER")
    chunk = result.fetch_chunk()
    var int_val = chunk.get[Int32](col=0, row=0)
    assert_equal(int_val, -42)

    result = con.execute("SELECT 42::UINTEGER")
    chunk = result.fetch_chunk()
    var uint_val = chunk.get[UInt32](col=0, row=0)
    assert_equal(uint_val, 42)

    result = con.execute("SELECT -42::BIGINT")
    chunk = result.fetch_chunk()
    var bigint_val = chunk.get[Int64](col=0, row=0)
    assert_equal(bigint_val, -42)

    result = con.execute("SELECT 42::UBIGINT")
    chunk = result.fetch_chunk()
    var ubigint_val = chunk.get[UInt64](col=0, row=0)
    assert_equal(ubigint_val, 42)

    # Float types
    result = con.execute("SELECT 42.5::FLOAT")
    chunk = result.fetch_chunk()
    var float_val = chunk.get[Float32](col=0, row=0)
    assert_equal(float_val, 42.5)

    result = con.execute("SELECT 42.5::DOUBLE")
    chunk = result.fetch_chunk()
    var double_val = chunk.get[Float64](col=0, row=0)
    assert_equal(double_val, 42.5)

    # String
    result = con.execute("SELECT 'hello'")
    chunk = result.fetch_chunk()
    var str_val = chunk.get[String](col=0, row=0)
    assert_equal(str_val, "hello")

    result = con.execute("SELECT 'hello longer varchar'")
    chunk = result.fetch_chunk()
    var long_str_val = chunk.get[String](col=0, row=0)
    assert_equal(long_str_val, "hello longer varchar")

    # Mojo Int (platform-dependent width, maps to BIGINT on 64-bit)
    result = con.execute("SELECT 42::BIGINT")
    chunk = result.fetch_chunk()
    var mojo_int_val = chunk.get[Int](col=0, row=0)
    assert_equal(mojo_int_val, 42)

    # Mojo UInt (platform-dependent width, maps to UBIGINT on 64-bit)
    result = con.execute("SELECT 42::UBIGINT")
    chunk = result.fetch_chunk()
    var mojo_uint_val = chunk.get[UInt](col=0, row=0)
    assert_equal(mojo_uint_val, 42)


def test_date_time_types_new_api() raises:
    """Test date/time types with the new API."""
    con = DuckDB.connect(":memory:")

    # Timestamp
    result = con.execute("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789'")
    var chunk = result.fetch_chunk()
    var ts_val = chunk.get[Timestamp](col=0, row=0)
    assert_equal(ts_val, Timestamp(716988600123456))

    # Date
    result = con.execute("SELECT DATE '1992-09-20'")
    chunk = result.fetch_chunk()
    var date_val = chunk.get[Date](col=0, row=0)
    assert_equal(date_val, Date(8298))

    # Time
    result = con.execute("SELECT TIME '1992-09-20 11:30:00.123456'")
    chunk = result.fetch_chunk()
    var time_val = chunk.get[Time](col=0, row=0)
    assert_equal(time_val, Time(41400123456))


def test_timestamp_variants_typed_api() raises:
    """Test TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS with get[T]."""
    con = DuckDB.connect(":memory:")

    # TIMESTAMP_S
    result = con.execute("SELECT TIMESTAMP_S '2021-01-01 00:00:00'")
    var chunk = result.fetch_chunk()
    var ts_s = chunk.get[TimestampS](col=0, row=0)
    assert_equal(ts_s, TimestampS(1609459200))

    # TIMESTAMP_MS
    result = con.execute("SELECT TIMESTAMP_MS '2021-01-01 00:00:00'")
    chunk = result.fetch_chunk()
    var ts_ms = chunk.get[TimestampMS](col=0, row=0)
    assert_equal(ts_ms, TimestampMS(1609459200000))

    # TIMESTAMP_NS
    result = con.execute("SELECT TIMESTAMP_NS '2021-01-01 00:00:00'")
    chunk = result.fetch_chunk()
    var ts_ns = chunk.get[TimestampNS](col=0, row=0)
    assert_equal(ts_ns, TimestampNS(1609459200000000000))


def test_timestamp_tz_typed_api() raises:
    """Test TIMESTAMPTZ with get[T]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("SET TimeZone = 'UTC'")

    result = con.execute("SELECT TIMESTAMPTZ '2021-01-01 00:00:00+00'")
    var chunk = result.fetch_chunk()
    var ts_tz = chunk.get[TimestampTZ](col=0, row=0)
    assert_equal(ts_tz, TimestampTZ(1609459200000000))


def test_time_tz_typed_api() raises:
    """Test TIMETZ with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT TIMETZ '12:30:00+02:00'")
    var chunk = result.fetch_chunk()
    var ttz = chunk.get[TimeTZ](col=0, row=0)

    # Build the expected value using the same helper
    var expected = TimeTZ(
        micros=Int64(12) * 3600 * 1_000_000 + Int64(30) * 60 * 1_000_000,
        offset=Int32(7200),
    )
    assert_equal(ttz, expected)


def test_uuid_typed_api() raises:
    """Test UUID with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID")
    var chunk = result.fetch_chunk()
    var uuid = chunk.get[UUID](col=0, row=0)

    # Verify round-trip: read UUID, cast back to string via SQL
    _ = con.execute("CREATE TABLE t (id UUID)")
    var appender = Appender(con, "t")
    appender.append_value(uuid)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(
        chunk.get[String](col=0, row=0),
        "550e8400-e29b-41d4-a716-446655440000",
    )


def test_uuid_zero_typed_api() raises:
    """Test UUID zero value with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT '00000000-0000-0000-0000-000000000000'::UUID")
    var chunk = result.fetch_chunk()
    var uuid = chunk.get[UUID](col=0, row=0)
    assert_equal(uuid, UUID(UInt128(0)))


def test_hugeint_typed_api() raises:
    """Test HUGEINT (Int128) with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 123456789012345::HUGEINT")
    var chunk = result.fetch_chunk()
    var val = chunk.get[Int128](col=0, row=0)
    assert_equal(val, Int128(123456789012345))

    # Negative value
    result = con.execute("SELECT (-42)::HUGEINT")
    chunk = result.fetch_chunk()
    var neg = chunk.get[Int128](col=0, row=0)
    assert_equal(neg, Int128(-42))

    # Zero
    result = con.execute("SELECT 0::HUGEINT")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int128](col=0, row=0), Int128(0))


def test_uhugeint_typed_api() raises:
    """Test UHUGEINT (UInt128) with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 999999999999999::UHUGEINT")
    var chunk = result.fetch_chunk()
    var val = chunk.get[UInt128](col=0, row=0)
    assert_equal(val, UInt128(999999999999999))

    # Zero
    result = con.execute("SELECT 0::UHUGEINT")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt128](col=0, row=0), UInt128(0))


def test_decimal_typed_api() raises:
    """Test DECIMAL with get[T]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE d (val DECIMAL(10, 2))")
    _ = con.execute("INSERT INTO d VALUES (123.45)")
    _ = con.execute("INSERT INTO d VALUES (-99.99)")

    result = con.execute("SELECT val FROM d ORDER BY rowid")
    var chunk = result.fetch_chunk()

    var d1 = chunk.get[Decimal](col=0, row=0)
    assert_equal(d1.width, UInt8(10))
    assert_equal(d1.scale, UInt8(2))
    assert_equal(d1.value(), Int128(12345))

    var d2 = chunk.get[Decimal](col=0, row=1)
    assert_equal(d2.value(), Int128(-9999))


def test_timestamp_variants_column_api() raises:
    """Test fetching a full column of timestamp variants."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE ts (a TIMESTAMP_S, b TIMESTAMP_MS, c TIMESTAMP_NS)")
    _ = con.execute("INSERT INTO ts VALUES (TIMESTAMP_S '2021-01-01', TIMESTAMP_MS '2021-01-01', TIMESTAMP_NS '2021-01-01')")
    _ = con.execute("INSERT INTO ts VALUES (TIMESTAMP_S '1970-01-01', TIMESTAMP_MS '1970-01-01', TIMESTAMP_NS '1970-01-01')")

    result = con.execute("SELECT a, b, c FROM ts ORDER BY rowid")
    var chunk = result.fetch_chunk()

    var col_s = chunk.get[TimestampS](col=0)
    assert_equal(len(col_s), 2)
    assert_equal(col_s[0], TimestampS(1609459200))
    assert_equal(col_s[1], TimestampS(0))

    var col_ms = chunk.get[TimestampMS](col=1)
    assert_equal(len(col_ms), 2)
    assert_equal(col_ms[0], TimestampMS(1609459200000))
    assert_equal(col_ms[1], TimestampMS(0))

    var col_ns = chunk.get[TimestampNS](col=2)
    assert_equal(len(col_ns), 2)
    assert_equal(col_ns[0], TimestampNS(1609459200000000000))
    assert_equal(col_ns[1], TimestampNS(0))


def test_null_handling_new_api() raises:
    """Test NULL handling with the new API."""
    con = DuckDB.connect(":memory:")

    # NULL integer
    result = con.execute("SELECT null::INT")
    var chunk = result.fetch_chunk()
    var null_val = chunk.get[Optional[Int32]](col=0, row=0)
    assert_false(null_val)

    # Mixed nulls and values
    result = con.execute("SELECT * FROM (VALUES (1), (null), (3)) AS t(x)")
    chunk = result.fetch_chunk()

    var val1 = chunk.get[Int32](col=0, row=0)
    assert_equal(val1, 1)

    var val2 = chunk.get[Optional[Int32]](col=0, row=1)
    assert_false(val2)

    var val3 = chunk.get[Int32](col=0, row=2)
    assert_equal(val3, 3)


def test_column_retrieval_new_api() raises:
    """Test getting all values from a column."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)")
    var chunk = result.fetch_chunk()
    var values = chunk.get[Int32](col=0)

    assert_equal(len(values), 5)
    for i in range(5):
        assert_equal(values[i], Int32(i + 1))


def test_multiple_columns_new_api() raises:
    """Test retrieving multiple columns with different types."""
    con = DuckDB.connect(":memory:")

    result = con.execute("""
        SELECT * FROM (VALUES
            (1, 'alice', 25.5::DOUBLE),
            (2, 'bob', 30.0::DOUBLE),
            (3, 'charlie', 35.5::DOUBLE)
        ) AS t(id, name, score)
    """)
    var chunk = result.fetch_chunk()

    var ids = chunk.get[Int32](col=0)
    var names = chunk.get[String](col=1)
    var scores = chunk.get[Float64](col=2)

    assert_equal(len(ids), 3)
    assert_equal(len(names), 3)
    assert_equal(len(scores), 3)

    assert_equal(ids[0], 1)
    assert_equal(names[0], "alice")
    assert_equal(scores[0], 25.5)

    assert_equal(ids[2], 3)
    assert_equal(names[2], "charlie")
    assert_equal(scores[2], 35.5)


def test_type_mismatch_new_api() raises:
    """Test that type mismatches raise errors."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 'hello'")
    var chunk = result.fetch_chunk()

    # Try to get a string as an int
    with assert_raises(contains="Type mismatch"):
        _ = chunk.get[Int32](col=0, row=0)


def test_blob_deserialize() raises:
    """Read BLOB column as List[UInt8] via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BLOB)")
    _ = con.execute("INSERT INTO t VALUES ('\\x01\\x02\\x03'::BLOB), ('\\xDE\\xAD'::BLOB)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[UInt8]](col=0, row=0)
    assert_equal(len(row0), 3)
    assert_equal(row0[0], 1)
    assert_equal(row0[1], 2)
    assert_equal(row0[2], 3)
    var row1 = chunk.get[List[UInt8]](col=0, row=1)
    assert_equal(len(row1), 2)
    assert_equal(row1[0], 0xDE)
    assert_equal(row1[1], 0xAD)


def test_blob_deserialize_column() raises:
    """Read BLOB column as full column of List[UInt8]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BLOB)")
    _ = con.execute("INSERT INTO t VALUES ('\\x01\\x02'::BLOB), (NULL)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[List[UInt8]]](col=0)
    assert_equal(len(vals), 2)
    assert_true(vals[0])
    assert_equal(len(vals[0].value()), 2)
    assert_false(vals[1])


def test_enum_deserialize() raises:
    """Read ENUM column as String via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
    _ = con.execute("CREATE TABLE t (m mood)")
    _ = con.execute("INSERT INTO t VALUES ('happy'), ('sad'), ('neutral')")
    result = con.execute("SELECT m FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "happy")
    assert_equal(chunk.get[String](col=0, row=1), "sad")
    assert_equal(chunk.get[String](col=0, row=2), "neutral")


def test_enum_deserialize_with_null() raises:
    """Read ENUM column with NULL values as Optional[String]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
    _ = con.execute("CREATE TABLE t (c color)")
    _ = con.execute("INSERT INTO t VALUES ('red'), (NULL), ('blue')")
    result = con.execute("SELECT c FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[String]](col=0)
    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(vals[0].value(), "red")
    assert_false(vals[1])
    assert_true(vals[2])
    assert_equal(vals[2].value(), "blue")


def test_bit_deserialize() raises:
    """Read BIT column as Bit."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    _ = con.execute("INSERT INTO t VALUES ('10110'::BIT), ('0'::BIT), ('11111111'::BIT)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var b0 = chunk.get[Bit](col=0, row=0)
    assert_equal(String(b0), "10110")
    assert_equal(len(b0), 5)
    var b1 = chunk.get[Bit](col=0, row=1)
    assert_equal(String(b1), "0")
    var b2 = chunk.get[Bit](col=0, row=2)
    assert_equal(String(b2), "11111111")
    assert_equal(len(b2), 8)


def test_bit_deserialize_column() raises:
    """Read entire BIT column as List[Optional[Bit]]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    _ = con.execute("INSERT INTO t VALUES ('101'::BIT), (NULL), ('0'::BIT)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[Bit]](col=0)
    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(String(vals[0].value()), "101")
    assert_false(vals[1])
    assert_true(vals[2])
    assert_equal(String(vals[2].value()), "0")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
