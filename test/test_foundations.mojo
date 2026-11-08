"""Tests for the foundations: database, connection,
logical types, values, column readers and the scalar type mapping."""

from std.memory import ArcPointer
from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb import *
from duckdb.chunk import _Schema
from duckdb._runtime import builtin_factory, check, lib
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb.scalar_types import (
    Bit,
    Date,
    Decimal,
    Interval,
    Time,
    TimeNS,
    TimeTZ,
    Timestamp,
    TimestampMS,
    TimestampNS,
    TimestampS,
    TimestampTZ,
    TimestampTZNS,
    UUID,
)


def _one(mut con: Connection, sql: String) raises -> MaterializedResult:
    var r = con.execute(sql).fetchall()
    assert_equal(r.chunk_count(), 1)
    return r^


def _roundtrip[T: Movable & Deinitable & Equatable & Writable](x: T) raises:
    """A value written with `Value(x)` reads back as the same Mojo value."""
    var v = Value(x)
    assert_false(v.is_null())
    assert_equal(v.get[T](), x)


# ── Database and connection ──────────────────────────────────────


def test_connect_and_query() raises:
    var db = Database()
    var con = db.connect()
    var r = _one(con, "SELECT 42::BIGINT AS answer")
    assert_equal(r.column_count(), 1)
    assert_equal(r.column_names()[0], "answer")
    assert_equal(r.get[Int64](col=0, row=0), 42)


def test_connections_share_the_database() raises:
    var db = Database()
    var a = db.connect()
    var b = db.connect()
    _ = a.execute("CREATE TABLE t AS SELECT 1 AS x")
    assert_equal(_one(b, "SELECT count(*)::BIGINT FROM t").get[Int64](col=0, row=0), 1)


def test_connection_keeps_database_alive() raises:
    var con = Database().connect()  # the Database value is gone after this line
    _ = con.execute("CREATE TABLE t AS SELECT 7 AS x")
    assert_equal(_one(con, "SELECT x FROM t").get[Int32](col=0, row=0), 7)


def test_config_options() raises:
    var db = Database(config={"threads": "3"})
    var con = db.connect()
    var r = _one(con, "SELECT current_setting('threads')::BIGINT")
    assert_equal(r.get[Int64](col=0, row=0), 3)


def test_read_only_and_same_file_twice() raises:
    var path = String("/tmp/duckdb_mojo_v2_foundations.db")
    from std.os import remove
    from std.os.path import exists

    for p in [path, path + ".wal"]:
        if exists(p):
            remove(p)
    var db = Database(path)
    var con = db.connect()
    _ = con.execute("CREATE TABLE t AS SELECT 1 AS x")
    # The process shares one instance cache, so a second open is refused.
    var raised = False
    try:
        _ = Database(path)
    except e:
        raised = True
        assert_true(e.code == ErrorCode.RESOURCE_IN_USE, String(e))
    assert_true(raised)
    _ = con^
    _ = db^
    var ro = Database(path, read_only=True)
    var con2 = ro.connect()
    raised = False
    try:
        _ = con2.execute("INSERT INTO t VALUES (2)")
    except e:
        raised = True
    assert_true(raised, "a read-only database accepted an INSERT")


def test_errors_have_codes() raises:
    var con = Database().connect()
    var error = Optional[DuckDBError]()
    try:
        _ = con.execute("SELECT * FROM no_such_table")
    except e:
        error = e^
    assert_true(error, "expected an error")
    ref e = error.value()
    assert_true(e.code.is_database() or e.code.is_query(), String(e))
    assert_true("no_such_table" in e.message, e.message)


# ── Logical types ─────────────────────────────────────────────────


def test_logical_types() raises:
    assert_equal(LogicalType(TypeId.INTEGER).id(), TypeId.INTEGER)
    assert_equal(String(LogicalType(TypeId.VARCHAR)), "VARCHAR")
    var d = LogicalType.decimal(18, 3)
    assert_equal(d.id(), TypeId.DECIMAL)
    assert_equal(d.decimal_width(), 18)
    assert_equal(d.decimal_scale(), 3)
    var l = LogicalType.list(LogicalType(TypeId.BIGINT))
    assert_equal(String(l), "BIGINT[]")
    var s = LogicalType.parse("STRUCT(a INTEGER, b VARCHAR[])")
    var kids = s.child_types()
    assert_equal(len(kids), 2)
    assert_equal(kids[0][0], "a")
    assert_equal(String(kids[1][1]), "VARCHAR[]")
    assert_true(LogicalType.parse("INTEGER") == LogicalType(TypeId.INTEGER))
    assert_false(LogicalType.parse("INTEGER") == LogicalType(TypeId.BIGINT))
    var e = LogicalType.parse("ENUM('a', 'b', 'c')")
    assert_equal(len(e.enum_values()), 3)
    assert_equal(e.enum_values()[2], "c")


# ── Reading every scalar type ────────────────────────────────────


def test_read_numbers() raises:
    var con = Database().connect()
    var c = _one(
        con,
        "SELECT true, -8::TINYINT, -16::SMALLINT, -32::INTEGER, -64::BIGINT,"
        " 8::UTINYINT, 16::USMALLINT, 32::UINTEGER, 64::UBIGINT,"
        " 1.5::FLOAT, 2.25::DOUBLE,"
        " (-170141183460469231731687303715884105727)::HUGEINT, 340282366920938463463374607431768211455::UHUGEINT,"
        " 64::BIGINT",
    )
    ref ch = c.chunk(0)
    assert_equal(ch.get[Bool](col=0, row=0), True)
    assert_equal(ch.get[Int8](col=1, row=0), -8)
    assert_equal(ch.get[Int16](col=2, row=0), -16)
    assert_equal(ch.get[Int32](col=3, row=0), -32)
    assert_equal(ch.get[Int64](col=4, row=0), -64)
    assert_equal(ch.get[UInt8](col=5, row=0), 8)
    assert_equal(ch.get[UInt16](col=6, row=0), 16)
    assert_equal(ch.get[UInt32](col=7, row=0), 32)
    assert_equal(ch.get[UInt64](col=8, row=0), 64)
    assert_equal(ch.get[Float32](col=9, row=0), 1.5)
    assert_equal(ch.get[Float64](col=10, row=0), 2.25)
    assert_equal(ch.get[Int128](col=11, row=0), Int128(-170141183460469231731687303715884105727))
    assert_equal(ch.get[UInt128](col=12, row=0), UInt128(340282366920938463463374607431768211455))
    assert_equal(ch.get[Int](col=13, row=0), 64)


def test_read_strings_and_bytes() raises:
    var con = Database().connect()
    var c = _one(
        con,
        "SELECT 'short', 'a string longer than twelve bytes', ''::VARCHAR,"
        " '\\x00\\xFFab'::BLOB, '10110'::BIT, 'b'::ENUM('a', 'b', 'c')",
    )
    ref ch = c.chunk(0)
    assert_equal(ch.get[String](col=0, row=0), "short")
    assert_equal(ch.get[String](col=1, row=0), "a string longer than twelve bytes")
    assert_equal(ch.get[String](col=2, row=0), "")
    var blob = ch.get[Blob](col=3, row=0)
    assert_equal(len(blob), 4)
    assert_equal(blob.data[0], 0)
    assert_equal(blob.data[1], 255)
    assert_equal(String(ch.get[Bit](col=4, row=0)), "10110")
    assert_equal(ch.get[String](col=5, row=0), "b")


def test_read_large_enum() raises:
    """ENUM storage width follows the dictionary size (u8 / u16)."""
    var con = Database().connect()
    _ = con.execute("CREATE TYPE big AS ENUM (SELECT 'v' || range FROM range(300))")
    var c = _one(con, "SELECT 'v299'::big, 'v0'::big")
    assert_equal(c.chunk(0).get[String](col=0, row=0), "v299")
    assert_equal(c.chunk(0).get[String](col=1, row=0), "v0")


def test_read_temporal() raises:
    var con = Database().connect()
    var c = _one(
        con,
        "SELECT DATE '1970-01-11', TIME '00:00:01', '00:00:00.000000007'::TIME_NS,"
        " TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP_S '1970-01-01 00:00:02',"
        " TIMESTAMP_MS '1970-01-01 00:00:00.003', TIMESTAMP_NS '1970-01-01 00:00:00.000000004',"
        " TIMESTAMPTZ '1970-01-01 00:00:05+00', '1970-01-01 00:00:00.000000006+00'::TIMESTAMPTZ_NS,"
        " INTERVAL '1 month 2 days 3 microseconds', '01:02:03+04:00'::TIMETZ",
    )
    ref ch = c.chunk(0)
    assert_equal(ch.get[Date](col=0, row=0).days, 10)
    assert_equal(ch.get[Time](col=1, row=0).micros, 1_000_000)
    assert_equal(ch.get[TimeNS](col=2, row=0).nanos, 7)
    assert_equal(ch.get[Timestamp](col=3, row=0).micros, 1_000_000)
    assert_equal(ch.get[TimestampS](col=4, row=0).seconds, 2)
    assert_equal(ch.get[TimestampMS](col=5, row=0).millis, 3)
    assert_equal(ch.get[TimestampNS](col=6, row=0).nanos, 4)
    assert_equal(ch.get[TimestampTZ](col=7, row=0).micros, 5_000_000)
    assert_equal(ch.get[TimestampTZNS](col=8, row=0).nanos, 6)
    var i = ch.get[Interval](col=9, row=0)
    assert_equal(i.months, 1)
    assert_equal(i.days, 2)
    assert_equal(i.micros, 3)
    # TIME_TZ stores micros in the high 40 bits and a biased offset in the low 24.
    var tz = ch.get[TimeTZ](col=10, row=0)
    assert_equal(Int64(tz.bits >> 24), 3_723_000_000)


def test_read_decimal_widths() raises:
    """DECIMAL storage depends on the width: int16, int32, int64, int128."""
    var con = Database().connect()
    var c = _one(
        con,
        "SELECT 12.3::DECIMAL(4,1), -1234567.89::DECIMAL(9,2),"
        " 123456789012345.678::DECIMAL(18,3), 1234567890123456789012345678.0123456789::DECIMAL(38,10)",
    )
    ref ch = c.chunk(0)
    var d1 = ch.get[Decimal](col=0, row=0)
    assert_equal(Int(d1.width), 4)
    assert_equal(Int(d1.scale), 1)
    assert_equal(d1.value(), 123)
    assert_equal(ch.get[Decimal](col=1, row=0).value(), -123456789)
    assert_equal(ch.get[Decimal](col=2, row=0).value(), 123456789012345678)
    assert_equal(ch.get[Decimal](col=3, row=0).value(), Int128(12345678901234567890123456780123456789))


def test_read_uuid() raises:
    var con = Database().connect()
    var c = _one(con, "SELECT '00000000-0000-0000-0000-000000000001'::UUID, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID")
    assert_equal(String(c.chunk(0).get[UUID](col=0, row=0)), "00000000-0000-0000-0000-000000000001")
    assert_equal(String(c.chunk(0).get[UUID](col=1, row=0)), "ffffffff-ffff-ffff-ffff-ffffffffffff")


# ── NULLs, type checks, many rows ────────────────────────────────


def test_nulls() raises:
    var con = Database().connect()
    var c = _one(con, "SELECT * FROM (VALUES (1), (NULL), (3)) t(x)")
    var col = c.chunk(0).column[Int32](0)
    assert_equal(len(col), 3)
    assert_true(col.is_valid(0))
    assert_false(col.is_valid(1))
    assert_true(col.get(1) is None)
    assert_equal(col.get(2).value(), 3)
    var raised = False
    try:
        _ = col[1]
    except e:
        raised = True
        assert_true("NULL" in e.message, e.message)
    assert_true(raised)


def test_type_mismatch() raises:
    var con = Database().connect()
    var c = _one(con, "SELECT 'text' AS name")
    var error = Optional[DuckDBError]()
    try:
        _ = c.chunk(0).column[Int64](0)
    except e:
        error = e^
    assert_true(error, "expected a type mismatch")
    ref e = error.value()
    assert_true(e.code == ErrorCode.TYPE_MISMATCH, String(e))
    assert_true("name" in e.message and "VARCHAR" in e.message, e.message)


def test_many_chunks() raises:
    var con = Database().connect()
    var r = con.execute("SELECT range AS i FROM range(10000)").fetchall()
    assert_true(r.chunk_count() > 1)
    assert_equal(len(r), 10000)
    var total = 0
    var count = 0
    for c in range(r.chunk_count()):
        var col = r.chunk(c).column[Int64](0)
        for i in range(len(col)):
            total += Int(col[i])
            count += 1
    assert_equal(count, 10000)
    assert_equal(total, 9999 * 10000 // 2)


# ── Constant and sequence vectors (built directly; results are always flat) ──


def _built_chunk(type: LogicalType) raises -> Chunk:
    var err = null_error_info()
    var chunk = null[duckdb_v2_data_chunk_handle]()
    var h = type.handle()
    check(lib().duckdb_v2_data_chunk_create(builtin_factory(), Pointer(to=h), 1, Pointer(to=chunk), Pointer(to=err)), err)
    return Chunk(_owned=chunk, schema=ArcPointer(_Schema(["c"], [type.copy()])))


def _vector(chunk: Chunk) raises -> duckdb_v2_vector_handle:
    var err = null_error_info()
    var vec = null[duckdb_v2_vector_handle]()
    check(lib().duckdb_v2_data_chunk_get_vector(chunk._handle, 0, Pointer(to=vec), Pointer(to=err)), err)
    return vec


def test_constant_vector() raises:
    var chunk = _built_chunk(LogicalType(TypeId.BIGINT))
    var err = null_error_info()
    var v = Value(Int64(99))
    check(lib().duckdb_v2_vector_make_constant(_vector(chunk), v.handle(), 5, Pointer(to=err)), err)
    _ = v^  # `v.handle()` is a raw handle: keep the owner alive until the call returned
    chunk._size = 5
    var col = chunk.column[Int64](0)
    for i in range(5):
        assert_equal(col[i], 99)


def test_sequence_vector() raises:
    var chunk = _built_chunk(LogicalType(TypeId.BIGINT))
    var err = null_error_info()
    check(lib().duckdb_v2_vector_make_sequence(_vector(chunk), 10, 3, 4, Pointer(to=err)), err)
    chunk._size = 4
    var col = chunk.column[Int64](0)  # flattened by the reader
    assert_equal(col[0], 10)
    assert_equal(col[3], 19)


# ── Values ────────────────────────────────────────────────────────


def test_value_roundtrips() raises:
    _roundtrip(True)
    _roundtrip(Int8(-8))
    _roundtrip(Int16(-16))
    _roundtrip(Int32(-32))
    _roundtrip(Int64(-64))
    _roundtrip(UInt8(8))
    _roundtrip(UInt64(64))
    _roundtrip(Float32(1.5))
    _roundtrip(Float64(2.25))
    _roundtrip(Int128(-170141183460469231731687303715884105727))
    _roundtrip(UInt128(340282366920938463463374607431768211455))
    _roundtrip(String("a string longer than twelve bytes"))
    _roundtrip(Date(10))
    _roundtrip(Time(1_000_000))
    _roundtrip(TimeNS(7))
    _roundtrip(Timestamp(1_000_000))
    _roundtrip(TimestampS(2))
    _roundtrip(TimestampMS(3))
    _roundtrip(TimestampNS(4))
    _roundtrip(TimestampTZ(5))
    _roundtrip(TimestampTZNS(6))
    _roundtrip(Interval(1, 2, 3))
    _roundtrip(Decimal(18, 3, Int128(123456)))
    _roundtrip(UUID(internal=Int128(1)))
    _roundtrip(Blob([UInt8(0), 255, 97]))
    _roundtrip(Bit("10110"))


def test_value_null_and_conversion() raises:
    var n = Value.null(LogicalType(TypeId.INTEGER))
    assert_true(n.is_null())
    assert_true(n.get_optional[Int32]() is None)
    assert_equal(String(n), "NULL")
    # Getters convert through DuckDB's default cast.
    assert_equal(Value(Int32(7)).get[Int64](), 7)
    assert_equal(String(Value(Int64(42)).type()), "BIGINT")


def test_date_time_components_match_duckdb() raises:
    # The value structs compute calendar fields in Mojo (no DuckDB call).
    var con = Database().connect()
    var r = con.execute(
        "SELECT d, year(d), month(d), day(d) FROM"
        " (SELECT DATE '0001-01-01' - 800000 + (i * 37)::INTEGER AS d FROM range(40000) t(i))"
    ).fetchall()
    for i in range(len(r)):
        var d = r.get[Date](col=0, row=i)
        assert_equal(Int(d.year()), Int(r.get[Int64](col=1, row=i)))
        assert_equal(Int(d.month()), Int(r.get[Int64](col=2, row=i)))
        assert_equal(Int(d.day()), Int(r.get[Int64](col=3, row=i)))
        assert_true(Date(d.year(), d.month(), d.day()) == d)
    var ts = con.execute("SELECT TIMESTAMP '1969-12-31 23:59:58.5'").fetchall().get[Timestamp](col=0, row=0)
    assert_equal(String(ts), "1969-12-31 23:59:58.5")
    var tz = con.execute("SELECT TIMETZ '12:00:00+02'").fetchall().get[TimeTZ](col=0, row=0)
    assert_true(tz == TimeTZ(micros=12 * 3600 * 1_000_000, offset=7200))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
