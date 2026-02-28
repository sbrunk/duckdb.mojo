"""Tests for duckdb_type conversions."""

from duckdb.duckdb_type import (
    Date,
    Time,
    Timestamp,
    TimestampS,
    TimestampMS,
    TimestampNS,
    TimestampTZ,
    TimeTZ,
    Interval,
    Decimal,
    UUID,
    TimeNS,
)
from testing import assert_equal, assert_true, assert_false, assert_almost_equal
from testing.suite import TestSuite
from math import abs as math_abs


# ─── Decimal conversions ─────────────────────────────────────────


def test_decimal_to_float64():
    """Decimal.to_float64() converts correctly."""
    # 12345 with scale=2 → 123.45
    var d = Decimal(10, 2, Int128(12345))
    var f = d.to_float64()
    assert_almost_equal(f, 123.45, atol=1e-10)


def test_decimal_to_float64_negative():
    """Decimal.to_float64() handles negative values."""
    var d = Decimal(10, 3, Int128(-9876))
    var f = d.to_float64()
    assert_almost_equal(f, -9.876, atol=1e-10)


def test_decimal_to_float64_zero_scale():
    """Decimal.to_float64() with scale=0 returns integer value."""
    var d = Decimal(10, 0, Int128(42))
    var f = d.to_float64()
    assert_almost_equal(f, 42.0, atol=1e-10)


def test_decimal_to_float32():
    """Decimal.to_float32() converts correctly."""
    var d = Decimal(10, 2, Int128(12345))
    var f = d.to_float32()
    assert_almost_equal(f, 123.45, atol=1e-4)


def test_decimal_from_float64():
    """Decimal(width, scale, Float64) encodes correctly."""
    var d = Decimal(10, 2, Float64(123.45))
    assert_equal(d.width, UInt8(10))
    assert_equal(d.scale, UInt8(2))
    assert_equal(d.value(), Int128(12345))


def test_decimal_from_float64_negative():
    """Decimal(width, scale, Float64) handles negative values."""
    var d = Decimal(10, 3, Float64(-9.876))
    assert_equal(d.value(), Int128(-9876))


def test_decimal_from_float32():
    """Decimal(width, scale, Float32) encodes correctly."""
    var d = Decimal(10, 2, Float32(123.45))
    assert_equal(d.width, UInt8(10))
    assert_equal(d.scale, UInt8(2))
    assert_equal(d.value(), Int128(12345))


def test_decimal_roundtrip_float64():
    """Decimal → Float64 → Decimal round-trips."""
    var original = Decimal(18, 4, Int128(123456789))
    var f = original.to_float64()
    var restored = Decimal(18, 4, f)
    assert_equal(original.value(), restored.value())


def test_decimal_equatable():
    """Decimal equality and inequality."""
    var a = Decimal(10, 2, Int128(12345))
    var b = Decimal(10, 2, Int128(12345))
    var c = Decimal(10, 2, Int128(99999))
    var d = Decimal(10, 3, Int128(12345))  # different scale
    assert_true(a == b)
    assert_false(a != b)
    assert_true(a != c)
    assert_true(a != d)


# ─── Timestamp conversions ───────────────────────────────────────


def test_timestamp_to_seconds():
    """Timestamp.to_seconds() converts micros to seconds."""
    var ts = Timestamp(1_500_000)  # 1.5 seconds
    var s = ts.to_seconds()
    assert_almost_equal(s, 1.5, atol=1e-10)


def test_timestamp_from_seconds():
    """Timestamp(seconds=) converts seconds to micros."""
    var ts = Timestamp(seconds=1.5)
    assert_equal(ts.micros, Int64(1_500_000))


def test_timestamp_to_from_seconds_roundtrip():
    """Timestamp → seconds → Timestamp round-trips."""
    var original = Timestamp(1_234_567_890)
    var seconds = original.to_seconds()
    var restored = Timestamp(seconds=seconds)
    assert_equal(original.micros, restored.micros)


def test_timestamp_to_timestamp_s():
    """Timestamp.to_timestamp_s() truncates to seconds."""
    var ts = Timestamp(2_500_000)  # 2.5 seconds → 2 seconds
    var ts_s = ts.to_timestamp_s()
    assert_equal(ts_s.seconds, Int64(2))


def test_timestamp_to_timestamp_ms():
    """Timestamp.to_timestamp_ms() truncates to milliseconds."""
    var ts = Timestamp(2_500_000)  # 2500 ms
    var ts_ms = ts.to_timestamp_ms()
    assert_equal(ts_ms.millis, Int64(2500))


def test_timestamp_to_timestamp_ns():
    """Timestamp.to_timestamp_ns() multiplies to nanoseconds."""
    var ts = Timestamp(2_500_000)
    var ts_ns = ts.to_timestamp_ns()
    assert_equal(ts_ns.nanos, Int64(2_500_000_000))


def test_timestamp_s_to_timestamp():
    """TimestampS.to_timestamp() converts to micros."""
    var ts_s = TimestampS(5)
    var ts = ts_s.to_timestamp()
    assert_equal(ts.micros, Int64(5_000_000))


def test_timestamp_ms_to_timestamp():
    """TimestampMS.to_timestamp() converts to micros."""
    var ts_ms = TimestampMS(1500)
    var ts = ts_ms.to_timestamp()
    assert_equal(ts.micros, Int64(1_500_000))


def test_timestamp_ns_to_timestamp():
    """TimestampNS.to_timestamp() converts to micros (truncates)."""
    var ts_ns = TimestampNS(1_500_999)
    var ts = ts_ns.to_timestamp()
    assert_equal(ts.micros, Int64(1500))  # 1_500_999 // 1000 = 1500


def test_timestamp_tz_to_timestamp():
    """TimestampTZ.to_timestamp() converts to plain Timestamp."""
    var ts_tz = TimestampTZ(9_999_999)
    var ts = ts_tz.to_timestamp()
    assert_equal(ts.micros, Int64(9_999_999))


def test_timestamp_cross_conversion_roundtrip():
    """Timestamp → TimestampS → Timestamp preserves seconds."""
    var original = Timestamp(3_000_000)  # exactly 3 seconds
    var via_s = original.to_timestamp_s().to_timestamp()
    assert_equal(original.micros, via_s.micros)


def test_timestamp_cross_conversion_ms_roundtrip():
    """Timestamp → TimestampMS → Timestamp preserves milliseconds."""
    var original = Timestamp(3_500_000)  # exactly 3500 ms
    var via_ms = original.to_timestamp_ms().to_timestamp()
    assert_equal(original.micros, via_ms.micros)


def test_timestamp_cross_conversion_ns_roundtrip():
    """Timestamp → TimestampNS → Timestamp preserves microseconds."""
    var original = Timestamp(3_500_000)
    var via_ns = original.to_timestamp_ns().to_timestamp()
    assert_equal(original.micros, via_ns.micros)


# ─── Time conversions ────────────────────────────────────────────


def test_time_to_seconds():
    """Time.to_seconds() converts micros since midnight to seconds."""
    # 1 hour = 3_600_000_000 micros
    var t = Time(3_600_000_000)
    var s = t.to_seconds()
    assert_almost_equal(s, 3600.0, atol=1e-10)


def test_time_to_seconds_fractional():
    """Time.to_seconds() handles fractional seconds."""
    var t = Time(1_500_000)  # 1.5 seconds
    var s = t.to_seconds()
    assert_almost_equal(s, 1.5, atol=1e-10)


# ─── Interval conversions ────────────────────────────────────────


def test_interval_to_total_seconds():
    """Interval.to_total_seconds() approximates total seconds."""
    # 1 day = 86400 seconds
    var iv = Interval(0, 1, 0)
    var s = iv.to_total_seconds()
    assert_almost_equal(s, 86400.0, atol=1e-6)


def test_interval_to_total_seconds_with_months():
    """Interval.to_total_seconds() uses 30-day month approximation."""
    # 1 month ≈ 30 days = 2_592_000 seconds
    var iv = Interval(1, 0, 0)
    var s = iv.to_total_seconds()
    assert_almost_equal(s, 2_592_000.0, atol=1e-6)


def test_interval_to_total_seconds_combined():
    """Interval.to_total_seconds() with months, days, and micros."""
    # 1 month + 2 days + 500_000 micros (0.5 seconds)
    var iv = Interval(1, 2, 500_000)
    var expected = (30.0 + 2.0) * 86400.0 + 0.5
    var s = iv.to_total_seconds()
    assert_almost_equal(s, expected, atol=1e-6)


def test_interval_writable():
    """Interval is Writable via String.write()."""
    var iv = Interval(1, 2, 3)
    var s = String.write(iv)
    assert_true("months: 1" in s)
    assert_true("days: 2" in s)
    assert_true("micros: 3" in s)


def test_interval_equatable():
    """Interval equality."""
    var a = Interval(1, 2, 3)
    var b = Interval(1, 2, 3)
    var c = Interval(1, 2, 4)
    assert_true(a == b)
    assert_true(a != c)


# ─── UUID internals ──────────────────────────────────────────────


def test_uuid_internal_roundtrip():
    """UUID → internal → UUID roundtrip."""
    var u = UUID(UInt128(0x0123456789ABCDEF))
    var internal = u._to_internal()
    var restored = UUID(internal=internal)
    assert_equal(u.value, restored.value)


def test_uuid_internal_roundtrip_large():
    """UUID → internal → UUID roundtrip with large value."""
    var u = UUID(UInt128(340282366920938463463374607431768211455))  # max UInt128
    var internal = u._to_internal()
    var restored = UUID(internal=internal)
    assert_equal(u.value, restored.value)


def test_uuid_equatable():
    """UUID equality."""
    var a = UUID(UInt128(42))
    var b = UUID(UInt128(42))
    var c = UUID(UInt128(99))
    assert_true(a == b)
    assert_true(a != c)


# ─── TimeNS conversions ──────────────────────────────────────────


def test_time_ns_to_seconds():
    """TimeNS.to_seconds() converts correctly."""
    var t = TimeNS(1_500_000_000)  # 1.5 seconds
    assert_almost_equal(t.to_seconds(), 1.5)


def test_time_ns_to_time():
    """TimeNS.to_time() converts to microseconds."""
    var t = TimeNS(1_500_000_000)  # 1.5 seconds = 1_500_000 micros
    assert_equal(t.to_time(), Time(1_500_000))


def test_time_ns_equatable():
    """TimeNS equality."""
    var a = TimeNS(42)
    var b = TimeNS(42)
    var c = TimeNS(99)
    assert_true(a == b)
    assert_true(a != c)


def test_time_ns_repr():
    """TimeNS repr."""
    var t = TimeNS(12345)
    assert_equal(repr(t), "TimeNS(12345)")


# ─── run_suite ────────────────────────────────────────────────────


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
