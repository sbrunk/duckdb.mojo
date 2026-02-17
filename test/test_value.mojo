from testing import assert_equal, assert_true, assert_false
from duckdb import DuckDBValue
from duckdb._libduckdb import *
from duckdb.duckdb_type import Decimal
from math import isnan
from testing.suite import TestSuite
from collections import List
from duckdb.logical_type import enum_type


fn test_null_value() raises:
    """Test creating and checking NULL values."""
    var null_val = DuckDBValue.null()
    assert_true(null_val.is_null(), "NULL value should be null")


fn test_bool_values() raises:
    """Test creating and extracting boolean values."""
    var true_val = DuckDBValue.from_bool(True)
    var false_val = DuckDBValue.from_bool(False)

    assert_false(true_val.is_null(), "Boolean value should not be null")
    assert_true(true_val.as_bool(), "True value should be True")
    assert_false(false_val.as_bool(), "False value should be False")


fn test_int8_values() raises:
    """Test creating and extracting int8 values."""
    var val_pos = DuckDBValue.from_int8(42)
    var val_neg = DuckDBValue.from_int8(-42)
    var val_min = DuckDBValue.from_int8(-128)
    var val_max = DuckDBValue.from_int8(127)

    assert_equal(val_pos.as_int8(), 42, "Positive int8 should match")
    assert_equal(val_neg.as_int8(), -42, "Negative int8 should match")
    assert_equal(val_min.as_int8(), -128, "Min int8 should match")
    assert_equal(val_max.as_int8(), 127, "Max int8 should match")


fn test_uint8_values() raises:
    """Test creating and extracting uint8 values."""
    var val_zero = DuckDBValue.from_uint8(0)
    var val_mid = DuckDBValue.from_uint8(128)
    var val_max = DuckDBValue.from_uint8(255)

    assert_equal(val_zero.as_uint8(), 0, "Zero uint8 should match")
    assert_equal(val_mid.as_uint8(), 128, "Mid uint8 should match")
    assert_equal(val_max.as_uint8(), 255, "Max uint8 should match")


fn test_int16_values() raises:
    """Test creating and extracting int16 values."""
    var val_pos = DuckDBValue.from_int16(1000)
    var val_neg = DuckDBValue.from_int16(-1000)
    var val_min = DuckDBValue.from_int16(-32768)
    var val_max = DuckDBValue.from_int16(32767)

    assert_equal(val_pos.as_int16(), 1000, "Positive int16 should match")
    assert_equal(val_neg.as_int16(), -1000, "Negative int16 should match")
    assert_equal(val_min.as_int16(), -32768, "Min int16 should match")
    assert_equal(val_max.as_int16(), 32767, "Max int16 should match")


fn test_uint16_values() raises:
    """Test creating and extracting uint16 values."""
    var val_zero = DuckDBValue.from_uint16(0)
    var val_mid = DuckDBValue.from_uint16(32768)
    var val_max = DuckDBValue.from_uint16(65535)

    assert_equal(val_zero.as_uint16(), 0, "Zero uint16 should match")
    assert_equal(val_mid.as_uint16(), 32768, "Mid uint16 should match")
    assert_equal(val_max.as_uint16(), 65535, "Max uint16 should match")


fn test_int32_values() raises:
    """Test creating and extracting int32 values."""
    var val_pos = DuckDBValue.from_int32(100000)
    var val_neg = DuckDBValue.from_int32(-100000)
    var val_min = DuckDBValue.from_int32(-2147483648)
    var val_max = DuckDBValue.from_int32(2147483647)

    assert_equal(val_pos.as_int32(), 100000, "Positive int32 should match")
    assert_equal(val_neg.as_int32(), -100000, "Negative int32 should match")
    assert_equal(val_min.as_int32(), -2147483648, "Min int32 should match")
    assert_equal(val_max.as_int32(), 2147483647, "Max int32 should match")


fn test_uint32_values() raises:
    """Test creating and extracting uint32 values."""
    var val_zero = DuckDBValue.from_uint32(0)
    var val_mid = DuckDBValue.from_uint32(2147483648)
    var val_max = DuckDBValue.from_uint32(4294967295)

    assert_equal(val_zero.as_uint32(), 0, "Zero uint32 should match")
    assert_equal(val_mid.as_uint32(), 2147483648, "Mid uint32 should match")
    assert_equal(val_max.as_uint32(), 4294967295, "Max uint32 should match")


fn test_int64_values() raises:
    """Test creating and extracting int64 values."""
    var val_pos = DuckDBValue.from_int64(9223372036854775807)
    var val_neg = DuckDBValue.from_int64(-9223372036854775808)
    var val_zero = DuckDBValue.from_int64(0)

    assert_equal(
        val_pos.as_int64(), 9223372036854775807, "Max int64 should match"
    )
    assert_equal(
        val_neg.as_int64(), -9223372036854775808, "Min int64 should match"
    )
    assert_equal(val_zero.as_int64(), 0, "Zero int64 should match")


fn test_uint64_values() raises:
    """Test creating and extracting uint64 values."""
    var val_zero = DuckDBValue.from_uint64(0)
    var val_mid = DuckDBValue.from_uint64(9223372036854775808)
    var val_large = DuckDBValue.from_uint64(18446744073709551615)

    assert_equal(val_zero.as_uint64(), 0, "Zero uint64 should match")
    assert_equal(
        val_mid.as_uint64(), 9223372036854775808, "Mid uint64 should match"
    )
    assert_equal(
        val_large.as_uint64(), 18446744073709551615, "Max uint64 should match"
    )


fn test_hugeint_values() raises:
    """Test creating and extracting hugeint values."""
    # Construct hugeint from upper (int64) and lower (uint64) parts manually
    # duckdb_hugeint is aliased to Int128
    
    # Max hugeint: upper = MAX_INT64, lower = MAX_UINT64
    var max_upper = Int128(9223372036854775807)
    var max_lower = Int128(18446744073709551615)
    var val_pos_int128 = (max_upper << 64) | max_lower
    var val_pos = DuckDBValue.from_hugeint(val_pos_int128)

    # Min hugeint: upper = MIN_INT64, lower = 0
    var min_upper = Int128(-9223372036854775808)
    var val_neg_int128 = (min_upper << 64) 
    var val_neg = DuckDBValue.from_hugeint(val_neg_int128)

    var val_zero = DuckDBValue.from_hugeint(0)
    
    # Additional test cases
    var val_small_pos_int128 = Int128(123456789)
    var val_small_pos = DuckDBValue.from_hugeint(val_small_pos_int128)
    
    var val_small_neg_int128 = Int128(-123456789)
    var val_small_neg = DuckDBValue.from_hugeint(val_small_neg_int128)
    
    # Just above 2^64
    var val_above_64_int128 = (Int128(1) << 64) + 1
    var val_above_64 = DuckDBValue.from_hugeint(val_above_64_int128)

    # Just below -2^63 (min int64)
    var val_below_min_64_int128 = Int128(-9223372036854775808) - 1
    var val_below_min_64 = DuckDBValue.from_hugeint(val_below_min_64_int128)

    assert_equal(
        val_pos.as_hugeint(), val_pos_int128, "Max hugeint should match"
    )
    assert_equal(
        val_neg.as_hugeint(), val_neg_int128, "Min hugeint should match"
    )
    assert_equal(val_zero.as_hugeint(), 0, "Zero hugeint should match")
    assert_equal(val_small_pos.as_hugeint(), val_small_pos_int128, "Small positive hugeint should match")
    assert_equal(val_small_neg.as_hugeint(), val_small_neg_int128, "Small negative hugeint should match")
    assert_equal(val_above_64.as_hugeint(), val_above_64_int128, "Above 2^64 hugeint should match")
    assert_equal(val_below_min_64.as_hugeint(), val_below_min_64_int128, "Below min int64 hugeint should match")


fn test_uhugeint_values() raises:
    """Test creating and extracting uhugeint values."""
    # duckdb_uhugeint is aliased to UInt128
    
    # Max uhugeint: upper = MAX_UINT64, lower = MAX_UINT64
    var max_upper = UInt128(18446744073709551615)
    var max_lower = UInt128(18446744073709551615)
    var val_pos_uint128 = (max_upper << 64) | max_lower
    var val_pos = DuckDBValue.from_uhugeint(val_pos_uint128)
    
    var val_zero = DuckDBValue.from_uhugeint(0)

    # Additional test cases
    var val_small_pos_uint128 = UInt128(123456789)
    var val_small_pos = DuckDBValue.from_uhugeint(val_small_pos_uint128)
    
    # Just above 2^64
    var val_above_64_uint128 = (UInt128(1) << 64) + 1
    var val_above_64 = DuckDBValue.from_uhugeint(val_above_64_uint128)

    assert_equal(
        val_pos.as_uhugeint(), val_pos_uint128, "Max uhugeint should match"
    )
    assert_equal(val_zero.as_uhugeint(), 0, "Zero uhugeint should match")
    assert_equal(val_small_pos.as_uhugeint(), val_small_pos_uint128, "Small positive uhugeint should match")
    assert_equal(val_above_64.as_uhugeint(), val_above_64_uint128, "Above 2^64 uhugeint should match")


fn test_float32_values() raises:
    """Test creating and extracting float32 values."""
    var val_pos = DuckDBValue.from_float32(3.14)
    var val_neg = DuckDBValue.from_float32(-3.14)
    var val_zero = DuckDBValue.from_float32(0.0)

    # Use approximate comparison for floats
    var pos_result = val_pos.as_float32()
    assert_true(
        abs(pos_result - 3.14) < 0.01,
        "Positive float32 should be close to 3.14",
    )

    var neg_result = val_neg.as_float32()
    assert_true(
        abs(neg_result - (-3.14)) < 0.01,
        "Negative float32 should be close to -3.14",
    )

    var zero_result = val_zero.as_float32()
    assert_equal(zero_result, 0.0, "Zero float32 should be exactly 0.0")


fn test_float64_values() raises:
    """Test creating and extracting float64 values."""
    var val_pos = DuckDBValue.from_float64(3.141592653589793)
    var val_neg = DuckDBValue.from_float64(-3.141592653589793)
    var val_zero = DuckDBValue.from_float64(0.0)

    # Use approximate comparison for floats
    var pos_result = val_pos.as_float64()
    assert_true(
        abs(pos_result - 3.141592653589793) < 0.000001,
        "Positive float64 should be close to pi",
    )

    var neg_result = val_neg.as_float64()
    assert_true(
        abs(neg_result - (-3.141592653589793)) < 0.000001,
        "Negative float64 should be close to -pi",
    )

    var zero_result = val_zero.as_float64()
    assert_equal(zero_result, 0.0, "Zero float64 should be exactly 0.0")


fn test_string_values() raises:
    """Test creating and extracting string values."""
    var val_hello = DuckDBValue.from_string("Hello, DuckDB!")
    var val_empty = DuckDBValue.from_string("")
    var val_unicode = DuckDBValue.from_string("Hello 🦆 World")

    assert_equal(val_hello.as_string(), "Hello, DuckDB!", "String should match")
    assert_equal(val_empty.as_string(), "", "Empty string should match")
    assert_equal(
        val_unicode.as_string(), "Hello 🦆 World", "Unicode string should match"
    )


fn test_date_values() raises:
    """Test creating and extracting date values."""
    # Date is stored as days since 1970-01-01
    var date_epoch = duckdb_date(days=0)  # 1970-01-01
    var val_date = DuckDBValue.from_date(date_epoch)

    var result = val_date.as_date()
    assert_equal(result.days, 0, "Epoch date should have 0 days")


fn test_timestamp_values() raises:
    """Test creating and extracting timestamp values."""
    # Timestamp is stored as microseconds since epoch
    # Use a known timestamp value: 1 million microseconds = 1 second
    var ts = duckdb_timestamp(micros=1000000)
    var val_ts = DuckDBValue.from_timestamp(ts)

    var result = val_ts.as_timestamp()

    assert_false(val_ts.is_null(), "Timestamp value should not be null")
    assert_equal(result, ts)


fn test_time_values() raises:
    """Test creating and extracting time values."""
    # Time is stored as microseconds since midnight
    var t = duckdb_time(micros=123456000)
    var val = DuckDBValue.from_time(t)

    var result = val.as_time()

    assert_false(val.is_null(), "Time value should not be null")
    assert_equal(result, t)


fn test_interval_values() raises:
    var interval = Interval(
        months=1, days=2, micros=3000000
    )  # 1 month, 2 days, 3 seconds
    var val_interval = DuckDBValue.from_interval(interval)
    var result = val_interval.as_interval()

    assert_equal(result, interval)


fn test_decimal_values() raises:
    """Test creating and extracting decimal values."""
    # Create a decimal with width 18, scale 3.
    # Value is (internal) 123456 -> 123.456
    var h: Int128 = Int128(123456)
    var dec = Decimal(width=18, scale=3, value=h)
    var val = DuckDBValue.from_decimal(dec)

    var result = val.as_decimal()
    
    assert_equal(result.width, 18, "Width should match")
    assert_equal(result.scale, 3, "Scale should match")
    assert_equal(result.value(), 123456, "Value should match")

fn test_enum_values() raises:
    """Test creating and extracting enum values."""
    # Create logic type enum
    var names: List[String] = ["Apple", "Banana", "Cherry"]
    var t_enum = enum_type(names)
    
    # Create enum value (index 1 = Banana)
    var val = DuckDBValue.from_enum(t_enum, 1)
    
    var result = val.as_enum_value()
    assert_equal(result, 1, "Enum index should be 1")

fn test_blob_values() raises:
    """Test creating and extracting blob values."""
    var data: List[UInt8] = [1, 2, 3, 4, 5]
    var span = Span[UInt8, ImmutAnyOrigin](
        ptr=data.unsafe_ptr(), length=len(data)
    )
    var val_blob = DuckDBValue.from_blob(span)

    # We can't easily extract blob data back out without more complex
    # infrastructure, but we can check it's not null and has the right type
    assert_false(val_blob.is_null(), "Blob value should not be null")


fn test_sql_string_representation() raises:
    """Test SQL string representation of values."""
    print("Testing SQL string representation...")
    var val_int = DuckDBValue.from_int64(42)
    var val_str = DuckDBValue.from_string("Hello")
    var val_null = DuckDBValue.null()

    var int_sql = val_int.to_sql_string()
    var str_sql = val_str.to_sql_string()
    var null_sql = val_null.to_sql_string()

    assert_equal(int_sql, "42", "Int SQL representation should be '42'")
    assert_equal(
        str_sql, "'Hello'", "String SQL representation should be quoted"
    )
    assert_equal(null_sql, "NULL", "NULL SQL representation should be 'NULL'")
    print("  ✓ SQL string representations work correctly")


fn test_type_information() raises:
    """Test getting type information from values."""
    var val_int = DuckDBValue.from_int64(42)
    var val_str = DuckDBValue.from_string("test")
    var val_bool = DuckDBValue.from_bool(True)

    # Note: get_type() has ownership issues (the returned LogicalType shouldn't be destroyed)
    # For now, just verify the values aren't null
    assert_false(val_int.is_null(), "Int value should not be null")
    assert_false(val_str.is_null(), "String value should not be null")
    assert_false(val_bool.is_null(), "Bool value should not be null")


fn test_value_conversions() raises:
    """Test implicit conversions between types."""

    # Int to larger int should work
    var val_int8 = DuckDBValue.from_int8(42)
    assert_equal(val_int8.as_int16(), 42, "Int8 should convert to Int16")
    assert_equal(val_int8.as_int32(), 42, "Int8 should convert to Int32")
    assert_equal(val_int8.as_int64(), 42, "Int8 should convert to Int64")

    # String representation of numbers
    var val_num = DuckDBValue.from_int64(123)
    var num_str = val_num.as_string()
    assert_equal(num_str, "123", "Number should convert to string")


fn test_logical_type_ownership() raises:
    """Test that LogicalType returned from get_type is properly borrowed and tied to the value's lifetime.
    """

    var val = DuckDBValue.from_int64(42)

    # Get borrowed type - this should work and be tied to val's lifetime
    var borrowed_type = val.get_type()
    var type_id = borrowed_type.get_type_id()
    assert_equal(type_id.value, DUCKDB_TYPE_BIGINT, "Type should be BIGINT")

    # The borrowed type should still be valid as long as val is alive
    assert_equal(
        borrowed_type.get_type_id().value,
        DUCKDB_TYPE_BIGINT,
        "Borrowed type should still be valid",
    )


fn test_bit_values() raises:
    """Test creating and extracting bit values."""
    var bit_data: List[UInt8] = [0b10101010, 0b01010101]
    var val = DuckDBValue.from_bit(bit_data)
    var result = val.as_bit()
    assert_equal(len(result), 2)
    assert_equal(result[0], 0b10101010)
    assert_equal(result[1], 0b01010101)


fn test_uuid_values() raises:
    """Test creating and extracting uuid values."""
    var uuid_val = UInt128(12345678901234567890)
    var val = DuckDBValue.from_uuid(uuid_val)
    var result = val.as_uuid()
    assert_equal(result, uuid_val)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
