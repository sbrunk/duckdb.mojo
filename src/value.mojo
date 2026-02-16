from duckdb._libduckdb import *
from duckdb.logical_type import LogicalType
from duckdb.api import DuckDB
from collections import Optional, List


struct DuckDBValue(Movable):
    """A high-level wrapper around a DuckDB value.
    
    `DuckDBValue` provides a safe and ergonomic API for creating and manipulating
    DuckDB values. Values can represent any SQL data type including primitives
    (integers, floats, strings, booleans), temporal types (dates, timestamps, intervals),
    and NULL values.
    
    The value owns its underlying `duckdb_value` pointer and will automatically
    destroy it when it goes out of scope.
    
    Example:
        ```mojo
        # Create values
        from duckdb import DuckDBValue
        var val_int = DuckDBValue.from_int64(42)
        var val_str = DuckDBValue.from_string("Hello, DuckDB!")
        var val_null = DuckDBValue.null()
        
        # Read values
        var i = val_int.as_int64()
        var s = val_str.as_string()
        var is_null = val_null.is_null()
        ```
    """
    var _value: duckdb_value

    fn __init__(out self, value: duckdb_value):
        """Constructs a DuckDBValue from a raw duckdb_value pointer.

        Warning: The DuckDBValue takes ownership of the pointer and will destroy it.
        
        Args:
            value: The duckdb_value pointer to wrap.
        """
        self._value = value

    fn __moveinit__(out self, deinit existing: Self):
        """Move constructor that transfers ownership of the underlying value.
        
        Args:
            existing: The existing DuckDBValue to move from.
        """
        self._value = existing._value

    fn __del__(deinit self):
        """Destroys the value and deallocates all associated memory."""
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_value(UnsafePointer(to=self._value))

    # ===--------------------------------------------------------------------===#
    # Factory methods for creating values
    # ===--------------------------------------------------------------------===#

    @staticmethod
    fn from_string(text: String) -> Self:
        """Creates a value from a string.

        Args:
            text: The string value to wrap.

        Returns:
            A new DuckDBValue containing the string.
        """
        ref libduckdb = DuckDB().libduckdb()
        var text_copy = text
        var c_str = text_copy.as_c_string_slice()
        return Self(libduckdb.duckdb_create_varchar_length(
            c_str.unsafe_ptr(), UInt64(len(text_copy))
        ))

    @staticmethod
    fn from_bool(value: Bool) -> Self:
        """Creates a value from a boolean.

        Args:
            value: The boolean value to wrap.

        Returns:
            A new DuckDBValue containing the boolean.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_bool(value))

    @staticmethod
    fn from_decimal(value: duckdb_decimal) -> Self:
        """Creates a value from a decimal (DECIMAL).

        Args:
            value: The decimal value to wrap.

        Returns:
            A new DuckDBValue containing the decimal.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_decimal(value))

    @staticmethod
    fn from_enum[is_owned: Bool, origin: ImmutOrigin](type: LogicalType[is_owned, origin], value: UInt64) -> Self:
        """Creates a value from an enum (ENUM).

        Args:
            type: The enum logical type.
            value: The enum value (integer representation).

        Returns:
            A new DuckDBValue containing the enum.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_enum_value(type.internal_ptr(), value))

    @staticmethod
    fn from_int8(value: Int8) -> Self:
        """Creates a value from an int8 (TINYINT).

        Args:
            value: The int8 value to wrap.

        Returns:
            A new DuckDBValue containing the tinyint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_int8(value))

    @staticmethod
    fn from_uint8(value: UInt8) -> Self:
        """Creates a value from a uint8 (UTINYINT).

        Args:
            value: The uint8 value to wrap.

        Returns:
            A new DuckDBValue containing the utinyint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uint8(value))

    @staticmethod
    fn from_int16(value: Int16) -> Self:
        """Creates a value from an int16 (SMALLINT).

        Args:
            value: The int16 value to wrap.

        Returns:
            A new DuckDBValue containing the smallint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_int16(value))

    @staticmethod
    fn from_uint16(value: UInt16) -> Self:
        """Creates a value from a uint16 (USMALLINT).

        Args:
            value: The uint16 value to wrap.

        Returns:
            A new DuckDBValue containing the usmallint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uint16(value))

    @staticmethod
    fn from_int32(value: Int32) -> Self:
        """Creates a value from an int32 (INTEGER).

        Args:
            value: The int32 value to wrap.

        Returns:
            A new DuckDBValue containing the integer.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_int32(value))

    @staticmethod
    fn from_uint32(value: UInt32) -> Self:
        """Creates a value from a uint32 (UINTEGER).

        Args:
            value: The uint32 value to wrap.

        Returns:
            A new DuckDBValue containing the uinteger.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uint32(value))

    @staticmethod
    fn from_int64(value: Int64) -> Self:
        """Creates a value from an int64 (BIGINT).

        Args:
            value: The int64 value to wrap.

        Returns:
            A new DuckDBValue containing the bigint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_int64(value))

    @staticmethod
    fn from_uint64(value: UInt64) -> Self:
        """Creates a value from a uint64 (UBIGINT).

        Args:
            value: The uint64 value to wrap.

        Returns:
            A new DuckDBValue containing the ubigint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uint64(value))

    @staticmethod
    fn from_hugeint(value: duckdb_hugeint) -> Self:
        """Creates a value from a hugeint (HUGEINT).

        Args:
            value: The hugeint value to wrap.

        Returns:
            A new DuckDBValue containing the hugeint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_hugeint(value))

    @staticmethod
    fn from_uhugeint(value: duckdb_uhugeint) -> Self:
        """Creates a value from a uhugeint (UHUGEINT).

        Args:
            value: The uhugeint value to wrap.

        Returns:
            A new DuckDBValue containing the uhugeint.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uhugeint(value))

    @staticmethod
    fn from_float32(value: Float32) -> Self:
        """Creates a value from a float32 (FLOAT).

        Args:
            value: The float32 value to wrap.

        Returns:
            A new DuckDBValue containing the float.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_float(value))

    @staticmethod
    fn from_float64(value: Float64) -> Self:
        """Creates a value from a float64 (DOUBLE).

        Args:
            value: The float64 value to wrap.

        Returns:
            A new DuckDBValue containing the double.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_double(value))

    @staticmethod
    fn from_date(value: duckdb_date) -> Self:
        """Creates a value from a date.

        Args:
            value: The date value to wrap.

        Returns:
            A new DuckDBValue containing the date.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_date(value))

    @staticmethod
    fn from_timestamp(value: duckdb_timestamp) -> Self:
        """Creates a value from a timestamp.

        Args:
            value: The timestamp value to wrap.

        Returns:
            A new DuckDBValue containing the timestamp.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_timestamp(value))

    @staticmethod
    fn from_time(value: duckdb_time) -> Self:
        """Creates a value from a time.

        Args:
            value: The time value to wrap.

        Returns:
            A new DuckDBValue containing the time.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_time(value))

    @staticmethod
    fn from_interval(value: Interval) -> Self:
        """Creates a value from an interval.

        Args:
            value: The interval value to wrap.

        Returns:
            A new DuckDBValue containing the interval.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_interval(UnsafePointer(to=value).bitcast[duckdb_interval]()[]))

    @staticmethod
    fn from_blob(data: Span[UInt8, ImmutAnyOrigin]) -> Self:
        """Creates a value from binary data (BLOB).

        Args:
            data: The binary data as a span of bytes.

        Returns:
            A new DuckDBValue containing the blob.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_blob(
            data.unsafe_ptr(), UInt64(len(data))
        ))

    @staticmethod
    fn from_bit(data: Span[UInt8, ImmutAnyOrigin]) -> Self:
        """Creates a value from a BIT string.

        Args:
            data: The bit data as a span of bytes.

        Returns:
            A new DuckDBValue containing the bit string.
        """
        ref libduckdb = DuckDB().libduckdb()
        var bit_val = duckdb_bit(
            UnsafePointer[UInt8, MutExternalOrigin](unsafe_from_address=Int(data.unsafe_ptr())),
            idx_t(len(data))
        )
        return Self(libduckdb.duckdb_create_bit(bit_val))

    @staticmethod
    fn from_uuid(value: UInt128) -> Self:
        """Creates a value from a UUID.

        Args:
            value: The UUID value as UInt128.

        Returns:
            A new DuckDBValue containing the UUID.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_uuid(value))

    @staticmethod
    fn null() -> Self:
        """Creates a NULL value.

        Returns:
            A new DuckDBValue representing SQL NULL.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_null_value())

    # ===--------------------------------------------------------------------===#
    # Getter methods for extracting values
    # ===--------------------------------------------------------------------===#

    fn is_null(self) -> Bool:
        """Checks if this value is SQL NULL.

        Returns:
            True if the value is NULL, False otherwise.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_is_null_value(self._value)

    fn as_bool(self) -> Bool:
        """Extracts the boolean value.

        Returns:
            The boolean value, or False if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_bool(self._value)

    fn as_enum_value(self) -> UInt64:
        """Extracts the enum value (index).

        Returns:
            The enum value (index).
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_enum_value(self._value)

    fn as_decimal(self) -> duckdb_decimal:
        """Extracts the decimal value.

        Returns:
            The decimal value.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_decimal(self._value)

    fn as_int8(self) -> Int8:
        """Extracts the int8 value.

        Returns:
            The int8 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_int8(self._value)

    fn as_uint8(self) -> UInt8:
        """Extracts the uint8 value.

        Returns:
            The uint8 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uint8(self._value)

    fn as_int16(self) -> Int16:
        """Extracts the int16 value.

        Returns:
            The int16 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_int16(self._value)

    fn as_uint16(self) -> UInt16:
        """Extracts the uint16 value.

        Returns:
            The uint16 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uint16(self._value)

    fn as_int32(self) -> Int32:
        """Extracts the int32 value.

        Returns:
            The int32 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_int32(self._value)

    fn as_uint32(self) -> UInt32:
        """Extracts the uint32 value.

        Returns:
            The uint32 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uint32(self._value)

    fn as_int64(self) -> Int64:
        """Extracts the int64 value.

        Returns:
            The int64 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_int64(self._value)

    fn as_uint64(self) -> UInt64:
        """Extracts the uint64 value.

        Returns:
            The uint64 value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uint64(self._value)

    fn as_hugeint(self) -> duckdb_hugeint:
        """Extracts the hugeint value.

        Returns:
            The hugeint value.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_hugeint(self._value)

    fn as_uhugeint(self) -> duckdb_uhugeint:
        """Extracts the uhugeint value.

        Returns:
            The uhugeint value.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uhugeint(self._value)

    fn as_float32(self) -> Float32:
        """Extracts the float32 value.

        Returns:
            The float32 value, or NaN if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_float(self._value)

    fn as_float64(self) -> Float64:
        """Extracts the float64 value.

        Returns:
            The float64 value, or NaN if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_double(self._value)

    fn as_date(self) -> duckdb_date:
        """Extracts the date value.

        Returns:
            The date value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_date(self._value)

    fn as_timestamp(self) -> duckdb_timestamp:
        """Extracts the timestamp value.

        Returns:
            The timestamp value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_timestamp(self._value)

    fn as_time(self) -> duckdb_time:
        """Extracts the time value.

        Returns:
            The time value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_time(self._value)

    fn as_interval(self) -> Interval:
        """Extracts the interval value.

        Returns:
            The interval value, or MinValue if the value cannot be converted.
        """
        ref libduckdb = DuckDB().libduckdb()
        return UnsafePointer(to=libduckdb.duckdb_get_interval(self._value)).bitcast[Interval]()[]

    fn as_blob(self) -> List[UInt8]:
        """Extracts the blob value.

        Returns:
            The blob data as a list of bytes.
        """
        ref libduckdb = DuckDB().libduckdb()
        var blob = libduckdb.duckdb_get_blob(self._value)
        var result = List[UInt8](capacity=Int(blob.size))
        var data_ptr = blob.data.bitcast[UInt8]()
        for i in range(Int(blob.size)):
            result.append(data_ptr[i])
        libduckdb.duckdb_free(blob.data)
        return result^

    fn as_bit(self) -> List[UInt8]:
        """Extracts the bit value.

        Returns:
            The bit data as a list of bytes.
        """
        ref libduckdb = DuckDB().libduckdb()
        var bit_val = libduckdb.duckdb_get_bit(self._value)
        var result = List[UInt8](capacity=Int(bit_val.size))
        for i in range(Int(bit_val.size)):
            result.append(bit_val.data[i])
        libduckdb.duckdb_free(bit_val.data.bitcast[NoneType]())
        return result^

    fn as_uuid(self) -> UInt128:
        """Extracts the UUID value.

        Returns:
            The UUID value as UInt128.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_get_uuid(self._value)

    fn as_string(self) -> String:
        """Gets the string representation of the value.

        The returned string is allocated by DuckDB and owned by this method.
        It will be automatically freed when the String goes out of scope.

        Returns:
            The string representation of the value.
        """
        ref libduckdb = DuckDB().libduckdb()
        var c_str = libduckdb.duckdb_get_varchar(self._value)
        var result = String(unsafe_from_utf8_ptr=c_str)
        libduckdb.duckdb_free(c_str.bitcast[NoneType]())
        return result

    fn to_sql_string(self) -> String:
        """Gets the SQL string representation of the value.

        This formats the value as it would appear in a SQL query
        (e.g., strings are quoted, NULL is rendered as "NULL", etc.).

        Returns:
            The SQL string representation of the value.
        """
        ref libduckdb = DuckDB().libduckdb()
        var c_str = libduckdb.duckdb_value_to_string(self._value)
        var result = String(unsafe_from_utf8_ptr=c_str)
        libduckdb.duckdb_free(c_str.bitcast[NoneType]())
        return result

    fn get_type(ref [_]self: Self) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        """Gets the logical type of this value.

        The returned type is borrowed from the value and will not be destroyed.
        The lifetime of the returned type is tied to this value.

        Returns:
            A borrowed LogicalType that is valid as long as this value exists.
        """
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](libduckdb.duckdb_get_value_type(self._value))
