from duckdb._libduckdb import *
from duckdb.api import _impl


@value
@register_passable("trivial")
struct DuckDBType(
    Stringable, Formattable, CollectionElementNew, EqualityComparable
):
    """Represents DuckDB types."""

    var value: Int32

    alias invalid = DuckDBType(DUCKDB_TYPE_INVALID)
    """invalid"""
    alias boolean = DuckDBType(DUCKDB_TYPE_BOOLEAN)
    """boolean"""
    alias tinyint = DuckDBType(DUCKDB_TYPE_TINYINT)
    """int8"""
    alias smallint = DuckDBType(DUCKDB_TYPE_SMALLINT)
    """int16"""
    alias integer = DuckDBType(DUCKDB_TYPE_INTEGER)
    """int32"""
    alias bigint = DuckDBType(DUCKDB_TYPE_BIGINT)
    """int64"""
    alias utinyint = DuckDBType(DUCKDB_TYPE_UTINYINT)
    """uint8"""
    alias usmallint = DuckDBType(DUCKDB_TYPE_USMALLINT)
    """uint16"""
    alias uinteger = DuckDBType(DUCKDB_TYPE_UINTEGER)
    """uint32"""
    alias ubigint = DuckDBType(DUCKDB_TYPE_UBIGINT)
    """uint64"""
    alias float = DuckDBType(DUCKDB_TYPE_FLOAT)
    """float32"""
    alias double = DuckDBType(DUCKDB_TYPE_DOUBLE)
    """float64"""
    alias timestamp = DuckDBType(DUCKDB_TYPE_TIMESTAMP)
    """duckdb_timestamp, in microseconds"""
    alias date = DuckDBType(DUCKDB_TYPE_DATE)
    """duckdb_date"""
    alias time = DuckDBType(DUCKDB_TYPE_TIME)
    """duckdb_time"""
    alias interval = DuckDBType(DUCKDB_TYPE_INTERVAL)
    """duckdb_interval"""
    alias hugeint = DuckDBType(DUCKDB_TYPE_HUGEINT)
    """duckdb_hugeint"""
    alias uhugeint = DuckDBType(DUCKDB_TYPE_UHUGEINT)
    """duckdb_uhugeint"""
    alias varchar = DuckDBType(DUCKDB_TYPE_VARCHAR)
    """String"""
    alias blob = DuckDBType(DUCKDB_TYPE_BLOB)
    """duckdb_blob"""
    alias decimal = DuckDBType(DUCKDB_TYPE_DECIMAL)
    """decimal"""
    alias timestamp_s = DuckDBType(DUCKDB_TYPE_TIMESTAMP_S)
    """duckdb_timestamp, in seconds"""
    alias timestamp_ms = DuckDBType(DUCKDB_TYPE_TIMESTAMP_MS)
    """duckdb_timestamp, in milliseconds"""
    alias timestamp_ns = DuckDBType(DUCKDB_TYPE_TIMESTAMP_NS)
    """duckdb_timestamp, in nanoseconds"""
    alias enum = DuckDBType(DUCKDB_TYPE_ENUM)
    """enum type, only useful as logical type"""
    alias list = DuckDBType(DUCKDB_TYPE_LIST)
    """list type, only useful as logical type"""
    alias struct_t = DuckDBType(DUCKDB_TYPE_STRUCT)
    """struct type, only useful as logical type"""
    alias map = DuckDBType(DUCKDB_TYPE_MAP)
    """map type, only useful as logical type"""
    alias array = DuckDBType(DUCKDB_TYPE_ARRAY)
    """duckdb_array, only useful as logical type"""
    alias uuid = DuckDBType(DUCKDB_TYPE_UUID)
    """duckdb_hugeint"""
    alias union = DuckDBType(DUCKDB_TYPE_UNION)
    """union type, only useful as logical type"""
    alias bit = DuckDBType(DUCKDB_TYPE_BIT)
    """duckdb_bit"""
    alias time_tz = DuckDBType(DUCKDB_TYPE_TIME_TZ)
    """duckdb_time_tz"""
    alias timestamp_tz = DuckDBType(DUCKDB_TYPE_TIMESTAMP_TZ)
    """duckdb_timestamp"""

    @always_inline
    fn __init__(inout self, *, other: Self):
        """Copy this DuckDBType.

        Args:
            other: The DuckDBType to copy.
        """
        self = other

    @always_inline("nodebug")
    fn __repr__(self) -> String:
        return "DuckDBType." + str(self)

    @always_inline("nodebug")
    fn __str__(self) -> String:
        return String.format_sequence(self)

    fn format_to(self, inout writer: Formatter):
        if self == DuckDBType.invalid:
            return writer.write_str["invalid"]()
        if self == DuckDBType.tinyint:
            return writer.write_str["tinyint"]()
        if self == DuckDBType.boolean:
            return writer.write_str["boolean"]()
        if self == DuckDBType.smallint:
            return writer.write_str["smallint"]()
        if self == DuckDBType.integer:
            return writer.write_str["integer"]()
        if self == DuckDBType.bigint:
            return writer.write_str["bigint"]()
        if self == DuckDBType.utinyint:
            return writer.write_str["utinyint"]()
        if self == DuckDBType.usmallint:
            return writer.write_str["usmallint"]()
        if self == DuckDBType.uinteger:
            return writer.write_str["uinteger"]()
        if self == DuckDBType.ubigint:
            return writer.write_str["ubigint"]()
        if self == DuckDBType.float:
            return writer.write_str["float"]()
        if self == DuckDBType.double:
            return writer.write_str["double"]()
        if self == DuckDBType.timestamp:
            return writer.write_str["timestamp"]()
        if self == DuckDBType.date:
            return writer.write_str["date"]()
        if self == DuckDBType.time:
            return writer.write_str["time"]()
        if self == DuckDBType.interval:
            return writer.write_str["interval"]()
        if self == DuckDBType.hugeint:
            return writer.write_str["hugeint"]()
        if self == DuckDBType.uhugeint:
            return writer.write_str["uhugeint"]()
        if self == DuckDBType.varchar:
            return writer.write_str["varchar"]()
        if self == DuckDBType.blob:
            return writer.write_str["blob"]()
        if self == DuckDBType.decimal:
            return writer.write_str["decimal"]()
        if self == DuckDBType.timestamp_s:
            return writer.write_str["timestamp_s"]()
        if self == DuckDBType.timestamp_ms:
            return writer.write_str["timestamp_ms"]()
        if self == DuckDBType.timestamp_ns:
            return writer.write_str["timestamp_ns"]()
        if self == DuckDBType.enum:
            return writer.write_str["enum"]()
        if self == DuckDBType.list:
            return writer.write_str["list"]()
        if self == DuckDBType.struct_t:
            return writer.write_str["struct"]()
        if self == DuckDBType.map:
            return writer.write_str["map"]()
        if self == DuckDBType.array:
            return writer.write_str["array"]()
        if self == DuckDBType.uuid:
            return writer.write_str["uuid"]()
        if self == DuckDBType.union:
            return writer.write_str["union"]()
        if self == DuckDBType.bit:
            return writer.write_str["bit"]()
        if self == DuckDBType.time_tz:
            return writer.write_str["time_tz"]()
        if self == DuckDBType.timestamp_tz:
            return writer.write_str["timestamp_tz"]()
        return writer.write_str["<<unknown>>"]()

    fn __eq__(self, rhs: DuckDBType) -> Bool:
        return self.value == rhs.value

    @always_inline("nodebug")
    fn __ne__(self, rhs: DuckDBType) -> Bool:
        return self.value != rhs.value

    @staticmethod
    fn from_dtype[dtype: DType]() -> Self:
        """Convert a Mojo numeric DType to a DuckDBType."""
        if dtype == DType.bool:
            return DuckDBType.boolean
        if dtype == DType.int8:
            return DuckDBType.tinyint
        if dtype == DType.int16:
            return DuckDBType.smallint
        if dtype == DType.int32:
            return DuckDBType.integer
        if dtype == DType.int64:
            return DuckDBType.bigint
        if dtype == DType.uint8:
            return DuckDBType.utinyint
        if dtype == DType.uint16:
            return DuckDBType.usmallint
        if dtype == DType.uint32:
            return DuckDBType.uinteger
        if dtype == DType.uint64:
            return DuckDBType.ubigint
        if dtype == DType.float32:
            return DuckDBType.float
        if dtype == DType.float64:
            return DuckDBType.double
        return DuckDBType.invalid


@value
struct Date(EqualityComparable, Formattable, Stringable):
    """Days are stored as days since 1970-01-01.
    
    TODO calling duckdb_to_date/duckdb_from_date is currently broken for unknown reasons.
    """

    var days: Int32

    # fn __init__(inout self, year: Int32, month: Int8, day: Int8):
    #     self = _impl().duckdb_to_date(duckdb_date_struct(year, month, day))

    fn format_to(self, inout writer: Formatter):
        return writer.write(self.days)
        # return writer.write(self.year(), "-", self.month(), "-", self.day())

    fn __str__(self) -> String:
        return str(self.days)

    fn __eq__(self, other: Date) -> Bool:
        return self.days == other.days

    fn __ne__(self, other: Date) -> Bool:
        return not self == other

    # fn year(self) -> Int32:
    #     return _impl().duckdb_from_date(self).year

    # fn month(self) -> Int8:
    #     return _impl().duckdb_from_date(self).month

    # fn day(self) -> Int8:
    #     return _impl().duckdb_from_date(self).day


@value
struct Time(EqualityComparable, Formattable, Stringable):
    """Time is stored as microseconds since 00:00:00.
    
    TODO calling duckdb_to_time/duckdb_from_time is currently broken for unknown reasons.
    """

    var micros: Int64

    # fn __init__(
    #     inout self, hour: Int8, minute: Int8, second: Int8, micros: Int32
    # ):
    #     self = _impl().duckdb_to_time(
    #         duckdb_time_struct(hour, minute, second, micros)
    #     )

    fn __str__(self) -> String:
        return str(self.micros)

    fn format_to(self, inout writer: Formatter):
        return writer.write(self.micros)
        # return writer.write(self.hour(), ":", self.minute(), ":", self.second())

    fn __eq__(self, other: Time) -> Bool:
        return self.micros == other.micros

    fn __ne__(self, other: Time) -> Bool:
        return not self == other

    # fn hour(self) -> Int8:
    #     return _impl().duckdb_from_time(self).hour

    # fn minute(self) -> Int8:
    #     return _impl().duckdb_from_time(self).min

    # fn second(self) -> Int8:
    #     return _impl().duckdb_from_time(self).sec

    # fn micro(self) -> Int32:
    #     return _impl().duckdb_from_time(self).micros


@value
struct Timestamp(EqualityComparable, Formattable, Stringable):
    """Timestamps are stored as microseconds since 1970-01-01."""

    var micros: Int64

    # fn __init__(inout self, date: Date, time: Time):
    #     self = _impl().duckdb_to_timestamp(
    #         duckdb_timestamp_struct(
    #             _impl().duckdb_from_date(date), _impl().duckdb_from_time(time)
    #         )
    #     )

    fn __str__(self) -> String:
        return str(self.micros)

    fn format_to(self, inout writer: Formatter):
        return writer.write(self.micros)
        # return writer.write(self.date(), " ", self.time())

    fn __eq__(self, other: Timestamp) -> Bool:
        return self.micros == other.micros

    fn __ne__(self, other: Timestamp) -> Bool:
        return not self == other

    # fn date(self) -> Date:
    #     return _impl().duckdb_to_date(_impl().duckdb_from_timestamp(self).date)

    # fn time(self) -> Time:
    #     return _impl().duckdb_to_time(_impl().duckdb_from_timestamp(self).time)
