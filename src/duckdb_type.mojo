from duckdb._libduckdb import *
from duckdb.vector import Vector
from collections import Set
from hashlib.hasher import Hasher


@fieldwise_init
struct DuckDBType(
    TrivialRegisterPassable,
    Hashable,
    Stringable,
    Writable,
    Equatable,
    KeyElement,
):
    """Represents DuckDB types."""

    var value: Int32

    comptime invalid = DuckDBType(DUCKDB_TYPE_INVALID)
    """DuckDB type: INVALID."""
    comptime boolean = DuckDBType(DUCKDB_TYPE_BOOLEAN)
    """DuckDB type: BOOLEAN."""
    comptime tinyint = DuckDBType(DUCKDB_TYPE_TINYINT)
    """DuckDB type: TINYINT (int8)."""
    comptime smallint = DuckDBType(DUCKDB_TYPE_SMALLINT)
    """DuckDB type: SMALLINT (int16)."""
    comptime integer = DuckDBType(DUCKDB_TYPE_INTEGER)
    """DuckDB type: INTEGER (int32)."""
    comptime bigint = DuckDBType(DUCKDB_TYPE_BIGINT)
    """DuckDB type: BIGINT (int64)."""
    comptime utinyint = DuckDBType(DUCKDB_TYPE_UTINYINT)
    """DuckDB type: UTINYINT (uint8)."""
    comptime usmallint = DuckDBType(DUCKDB_TYPE_USMALLINT)
    """DuckDB type: USMALLINT (uint16)."""
    comptime uinteger = DuckDBType(DUCKDB_TYPE_UINTEGER)
    """DuckDB type: UINTEGER (uint32)."""
    comptime ubigint = DuckDBType(DUCKDB_TYPE_UBIGINT)
    """DuckDB type: UBIGINT (uint64)."""
    comptime float = DuckDBType(DUCKDB_TYPE_FLOAT)
    """DuckDB type: FLOAT (float32)."""
    comptime double = DuckDBType(DUCKDB_TYPE_DOUBLE)
    """DuckDB type: DOUBLE (float64)."""
    comptime timestamp = DuckDBType(DUCKDB_TYPE_TIMESTAMP)
    """DuckDB type: TIMESTAMP, in microseconds."""
    comptime date = DuckDBType(DUCKDB_TYPE_DATE)
    """DuckDB type: DATE."""
    comptime time = DuckDBType(DUCKDB_TYPE_TIME)
    """DuckDB type: TIME."""
    comptime interval = DuckDBType(DUCKDB_TYPE_INTERVAL)
    """DuckDB type: INTERVAL."""
    comptime hugeint = DuckDBType(DUCKDB_TYPE_HUGEINT)
    """DuckDB type: HUGEINT."""
    comptime uhugeint = DuckDBType(DUCKDB_TYPE_UHUGEINT)
    """DuckDB type: UHUGEINT."""
    comptime varchar = DuckDBType(DUCKDB_TYPE_VARCHAR)
    """DuckDB type: VARCHAR (String)."""
    comptime blob = DuckDBType(DUCKDB_TYPE_BLOB)
    """DuckDB type: BLOB."""
    comptime decimal = DuckDBType(DUCKDB_TYPE_DECIMAL)
    """DuckDB type: DECIMAL."""
    comptime timestamp_s = DuckDBType(DUCKDB_TYPE_TIMESTAMP_S)
    """DuckDB type: TIMESTAMP_S, in seconds."""
    comptime timestamp_ms = DuckDBType(DUCKDB_TYPE_TIMESTAMP_MS)
    """DuckDB type: TIMESTAMP_MS, in milliseconds."""
    comptime timestamp_ns = DuckDBType(DUCKDB_TYPE_TIMESTAMP_NS)
    """DuckDB type: TIMESTAMP_NS, in nanoseconds."""
    comptime enum = DuckDBType(DUCKDB_TYPE_ENUM)
    """DuckDB type: ENUM, only useful as logical type."""
    comptime list = DuckDBType(DUCKDB_TYPE_LIST)
    """DuckDB type: LIST, only useful as logical type."""
    comptime struct_t = DuckDBType(DUCKDB_TYPE_STRUCT)
    """DuckDB type: STRUCT, only useful as logical type."""
    comptime map = DuckDBType(DUCKDB_TYPE_MAP)
    """DuckDB type: MAP, only useful as logical type."""
    comptime array = DuckDBType(DUCKDB_TYPE_ARRAY)
    """DuckDB type: ARRAY, only useful as logical type."""
    comptime uuid = DuckDBType(DUCKDB_TYPE_UUID)
    """DuckDB type: UUID."""
    comptime union = DuckDBType(DUCKDB_TYPE_UNION)
    """DuckDB type: UNION, only useful as logical type."""
    comptime bit = DuckDBType(DUCKDB_TYPE_BIT)
    """DuckDB type: BIT."""
    comptime time_tz = DuckDBType(DUCKDB_TYPE_TIME_TZ)
    """DuckDB type: TIME_TZ."""
    comptime timestamp_tz = DuckDBType(DUCKDB_TYPE_TIMESTAMP_TZ)
    """DuckDB type: TIMESTAMP_TZ."""

    # fn __init__(out self, value: LogicalType):
    #     """Create a DuckDBType from a LogicalType."""
    #     self = value.get_type_id()

    fn is_fixed_size(self) -> Bool:
        return self in Set(
            DuckDBType.boolean,
            DuckDBType.tinyint,
            DuckDBType.smallint,
            DuckDBType.integer,
            DuckDBType.bigint,
            DuckDBType.utinyint,
            DuckDBType.usmallint,
            DuckDBType.uinteger,
            DuckDBType.ubigint,
            DuckDBType.float,
            DuckDBType.double,
            DuckDBType.timestamp,
            DuckDBType.date,
            DuckDBType.time,
            DuckDBType.interval,
            DuckDBType.hugeint,
            DuckDBType.uhugeint,
            DuckDBType.timestamp_s,
            DuckDBType.timestamp_ms,
            DuckDBType.timestamp_ns,
            DuckDBType.time_tz,
            DuckDBType.timestamp_tz
            # TODO what else?
        )

    fn is_nested(self) -> Bool:
        return self in Set(
            DuckDBType.list,
            DuckDBType.struct_t,
            DuckDBType.map,
            DuckDBType.array,
            DuckDBType.union,
        )

    @always_inline
    fn __init__(out self, *, other: Self):
        """Copy this DuckDBType.

        Args:
            other: The DuckDBType to copy.
        """
        self = other

    fn __hash__[H: Hasher](self, mut hasher: H):
        hasher.update(self.value)

    @always_inline("nodebug")
    fn __repr__(self) -> String:
        return "DuckDBType." + String(self)

    @always_inline("nodebug")
    fn __str__(self) -> String:
        return String.write(self)

    fn write_to[W: Writer](self, mut writer: W):
        if self == DuckDBType.invalid:
            return writer.write("invalid")
        if self == DuckDBType.tinyint:
            return writer.write("tinyint")
        if self == DuckDBType.boolean:
            return writer.write("boolean")
        if self == DuckDBType.smallint:
            return writer.write("smallint")
        if self == DuckDBType.integer:
            return writer.write("integer")
        if self == DuckDBType.bigint:
            return writer.write("bigint")
        if self == DuckDBType.utinyint:
            return writer.write("utinyint")
        if self == DuckDBType.usmallint:
            return writer.write("usmallint")
        if self == DuckDBType.uinteger:
            return writer.write("uinteger")
        if self == DuckDBType.ubigint:
            return writer.write("ubigint")
        if self == DuckDBType.float:
            return writer.write("float")
        if self == DuckDBType.double:
            return writer.write("double")
        if self == DuckDBType.timestamp:
            return writer.write("timestamp")
        if self == DuckDBType.date:
            return writer.write("date")
        if self == DuckDBType.time:
            return writer.write("time")
        if self == DuckDBType.interval:
            return writer.write("interval")
        if self == DuckDBType.hugeint:
            return writer.write("hugeint")
        if self == DuckDBType.uhugeint:
            return writer.write("uhugeint")
        if self == DuckDBType.varchar:
            return writer.write("varchar")
        if self == DuckDBType.blob:
            return writer.write("blob")
        if self == DuckDBType.decimal:
            return writer.write("decimal")
        if self == DuckDBType.timestamp_s:
            return writer.write("timestamp_s")
        if self == DuckDBType.timestamp_ms:
            return writer.write("timestamp_ms")
        if self == DuckDBType.timestamp_ns:
            return writer.write("timestamp_ns")
        if self == DuckDBType.enum:
            return writer.write("enum")
        if self == DuckDBType.list:
            return writer.write("list")
        if self == DuckDBType.struct_t:
            return writer.write("struct")
        if self == DuckDBType.map:
            return writer.write("map")
        if self == DuckDBType.array:
            return writer.write("array")
        if self == DuckDBType.uuid:
            return writer.write("uuid")
        if self == DuckDBType.union:
            return writer.write("union")
        if self == DuckDBType.bit:
            return writer.write("bit")
        if self == DuckDBType.time_tz:
            return writer.write("time_tz")
        if self == DuckDBType.timestamp_tz:
            return writer.write("timestamp_tz")
        return writer.write("<<unknown>>")

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

    fn to_dtype(self) -> DType:
        """Convert a DuckDBType to a Mojo numeric DType."""
        if self == DuckDBType.boolean:
            return DType.bool
        if self == DuckDBType.tinyint:
            return DType.int8
        if self == DuckDBType.smallint:
            return DType.int16
        if self == DuckDBType.integer:
            return DType.int32
        if self == DuckDBType.bigint:
            return DType.int64
        if self == DuckDBType.utinyint:
            return DType.uint8
        if self == DuckDBType.usmallint:
            return DType.uint16
        if self == DuckDBType.uinteger:
            return DType.uint32
        if self == DuckDBType.ubigint:
            return DType.uint64
        if self == DuckDBType.float:
            return DType.float32
        if self == DuckDBType.double:
            return DType.float64
        return DType.invalid


@fieldwise_init
struct Date(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Writable, Representable, Stringable):
    """Days are stored as days since 1970-01-01.

    TODO calling duckdb_to_date/duckdb_from_date is currently broken for unknown reasons.
    """

    var days: Int32

    # fn __init__(out self, year: Int32, month: Int8, day: Int8):
    #     self = duckdb_to_date(duckdb_date_struct(year, month, day))

    fn write_to[W: Writer](self, mut writer: W):
        return writer.write(self.days)
        # return writer.write(self.year(), "-", self.month(), "-", self.day())

    fn __str__(self) -> String:
        return String(self.days)

    fn __repr__(self) -> String:
        return "Date(" + String(self.days) + ")"

    fn __eq__(self, other: Date) -> Bool:
        return self.days == other.days

    fn __ne__(self, other: Date) -> Bool:
        return not self == other

    # fn year(self) -> Int32:
    #     return duckdb_from_date(self).year

    # fn month(self) -> Int8:
    #     return duckdb_from_date(self).month

    # fn day(self) -> Int8:
    #     return duckdb_from_date(self).day


@fieldwise_init
struct Time(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Writable, Representable, Stringable):
    """Time is stored as microseconds since 00:00:00.

    TODO calling duckdb_to_time/duckdb_from_time is currently broken for unknown reasons.
    """

    var micros: Int64

    # fn __init__(
    #     out self, hour: Int8, minute: Int8, second: Int8, micros: Int32
    # ):
    #     self = duckdb_to_time(
    #         duckdb_time_struct(hour, minute, second, micros)
    #     )

    fn __str__(self) -> String:
        return String(self.micros)

    fn write_to[W: Writer](self, mut writer: W):
        return writer.write(self.micros)
        # return writer.write(self.hour(), ":", self.minute(), ":", self.second())

    fn __repr__(self) -> String:
        return "Time(" + String(self.micros) + ")"

    fn __eq__(self, other: Time) -> Bool:
        return self.micros == other.micros

    fn __ne__(self, other: Time) -> Bool:
        return not self == other

    # fn hour(self) -> Int8:
    #     return duckdb_from_time(self).hour

    # fn minute(self) -> Int8:
    #     return duckdb_from_time(self).min

    # fn second(self) -> Int8:
    #     return duckdb_from_time(self).sec

    # fn micro(self) -> Int32:
    #     return duckdb_from_time(self).micros


@fieldwise_init
struct Timestamp(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Timestamps are stored as microseconds since 1970-01-01."""

    var micros: Int64

    # fn __init__(out self, date: Date, time: Time):
    #     self = duckdb_to_timestamp(
    #         duckdb_timestamp_struct(
    #             duckdb_from_date(date), duckdb_from_time(time)
    #         )
    #     )

    fn __str__(self) -> String:
        return String(self.micros)

    fn write_to[W: Writer](self, mut writer: W):
        return writer.write(self.micros)
        # return writer.write(self.date(), " ", self.time())

    fn __eq__(self, other: Timestamp) -> Bool:
        return self.micros == other.micros

    fn __ne__(self, other: Timestamp) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "Timestamp(" + String(self.micros) + ")"

    # fn date(self) -> Date:
    #     return duckdb_to_date(duckdb_from_timestamp(self).date)

    # fn time(self) -> Time:
    #     return duckdb_to_time(duckdb_from_timestamp(self).time)


@fieldwise_init
struct Interval(TrivialRegisterPassable, Equatable, Stringable, Representable):
    var months: Int32
    var days: Int32
    var micros: Int64

    fn __str__(self) -> String:
        return (
            "months: "
            + String(self.months)
            + ", days: "
            + String(self.days)
            + ", micros: "
            + String(self.micros)
        )

    fn __repr__(self) -> String:
        return (
            "Interval("
            + String(self.months)
            + ", "
            + String(self.days)
            + ", "
            + String(self.micros)
            + ")"
        )


@fieldwise_init
struct Int128(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Hugeints are composed of a (lower, upper) component.

    The value of the hugeint is upper * 2^64 + lower
    For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
    """

    var lower: UInt64
    var upper: Int64

    fn __str__(self) -> String:
        return "lower: " + String(self.lower) + ", upper: " + String(self.upper)

    fn __repr__(self) -> String:
        return "Int128(" + String(self.lower) + ", " + String(self.upper) + ")"


@fieldwise_init
struct UInt128(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """UHugeints are composed of a (lower, upper) component."""

    var lower: UInt64
    var upper: UInt64

    fn __str__(self) -> String:
        return "lower: " + String(self.lower) + ", upper: " + String(self.upper)

    fn __repr__(self) -> String:
        return "UInt128(" + String(self.lower) + ", " + String(self.upper) + ")"


@fieldwise_init
struct Decimal(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Decimals are composed of a width and a scale, and are stored in a hugeint.
    """

    var width: UInt8
    var scale: UInt8
    var value: UInt128

    fn __str__(self) -> String:
        return (
            "width: "
            + String(self.width)
            + ", scale: "
            + String(self.scale)
            + ", value: "
            + String(self.value)
        )

    fn __repr__(self) -> String:
        return (
            "Decimal("
            + String(self.width)
            + ", "
            + String(self.scale)
            + ", "
            + String(self.value)
            + ")"
        )
