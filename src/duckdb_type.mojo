from duckdb._libduckdb import *
from duckdb.vector import Vector
from duckdb.api import DuckDB
from collections import Set
from hashlib.hasher import Hasher
from sys.intrinsics import _type_is_eq
from sys.info import size_of


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
    comptime time_ns = DuckDBType(DUCKDB_TYPE_TIME_NS)
    """DuckDB type: TIME_NS, time in nanoseconds."""

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
            DuckDBType.timestamp_tz,
            DuckDBType.uuid,
            DuckDBType.time_ns,
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
        if self == DuckDBType.time_ns:
            return writer.write("time_ns")
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

    fn to_seconds(self) -> Float64:
        """Convert to seconds since midnight as a Float64."""
        return self.micros.cast[DType.float64]() / 1_000_000.0

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

    fn to_seconds(self) -> Float64:
        """Convert to seconds since epoch as a Float64."""
        return self.micros.cast[DType.float64]() / 1_000_000.0

    fn __init__(out self, *, seconds: Float64):
        """Create a Timestamp from seconds since epoch.

        Args:
            seconds: Seconds since 1970-01-01 00:00:00.
        """
        self.micros = (seconds * 1_000_000.0).cast[DType.int64]()

    fn to_timestamp_s(self) -> TimestampS:
        """Convert to second-precision timestamp (truncates sub-second part)."""
        return TimestampS(self.micros // 1_000_000)

    fn to_timestamp_ms(self) -> TimestampMS:
        """Convert to millisecond-precision timestamp (truncates sub-ms part)."""
        return TimestampMS(self.micros // 1_000)

    fn to_timestamp_ns(self) -> TimestampNS:
        """Convert to nanosecond-precision timestamp."""
        return TimestampNS(self.micros * 1_000)

    # fn date(self) -> Date:
    #     return duckdb_to_date(duckdb_from_timestamp(self).date)

    # fn time(self) -> Time:
    #     return duckdb_to_time(duckdb_from_timestamp(self).time)


@fieldwise_init
struct TimestampS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Timestamps with second precision, stored as seconds since 1970-01-01."""

    var seconds: Int64

    fn __str__(self) -> String:
        return String(self.seconds)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.seconds)

    fn __eq__(self, other: TimestampS) -> Bool:
        return self.seconds == other.seconds

    fn __ne__(self, other: TimestampS) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimestampS(" + String(self.seconds) + ")"

    fn to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp."""
        return Timestamp(self.seconds * 1_000_000)


@fieldwise_init
struct TimestampMS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Timestamps with millisecond precision, stored as milliseconds since 1970-01-01."""

    var millis: Int64

    fn __str__(self) -> String:
        return String(self.millis)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.millis)

    fn __eq__(self, other: TimestampMS) -> Bool:
        return self.millis == other.millis

    fn __ne__(self, other: TimestampMS) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimestampMS(" + String(self.millis) + ")"

    fn to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp."""
        return Timestamp(self.millis * 1_000)


@fieldwise_init
struct TimestampNS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Timestamps with nanosecond precision, stored as nanoseconds since 1970-01-01."""

    var nanos: Int64

    fn __str__(self) -> String:
        return String(self.nanos)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.nanos)

    fn __eq__(self, other: TimestampNS) -> Bool:
        return self.nanos == other.nanos

    fn __ne__(self, other: TimestampNS) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimestampNS(" + String(self.nanos) + ")"

    fn to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp (truncates sub-microsecond part)."""
        return Timestamp(self.nanos // 1_000)


@fieldwise_init
struct TimeNS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Time with nanosecond precision, stored as nanoseconds since midnight."""

    var nanos: Int64

    fn __str__(self) -> String:
        return String(self.nanos)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.nanos)

    fn __eq__(self, other: TimeNS) -> Bool:
        return self.nanos == other.nanos

    fn __ne__(self, other: TimeNS) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimeNS(" + String(self.nanos) + ")"

    fn to_seconds(self) -> Float64:
        """Convert to seconds since midnight as a Float64."""
        return self.nanos.cast[DType.float64]() / 1_000_000_000.0

    fn to_time(self) -> Time:
        """Convert to microsecond-precision Time (truncates sub-microsecond part)."""
        return Time(self.nanos // 1_000)


@fieldwise_init
struct TimestampTZ(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Timestamps with timezone, stored as microseconds since 1970-01-01 (UTC)."""

    var micros: Int64

    fn __str__(self) -> String:
        return String(self.micros)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.micros)

    fn __eq__(self, other: TimestampTZ) -> Bool:
        return self.micros == other.micros

    fn __ne__(self, other: TimestampTZ) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimestampTZ(" + String(self.micros) + ")"

    fn to_timestamp(self) -> Timestamp:
        """Convert to a plain Timestamp (discards timezone semantics)."""
        return Timestamp(self.micros)


@fieldwise_init
struct TimeTZ(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """Time with timezone, stored as 40 bits for microseconds and 24 bits for UTC offset.

    Use ``TimeTZ(micros=..., offset=...)`` to create from components.
    """

    var bits: UInt64

    fn __init__(out self, *, micros: Int64, offset: Int32):
        """Create a TimeTZ from microseconds since midnight and UTC offset in seconds.

        Args:
            micros: Microseconds since 00:00:00.
            offset: UTC offset in seconds.
        """
        ref libduckdb = DuckDB().libduckdb()
        var raw = libduckdb.duckdb_create_time_tz(micros, offset)
        self.bits = raw.bits

    fn __str__(self) -> String:
        return String(self.bits)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.bits)

    fn __eq__(self, other: TimeTZ) -> Bool:
        return self.bits == other.bits

    fn __ne__(self, other: TimeTZ) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "TimeTZ(" + String(self.bits) + ")"


@fieldwise_init
struct UUID(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """UUID stored as a UInt128.

    In DuckDB vectors, UUIDs are stored as Int128 with a special encoding
    (upper 64 bits offset by ``INT64_MAX + 1``).  The ``UUID`` struct stores
    the canonical unsigned representation as used by the DuckDB value API
    (``duckdb_create_uuid`` / ``duckdb_get_uuid``).
    """

    var value: UInt128

    fn __str__(self) -> String:
        return String(self.value)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.value)

    fn __eq__(self, other: UUID) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: UUID) -> Bool:
        return not self == other

    fn __repr__(self) -> String:
        return "UUID(" + String(self.value) + ")"

    fn __init__(out self, *, internal: Int128):
        """Create a UUID from DuckDB's internal hugeint representation.

        In vectors, UUIDs are stored as Int128 with ``upper`` offset
        by ``INT64_MAX + 1``.

        Args:
            internal: The raw Int128 value from a DuckDB vector.
        """
        var lower = internal.cast[DType.uint64]()
        var upper_signed = (internal >> 64).cast[DType.int64]()
        var upper: UInt64
        if upper_signed >= 0:
            upper = upper_signed.cast[DType.uint64]() + UInt64(Int64.MAX) + 1
        else:
            upper = (upper_signed + Int64.MAX + 1).cast[DType.uint64]()
        self.value = upper.cast[DType.uint128]() << 64 | lower.cast[DType.uint128]()

    fn _to_internal(self) -> Int128:
        """Convert from UUID to DuckDB's internal hugeint representation."""
        var lower = self.value.cast[DType.uint64]()
        var upper = (self.value >> 64).cast[DType.uint64]()
        var upper_signed: Int64
        if upper > UInt64(Int64.MAX):
            upper_signed = (upper - UInt64(Int64.MAX) - 1).cast[DType.int64]()
        else:
            upper_signed = upper.cast[DType.int64]() - Int64.MAX - 1
        return upper_signed.cast[DType.int128]() << 64 | lower.cast[DType.int128]()


@fieldwise_init
struct Interval(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable, Stringable, Representable):
    """An interval with months, days, and microseconds components."""

    var months: Int32
    var days: Int32
    var micros: Int64

    fn __str__(self) -> String:
        return String.write(self)

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(
            "months: ",
            self.months,
            ", days: ",
            self.days,
            ", micros: ",
            self.micros,
        )

    fn to_total_seconds(self) -> Float64:
        """Approximate total seconds, assuming 30-day months.

        Note: This is an approximation since month length varies.
        """
        var total_days = Int64(self.months) * 30 + Int64(self.days)
        return (total_days * 86_400_000_000 + self.micros).cast[DType.float64]() / 1_000_000.0

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


# Int128 and UInt128 are builtin types in Mojo.

struct Decimal(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Stringable, Representable):
    """Decimals are composed of a width and a scale, and are stored in a hugeint.
    """

    var width: UInt8
    var scale: UInt8
    # Int128 is 16-byte aligned, which causes 14 bytes of padding after scale.
    # We want 8-byte alignment to match C layout (width+scale+6pad+value).
    # But we can't easily force 8-byte alignment on Int128 field if type itself is 16-byte aligned.
    # So we use 2 Int64s/UInt64s to mimic the layout of duckdb_hugeint.
    
    # Padding: 6 bytes to reach 8-byte alignment for the next fields
    var _pad0: UInt8
    var _pad1: UInt8
    var _pad2: UInt8
    var _pad3: UInt8
    var _pad4: UInt8
    var _pad5: UInt8

    var lower: UInt64
    var upper: Int64

    fn __init__(out self, width: UInt8, scale: UInt8, value: Int128):
        self.width = width
        self.scale = scale
        self._pad0 = 0
        self._pad1 = 0
        self._pad2 = 0
        self._pad3 = 0
        self._pad4 = 0
        self._pad5 = 0
        self.lower = value.cast[DType.uint64]()
        self.upper = (value >> 64).cast[DType.int64]() # Shift right to get upper bits

    fn value(self) -> Int128:
        var l = self.lower.cast[DType.int128]()
        var u = self.upper.cast[DType.int128]() 
        return (u << 64) | l

    fn __str__(self) -> String:
        return (
            "width: "
            + String(self.width)
            + ", scale: "
            + String(self.scale)
            + ", value: "
            + String(self.value())
        )

    fn __repr__(self) -> String:
        return (
            "Decimal("
            + String(self.width)
            + ", "
            + String(self.scale)
            + ", "
            + String(self.value())
            + ")"
        )

    fn __eq__(self, other: Decimal) -> Bool:
        return (
            self.width == other.width
            and self.scale == other.scale
            and self.value() == other.value()
        )

    fn __ne__(self, other: Decimal) -> Bool:
        return not self == other

    fn to_float64(self) -> Float64:
        """Convert this Decimal to a Float64.

        Note: This may lose precision for large values or high scales.
        """
        var v = self.value()
        var divisor = Int128(1)
        for _ in range(Int(self.scale)):
            divisor *= 10
        # Split into integer and fractional parts to preserve precision
        var int_part = v // divisor
        var frac_part = v % divisor
        return int_part.cast[DType.float64]() + frac_part.cast[DType.float64]() / divisor.cast[DType.float64]()

    fn to_float32(self) -> Float32:
        """Convert this Decimal to a Float32.

        Note: This may lose precision for large values or high scales.
        """
        return self.to_float64().cast[DType.float32]()

    fn __init__(out self, width: UInt8, scale: UInt8, value: Float64):
        """Create a Decimal from a Float64 with the given width and scale.

        The float is multiplied by 10^scale and rounded to the nearest integer.

        Args:
            width: The total number of decimal digits.
            scale: The number of digits after the decimal point.
            value: The floating-point value to convert.
        """
        var multiplier = Float64(1.0)
        for _ in range(Int(scale)):
            multiplier *= 10.0
        var scaled = value * multiplier
        # Round to nearest integer
        var rounded: Int128
        if scaled >= 0:
            rounded = (scaled + 0.5).cast[DType.int128]()
        else:
            rounded = (scaled - 0.5).cast[DType.int128]()
        self = Decimal(width, scale, rounded)

    fn __init__(out self, width: UInt8, scale: UInt8, value: Float32):
        """Create a Decimal from a Float32 with the given width and scale.

        Args:
            width: The total number of decimal digits.
            scale: The number of digits after the decimal point.
            value: The floating-point value to convert.
        """
        self = Decimal(width, scale, value.cast[DType.float64]())


fn dtype_to_duckdb_type[dt: DType]() -> DuckDBType:
    """Maps a Mojo DType to its corresponding DuckDB type at compile time.

    This enables deriving DuckDB parameter/return types from Mojo scalar types,
    eliminating the need to manually create `LogicalType` instances.

    Parameters:
        dt: The Mojo DType to map.

    Returns:
        The corresponding DuckDBType.

    Example:
    ```mojo
    comptime duckdb_int = dtype_to_duckdb_type[DType.int32]()
    # duckdb_int == DuckDBType.integer
    ```
    """
    @parameter
    if dt == DType.bool:
        return DuckDBType.boolean
    elif dt == DType.int8:
        return DuckDBType.tinyint
    elif dt == DType.int16:
        return DuckDBType.smallint
    elif dt == DType.int32:
        return DuckDBType.integer
    elif dt == DType.int64:
        return DuckDBType.bigint
    elif dt == DType.uint8:
        return DuckDBType.utinyint
    elif dt == DType.uint16:
        return DuckDBType.usmallint
    elif dt == DType.uint32:
        return DuckDBType.uinteger
    elif dt == DType.uint64:
        return DuckDBType.ubigint
    elif dt == DType.float32:
        return DuckDBType.float
    elif dt == DType.float64:
        return DuckDBType.double
    else:
        constrained[False, "Unsupported DType for DuckDB mapping"]()
        return DuckDBType.invalid


fn mojo_to_duckdb_type[T: AnyType]() -> DuckDBType:
    """Maps a Mojo scalar type to its corresponding DuckDB type at compile time.

    Supports: Bool, Int8–Int64, UInt8–UInt64, Float32, Float64, Int, UInt,
    Int128, UInt128, String, Date, Time, TimeNS, Timestamp, TimestampS,
    TimestampMS, TimestampNS, TimestampTZ, TimeTZ, Interval, Decimal, UUID.

    Parameters:
        T: The Mojo scalar type to map.

    Returns:
        The corresponding DuckDBType.

    Example:
    ```mojo
    comptime duckdb_int = mojo_to_duckdb_type[Int32]()
    # duckdb_int == DuckDBType.integer
    ```
    """
    @parameter
    if _type_is_eq[T, Bool]():
        return DuckDBType.boolean
    elif _type_is_eq[T, Int8]():
        return DuckDBType.tinyint
    elif _type_is_eq[T, Int16]():
        return DuckDBType.smallint
    elif _type_is_eq[T, Int32]():
        return DuckDBType.integer
    elif _type_is_eq[T, Int64]():
        return DuckDBType.bigint
    elif _type_is_eq[T, UInt8]():
        return DuckDBType.utinyint
    elif _type_is_eq[T, UInt16]():
        return DuckDBType.usmallint
    elif _type_is_eq[T, UInt32]():
        return DuckDBType.uinteger
    elif _type_is_eq[T, UInt64]():
        return DuckDBType.ubigint
    elif _type_is_eq[T, Float32]():
        return DuckDBType.float
    elif _type_is_eq[T, Float64]():
        return DuckDBType.double
    elif _type_is_eq[T, Int128]():
        return DuckDBType.hugeint
    elif _type_is_eq[T, UInt128]():
        return DuckDBType.uhugeint
    elif _type_is_eq[T, String]():
        return DuckDBType.varchar
    elif _type_is_eq[T, Date]():
        return DuckDBType.date
    elif _type_is_eq[T, Time]():
        return DuckDBType.time
    elif _type_is_eq[T, TimeNS]():
        return DuckDBType.time_ns
    elif _type_is_eq[T, Timestamp]():
        return DuckDBType.timestamp
    elif _type_is_eq[T, TimestampS]():
        return DuckDBType.timestamp_s
    elif _type_is_eq[T, TimestampMS]():
        return DuckDBType.timestamp_ms
    elif _type_is_eq[T, TimestampNS]():
        return DuckDBType.timestamp_ns
    elif _type_is_eq[T, TimestampTZ]():
        return DuckDBType.timestamp_tz
    elif _type_is_eq[T, TimeTZ]():
        return DuckDBType.time_tz
    elif _type_is_eq[T, Interval]():
        return DuckDBType.interval
    elif _type_is_eq[T, Decimal]():
        return DuckDBType.decimal
    elif _type_is_eq[T, UUID]():
        return DuckDBType.uuid
    elif _type_is_eq[T, Int]():
        @parameter
        if size_of[Int]() == 4:
            return DuckDBType.integer
        else:
            return DuckDBType.bigint
    elif _type_is_eq[T, UInt]():
        @parameter
        if size_of[UInt]() == 4:
            return DuckDBType.uinteger
        else:
            return DuckDBType.ubigint
    else:
        constrained[False, "Unsupported Mojo type for DuckDB mapping"]()
        return DuckDBType.invalid
