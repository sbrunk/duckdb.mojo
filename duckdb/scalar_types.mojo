"""Mojo structs for DuckDB's date, time, decimal, UUID, interval and bit values.

They hold DuckDB's storage layout, so columns of these types are read and
written without conversion. Calendar fields are computed in pure Mojo.
"""

from duckdb._libduckdb import duckdb_date_struct, duckdb_time_struct, duckdb_timestamp_struct


def _zpad(value: Int, width: Int) -> String:
    """Left-pad ``value``'s decimal digits with zeros to at least ``width``.

    Intended for non-negative date/time components.
    """
    var s = String(value)
    if s.byte_length() >= width:
        return s^
    var out = String("")
    for _ in range(width - s.byte_length()):
        out += "0"
    out += s
    return out^


def _frac_str(micros_in: Int64) -> String:
    """Fractional-second suffix for ``micros`` (0 to 999999): ``""`` or ``.ddd``.

    Trailing zeros are stripped, matching DuckDB's display (``.5`` not
    ``.500000``).
    """
    var micros = micros_in % 1_000_000
    if micros < 0:
        micros += 1_000_000
    if micros == 0:
        return String("")
    var digits = 6
    while micros % 10 == 0:
        micros //= 10
        digits -= 1
    return String(".") + _zpad(Int(micros), digits)



# ===--------------------------------------------------------------------===#
# Calendar arithmetic in pure Mojo (no DuckDB call, so it also works inside
# V2 extensions). Proleptic Gregorian calendar, as DuckDB uses.
# Algorithms: Howard Hinnant, "chrono-compatible low-level date algorithms".
# ===--------------------------------------------------------------------===#

comptime _MICROS_PER_DAY: Int64 = 86_400_000_000


def _days_from_civil(year: Int64, month: Int64, day: Int64) -> Int64:
    var y = year - 1 if month <= 2 else year
    var era = y // 400  # floor division, also for negative years
    var yoe = y - era * 400
    var mp = (month + 9) % 12
    var doy = (153 * mp + 2) // 5 + day - 1
    var doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _civil_from_days(days: Int64) -> duckdb_date_struct:
    var z = days + 719468
    var era = z // 146097
    var doe = z - era * 146097
    var yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    var doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    var mp = (5 * doy + 2) // 153
    var d = doy - (153 * mp + 2) // 5 + 1
    var m = mp + 3 if mp < 10 else mp - 9
    var y = yoe + era * 400 + (Int64(1) if m <= 2 else Int64(0))
    return duckdb_date_struct(Int32(y), Int8(m), Int8(d))


def _time_parts(micros: Int64) -> duckdb_time_struct:
    var us = micros % 1_000_000
    var secs = micros // 1_000_000
    return duckdb_time_struct(Int8(secs // 3600), Int8((secs // 60) % 60), Int8(secs % 60), Int32(us))


def _time_micros(t: duckdb_time_struct) -> Int64:
    return ((Int64(t.hour) * 60 + Int64(t.min)) * 60 + Int64(t.sec)) * 1_000_000 + Int64(t.micros)


@fieldwise_init
struct Date(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Writable):
    """Days are stored as days since 1970-01-01."""

    var days: Int32

    def __init__(out self, year: Int32, month: Int8, day: Int8):
        """Create a Date from year, month, and day components."""
        self.days = Int32(_days_from_civil(Int64(year), Int64(month), Int64(day)))

    def write_to[W: Writer](self, mut writer: W):
        # ISO-8601 with zero-padded components (e.g. 2024-01-05). Negative
        # (BC / proleptic) years keep the sign before the zero-padded digits.
        var p = self._parts()
        var y = Int(p.year)
        if y < 0:
            writer.write("-", _zpad(-y, 4))
        else:
            writer.write(_zpad(y, 4))
        writer.write("-", _zpad(Int(p.month), 2), "-", _zpad(Int(p.day), 2))

    def __str__(self) -> String:
        return String(self)

    def __repr__(self) -> String:
        return "Date(" + String(self.days) + ")"

    def __eq__(self, other: Date) -> Bool:
        return self.days == other.days

    def __ne__(self, other: Date) -> Bool:
        return not self == other

    def _parts(self) -> duckdb_date_struct:
        return _civil_from_days(Int64(self.days))

    def year(self) -> Int32:
        return self._parts().year

    def month(self) -> Int8:
        return self._parts().month

    def day(self) -> Int8:
        return self._parts().day


@fieldwise_init
struct Time(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Writable):
    """Time is stored as microseconds since 00:00:00."""

    var micros: Int64

    def __init__(out self, hour: Int8, minute: Int8, second: Int8, micros: Int32):
        """Create a Time from hour, minute, second, and microseconds."""
        self.micros = _time_micros(duckdb_time_struct(hour, minute, second, micros))

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # HH:MM:SS with zero-padding and a fractional suffix when present.
        var p = self._parts()
        writer.write(
            _zpad(Int(p.hour), 2),
            ":",
            _zpad(Int(p.min), 2),
            ":",
            _zpad(Int(p.sec), 2),
        )
        writer.write(_frac_str(Int64(p.micros)))

    def __repr__(self) -> String:
        return "Time(" + String(self.micros) + ")"

    def to_seconds(self) -> Float64:
        """Convert to seconds since midnight as a Float64."""
        return self.micros.cast[DType.float64]() / 1_000_000.0

    def __eq__(self, other: Time) -> Bool:
        return self.micros == other.micros

    def __ne__(self, other: Time) -> Bool:
        return not self == other

    def _parts(self) -> duckdb_time_struct:
        return _time_parts(self.micros)

    def hour(self) -> Int8:
        return self._parts().hour

    def minute(self) -> Int8:
        return self._parts().min

    def second(self) -> Int8:
        return self._parts().sec

    def micro(self) -> Int32:
        return self._parts().micros


@fieldwise_init
struct Timestamp(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps are stored as microseconds since 1970-01-01."""

    var micros: Int64

    def __init__(out self, date: Date, time: Time):
        """Create a Timestamp from a Date and Time."""
        self.micros = Int64(date.days) * _MICROS_PER_DAY + time.micros

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        return writer.write(self.date(), " ", self.time())

    def __eq__(self, other: Timestamp) -> Bool:
        return self.micros == other.micros

    def __ne__(self, other: Timestamp) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "Timestamp(" + String(self.micros) + ")"

    def to_seconds(self) -> Float64:
        """Convert to seconds since epoch as a Float64."""
        return self.micros.cast[DType.float64]() / 1_000_000.0

    def __init__(out self, *, seconds: Float64):
        """Create a Timestamp from seconds since epoch.

        Args:
            seconds: Seconds since 1970-01-01 00:00:00.
        """
        self.micros = (seconds * 1_000_000.0).cast[DType.int64]()

    def to_timestamp_s(self) -> TimestampS:
        """Convert to second-precision timestamp (truncates sub-second part)."""
        return TimestampS(self.micros // 1_000_000)

    def to_timestamp_ms(self) -> TimestampMS:
        """Convert to millisecond-precision timestamp (truncates sub-ms part)."""
        return TimestampMS(self.micros // 1_000)

    def to_timestamp_ns(self) -> TimestampNS:
        """Convert to nanosecond-precision timestamp."""
        return TimestampNS(self.micros * 1_000)

    def _parts(self) -> duckdb_timestamp_struct:
        return duckdb_timestamp_struct(self.date()._parts(), self.time()._parts())

    def date(self) -> Date:
        # Floor division: times before 1970 belong to the earlier day.
        var d = self.micros // _MICROS_PER_DAY
        return Date(Int32(d))

    def time(self) -> Time:
        return Time(self.micros - (self.micros // _MICROS_PER_DAY) * _MICROS_PER_DAY)


@fieldwise_init
struct TimestampS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps with second precision, stored as seconds since 1970-01-01."""

    var seconds: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted timestamp, not the raw second count.
        writer.write(self.to_timestamp())

    def __eq__(self, other: TimestampS) -> Bool:
        return self.seconds == other.seconds

    def __ne__(self, other: TimestampS) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimestampS(" + String(self.seconds) + ")"

    def to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp."""
        return Timestamp(self.seconds * 1_000_000)


@fieldwise_init
struct TimestampMS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps with millisecond precision, stored as milliseconds since 1970-01-01."""

    var millis: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted timestamp, not the raw millisecond count.
        writer.write(self.to_timestamp())

    def __eq__(self, other: TimestampMS) -> Bool:
        return self.millis == other.millis

    def __ne__(self, other: TimestampMS) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimestampMS(" + String(self.millis) + ")"

    def to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp."""
        return Timestamp(self.millis * 1_000)


@fieldwise_init
struct TimestampNS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps with nanosecond precision, stored as nanoseconds since 1970-01-01."""

    var nanos: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted timestamp (microsecond precision).
        writer.write(self.to_timestamp())

    def __eq__(self, other: TimestampNS) -> Bool:
        return self.nanos == other.nanos

    def __ne__(self, other: TimestampNS) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimestampNS(" + String(self.nanos) + ")"

    def to_timestamp(self) -> Timestamp:
        """Convert to microsecond-precision Timestamp (truncates sub-microsecond part)."""
        return Timestamp(self.nanos // 1_000)


@fieldwise_init
struct TimeNS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Time with nanosecond precision, stored as nanoseconds since midnight."""

    var nanos: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted time (microsecond precision).
        writer.write(self.to_time())

    def __eq__(self, other: TimeNS) -> Bool:
        return self.nanos == other.nanos

    def __ne__(self, other: TimeNS) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimeNS(" + String(self.nanos) + ")"

    def write_repr_to[W: Writer](self, mut writer: W):
        writer.write("TimeNS(", self.nanos, ")")

    def to_seconds(self) -> Float64:
        """Convert to seconds since midnight as a Float64."""
        return self.nanos.cast[DType.float64]() / 1_000_000_000.0

    def to_time(self) -> Time:
        """Convert to microsecond-precision Time (truncates sub-microsecond part)."""
        return Time(self.nanos // 1_000)


struct Bit(Copyable, Movable, Equatable, Writable, Sized):
    """A DuckDB BIT (bitstring) value stored as packed bytes.

    Internally uses DuckDB's packed format: a padding byte followed by
    ceil(n/8) data bytes with bits packed MSB-first.  The padding byte
    indicates how many leading bits in the first data byte are unused.

    Construct from a binary string, an integer, or raw packed data:

    .. code-block:: mojo

        var a = Bit("10110")         # from binary string
        var b = Bit(Int32(123))      # 32-bit: 00000000000000000000000001111011
        var c = Bit(UInt8(255))      # 8-bit:  11111111
    """

    var _data: List[UInt8]
    """Padding byte + packed data bytes."""
    var _size: Int
    """Number of bits."""

    def __init__(out self, var data: List[UInt8], size: Int):
        """Create a Bit from raw packed data and bit count.

        Args:
            data: Padding byte followed by packed data bytes.
            size: Number of bits.
        """
        self._data = data^
        self._size = size

    def __init__(out self, s: StringSlice):
        """Create a Bit from a binary string like ``"10110"``.

        Args:
            s: A string of '0' and '1' characters.
        """
        var n = s.byte_length()
        var padding = (8 - (n % 8)) % 8
        var num_data_bytes = (n + 7) // 8
        var data = List[UInt8](capacity=num_data_bytes + 1)
        data.append(UInt8(padding))
        var src = s.as_bytes()
        for byte_i in range(num_data_bytes):
            var byte_val: UInt8 = 0
            for bit_i in range(8):
                var src_idx = byte_i * 8 + bit_i - padding
                if 0 <= src_idx < n:
                    if src[src_idx] == UInt8(ord("1")):
                        byte_val |= UInt8(1 << (7 - bit_i))
            data.append(byte_val)
        self._data = data^
        self._size = n

    @staticmethod
    def _pack_bytes(v: UInt64, num_bytes: Int) -> List[UInt8]:
        """Pack an integer value into big-endian bytes with padding byte prefix."""
        var data = List[UInt8](capacity=num_bytes + 1)
        data.append(UInt8(0))  # no padding, all widths are multiples of 8
        for i in range(num_bytes):
            var shift = UInt64((num_bytes - 1 - i) * 8)
            data.append(((v >> shift) & UInt64(0xFF)).cast[DType.uint8]())
        return data^

    def __init__(out self, value: Int8):
        """Create a Bit from an Int8 (8-bit two's complement).

        Matches DuckDB's ``value::TINYINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint8]().cast[DType.uint64](), 1)
        self._size = 8

    def __init__(out self, value: UInt8):
        """Create a Bit from a UInt8 (8-bit unsigned).

        Matches DuckDB's ``value::UTINYINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint64](), 1)
        self._size = 8

    def __init__(out self, value: Int16):
        """Create a Bit from an Int16 (16-bit two's complement).

        Matches DuckDB's ``value::SMALLINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint16]().cast[DType.uint64](), 2)
        self._size = 16

    def __init__(out self, value: UInt16):
        """Create a Bit from a UInt16 (16-bit unsigned).

        Matches DuckDB's ``value::USMALLINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint64](), 2)
        self._size = 16

    def __init__(out self, value: Int32):
        """Create a Bit from an Int32 (32-bit two's complement).

        Matches DuckDB's ``value::INTEGER::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint32]().cast[DType.uint64](), 4)
        self._size = 32

    def __init__(out self, value: UInt32):
        """Create a Bit from a UInt32 (32-bit unsigned).

        Matches DuckDB's ``value::UINTEGER::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint64](), 4)
        self._size = 32

    def __init__(out self, value: Int64):
        """Create a Bit from an Int64 (64-bit two's complement).

        Matches DuckDB's ``value::BIGINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value.cast[DType.uint64](), 8)
        self._size = 64

    def __init__(out self, value: UInt64):
        """Create a Bit from a UInt64 (64-bit unsigned).

        Matches DuckDB's ``value::UBIGINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(value, 8)
        self._size = 64

    def __init__(out self, value: Int):
        """Create a Bit from an Int (64-bit, same as BIGINT).

        Matches DuckDB's ``value::BIGINT::BITSTRING`` cast.
        """
        self._data = Self._pack_bytes(UInt64(value), 8)
        self._size = 64

    def __len__(self) -> Int:
        """Return the number of bits."""
        return self._size

    def __getitem__(self, idx: Int) -> Bool:
        """Get the bit at the given index (0-based, left-to-right).

        Args:
            idx: Bit index.

        Returns:
            True if the bit is set, False otherwise.
        """
        var padding = Int(self._data[0])
        var bit_pos = padding + idx
        var byte_idx = bit_pos // 8 + 1  # +1 to skip padding byte
        var bit_in_byte = UInt8(7 - (bit_pos % 8))
        return (self._data[byte_idx] >> bit_in_byte) & 1 == 1

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        for i in range(self._size):
            writer.write("1" if self[i] else "0")

    def __repr__(self) -> String:
        return 'Bit("' + String(self) + '")'

    def write_repr_to[W: Writer](self, mut writer: W):
        writer.write('Bit("')
        self.write_to(writer)
        writer.write('")')

    def __eq__(self, other: Bit) -> Bool:
        if self._size != other._size:
            return False
        for i in range(self._size):
            if self[i] != other[i]:
                return False
        return True

    def __ne__(self, other: Bit) -> Bool:
        return not self == other


@fieldwise_init
struct TimestampTZ(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps with timezone, stored as microseconds since 1970-01-01 (UTC)."""

    var micros: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted timestamp (UTC); timezone offset is not shown.
        writer.write(self.to_timestamp())

    def __eq__(self, other: TimestampTZ) -> Bool:
        return self.micros == other.micros

    def __ne__(self, other: TimestampTZ) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimestampTZ(" + String(self.micros) + ")"

    def to_timestamp(self) -> Timestamp:
        """Convert to a plain Timestamp (discards timezone semantics)."""
        return Timestamp(self.micros)


@fieldwise_init
struct TimestampTZNS(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Timestamps with timezone and nanosecond precision, stored as nanoseconds since 1970-01-01 (UTC).

    Maps to DuckDB's TIMESTAMPTZ_NS type (DuckDB 2.0+).
    """

    var nanos: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Render as a formatted timestamp (UTC, microsecond precision); timezone offset is not shown.
        writer.write(self.to_timestamp())

    def __eq__(self, other: TimestampTZNS) -> Bool:
        return self.nanos == other.nanos

    def __ne__(self, other: TimestampTZNS) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimestampTZNS(" + String(self.nanos) + ")"

    def to_timestamp(self) -> Timestamp:
        """Convert to a plain microsecond Timestamp (discards timezone semantics, truncates sub-microsecond part)."""
        return Timestamp(self.nanos // 1_000)


@fieldwise_init
struct TimeTZ(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """Time with timezone, stored as 40 bits for microseconds and 24 bits for UTC offset.

    Use ``TimeTZ(micros=..., offset=...)`` to create from components.
    """

    var bits: UInt64

    def __init__(out self, *, micros: Int64, offset: Int32):
        """Create a TimeTZ from microseconds since midnight and UTC offset in seconds.

        Args:
            micros: Microseconds since 00:00:00.
            offset: UTC offset in seconds.
        """
        # DuckDB's dtime_tz_t layout: micros in the upper 40 bits, the offset
        # encoded as (MAX_OFFSET - offset) in the lower 24.
        comptime MAX_OFFSET: Int64 = 16 * 60 * 60 - 1
        self.bits = (UInt64(micros) << 24) | UInt64(MAX_OFFSET - Int64(offset))

    def __str__(self) -> String:
        return String(self.bits)

    def write_to[W: Writer](self, mut writer: W):
        writer.write(self.bits)

    def __eq__(self, other: TimeTZ) -> Bool:
        return self.bits == other.bits

    def __ne__(self, other: TimeTZ) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "TimeTZ(" + String(self.bits) + ")"


@fieldwise_init
struct UUID(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """UUID stored as a UInt128.

    In DuckDB vectors, UUIDs are stored as Int128 with a special encoding
    (upper 64 bits offset by ``INT64_MAX + 1``).  The ``UUID`` struct stores
    the canonical unsigned representation as used by the DuckDB value API
    (``duckdb_create_uuid`` / ``duckdb_get_uuid``).
    """

    var value: UInt128

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        # Canonical 8-4-4-4-12 lowercase hex form (e.g. 4f6e...-...-...).
        comptime HEX: StaticString = "0123456789abcdef"
        var v = self.value
        var out = String("")
        for i in range(32):
            if i == 8 or i == 12 or i == 16 or i == 20:
                out += "-"
            var nibble = Int((v >> UInt128(((31 - i) * 4))) & 0xF)
            out += String(HEX[byte=nibble])
        writer.write(out)

    def __eq__(self, other: UUID) -> Bool:
        return self.value == other.value

    def __ne__(self, other: UUID) -> Bool:
        return not self == other

    def __repr__(self) -> String:
        return "UUID(" + String(self.value) + ")"

    def __init__(out self, *, internal: Int128):
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

    def _to_internal(self) -> Int128:
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
struct Interval(TrivialRegisterPassable, Equatable, Writable, ImplicitlyCopyable, Movable):
    """An interval with months, days, and microseconds components."""

    var months: Int32
    var days: Int32
    var micros: Int64

    def __str__(self) -> String:
        return String(self)

    def write_to[W: Writer](self, mut writer: W):
        writer.write(
            "months: ",
            self.months,
            ", days: ",
            self.days,
            ", micros: ",
            self.micros,
        )

    def to_total_seconds(self) -> Float64:
        """Approximate total seconds, assuming 30-day months.

        Note: This is an approximation since month length varies.
        """
        var total_days = Int64(self.months) * 30 + Int64(self.days)
        return (total_days * 86_400_000_000 + self.micros).cast[DType.float64]() / 1_000_000.0

    def __repr__(self) -> String:
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

struct Decimal(TrivialRegisterPassable, ImplicitlyCopyable, Movable, Equatable, Writable):
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

    def __init__(out self, width: UInt8, scale: UInt8, value: Int128):
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

    def value(self) -> Int128:
        var l = self.lower.cast[DType.int128]()
        var u = self.upper.cast[DType.int128]() 
        return (u << 64) | l

    def write_to[W: Writer](self, mut writer: W):
        writer.write("width: ", self.width, ", scale: ", self.scale, ", value: ", self.value())

    def write_repr_to[W: Writer](self, mut writer: W):
        writer.write("Decimal(", self.width, ", ", self.scale, ", ", self.value(), ")"
        )

    def __eq__(self, other: Decimal) -> Bool:
        return (
            self.width == other.width
            and self.scale == other.scale
            and self.value() == other.value()
        )

    def __ne__(self, other: Decimal) -> Bool:
        return not self == other

    def to_float64(self) -> Float64:
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

    def to_float32(self) -> Float32:
        """Convert this Decimal to a Float32.

        Note: This may lose precision for large values or high scales.
        """
        return self.to_float64().cast[DType.float32]()

    def __init__(out self, width: UInt8, scale: UInt8, value: Float64):
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

    def __init__(out self, width: UInt8, scale: UInt8, value: Float32):
        """Create a Decimal from a Float32 with the given width and scale.

        Args:
            width: The total number of decimal digits.
            scale: The number of digits after the decimal point.
            value: The floating-point value to convert.
        """
        self = Decimal(width, scale, value.cast[DType.float64]())


