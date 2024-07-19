from duckdb._libduckdb import *
from duckdb.api import _impl


@value
@register_passable("trivial")
struct DuckDBType(
    Stringable,
    Formattable,
    CollectionElementNew,
    EqualityComparable,
    KeyElement,
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

    # fn __init__(inout self, value: LogicalType):
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

    @always_inline
    fn __init__(inout self, *, other: Self):
        """Copy this DuckDBType.

        Args:
            other: The DuckDBType to copy.
        """
        self = other

    fn __hash__(self) -> UInt:
        return self.value.__hash__()

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


@value
struct Date(EqualityComparable, Formattable, Representable, Stringable):
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

    fn __repr__(self) -> String:
        return "Date(" + str(self.days) + ")"

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
struct Time(EqualityComparable, Formattable, Representable, Stringable):
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

    fn __repr__(self) -> String:
        return "Time(" + str(self.micros) + ")"

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
struct Timestamp(EqualityComparable, Formattable, Stringable, Representable):
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

    fn __repr__(self) -> String:
        return "Timestamp(" + str(self.micros) + ")"

    # fn date(self) -> Date:
    #     return _impl().duckdb_to_date(_impl().duckdb_from_timestamp(self).date)

    # fn time(self) -> Time:
    #     return _impl().duckdb_to_time(_impl().duckdb_from_timestamp(self).time)


@value
struct Interval(Stringable, Representable):
    var months: Int32
    var days: Int32
    var micros: Int64

    fn __str__(self) -> String:
        return (
            "months: "
            + str(self.months)
            + ", days: "
            + str(self.days)
            + ", micros: "
            + str(self.micros)
        )

    fn __repr__(self) -> String:
        return (
            "Interval("
            + str(self.months)
            + ", "
            + str(self.days)
            + ", "
            + str(self.micros)
            + ")"
        )


@value
struct Int128(Stringable, Representable):
    """Hugeints are composed of a (lower, upper) component.

    The value of the hugeint is upper * 2^64 + lower
    For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
    """

    var lower: UInt64
    var upper: Int64

    fn __str__(self) -> String:
        return "lower: " + str(self.lower) + ", upper: " + str(self.upper)

    fn __repr__(self) -> String:
        return "Int128(" + str(self.lower) + ", " + str(self.upper) + ")"


@value
struct UInt128(Stringable, Representable):
    """UHugeints are composed of a (lower, upper) component."""

    var lower: UInt64
    var upper: UInt64

    fn __str__(self) -> String:
        return "lower: " + str(self.lower) + ", upper: " + str(self.upper)

    fn __repr__(self) -> String:
        return "UInt128(" + str(self.lower) + ", " + str(self.upper) + ")"


@value
struct Decimal(Stringable, Representable):
    """Decimals are composed of a width and a scale, and are stored in a hugeint.
    """

    var width: UInt8
    var scale: UInt8
    var value: UInt128

    fn __str__(self) -> String:
        return (
            "width: "
            + str(self.width)
            + ", scale: "
            + str(self.scale)
            + ", value: "
            + str(self.value)
        )

    fn __repr__(self) -> String:
        return (
            "Decimal("
            + str(self.width)
            + ", "
            + str(self.scale)
            + ", "
            + str(self.value)
            + ")"
        )


trait DBVal(CollectionElement, Stringable):
    """Represents a DuckDB value of any supported type."""

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        pass

    @staticmethod
    fn type() -> DuckDBType:
        pass


trait KeyElementVal(DBVal, KeyElement):
    pass


@value
struct DTypeVal[duckdb_type: DuckDBType](DBVal, KeyElementVal):
    var value: Scalar[duckdb_type.to_dtype()]

    fn __str__(self) -> String:
        return self.value.__str__()

    fn __hash__(self) -> UInt:
        return self.value.__hash__()

    fn __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: Self) -> Bool:
        return self.value != other.value

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != duckdb_type:
            raise "Expected type " + str(duckdb_type) + " but got " + str(
                vector.get_column_type().get_type_id()
            )
        self = vector.get_value[Self](offset=offset)

    @staticmethod
    fn type() -> DuckDBType:
        return duckdb_type


alias BoolVal = DTypeVal[DuckDBType.boolean]
alias Int8Val = DTypeVal[DuckDBType.tinyint]
alias Int16Val = DTypeVal[DuckDBType.smallint]
alias Int32Val = DTypeVal[DuckDBType.integer]
alias Int64Val = DTypeVal[DuckDBType.bigint]
alias UInt8Val = DTypeVal[DuckDBType.utinyint]
alias UInt16Val = DTypeVal[DuckDBType.usmallint]
alias UInt32Val = DTypeVal[DuckDBType.uinteger]
alias UInt64Val = DTypeVal[DuckDBType.ubigint]
alias Float32Val = DTypeVal[DuckDBType.float]
alias Float64Val = DTypeVal[DuckDBType.double]


@value
struct StringVal(DBVal):
    var value: String

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != DuckDBType.varchar:
            raise "Expected type " + str(
                DuckDBType.varchar
            ) + " but got " + str(vector.get_column_type().get_type_id())
        var data_str_ptr = vector._get_data().bitcast[duckdb_string_t_pointer]()
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = int(data_str_ptr[offset].length)
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[offset].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[
                duckdb_string_t_inlined
            ]()
            self.value = StringRef(
                data_str_inlined[offset].inlined.unsafe_ptr(), string_length
            )
        else:
            self.value = StringRef(data_str_ptr[offset].ptr, string_length)

    fn __str__(self) -> String:
        return self.value

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.varchar


@value
struct ListVal[T: DBVal](DBVal):
    """A DuckDB list."""

    alias expected_element_type = T.type()
    var value: List[Optional[T]]

    fn __str__(self) -> String:
        return "ListVal"  # TODO

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        var runtime_element_type = vector.get_column_type().get_type_id()
        if runtime_element_type != Self.expected_element_type:
            raise "Expected type " + str(
                Self.expected_element_type
            ) + " but got " + str(runtime_element_type)
        self.value = List[Optional[T]](capacity=length)
        if Self.expected_element_type.is_fixed_size():
            # if the element type is fixed size, we can directly get all values from the vector
            # that way we can avoid calling the constructor for each element
            self.value = vector.get_fixed_size_values[T](
                length=length, offset=offset
            )
            # for i in range(length):
            #     self.value.append(T(vector, length=1, offset=int(offset + i)))
        elif Self.expected_element_type == DuckDBType.varchar:
            for i in range(length):
                self.value.append(T(vector, length=1, offset=offset + i))
        elif Self.expected_element_type == DuckDBType.list:
            # pointer to list metadata (length and offset) that allows us to get the
            # correct positions of the actual data in the child vector
            var data_ptr = vector._get_data().bitcast[duckdb_list_entry]()
            # The child vector holds the actual list data in variable size entries (list_entry.length)
            var child_vector = vector.list_get_child()
            for i in range(length):
                var list_entry = data_ptr[offset + i]
                # if the subtime is a list itself, we need to call the constructor for each element recursively
                self.value.append(
                    Optional(
                        T(
                            child_vector,
                            length=int(list_entry.length),
                            offset=int(list_entry.offset),
                        )
                    )
                )
        else:
            raise Error(
                "Unsupported or invalid type: " + str(runtime_element_type)
            )

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.list


@value
struct MapVal[K: KeyElementVal, V: DBVal](DBVal):
    var value: Dict[K, V]

    fn __str__(self) -> String:
        return "MapVal"  # TODO

    fn __init__(
        inout self, vector: Vector, length: Int, offset: Int = 0
    ) raises:
        self.value = Dict[K, V]()
        raise "Not implemented"
        # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
        # Via https://duckdb.org/docs/internals/vector#map-vectors
        # TODO fill dict
        # for i in range(length):
        #     self.key = K()
        #     self.value = V()

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.map
