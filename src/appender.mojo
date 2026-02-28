"""High-level Appender API for efficiently loading data into DuckDB.

Appenders are the most efficient way of loading data into DuckDB, and are
recommended for fast data loading. The appender is much faster than using
prepared statements or individual ``INSERT INTO`` statements.

This module provides a type-safe, idiomatic Mojo API that uses compile-time
reflection to map Mojo structs and tuples to DuckDB rows automatically.

Example — appending individual values:
```mojo
var con = DuckDB.connect(":memory:")
_ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")
var appender = Appender(con, "people")
appender.append_value(Int32(1))
appender.append_value("Mark")
appender.end_row()
appender.append_value(Int32(2))
appender.append_value("Hannes")
appender.end_row()
# appender is flushed and destroyed automatically
```

Example — appending structs:
```mojo
@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String

var appender = Appender(con, "people")
appender.append_row(Person(1, "Mark"))
appender.append_row(Person(2, "Hannes"))
```

Example — appending tuples:
```mojo
var appender = Appender(con, "people")
appender.append_tuple_row(Tuple(Int32(1), String("Mark")))
```

Example — appending from a list:
```mojo
var people = List[Person](Person(1, "Mark"), Person(2, "Hannes"))
var appender = Appender(con, "people")
appender.append_rows(people)
```

Example — nullable columns with Optional:
```mojo
@fieldwise_init
struct PersonOpt(Copyable, Movable):
    var id: Int32
    var name: Optional[String]

var appender = Appender(con, "people")
appender.append_row(PersonOpt(1, String("Mark")))
appender.append_row(PersonOpt(2, None))
```

Example — appending a List column:
```mojo
_ = con.execute("CREATE TABLE t (tags LIST(VARCHAR))")
var appender = Appender(con, "t")
appender.append_value(List[String]("a", "b", "c"))
appender.end_row()
```

Example — appending a Dict as a MAP column:
```mojo
_ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
var appender = Appender(con, "t")
var d: Dict[String, Int32] = {'key1': 10, 'key2': 20}
appender.append_value(d)
appender.end_row()
```

Example — appending a Variant as a UNION column:
```mojo
_ = con.execute("CREATE TABLE t (u UNION(i INTEGER, s VARCHAR))")
var appender = Appender(con, "t")
appender.append_value(Variant[Int32, String](Int32(42)))
appender.end_row()
appender.append_value(Variant[Int32, String](String("hello")))
appender.end_row()
```
"""

from sys.intrinsics import _type_is_eq
from sys.info import size_of
from reflection import (
    get_base_type_name,
    get_type_name,
    is_struct_type,
    struct_field_count,
    struct_field_names,
    struct_field_types,
)
from collections import Optional, List, Dict
from utils import Variant
from builtin.variadics import Variadic
from std.builtin.rebind import downcast, rebind_var, trait_downcast
from memory.unsafe_pointer import alloc
from duckdb._libduckdb import *
from duckdb.duckdb_type import *
from duckdb.api import DuckDB, _get_duckdb_interface
from duckdb.connection import Connection
from duckdb.logical_type import LogicalType
from duckdb.typed_api import (
    mojo_type_to_duckdb_type,
    _is_known_scalar_type,
    _scalar_type_to_duckdb,
)


# ──────────────────────────────────────────────────────────────────
# Value conversion — create duckdb_value from Mojo types
# ──────────────────────────────────────────────────────────────────


fn _to_duckdb_value[T: Copyable & Movable](ref value: T) raises -> duckdb_value:
    """Convert a Mojo value to a duckdb_value.

    Supports scalar types (Bool, integers, floats, String),
    Date/Time/Timestamp/Interval, Decimal, UUID, and timestamp variants.

    The caller is responsible for calling `duckdb_destroy_value` on the result.

    Parameters:
        T: The Mojo type of the value.

    Args:
        value: The value to convert.

    Returns:
        A new duckdb_value that must be destroyed by the caller.
    """
    ref libduckdb = DuckDB().libduckdb()
    # Use UnsafePointer.bitcast to reinterpret ref value as the concrete
    # type without requiring ImplicitlyCopyable.
    var vp = UnsafePointer(to=value)

    @parameter
    if _type_is_eq[T, Bool]():
        return libduckdb.duckdb_create_bool(vp.bitcast[Bool]()[])
    elif _type_is_eq[T, Int8]():
        return libduckdb.duckdb_create_int8(vp.bitcast[Int8]()[])
    elif _type_is_eq[T, Int16]():
        return libduckdb.duckdb_create_int16(vp.bitcast[Int16]()[])
    elif _type_is_eq[T, Int32]():
        return libduckdb.duckdb_create_int32(vp.bitcast[Int32]()[])
    elif _type_is_eq[T, Int64]():
        return libduckdb.duckdb_create_int64(vp.bitcast[Int64]()[])
    elif _type_is_eq[T, Int]():
        # Int is platform-dependent: cast to the appropriate fixed-width type
        @parameter
        if size_of[Int]() == 4:
            return libduckdb.duckdb_create_int32(
                Int32(vp.bitcast[Int]()[])
            )
        else:
            return libduckdb.duckdb_create_int64(
                Int64(vp.bitcast[Int]()[])
            )
    elif _type_is_eq[T, UInt8]():
        return libduckdb.duckdb_create_uint8(vp.bitcast[UInt8]()[])
    elif _type_is_eq[T, UInt16]():
        return libduckdb.duckdb_create_uint16(vp.bitcast[UInt16]()[])
    elif _type_is_eq[T, UInt32]():
        return libduckdb.duckdb_create_uint32(vp.bitcast[UInt32]()[])
    elif _type_is_eq[T, UInt64]():
        return libduckdb.duckdb_create_uint64(vp.bitcast[UInt64]()[])
    elif _type_is_eq[T, UInt]():
        @parameter
        if size_of[UInt]() == 4:
            return libduckdb.duckdb_create_uint32(
                UInt32(vp.bitcast[UInt]()[])
            )
        else:
            return libduckdb.duckdb_create_uint64(
                UInt64(vp.bitcast[UInt]()[])
            )
    elif _type_is_eq[T, Float32]():
        return libduckdb.duckdb_create_float(vp.bitcast[Float32]()[])
    elif _type_is_eq[T, Float64]():
        return libduckdb.duckdb_create_double(vp.bitcast[Float64]()[])
    elif _type_is_eq[T, Int128]():
        return libduckdb.duckdb_create_hugeint(vp.bitcast[Int128]()[])
    elif _type_is_eq[T, UInt128]():
        return libduckdb.duckdb_create_uhugeint(vp.bitcast[UInt128]()[])
    elif _type_is_eq[T, String]():
        var s = String(vp.bitcast[String]()[])
        return libduckdb.duckdb_create_varchar_length(
            s.as_c_string_slice().unsafe_ptr(), idx_t(len(s))
        )
    elif _type_is_eq[T, Date]():
        return libduckdb.duckdb_create_date(vp.bitcast[Date]()[])
    elif _type_is_eq[T, Time]():
        return libduckdb.duckdb_create_time(vp.bitcast[Time]()[])
    elif _type_is_eq[T, Timestamp]():
        return libduckdb.duckdb_create_timestamp(vp.bitcast[Timestamp]()[])
    elif _type_is_eq[T, Interval]():
        return libduckdb.duckdb_create_interval(
            vp.bitcast[duckdb_interval]()[]
        )
    elif _type_is_eq[T, Decimal]():
        return libduckdb.duckdb_create_decimal(vp.bitcast[Decimal]()[])
    elif _type_is_eq[T, TimestampS]():
        var raw = duckdb_timestamp_s(vp.bitcast[TimestampS]()[].seconds)
        return libduckdb.duckdb_create_timestamp_s(raw)
    elif _type_is_eq[T, TimestampMS]():
        var raw = duckdb_timestamp_ms(vp.bitcast[TimestampMS]()[].millis)
        return libduckdb.duckdb_create_timestamp_ms(raw)
    elif _type_is_eq[T, TimestampNS]():
        var raw = duckdb_timestamp_ns(vp.bitcast[TimestampNS]()[].nanos)
        return libduckdb.duckdb_create_timestamp_ns(raw)
    elif _type_is_eq[T, TimestampTZ]():
        var ts = Timestamp(vp.bitcast[TimestampTZ]()[].micros)
        return libduckdb.duckdb_create_timestamp_tz(ts)
    elif _type_is_eq[T, TimeTZ]():
        var raw = duckdb_time_tz(vp.bitcast[TimeTZ]()[].bits)
        return libduckdb.duckdb_create_time_tz_value(raw)
    elif _type_is_eq[T, UUID]():
        return libduckdb.duckdb_create_uuid(vp.bitcast[UUID]()[].value)
    elif _type_is_eq[T, TimeNS]():
        var raw = duckdb_time_ns(vp.bitcast[TimeNS]()[].nanos)
        return libduckdb.duckdb_create_time_ns(raw)
    elif _type_is_eq[T, Bit]():
        var bit_ref = vp.bitcast[Bit]()[].copy()
        # Allocate temp buffer for duckdb_bit.data — padding byte + bit bytes
        var buf = alloc[UInt8](len(bit_ref._data))
        for i in range(len(bit_ref._data)):
            buf[i] = bit_ref._data[i]
        var raw = duckdb_bit(buf, idx_t(len(bit_ref._data)))
        var val = libduckdb.duckdb_create_bit(raw)
        buf.free()
        return val
    else:
        raise Error(
            "Unsupported type for value creation: "
            + String(get_type_name[T]())
        )


# ──────────────────────────────────────────────────────────────────
# Appendable trait — high-level interface for appending typed values
# ──────────────────────────────────────────────────────────────────


trait Appendable:
    """Trait for types that can be appended to a DuckDB Appender.

    Implement this trait to make a type appendable via `Appender.append_value`.
    """

    fn append(ref self, mut appender: Appender) raises:
        """Append this value to the given appender."""
        ...


# ──────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────


fn _get_appender_error(raw: duckdb_appender) -> String:
    """Return the current appender error message, or empty string."""
    ref libduckdb = DuckDB().libduckdb()
    var error_data = libduckdb.duckdb_appender_error_data(raw)
    if not libduckdb.duckdb_error_data_has_error(error_data):
        libduckdb.duckdb_destroy_error_data(
            UnsafePointer(to=error_data)
        )
        return ""
    var msg_ptr = libduckdb.duckdb_error_data_message(error_data)
    var msg = String("")
    if msg_ptr:
        msg = String(unsafe_from_utf8_ptr=msg_ptr)
    libduckdb.duckdb_destroy_error_data(UnsafePointer(to=error_data))
    return msg


# ──────────────────────────────────────────────────────────────────
# Appendable extension conformances
# ──────────────────────────────────────────────────────────────────


__extension Bool(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_bool(appender._appender, self))


# Use SIMD extension to cover all numeric types at once:
# Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64
__extension SIMD(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        constrained[size == 1, "Only scalar SIMD (size=1) can be appended"]()
        ref libduckdb = DuckDB().libduckdb()
        var raw = appender._appender

        @parameter
        if _type_is_eq[Self, Int8]():
            appender._check(libduckdb.duckdb_append_int8(raw, rebind_var[Int8](self)))
        elif _type_is_eq[Self, Int16]():
            appender._check(libduckdb.duckdb_append_int16(raw, rebind_var[Int16](self)))
        elif _type_is_eq[Self, Int32]():
            appender._check(libduckdb.duckdb_append_int32(raw, rebind_var[Int32](self)))
        elif _type_is_eq[Self, Int64]():
            appender._check(libduckdb.duckdb_append_int64(raw, rebind_var[Int64](self)))
        elif _type_is_eq[Self, UInt8]():
            appender._check(libduckdb.duckdb_append_uint8(raw, rebind_var[UInt8](self)))
        elif _type_is_eq[Self, UInt16]():
            appender._check(libduckdb.duckdb_append_uint16(raw, rebind_var[UInt16](self)))
        elif _type_is_eq[Self, UInt32]():
            appender._check(libduckdb.duckdb_append_uint32(raw, rebind_var[UInt32](self)))
        elif _type_is_eq[Self, UInt64]():
            appender._check(libduckdb.duckdb_append_uint64(raw, rebind_var[UInt64](self)))
        elif _type_is_eq[Self, Float32]():
            appender._check(libduckdb.duckdb_append_float(raw, rebind_var[Float32](self)))
        elif _type_is_eq[Self, Float64]():
            appender._check(libduckdb.duckdb_append_double(raw, rebind_var[Float64](self)))
        elif _type_is_eq[Self, Int128]():
            appender._check(libduckdb.duckdb_append_hugeint(raw, rebind_var[Int128](self)))
        elif _type_is_eq[Self, UInt128]():
            appender._check(libduckdb.duckdb_append_uhugeint(raw, rebind_var[UInt128](self)))
        else:
            constrained[False, "Unsupported SIMD DType for appender"]()


# Mojo's native Int/UInt are __mlir_type.index, not SIMD — need separate extensions.
__extension Int(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = appender._appender

        @parameter
        if size_of[Int]() == 4:
            appender._check(libduckdb.duckdb_append_int32(raw, Int32(self)))
        else:
            appender._check(libduckdb.duckdb_append_int64(raw, Int64(self)))


__extension String(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var copy = String(self)
        appender._check(
            libduckdb.duckdb_append_varchar(
                appender._appender, copy.as_c_string_slice().unsafe_ptr()
            ),
        )


__extension Date(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_date(appender._appender, self))


__extension Time(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_time(appender._appender, self))


__extension Timestamp(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(
            libduckdb.duckdb_append_timestamp(appender._appender, self)
        )


__extension Interval(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        # Interval (months: Int32, days: Int32, micros: Int64) must be
        # reinterpreted as duckdb_interval (months_days: Int64, micros: Int64)
        # via memory bitcast, same pattern as src/value.mojo
        appender._check(
            libduckdb.duckdb_append_interval(
                appender._appender,
                UnsafePointer(to=self).bitcast[duckdb_interval]()[],
            ),
        )


__extension Decimal(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        # No dedicated duckdb_append_decimal — use value-based appending.
        var val = libduckdb.duckdb_create_decimal(self)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimestampS(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = duckdb_timestamp_s(self.seconds)
        var val = libduckdb.duckdb_create_timestamp_s(raw)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimestampMS(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = duckdb_timestamp_ms(self.millis)
        var val = libduckdb.duckdb_create_timestamp_ms(raw)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimestampNS(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = duckdb_timestamp_ns(self.nanos)
        var val = libduckdb.duckdb_create_timestamp_ns(raw)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimestampTZ(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var ts = Timestamp(self.micros)
        var val = libduckdb.duckdb_create_timestamp_tz(ts)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimeTZ(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = duckdb_time_tz(self.bits)
        var val = libduckdb.duckdb_create_time_tz_value(raw)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension UUID(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var val = libduckdb.duckdb_create_uuid(self.value)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension TimeNS(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var raw = duckdb_time_ns(self.nanos)
        var val = libduckdb.duckdb_create_time_ns(raw)
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension Bit(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var buf = alloc[UInt8](len(self._data))
        for i in range(len(self._data)):
            buf[i] = self._data[i]
        var raw = duckdb_bit(buf, idx_t(len(self._data)))
        var val = libduckdb.duckdb_create_bit(raw)
        buf.free()
        appender._check(libduckdb.duckdb_append_value(appender._appender, val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=val))


__extension List(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        @parameter
        if _type_is_eq[Self.T, UInt8]():
            ref libduckdb = DuckDB().libduckdb()
            # Access the underlying data pointer directly via UnsafePointer
            # to avoid copying the list.
            var src_ptr = UnsafePointer(to=self).bitcast[List[UInt8]]()
            appender._check(
                libduckdb.duckdb_append_blob(
                    appender._appender,
                    src_ptr[].unsafe_ptr().bitcast[NoneType](),
                    idx_t(len(src_ptr[])),
                ),
            )
        elif _is_known_scalar_type[Self.T]():
            # General list of known scalars — create a list or array value
            ref libduckdb = DuckDB().libduckdb()
            var src_ptr = UnsafePointer(to=self).bitcast[List[Self.T]]()
            var n = len(src_ptr[])

            # Create duckdb_values for each element
            var values = alloc[duckdb_value](n)
            for i in range(n):
                values[i] = _to_duckdb_value(src_ptr[][i])

            # Check target column type to decide LIST vs ARRAY
            var col_type = libduckdb.duckdb_appender_column_type(
                appender._appender, idx_t(appender._current_col)
            )
            var col_type_id = DuckDBType(
                libduckdb.duckdb_get_type_id(col_type)
            )

            var val: duckdb_value
            if col_type_id == DuckDBType.array:
                # ARRAY: need the element type from the column's logical type
                var child_type = libduckdb.duckdb_array_type_child_type(col_type)
                val = libduckdb.duckdb_create_array_value(
                    child_type,
                    values.as_immutable().unsafe_origin_cast[ImmutAnyOrigin](),
                    idx_t(n),
                )
                libduckdb.duckdb_destroy_logical_type(UnsafePointer(to=child_type))
            else:
                # LIST (default)
                var elem_type = LogicalType[True, MutExternalOrigin](
                    mojo_type_to_duckdb_type[Self.T]()
                )
                val = libduckdb.duckdb_create_list_value(
                    elem_type.internal_ptr(),
                    values.as_immutable().unsafe_origin_cast[ImmutAnyOrigin](),
                    idx_t(n),
                )

            appender._check(
                libduckdb.duckdb_append_value(appender._appender, val)
            )

            # Clean up
            for i in range(n):
                libduckdb.duckdb_destroy_value(
                    UnsafePointer(to=values[i])
                )
            values.free()
            libduckdb.duckdb_destroy_value(UnsafePointer(to=val))
            libduckdb.duckdb_destroy_logical_type(UnsafePointer(to=col_type))
        else:
            raise Error(
                "Only List[UInt8] (BLOB) and List[scalar] can be appended."
                " For complex nested types, use a different approach."
            )


__extension Optional(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        if self:
            @parameter
            if conforms_to(Self.T, Appendable):
                trait_downcast[Appendable](self.value()).append(appender)
            else:
                raise Error(
                    "Unsupported inner type for Optional in appender"
                )
        else:
            appender.append_null()


__extension Dict(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        """Append a Dict value to a MAP column.

        The MAP logical type is obtained from the target table's column
        schema via ``duckdb_appender_column_type``.  Each Dict entry
        becomes a key/value pair in the MAP.
        """
        ref libduckdb = DuckDB().libduckdb()

        # Get the MAP logical type from the target column
        var col_type = libduckdb.duckdb_appender_column_type(
            appender._appender, idx_t(appender._current_col)
        )

        var n = len(self)
        var keys = alloc[duckdb_value](n)
        var vals = alloc[duckdb_value](n)

        var i = 0
        for item in self.items():
            keys[i] = _to_duckdb_value(item.key)
            vals[i] = _to_duckdb_value(item.value)
            i += 1

        var map_val = libduckdb.duckdb_create_map_value(
            col_type,
            keys.as_immutable().unsafe_origin_cast[ImmutAnyOrigin](),
            vals.as_immutable().unsafe_origin_cast[ImmutAnyOrigin](),
            idx_t(n),
        )
        appender._check(
            libduckdb.duckdb_append_value(appender._appender, map_val)
        )

        # Clean up
        for j in range(n):
            libduckdb.duckdb_destroy_value(UnsafePointer(to=keys[j]))
            libduckdb.duckdb_destroy_value(UnsafePointer(to=vals[j]))
        keys.free()
        vals.free()
        libduckdb.duckdb_destroy_value(UnsafePointer(to=map_val))
        libduckdb.duckdb_destroy_logical_type(UnsafePointer(to=col_type))


__extension Variant(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        """Append a Variant value to a UNION column.

        The UNION logical type (including member names) is obtained from the
        target table's column schema via ``duckdb_appender_column_type``.
        The Variant's discriminant maps to the UNION tag index and the active
        value is converted to a ``duckdb_value``.

        Positional mapping: ``Variant`` type index ``i`` corresponds to UNION
        member ``i``.  Member names are not required from the Mojo side.
        """
        ref libduckdb = DuckDB().libduckdb()
        var tag = Int(self._get_discr())

        # Get the UNION logical type from the target column's schema
        var col_type = libduckdb.duckdb_appender_column_type(
            appender._appender, idx_t(appender._current_col)
        )

        # Create the member value by matching the tag at runtime
        var member_val = duckdb_value()
        comptime for i in range(Variadic.size(Self.Ts)):
            if tag == i:
                comptime MemberType = Self.Ts[i]
                comptime MT = downcast[MemberType, Copyable & Movable]
                member_val = _to_duckdb_value(self.unsafe_get[MT]())

        # Create the union value and append it
        var union_val = libduckdb.duckdb_create_union_value(
            col_type, idx_t(tag), member_val
        )
        appender._check(
            libduckdb.duckdb_append_value(appender._appender, union_val)
        )

        # Clean up
        libduckdb.duckdb_destroy_value(UnsafePointer(to=member_val))
        libduckdb.duckdb_destroy_value(UnsafePointer(to=union_val))
        libduckdb.duckdb_destroy_logical_type(UnsafePointer(to=col_type))


# ──────────────────────────────────────────────────────────────────
# Appender — high-level API
# ──────────────────────────────────────────────────────────────────


struct Appender(Movable):
    """Efficiently load data into a DuckDB table.

    Appenders are the fastest way to insert data into DuckDB from Mojo.
    They support appending individual values, complete rows as structs or
    tuples, and bulk-appending from lists.

    ``Optional`` fields/values map to SQL ``NULL``.

    The appender is automatically flushed and destroyed when it goes out
    of scope.
    """

    var _appender: duckdb_appender
    var _current_col: Int
    """Tracks the current column index for value-level appending.

    Used internally by `Variant(Appendable)` to query the target column's
    UNION logical type from the table schema.
    """

    # ── Construction / Destruction ────────────────────────────────

    fn __init__(out self, ref con: Connection, table: String, schema: String = "") raises:
        """Create an appender for the given table.

        Args:
            con: An open DuckDB connection.
            table: The table name to append to.
            schema: The schema name (empty string for default schema).
        """
        self._appender = duckdb_appender()
        self._current_col = 0
        var _table = table.copy()
        ref libduckdb = DuckDB().libduckdb()

        var state: duckdb_state
        if schema == "":
            state = libduckdb.duckdb_appender_create(
                con._conn,
                UnsafePointer[c_char, ImmutAnyOrigin](),
                _table.as_c_string_slice().unsafe_ptr(),
                UnsafePointer(to=self._appender),
            )
        else:
            var _schema = schema.copy()
            state = libduckdb.duckdb_appender_create(
                con._conn,
                _schema.as_c_string_slice().unsafe_ptr(),
                _table.as_c_string_slice().unsafe_ptr(),
                UnsafePointer(to=self._appender),
            )
        if state == DuckDBError:
            var err = _get_appender_error(self._appender)
            # Still need to destroy even on error
            _ = libduckdb.duckdb_appender_destroy(UnsafePointer(to=self._appender))
            raise Error("Failed to create appender: " + err)

    fn __moveinit__(out self, deinit take: Self):
        self._appender = take._appender
        self._current_col = take._current_col

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        # duckdb_appender_destroy flushes, closes, and frees
        _ = libduckdb.duckdb_appender_destroy(UnsafePointer(to=self._appender))

    # ── Error handling ────────────────────────────────────────────

    fn _check(self, state: duckdb_state) raises:
        """Check a return state and raise on error."""
        if state == DuckDBError:
            raise Error("Appender error: " + _get_appender_error(self._appender))

    # ── Low-level value appending ─────────────────────────────────

    fn append_null(mut self) raises:
        """Append a NULL value for the current column."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_append_null(self._appender))
        self._current_col += 1

    fn append_default(mut self) raises:
        """Append the DEFAULT value for the current column."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_append_default(self._appender))
        self._current_col += 1

    fn append_value[T: Copyable & Movable](mut self, value: T) raises:
        """Append a single typed value to the current row.

        Supports all DuckDB-mappable scalar types, String, and Optional[T]
        (which appends NULL for None).

        Parameters:
            T: The Mojo type of the value.

        Args:
            value: The value to append.
        """
        @parameter
        if _type_is_eq[T, UInt]():
            # UInt is a type alias, not a struct, so it can't have an
            # __extension Appendable.  Handle it inline instead.
            ref libduckdb = DuckDB().libduckdb()
            var raw = self._appender
            var vp = UnsafePointer(to=value).bitcast[UInt]()

            @parameter
            if size_of[UInt]() == 4:
                self._check(
                    libduckdb.duckdb_append_uint32(raw, UInt32(vp[]))
                )
            else:
                self._check(
                    libduckdb.duckdb_append_uint64(raw, UInt64(vp[]))
                )
            self._current_col += 1
        elif conforms_to(T, Appendable):
            trait_downcast[Appendable](value).append(self)
            self._current_col += 1
        else:
            raise Error(
                "Unsupported type for appender: "
                + String(get_type_name[T]())
            )

    # ── Row-level appending ───────────────────────────────────────

    fn end_row(mut self) raises:
        """Finish the current row and advance to the next."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_end_row(self._appender))
        self._current_col = 0

    fn begin_row(mut self) raises:
        """Begin a new row (optional — rows begin automatically)."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_begin_row(self._appender))
        self._current_col = 0

    fn append_row[T: Copyable & Movable](mut self, row: T) raises:
        """Append a struct as a complete table row.

        Each struct field is mapped to a column by position. Field types
        must be DuckDB-mappable. ``Optional`` fields become ``NULL``.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            row: The struct instance to append.

        Example:
            ```mojo
            @fieldwise_init
            struct Person(Copyable, Movable):
                var id: Int32
                var name: String

            appender.append_row(Person(1, "Mark"))
            ```
        """
        constrained[
            mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t,
            "append_row[T] requires a struct type. For scalar values use append_value.",
        ]()
        self._append_struct_fields(row)
        self.end_row()

    fn _append_struct_fields[T: Copyable & Movable](mut self, ref row: T) raises:
        """Append all fields of a struct as values in the current row."""
        comptime field_count = struct_field_count[T]()

        @parameter
        for idx in range(field_count):
            comptime FieldType = struct_field_types[T]()[idx]

            @parameter
            if conforms_to(FieldType, Appendable):
                trait_downcast[Appendable](
                    __struct_field_ref(idx, row)
                ).append(self)
                self._current_col += 1
            else:
                comptime field_name = struct_field_names[T]()[idx]
                raise Error(
                    "Unsupported field type for appender: field '"
                    + String(field_name)
                    + "'"
                )

    fn append_tuple_row[*Ts: Copyable & Movable](mut self, row: Tuple[*Ts]) raises:
        """Append a tuple as a complete table row.

        Each tuple element maps to a column by position.

        Parameters:
            Ts: The types of the tuple elements.

        Args:
            row: The tuple to append.

        Example:
            ```mojo
            appender.append_tuple_row(Tuple(Int32(1), String("Mark")))
            ```
        """
        self._append_tuple_elements(row)
        self.end_row()

    fn _append_tuple_elements[
        *Ts: Copyable & Movable
    ](mut self, ref row: Tuple[*Ts]) raises:
        """Append all elements of a tuple as values in the current row."""
        comptime T = Tuple[*Ts]
        comptime n = T.__len__()

        @parameter
        for idx in range(n):
            comptime ET = T.element_types[idx]

            @parameter
            if conforms_to(ET, Appendable):
                trait_downcast[Appendable](row[idx]).append(
                    self
                )
                self._current_col += 1
            else:
                raise Error(
                    "Unsupported tuple element type at index "
                    + String(idx)
                )

    # ── Bulk appending ────────────────────────────────────────────

    fn append_rows[T: Copyable & Movable](mut self, rows: List[T]) raises:
        """Append multiple structs as table rows.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            rows: A list of struct instances to append.

        Example:
            ```mojo
            var people = List[Person](Person(1, "Mark"), Person(2, "Hannes"))
            appender.append_rows(people)
            ```
        """
        for i in range(len(rows)):
            self.append_row(rows[i])

    # ── Flush / Close ─────────────────────────────────────────────

    fn flush(mut self) raises:
        """Flush the appender to the table, forcing the cache of the
        appender to be cleared. If flushing fails, the appender remains
        valid. Subsequent rows can still be appended after flushing.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_flush(self._appender))

    fn close(mut self) raises:
        """Close the appender. Unflushed data is flushed first.

        After closing, no more rows can be appended.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_close(self._appender))

    # ── Metadata ──────────────────────────────────────────────────

    fn column_count(self) -> Int:
        """Return the number of columns in the appender's target table."""
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_appender_column_count(self._appender))
