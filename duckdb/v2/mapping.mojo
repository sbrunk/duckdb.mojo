"""The mapping between Mojo types and DuckDB's scalar types.

`DuckDBMapped` is implemented (through `__extension`) for `Bool`, every
numeric scalar (`Int8`..`Int64`, `Int`, `UInt8`..`UInt64`, `Float32`,
`Float64`, `Int128`, `UInt128`), `String`, `Blob` and the value structs of
`duckdb.duckdb_type` (`Date`, `Time`, `TimeNS`, `TimeTZ`, `Timestamp`,
`TimestampS`, `TimestampMS`, `TimestampNS`, `TimestampTZ`, `TimestampTZNS`,
`Interval`, `Decimal`, `UUID`, `Bit`).

Each type reads its value straight from DuckDB's storage layout. Storage
details are decoded here and only here: the 16-byte `bytes` layout of
VARCHAR/BLOB/BIT, the UUID bit flip, decimal width tiers, enum index widths.

Nested types (lists, structs, maps, unions, optionals) are not here: they go
through emberserde in a later phase (see docs/design/v2-high-level-api.md).
"""

from std.builtin.rebind import downcast, rebind_var
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.duckdb_type import (
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
from duckdb.v2._runtime import api_error, check, lib
from duckdb.v2.error import DuckDBError, ErrorCode
from duckdb.v2.types import LogicalType, TypeId


comptime RawData = Pointer[NoneType, ImmUntrackedOrigin]


struct ColumnMeta(Copyable, Movable):
    """What a reader needs to know about a column's type, read once per column."""

    var kind: TypeId
    var decimal_width: Int
    var decimal_scale: Int
    var enum_values: List[String]
    var enum_index_bytes: Int

    def __init__(out self, type: LogicalType) raises DuckDBError:
        self.kind = type.id()
        self.decimal_width = 0
        self.decimal_scale = 0
        self.enum_values = List[String]()
        self.enum_index_bytes = 0
        if self.kind == TypeId.DECIMAL:
            self.decimal_width = type.decimal_width()
            self.decimal_scale = type.decimal_scale()
        elif self.kind == TypeId.ENUM:
            self.enum_values = type.enum_values()
            # Storage width follows the dictionary size (contract in the V2 spec).
            var n = len(self.enum_values)
            self.enum_index_bytes = 1 if n <= 255 else (2 if n <= 65535 else 4)


trait DuckDBMapped(Movable):
    """A Mojo type that maps to a DuckDB scalar type."""

    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        """The DuckDB type a value of this Mojo type is written as."""
        ...

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        """Whether a column of this type id can be read as this Mojo type."""
        ...

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        """Read the (valid) value at storage index `idx`."""
        ...

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        """Create an owned DuckDB value."""
        ...

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        """Read a (non-NULL) DuckDB value."""
        ...


# ===--------------------------------------------------------------------===#
# Dispatch
# ===--------------------------------------------------------------------===#
#
# Public generics take a plain `T` and call these instead of using
# `DuckDBMapped` as a bound. In Mojo 1.2 a conformance added with
# `__extension` is only visible to `conforms_to` checks in the module that
# declares it: another module using `T: DuckDBMapped` would reject `Int64` or
# `String`. These functions live next to the extensions, so the check works,
# and an unmapped type is a readable compile error.

comptime _UNMAPPED = "this type has no DuckDB mapping (it must conform to DuckDBMapped)"


def mapped_logical_type[T: AnyType]() raises DuckDBError -> LogicalType:
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    return downcast[T, DuckDBMapped].logical_type()


def mapped_accepts[T: AnyType](kind: TypeId) -> Bool:
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    return downcast[T, DuckDBMapped].accepts(kind)


@always_inline
def mapped_read[T: AnyType](meta: ColumnMeta, data: RawData, idx: Int) -> T:
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    return rebind_var[T](downcast[T, DuckDBMapped].read(meta, data, idx))


def mapped_to_value[T: AnyType](value: T, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    return rebind[downcast[T, DuckDBMapped]](value).to_value(factory)


def mapped_from_value[T: AnyType](value: duckdb_v2_value_handle) raises DuckDBError -> T:
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    return rebind_var[T](downcast[T, DuckDBMapped].from_value(value))


# ===--------------------------------------------------------------------===#
# Storage helpers
# ===--------------------------------------------------------------------===#


@always_inline
def read_bytes(data: RawData, idx: Int) -> Span[UInt8, ImmUntrackedOrigin]:
    """The bytes of a VARCHAR/BLOB/BIT value (16-byte `bytes` storage)."""
    var base = data.unsafe_bitcast[UInt8]().unsafe_offset(idx * 16)
    var length = Int(base.unsafe_bitcast[UInt32]()[])
    var ptr: Pointer[UInt8, ImmUntrackedOrigin]
    if length <= Int(DUCKDB_V2_BYTES_INLINE_LENGTH):
        ptr = base.unsafe_offset(4)
    else:
        ptr = base.unsafe_offset(8).unsafe_bitcast[Pointer[UInt8, ImmUntrackedOrigin]]()[]
    return Span[UInt8, ImmUntrackedOrigin](unsafe_ptr=ptr, length=length)


@always_inline
def _load[T: TrivialRegisterPassable](data: RawData, idx: Int) -> T:
    return data.unsafe_bitcast[T]()[unsafe_offset=idx]


def _hugeint(v: Int128) -> duckdb_v2_hugeint_t:
    return duckdb_v2_hugeint_t(UInt64(v & Int128(0xFFFFFFFFFFFFFFFF)), Int64(v >> 64))


def _from_hugeint(h: duckdb_v2_hugeint_t) -> Int128:
    return (Int128(h.upper) << 64) | Int128(h.lower)


def _new_value(code: DUCKDB_V2_ERROR, out_value: duckdb_v2_value_handle, mut err: duckdb_v2_error_info_handle) raises DuckDBError -> duckdb_v2_value_handle:
    check(code, err)
    return out_value


def _value_bytes(value: duckdb_v2_value_handle, varchar: Bool) raises DuckDBError -> List[UInt8]:
    var s = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
    var err = null_error_info()
    if varchar:
        check(lib().duckdb_v2_value_get_varchar(value, Pointer(to=s), Pointer(to=err)), err)
    else:
        check(lib().duckdb_v2_value_get_blob(value, Pointer(to=s), Pointer(to=err)), err)
    var out = List[UInt8](capacity=Int(s.len))
    for i in range(Int(s.len)):
        out.append(s.ptr.unsafe_bitcast[UInt8]()[unsafe_offset=i])
    return out^


def _bytes_arg(bytes: Span[UInt8, _]) -> duckdb_v2_str:
    return duckdb_v2_str(
        bytes.unsafe_ptr().unsafe_bitcast[c_char]().as_imm().unsafe_origin_cast[ImmUntrackedOrigin](),
        idx_t(len(bytes)),
    )


# ===--------------------------------------------------------------------===#
# Bool and numbers
# ===--------------------------------------------------------------------===#


__extension Bool(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.BOOLEAN)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.BOOLEAN

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return _load[UInt8](data, idx) != 0

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_bool(factory, self, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var out = False
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_bool(value, Pointer(to=out), Pointer(to=err)), err)
        return out


def _numeric_type_id[dt: DType]() -> TypeId:
    comptime if dt == DType.bool:
        return TypeId.BOOLEAN
    elif dt == DType.int8:
        return TypeId.TINYINT
    elif dt == DType.int16:
        return TypeId.SMALLINT
    elif dt == DType.int32:
        return TypeId.INTEGER
    elif dt == DType.int64 or dt == DType.int:
        return TypeId.BIGINT
    elif dt == DType.uint8:
        return TypeId.UTINYINT
    elif dt == DType.uint16:
        return TypeId.USMALLINT
    elif dt == DType.uint32:
        return TypeId.UINTEGER
    elif dt == DType.uint64 or dt == DType.uint:
        return TypeId.UBIGINT
    elif dt == DType.int128:
        return TypeId.HUGEINT
    elif dt == DType.uint128:
        return TypeId.UHUGEINT
    elif dt == DType.float32:
        return TypeId.FLOAT
    elif dt == DType.float64:
        return TypeId.DOUBLE
    else:
        comptime assert False, "this DType has no DuckDB type"


__extension SIMD(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        return LogicalType(_numeric_type_id[Self.dtype]())

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == _numeric_type_id[Self.dtype]()

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        comptime if Self.dtype == DType.bool:
            return Self(_load[UInt8](data, idx) != 0)
        elif Self.dtype == DType.int:
            return Self(_load[Int64](data, idx).cast[Self.dtype]())
        elif Self.dtype == DType.uint:
            return Self(_load[UInt64](data, idx).cast[Self.dtype]())
        else:
            return rebind[Self](_load[Scalar[Self.dtype]](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        var p = Pointer(to=v)
        var e = Pointer(to=err)
        var code: DUCKDB_V2_ERROR
        comptime dt = Self.dtype
        var x = rebind[Scalar[dt]](self)
        comptime if dt == DType.bool:
            code = lib().duckdb_v2_value_create_bool(factory, Bool(x), p, e)
        elif dt == DType.int8:
            code = lib().duckdb_v2_value_create_tinyint(factory, x.cast[DType.int8](), p, e)
        elif dt == DType.int16:
            code = lib().duckdb_v2_value_create_smallint(factory, x.cast[DType.int16](), p, e)
        elif dt == DType.int32:
            code = lib().duckdb_v2_value_create_int(factory, x.cast[DType.int32](), p, e)
        elif dt == DType.int64 or dt == DType.int:
            code = lib().duckdb_v2_value_create_bigint(factory, x.cast[DType.int64](), p, e)
        elif dt == DType.uint8:
            code = lib().duckdb_v2_value_create_utinyint(factory, x.cast[DType.uint8](), p, e)
        elif dt == DType.uint16:
            code = lib().duckdb_v2_value_create_usmallint(factory, x.cast[DType.uint16](), p, e)
        elif dt == DType.uint32:
            code = lib().duckdb_v2_value_create_uint(factory, x.cast[DType.uint32](), p, e)
        elif dt == DType.uint64 or dt == DType.uint:
            code = lib().duckdb_v2_value_create_ubigint(factory, x.cast[DType.uint64](), p, e)
        elif dt == DType.int128:
            var h = _hugeint(x.cast[DType.int128]())
            code = lib().duckdb_v2_value_create_hugeint(factory, Pointer(to=h), p, e)
        elif dt == DType.uint128:
            var u = x.cast[DType.uint128]()
            var h = duckdb_v2_uhugeint_t(UInt64(u & UInt128(0xFFFFFFFFFFFFFFFF)), UInt64(u >> 64))
            code = lib().duckdb_v2_value_create_uhugeint(factory, Pointer(to=h), p, e)
        elif dt == DType.float32:
            code = lib().duckdb_v2_value_create_float(factory, x.cast[DType.float32](), p, e)
        elif dt == DType.float64:
            code = lib().duckdb_v2_value_create_double(factory, x.cast[DType.float64](), p, e)
        else:
            comptime assert False, "this DType has no DuckDB type"
        return _new_value(code, v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        comptime dt = Self.dtype
        var err = null_error_info()
        var e = Pointer(to=err)
        comptime if dt == DType.bool:
            var o = False
            check(lib().duckdb_v2_value_get_bool(value, Pointer(to=o), e), err)
            return Self(o)
        elif dt == DType.int8:
            var o: Int8 = 0
            check(lib().duckdb_v2_value_get_tinyint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.int16:
            var o: Int16 = 0
            check(lib().duckdb_v2_value_get_smallint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.int32:
            var o: Int32 = 0
            check(lib().duckdb_v2_value_get_int(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.int64 or dt == DType.int:
            var o: Int64 = 0
            check(lib().duckdb_v2_value_get_bigint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.uint8:
            var o: UInt8 = 0
            check(lib().duckdb_v2_value_get_utinyint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.uint16:
            var o: UInt16 = 0
            check(lib().duckdb_v2_value_get_usmallint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.uint32:
            var o: UInt32 = 0
            check(lib().duckdb_v2_value_get_uint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.uint64 or dt == DType.uint:
            var o: UInt64 = 0
            check(lib().duckdb_v2_value_get_ubigint(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.int128:
            var h = duckdb_v2_hugeint_t(0, 0)
            check(lib().duckdb_v2_value_get_hugeint(value, Pointer(to=h), e), err)
            return _from_hugeint(h).cast[dt]()
        elif dt == DType.uint128:
            var h = duckdb_v2_uhugeint_t(0, 0)
            check(lib().duckdb_v2_value_get_uhugeint(value, Pointer(to=h), e), err)
            return ((UInt128(h.upper) << 64) | UInt128(h.lower)).cast[dt]()
        elif dt == DType.float32:
            var o: Float32 = 0
            check(lib().duckdb_v2_value_get_float(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        elif dt == DType.float64:
            var o: Float64 = 0
            check(lib().duckdb_v2_value_get_double(value, Pointer(to=o), e), err)
            return o.cast[dt]()
        else:
            comptime assert False, "this DType has no DuckDB type"


# ===--------------------------------------------------------------------===#
# Strings and bytes
# ===--------------------------------------------------------------------===#


__extension String(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.VARCHAR)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.VARCHAR or kind == TypeId.ENUM

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        if meta.kind == TypeId.ENUM:
            var i: Int
            if meta.enum_index_bytes == 1:
                i = Int(_load[UInt8](data, idx))
            elif meta.enum_index_bytes == 2:
                i = Int(_load[UInt16](data, idx))
            else:
                i = Int(_load[UInt32](data, idx))
            return meta.enum_values[i].copy()
        return String(from_utf8_lossy=read_bytes(data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var arg = StrArg(self)
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_varchar(factory, arg.ptr(), Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var s = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_varchar(value, Pointer(to=s), Pointer(to=err)), err)
        return str_to_string(s)


@fieldwise_init
struct Blob(Copyable, Equatable, Movable, Sized, Writable):
    """The bytes of a BLOB value."""

    var data: List[UInt8]

    def __len__(self) -> Int:
        return len(self.data)

    def __eq__(self, other: Self) -> Bool:
        return self.data == other.data

    def write_to(self, mut writer: Some[Writer]):
        writer.write("Blob(", len(self.data), " bytes)")


def _copy_bytes(s: Span[UInt8, _]) -> List[UInt8]:
    var out = List[UInt8](capacity=len(s))
    for b in s:
        out.append(b)
    return out^


__extension Blob(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.BLOB)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.BLOB

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return Blob(_copy_bytes(read_bytes(data, idx)))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var arg = _bytes_arg(Span(self.data))
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        var code = lib().duckdb_v2_value_create_blob(factory, Pointer(to=arg), Pointer(to=v), Pointer(to=err))
        _ = self.data  # the view points into it
        return _new_value(code, v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        return Blob(_value_bytes(value, varchar=False))


__extension Bit(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.BIT)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.BIT

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        # Byte 0 is the number of padding bits, the rest the packed bits.
        var bytes = _copy_bytes(read_bytes(data, idx))
        var size = (len(bytes) - 1) * 8 - Int(bytes[0])
        return Bit(bytes^, size)

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var arg = _bytes_arg(Span(self._data))
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        var code = lib().duckdb_v2_value_create_bit(factory, Pointer(to=arg), Pointer(to=v), Pointer(to=err))
        _ = self._data  # the view points into it
        return _new_value(code, v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var bytes = _value_bytes(value, varchar=False)
        var size = (len(bytes) - 1) * 8 - Int(bytes[0])
        return Bit(bytes^, size)


# ===--------------------------------------------------------------------===#
# Date and time
# ===--------------------------------------------------------------------===#


__extension Date(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.DATE)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.DATE

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return Date(_load[Int32](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_date(factory, self.days, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int32 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_date(value, Pointer(to=o), Pointer(to=err)), err)
        return Date(o)


__extension Time(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIME)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIME

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return Time(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_time(factory, self.micros, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_time(value, Pointer(to=o), Pointer(to=err)), err)
        return Time(o)


__extension TimeNS(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIME_NS)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIME_NS

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimeNS(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_time_ns(factory, self.nanos, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_time_ns(value, Pointer(to=o), Pointer(to=err)), err)
        return TimeNS(o)


__extension TimeTZ(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIME_TZ)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIME_TZ

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimeTZ(_load[UInt64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_time_tz(factory, self.bits, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: UInt64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_time_tz(value, Pointer(to=o), Pointer(to=err)), err)
        return TimeTZ(o)


__extension Timestamp(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return Timestamp(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp(factory, self.micros, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp(value, Pointer(to=o), Pointer(to=err)), err)
        return Timestamp(o)


__extension TimestampS(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP_SEC)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP_SEC

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimestampS(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp_sec(factory, self.seconds, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp_sec(value, Pointer(to=o), Pointer(to=err)), err)
        return TimestampS(o)


__extension TimestampMS(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP_MS)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP_MS

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimestampMS(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp_ms(factory, self.millis, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp_ms(value, Pointer(to=o), Pointer(to=err)), err)
        return TimestampMS(o)


__extension TimestampNS(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP_NS)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP_NS

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimestampNS(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp_ns(factory, self.nanos, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp_ns(value, Pointer(to=o), Pointer(to=err)), err)
        return TimestampNS(o)


__extension TimestampTZ(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP_TZ)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP_TZ

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimestampTZ(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp_tz(factory, self.micros, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp_tz(value, Pointer(to=o), Pointer(to=err)), err)
        return TimestampTZ(o)


__extension TimestampTZNS(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.TIMESTAMP_TZ_NS)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.TIMESTAMP_TZ_NS

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return TimestampTZNS(_load[Int64](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_timestamp_tz_ns(factory, self.nanos, Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var o: Int64 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_timestamp_tz_ns(value, Pointer(to=o), Pointer(to=err)), err)
        return TimestampTZNS(o)


__extension Interval(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.INTERVAL)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.INTERVAL

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        ref i = data.unsafe_bitcast[duckdb_v2_interval_t]()[unsafe_offset=idx]
        return Interval(i.months, i.days, i.micros)

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var i = duckdb_v2_interval_t(self.months, self.days, self.micros)
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_interval(factory, Pointer(to=i), Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var i = duckdb_v2_interval_t(0, 0, 0)
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_interval(value, Pointer(to=i), Pointer(to=err)), err)
        return Interval(i.months, i.days, i.micros)


# ===--------------------------------------------------------------------===#
# Decimal and UUID
# ===--------------------------------------------------------------------===#


__extension Decimal(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        # A Decimal value carries its own width and scale; there is no single
        # type for the Mojo type.
        raise api_error("Decimal has no fixed DuckDB type: build one with LogicalType.decimal(width, scale)")

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.DECIMAL

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        # Storage follows the width (contract in the V2 spec).
        var w = meta.decimal_width
        var v: Int128
        if w <= 4:
            v = Int128(_load[Int16](data, idx))
        elif w <= 9:
            v = Int128(_load[Int32](data, idx))
        elif w <= 18:
            v = Int128(_load[Int64](data, idx))
        else:
            v = _load[Int128](data, idx)
        return Decimal(UInt8(w), UInt8(meta.decimal_scale), v)

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var h = _hugeint(self.value())
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(
            lib().duckdb_v2_value_create_decimal(factory, Pointer(to=h), self.width, self.scale, Pointer(to=v), Pointer(to=err)),
            v,
            err,
        )

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var h = duckdb_v2_hugeint_t(0, 0)
        var w: UInt8 = 0
        var s: UInt8 = 0
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_decimal(value, Pointer(to=h), Pointer(to=w), Pointer(to=s), Pointer(to=err)), err)
        return Decimal(w, s, _from_hugeint(h))


__extension UUID(DuckDBMapped):
    @staticmethod
    def logical_type() raises DuckDBError -> LogicalType:
        return LogicalType(TypeId.UUID)

    @staticmethod
    def accepts(kind: TypeId) -> Bool:
        return kind == TypeId.UUID

    @staticmethod
    def read(meta: ColumnMeta, data: RawData, idx: Int) -> Self:
        return UUID(internal=_load[Int128](data, idx))

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        var h = _hugeint(self._to_internal())
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        return _new_value(lib().duckdb_v2_value_create_uuid(factory, Pointer(to=h), Pointer(to=v), Pointer(to=err)), v, err)

    @staticmethod
    def from_value(value: duckdb_v2_value_handle) raises DuckDBError -> Self:
        var h = duckdb_v2_hugeint_t(0, 0)
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_uuid(value, Pointer(to=h), Pointer(to=err)), err)
        return UUID(internal=_from_hugeint(h))
