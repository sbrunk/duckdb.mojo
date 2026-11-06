"""The mapping between Mojo types and DuckDB's scalar types.

`DuckDBMapped` is implemented (through `__extension`) for `Bool`, every
numeric scalar (`Int8`..`Int64`, `Int`, `UInt8`..`UInt64`, `Float32`,
`Float64`, `Int128`, `UInt128`), `String`, `Blob` and the value structs of
`duckdb.scalar_types` (`Date`, `Time`, `TimeNS`, `TimeTZ`, `Timestamp`,
`TimestampS`, `TimestampMS`, `TimestampNS`, `TimestampTZ`, `TimestampTZNS`,
`Interval`, `Decimal`, `UUID`, `Bit`).

`Blob` is the exception. It lives here next to its mapping.

Each type reads its value straight from DuckDB's storage layout. Storage
details are decoded here and only here: the 16-byte `bytes` layout of
VARCHAR/BLOB/BIT, the UUID bit flip, decimal width tiers, enum index widths.

Nested types (lists, structs, maps, unions, optionals) are not here: they go
through emberserde (`duckdb/serde.mojo`), which calls into this module for
every scalar.
"""

from std.builtin.rebind import downcast, rebind_var
from std.memory import ArcPointer
from std.sys import bit_width_of
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.scalar_types import (
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
from duckdb._runtime import api_error, builtin_factory, check, lib
from duckdb.error import DuckDBError, ErrorCode
from duckdb.types import LogicalType, TypeId


comptime RawData = Pointer[NoneType, ImmUntrackedOrigin]
comptime MutRawData = Pointer[NoneType, MutUntrackedOrigin]


struct ColumnMeta(Copyable, Movable):
    """What a reader needs to know about a column's type, read once per column."""

    var kind: TypeId
    var decimal_width: Int
    var decimal_scale: Int
    var enum_values: List[String]
    var enum_index_bytes: Int
    # ENUM entry -> index, for writing. Behind a pointer: a Dict field
    # here slows down reading (measured), though reads never touch it.
    var enum_index: Optional[ArcPointer[Dict[String, Int]]]  # ENUM columns only

    def __init__(out self, type: LogicalType) raises DuckDBError:
        self.kind = type.id()
        self.decimal_width = 0
        self.decimal_scale = 0
        self.enum_values = List[String]()
        self.enum_index_bytes = 0
        self.enum_index = None
        if self.kind == TypeId.DECIMAL:
            self.decimal_width = type.decimal_width()
            self.decimal_scale = type.decimal_scale()
        elif self.kind == TypeId.ENUM:
            self.enum_values = type.enum_values()
            # Storage width follows the dictionary size (contract in the V2 spec).
            var n = len(self.enum_values)
            self.enum_index_bytes = 1 if n <= 255 else (2 if n <= 65535 else 4)
            var index = Dict[String, Int]()
            for i in range(n):
                index[self.enum_values[i]] = i
            self.enum_index = ArcPointer(index^)

    def __init__(out self, *, kind: TypeId) raises DuckDBError:
        """Metadata for a type without parameters.

        Raises a `DuckDBError` for `DECIMAL`/`ENUM`: their storage width comes
        from the type's parameters, so build those from a `LogicalType`.
        """
        self.kind = kind
        self.decimal_width = 0
        self.decimal_scale = 0
        self.enum_values = List[String]()
        self.enum_index_bytes = 0
        self.enum_index = None
        if kind == TypeId.DECIMAL or kind == TypeId.ENUM:
            raise api_error(
                String("ColumnMeta(kind=) is not valid for ", kind, ", use the LogicalType constructor")
            )


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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        """Write into a flat vector at storage index `idx`.

        Returns False when the column is not this type's own storage layout.
        The caller then writes a value and lets DuckDB cast it.
        """
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


def is_mapped[T: AnyType]() -> Bool:
    """Whether `T` is a scalar type of this module (usable in `comptime if`)."""
    return conforms_to(T, DuckDBMapped)


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


@always_inline
def mapped_write[T: AnyType](
    value: T, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int
) raises DuckDBError:
    """Write `value` into a flat vector, through a cast value when the layouts differ."""
    comptime assert conforms_to(T, DuckDBMapped), _UNMAPPED
    ref v = rebind[downcast[T, DuckDBMapped]](value)
    if v.write(meta, vec, data, idx):
        return
    _write_cast(v, vec, idx)


@no_inline
def _write_cast[T: DuckDBMapped](v: T, vec: duckdb_v2_vector_handle, idx: Int) raises DuckDBError:
    var h = v.to_value(builtin_factory())
    var err = null_error_info()
    var code = lib().duckdb_v2_vector_set_value(vec, idx_t(idx), h, Pointer(to=err))
    _ = lib().duckdb_v2_value_destroy(Pointer(to=h))
    check(code, err)


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


@always_inline
def _store[T: TrivialRegisterPassable](data: MutRawData, idx: Int, v: T):
    data.unsafe_bitcast[T]()[unsafe_offset=idx] = v


def write_bytes(vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int, bytes: Span[UInt8, _]) raises DuckDBError:
    """Write a VARCHAR/BLOB/BIT value: inline up to 12 bytes, else into the vector's arena."""
    var base = data.unsafe_bitcast[UInt8]().unsafe_offset(idx * 16)
    var n = len(bytes)
    base.unsafe_bitcast[UInt32]()[] = UInt32(n)
    if n <= Int(DUCKDB_V2_BYTES_INLINE_LENGTH):
        for i in range(Int(DUCKDB_V2_BYTES_INLINE_LENGTH)):
            base.unsafe_offset(4 + i)[] = bytes[i] if i < n else 0
        return
    var err = null_error_info()
    var arena = null[duckdb_v2_arena_handle]()
    check(lib().duckdb_v2_vector_get_arena(vec, Pointer(to=arena), Pointer(to=err)), err)
    var p = null[Pointer[UInt8, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_arena_allocate(arena, idx_t(n), Pointer(to=p), Pointer(to=err)), err)
    for i in range(n):
        p.unsafe_offset(i)[] = bytes[i]
    for i in range(4):  # the prefix
        base.unsafe_offset(4 + i)[] = bytes[i]
    base.unsafe_offset(8).unsafe_bitcast[Pointer[UInt8, MutUntrackedOrigin]]()[] = p


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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.BOOLEAN:
            return False
        _store[UInt8](data, idx, UInt8(1) if self else UInt8(0))
        return True

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


def _store_int(kind: TypeId, data: MutRawData, idx: Int, v: Int128) -> Bool:
    """Store an integer into another integer (or DOUBLE) column when it fits.
    False sends it through DuckDB's cast, which reports the overflow."""
    if kind == TypeId.TINYINT and v >= -128 and v <= 127:
        _store[Int8](data, idx, Int8(v))
    elif kind == TypeId.SMALLINT and v >= -32768 and v <= 32767:
        _store[Int16](data, idx, Int16(v))
    elif kind == TypeId.INTEGER and v >= Int128(Int32.MIN) and v <= Int128(Int32.MAX):
        _store[Int32](data, idx, Int32(v))
    elif kind == TypeId.BIGINT and v >= Int128(Int64.MIN) and v <= Int128(Int64.MAX):
        _store[Int64](data, idx, Int64(v))
    elif kind == TypeId.HUGEINT:
        _store[Int128](data, idx, v)
    elif kind == TypeId.UTINYINT and v >= 0 and v <= 255:
        _store[UInt8](data, idx, UInt8(v))
    elif kind == TypeId.USMALLINT and v >= 0 and v <= 65535:
        _store[UInt16](data, idx, UInt16(v))
    elif kind == TypeId.UINTEGER and v >= 0 and v <= Int128(UInt32.MAX):
        _store[UInt32](data, idx, UInt32(v))
    elif kind == TypeId.UBIGINT and v >= 0 and v <= Int128(UInt64.MAX):
        _store[UInt64](data, idx, UInt64(v))
    elif kind == TypeId.UHUGEINT and v >= 0:
        _store[UInt128](data, idx, UInt128(v))
    elif kind == TypeId.DOUBLE:
        _store[Float64](data, idx, Float64(v))
    else:
        return False
    return True


def _numeric_type_id[dt: DType]() -> TypeId:
    comptime match dt:
        case .bool:
            return TypeId.BOOLEAN
        case .int8:
            return TypeId.TINYINT
        case .int16:
            return TypeId.SMALLINT
        case .int32:
            return TypeId.INTEGER
        case .int64 | .int:
            return TypeId.BIGINT
        case .uint8:
            return TypeId.UTINYINT
        case .uint16:
            return TypeId.USMALLINT
        case .uint32:
            return TypeId.UINTEGER
        case .uint64 | .uint:
            return TypeId.UBIGINT
        case .int128:
            return TypeId.HUGEINT
        case .uint128:
            return TypeId.UHUGEINT
        case .float32:
            return TypeId.FLOAT
        case .float64:
            return TypeId.DOUBLE
        case _:
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
        comptime match Self.dtype:
            case .bool:
                return Self(_load[UInt8](data, idx) != 0)
            case .int:
                return Self(_load[Int64](data, idx).cast[Self.dtype]())
            case .uint:
                return Self(_load[UInt64](data, idx).cast[Self.dtype]())
            case _:
                return rebind[Self](_load[Scalar[Self.dtype]](data, idx))

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        if meta.kind != _numeric_type_id[Self.dtype]():
            comptime if Self.dtype.is_integral() and bit_width_of[Self.dtype]() <= 64:
                return _store_int(meta.kind, data, idx, Int128(rebind[Scalar[Self.dtype]](self)))
            elif Self.dtype == DType.float32:
                if meta.kind == TypeId.DOUBLE:
                    _store[Float64](data, idx, rebind[Scalar[Self.dtype]](self).cast[DType.float64]())
                    return True
            return False
        comptime match Self.dtype:
            case .bool:
                _store[UInt8](data, idx, UInt8(1) if Bool(rebind[Scalar[DType.bool]](self)) else UInt8(0))
            case .int:
                _store[Int64](data, idx, rebind[Scalar[Self.dtype]](self).cast[DType.int64]())
            case .uint:
                _store[UInt64](data, idx, rebind[Scalar[Self.dtype]](self).cast[DType.uint64]())
            case _:
                _store[Scalar[Self.dtype]](data, idx, rebind[Scalar[Self.dtype]](self))
        return True

    def to_value(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_value_handle:
        comptime assert Self.length == 1, "only scalars map to a DuckDB type"
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        var p = Pointer(to=v)
        var e = Pointer(to=err)
        var code: DUCKDB_V2_ERROR
        comptime dt = Self.dtype
        var x = rebind[Scalar[dt]](self)
        comptime match dt:
            case .bool:
                code = lib().duckdb_v2_value_create_bool(factory, Bool(x), p, e)
            case .int8:
                code = lib().duckdb_v2_value_create_tinyint(factory, x.cast[DType.int8](), p, e)
            case .int16:
                code = lib().duckdb_v2_value_create_smallint(factory, x.cast[DType.int16](), p, e)
            case .int32:
                code = lib().duckdb_v2_value_create_int(factory, x.cast[DType.int32](), p, e)
            case .int64 | .int:
                code = lib().duckdb_v2_value_create_bigint(factory, x.cast[DType.int64](), p, e)
            case .uint8:
                code = lib().duckdb_v2_value_create_utinyint(factory, x.cast[DType.uint8](), p, e)
            case .uint16:
                code = lib().duckdb_v2_value_create_usmallint(factory, x.cast[DType.uint16](), p, e)
            case .uint32:
                code = lib().duckdb_v2_value_create_uint(factory, x.cast[DType.uint32](), p, e)
            case .uint64 | .uint:
                code = lib().duckdb_v2_value_create_ubigint(factory, x.cast[DType.uint64](), p, e)
            case .int128:
                var h = _hugeint(x.cast[DType.int128]())
                code = lib().duckdb_v2_value_create_hugeint(factory, Pointer(to=h), p, e)
            case .uint128:
                var u = x.cast[DType.uint128]()
                var hu = duckdb_v2_uhugeint_t(UInt64(u & UInt128(0xFFFFFFFFFFFFFFFF)), UInt64(u >> 64))
                code = lib().duckdb_v2_value_create_uhugeint(factory, Pointer(to=hu), p, e)
            case .float32:
                code = lib().duckdb_v2_value_create_float(factory, x.cast[DType.float32](), p, e)
            case .float64:
                code = lib().duckdb_v2_value_create_double(factory, x.cast[DType.float64](), p, e)
            case _:
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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind == TypeId.ENUM:
            var i = meta.enum_index.value()[].get(self)
            if not i:
                return False  # DuckDB's cast reports the unknown entry
            if meta.enum_index_bytes == 1:
                _store[UInt8](data, idx, UInt8(i.value()))
            elif meta.enum_index_bytes == 2:
                _store[UInt16](data, idx, UInt16(i.value()))
            else:
                _store[UInt32](data, idx, UInt32(i.value()))
            return True
        if meta.kind != TypeId.VARCHAR:
            return False
        write_bytes(vec, data, idx, self.as_bytes())
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.BLOB:
            return False
        write_bytes(vec, data, idx, Span(self.data))
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.BIT:
            return False
        write_bytes(vec, data, idx, Span(self._data))
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.DATE:
            return False
        _store[Int32](data, idx, self.days)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIME:
            return False
        _store[Int64](data, idx, self.micros)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIME_NS:
            return False
        _store[Int64](data, idx, self.nanos)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIME_TZ:
            return False
        _store[UInt64](data, idx, self.bits)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP:
            return False
        _store[Int64](data, idx, self.micros)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP_SEC:
            return False
        _store[Int64](data, idx, self.seconds)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP_MS:
            return False
        _store[Int64](data, idx, self.millis)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP_NS:
            return False
        _store[Int64](data, idx, self.nanos)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP_TZ:
            return False
        _store[Int64](data, idx, self.micros)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.TIMESTAMP_TZ_NS:
            return False
        _store[Int64](data, idx, self.nanos)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.INTERVAL:
            return False
        data.unsafe_bitcast[duckdb_v2_interval_t]()[unsafe_offset=idx] = duckdb_v2_interval_t(self.months, self.days, self.micros)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.DECIMAL or Int(self.width) != meta.decimal_width or Int(self.scale) != meta.decimal_scale:
            return False
        var w = meta.decimal_width
        var v = self.value()
        if w <= 4:
            _store[Int16](data, idx, Int16(v))
        elif w <= 9:
            _store[Int32](data, idx, Int32(v))
        elif w <= 18:
            _store[Int64](data, idx, Int64(v))
        else:
            _store[Int128](data, idx, v)
        return True

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

    def write(self, meta: ColumnMeta, vec: duckdb_v2_vector_handle, data: MutRawData, idx: Int) raises DuckDBError -> Bool:
        if meta.kind != TypeId.UUID:
            return False
        _store[Int128](data, idx, self._to_internal())
        return True

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
