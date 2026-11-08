"""Logical types: `TypeId` and the owned `LogicalType`."""

from std.os import abort
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb._runtime import api_error, builtin_factory, check, lib
from duckdb.error import DuckDBError


@fieldwise_init
struct TypeId(Equatable, ImplicitlyCopyable, Writable):
    """A DuckDB logical type id (V2 `LOGICAL_TYPE_ID`)."""

    var value: DUCKDB_V2_LOGICAL_TYPE_ID

    comptime INVALID = Self(DUCKDB_V2_LOGICAL_TYPE_ID_INVALID)
    comptime ANY = Self(DUCKDB_V2_LOGICAL_TYPE_ID_ANY)
    comptime BOOLEAN = Self(DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN)
    comptime TINYINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT)
    comptime SMALLINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT)
    comptime INTEGER = Self(DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER)
    comptime BIGINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT)
    comptime HUGEINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT)
    comptime UTINYINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT)
    comptime USMALLINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT)
    comptime UINTEGER = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER)
    comptime UBIGINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT)
    comptime UHUGEINT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT)
    comptime FLOAT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT)
    comptime DOUBLE = Self(DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE)
    comptime DECIMAL = Self(DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL)
    comptime VARCHAR = Self(DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR)
    comptime BLOB = Self(DUCKDB_V2_LOGICAL_TYPE_ID_BLOB)
    comptime BIT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_BIT)
    comptime DATE = Self(DUCKDB_V2_LOGICAL_TYPE_ID_DATE)
    comptime TIME = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIME)
    comptime TIME_NS = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS)
    comptime TIME_TZ = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ)
    comptime TIMESTAMP = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP)
    comptime TIMESTAMP_SEC = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC)
    comptime TIMESTAMP_MS = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS)
    comptime TIMESTAMP_NS = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS)
    comptime TIMESTAMP_TZ = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ)
    comptime TIMESTAMP_TZ_NS = Self(DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS)
    comptime INTERVAL = Self(DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL)
    comptime UUID = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UUID)
    comptime ENUM = Self(DUCKDB_V2_LOGICAL_TYPE_ID_ENUM)
    comptime LIST = Self(DUCKDB_V2_LOGICAL_TYPE_ID_LIST)
    comptime ARRAY = Self(DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY)
    comptime STRUCT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT)
    comptime MAP = Self(DUCKDB_V2_LOGICAL_TYPE_ID_MAP)
    comptime UNION = Self(DUCKDB_V2_LOGICAL_TYPE_ID_UNION)
    comptime VARIANT = Self(DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT)
    comptime GEOMETRY = Self(DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY)
    comptime BIGNUM = Self(DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM)

    def name(self) -> StaticString:
        """The id's name, e.g. `"INTEGER"`."""
        # `case _` keeps this valid when DuckDB adds a type id.
        match self.value:
            case == DUCKDB_V2_LOGICAL_TYPE_ID_INVALID:
                return "INVALID"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_ANY:
                return "ANY"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN:
                return "BOOLEAN"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT:
                return "TINYINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT:
                return "SMALLINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER:
                return "INTEGER"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT:
                return "BIGINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT:
                return "HUGEINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT:
                return "UTINYINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT:
                return "USMALLINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER:
                return "UINTEGER"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT:
                return "UBIGINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT:
                return "UHUGEINT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT:
                return "FLOAT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE:
                return "DOUBLE"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL:
                return "DECIMAL"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR:
                return "VARCHAR"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_BLOB:
                return "BLOB"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_BIT:
                return "BIT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_DATE:
                return "DATE"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIME:
                return "TIME"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS:
                return "TIME_NS"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ:
                return "TIME_TZ"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP:
                return "TIMESTAMP"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC:
                return "TIMESTAMP_SEC"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS:
                return "TIMESTAMP_MS"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS:
                return "TIMESTAMP_NS"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ:
                return "TIMESTAMP_TZ"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS:
                return "TIMESTAMP_TZ_NS"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL:
                return "INTERVAL"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UUID:
                return "UUID"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_ENUM:
                return "ENUM"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_LIST:
                return "LIST"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY:
                return "ARRAY"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT:
                return "STRUCT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_MAP:
                return "MAP"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_UNION:
                return "UNION"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT:
                return "VARIANT"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY:
                return "GEOMETRY"
            case == DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM:
                return "BIGNUM"
            case _:
                return "UNKNOWN"

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.name())


struct LogicalType(Copyable, Equatable, Movable, Writable):
    """An owned DuckDB logical type, including its parameters.

    Built-in types are created through a process-wide built-in factory, so
    they need no connection. Catalog types (an ENUM by name, extension types)
    need a connection's factory: `LogicalType.parse(text, factory)`.
    """

    var _handle: duckdb_v2_logical_type_handle

    def __init__(out self, *, var _owned: duckdb_v2_logical_type_handle):
        """Take ownership of a handle DuckDB returned as owned."""
        self._handle = _owned

    def __init__(out self, id: TypeId) raises DuckDBError:
        """A type without parameters, e.g. `LogicalType(TypeId.INTEGER)`."""
        self._handle = _from_id(builtin_factory(), id, List[duckdb_v2_value_handle](), List[String]())

    @staticmethod
    def parse(text: StringSlice, factory: Optional[duckdb_v2_factory_handle] = None) raises DuckDBError -> Self:
        """A type from SQL text, e.g. `"STRUCT(a INTEGER, b VARCHAR[])"`.

        Without a factory only built-in types resolve. Pass a connection's
        factory for catalog types (an ENUM by name, extension types).
        """
        var f = factory.or_else(builtin_factory())
        var arg = StrArg(text)
        var out = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_factory_create_type_from_text(f, arg.ptr(), Pointer(to=out), Pointer(to=err)), err)
        return Self(_owned=out)

    @staticmethod
    def decimal(width: Int, scale: Int) raises DuckDBError -> Self:
        var err = null_error_info()
        var w = null[duckdb_v2_value_handle]()
        var s = null[duckdb_v2_value_handle]()
        try:
            check(lib().duckdb_v2_value_create_utinyint(builtin_factory(), UInt8(width), Pointer(to=w), Pointer(to=err)), err)
            check(lib().duckdb_v2_value_create_utinyint(builtin_factory(), UInt8(scale), Pointer(to=s), Pointer(to=err)), err)
            var h = _from_id(builtin_factory(), TypeId.DECIMAL, [w, s], List[String]())
            return Self(_owned=h)
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=w))
            _ = lib().duckdb_v2_value_destroy(Pointer(to=s))

    @staticmethod
    def list(child: LogicalType) raises DuckDBError -> Self:
        var err = null_error_info()
        var t = null[duckdb_v2_value_handle]()
        try:
            check(lib().duckdb_v2_value_create_type(builtin_factory(), child._handle, Pointer(to=t), Pointer(to=err)), err)
            return Self(_owned=_from_id(builtin_factory(), TypeId.LIST, [t], List[String]()))
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=t))

    @staticmethod
    def array(child: LogicalType, size: Int) raises DuckDBError -> Self:
        """A fixed-size ARRAY, e.g. `INTEGER[3]`."""
        var err = null_error_info()
        var t = null[duckdb_v2_value_handle]()
        var n = null[duckdb_v2_value_handle]()
        try:
            check(lib().duckdb_v2_value_create_type(builtin_factory(), child._handle, Pointer(to=t), Pointer(to=err)), err)
            check(lib().duckdb_v2_value_create_bigint(builtin_factory(), Int64(size), Pointer(to=n), Pointer(to=err)), err)
            return Self(_owned=_from_id(builtin_factory(), TypeId.ARRAY, [t, n], List[String]()))
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=t))
            _ = lib().duckdb_v2_value_destroy(Pointer(to=n))

    @staticmethod
    def map(key: LogicalType, value: LogicalType) raises DuckDBError -> Self:
        return Self(_owned=_from_types(TypeId.MAP, List[String](), [key.copy(), value.copy()]))

    @staticmethod
    def struct_(names: List[String], types: List[LogicalType]) raises DuckDBError -> Self:
        """A STRUCT with the given field names and types."""
        return Self(_owned=_from_types(TypeId.STRUCT, names, types))

    @staticmethod
    def union_(names: List[String], types: List[LogicalType]) raises DuckDBError -> Self:
        """A UNION with the given member names and types."""
        return Self(_owned=_from_types(TypeId.UNION, names, types))

    def __init__(out self, *, copy: Self):
        # A Copyable copy constructor may not declare `raises` (Mojo 1.2), and
        # `copy()` is called from non-raising code, so a copy failure cannot be
        # raised out. It also has no data-dependent failure: the C call deep-
        # copies an internal LogicalType, so the only error it can report is
        # allocation failure - an unrecoverable, process-level condition, the
        # one case `abort` is for. The handle is NULL here, so it aborts cleanly.
        var err = null_error_info()
        self._handle = null[duckdb_v2_logical_type_handle]()
        try:
            check(lib().duckdb_v2_logical_type_copy(copy._handle, Pointer(to=self._handle), Pointer(to=err)), err)
        except e:
            abort_on_copy(e)

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_logical_type_destroy(Pointer(to=self._handle))

    def handle(self) -> duckdb_v2_logical_type_handle:
        """The underlying V2 handle (borrowed, valid while `self` lives)."""
        return self._handle

    def id(self) raises DuckDBError -> TypeId:
        var out: DUCKDB_V2_LOGICAL_TYPE_ID = 0
        var err = null_error_info()
        check(lib().duckdb_v2_logical_type_get_id(self._handle, Pointer(to=out), Pointer(to=err)), err)
        return TypeId(out)

    def to_text(self) raises DuckDBError -> String:
        """SQL text of the type, e.g. `DECIMAL(18,3)` or `INTEGER[]`."""
        var err = null_error_info()
        var needed: idx_t = 0
        check(
            lib().duckdb_v2_logical_type_to_text(
                self._handle, null[Pointer[c_char, MutUntrackedOrigin]](), 0, Pointer(to=needed), Pointer(to=err)
            ),
            err,
        )
        var buf = List[UInt8](length=Int(needed) + 1, fill=0)
        check(
            lib().duckdb_v2_logical_type_to_text(
                self._handle,
                buf.unsafe_ptr().unsafe_bitcast[c_char](),
                needed + 1,
                Pointer(to=needed),
                Pointer(to=err),
            ),
            err,
        )
        return String(from_utf8_lossy=Span(buf)[: Int(needed)])

    def __eq__(self, other: Self) -> Bool:
        """Deep equality (parameters and children included)."""
        var out = False
        var err = null_error_info()
        if lib().duckdb_v2_logical_type_is_equal(self._handle, other._handle, Pointer(to=out), Pointer(to=err)) != DUCKDB_V2_ERROR_NONE:
            _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))
            return False
        return out

    def write_to(self, mut writer: Some[Writer]):
        try:
            writer.write(self.to_text())
        except:
            writer.write("<type>")

    # ── Parameters ────────────────────────────────────────────────

    def param_count(self) raises DuckDBError -> Int:
        var n: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_logical_type_get_param_count(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return Int(n)

    def _param(self, i: Int) raises DuckDBError -> Tuple[String, duckdb_v2_value_handle]:
        """Name (empty when positional) and owned value of parameter `i`."""
        var name = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var value = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_logical_type_get_param(self._handle, idx_t(i), Pointer(to=name), Pointer(to=value), Pointer(to=err)), err)
        return (str_to_string(name), value)

    def decimal_width(self) raises DuckDBError -> Int:
        return Int(self._param_u8(0))

    def decimal_scale(self) raises DuckDBError -> Int:
        return Int(self._param_u8(1))

    def array_size(self) raises DuckDBError -> Int:
        """The size of an ARRAY type."""
        var p = self._param(1)
        var v = p[1]
        var out: Int64 = 0
        var err = null_error_info()
        try:
            check(lib().duckdb_v2_value_get_bigint(v, Pointer(to=out), Pointer(to=err)), err)
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=v))
        return Int(out)

    def _param_u8(self, i: Int) raises DuckDBError -> UInt8:
        var p = self._param(i)
        var v = p[1]
        var out: UInt8 = 0
        var err = null_error_info()
        try:
            check(lib().duckdb_v2_value_get_utinyint(v, Pointer(to=out), Pointer(to=err)), err)
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=v))
        return out

    def enum_values(self) raises DuckDBError -> List[String]:
        """The dictionary of an ENUM type, in index order."""
        var out = List[String]()
        for i in range(self.param_count()):
            var p = self._param(i)
            var v = p[1]
            var s = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
            var err = null_error_info()
            try:
                check(lib().duckdb_v2_value_get_varchar(v, Pointer(to=s), Pointer(to=err)), err)
                out.append(str_to_string(s))
            finally:
                _ = lib().duckdb_v2_value_destroy(Pointer(to=v))
        return out^

    def child_types(self) raises DuckDBError -> List[Tuple[String, LogicalType]]:
        """Named child types: STRUCT fields, UNION members, the LIST/ARRAY
        element (unnamed), MAP key and value."""
        var out = List[Tuple[String, LogicalType]]()
        for i in range(self.param_count()):
            var p = self._param(i)
            var v = p[1]
            var t = null[duckdb_v2_logical_type_handle]()
            var err = null_error_info()
            if lib().duckdb_v2_value_get_type(v, Pointer(to=t), Pointer(to=err)) == DUCKDB_V2_ERROR_NONE:
                out.append((p[0].copy(), LogicalType(_owned=t)))
            else:
                _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))  # not a TYPE parameter
            _ = lib().duckdb_v2_value_destroy(Pointer(to=v))
        return out^


def abort_on_copy(e: DuckDBError):
    abort(String("Failed to copy a logical type: ", e))


def _from_types(id: TypeId, names: List[String], types: List[LogicalType]) raises DuckDBError -> duckdb_v2_logical_type_handle:
    """A nested type from child types (as TYPE parameters), named when `names` is not empty."""
    var values = List[duckdb_v2_value_handle]()
    var err = null_error_info()
    try:
        for i in range(len(types)):
            var t = null[duckdb_v2_value_handle]()
            check(lib().duckdb_v2_value_create_type(builtin_factory(), types[i].handle(), Pointer(to=t), Pointer(to=err)), err)
            values.append(t)
        return _from_id(builtin_factory(), id, values, names)
    finally:
        for i in range(len(values)):
            _ = lib().duckdb_v2_value_destroy(Pointer(to=values[i]))
        _ = types


def _from_id(
    factory: duckdb_v2_factory_handle,
    id: TypeId,
    values: List[duckdb_v2_value_handle],
    names: List[String],
) raises DuckDBError -> duckdb_v2_logical_type_handle:
    """Create a type from its id and parameter values (borrowed), named when `names` is not empty."""
    var out = null[duckdb_v2_logical_type_handle]()
    var err = null_error_info()
    var values_ptr = null[Pointer[duckdb_v2_value_handle, ImmUntrackedOrigin]]()
    if len(values) > 0:
        values_ptr = values.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]()
    var name_args = List[StrArg]()
    var name_views = List[duckdb_v2_identifier_t]()
    for i in range(len(names)):
        name_args.append(StrArg(names[i]))
    for i in range(len(name_args)):
        name_views.append(name_args[i].ptr()[].copy())
    var names_ptr = null[Pointer[duckdb_v2_identifier_t, ImmUntrackedOrigin]]()
    if len(name_views) > 0:
        names_ptr = name_views.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]()
    check(
        lib().duckdb_v2_factory_create_type_from_id(
            factory,
            id.value,
            names_ptr,
            values_ptr,
            idx_t(len(values)),
            Pointer(to=out),
            Pointer(to=err),
        ),
        err,
    )
    _ = values  # the arrays are read by the call above
    _ = name_views^
    _ = name_args^
    return out
