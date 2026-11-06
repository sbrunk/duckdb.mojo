"""Reading and changing settings (`SET`), from a connection or a callback."""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb._runtime import check, lib
from duckdb.error import DuckDBError
from duckdb.value import Value


@fieldwise_init
struct SettingScope(Equatable, ImplicitlyCopyable, Writable):
    var value: DUCKDB_V2_SETTING_SCOPE

    comptime DEFAULT = Self(DUCKDB_V2_SETTING_SCOPE_DEFAULT)
    """Where `SET name = value` writes (the option's default scope)."""
    comptime GLOBAL = Self(DUCKDB_V2_SETTING_SCOPE_GLOBAL)
    """One value for the whole database (`SET GLOBAL`)."""
    comptime SESSION = Self(DUCKDB_V2_SETTING_SCOPE_SESSION)
    """One value per connection (`SET SESSION`)."""

    def write_to(self, mut writer: Some[Writer]):
        # `case _` keeps this valid when DuckDB adds a scope.
        match self.value:
            case == DUCKDB_V2_SETTING_SCOPE_GLOBAL:
                writer.write("GLOBAL")
            case == DUCKDB_V2_SETTING_SCOPE_SESSION:
                writer.write("SESSION")
            case _:
                writer.write("DEFAULT")


struct Settings[origin: ImmOrigin](ImplicitlyCopyable):
    """The settings seen by one connection or query. It borrows the
    connection (or the callback's context), so it cannot outlive it."""

    var _handle: duckdb_v2_config_handle
    var _owner: Pointer[NoneType, Self.origin]

    def __init__(out self, handle: duckdb_v2_config_handle, owner: Pointer[NoneType, Self.origin]):
        self._handle = handle
        self._owner = owner

    def value(self, name: StringSlice) raises DuckDBError -> Value:
        """The effective value of setting `name` (built-in or custom)."""
        var n = StrArg(name)
        var v = null[duckdb_v2_value_handle]()
        var scope: DUCKDB_V2_SETTING_SCOPE = DUCKDB_V2_SETTING_SCOPE_DEFAULT
        var err = null_error_info()
        check(lib().duckdb_v2_config_get_option_value(self._handle, n.ptr(), Pointer(to=v), Pointer(to=scope), Pointer(to=err)), err)
        return Value(_owned=v)

    def get[T: Movable & Deinitable](self, name: StringSlice) raises DuckDBError -> Optional[T]:
        """The value of setting `name` as `T`, `None` when it is NULL."""
        return self.value(name).get_optional[T]()

    def set[T: Movable & Deinitable](self, name: StringSlice, value: T, scope: SettingScope = SettingScope.DEFAULT) raises DuckDBError:
        """Change setting `name`, like `SET`."""
        var n = StrArg(name)
        var v = Value(value)
        var err = null_error_info()
        var code = lib().duckdb_v2_config_set_option_value(self._handle, n.ptr(), v.handle(), scope.value, Pointer(to=err))
        _ = v^
        check(code, err)


def context_settings(context: duckdb_v2_context_handle) raises DuckDBError -> Settings[ImmUntrackedOrigin]:
    """Settings of a callback's context (valid during the callback)."""
    var c = null[duckdb_v2_config_handle]()
    var err = null_error_info()
    check(lib().duckdb_v2_context_get_config(context, Pointer(to=c), Pointer(to=err)), err)
    return Settings[ImmUntrackedOrigin](c, context.unsafe_bitcast[NoneType]().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]())
