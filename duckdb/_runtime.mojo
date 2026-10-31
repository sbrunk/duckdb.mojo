"""Process-wide state and helpers shared by the modules of the package.

- The V2 functions come from `duckdb.api_v2.DuckDBV2` (client or extension mode).
- One V2 environment per process, created on first use and never destroyed.
  Every database opened through this package lives in it, so the instance
  cache spans them all and DuckDB refuses to open one file twice
  (`RESOURCE_IN_USE`) instead of creating two independent instances on it.
- A hidden in-memory instance whose factory creates built-in types and values
  outside any connection (DuckDB's instance factories know only built-ins).
"""

from std.ffi import _Global
from std.os import abort
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import DuckDBV2, StrArg, null, null_error_info, str_to_string
from duckdb.error import DuckDBError, ErrorCode


@always_inline
def lib() -> ref[ImmStaticOrigin] LibDuckDBV2:
    return DuckDBV2().libduckdb()


def check(code: DUCKDB_V2_ERROR, mut err: duckdb_v2_error_info_handle) raises DuckDBError:
    """Raise a `DuckDBError` if a V2 call failed.

    The return code is authoritative: a slot can hold a stale info from an
    earlier failure. Reads the message, destroys the info and resets the slot.
    """
    if code == DUCKDB_V2_ERROR_NONE:
        return
    raise _error(code, err)


@no_inline
def _error(code: DUCKDB_V2_ERROR, mut err: duckdb_v2_error_info_handle) -> DuckDBError:
    var message = String("DuckDB error ", code)
    if Int(err) != 0:
        var text = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        if lib().duckdb_v2_error_info_get_text(err, Pointer(to=text)) == DUCKDB_V2_ERROR_NONE and text.len > 0:
            message = str_to_string(text)
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))
        err = null_error_info()
    return DuckDBError(ErrorCode(code), message^)


def api_error(var message: String) -> DuckDBError:
    """An error raised by this package itself (not by a DuckDB call)."""
    return DuckDBError(ErrorCode.API, message^)


# ===--------------------------------------------------------------------===#
# Process-wide environment and the built-in type factory
# ===--------------------------------------------------------------------===#

comptime _RUNTIME = _Global["DuckDBV2Runtime", _init_runtime]


def _init_runtime() -> _Runtime:
    return _Runtime()


struct _Runtime(Defaultable, Movable):
    var env: duckdb_v2_environment_handle
    var types_instance: duckdb_v2_instance_handle
    var types_factory: duckdb_v2_factory_handle

    def __init__(out self):
        self.env = null[duckdb_v2_environment_handle]()
        self.types_instance = null[duckdb_v2_instance_handle]()
        self.types_factory = null[duckdb_v2_factory_handle]()
        try:
            var err = null_error_info()
            check(lib().duckdb_v2_environment_create(Pointer(to=self.env), Pointer(to=err)), err)
            check(lib().duckdb_v2_instance_create(self.env, Pointer(to=self.types_instance), Pointer(to=err)), err)
            check(
                lib().duckdb_v2_instance_get_factory(self.types_instance, Pointer(to=self.types_factory), Pointer(to=err)),
                err,
            )
        except e:
            abort(String("Failed to initialize the DuckDB V2 runtime: ", e))


def environment() -> duckdb_v2_environment_handle:
    """The process-wide V2 environment."""
    try:
        return _RUNTIME.get_or_create_ptr()[].env
    except e:
        abort(String(e))


def builtin_factory() -> duckdb_v2_factory_handle:
    """A factory for built-in types and values, valid for the whole process.

    It belongs to a hidden in-memory instance, so it cannot resolve catalog
    types (ENUMs by name, extension types) and uses default settings.
    """
    try:
        return _RUNTIME.get_or_create_ptr()[].types_factory
    except e:
        abort(String(e))
