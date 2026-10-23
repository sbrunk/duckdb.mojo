"""Access to DuckDB's V2 C API functions (`LibDuckDBV2`).

The V2 functions are loaded once per process into a global, the same way the V1
functions are (see `duckdb.api`):

- In client mode they are looked up in libduckdb with dlsym. Every V2 function
  is still unstable in DuckDB 2.0, so the libduckdb version must match the one
  the bindings were generated from (`DUCKDB_V2_SOURCE_VERSION`). On a mismatch
  the first use aborts with an error message.
- In extension mode `ExtensionV2.run` stores the function pointer struct DuckDB
  hands to the extension before the first use, and the functions come from it.

Example:
```mojo
from duckdb.api_v2 import DuckDBV2

ref lib = DuckDBV2().libduckdb()
```
"""

from std.ffi import _Global
from std.os import abort
from std.sys.info import size_of
from duckdb.api import _kgen_insert_global, _kgen_get_global
from duckdb._libduckdb_v2 import *

comptime _DUCKDB_V2_GLOBAL = _Global["DuckDBV2", _init_duckdb_v2_global]

# Names of the process globals holding the extension API struct pointer. Only one
# of them is set, depending on the target `ExtensionV2.run` was built for.
comptime _EXT_API_V2_PTR = "DuckDBV2_ExtApiPtr"
comptime _EXT_API_V2_UNSTABLE_PTR = "DuckDBV2_ExtApiUnstablePtr"


def _set_ext_api_v2_ptr(ptr: Pointer[duckdb_ext_api_v2, ImmUntrackedOrigin]):
    """Use the stable extension API struct for `LibDuckDBV2`. Call before the first `DuckDBV2()`."""
    _kgen_insert_global(_EXT_API_V2_PTR, ptr)


def _set_ext_api_v2_unstable_ptr(ptr: Pointer[duckdb_ext_api_v2_unstable, ImmUntrackedOrigin]):
    """Use the full (unstable) extension API struct for `LibDuckDBV2`. Call before the first `DuckDBV2()`."""
    _kgen_insert_global(_EXT_API_V2_UNSTABLE_PTR, ptr)


def _init_duckdb_v2_global() -> _DuckDBV2Global:
    return _DuckDBV2Global()


struct _DuckDBV2Global(Defaultable, Movable):
    var libduckdb: LibDuckDBV2

    def __init__(out self):
        var unstable = Int(_kgen_get_global(_EXT_API_V2_UNSTABLE_PTR))
        if unstable != 0:
            self.libduckdb = LibDuckDBV2(
                Pointer[duckdb_ext_api_v2_unstable, ImmUntrackedOrigin](unsafe_from_address=unstable)
            )
            return
        var stable = Int(_kgen_get_global(_EXT_API_V2_PTR))
        if stable != 0:
            self.libduckdb = LibDuckDBV2(Pointer[duckdb_ext_api_v2, ImmUntrackedOrigin](unsafe_from_address=stable))
            return
        try:
            self.libduckdb = LibDuckDBV2()
        except e:
            abort(
                String(
                    "Failed to load the DuckDB V2 C API from libduckdb (the bindings were generated for DuckDB ",
                    DUCKDB_V2_SOURCE_VERSION,
                    "): ",
                    e,
                )
            )
        var version = _library_version(self.libduckdb)
        if version != DUCKDB_V2_SOURCE_VERSION:
            abort(
                String(
                    "libduckdb is DuckDB ",
                    version,
                    ", but the V2 C API bindings were generated for DuckDB ",
                    DUCKDB_V2_SOURCE_VERSION,
                    ". The V2 C API is unstable, so the versions must match exactly.",
                )
            )


def _library_version(lib: LibDuckDBV2) -> String:
    var version = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
    var err = null_error_info()
    if lib.duckdb_v2_library_version(Pointer(to=version), Pointer(to=err)) != DUCKDB_V2_ERROR_NONE:
        _ = lib.duckdb_v2_error_info_destroy(Pointer(to=err))
        return "unknown"
    return str_to_string(version)


def str_to_string(s: duckdb_v2_str) -> String:
    """Copy a V2 string view into a `String`. Invalid UTF-8 is replaced."""
    if s.len == 0:
        return ""
    return String(
        from_utf8_lossy=Span[UInt8, ImmUntrackedOrigin](unsafe_ptr=s.ptr.unsafe_bitcast[UInt8](), length=Int(s.len))
    )


def _str_view(s: StringSlice) -> duckdb_v2_str:
    """A V2 string view of ``s``. It borrows the bytes, so ``s`` must outlive it."""
    return duckdb_v2_str(
        s.unsafe_ptr().unsafe_bitcast[c_char]().as_imm().unsafe_origin_cast[ImmUntrackedOrigin](),
        idx_t(s.byte_length()),
    )


struct StrArg(Movable):
    """A string argument for V2 functions, which take strings as `const duckdb_v2_str *`.

    Owns a copy of the text. The pointer from `ptr()` is tied to this value,
    so the text stays alive while a call uses it:

    ```mojo
    var name = StrArg("my_function")
    check(lib.duckdb_v2_scalar_function_set_name(function, name.ptr(), Pointer(to=err)), err)
    ```
    """

    var _text: String
    var _view: duckdb_v2_str

    def __init__(out self, text: StringSlice):
        self._text = String(text)
        self._view = _str_view(self._text)

    def __init__(out self, *, deinit move: Self):
        self._text = move._text^
        self._view = move._view.copy()

    def ptr(mut self) -> Pointer[duckdb_v2_str, origin_of(self._view)]:
        """The string view to pass to a V2 function."""
        # Rebuilt on each call: a short String stores its bytes inline, so a
        # move of this struct moves them too.
        self._view = _str_view(self._text)
        return Pointer(to=self._view)


def null[P: ImplicitlyCopyable]() -> P:
    """A NULL value of the pointer or handle type ``P``.

    V2 functions take NULL for optional arguments, for example
    `null[duckdb_v2_value_handle]()` for "no default value".
    """
    comptime assert size_of[P]() == size_of[Int](), "null[P]: P must be a pointer or handle type"
    var none: Optional[Pointer[NoneType, MutUntrackedOrigin]] = None
    return Pointer(to=none).unsafe_bitcast[P]()[]


def null_error_info() -> duckdb_v2_error_info_handle:
    """An empty error slot to pass as the `err` argument of a V2 function."""
    return null[duckdb_v2_error_info_handle]()


def check(code: DUCKDB_V2_ERROR, mut err: duckdb_v2_error_info_handle) raises:
    """Raise an `Error` if a V2 function failed.

    Reads the message from ``err`` (if the function filled it in), destroys
    the error info and resets ``err`` to an empty slot.
    """
    if code == DUCKDB_V2_ERROR_NONE:
        return
    var message = String("DuckDB V2 C API error ", code)
    if Int(err) != 0:
        ref lib = DuckDBV2().libduckdb()
        var text = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        if lib.duckdb_v2_error_info_get_text(err, Pointer(to=text)) == DUCKDB_V2_ERROR_NONE and text.len > 0:
            message = str_to_string(text)
        _ = lib.duckdb_v2_error_info_destroy(Pointer(to=err))
        err = null_error_info()
    raise Error(message)


def _get_duckdb_v2_interface() raises -> Pointer[LibDuckDBV2, ImmStaticOrigin]:
    var ptr = _DUCKDB_V2_GLOBAL.get_or_create_ptr()
    var ptr2 = Pointer(to=ptr[].libduckdb).as_imm().unsafe_origin_cast[ImmStaticOrigin]()
    return Pointer(to=ptr2[])


struct DuckDBV2(ImplicitlyCopyable):
    """Handle to the process-wide `LibDuckDBV2`."""

    var _impl: Pointer[LibDuckDBV2, ImmStaticOrigin]

    def __init__(out self):
        try:
            self._impl = _get_duckdb_v2_interface()
        except e:
            abort(String("Failed to load the DuckDB V2 C API: ", e))

    @always_inline
    def libduckdb(self) -> ref[ImmStaticOrigin] LibDuckDBV2:
        return self._impl[]
