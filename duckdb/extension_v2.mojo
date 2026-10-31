"""The raw entrypoint for DuckDB extensions on the V2 C API.

`duckdb.Extension` builds on it and is what extensions normally use. Use this
module directly only to write an extension against the raw V2 functions
(`LibDuckDBV2`).

An extension picks one of two targets when it is built:

| Target                       | Footer (`append_extension_metadata.py`)          | Loads in                                |
| ---------------------------- | ------------------------------------------------ | --------------------------------------- |
| `unstable=False` (stable)    | `--abi-type C_STRUCT --capi-version v2.x.y`      | that version and every later 2.x        |
| `unstable=True`              | `--abi-type C_STRUCT_UNSTABLE`                   | exactly `DUCKDB_V2_SOURCE_VERSION`      |

With the stable target only the stable V2 functions can be used. The others are
bound to stubs that abort when called. DuckDB 2.0.0 has no stable V2 functions
yet and refuses to load stable V2 extensions, so for now only the unstable
target is useful. The footer must match the target: an unstable build with a
stable footer could load into a DuckDB whose V2 API has a different layout.

Example:
```mojo
from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb.extension_v2 import ExtensionV2

def init(ext: ExtensionV2) raises:
    ref lib = ext.libduckdb()
    # Register functions through lib and ext.extension ...

@export("my_ext_init_c_api_v2")
def my_ext_init(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    ExtensionV2.run[init, unstable=True](input)
```

Build and add the footer:
```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
python3 scripts/append_extension_metadata.py my_ext.duckdb_extension --abi-type C_STRUCT_UNSTABLE
```
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import (
    DuckDBV2,
    _set_ext_api_v2_ptr,
    _set_ext_api_v2_unstable_ptr,
    check,
    null,
    null_error_info,
    StrArg,
)


struct ExtensionV2(Movable):
    """The extension being loaded, as seen from its init function.

    Gives access to the extension handle (to register functions, types, casts,
    ...), the context DuckDB loads it in, and the error slot.
    """

    var extension: duckdb_v2_extension_handle
    """The extension handle. Pass it to the `duckdb_v2_extension_register_*` functions."""
    var context: duckdb_v2_context_handle
    """A context with an active transaction. Valid until the init function returns."""
    var _err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]

    def __init__(out self, input: duckdb_v2_extension_input):
        self.extension = input.extension
        self.context = input.context
        self._err = input.err

    @always_inline
    def libduckdb(self) -> ref[ImmStaticOrigin] LibDuckDBV2:
        """The V2 C API functions."""
        return DuckDBV2().libduckdb()

    def factory(self) raises -> duckdb_v2_factory_handle:
        """The factory of the load context, to create types, values and functions."""
        var factory = null[duckdb_v2_factory_handle]()
        var err = null_error_info()
        check(self.libduckdb().duckdb_v2_context_get_factory(self.context, Pointer(to=factory), Pointer(to=err)), err)
        return factory

    def set_error(self, message: String):
        """Fail the load with ``message``."""
        ref lib = self.libduckdb()
        var text = StrArg(message)
        _ = lib.duckdb_v2_error_info_set_code(self._err[], DUCKDB_V2_ERROR_API)
        _ = lib.duckdb_v2_error_info_set_text(self._err[], text.ptr())

    @staticmethod
    def run[
        init_fn: def(ext: ExtensionV2) raises thin -> None,
        *,
        unstable: Bool,
    ](input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]):
        """Fetch the V2 API struct for the target, then run ``init_fn``.

        If ``init_fn`` raises, the load fails with its message.

        Parameters:
            init_fn: Registers the extension's functionality. Raise on failure.
            unstable: `True` to use the full V2 API and pin the extension to
                `DUCKDB_V2_SOURCE_VERSION`. `False` to use only the stable V2
                functions of `DUCKDB_V2_EXTENSION_API_VERSION`. Must match the
                footer, see the module docs.

        Args:
            input: The argument DuckDB passes to the `<name>_init_c_api_v2` entrypoint.
        """
        # For C_STRUCT_UNSTABLE DuckDB ignores the requested version, so it is
        # only checked for the stable target.
        var version = String(DUCKDB_V2_EXTENSION_API_VERSION)
        var api = input[].get_api(
            input[].extension,
            version.as_c_string_span().ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin](),
        )
        _ = version^  # keep the string alive until get_api has returned
        if Int(api) == 0:
            # DuckDB already recorded why, and the error slot must stay untouched.
            return
        comptime if unstable:
            _set_ext_api_v2_unstable_ptr(api.unsafe_bitcast[duckdb_ext_api_v2_unstable]())
        else:
            _set_ext_api_v2_ptr(api.unsafe_bitcast[duckdb_ext_api_v2]())

        var ext = ExtensionV2(input[])
        try:
            init_fn(ext)
        except e:
            ext.set_error(String(e))
