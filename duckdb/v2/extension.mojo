"""DuckDB extensions written with the V2 high-level API.

```mojo
from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb.v2 import Extension, ScalarFunction

def add(a: Int64, b: Int64) -> Int64:
    return a + b

def init(ext: Extension) raises:
    ext.register(ScalarFunction.from_function["my_add", add]())

@export("my_ext_init_c_api_v2")
def my_ext_init(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    Extension.run[init](input)
```

Build it as a shared library and add the footer for the unstable V2 target:

```sh
mojo build my_ext.mojo --emit shared-lib -o my_ext.duckdb_extension
python3 scripts/append_extension_metadata.py my_ext.duckdb_extension --abi-type C_STRUCT_UNSTABLE
```

The extension loads only into the exact DuckDB build it was compiled
against (every V2 function is still unstable). An extension cannot run SQL:
it has no `Connection`.
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb.extension_v2 import ExtensionV2
from duckdb.v2._runtime import check, lib
from duckdb.v2.error import DuckDBError
from duckdb.v2.functions import Registrar


struct Extension(Movable, Registrar):
    """The extension being loaded, as seen from its init function."""

    var _ext: duckdb_v2_extension_handle
    var _context: duckdb_v2_context_handle

    def __init__(out self, ext: ExtensionV2):
        self._ext = ext.extension
        self._context = ext.context

    def factory(self) raises DuckDBError -> duckdb_v2_factory_handle:
        """The factory of the load context (valid while the init function runs)."""
        var factory = null[duckdb_v2_factory_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_context_get_factory(self._context, Pointer(to=factory), Pointer(to=err)), err)
        return factory

    def _register_scalar_handle(self, function: duckdb_v2_scalar_function_handle) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_extension_register_scalar_function(self._ext, function, Pointer(to=err)), err)

    @staticmethod
    def run[init: def(ext: Extension) raises thin -> None](
        input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]
    ):
        """Load the V2 API, then run `init`. If it raises, the load fails with its message."""
        ExtensionV2.run[_init[init], unstable=True](input)


def _init[init: def(ext: Extension) raises thin -> None](ext: ExtensionV2) raises:
    init(Extension(ext))
