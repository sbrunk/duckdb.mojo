"""Demo DuckDB extension written in Mojo.

Registers a scalar function `mojo_add_numbers(a BIGINT, b BIGINT) -> BIGINT`
through the V2 high-level API (`duckdb.Extension`).

Build with pixi (from the workspace root):
    pixi run build-demo-extension

Or manually:
    mojo build extensions/demo-extension/src/demo_mojo_extension.mojo --emit shared-lib \
        -o extensions/demo-extension/build/demo_mojo.duckdb_extension
    python3 scripts/append_extension_metadata.py \
        extensions/demo-extension/build/demo_mojo.duckdb_extension --abi-type C_STRUCT_UNSTABLE

Load in DuckDB:
    LOAD 'extensions/demo-extension/build/demo_mojo.duckdb_extension';
    SELECT mojo_add_numbers(40, 2);  -- Returns 42
"""

from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb import Extension, ScalarFunction


def add_numbers(a: Int64, b: Int64) -> Int64:
    """Adds two integers together."""
    return a + b


def init(ext: Extension) raises:
    """Register the extension's functions."""
    ext.register(ScalarFunction.from_function["mojo_add_numbers", add_numbers]())


@export("demo_mojo_init_c_api_v2")
def demo_mojo_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    """Entry point called by DuckDB when loading this extension."""
    Extension.run[init](input)
