"""Demo DuckDB extension written in Mojo.

This extension demonstrates how to create a DuckDB extension in Mojo using
the C Extension API. It registers a scalar function `mojo_add_numbers` that
adds two BIGINT values together.

Build with pixi (from the workspace root):
    pixi run build-demo-extension

Or manually:
    mojo build demo-extension/src/demo_mojo_extension.mojo --emit shared-lib \
        -o demo-extension/build/demo_mojo.duckdb_extension
    python3 scripts/append_extension_metadata.py \
        demo-extension/build/demo_mojo.duckdb_extension

Load in DuckDB:
    LOAD 'demo-extension/build/demo_mojo.duckdb_extension';
    SELECT mojo_add_numbers(40, 2);  -- Returns 42
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.api_level import ApiLevel
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction


# ===--------------------------------------------------------------------===#
# Scalar function: mojo_add_numbers(a BIGINT, b BIGINT) -> BIGINT
# ===--------------------------------------------------------------------===#


fn add_numbers(a: Int64, b: Int64) -> Int64:
    """Adds two integers together."""
    return a + b


# ===--------------------------------------------------------------------===#
# Extension entry point
# ===--------------------------------------------------------------------===#


fn init(conn: Connection[ApiLevel.EXT_STABLE]) raises:
    """Register extension functions."""
    ScalarFunction.from_function[
        "mojo_add_numbers", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)


@export("demo_mojo_init_c_api", ABI="C")
fn demo_mojo_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    """Entry point called by DuckDB when loading this extension."""
    return Extension.run[init](info, access)
