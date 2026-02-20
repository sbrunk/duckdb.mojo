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

from duckdb._libduckdb import (
    duckdb_extension_info,
    duckdb_function_info,
    duckdb_data_chunk,
    duckdb_vector,
    DuckDBError,
)
from duckdb.extension import duckdb_extension_access, ExtensionConnection
from duckdb.scalar_function import ScalarFunction, FunctionInfo
from duckdb.logical_type import LogicalType
from duckdb.duckdb_type import DuckDBType
from duckdb.chunk import Chunk
from duckdb.vector import Vector


# ===--------------------------------------------------------------------===#
# Scalar function: mojo_add_numbers(a BIGINT, b BIGINT) -> BIGINT
# ===--------------------------------------------------------------------===#


fn add_numbers_together(info: FunctionInfo, mut input: Chunk, output: Vector):
    """Adds two BIGINT columns element-wise."""
    var size = len(input)
    var a = input.get_vector(0).get_data().bitcast[Int64]()
    var b = input.get_vector(1).get_data().bitcast[Int64]()
    var result = output.get_data().bitcast[Int64]()
    for i in range(size):
        result[i] = a[i] + b[i]


fn register_add_numbers(ext_conn: ExtensionConnection):
    """Create and register the mojo_add_numbers function."""
    var func = ScalarFunction()
    func.set_name("mojo_add_numbers")

    var bigint_type = LogicalType(DuckDBType.bigint)
    func.add_parameter(bigint_type)
    func.add_parameter(bigint_type)
    func.set_return_type(bigint_type)

    func.set_function[add_numbers_together]()
    ext_conn.register(func)


# ===--------------------------------------------------------------------===#
# Extension entry point
# ===--------------------------------------------------------------------===#


@export("demo_mojo_init_c_api", ABI="C")
fn demo_mojo_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    """Entry point called by DuckDB when loading this extension.

    DuckDB calls this function with the extension info and an access struct
    containing function pointers to interact with the database.

    Returns True on success, False on failure.
    """
    # Create a connection to the database
    var ext_conn = ExtensionConnection(info, access)
    if not ext_conn:
        return False

    # Register our functions
    register_add_numbers(ext_conn)

    # Connection is automatically closed when ext_conn goes out of scope
    return True
