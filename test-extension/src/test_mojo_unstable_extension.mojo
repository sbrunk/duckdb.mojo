"""Test extension that exercises the unstable extension API path.

Mirrors the minimal pattern of `test_mojo_extension.mojo` but uses
`Extension.run_unstable[init]` and `Connection[ApiLevel.EXT_UNSTABLE]`
so that the unstable codegen path through `LibDuckDB.__init__(api:
UnsafePointer[duckdb_ext_api_v1_unstable, ...])` is actually executed
at test time.

Registers a single scalar function whose name distinguishes it from
the stable extension so both can be loaded into the same connection.
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.api_level import ApiLevel
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction


def triple_value(x: Int64) -> Int64:
    """Triples the input value."""
    return x * 3


def init(conn: Connection[ApiLevel.EXT_UNSTABLE]) raises:
    """Register the unstable-path test function."""
    # Unary scalar (row-at-a-time): BIGINT -> BIGINT
    ScalarFunction.from_function[
        "test_ext_unstable_triple", DType.int64, DType.int64, triple_value
    ](conn)


@export("mojo_unstable_init_c_api")
def mojo_unstable_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) abi("C") -> Bool:
    """Entry point called by DuckDB when loading this extension.

    Uses `Extension.run_unstable[init]` to request the unstable API
    struct, exercising the `LibDuckDB.__init__(unstable_api: ...)`
    constructor.
    """
    return Extension.run_unstable[init](info, access)
