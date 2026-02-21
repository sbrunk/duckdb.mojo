"""Test extension for exercising the duckdb.mojo extension API.

Registers multiple function types to test various extension code paths:
- Scalar functions (row-at-a-time and SIMD)
- Aggregate functions
- Multiple functions in one extension
- Extension.run error handling
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.api_level import ApiLevel
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction
from duckdb.aggregate_function import AggregateFunction


# ===--------------------------------------------------------------------===#
# Scalar functions
# ===--------------------------------------------------------------------===#


fn add_numbers(a: Int64, b: Int64) -> Int64:
    """Adds two integers together."""
    return a + b


fn negate(x: Int64) -> Int64:
    """Negates the input."""
    return -x


fn multiply(a: Float64, b: Float64) -> Float64:
    """Multiplies two floats."""
    return a * b


fn double_value(x: Int64) -> Int64:
    """Doubles the input value."""
    return x * 2


# ===--------------------------------------------------------------------===#
# Extension entry point
# ===--------------------------------------------------------------------===#


fn init(conn: Connection[ApiLevel.EXT_STABLE]) raises:
    """Register all test extension functions."""
    # Binary scalar (row-at-a-time): BIGINT x BIGINT -> BIGINT
    ScalarFunction.from_function[
        "test_ext_add", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)

    # Unary scalar (row-at-a-time): BIGINT -> BIGINT
    ScalarFunction.from_function[
        "test_ext_negate", DType.int64, DType.int64, negate
    ](conn)

    # Binary scalar (float): DOUBLE x DOUBLE -> DOUBLE
    ScalarFunction.from_function[
        "test_ext_multiply", DType.float64, DType.float64, DType.float64, multiply
    ](conn)

    # Unary scalar: BIGINT -> BIGINT (doubles the input)
    ScalarFunction.from_function[
        "test_ext_double", DType.int64, DType.int64, double_value
    ](conn)

    # Aggregate function: SUM(BIGINT) -> BIGINT
    AggregateFunction.from_sum["test_ext_sum", DType.int64](conn)


@export("mojo_init_c_api", ABI="C")
fn mojo_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    """Entry point called by DuckDB when loading this extension."""
    return Extension.run[init](info, access)
