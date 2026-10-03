"""Test extension for exercising the duckdb.mojo extension API.

Registers multiple function types to test various extension code paths:
- Scalar functions (row-at-a-time and SIMD)
- Aggregate functions
- Multiple functions in one extension
- Extension.run error handling
- C API functions that were only stabilized in API version v1.5.6
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction
from duckdb.aggregate_function import AggregateFunction
from duckdb.logical_type import LogicalType
from duckdb.duckdb_type import DuckDBType
from duckdb.vector import Vector


# ===--------------------------------------------------------------------===#
# Scalar functions
# ===--------------------------------------------------------------------===#


def add_numbers(a: Int64, b: Int64) -> Int64:
    """Adds two integers together."""
    return a + b


def negate(x: Int64) -> Int64:
    """Negates the input."""
    return -x


def multiply(a: Float64, b: Float64) -> Float64:
    """Multiplies two floats."""
    return a * b


def double_value(x: Int64) -> Int64:
    """Doubles the input value."""
    return x * 2


def triple_value(x: Int64) -> Int64:
    """Triples the input value."""
    return x * 3


def check_v1_5_6_api() raises:
    """Call C API functions from the v1.5.6 band of the extension API struct.

    `duckdb_create_vector` and `duckdb_destroy_vector` were unstable before
    DuckDB 1.5.6, so this checks that `Extension.run` wires them up from the
    struct.
    """
    var vector = Vector(LogicalType(DuckDBType.bigint), 16)
    if vector.get_column_type().get_type_id() != DuckDBType.bigint:
        raise Error("standalone vector has the wrong type")


# ===--------------------------------------------------------------------===#
# Extension entry point
# ===--------------------------------------------------------------------===#


def init(conn: Connection) raises:
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

    # Registered only if the v1.5.6 C API functions work
    check_v1_5_6_api()
    ScalarFunction.from_function[
        "test_ext_triple", DType.int64, DType.int64, triple_value
    ](conn)


@export("mojo_init_c_api")
def mojo_init_c_api(
    info: duckdb_extension_info,
    access: Pointer[duckdb_extension_access, MutUntrackedOrigin],
) abi("C") -> Bool:
    """Entry point called by DuckDB when loading this extension."""
    return Extension.run[init](info, access)
