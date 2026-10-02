"""Scalar functions: stdlib math, custom SIMD kernels and row-at-a-time functions.

    pixi run mojo run examples/scalar_function.mojo
"""

import std.math as math
from duckdb import *
from duckdb.scalar_function import ScalarFunction


def sin_plus_cos[w: SIMDLength](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return math.sin(x) + math.cos(x)


def add_one(x: Int32) -> Int32:
    return x + 1


def main() raises:
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE t AS SELECT i::DOUBLE AS x, i::INTEGER AS n FROM range(5) r(i)")

    # Stdlib math functions, passed directly
    ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, math.sqrt](conn)
    ScalarFunction.from_simd_function["mojo_atan2", DType.float64, math.atan2](conn)

    # A custom SIMD kernel
    ScalarFunction.from_simd_function[
        "mojo_sin_plus_cos", DType.float64, DType.float64, sin_plus_cos
    ](conn)

    # A row-at-a-time function
    ScalarFunction.from_function["add_one", DType.int32, DType.int32, add_one](conn)

    conn.execute(
        "SELECT x, mojo_sqrt(x), mojo_atan2(x, 2.0), mojo_sin_plus_cos(x), add_one(n) FROM t"
    ).show()
