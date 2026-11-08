"""Scalar functions: stdlib math, custom SIMD kernels and row-at-a-time functions.

    pixi run mojo run examples/scalar_function.mojo
"""

import std.math as math
import duckdb
from duckdb import ScalarFunction


def sin_plus_cos[w: SIMDLength](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return math.sin(x) + math.cos(x)


# Row-at-a-time functions take and return any mapped type. The SQL signature is
# inferred from the Mojo one. NULL arguments skip the call.
def add_one(x: Int32) -> Int32:
    return x + 1


def greet(name: String) -> String:
    return String("hello ", name)


# An Optional argument sees NULLs, an Optional result can return NULL.
def or_zero(x: Optional[Int64]) -> Int64:
    return x.value() if x else 0


def main() raises:
    var con = duckdb.connect()
    _ = con.execute(
        "CREATE TABLE t AS SELECT i::DOUBLE AS x, i::INTEGER AS n, 'row ' || i AS s,"
        " CASE WHEN i % 2 = 0 THEN i END AS maybe FROM range(5) r(i)"
    )

    # Stdlib math functions, passed directly
    con.register(ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, math.sqrt]())
    con.register(ScalarFunction.from_simd_function["mojo_atan2", DType.float64, math.atan2]())

    # A custom SIMD kernel
    con.register(ScalarFunction.from_simd_function["mojo_sin_plus_cos", DType.float64, DType.float64, sin_plus_cos]())

    # Row-at-a-time functions
    con.register(ScalarFunction.from_function["add_one", add_one]())
    con.register(ScalarFunction.from_function["greet", greet]())
    con.register(ScalarFunction.from_function["or_zero", or_zero]())

    con.execute(
        "SELECT x, mojo_sqrt(x), mojo_atan2(x, 2.0), mojo_sin_plus_cos(x), add_one(n), greet(s), or_zero(maybe) FROM t"
    ).show()
