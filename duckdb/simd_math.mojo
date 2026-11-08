"""Mojo SIMD math as DuckDB scalar functions.

`register_simd_math` registers `mojo_sqrt`, `mojo_sin`, `mojo_cos`, `mojo_ln`,
`mojo_exp` and `mojo_log10` (DOUBLE -> DOUBLE), backed by the vectorized
kernels in `duckdb.kernels.simd`. Works on a `Connection` and inside an
`Extension`.
"""

from duckdb.kernels.simd import kcos, kexp, kln, klog10, ksin, ksqrt
from duckdb.error import DuckDBError
from duckdb.functions import Registrar, ScalarFunction


def register_simd_math[R: Registrar](registrar: R) raises DuckDBError:
    """Register the SIMD math functions."""
    registrar.register(ScalarFunction.from_simd_function["mojo_sqrt", DType.float64, DType.float64, ksqrt]())
    registrar.register(ScalarFunction.from_simd_function["mojo_sin", DType.float64, DType.float64, ksin]())
    registrar.register(ScalarFunction.from_simd_function["mojo_cos", DType.float64, DType.float64, kcos]())
    registrar.register(ScalarFunction.from_simd_function["mojo_ln", DType.float64, DType.float64, kln]())
    registrar.register(ScalarFunction.from_simd_function["mojo_exp", DType.float64, DType.float64, kexp]())
    registrar.register(ScalarFunction.from_simd_function["mojo_log10", DType.float64, DType.float64, klog10]())
