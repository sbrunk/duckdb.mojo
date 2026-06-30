"""Convenience registration of Mojo SIMD math as DuckDB scalar UDFs.

Portable Tier-A path: works through the stable C API from the client or any
extension, no C++/internal-API needed. Registers `mojo_sqrt`, `mojo_sin`,
`mojo_cos`, `mojo_ln`, `mojo_exp`, `mojo_log10` as DOUBLE -> DOUBLE functions
backed by the vectorized kernels in `duckdb.kernels.simd`. Call them explicitly,
e.g. `SELECT mojo_sqrt(x)`.
"""

from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction
from duckdb.logical_type import LogicalType
from duckdb.duckdb_type import dtype_to_duckdb_type
from duckdb.kernels.simd import ksqrt, ksin, kcos, kln, kexp, klog10


def _register[
    name: StringLiteral,
    func: def[w: Int] (SIMD[DType.float64, w]) thin -> SIMD[DType.float64, w],
](conn: Connection[_]) raises:
    var sf = ScalarFunction()
    sf.set_name(name)
    sf.add_parameter(LogicalType(dtype_to_duckdb_type[DType.float64]()))
    sf.set_return_type(LogicalType(dtype_to_duckdb_type[DType.float64]()))
    sf.set_simd_function[DType.float64, DType.float64, func]()
    sf.register(conn)


def register_simd_math(conn: Connection[_]) raises:
    """Register the SIMD math scalar UDFs on a connection."""
    _register["mojo_sqrt", ksqrt](conn)
    _register["mojo_sin", ksin](conn)
    _register["mojo_cos", kcos](conn)
    _register["mojo_ln", kln](conn)
    _register["mojo_exp", kexp](conn)
    _register["mojo_log10", klog10](conn)
