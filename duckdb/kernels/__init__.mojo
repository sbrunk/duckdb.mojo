from duckdb.kernels.simd import (
    ksqrt,
    ksin,
    kcos,
    kln,
    kexp,
    klog10,
)
from duckdb.kernels.udf import register_simd_math
