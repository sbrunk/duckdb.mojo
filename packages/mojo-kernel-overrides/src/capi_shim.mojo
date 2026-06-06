"""C-ABI shim for the mojo_overrides extension.

Exports the `duckdb.kernels.simd` bulk kernels as `extern "C"` symbols so the
C++ override extension can `dlopen` them and call over raw FLAT column buffers.
Built to `build/libmojo_simd.{dylib,so}` by build.sh.
"""

from duckdb.kernels.simd import (
    ksqrt,
    ksin,
    kcos,
    kln,
    kexp,
    klog10,
    map_unary,
    reduce_sum_f64,
    reduce_min_f64,
    reduce_max_f64,
    reduce_min_f32,
    reduce_max_f32,
)


@export("mojo_sqrt_f64")
def mojo_sqrt_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[ksqrt](a, dst, n)


@export("mojo_sin_f64")
def mojo_sin_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[ksin](a, dst, n)


@export("mojo_cos_f64")
def mojo_cos_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[kcos](a, dst, n)


@export("mojo_ln_f64")
def mojo_ln_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[kln](a, dst, n)


@export("mojo_exp_f64")
def mojo_exp_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[kexp](a, dst, n)


@export("mojo_log10_f64")
def mojo_log10_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int) abi("C"):
    map_unary[klog10](a, dst, n)


@export("mojo_sum_f64")
def mojo_sum_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) abi("C") -> Float64:
    return reduce_sum_f64(a, n)


@export("mojo_min_f64")
def mojo_min_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) abi("C") -> Float64:
    return reduce_min_f64(a, n)


@export("mojo_max_f64")
def mojo_max_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) abi("C") -> Float64:
    return reduce_max_f64(a, n)


@export("mojo_min_f32")
def mojo_min_f32(a: UnsafePointer[Float32, ImmutAnyOrigin], n: Int) abi("C") -> Float32:
    return reduce_min_f32(a, n)


@export("mojo_max_f32")
def mojo_max_f32(a: UnsafePointer[Float32, ImmutAnyOrigin], n: Int) abi("C") -> Float32:
    return reduce_max_f32(a, n)
