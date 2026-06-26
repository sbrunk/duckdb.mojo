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
    reduce_sum_i128,
    reduce_min_f64,
    reduce_max_f64,
    reduce_min_f32,
    reduce_max_f32,
    array_dot,
    array_l2dist,
    array_cosine_sim,
    reduce_sum_f64_masked,
    reduce_minmax_masked,
    reduce_sum_i128_masked,
    W32,
    W64,
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


@export("mojo_sum_i128")
def mojo_sum_i128(
    a: UnsafePointer[Int128, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Int128, MutAnyOrigin],
    out_overflow: UnsafePointer[Int32, MutAnyOrigin],
) abi("C"):
    reduce_sum_i128(a, n, out_val, out_overflow)


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


# ---- vector-distance folds over two FLAT array buffers (per row) ----


@export("mojo_array_dot_f32")
def mojo_array_dot_f32(
    a: UnsafePointer[Float32, ImmutAnyOrigin], b: UnsafePointer[Float32, ImmutAnyOrigin], n: Int
) abi("C") -> Float32:
    return array_dot[DType.float32, W32](a, b, n)


@export("mojo_array_dot_f64")
def mojo_array_dot_f64(
    a: UnsafePointer[Float64, ImmutAnyOrigin], b: UnsafePointer[Float64, ImmutAnyOrigin], n: Int
) abi("C") -> Float64:
    return array_dot[DType.float64, W64](a, b, n)


@export("mojo_array_l2dist_f32")
def mojo_array_l2dist_f32(
    a: UnsafePointer[Float32, ImmutAnyOrigin], b: UnsafePointer[Float32, ImmutAnyOrigin], n: Int
) abi("C") -> Float32:
    return array_l2dist[DType.float32, W32](a, b, n)


@export("mojo_array_l2dist_f64")
def mojo_array_l2dist_f64(
    a: UnsafePointer[Float64, ImmutAnyOrigin], b: UnsafePointer[Float64, ImmutAnyOrigin], n: Int
) abi("C") -> Float64:
    return array_l2dist[DType.float64, W64](a, b, n)


@export("mojo_array_cosine_sim_f32")
def mojo_array_cosine_sim_f32(
    a: UnsafePointer[Float32, ImmutAnyOrigin], b: UnsafePointer[Float32, ImmutAnyOrigin], n: Int
) abi("C") -> Float32:
    return array_cosine_sim[DType.float32, W32](a, b, n)


@export("mojo_array_cosine_sim_f64")
def mojo_array_cosine_sim_f64(
    a: UnsafePointer[Float64, ImmutAnyOrigin], b: UnsafePointer[Float64, ImmutAnyOrigin], n: Int
) abi("C") -> Float64:
    return array_cosine_sim[DType.float64, W64](a, b, n)


# ---- nullable (validity-masked) reductions: A1 mask-multiply ----


@export("mojo_sum_f64_masked")
def mojo_sum_f64_masked(
    a: UnsafePointer[Float64, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_sum: UnsafePointer[Float64, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
) abi("C"):
    reduce_sum_f64_masked(a, valid, n, out_sum, out_count)


@export("mojo_min_f64_masked")
def mojo_min_f64_masked(
    a: UnsafePointer[Float64, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Float64, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
) abi("C"):
    reduce_minmax_masked[DType.float64, W64, True](a, valid, n, out_val, out_count)


@export("mojo_max_f64_masked")
def mojo_max_f64_masked(
    a: UnsafePointer[Float64, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Float64, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
) abi("C"):
    reduce_minmax_masked[DType.float64, W64, False](a, valid, n, out_val, out_count)


@export("mojo_min_f32_masked")
def mojo_min_f32_masked(
    a: UnsafePointer[Float32, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Float32, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
) abi("C"):
    reduce_minmax_masked[DType.float32, W32, True](a, valid, n, out_val, out_count)


@export("mojo_max_f32_masked")
def mojo_max_f32_masked(
    a: UnsafePointer[Float32, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Float32, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
) abi("C"):
    reduce_minmax_masked[DType.float32, W32, False](a, valid, n, out_val, out_count)


@export("mojo_sum_i128_masked")
def mojo_sum_i128_masked(
    a: UnsafePointer[Int128, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Int128, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
    out_overflow: UnsafePointer[Int32, MutAnyOrigin],
) abi("C"):
    reduce_sum_i128_masked(a, valid, n, out_val, out_count, out_overflow)
