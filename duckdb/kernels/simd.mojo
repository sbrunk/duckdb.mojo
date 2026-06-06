"""SIMD kernels shared by the scalar UDF helpers and the override extension.

Two shapes from the same math:
- Elementwise SIMD-generic primitives (`k*`), usable directly with
  `ScalarFunction.set_simd_function` to register fast custom UDFs.
- Pointer-based bulk kernels (`map_unary`, `reduce_*`) over raw FLAT column
  buffers, exported with C ABI by the override extension's shim and called from
  C++ for transparent built-in replacement.
"""

from std.math import sqrt, sin, cos, log, exp, min, max

comptime INV_LN10 = 0.4342944819032518
comptime W64 = 8
comptime W32 = 16


# ===--------------------------------------------------------------------===#
# Elementwise SIMD-generic primitives (DOUBLE -> DOUBLE)
# ===--------------------------------------------------------------------===#

def ksqrt[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return sqrt(x)

def ksin[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return sin(x)

def kcos[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return cos(x)

def kln[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return log(x)

def kexp[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return exp(x)

def klog10[w: Int](x: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return log(x) * INV_LN10


# ===--------------------------------------------------------------------===#
# Pointer-based bulk kernels (raw FLAT buffers) — used by the C-ABI shim.
# ===--------------------------------------------------------------------===#

def map_unary[
    f: def[w: Int] (SIMD[DType.float64, w]) thin -> SIMD[DType.float64, w]
](a: UnsafePointer[Float64, ImmutAnyOrigin], dst: UnsafePointer[Float64, MutAnyOrigin], n: Int):
    var i = 0
    while i + W64 <= n:
        dst.store(i, f[W64]((a + i).load[width=W64]()))
        i += W64
    while i < n:
        dst.store(i, f[1]((a + i).load[width=1]()))
        i += 1


def reduce_sum_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) -> Float64:
    var acc = SIMD[DType.float64, W64](0)
    var i = 0
    while i + W64 <= n:
        acc += (a + i).load[width=W64]()
        i += W64
    var s = acc.reduce_add()
    while i < n:
        s += a[i]
        i += 1
    return s


def reduce_min_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) -> Float64:
    var acc = SIMD[DType.float64, W64](a[0])
    var i = 0
    while i + W64 <= n:
        acc = min(acc, (a + i).load[width=W64]())
        i += W64
    var s = acc.reduce_min()
    while i < n:
        s = min(s, a[i])
        i += 1
    return s


def reduce_max_f64(a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) -> Float64:
    var acc = SIMD[DType.float64, W64](a[0])
    var i = 0
    while i + W64 <= n:
        acc = max(acc, (a + i).load[width=W64]())
        i += W64
    var s = acc.reduce_max()
    while i < n:
        s = max(s, a[i])
        i += 1
    return s


def reduce_min_f32(a: UnsafePointer[Float32, ImmutAnyOrigin], n: Int) -> Float32:
    var acc = SIMD[DType.float32, W32](a[0])
    var i = 0
    while i + W32 <= n:
        acc = min(acc, (a + i).load[width=W32]())
        i += W32
    var s = acc.reduce_min()
    while i < n:
        s = min(s, a[i])
        i += 1
    return s


def reduce_max_f32(a: UnsafePointer[Float32, ImmutAnyOrigin], n: Int) -> Float32:
    var acc = SIMD[DType.float32, W32](a[0])
    var i = 0
    while i + W32 <= n:
        acc = max(acc, (a + i).load[width=W32]())
        i += W32
    var s = acc.reduce_max()
    while i < n:
        s = max(s, a[i])
        i += 1
    return s
