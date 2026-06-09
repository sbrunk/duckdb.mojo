"""SIMD kernels shared by the scalar UDF helpers and the override extension.

Two shapes from the same math:
- Elementwise SIMD-generic primitives (`k*`), usable directly with
  `ScalarFunction.set_simd_function` to register fast custom UDFs.
- Pointer-based bulk kernels (`map_unary`, `reduce_*`) over raw FLAT column
  buffers, exported with C ABI by the override extension's shim and called from
  C++ for transparent built-in replacement.
"""

from std.collections import InlineArray
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


def reduce_sum_i128(
    a: UnsafePointer[Int128, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Int128, MutAnyOrigin],
    out_overflow: UnsafePointer[Int32, MutAnyOrigin],
):
    """Sum of a FLAT int128 column with multiple independent accumulators.

    DuckDB's HUGEINT / high-precision DECIMAL sum (HugeintSumOperation) adds each
    element via the overflow-checked, non-inlined `Hugeint::Add` (a function call
    per element). This inlines the add and breaks the latency chain
    with `K` accumulators. Signed overflow is detected branchlessly: the sign bit
    of `(acc ^ s) & (x ^ s)` is set if `acc + x` overflowed. On any overflow the
    caller falls back to stock (preserving the exact throw semantics). Because
    integer addition is exact and associative, a non-overflowing reduce returns
    the same total as stock regardless of accumulator partitioning.
    """
    comptime K = 4
    var acc = InlineArray[Int128, K](fill=Int128(0))
    var ovf = Int128(0)
    var i = 0
    while i + K <= n:
        comptime for k in range(K):
            var x = a[i + k]
            var s = acc[k] + x
            ovf |= (acc[k] ^ s) & (x ^ s)
            acc[k] = s
        i += K
    var total = Int128(0)
    comptime for k in range(K):
        var s = total + acc[k]
        ovf |= (total ^ s) & (acc[k] ^ s)
        total = s
    while i < n:
        var x = a[i]
        var s = total + x
        ovf |= (total ^ s) & (x ^ s)
        total = s
        i += 1
    out_val[0] = total
    out_overflow[0] = Int32(1) if ovf < 0 else Int32(0)


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
