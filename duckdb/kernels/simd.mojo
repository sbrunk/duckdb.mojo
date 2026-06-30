"""SIMD kernels shared by the scalar UDF helpers and the override extension.

Two shapes from the same math:
- Elementwise SIMD-generic primitives (`k*`), usable directly with
  `ScalarFunction.set_simd_function` to register fast custom UDFs.
- Pointer-based bulk kernels (`map_unary`, `reduce_*`) over raw FLAT column
  buffers, exported with C ABI by the override extension's shim and called from
  C++ for transparent built-in replacement.
"""

from std.collections import InlineArray
from std.math import sqrt, sin, cos, log, exp, min, max, iota
from std.bit import pop_count

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


# ===--------------------------------------------------------------------===#
# Vector-distance folds over two FLAT array buffers (dot / L2 / cosine).
#
# DuckDB's array_distance / array_inner_product / array_cosine_* are serial
# single-accumulator scalar loops (`result += x*y`) — latency-bound, since an FP
# reduction can't auto-vectorize without -ffast-math. These use `W`-wide SIMD
# accumulators with a 2× unroll to break the dependency chain (the same
# multi-accumulator trick as reduce_sum_i128). `array_dot` covers inner-product
# (and, negated by the caller, negative_inner_product); `array_l2dist` covers
# array_distance; `array_cosine_sim` covers cosine_similarity (and, as 1 - x by
# the caller, cosine_distance). Results match stock within ~1 ULP (different
# summation order).
# ===--------------------------------------------------------------------===#


def array_dot[
    dt: DType, w: Int
](a: UnsafePointer[Scalar[dt], ImmutAnyOrigin], b: UnsafePointer[Scalar[dt], ImmutAnyOrigin], n: Int) -> Scalar[dt]:
    var acc0 = SIMD[dt, w](0)
    var acc1 = SIMD[dt, w](0)
    var i = 0
    while i + 2 * w <= n:
        acc0 += (a + i).load[width=w]() * (b + i).load[width=w]()
        acc1 += (a + i + w).load[width=w]() * (b + i + w).load[width=w]()
        i += 2 * w
    var acc = acc0 + acc1
    while i + w <= n:
        acc += (a + i).load[width=w]() * (b + i).load[width=w]()
        i += w
    var s = acc.reduce_add()
    while i < n:
        s += a[i] * b[i]
        i += 1
    return s


def array_l2dist[
    dt: DType, w: Int
](a: UnsafePointer[Scalar[dt], ImmutAnyOrigin], b: UnsafePointer[Scalar[dt], ImmutAnyOrigin], n: Int) -> Scalar[dt]:
    var acc0 = SIMD[dt, w](0)
    var acc1 = SIMD[dt, w](0)
    var i = 0
    while i + 2 * w <= n:
        var d0 = (a + i).load[width=w]() - (b + i).load[width=w]()
        var d1 = (a + i + w).load[width=w]() - (b + i + w).load[width=w]()
        acc0 += d0 * d0
        acc1 += d1 * d1
        i += 2 * w
    var acc = acc0 + acc1
    while i + w <= n:
        var d = (a + i).load[width=w]() - (b + i).load[width=w]()
        acc += d * d
        i += w
    var s = acc.reduce_add()
    while i < n:
        var d = a[i] - b[i]
        s += d * d
        i += 1
    return sqrt(s)


def array_cosine_sim[
    dt: DType, w: Int
](a: UnsafePointer[Scalar[dt], ImmutAnyOrigin], b: UnsafePointer[Scalar[dt], ImmutAnyOrigin], n: Int) -> Scalar[dt]:
    var dot = SIMD[dt, w](0)
    var na = SIMD[dt, w](0)
    var nb = SIMD[dt, w](0)
    var i = 0
    while i + w <= n:
        var x = (a + i).load[width=w]()
        var y = (b + i).load[width=w]()
        dot += x * y
        na += x * x
        nb += y * y
        i += w
    var sdot = dot.reduce_add()
    var sna = na.reduce_add()
    var snb = nb.reduce_add()
    while i < n:
        var x = a[i]
        var y = b[i]
        sdot += x * y
        sna += x * x
        snb += y * y
        i += 1
    var sim = sdot / sqrt(sna * snb)
    return max(Scalar[dt](-1), min(sim, Scalar[dt](1)))


# ===--------------------------------------------------------------------===#
# Nullable (validity-masked) reductions — the A1 mask-multiply model.
#
# DuckDB stores per-row validity as a bitmask (uint64 words, bit set = valid).
# The non-masked kernels above require AllValid and otherwise fall back to stock.
# These variants reduce only the valid lanes branchlessly via SIMD select, so the
# overrides apply to nullable columns instead of bailing. They also return the
# valid count (for AVG, and so MIN/MAX/SUM can leave the state unset on an
# all-NULL chunk). Width `w` divides 64, and the loop index is a multiple of `w`,
# so each `w`-lane block lies within a single validity word.
# ===--------------------------------------------------------------------===#


def reduce_sum_f64_masked(
    a: UnsafePointer[Float64, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_sum: UnsafePointer[Float64, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
):
    comptime LOWMASK = (UInt64(1) << UInt64(W64)) - 1
    var lane = iota[DType.uint64, W64]()
    var acc = SIMD[DType.float64, W64](0)
    var cnt = Int64(0)
    var i = 0
    while i + W64 <= n:
        var bits = valid[i >> 6] >> UInt64(i & 63)
        var mbits = (SIMD[DType.uint64, W64](bits) >> lane) & SIMD[DType.uint64, W64](1)
        var m = mbits.gt(SIMD[DType.uint64, W64](0))
        acc += m.select((a + i).load[width=W64](), SIMD[DType.float64, W64](0))
        cnt += Int64(pop_count(Int(bits & LOWMASK)))
        i += W64
    var s = acc.reduce_add()
    while i < n:
        if (valid[i >> 6] >> UInt64(i & 63)) & 1:
            s += a[i]
            cnt += 1
        i += 1
    out_sum[0] = s
    out_count[0] = cnt


def reduce_minmax_masked[
    dt: DType, w: Int, is_min: Bool
](
    a: UnsafePointer[Scalar[dt], ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Scalar[dt], MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
):
    comptime LOWMASK = (UInt64(1) << UInt64(w)) - 1
    comptime ident = Scalar[dt].MAX_FINITE if is_min else -Scalar[dt].MAX_FINITE
    var identv = SIMD[dt, w](ident)
    var lane = iota[DType.uint64, w]()
    var acc = identv
    var cnt = Int64(0)
    var i = 0
    while i + w <= n:
        var bits = valid[i >> 6] >> UInt64(i & 63)
        var mbits = (SIMD[DType.uint64, w](bits) >> lane) & SIMD[DType.uint64, w](1)
        var m = mbits.gt(SIMD[DType.uint64, w](0))
        var x = m.select((a + i).load[width=w](), identv)
        comptime if is_min:
            acc = min(acc, x)
        else:
            acc = max(acc, x)
        cnt += Int64(pop_count(Int(bits & LOWMASK)))
        i += w
    var s: Scalar[dt]
    comptime if is_min:
        s = acc.reduce_min()
    else:
        s = acc.reduce_max()
    while i < n:
        if (valid[i >> 6] >> UInt64(i & 63)) & 1:
            comptime if is_min:
                s = min(s, a[i])
            else:
                s = max(s, a[i])
            cnt += 1
        i += 1
    out_val[0] = s
    out_count[0] = cnt


def reduce_sum_i128_masked(
    a: UnsafePointer[Int128, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_val: UnsafePointer[Int128, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
    out_overflow: UnsafePointer[Int32, MutAnyOrigin],
):
    """Validity-masked int128 sum. Scalar (int128 has no register SIMD path) with
    the same branchless overflow detection as `reduce_sum_i128`."""
    var total = Int128(0)
    var ovf = Int128(0)
    var cnt = Int64(0)
    var i = 0
    while i < n:
        if (valid[i >> 6] >> UInt64(i & 63)) & 1:
            var x = a[i]
            var s = total + x
            ovf |= (total ^ s) & (x ^ s)
            total = s
            cnt += 1
        i += 1
    out_val[0] = total
    out_count[0] = cnt
    out_overflow[0] = Int32(1) if ovf < 0 else Int32(0)


# ===--------------------------------------------------------------------===#
# Fused sum-of-transcendental (item 2): one pass computing f(x) and accumulating,
# no intermediate vector. Driven by an optimizer rewrite of sum/avg(f(col)). The
# masked variant reduces only valid lanes (A1 model) and returns the valid count.
# ===--------------------------------------------------------------------===#


def reduce_fsum_map[
    f: def[w: Int] (SIMD[DType.float64, w]) thin -> SIMD[DType.float64, w]
](a: UnsafePointer[Float64, ImmutAnyOrigin], n: Int) -> Float64:
    var acc = SIMD[DType.float64, W64](0)
    var i = 0
    while i + W64 <= n:
        acc += f[W64]((a + i).load[width=W64]())
        i += W64
    var s = acc.reduce_add()
    while i < n:
        s += f[1]((a + i).load[width=1]())[0]
        i += 1
    return s


def reduce_fsum_map_masked[
    f: def[w: Int] (SIMD[DType.float64, w]) thin -> SIMD[DType.float64, w]
](
    a: UnsafePointer[Float64, ImmutAnyOrigin],
    valid: UnsafePointer[UInt64, ImmutAnyOrigin],
    n: Int,
    out_sum: UnsafePointer[Float64, MutAnyOrigin],
    out_count: UnsafePointer[Int64, MutAnyOrigin],
):
    comptime LOWMASK = (UInt64(1) << UInt64(W64)) - 1
    var lane = iota[DType.uint64, W64]()
    var acc = SIMD[DType.float64, W64](0)
    var cnt = Int64(0)
    var i = 0
    while i + W64 <= n:
        var bits = valid[i >> 6] >> UInt64(i & 63)
        var mbits = (SIMD[DType.uint64, W64](bits) >> lane) & SIMD[DType.uint64, W64](1)
        var m = mbits.gt(SIMD[DType.uint64, W64](0))
        acc += m.select(f[W64]((a + i).load[width=W64]()), SIMD[DType.float64, W64](0))
        cnt += Int64(pop_count(Int(bits & LOWMASK)))
        i += W64
    var s = acc.reduce_add()
    while i < n:
        if (valid[i >> 6] >> UInt64(i & 63)) & 1:
            s += f[1]((a + i).load[width=1]())[0]
            cnt += 1
        i += 1
    out_sum[0] = s
    out_count[0] = cnt


# ===--------------------------------------------------------------------===#
# Blocked multi-query brute-force kNN (item 4).
#
# Register-tiled: process QT queries per embedding load so each embedding row's
# bytes are reused across QT queries from registers (raising arithmetic intensity
# — the per-pair dot is otherwise L1/L2 read-bound on the two vectors, so naive
# M separate scans waste loads). ~1.85× (NEON) / ~2.76× (AVX-512) over naive.
# Per-vector norms are precomputed by the caller: cosine -> L2 norms, l2 ->
# squared norms, ip -> ignored. metric: 0=cosine_distance, 1=l2(array_distance),
# 2=negative_inner_product. Maintains an ascending top-k (dist,id) per query.
# Results match stock up to FP-summation-order tie-breaks at rank k.
# ===--------------------------------------------------------------------===#


def _topk_insert(
    td: UnsafePointer[Float32, MutAnyOrigin],
    ti: UnsafePointer[Int64, MutAnyOrigin],
    base: Int,
    k: Int,
    d: Float32,
    id: Int64,
):
    if d >= td[base + k - 1]:
        return
    var j = k - 1
    while j > 0 and td[base + j - 1] > d:
        td[base + j] = td[base + j - 1]
        ti[base + j] = ti[base + j - 1]
        j -= 1
    td[base + j] = d
    ti[base + j] = id


def _dist_from_dot[metric: Int](s: Float32, qn: Float32, en: Float32) -> Float32:
    comptime if metric == 0:
        return 1.0 - s / (qn * en)
    comptime if metric == 1:
        return sqrt(max(Float32(0), qn + en - 2.0 * s))
    return -s


def knn_topk[
    metric: Int
](
    q: UnsafePointer[Float32, ImmutAnyOrigin],
    nrm_q: UnsafePointer[Float32, ImmutAnyOrigin],
    m: Int,
    e: UnsafePointer[Float32, ImmutAnyOrigin],
    nrm_e: UnsafePointer[Float32, ImmutAnyOrigin],
    n: Int,
    d_dim: Int,
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
):
    comptime QT = 4
    comptime W = W32
    for x in range(m * k):
        out_dists[x] = Float32(1e30)
        out_ids[x] = Int64(-1)
    var qg = 0
    while qg + QT <= m:
        for ni in range(n):
            var ep = e + ni * d_dim
            var acc = InlineArray[SIMD[DType.float32, W], QT](fill=SIMD[DType.float32, W](0))
            var dd = 0
            while dd + W <= d_dim:
                var ev = (ep + dd).load[width=W]()
                comptime for t in range(QT):
                    acc[t] += (q + (qg + t) * d_dim + dd).load[width=W]() * ev
                dd += W
            comptime for t in range(QT):
                var s = acc[t].reduce_add()
                var qp = q + (qg + t) * d_dim
                var j = dd
                while j < d_dim:
                    s += qp[j] * ep[j]
                    j += 1
                var dist = _dist_from_dot[metric](s, nrm_q[qg + t], nrm_e[ni])
                _topk_insert(out_dists, out_ids, (qg + t) * k, k, dist, Int64(ni))
        qg += QT
    while qg < m:
        var qp = q + qg * d_dim
        for ni in range(n):
            var s = array_dot[DType.float32, W](qp, e + ni * d_dim, d_dim)
            var dist = _dist_from_dot[metric](s, nrm_q[qg], nrm_e[ni])
            _topk_insert(out_dists, out_ids, qg * k, k, dist, Int64(ni))
        qg += 1


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
