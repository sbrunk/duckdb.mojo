"""Shared timing/quality helpers for the Mojo benchmark harnesses.

Import with `-I benchmark/mojo`: `from bench_util import warm_median, recall`.
"""
from std.collections import List


def warm_median(mut t: List[Int]) raises -> Float64:
    """Median of nanosecond samples, returned in ms (insertion sort; small lists)."""
    for i in range(1, len(t)):
        var v = t[i]
        var j = i - 1
        while j >= 0 and t[j] > v:
            t[j + 1] = t[j]
            j -= 1
        t[j + 1] = v
    return Float64(t[len(t) // 2]) / 1e6


def recall(got: List[Int64], exact: List[Int64]) raises -> Float64:
    """Fraction of `exact` ids present in `got` (recall@k vs an exact reference)."""
    if len(exact) == 0:
        return 0.0
    var hit = 0
    for g in got:
        for e in exact:
            if g == e:
                hit += 1
                break
    return Float64(hit) / Float64(len(exact))
