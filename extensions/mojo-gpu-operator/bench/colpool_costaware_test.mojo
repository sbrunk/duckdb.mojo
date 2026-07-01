"""Phase 2 cost-aware placement validation (Mojo-only, needs a GPU).

Exercises the column pool's eviction policy directly at the col_pool API level
(no full query shuttle needed): the cost-aware keep-benefit policy and plain LRU
are *different victim pickers*, so the cleanest proof is to drive the SAME pool
state through BOTH and assert they evict OPPOSITE columns.

Correctness is free here (invariant #2: eviction only changes WHICH column is
cold-rebuilt; results are unaffected), so this test asserts POLICY behavior, not
numeric results:

  Scenario 1 -- keep-benefit protects hot-small, LRU wrongly evicts it.
    A small column touched many times (HOT) and a large column touched once
    (COLD, but more RECENTLY). keep-benefit keeps A and evicts the large cold B;
    plain LRU keeps B (newer) and evicts A. Opposite victims on identical state.

  Scenario 2 -- proactive promotion protects a hot-but-huge column.
    A huge column touched past the promotion threshold (PROMOTED, yet LOW
    keep-benefit because it is large) and a small column touched once (NOT
    promoted, HIGHER keep-benefit). Pure benefit ranking would evict the huge
    promoted column; promotion makes the policy evict the small non-promoted one
    instead -- "keep proven-hot data resident under pressure."

Run from the repo root:
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/colpool_costaware_test.mojo
(No env flags needed -- the test calls evict_lru / evict_by_keep_benefit
directly, so it validates both policies in one process regardless of
GPU_OP_COLPOOL_COSTAWARE. It DOES read the default promotion threshold; an
override of GPU_OP_COLPOOL_PROMOTE_HITS outside [2,5] skips scenario 2.)
"""

from col_pool import (
    col_key,
    ensure_column,
    release_lease,
    evict_lru,
    evict_by_keep_benefit,
    keep_benefit,
    pool_bytes,
    pool_resident_cols,
    pool_promoted_cols,
    pool_evictions,
    pool_hits,
    pool_misses,
    col_pool_resident_nrows,
    _promote_threshold,
    REPR_INT64_PACKED,
    ORDERING_STORAGE,
)
from std.gpu.host import DeviceContext
from std.memory import alloc
from std.os import getenv
from std.sys import has_accelerator
from std.testing import assert_equal, assert_true, assert_false


comptime TABLE = "synth"


# Drain every evictable (refcount == 0) pooled column so each scenario starts
# from an empty pool. The test always release_lease()es after ensure, so nothing
# is leased here -> the pool empties fully.
def _drain() raises:
    while evict_lru() > 0:
        pass


# Touch `column` (`n` rows) `times` times via ensure_column, releasing the lease
# after each so the column ends resident + EVICTABLE with access_count == times.
# Returns the resident byte footprint (n * 8).
def _touch(
    ctx: DeviceContext, column: String, n: Int, times: Int
) raises -> Int:
    var host = alloc[Int64](n if n > 0 else 1)
    for i in range(n):
        host[i] = Int64(i)
    var key = col_key(TABLE, column, REPR_INT64_PACKED, ORDERING_STORAGE, n)
    for _t in range(times):
        var er = ensure_column(
            ctx, key, REPR_INT64_PACKED, ORDERING_STORAGE,
            host.unsafe_origin_cast[MutAnyOrigin](), n, Int64(0),
        )
        assert_true(er.ok, "ensure_column should succeed for " + column)
        release_lease(key)
    host.free()
    return n * 8


# True iff `column` (`n` rows) is currently resident in the pool.
def _resident(column: String, n: Int) raises -> Bool:
    var rn = col_pool_resident_nrows(
        TABLE, column, REPR_INT64_PACKED, ORDERING_STORAGE
    )
    return rn == n


def main() raises:
    if not has_accelerator():
        print("SKIP: colpool_costaware_test requires a GPU. ALL PASS")
        return

    var ctx = DeviceContext()

    var SMALL = 1000  # 8 KB
    var LARGE = 1_000_000  # 8 MB

    # =====================================================================
    # Scenario 1: keep-benefit vs LRU pick OPPOSITE victims on identical state.
    # =====================================================================
    _drain()
    var ev0 = pool_evictions()

    # HOT small column: touched 5x (older last_use after this).
    var a_bytes = _touch(ctx, "hot_small", SMALL, 5)
    # COLD large column: touched once, AFTER A -> newer last_use.
    var b_bytes = _touch(ctx, "cold_large", LARGE, 1)

    assert_true(_resident("hot_small", SMALL), "A should be resident")
    assert_true(_resident("cold_large", LARGE), "B should be resident")
    assert_equal(pool_resident_cols(), 2, "exactly A + B resident")

    # keep-benefit ranks A (hot, small) strictly above B (cold, large).
    var ben_a = keep_benefit(5, a_bytes)
    var ben_b = keep_benefit(1, b_bytes)
    print("scenario 1: keep_benefit(A hot/small) =", ben_a,
          "  keep_benefit(B cold/large) =", ben_b)
    assert_true(ben_a > ben_b, "hot small column must outrank cold large one")

    # Cost-aware policy evicts the LOW-benefit B (the large cold one).
    var freed_ca = evict_by_keep_benefit()
    print("cost-aware evicted bytes:", freed_ca, " (expect B =", b_bytes, ")")
    assert_equal(freed_ca, b_bytes, "keep-benefit must evict the large cold B")
    assert_true(_resident("hot_small", SMALL), "A must survive keep-benefit")
    assert_false(_resident("cold_large", LARGE), "B must be evicted by keep-benefit")

    # Re-add B (now the most-recently-used) so the state matches scenario start,
    # then run PLAIN LRU: it evicts the older-touched A instead -- the opposite.
    _ = _touch(ctx, "cold_large", LARGE, 1)
    assert_equal(pool_resident_cols(), 2, "A + B resident again")
    var freed_lru = evict_lru()
    print("plain-LRU evicted bytes:", freed_lru, " (expect A =", a_bytes, ")")
    assert_equal(freed_lru, a_bytes, "LRU must evict the older-touched small A")
    assert_false(_resident("hot_small", SMALL), "A is the LRU victim")
    assert_true(_resident("cold_large", LARGE), "B (newer) survives LRU")

    # Both policies actually evicted (counter moved by 2 over the scenario).
    assert_equal(pool_evictions() - ev0, 2, "two evictions happened")
    print("scenario 1 PASS: keep-benefit and LRU chose OPPOSITE victims")

    # =====================================================================
    # Scenario 2: proactive promotion protects a hot-but-huge column.
    # =====================================================================
    _drain()
    var threshold = _promote_threshold()
    if threshold < 2 or threshold > 5:
        print(
            "SKIP scenario 2: promotion threshold", threshold,
            "outside [2,5] (set GPU_OP_COLPOOL_PROMOTE_HITS in range to test)",
        )
        print("ALL PASS")
        return

    # PROMOTED huge column: touched 5x (>= threshold) but LOW keep-benefit (large).
    var c_bytes = _touch(ctx, "hot_huge", LARGE, 5)
    # NOT-promoted small column: touched once, HIGHER keep-benefit (small).
    var e_bytes = _touch(ctx, "cold_small", SMALL, 1)

    var ben_c = keep_benefit(5, c_bytes)
    var ben_e = keep_benefit(1, e_bytes)
    print("scenario 2: keep_benefit(C hot/huge, promoted) =", ben_c,
          "  keep_benefit(E cold/small) =", ben_e)
    # Pure benefit ranking would pick C (lower benefit) as the victim...
    assert_true(ben_c < ben_e, "huge promoted column has the LOWER raw benefit")
    # ...but C is promoted and E is not.
    assert_equal(pool_promoted_cols(), 1, "exactly the huge column is promoted")

    # Promotion flips the choice: the policy evicts the non-promoted small E.
    var freed_promo = evict_by_keep_benefit()
    print("with promotion, evicted bytes:", freed_promo,
          " (expect E =", e_bytes, ", NOT promoted C =", c_bytes, ")")
    assert_equal(freed_promo, e_bytes, "promotion must protect C, evict E")
    assert_true(_resident("hot_huge", LARGE), "promoted C survives pressure")
    assert_false(_resident("cold_small", SMALL), "non-promoted E is evicted")
    print("scenario 2 PASS: promotion protected the proven-hot column")

    _drain()
    print("ALL PASS")
