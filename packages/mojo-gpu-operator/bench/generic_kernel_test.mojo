"""Standalone bit-exact test for the generic GPU primitives (expr_vm + segreduce).

Synthesizes ~200k synthetic lineitem-like rows and exercises all three modes of
the generic segmented-reduction kernel, asserting bit-exact equality with a CPU
int128 reference (and, for the Q6 shape, against the original q6_kernel math).

Run:
  pixi run mojo run \
    -I packages/mojo-gpu-operator/src/primitives \
    -I packages/mojo-gpu-operator/src \
    packages/mojo-gpu-operator/bench/generic_kernel_test.mojo
"""

from std.gpu.host import DeviceContext
from std.memory import alloc
from std.sys import has_accelerator
from raw_plan_tags import (
    OP_LOAD_COL,
    OP_PUSH_CONST,
    OP_ADD,
    OP_SUB,
    OP_MUL,
    OP_SELECT,
    OP_LOAD_DIM,
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
)
from segreduce import run_segreduce

comptime N = 200_000

# Column slot assignments in the packed int64 buffer (cols[slot*N + row]).
comptime SLOT_SHIP = 0     # DATE -> int32 days, widened to int64
comptime SLOT_DISC = 1     # DECIMAL(15,2) -> int64 scale 2
comptime SLOT_EXT = 2      # DECIMAL(15,2) -> int64 scale 2
comptime SLOT_QTY = 3      # int64
comptime SLOT_TAX = 4      # DECIMAL(15,2) -> int64 scale 2
comptime SLOT_GID = 5      # dense group id (int64)
comptime SLOT_PASS = 6     # precomputed 0/1 pass column for Q6 filter
comptime N_COLS = 7


# Build a flattened (op,a,b) program from parallel op/a lists. b is always 0.
def build_prog(ops: List[Int64], aa: List[Int64]) -> List[Int64]:
    var p = List[Int64]()
    for i in range(len(ops)):
        p.append(ops[i])
        p.append(aa[i])
        p.append(Int64(0))
    return p^


def to_buf(src: List[Int64]) -> UnsafePointer[Scalar[DType.int64], MutAnyOrigin]:
    var n = len(src) if len(src) > 0 else 1
    var p = alloc[Int64](n)
    for i in range(len(src)):
        p[i] = src[i]
    return p


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()

    # ---- synthesize columns ----
    var ship = alloc[Int64](N)
    var disc = alloc[Int64](N)
    var ext = alloc[Int64](N)
    var qty = alloc[Int64](N)
    var tax = alloc[Int64](N)
    var gid = alloc[Int64](N)
    var passc = alloc[Int64](N)

    comptime N_GROUPS = 4
    # Q6 filter constants (same shape as q6_kernel_test).
    var ship_lo = Int64(8766)
    var ship_hi = Int64(9131)
    var disc_lo = Int64(5)
    var disc_hi = Int64(7)
    var qty_hi = Int64(24)

    for i in range(N):
        var sd = Int64(8000 + (i * 1103515245 + 12345) % 2000)
        var d = Int64((i * 48271) % 11)            # 0..10
        var e = Int64(100 + (i * 16807) % 9_999_900)
        var q = Int64(1 + (i * 22695477) % 50)
        var t = Int64((i * 30269) % 9)             # 0..8 tax scale 2
        ship[i] = sd
        disc[i] = d
        ext[i] = e
        qty[i] = q
        tax[i] = t
        gid[i] = Int64(i % N_GROUPS)
        # precomputed Q6 pass: shipdate/discount/quantity windows
        var ok = (
            sd >= ship_lo and sd < ship_hi
            and d >= disc_lo and d <= disc_hi and q < qty_hi
        )
        passc[i] = Int64(1) if ok else Int64(0)

    # ---- pack columns into one int64 buffer cols[slot*N + row] ----
    var cols = alloc[Int64](N_COLS * N)
    for i in range(N):
        cols[SLOT_SHIP * N + i] = ship[i]
        cols[SLOT_DISC * N + i] = disc[i]
        cols[SLOT_EXT * N + i] = ext[i]
        cols[SLOT_QTY * N + i] = qty[i]
        cols[SLOT_TAX * N + i] = tax[i]
        cols[SLOT_GID * N + i] = gid[i]
        cols[SLOT_PASS * N + i] = passc[i]

    var all_pass = to_buf(List[Int64]())  # placeholder, pass_len=0
    # No-dim placeholders for the FK-join gather params (n_dims=0 path).
    var no_dims = to_buf(List[Int64]())
    var no_doff = to_buf([Int64(0)])

    # =====================================================================
    # Shape 1: Q6 (UNGROUPED, M=1). metric = ext*disc; filter = precomputed
    # pass column (1-op OP_LOAD_COL pass_slot).
    # =====================================================================
    var q6_metric = build_prog(
        [OP_LOAD_COL, OP_LOAD_COL, OP_MUL],
        [Int64(SLOT_EXT), Int64(SLOT_DISC), Int64(0)],
    )
    var q6_pass = build_prog([OP_LOAD_COL], [Int64(SLOT_PASS)])
    var q6_moff = to_buf([Int64(0)])
    var q6_mlen = to_buf([Int64(3)])

    var q6_res = run_segreduce(
        ctx, STRAT_UNGROUPED, N,
        cols, N_COLS,
        to_buf(q6_pass), len(q6_pass) // 3,
        to_buf(q6_metric), len(q6_metric) // 3,
        q6_moff, q6_mlen, 1,
        0, 1,                      # gid_slot, G (unused)
        all_pass, 0,               # seg_off, n_seg (unused)
        no_dims, no_doff, 0,       # dims, dim_offsets, n_dims (no gather)
    )

    # CPU int128 reference (== q6_kernel math: ext*disc over passing rows)
    var q6_cpu = Int128(0)
    for i in range(N):
        if passc[i] != 0:
            q6_cpu += Int128(ext[i]) * Int128(disc[i])
    var q6_ok = q6_res[0] == q6_cpu
    print("[Q6 UNGROUPED M=1] gpu=", q6_res[0], " cpu=", q6_cpu, " match=", q6_ok)

    # =====================================================================
    # Shape 2: Q1 (DENSE_GROUP, M=3). metrics:
    #   m0 = count                      = PUSH 1
    #   m1 = sum(ext)                   = LOAD ext
    #   m2 = sum(ext*(100-disc)*(100+tax)) scale 6
    #        = LOAD ext; PUSH 100; LOAD disc; SUB; MUL; PUSH 100; LOAD tax; ADD; MUL
    # no filter (pass_len=0).
    # =====================================================================
    var m_count = build_prog([OP_PUSH_CONST], [Int64(1)])
    var m_ext = build_prog([OP_LOAD_COL], [Int64(SLOT_EXT)])
    var m_charge = build_prog(
        [OP_LOAD_COL, OP_PUSH_CONST, OP_LOAD_COL, OP_SUB, OP_MUL,
         OP_PUSH_CONST, OP_LOAD_COL, OP_ADD, OP_MUL],
        [Int64(SLOT_EXT), Int64(100), Int64(SLOT_DISC), Int64(0), Int64(0),
         Int64(100), Int64(SLOT_TAX), Int64(0), Int64(0)],
    )
    # concatenate the 3 metric programs
    var q1_progs = List[Int64]()
    for x in m_count: q1_progs.append(x)
    for x in m_ext: q1_progs.append(x)
    for x in m_charge: q1_progs.append(x)
    var len_count = len(m_count) // 3
    var len_ext = len(m_ext) // 3
    var len_charge = len(m_charge) // 3
    var q1_moff = to_buf([Int64(0), Int64(len_count), Int64(len_count + len_ext)])
    var q1_mlen = to_buf([Int64(len_count), Int64(len_ext), Int64(len_charge)])

    var q1_res = run_segreduce(
        ctx, STRAT_DENSE_GROUP, N,
        cols, N_COLS,
        all_pass, 0,
        to_buf(q1_progs), len(q1_progs) // 3,
        q1_moff, q1_mlen, 3,
        SLOT_GID, N_GROUPS,
        all_pass, 0,
        no_dims, no_doff, 0,       # no gather
    )

    # CPU int128 reference per (group, metric)
    var q1_ok = True
    for g in range(N_GROUPS):
        var c_cnt = Int128(0)
        var c_ext = Int128(0)
        var c_chg = Int128(0)
        for i in range(N):
            if Int(gid[i]) == g:
                c_cnt += 1
                c_ext += Int128(ext[i])
                c_chg += Int128(ext[i]) * (Int128(100) - Int128(disc[i])) * (Int128(100) + Int128(tax[i]))
        var g_cnt = q1_res[g * 3 + 0]
        var g_ext = q1_res[g * 3 + 1]
        var g_chg = q1_res[g * 3 + 2]
        var ok = (g_cnt == c_cnt) and (g_ext == c_ext) and (g_chg == c_chg)
        if not ok:
            q1_ok = False
        if g == 0:
            print("[Q1 DENSE g=0] cnt gpu=", g_cnt, " cpu=", c_cnt,
                  " | charge gpu=", g_chg, " cpu=", c_chg, " match=", ok)
    print("[Q1 DENSE_GROUP M=3] all groups match=", q1_ok)

    # =====================================================================
    # Shape 3: SORT_SEGREDUCE. Build contiguous segments by a sorted key:
    # segment s owns rows [s*seg_size, (s+1)*seg_size). metric = ext*(100-disc).
    # no filter.
    # =====================================================================
    comptime N_SEG = 500
    comptime SEG_SIZE = N // N_SEG       # 200000/500 = 400
    var seg_off = alloc[Int64](N_SEG + 1)
    for s in range(N_SEG + 1):
        seg_off[s] = Int64(s * SEG_SIZE)
    seg_off[N_SEG] = Int64(N)            # ensure last covers all rows

    var seg_metric = build_prog(
        [OP_LOAD_COL, OP_PUSH_CONST, OP_LOAD_COL, OP_SUB, OP_MUL],
        [Int64(SLOT_EXT), Int64(100), Int64(SLOT_DISC), Int64(0), Int64(0)],
    )
    var seg_moff = to_buf([Int64(0)])
    var seg_mlen = to_buf([Int64(len(seg_metric) // 3)])

    var seg_res = run_segreduce(
        ctx, STRAT_SORT_SEGREDUCE, N,
        cols, N_COLS,
        all_pass, 0,
        to_buf(seg_metric), len(seg_metric) // 3,
        seg_moff, seg_mlen, 1,
        0, 1,
        seg_off, N_SEG,
        no_dims, no_doff, 0,       # no gather
    )

    var seg_ok = True
    var first_bad = -1
    for s in range(N_SEG):
        var lo = Int(seg_off[s])
        var hi = Int(seg_off[s + 1])
        var c = Int128(0)
        for i in range(lo, hi):
            c += Int128(ext[i]) * (Int128(100) - Int128(disc[i]))
        if seg_res[s] != c:
            seg_ok = False
            if first_bad < 0:
                first_bad = s
    print("[SORT_SEGREDUCE M=1] seg0 gpu=", seg_res[0],
          " cpu(seg0 recomputed below) match_all=", seg_ok)
    # sample value for seg 0
    var c0 = Int128(0)
    for i in range(Int(seg_off[0]), Int(seg_off[1])):
        c0 += Int128(ext[i]) * (Int128(100) - Int128(disc[i]))
    print("[SORT_SEGREDUCE M=1] seg0 cpu=", c0)

    # =====================================================================
    # Shape 4: FK-JOIN dimension lookup via OP_LOAD_DIM (on-GPU gather).
    #
    # Models a TPC-H style FK join (e.g. Q14 lineitem->part): the fact has a FK
    # column `partkey` in [0, P); two dense dim arrays sized to P index it:
    #   dim 0 (promo_dim[pk])   = 0/1 promo flag per part
    #   dim 1 (revenue_dim[pk]) = a carried per-part numeric value
    #
    # Metric program (a promo-CASE):
    #   LOAD_DIM(revenue_dim, partkey)   -> carried value
    #   PUSH_CONST 0                     -> else value
    #   LOAD_DIM(promo_dim, partkey)     -> predicate
    #   SELECT                           -> (promo!=0 ? carried : 0)
    # combined with ext via MUL, so metric = ext * (promo ? revenue : 0).
    #
    # Row filter (a dim pass-filter): 1-op LOAD_DIM(promo_dim, partkey) ANDed via
    # the VM, i.e. only promo parts pass. We test BOTH a metric-side gather AND a
    # filter-side gather.
    # =====================================================================
    comptime P = 1000  # distinct partkeys (dense dim index space)
    var partkey = alloc[Int64](N)
    var promo_dim = alloc[Int64](P)
    var revenue_dim = alloc[Int64](P)
    for pk in range(P):
        # promo flag: ~1/3 of parts are promo
        promo_dim[pk] = Int64(1) if (pk % 3 == 0) else Int64(0)
        revenue_dim[pk] = Int64(7 + (pk * 2654435761) % 500)  # small carried val
    for i in range(N):
        partkey[i] = Int64((i * 2246822519) % P)

    # extended packed columns: reuse 7 slots + partkey in a new slot 7.
    comptime SLOT_PK = N_COLS          # 7
    comptime N_COLS2 = N_COLS + 1      # 8
    var cols2 = alloc[Int64](N_COLS2 * N)
    for i in range(N):
        cols2[SLOT_SHIP * N + i] = ship[i]
        cols2[SLOT_DISC * N + i] = disc[i]
        cols2[SLOT_EXT * N + i] = ext[i]
        cols2[SLOT_QTY * N + i] = qty[i]
        cols2[SLOT_TAX * N + i] = tax[i]
        cols2[SLOT_GID * N + i] = gid[i]
        cols2[SLOT_PASS * N + i] = passc[i]
        cols2[SLOT_PK * N + i] = partkey[i]

    # pack the two dim arrays back-to-back: dim 0 = promo, dim 1 = revenue.
    comptime DIM_PROMO = 0
    comptime DIM_REV = 1
    var dims_list = List[Int64]()
    for pk in range(P): dims_list.append(promo_dim[pk])
    for pk in range(P): dims_list.append(revenue_dim[pk])
    var dims_buf = to_buf(dims_list)
    var dim_off = to_buf([Int64(0), Int64(P), Int64(2 * P)])  # len n_dims+1=3

    # metric: ext * (promo ? revenue : 0)
    # SELECT pops else(top), then, pred -> push order must be pred, then, else:
    #   LOAD ext; LOAD_DIM(promo, pk)[pred]; LOAD_DIM(rev, pk)[then];
    #   PUSH 0[else]; SELECT; MUL
    var join_metric = build_prog(
        [OP_LOAD_COL, OP_LOAD_DIM, OP_LOAD_DIM, OP_PUSH_CONST, OP_SELECT, OP_MUL],
        [Int64(SLOT_EXT), Int64(DIM_PROMO), Int64(DIM_REV), Int64(0),
         Int64(0), Int64(0)],
    )
    # OP_LOAD_DIM operand b (the fact-key slot) goes in the program's 'b' field;
    # build_prog set b=0, so patch the two LOAD_DIM ops' b to SLOT_PK.
    # triples: op,a,b at indices [3k..3k+2]; LOAD_DIM are ops k=1 and k=2.
    join_metric[3 * 1 + 2] = Int64(SLOT_PK)
    join_metric[3 * 2 + 2] = Int64(SLOT_PK)
    var join_moff = to_buf([Int64(0)])
    var join_mlen = to_buf([Int64(len(join_metric) // 3)])

    # filter: only promo parts pass -> 1-op LOAD_DIM(promo, partkey)
    var join_pass = build_prog([OP_LOAD_DIM], [Int64(DIM_PROMO)])
    join_pass[3 * 0 + 2] = Int64(SLOT_PK)  # set b=fact-key slot

    # ---- 4a: UNGROUPED with metric gather + filter gather ----
    var join_ung = run_segreduce(
        ctx, STRAT_UNGROUPED, N,
        cols2, N_COLS2,
        to_buf(join_pass), len(join_pass) // 3,
        to_buf(join_metric), len(join_metric) // 3,
        join_moff, join_mlen, 1,
        0, 1,
        all_pass, 0,
        dims_buf, dim_off, 2,
    )

    # CPU int128 reference: same gather, bit-exact.
    var join_cpu_ung = Int128(0)
    for i in range(N):
        var pk = Int(partkey[i])
        if promo_dim[pk] != 0:                       # filter gather
            var carried = revenue_dim[pk] if promo_dim[pk] != 0 else Int64(0)
            join_cpu_ung += Int128(ext[i]) * Int128(carried)
    var join_ung_ok = join_ung[0] == join_cpu_ung
    print("[JOIN UNGROUPED] gpu=", join_ung[0], " cpu=", join_cpu_ung,
          " match=", join_ung_ok)

    # ---- 4b: SORT_SEGREDUCE with the same gather metric + filter ----
    comptime NJ_SEG = 500
    comptime JSEG_SIZE = N // NJ_SEG
    var jseg_off = alloc[Int64](NJ_SEG + 1)
    for s in range(NJ_SEG + 1):
        jseg_off[s] = Int64(s * JSEG_SIZE)
    jseg_off[NJ_SEG] = Int64(N)

    var join_seg = run_segreduce(
        ctx, STRAT_SORT_SEGREDUCE, N,
        cols2, N_COLS2,
        to_buf(join_pass), len(join_pass) // 3,
        to_buf(join_metric), len(join_metric) // 3,
        join_moff, join_mlen, 1,
        0, 1,
        jseg_off, NJ_SEG,
        dims_buf, dim_off, 2,
    )

    var join_seg_ok = True
    for s in range(NJ_SEG):
        var lo = Int(jseg_off[s])
        var hi = Int(jseg_off[s + 1])
        var c = Int128(0)
        for i in range(lo, hi):
            var pk = Int(partkey[i])
            if promo_dim[pk] != 0:
                var carried = revenue_dim[pk] if promo_dim[pk] != 0 else Int64(0)
                c += Int128(ext[i]) * Int128(carried)
        if join_seg[s] != c:
            join_seg_ok = False
    # sample value for seg 0
    var jc0 = Int128(0)
    for i in range(Int(jseg_off[0]), Int(jseg_off[1])):
        var pk = Int(partkey[i])
        if promo_dim[pk] != 0:
            jc0 += Int128(ext[i]) * Int128(revenue_dim[pk])
    print("[JOIN SORT_SEGREDUCE] seg0 gpu=", join_seg[0], " cpu=", jc0,
          " match_all=", join_seg_ok)

    print("")
    if q6_ok and q1_ok and seg_ok and join_ung_ok and join_seg_ok:
        print("ALL PASS")
    else:
        print("FAIL  q6=", q6_ok, " q1=", q1_ok, " seg=", seg_ok,
              " join_ung=", join_ung_ok, " join_seg=", join_seg_ok)

    ship.free(); disc.free(); ext.free(); qty.free(); tax.free()
    gid.free(); passc.free(); cols.free()
    partkey.free(); promo_dim.free(); revenue_dim.free(); cols2.free()
    jseg_off.free()
