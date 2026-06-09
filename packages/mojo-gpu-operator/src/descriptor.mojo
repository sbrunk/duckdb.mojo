"""RawPlan -> descriptor IR + matcher brain (Stage 1+, PURE, no GPU, no exports).

Parses the flat int64 "tape" + string "blob" emitted by the C++
`SerializeMatchedPlan` (see RAW_PLAN_CONTRACT.md) into a `GpuPlanDescriptor` and
runs the fail-closed matcher (fact/dim resolution, strategy + kind
classification).

This module is PURE: structs, `RawPlanReader`, `parse_raw_plan`,
`build_descriptor_impl`, `classify`, and helpers -- all importable, no GPU, no
`@export` wrappers. The C-ABI `@export` wrappers that surface this logic live in
`gpu_kernels.mojo` (the root build file) so they share one compilation unit with
the GPU kernels and are retained in the dylib (a bare `import` strips exports).

Exercised directly by `bench/raw_plan_roundtrip_test.mojo` and the Stage-2
shuttle test `bench/q6_shuttle_test.mojo`.
"""

from raw_plan_tags import (
    RP_MAGIC,
    TYPE_INTEGER,
    TYPE_BIGINT,
    TYPE_VARCHAR,
    AGG_SUM,
    AGG_COUNT_STAR,
    AGG_AVG,
    AGG_MIN,
    AGG_MAX,
    OP_LOAD_COL,
    OP_PROMO_PRED,
    JOIN_INNER,
    KIND_UNKNOWN,
    KIND_Q6,
    KIND_Q1,
    KIND_Q14,
    KIND_Q3,
    KIND_Q5,
    STRAT_UNGROUPED,
    STRAT_DENSE_GROUP,
    STRAT_SORT_SEGREDUCE,
    STRAT_HASH_GROUP,
    IDX_NONE,
)


# ---------------------------------------------------------------------------
# Tape reader: a cursor over the flat int64 tape + the raw string blob.
# ---------------------------------------------------------------------------
struct RawPlanReader(Copyable, Movable):
    var tape: UnsafePointer[Int64, MutAnyOrigin]
    var tape_len: Int
    var blob: UnsafePointer[UInt8, MutAnyOrigin]
    var blob_len: Int
    var cursor: Int
    # STRING_TABLE entries, filled in by parse_raw_plan once the section is read.
    var str_off: List[Int]
    var str_len: List[Int]

    def __init__(
        out self,
        tape: UnsafePointer[Int64, MutAnyOrigin],
        tape_len: Int,
        blob: UnsafePointer[UInt8, MutAnyOrigin],
        blob_len: Int,
    ):
        self.tape = tape
        self.tape_len = tape_len
        self.blob = blob
        self.blob_len = blob_len
        self.cursor = 0
        self.str_off = []
        self.str_len = []

    def next(mut self) raises -> Int64:
        if self.cursor >= self.tape_len:
            raise Error("RawPlanReader: tape overrun")
        var v = self.tape[self.cursor]
        self.cursor += 1
        return v

    # Materialize the interned string `strid` from the blob as UTF-8.
    def string_at(self, strid: Int) raises -> String:
        if strid < 0 or strid >= len(self.str_off):
            raise Error("RawPlanReader: bad string id")
        var off = self.str_off[strid]
        var n = self.str_len[strid]
        if off < 0 or n < 0 or off + n > self.blob_len:
            raise Error("RawPlanReader: string out of blob bounds")
        var s = String("")
        for i in range(n):
            # Append one byte; blob is UTF-8, ASCII table/column names in practice.
            s += chr(Int(self.blob[off + i]))
        return s


# ---------------------------------------------------------------------------
# Descriptor IR structs.
# ---------------------------------------------------------------------------
@fieldwise_init
struct GpuColRef(Copyable, Movable):
    var table: String
    var column: String


@fieldwise_init
struct GpuConst(Copyable, Movable):
    var type_tag: Int64
    var scale: Int64
    var width: Int64
    var lo: Int64
    var hi: Int64
    var str_val: String


@fieldwise_init
struct GpuPredicate(Copyable, Movable):
    var col: GpuColRef
    var cmp: Int64
    var const_id: Int


@fieldwise_init
struct GpuExprOp(Copyable, Movable):
    """One postfix (RPN) program op."""

    var op: Int64
    var a: Int64
    var b: Int64


@fieldwise_init
struct GpuAggregate(Copyable, Movable):
    var kind: Int64
    var ret_type_tag: Int64
    var ret_scale: Int64
    var ret_width: Int64
    var ret_is_int128: Bool
    var program: List[GpuExprOp]
    # Resolved (table,col) for every LOAD_COL op in `program`, in program order.
    # Filled at parse time because the program ops keep raw string ids and the
    # reader's string table is gone afterwards.
    var load_cols: List[GpuColRef]
    # Resolved (table,col) for every OP_PROMO_PRED op in `program`, in program
    # order. Same rationale as `load_cols`: the raw string ids (op.a/op.b) would
    # otherwise be unrecoverable after the reader's string table is dropped. For
    # Q14 this is the dim-side `part.p_type` whose LIKE 'PROMO%' becomes a dense
    # promo-flag dim array gathered by OP_LOAD_DIM at lowering time.
    var promo_cols: List[GpuColRef]


@fieldwise_init
struct GpuJoinEdge(Copyable, Movable):
    """A resolved fact->dim equi-join edge.

    `fact_key` is the fact (or already-included dim) side column; `dim_key` is
    this dim's side. The carried payload (e.g. dim filter columns / build-side
    layout) is not needed for Stage 1 -- left for Stage 2.
    """

    var dim_table: String
    var fact_key: GpuColRef
    var dim_key: GpuColRef


# An out-type triple: (type_tag, scale, width).
comptime GpuOutType = Tuple[Int64, Int64, Int64]


# A GET: table name, est_cardinality, the table's filters.
@fieldwise_init
struct GpuGet(Copyable, Movable):
    var table: String
    var est_cardinality: Int64
    var filters: List[GpuPredicate]


# A join cond: resolved (table,col) pairs for the left and right side.
@fieldwise_init
struct GpuJoinCond(Copyable, Movable):
    var lt: String
    var lc: String
    var rt: String
    var rc: String


# A JOIN section entry: a join type + its conds.
@fieldwise_init
struct GpuJoin(Copyable, Movable):
    var join_type: Int64
    var conds: List[GpuJoinCond]


@fieldwise_init
struct GpuPlanDescriptor(Copyable, Movable):
    # --- straight from the tape ---
    var group_index: Int64
    var aggregate_index: Int64
    var out_types: List[GpuOutType]
    var consts: List[GpuConst]
    var gets: List[GpuGet]
    var joins: List[GpuJoin]
    var group_keys: List[GpuColRef]
    var aggregates: List[GpuAggregate]
    # --- derived by build_descriptor_impl ---
    var fact_table: String
    var dim_edges: List[GpuJoinEdge]
    var strategy: Int64
    var kind: Int64


# ---------------------------------------------------------------------------
# Straight decode -- no decisions. Verifies MAGIC first.
# ---------------------------------------------------------------------------
def parse_raw_plan(mut r: RawPlanReader) raises -> GpuPlanDescriptor:
    # HEADER
    var magic = r.next()
    if magic != RP_MAGIC:
        raise Error("parse_raw_plan: MAGIC mismatch")
    var group_index = r.next()
    var aggregate_index = r.next()

    # STRING_TABLE
    var n_strings = Int(r.next())
    for _ in range(n_strings):
        var off = Int(r.next())
        var blen = Int(r.next())
        r.str_off.append(off)
        r.str_len.append(blen)

    # OUT_TYPES
    var out_types: List[GpuOutType] = []
    var n_out = Int(r.next())
    for _ in range(n_out):
        var tt = r.next()
        var sc = r.next()
        var w = r.next()
        out_types.append((tt, sc, w))

    # CONSTS
    var consts: List[GpuConst] = []
    var n_consts = Int(r.next())
    for _ in range(n_consts):
        var tt = r.next()
        var sc = r.next()
        var w = r.next()
        var lo = r.next()
        var hi = r.next()
        var sid = Int(r.next())
        var sval = String("")
        if tt == TYPE_VARCHAR:
            sval = r.string_at(sid)
        consts.append(GpuConst(tt, sc, w, lo, hi, sval))

    # GETS
    var gets: List[GpuGet] = []
    var n_gets = Int(r.next())
    for _ in range(n_gets):
        var table_sid = Int(r.next())
        var est = r.next()
        var n_filters = Int(r.next())
        var filters: List[GpuPredicate] = []
        for _ in range(n_filters):
            var col_sid = Int(r.next())
            var cmp = r.next()
            var cid = Int(r.next())
            var col = GpuColRef(r.string_at(table_sid), r.string_at(col_sid))
            filters.append(GpuPredicate(col^, cmp, cid))
        gets.append(GpuGet(r.string_at(table_sid), est, filters^))

    # JOINS
    var joins: List[GpuJoin] = []
    var n_joins = Int(r.next())
    for _ in range(n_joins):
        var jtype = r.next()
        var n_conds = Int(r.next())
        var conds: List[GpuJoinCond] = []
        for _ in range(n_conds):
            var lt = r.string_at(Int(r.next()))
            var lc = r.string_at(Int(r.next()))
            var rt = r.string_at(Int(r.next()))
            var rc = r.string_at(Int(r.next()))
            conds.append(GpuJoinCond(lt, lc, rt, rc))
        joins.append(GpuJoin(jtype, conds^))

    # GROUP_KEYS
    var group_keys: List[GpuColRef] = []
    var n_gkeys = Int(r.next())
    for _ in range(n_gkeys):
        var table_sid = Int(r.next())
        var col_sid = Int(r.next())
        group_keys.append(GpuColRef(r.string_at(table_sid), r.string_at(col_sid)))

    # AGGREGATES
    var aggregates: List[GpuAggregate] = []
    var n_aggs = Int(r.next())
    for _ in range(n_aggs):
        var kind = r.next()
        var ret_tt = r.next()
        var ret_sc = r.next()
        var ret_w = r.next()
        var ret_i128 = r.next() != 0
        var prog_len = Int(r.next())
        var program: List[GpuExprOp] = []
        var load_cols: List[GpuColRef] = []
        var promo_cols: List[GpuColRef] = []
        for _ in range(prog_len):
            var op = r.next()
            var a = r.next()
            var b = r.next()
            if op == OP_LOAD_COL:
                # a = table_strid, b = col_strid -> resolve now.
                load_cols.append(
                    GpuColRef(r.string_at(Int(a)), r.string_at(Int(b)))
                )
            elif op == OP_PROMO_PRED:
                # a = table_strid, b = col_strid (the dim-side p_type) -> resolve.
                promo_cols.append(
                    GpuColRef(r.string_at(Int(a)), r.string_at(Int(b)))
                )
            program.append(GpuExprOp(op, a, b))
        aggregates.append(
            GpuAggregate(
                kind,
                ret_tt,
                ret_sc,
                ret_w,
                ret_i128,
                program^,
                load_cols^,
                promo_cols^,
            )
        )

    return GpuPlanDescriptor(
        group_index,
        aggregate_index,
        out_types^,
        consts^,
        gets^,
        joins^,
        group_keys^,
        aggregates^,
        String(""),  # fact_table (derived)
        [],  # dim_edges (derived)
        STRAT_UNGROUPED,  # strategy (derived)
        KIND_UNKNOWN,  # kind (derived)
    )


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------
def _agg_kind_supported(k: Int64) -> Bool:
    return (
        k == AGG_SUM
        or k == AGG_AVG
        or k == AGG_COUNT_STAR
        or k == AGG_MIN
        or k == AGG_MAX
    )


# True if (table,col) is the named table's column on either side of the cond,
# returning via `out_other` the (table,col) on the opposite side.
def _cond_touches(
    cond: GpuJoinCond, table: String
) -> Bool:
    return cond.lt == table or cond.rt == table


# ---------------------------------------------------------------------------
# The matcher brain. Fail-closed: returns None on anything unsupported.
# ---------------------------------------------------------------------------
def build_descriptor_impl(
    mut r: RawPlanReader,
) raises -> Optional[GpuPlanDescriptor]:
    var desc: GpuPlanDescriptor
    try:
        desc = parse_raw_plan(r)
    except:
        return None

    var n_gets = len(desc.gets)
    if n_gets == 0:
        return None

    # --- Fact = the GET with the largest est_cardinality ---
    var fact_idx = 0
    for i in range(1, n_gets):
        if desc.gets[i].est_cardinality > desc.gets[fact_idx].est_cardinality:
            fact_idx = i
    var fact_table = desc.gets[fact_idx].table

    # All INNER join conds flattened (we only support INNER for Stage 1).
    var conds: List[GpuJoinCond] = []
    for j in range(len(desc.joins)):
        ref jn = desc.joins[j]
        if jn.join_type != JOIN_INNER:
            return None
        for c in range(len(jn.conds)):
            conds.append(jn.conds[c].copy())

    # --- Fact vs dims: iterative attach to fixpoint ---
    # `included` = set of table names reachable (fact + attached dims).
    var included: List[String] = [fact_table]
    var dim_edges: List[GpuJoinEdge] = []
    # `attached[g]` tracks which GETs (by table) are already in `included`.
    var n_included_changed = True
    while n_included_changed:
        n_included_changed = False
        for gi in range(n_gets):
            if gi == fact_idx:
                continue
            var dim_table = desc.gets[gi].table
            # Already attached?
            var already = False
            for k in range(len(included)):
                if included[k] == dim_table:
                    already = True
                    break
            if already:
                continue
            # Find an equi-cond connecting this dim to an already-included table.
            for ci in range(len(conds)):
                ref cond = conds[ci]
                var near = String("")  # included side table
                var near_col = String("")
                var dim_col = String("")
                var matched = False
                if cond.rt == dim_table:
                    # left side must be an already-included table
                    for k in range(len(included)):
                        if included[k] == cond.lt:
                            near = cond.lt
                            near_col = cond.lc
                            dim_col = cond.rc
                            matched = True
                            break
                elif cond.lt == dim_table:
                    for k in range(len(included)):
                        if included[k] == cond.rt:
                            near = cond.rt
                            near_col = cond.rc
                            dim_col = cond.lc
                            matched = True
                            break
                if matched:
                    dim_edges.append(
                        GpuJoinEdge(
                            dim_table,
                            GpuColRef(near, near_col),
                            GpuColRef(dim_table, dim_col),
                        )
                    )
                    included.append(dim_table)
                    n_included_changed = True
                    break
    # Every non-fact GET must be attached.
    if len(included) != n_gets:
        return None

    # --- Aggregates: require >=1, all supported kinds ---
    if len(desc.aggregates) == 0:
        return None
    for ai in range(len(desc.aggregates)):
        if not _agg_kind_supported(desc.aggregates[ai].kind):
            return None

    # --- Strategy ---
    var n_gkeys = len(desc.group_keys)
    var strategy: Int64
    if n_gkeys == 0:
        strategy = STRAT_UNGROUPED
    else:
        # Integer fact group key -> sort + segmented reduce.
        var has_int_fact_key = False
        for gk in range(n_gkeys):
            ref key = desc.group_keys[gk]
            if key.table != fact_table:
                continue
            # Look up the key column's type via OUT_TYPES is not reliable here
            # (OUT_TYPES is positional schema, no col names). The contract states
            # the integer fact group key is BIGINT/INTEGER; we classify by the
            # group-key column being on the fact table AND an aggregate program
            # not depending on it. Use the out-type of the group column slot:
            # group columns come first in OUT_TYPES, in group-key order.
            if gk < len(desc.out_types):
                var tt = desc.out_types[gk][0]
                if tt == TYPE_BIGINT or tt == TYPE_INTEGER:
                    has_int_fact_key = True
        if has_int_fact_key:
            strategy = STRAT_SORT_SEGREDUCE
        else:
            # Small group-key count / VARCHAR dimension keys -> dense group.
            # (Q1: 2 VARCHAR fact keys; Q5: 1 VARCHAR dim key.)
            var all_small = n_gkeys <= 4
            if all_small:
                strategy = STRAT_DENSE_GROUP
            else:
                strategy = STRAT_HASH_GROUP

    # --- kind classification (diagnostic only) ---
    var n_dims = len(dim_edges)
    var n_aggs = len(desc.aggregates)
    var kind: Int64 = KIND_UNKNOWN
    if n_dims == 0 and n_gkeys == 0 and n_aggs == 1:
        kind = KIND_Q6
    elif n_dims == 0 and n_gkeys == 2 and n_aggs == 8:
        kind = KIND_Q1
    elif n_dims == 1 and n_gkeys == 0 and n_aggs == 2:
        kind = KIND_Q14
    elif n_dims == 2 and n_gkeys == 3 and n_aggs == 1:
        kind = KIND_Q3
    elif n_dims == 5 and n_gkeys == 1 and n_aggs == 1:
        kind = KIND_Q5

    desc.fact_table = fact_table
    desc.dim_edges = dim_edges^
    desc.strategy = strategy
    desc.kind = kind
    return desc^


# ---------------------------------------------------------------------------
# Materialization helper: the DISTINCT fact-table columns referenced by the
# fact filters + aggregate programs (LOAD_COL ops whose table == fact), in a
# deterministic order (first-seen). The Stage-2 shuttle SELECTs these columns
# and feeds them back in this exact order. Pure -- no GPU.
# ---------------------------------------------------------------------------
def fact_projected_columns(desc: GpuPlanDescriptor) -> List[String]:
    var cols: List[String] = []

    def _add(mut cols: List[String], name: String):
        for i in range(len(cols)):
            if cols[i] == name:
                return
        cols.append(name)

    # Fact filters first (in filter order), then aggregate-program LOAD_COLs.
    for gi in range(len(desc.gets)):
        ref g = desc.gets[gi]
        if g.table != desc.fact_table:
            continue
        for fi in range(len(g.filters)):
            ref p = g.filters[fi]
            if p.col.table == desc.fact_table:
                _add(cols, p.col.column)
    for ai in range(len(desc.aggregates)):
        ref agg = desc.aggregates[ai]
        for li in range(len(agg.load_cols)):
            ref c = agg.load_cols[li]
            if c.table == desc.fact_table:
                _add(cols, c.column)
    return cols^
