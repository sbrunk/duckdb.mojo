# RawPlan C-ABI contract (v1)

The ABI-neutral serialization of a matched DuckDB `LogicalAggregate` subtree. C++
(`SerializeMatchedPlan`) walks the plan and emits it; Mojo (`build_descriptor`)
parses it. No DuckDB types cross the boundary, only two flat buffers.

## Wire form

Two buffers handed to Mojo:

- `tape: const int64_t*`, `tape_len: int64_t`: a flat array of int64 words.
- `blob: const uint8_t*`, `blob_len: int64_t`: raw bytes for interned strings.

Strings are interned: a string id indexes a string table on the tape, where each
entry is a `(blob_offset, byte_len)` pair. C++ may deduplicate identical strings.
Mojo builds a `String` from `blob[off : off+len]` (UTF-8).

The tape is a sequence of sections in a fixed order, each prefixed with its count.
All values are `int64`. The reader advances a cursor and the writer appends. Layout:

```
HEADER
  MAGIC          = 0x4750504c414e0001   ("GPPLAN" + v1)
  group_index                     // idx_t; -1 (= 0xFFFF...) if ungrouped
  aggregate_index                 // idx_t
STRING_TABLE
  n_strings
  repeat n_strings: blob_off, byte_len
OUT_TYPES                          // output schema, group cols then agg cols
  n_out
  repeat n_out: type_tag, scale, width
CONSTS                             // constant pool referenced by exprs & filters
  n_consts
  repeat n_consts: type_tag, scale, width, val_lo, val_hi, str_id
                                   //   numeric: (val_lo,val_hi) = int128 limbs at `scale`
                                   //   DATE:    val_lo = days, others 0
                                   //   VARCHAR: str_id set, val_* = 0
GETS
  n_gets
  repeat n_gets:
    table_strid
    est_cardinality
    n_filters
    repeat n_filters: col_strid, cmp_tag, const_id
JOINS
  n_joins
  repeat n_joins:
    join_type_tag
    n_conds
    repeat n_conds: lt_strid, lc_strid, rt_strid, rc_strid   // resolved (table,col) pairs
GROUP_KEYS
  n_gkeys
  repeat n_gkeys: table_strid, col_strid
AGGREGATES
  n_aggs
  repeat n_aggs:
    kind_tag
    ret_type_tag, ret_scale, ret_width, ret_is_int128
    prog_len
    repeat prog_len: op_tag, operand_a, operand_b
PASS_PROGRAMS                      // NR3 (GPU_OP_FILTER_OR); TRAILING additive section
  n_pass                           //   0 when no residual OR-filter (the default)
  repeat n_pass:
    get_ordinal                    //   indexes GETS in emit order (the GET this filter sits above)
    n_ops
    repeat n_ops: op_tag, operand_a, operand_b   // same postfix Op triples as AGGREGATES
```

A program is postfix (RPN). `operand_b` is 0 unless noted.

## Tag values (must match exactly on both sides)

```
# TypeTag (type_tag, ret_type_tag, const type_tag)
TYPE_INVALID   = 0
TYPE_BOOL      = 1
TYPE_TINYINT   = 2
TYPE_SMALLINT  = 3
TYPE_INTEGER   = 4
TYPE_BIGINT    = 5
TYPE_HUGEINT   = 6
TYPE_FLOAT     = 7
TYPE_DOUBLE    = 8
TYPE_DECIMAL   = 9     # scale/width meaningful
TYPE_DATE      = 10
TYPE_VARCHAR   = 11

# CmpTag (filter comparison)  -- matches DuckDB ExpressionType intent
CMP_EQ   = 1
CMP_NE   = 2
CMP_LT   = 3
CMP_LE   = 4
CMP_GT   = 5
CMP_GE   = 6

# AggKind
AGG_SUM        = 1     # sum / sum_no_overflow
AGG_COUNT_STAR = 2
AGG_AVG        = 3
AGG_MIN        = 4
AGG_MAX        = 5

# JoinType
JOIN_INNER = 1

# ExprOp (postfix program)
OP_LOAD_COL   = 1   # operand_a = table_strid, operand_b = col_strid    -> push column value
OP_PUSH_CONST = 2   # operand_a = const_id                              -> push constant
OP_ADD        = 3   # pop b,a -> push a+b
OP_SUB        = 4   # pop b,a -> push a-b
OP_MUL        = 5   # pop b,a -> push a*b
OP_SELECT     = 6   # pop else,then,pred -> push (pred ? then : else)   (CASE)
OP_PROMO_PRED = 7   # operand_a = table_strid, operand_b = col_strid (p_type);
                    #   the 'PROMO%' pattern is implicit (prefix match), not a constant
                    #   -> push bool (p_type LIKE 'PROMO%')
OP_LOAD_DIM   = 8   # operand_a = dim-array index, operand_b = fact-key col slot (FK gather)
OP_EQ         = 9   # pop b,a -> push (a==b) ? 1 : 0          (FILTER/PREDICATE 0/1)
# Transcendental unary/binary ops (FLOAT eval path only; GPU_OP_TRANSCENDENTAL).
OP_SQRT       = 10  # pop a -> push sqrt(a)
OP_EXP        = 11  # pop a -> push exp(a)
OP_LN         = 12  # pop a -> push ln(a)
OP_LOG10      = 13  # pop a -> push log10(a)
OP_SIN        = 14  # pop a -> push sin(a)
OP_COS        = 15  # pop a -> push cos(a)
OP_ARGSEP     = 16  # 2-arg stat aggregate argument separator (never reaches an expr-VM)
OP_POW        = 17  # pop exp,base -> push base**exp (integer-valued exponents only)
OP_LOG2       = 18  # pop a -> push log2(a)
# Range/inequality comparison ops (GPU_OP_FILTER_OR widening). FILTER/PREDICATE ops
# producing 0/1, evaluated by the int64 expr-VM exactly like OP_EQ. They lower an
# OR-of-RANGE / inequality residual LogicalFilter (a<10 OR a>95, BETWEEN, a!=5, ...)
# over INTEGER/DATE columns, chained with OP_ADD (OR) / OP_MUL (AND) like OR-of-eq.
OP_LT         = 19  # pop b,a -> push (a< b) ? 1 : 0          (FILTER/PREDICATE 0/1)
OP_LE         = 20  # pop b,a -> push (a<=b) ? 1 : 0          (FILTER/PREDICATE 0/1)
OP_GT         = 21  # pop b,a -> push (a> b) ? 1 : 0          (FILTER/PREDICATE 0/1)
OP_GE         = 22  # pop b,a -> push (a>=b) ? 1 : 0          (FILTER/PREDICATE 0/1)
OP_NE         = 23  # pop b,a -> push (a!=b) ? 1 : 0          (FILTER/PREDICATE 0/1)
```

### Notes / invariants
- `MAGIC` is the first word; on a mismatch Mojo aborts or returns null.
- `group_index == -1` (all bits set) means ungrouped (Q6/Q14 class). `n_gkeys==0`
  in that case.
- Filter constants and expr constants both live in the single `CONSTS` pool.
- Numeric constants are emitted at their column's decimal `scale` as int128 limbs
  (`val_lo` = low 64 bits, `val_hi` = high 64 bits, two's-complement). DATE uses
  `val_lo = date_t.days`. C++ does not rescale to a fixed scale. It emits the
  raw decimal integer plus `scale`, so Mojo keeps full precision.
- The revenue grammar `ext * (1 - disc)` serializes as:
  `LOAD_COL(ext); PUSH_CONST(1@scale); LOAD_COL(disc); SUB; MUL`.
- The Q6 product `ext * disc` serializes as `LOAD_COL(ext); LOAD_COL(disc); MUL`.
- The Q14 promo aggregate serializes as a CASE:
  `PROMO_PRED(p_type); <then-program: ext*(1-disc)>; PUSH_CONST(0); SELECT`.
- `OUT_TYPES` order is exactly the operator's output: all group columns first
  (in group-key order), then all aggregate columns (in aggregate order).
- `PASS_PROGRAMS` (NR3, behind `GPU_OP_FILTER_OR`) is an additional section at the
  end, after `AGGREGATES`. It does not change `MAGIC` or the layout of any earlier
  section. It carries a residual single-table `LogicalFilter` over INTEGER/DATE
  columns that is either an OR of equalities (including small sparse `IN` lists,
  which DuckDB lowers to an OR of equalities) or an OR of ranges / inequalities
  (e.g. `a<10 OR a>95`, `a BETWEEN .. OR a BETWEEN ..`, `a!=5 OR ...`). It is
  serialized as a postfix program:
  - A comparison `col <op> k` becomes `OP_LOAD_COL slot; OP_PUSH_CONST const_id;
    OP_{EQ,NE,LT,LE,GT,GE}`, which pushes 0 or 1. Ops 19 to 23 are the range and
    inequality ops added for `GPU_OP_FILTER_OR`. When the column is on the right
    side the comparison is flipped, e.g. `5 > a` becomes `a < 5`.
  - A `BETWEEN` arrives as a `CONJUNCTION_AND` of two ranges (AND is lowered to MUL
    recursively), and an OR of BETWEENs as an OR of those AND groups.
  - An OR of N branches is chained with `OP_ADD`. Every leaf is 0 or 1, so the sum
    is never negative and is `!=0` if and only if some leaf is 1.
  - An AND of M sub-results (a `CONJUNCTION_AND`, and the entries of
    `LogicalFilter.expressions[]`, which are implicitly ANDed) is chained with
    `OP_MUL`, which is `!=0` if and only if all are.

  A row passes if and only if `eval_program(...) != 0`. The Mojo side combines this
  with the host pass column via `OP_MUL`, so both the pushed range filters and the
  OR predicate are applied. `n_pass == 0` when there is no such filter (the
  default), and `get_ordinal` indexes `GETS` in emit order. Hand-built tapes (the
  round-trip and shuttle tests) must append the `n_pass=0` token so the reader does
  not read past the end.

## Mojo C-ABI surface (exported from engine, Stage 1 subset)

```
mojo_gpu_build_descriptor(tape: ptr<int64>, tape_len: int64,
                          blob: ptr<uint8>, blob_len: int64) -> int64   # handle (0 = unsupported/reject)
mojo_gpu_desc_free(handle: int64)
# Stage-1 introspection (used by the shadow-validation + round-trip test):
mojo_gpu_desc_kind(handle) -> int64        # which class: 1=Q6 2=Q1 3=Q14 4=Q3 5=Q5 0=generic/unknown
mojo_gpu_desc_strategy(handle) -> int64     # 0=UNGROUPED 1=DENSE_GROUP 2=SORT_SEGREDUCE 3=HASH_GROUP
mojo_gpu_desc_n_dims(handle) -> int64       # number of dimension join edges
mojo_gpu_desc_n_aggs(handle) -> int64
mojo_gpu_desc_fact_table(handle, out: ptr<uint8>, cap: int64) -> int64   # writes name, returns len
```

`build_descriptor` returns 0 if any field is outside the supported class (the
query is rejected and falls back to the CPU; shadow mode logs "unsupported").
Strategy selection: 0 group keys gives UNGROUPED; an integer fact group key gives
SORT_SEGREDUCE; a small dense key space gives DENSE_GROUP; anything else gives
HASH_GROUP (rewritten to SORT_SEGREDUCE on Apple later).

## Stage-1 usage (no behavior change)
Execution stays on the existing `MatchQ*`/`LogicalQ*`. When an existing matcher
fires, C++ also calls `SerializeMatchedPlan` + `mojo_gpu_build_descriptor` and
logs whether the descriptor's `kind`/`strategy`/`n_dims`/`n_aggs`/`fact_table`
agree with the matcher (shadow validation). The emitted plan does not change.

## Stage-2 execution shuttle (descriptor drives execution)

Where the Mojo code lives: from Stage 2 on, the descriptor logic
(`descriptor.mojo`, pure, no exports) is imported by `gpu_kernels.mojo`, which
holds all `@export` C-ABI wrappers in one compilation unit (the GPU dylib). That
way the same code can read the descriptor and run kernels. `descriptor.o` is no
longer linked separately.

C++ `PhysicalGpuAgg` (a source op, `IsSource()==true`, `ParallelSource()==false`)
holds the descriptor handle and drives:

```
# Output schema / bindings (C++ builds LogicalGpuAgg types + GetColumnBindings)
mojo_gpu_desc_group_index(h) -> int64           # IDX_NONE if ungrouped
mojo_gpu_desc_aggregate_index(h) -> int64
mojo_gpu_desc_out_arity(h) -> int64
mojo_gpu_desc_out_type(h, i, out_tag*, out_scale*, out_width*) -> int64   # 0 ok

# Materialization: Mojo decides what to SELECT (built from the descriptor)
mojo_gpu_desc_materialize_count(h) -> int64
mojo_gpu_desc_materialize_sql(h, i, out: ptr<uint8>, cap) -> int64        # writes SQL, returns full len

# Pin cache: WARM => resident buffers already cached, skip feeding
mojo_gpu_pin_begin(h) -> int64                  # 0 = WARM, 1 = COLD
# Feed raw materialized columns (COLD only). One call per (request, column).
# `type_tag` is the contract TypeTag; `ptr` is the flat column data.
mojo_gpu_feed_column(h, req_i, col_j, ptr: ptr<void>, n_rows, type_tag) -> int64   # 0 ok
# Build dim arrays / stage / upload (if COLD) + run kernel + int128 reduce.
mojo_gpu_pin_finalize(h) -> int64               # 0 ok, nonzero -> error/fallback

# Results (C++ dispatches by out_type per column)
mojo_gpu_result_rows(h) -> int64
mojo_gpu_result_i128(h, row, col, out_lo: ptr<int64>, out_hi: ptr<int64>) -> int64
mojo_gpu_result_i64(h, row, col) -> int64       # BIGINT/INTEGER/DATE
mojo_gpu_result_f64(h, row, col) -> float64     # DOUBLE
mojo_gpu_result_str(h, row, col, out: ptr<uint8>, cap) -> int64
```

Flow in `GetGlobalSourceState`: read out schema; `count = materialize_count`; if
`pin_begin == COLD`, for each request run `Connection::Query(materialize_sql(i))`
and `feed_column` each flat column; then `pin_finalize`. `GetDataInternal` walks
`result_rows` × out columns, filling the chunk via the typed result getters.

Materialization SQL is generic: Mojo selects the distinct fact columns referenced
by the fact filters + aggregate programs (and per-dim queries for dim tables),
ordering them deterministically and remembering that order for `feed_column`.
`ORDER BY <fact group key>` is appended only when `strategy == SORT_SEGREDUCE`.

Pin-cache signature (lives for the process, keyed in Mojo, see `_signature` in
`gpu_kernels.mojo`): fact table + sorted projected fact columns + strategy + kind +
dim tables and their carried columns + the filter constants + an aggregate program
fingerprint. Including the constants means a warm hit guarantees an identical
predicate, so a repeated query with different constants is cold. The exception is
the predicate-independent paths (`GPU_OP_NATIVE_DECODE` for Q6/Q1/Q5/Q14, and the
f64 paths): they evaluate the filter in the kernel from per-run bounds, so their
signature keeps only the filter columns and comparisons, not the constants, and
queries that differ only in constants stay warm.

The Stage-2 migration routes one query class at a time through `LogicalGpuAgg`
(behind an env flag, e.g. `GPU_OP_GENERIC=q6`); others keep their bespoke
`MatchQ*` path. `pin_finalize` may reuse the existing per-query kernels
(`mojo_q6_*` etc.) selected by descriptor shape during Stage 2; Stage 3 replaces
them with generic kernels.
```
