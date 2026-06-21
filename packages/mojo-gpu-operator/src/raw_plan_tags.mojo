"""RawPlan C-ABI contract tag constants (v1). See RAW_PLAN_CONTRACT.md.

MUST stay in lockstep with raw_plan.h (identical integer values).
"""

comptime RP_MAGIC: Int64 = 0x4750504C414E0001  # "GPPLAN" + v1

# TypeTag
comptime TYPE_INVALID: Int64 = 0
comptime TYPE_BOOL: Int64 = 1
comptime TYPE_TINYINT: Int64 = 2
comptime TYPE_SMALLINT: Int64 = 3
comptime TYPE_INTEGER: Int64 = 4
comptime TYPE_BIGINT: Int64 = 5
comptime TYPE_HUGEINT: Int64 = 6
comptime TYPE_FLOAT: Int64 = 7
comptime TYPE_DOUBLE: Int64 = 8
comptime TYPE_DECIMAL: Int64 = 9
comptime TYPE_DATE: Int64 = 10
comptime TYPE_VARCHAR: Int64 = 11
# Unsigned integer RESULT types (GPU_OP_STATS; see raw_plan.h). Additive vocabulary
# -- only ever an aggregate OUT_TYPE / ret_type tag (regr_count -> UINTEGER/UBIGINT),
# never a tape-section field-count, so the fixed header + shuttle tapes are unaffected.
comptime TYPE_UTINYINT: Int64 = 12
comptime TYPE_USMALLINT: Int64 = 13
comptime TYPE_UINTEGER: Int64 = 14
comptime TYPE_UBIGINT: Int64 = 15

# CmpTag
comptime CMP_EQ: Int64 = 1
comptime CMP_NE: Int64 = 2
comptime CMP_LT: Int64 = 3
comptime CMP_LE: Int64 = 4
comptime CMP_GT: Int64 = 5
comptime CMP_GE: Int64 = 6

# AggKind
comptime AGG_SUM: Int64 = 1
comptime AGG_COUNT_STAR: Int64 = 2
comptime AGG_AVG: Int64 = 3
comptime AGG_MIN: Int64 = 4
comptime AGG_MAX: Int64 = 5
# Statistical aggregates (GPU_OP_STATS). DOUBLE result, derived CLOSED-FORM on the
# host from the shared sums {n, Sx, Sx2, Sy, Sy2, Sxy} the f64 seg kernels already
# accumulate. 1-arg: stddev/var (over x). 2-arg: covar/corr/regr_* (regr arg order
# is (y_dependent, x_independent), matching DuckDB). regr_count returns BIGINT.
comptime AGG_STDDEV_SAMP: Int64 = 10
comptime AGG_STDDEV_POP: Int64 = 11
comptime AGG_VAR_SAMP: Int64 = 12
comptime AGG_VAR_POP: Int64 = 13
comptime AGG_COVAR_SAMP: Int64 = 14
comptime AGG_COVAR_POP: Int64 = 15
comptime AGG_CORR: Int64 = 16
comptime AGG_REGR_SLOPE: Int64 = 17
comptime AGG_REGR_INTERCEPT: Int64 = 18
comptime AGG_REGR_R2: Int64 = 19
comptime AGG_REGR_AVGX: Int64 = 20
comptime AGG_REGR_AVGY: Int64 = 21
comptime AGG_REGR_SXX: Int64 = 22
comptime AGG_REGR_SYY: Int64 = 23
comptime AGG_REGR_SXY: Int64 = 24
comptime AGG_REGR_COUNT: Int64 = 25

# JoinType
comptime JOIN_INNER: Int64 = 1

# ExprOp (postfix program)
comptime OP_LOAD_COL: Int64 = 1
comptime OP_PUSH_CONST: Int64 = 2
comptime OP_ADD: Int64 = 3
comptime OP_SUB: Int64 = 4
comptime OP_MUL: Int64 = 5
comptime OP_SELECT: Int64 = 6
comptime OP_PROMO_PRED: Int64 = 7
comptime OP_LOAD_DIM: Int64 = 8  # a=dim-array index, b=fact-key column slot
comptime OP_EQ: Int64 = 9  # pop b, a -> push (a == b) ? 1 : 0
# Transcendental unary ops (FLOAT eval path only; GPU_OP_TRANSCENDENTAL). Handled
# by eval_program_f64, never by the int64 eval_program. pop a -> push f(a) where
# `a` is the reconstructed true double of the operand. See raw_plan.h lockstep.
comptime OP_SQRT: Int64 = 10
comptime OP_EXP: Int64 = 11
comptime OP_LN: Int64 = 12
comptime OP_LOG10: Int64 = 13
comptime OP_SIN: Int64 = 14
comptime OP_COS: Int64 = 15
# Argument separator (GPU_OP_STATS). A 2-arg stat aggregate (covar/corr/regr_*)
# emits its dependent-arg program, then OP_ARGSEP, then its independent-arg
# program, into the single Agg.program field. Split out on the Mojo side; never
# reaches any expr-VM (it lives only in the un-lowered Agg.program tape).
comptime OP_ARGSEP: Int64 = 16
# Power (FLOAT eval path only; GPU_OP_TRANSCENDENTAL). BINARY: pop exp, pop base ->
# push base**exp. Only integer-valued constant exponents are emitted (the f64 const
# tape rounds doubles to int — fractional exponents fail-closed on the C++ side);
# eval_program_f64 computes it via exact binary exponentiation (pure multiplies).
comptime OP_POW: Int64 = 17
# log2 (FLOAT eval path only; GPU_OP_TRANSCENDENTAL). Unary: pop a -> push log2(a),
# derived from the f64 natural log (NVIDIA has no in-kernel f64 log2). See raw_plan.h.
comptime OP_LOG2: Int64 = 18

# Descriptor kind
comptime KIND_UNKNOWN: Int64 = 0
comptime KIND_Q6: Int64 = 1
comptime KIND_Q1: Int64 = 2
comptime KIND_Q14: Int64 = 3
comptime KIND_Q3: Int64 = 4
comptime KIND_Q5: Int64 = 5

# ExecStrategy
comptime STRAT_UNGROUPED: Int64 = 0
comptime STRAT_DENSE_GROUP: Int64 = 1
comptime STRAT_SORT_SEGREDUCE: Int64 = 2
comptime STRAT_HASH_GROUP: Int64 = 3

comptime IDX_NONE: Int64 = -1
