// RawPlan C-ABI contract tag constants (v1). See RAW_PLAN_CONTRACT.md.
// MUST stay in lockstep with raw_plan_tags.mojo (identical integer values).
#pragma once
#include <cstdint>

namespace mojo_gpu_rawplan {

inline constexpr int64_t MAGIC = 0x4750504c414e0001LL; // "GPPLAN" + v1

// TypeTag
enum : int64_t {
  TYPE_INVALID = 0,
  TYPE_BOOL = 1,
  TYPE_TINYINT = 2,
  TYPE_SMALLINT = 3,
  TYPE_INTEGER = 4,
  TYPE_BIGINT = 5,
  TYPE_HUGEINT = 6,
  TYPE_FLOAT = 7,
  TYPE_DOUBLE = 8,
  TYPE_DECIMAL = 9,
  TYPE_DATE = 10,
  TYPE_VARCHAR = 11,
  // Unsigned integer RESULT types (GPU_OP_STATS only -- DuckDB's regr_count
  // returns UINTEGER/UBIGINT). Additive to the TypeTag vocabulary: they appear
  // ONLY in an aggregate's OUT_TYPE / ret_type slot, never in a group key, filter
  // const, or any tape-section field-count, so the fixed header layout + the
  // hand-built shuttle tapes are unaffected. They round-trip through the existing
  // (tag, scale, width) OUT_TYPES triples with no layout change.
  TYPE_UTINYINT = 12,
  TYPE_USMALLINT = 13,
  TYPE_UINTEGER = 14,
  TYPE_UBIGINT = 15,
};

// CmpTag
enum : int64_t {
  CMP_EQ = 1,
  CMP_NE = 2,
  CMP_LT = 3,
  CMP_LE = 4,
  CMP_GT = 5,
  CMP_GE = 6,
};

// AggKind
enum : int64_t {
  AGG_SUM = 1, // sum / sum_no_overflow
  AGG_COUNT_STAR = 2,
  AGG_AVG = 3,
  AGG_MIN = 4,
  AGG_MAX = 5,
  // Statistical aggregates (GPU_OP_STATS). DOUBLE result derived CLOSED-FORM on
  // the host from the shared sums {n, Sx, Sx2, Sy, Sy2, Sxy} the f64 seg kernels
  // accumulate. 1-arg: stddev/var (x). 2-arg: covar/corr/regr_* with arg order
  // (y_dependent, x_independent) matching DuckDB. regr_count returns BIGINT.
  AGG_STDDEV_SAMP = 10,
  AGG_STDDEV_POP = 11,
  AGG_VAR_SAMP = 12,
  AGG_VAR_POP = 13,
  AGG_COVAR_SAMP = 14,
  AGG_COVAR_POP = 15,
  AGG_CORR = 16,
  AGG_REGR_SLOPE = 17,
  AGG_REGR_INTERCEPT = 18,
  AGG_REGR_R2 = 19,
  AGG_REGR_AVGX = 20,
  AGG_REGR_AVGY = 21,
  AGG_REGR_SXX = 22,
  AGG_REGR_SYY = 23,
  AGG_REGR_SXY = 24,
  AGG_REGR_COUNT = 25,
};

// JoinType
enum : int64_t {
  JOIN_INNER = 1,
};

// ExprOp (postfix program)
enum : int64_t {
  OP_LOAD_COL = 1,   // a=table_strid, b=col_strid
  OP_PUSH_CONST = 2, // a=const_id
  OP_ADD = 3,
  OP_SUB = 4,
  OP_MUL = 5,
  OP_SELECT = 6,     // CASE: pop else,then,pred
  OP_PROMO_PRED = 7, // a=table_strid, b=col_strid (p_type LIKE 'PROMO%')
  OP_LOAD_DIM = 8,   // a=dim-array index, b=fact-key column slot:
                     //   push dim_arrays[a][ cols[b][row] ] (FK gather)
  OP_EQ = 9,         // pop b, a -> push (a == b) ? 1 : 0
  // --- Transcendental unary ops (FLOAT eval path only; GPU_OP_TRANSCENDENTAL).
  // These appear ONLY in a metric program evaluated by the float64 VM
  // (eval_program_f64); the int64 VM (eval_program) treats them as no-ops and
  // never sees them (the builder emits them only for a DOUBLE-returning
  // transcendental aggregate routed to the float accumulator). Unary: pop a,
  // push f(a) where `a` is the reconstructed true double of the operand.
  OP_SQRT = 10,
  OP_EXP = 11,
  OP_LN = 12,
  OP_LOG10 = 13,
  OP_SIN = 14,
  OP_COS = 15,
  // Argument separator (GPU_OP_STATS). A 2-arg stat aggregate emits its
  // dependent-arg program, OP_ARGSEP, then its independent-arg program into the
  // single Agg.program tape; the Mojo side splits on it. Never reaches any
  // expr-VM (it is stripped during the per-stat metric lowering).
  OP_ARGSEP = 16,
};

// Descriptor kind (Stage-1 shadow validation introspection)
enum : int64_t {
  KIND_UNKNOWN = 0,
  KIND_Q6 = 1,
  KIND_Q1 = 2,
  KIND_Q14 = 3,
  KIND_Q3 = 4,
  KIND_Q5 = 5,
};

// ExecStrategy
enum : int64_t {
  STRAT_UNGROUPED = 0,
  STRAT_DENSE_GROUP = 1,
  STRAT_SORT_SEGREDUCE = 2,
  STRAT_HASH_GROUP = 3,
};

inline constexpr int64_t IDX_NONE = -1; // group_index when ungrouped

} // namespace mojo_gpu_rawplan
