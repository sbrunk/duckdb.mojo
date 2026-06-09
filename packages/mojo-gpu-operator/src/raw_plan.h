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
