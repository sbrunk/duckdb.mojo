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
