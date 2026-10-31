"""Aggregate functions: one-line reductions and custom state structs.

    pixi run mojo run examples/aggregate_function.mojo
"""

import duckdb
from duckdb import AggregateFunction, AggregateState


# A custom reduction: a SIMD combine function plus its identity.
def add[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    return a + b


def zero() -> Scalar[DType.int64]:
    return 0


# A state struct: one per group. `update` takes the arguments of one row (here
# one DOUBLE), `combine` merges states computed in parallel, `finalize` gives
# the result. NULL rows are skipped; a group without rows gives NULL.
struct Avg(AggregateState):
    comptime Result = Float64
    var sum: Float64
    var n: Int

    def __init__(out self):
        self.sum = 0
        self.n = 0

    def update(mut self, x: Float64):
        self.sum += x
        self.n += 1

    def combine(mut self, other: Self):
        self.sum += other.sum
        self.n += other.n

    def finalize(self) -> Float64:
        return self.sum / Float64(self.n)


def main() raises:
    var con = duckdb.connect()
    _ = con.execute("CREATE TABLE t AS SELECT i::BIGINT AS x, i % 3 AS g FROM range(1000) r(i)")

    # Built-in reductions, one line each
    con.register(AggregateFunction.from_sum["mojo_sum", DType.int64]())
    con.register(AggregateFunction.from_max["mojo_max", DType.int64]())

    # Custom reduction
    con.register(AggregateFunction.from_reduce["my_sum", DType.int64, add, zero]())

    # Custom state; the signature is inferred from `Avg.update`
    con.register(AggregateFunction.from_state["my_avg", Avg.update]())

    con.execute(
        "SELECT g, mojo_sum(x), mojo_max(x), my_sum(x), my_avg(x::DOUBLE) FROM t GROUP BY g ORDER BY g"
    ).show()
