"""Aggregate functions: one-line reductions and the low-level callback API.

    pixi run mojo run examples/aggregate_function.mojo
"""

from std.sys.info import size_of
from duckdb import *
from duckdb.aggregate_function import (
    AggregateFunction,
    AggregateFunctionInfo,
    AggregateState,
    AggregateStateArray,
)


# ---- Custom reduction: a SIMD combine function plus its identity ----

def add[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    return a + b


def zero() -> Scalar[DType.int64]:
    return 0


# ---- Low-level API: a sum over INTEGER that returns BIGINT ----

def state_size(info: AggregateFunctionInfo) -> idx_t:
    return idx_t(size_of[Int64]())


def state_init(info: AggregateFunctionInfo, state: AggregateState):
    state.get_data().unsafe_bitcast[Int64]().unsafe_write(0)


def update(info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray):
    var data = input.get_vector(0).get_data().unsafe_bitcast[Int32]()
    for i in range(len(input)):
        var s = states.get_state(i).get_data().unsafe_bitcast[Int64]()
        s[] += Int64(data[unsafe_offset=i])


def combine(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    target: AggregateStateArray,
    count: Int,
):
    for i in range(count):
        var s = source.get_state(i).get_data().unsafe_bitcast[Int64]()
        var t = target.get_state(i).get_data().unsafe_bitcast[Int64]()
        t[] += s[]


def finalize(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    mut result: Vector,
    count: Int,
    offset: Int,
):
    var out = result.get_data().unsafe_bitcast[Int64]()
    for i in range(count):
        out[unsafe_offset=offset + i] = source.get_state(i).get_data().unsafe_bitcast[Int64]()[]


def main() raises:
    var conn = DuckDB.connect(":memory:")
    _ = conn.execute("CREATE TABLE t AS SELECT i::INTEGER AS x FROM range(1000) r(i)")

    # Built-in reductions, one line each
    AggregateFunction.from_sum["mojo_sum", DType.int64](conn)
    AggregateFunction.from_max["mojo_max", DType.int64](conn)

    # Custom reduction, accumulating INTEGER input into BIGINT
    AggregateFunction.from_reduce["wide_sum", DType.int32, DType.int64, add, zero](conn)

    # Low-level API
    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[state_size, state_init, update, combine, finalize]()
    func.register(conn)

    conn.execute(
        "SELECT mojo_sum(x::BIGINT), mojo_max(x::BIGINT), wide_sum(x), my_sum(x) FROM t"
    ).show()
