"""Benchmark: scalar UDFs on the V2 API against the V1 API and DuckDB's built-ins.

Each query aggregates the function over 10M rows, so the time is mostly the
function. Run:

    pixi run mojo build benchmark/v2_udf_bench.mojo -o /tmp/v2_udf_bench && /tmp/v2_udf_bench
"""

from std.time import perf_counter_ns
from duckdb import DuckDB
from duckdb.connection import Connection as DuckDBConnection
from duckdb.kernels import register_simd_math as register_simd_math_v1
from duckdb.scalar_function import ScalarFunction as ScalarFunctionV1
from duckdb.aggregate_function import AggregateFunction as AggregateFunctionV1
from duckdb.v2 import AggregateFunction, AggregateState, Connection, Database, ScalarFunction, register_simd_math

comptime N = "10000000"


def add(a: Int64, b: Int64) -> Int64:
    return a + b


def greet(name: String) -> Int64:
    return Int64(name.byte_length())


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


def _ms(ns: Int) -> String:
    return String(Float64(ns) / 1e6, " ms")


def _time_v2(con: Connection, sql: String) raises -> Int:
    _ = con.execute(sql).fetchall()  # warm up
    var t0 = perf_counter_ns()
    _ = con.execute(sql).fetchall()
    return Int(perf_counter_ns() - t0)


def _time_v1(con: DuckDBConnection, sql: String) raises -> Int:
    _ = con.execute(sql)
    var t0 = perf_counter_ns()
    _ = con.execute(sql)
    return Int(perf_counter_ns() - t0)


def main() raises:
    var v2 = Database().connect()
    _ = v2.execute("SET threads = 1")
    register_simd_math(v2)
    v2.register(ScalarFunction.from_function["mojo_add", add]())
    v2.register(ScalarFunction.from_function["mojo_len", greet]())

    var v1 = DuckDB.connect(":memory:")
    _ = v1.execute("SET threads = 1")
    register_simd_math_v1(v1)
    ScalarFunctionV1.from_function["mojo_add", DType.int64, DType.int64, DType.int64, add](v1)
    AggregateFunctionV1.from_sum["mojo_sum", DType.float64](v1)
    v2.register(AggregateFunction.from_sum["mojo_sum", DType.float64]())
    v2.register(AggregateFunction.from_state["mojo_avg", Avg.update]())

    var sqrt_sql = String("SELECT sum(mojo_sqrt(i::DOUBLE)) FROM range(", N, ") t(i)")
    var add_sql = String("SELECT sum(mojo_add(i, 1)) FROM range(", N, ") t(i)")
    print(
        "sqrt   | builtin:", _ms(_time_v2(v2, String("SELECT sum(sqrt(i::DOUBLE)) FROM range(", N, ") t(i)"))),
        "| V1 SIMD:", _ms(_time_v1(v1, sqrt_sql)),
        "| V2 SIMD:", _ms(_time_v2(v2, sqrt_sql)),
    )
    print(
        "add    | builtin:", _ms(_time_v2(v2, String("SELECT sum(i + 1) FROM range(", N, ") t(i)"))),
        "| V1 row-wise:", _ms(_time_v1(v1, add_sql)),
        "| V2 row-wise:", _ms(_time_v2(v2, add_sql)),
    )
    print(
        "strlen | builtin:", _ms(_time_v2(v2, String("SELECT sum(strlen('name ' || i)) FROM range(", N, ") t(i)"))),
        "| V2 row-wise (String):", _ms(_time_v2(v2, String("SELECT sum(mojo_len('name ' || i)) FROM range(", N, ") t(i)"))),
    )
    var sum_sql = String("SELECT mojo_sum(i::DOUBLE) FROM range(", N, ") t(i)")
    print(
        "sum    | builtin:", _ms(_time_v2(v2, String("SELECT sum(i::DOUBLE) FROM range(", N, ") t(i)"))),
        "| V1 SIMD:", _ms(_time_v1(v1, sum_sql)),
        "| V2 SIMD:", _ms(_time_v2(v2, sum_sql)),
    )
    var group_sql = String("SELECT i % 100, mojo_sum(i::DOUBLE) FROM range(", N, ") t(i) GROUP BY 1")
    print(
        "sum by 100 groups | builtin:", _ms(_time_v2(v2, String("SELECT i % 100, sum(i::DOUBLE) FROM range(", N, ") t(i) GROUP BY 1"))),
        "| V2:", _ms(_time_v2(v2, group_sql)),
        "| V2 avg state:", _ms(_time_v2(v2, String("SELECT i % 100, mojo_avg(i::DOUBLE) FROM range(", N, ") t(i) GROUP BY 1"))),
        "| builtin avg:", _ms(_time_v2(v2, String("SELECT i % 100, avg(i::DOUBLE) FROM range(", N, ") t(i) GROUP BY 1"))),
    )
