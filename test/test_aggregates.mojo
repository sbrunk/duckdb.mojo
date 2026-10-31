"""Tests for aggregate functions: state structs, GROUP BY, NULLs,
combine, nested results, the SIMD reductions."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb import *


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


struct Collect(AggregateState):
    """Collects strings longer than a minimum length (two arguments, a list result)."""

    comptime Result = List[String]
    var items: List[String]

    def __init__(out self):
        self.items = []

    def update(mut self, s: String, min_len: Int64):
        if s.byte_length() >= Int(min_len):
            self.items.append(s)

    def combine(mut self, other: Self):
        self.items.extend(other.items.copy())

    def finalize(self) -> List[String]:
        var out = self.items.copy()
        sort(out)
        return out^


struct CountNulls(AggregateState):
    comptime Result = Int64
    var nulls: Int64

    def __init__(out self):
        self.nulls = 0

    def update(mut self, x: Optional[Int64]):
        if not x:
            self.nulls += 1

    def combine(mut self, other: Self):
        self.nulls += other.nulls

    def finalize(self) -> Int64:
        return self.nulls


struct Failing(AggregateState):
    comptime Result = Int64
    var n: Int64

    def __init__(out self):
        self.n = 0

    def update(mut self, x: Int64) raises:
        if x == 13:
            raise Error("unlucky value in failing_agg")
        self.n += 1

    def combine(mut self, other: Self):
        self.n += other.n

    def finalize(self) -> Int64:
        return self.n


def test_basic_and_group_by() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_state["my_avg", Avg.update]())
    assert_equal(con.execute("SELECT my_avg(i::DOUBLE) FROM range(10001) t(i)").get[Float64]()[0], 5000.0)
    var g = con.execute(
        "SELECT i % 3 AS k, my_avg(i::DOUBLE) AS a FROM range(30000) t(i) GROUP BY k ORDER BY k"
    ).fetchall()
    assert_equal(len(g), 3)
    assert_equal(g.get[Float64](col=1, row=0), 14998.5)
    assert_equal(g.get[Float64](col=1, row=2), 15000.5)


def test_nulls_and_empty_groups() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_state["my_avg", Avg.update]())
    con.register(AggregateFunction.from_state["count_nulls", CountNulls.update]())
    # NULL rows are skipped; a group without rows is NULL.
    assert_equal(con.execute("SELECT my_avg(CASE WHEN i < 4 THEN i::DOUBLE END) FROM range(100) t(i)").get[Float64]()[0], 1.5)
    assert_false(con.execute("SELECT my_avg(NULL::DOUBLE) FROM range(10)").get[Optional[Float64]]()[0].__bool__())
    assert_false(con.execute("SELECT my_avg(i::DOUBLE) FROM range(0) t(i)").get[Optional[Float64]]()[0].__bool__())
    # An Optional argument sees NULLs.
    assert_equal(con.execute("SELECT count_nulls(CASE WHEN i % 4 = 0 THEN NULL ELSE i END) FROM range(100) t(i)").get[Int64]()[0], 25)


def test_two_arguments_and_list_result() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_state["collect_long", Collect.update]())
    var r = con.execute(
        "SELECT k, collect_long(s, 3) FROM (VALUES (1, 'ab'), (1, 'abc'), (2, 'a long string, longer than twelve'), (1, 'abcd'))"
        " t(k, s) GROUP BY k ORDER BY k"
    ).fetchall()
    var a = r.get[List[String]](col=1, row=0)
    assert_equal(len(a), 2)
    assert_equal(a[1], "abcd")
    assert_equal(r.get[List[String]](col=1, row=1)[0], "a long string, longer than twelve")


def test_errors() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_state["failing_agg", Failing.update]())
    var message = String()
    try:
        _ = con.execute("SELECT failing_agg(i) FROM range(100) t(i)").fetchall()
    except e:
        message = e.message
    assert_true("unlucky value in failing_agg" in message, message)


def bit_or[w: SIMDLength](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
    return a | b


def zero() -> Int64:
    return 0


def test_from_reduce() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_reduce["bits", DType.int64, bit_or, zero]())
    assert_equal(con.execute("SELECT bits(1::BIGINT << (i % 5)) FROM range(5000) t(i)").get[Int64]()[0], 31)


def test_simd_reductions() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_sum["msum", DType.float64]())
    con.register(AggregateFunction.from_min["mmin", DType.int64]())
    con.register(AggregateFunction.from_max["mmax", DType.int64]())
    con.register(AggregateFunction.from_mean["mmean", DType.float64]())
    con.register(AggregateFunction.from_product["mprod", DType.int64]())
    var r = con.execute(
        "SELECT msum(i::DOUBLE), mmin(i), mmax(i), mmean(i::DOUBLE) FROM range(100000) t(i)"
    ).fetchall()
    assert_equal(r.get[Float64](col=0, row=0), 4999950000.0)
    assert_equal(r.get[Int64](col=1, row=0), 0)
    assert_equal(r.get[Int64](col=2, row=0), 99999)
    assert_equal(r.get[Float64](col=3, row=0), 49999.5)
    assert_equal(con.execute("SELECT mprod(i) FROM range(1, 11) t(i)").get[Int64]()[0], 3628800)
    # Two aggregates with one signature over one column stay separate.
    var both = con.execute("SELECT mmin(i), mmax(i) FROM range(10) t(i)").fetchall()
    assert_equal(both.get[Int64](col=0, row=0), 0)
    assert_equal(both.get[Int64](col=1, row=0), 9)
    # With GROUP BY and NULLs (the per-row path).
    var g = con.execute(
        "SELECT i % 2 AS k, msum(CASE WHEN i % 3 = 0 THEN NULL ELSE i::DOUBLE END) FROM range(12) t(i) GROUP BY k ORDER BY k"
    ).fetchall()
    assert_equal(g.get[Float64](col=1, row=0), 2.0 + 4 + 8 + 10)
    assert_equal(g.get[Float64](col=1, row=1), 1.0 + 5 + 7 + 11)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
