"""Tests for V2 scalar functions: row-wise functions with inferred
signatures, NULL handling, nested types, errors, bind data."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
import std.math
from duckdb.v2 import *


def add(a: Int64, b: Int64) -> Int64:
    return a + b


def greet(name: String) -> String:
    return String("hello ", name)


def first_or_default(a: Optional[Int64], b: Int64) -> Int64:
    return a.value() if a else b


def maybe_half(x: Int64) -> Optional[Int64]:
    if x % 2 == 1:
        return None
    return x // 2


def checked_div(a: Int64, b: Int64) raises -> Int64:
    if b == 0:
        raise Error("division by zero in checked_div")
    return a // b


def list_total(values: List[Int64]) -> Int64:
    var total: Int64 = 0
    for v in values:
        total += v
    return total


def range_list(n: Int64) -> List[String]:
    var out = List[String]()
    for i in range(Int(n)):
        out.append(String("item number ", i, " with a long suffix"))
    return out^


@fieldwise_init
struct Point(Copyable, Movable):
    var x: Int32
    var y: Int32


def swap(p: Point) -> Point:
    return Point(p.y, p.x)


def sum8(a: Int64, b: Int64, c: Int64, d: Int64, e: Int64, f: Int64, g: Int64, h: Int64) -> Int64:
    return a + b + c + d + e + f + g + h


@fieldwise_init
struct Scale(BindData, Copyable, Equatable):
    var factor: Float64

    @staticmethod
    def bind(args: BindArgs) raises -> Self:
        var f = args.constant[Float64](1)
        if not f:
            raise Error("the factor must not be NULL")
        return Self(f.value())


def scale(s: Scale, x: Float64, factor: Float64) -> Float64:
    return x * s.factor


def times_ten[w: SIMDLength](x: SIMD[DType.int64, w]) -> SIMD[DType.float64, w]:
    return x.cast[DType.float64]() * 10.0


def add_mixed[w: SIMDLength](x: SIMD[DType.int32, w], y: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
    return x.cast[DType.float64]() + y


def double_it(mut args: ScalarArgs, mut out: VectorWriter) raises:
    var x = args.column[Int64](0)
    for row in range(args.size()):
        var v = x.get(row)
        if v:
            out.write(row, v.value() * 2)
        else:
            out.write_null(row)


def scaled_chunk(mut args: ScalarArgs, mut out: VectorWriter) raises:
    ref s = args.bind_data[Scale]()
    var x = args.column[Float64](0)
    var span = x.span()
    var dst = out.data[Float64]()
    for row in range(args.size()):
        if span:
            dst[unsafe_offset=row] = span.value()[row] * s.factor
        else:
            var v = x.get(row)
            if v:
                dst[unsafe_offset=row] = v.value() * s.factor
            else:
                out.write_null(row)


struct Scratch(LocalState):
    var buffer: List[Int64]
    var calls: Int

    def __init__(out self):
        self.buffer = []
        self.calls = 0

    @staticmethod
    def init() raises -> Self:
        return Self()


def running_calls(mut args: ScalarArgs, mut out: VectorWriter) raises:
    """Writes how many chunks this thread has processed so far (per-thread state)."""
    ref s = args.local_state[Scratch]()
    s.calls += 1
    s.buffer.resize(args.size(), 0)
    for row in range(args.size()):
        out.write(row, Int64(s.calls))


def test_row_wise_numbers() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["mojo_add", add]())
    var r = con.execute("SELECT mojo_add(i, 10) FROM range(5000) t(i)").get[Int64]()
    assert_equal(len(r), 5000)
    assert_equal(r[4999], 5009)
    # Constant and dictionary inputs.
    assert_equal(con.execute("SELECT mojo_add(2, 3)").get[Int64]()[0], 5)
    var d = con.execute("SELECT mojo_add(x, x) FROM (SELECT (i % 3)::BIGINT AS x FROM range(10) t(i)) ORDER BY 1 DESC LIMIT 1").get[Int64]()
    assert_equal(d[0], 4)


def test_strings_and_nulls() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["greet", greet]())
    con.register(ScalarFunction.from_function["mojo_add", add]())
    con.register(ScalarFunction.from_function["first_or_default", first_or_default]())
    con.register(ScalarFunction.from_function["maybe_half", maybe_half]())
    assert_equal(con.execute("SELECT greet('world, from a long string')").get[String]()[0], "hello world, from a long string")
    # Default handling: a NULL argument gives NULL.
    var n = con.execute("SELECT mojo_add(NULL, 1), mojo_add(x, 1) FROM (VALUES (NULL::BIGINT), (1)) t(x)").fetchall()
    assert_false(n.get_optional[Int64](col=0, row=0).__bool__())
    assert_false(n.get_optional[Int64](col=1, row=0).__bool__())
    assert_equal(n.get[Int64](col=1, row=1), 2)
    # An Optional argument sees NULLs.
    assert_equal(con.execute("SELECT first_or_default(NULL, 7)").get[Int64]()[0], 7)
    assert_equal(con.execute("SELECT first_or_default(x, 7) FROM (VALUES (NULL::BIGINT), (3)) t(x) ORDER BY 1").get[Int64]()[0], 3)
    # An Optional result returns NULL.
    var h = con.execute("SELECT maybe_half(i) FROM range(4) t(i)").get[Optional[Int64]]()
    assert_false(h[1].__bool__())
    assert_equal(h[2].value(), 1)


def test_errors() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["checked_div", checked_div]())
    assert_equal(con.execute("SELECT checked_div(9, 3)").get[Int64]()[0], 3)
    var message = String()
    try:
        _ = con.execute("SELECT checked_div(i, i - 2) FROM range(5) t(i)").fetchall()
    except e:
        message = e.message
    assert_true("division by zero in checked_div" in message, message)
    # A NULL divisor gives NULL; the function is not called for that row.
    var n = con.execute("SELECT checked_div(10, CASE WHEN i > 0 THEN i END) FROM range(4) t(i)").get[Optional[Int64]]()
    assert_false(n[0].__bool__())
    assert_equal(n[3].value(), 3)


def test_nested_types() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["list_total", list_total]())
    con.register(ScalarFunction.from_function["range_list", range_list]())
    con.register(ScalarFunction.from_function["swap", swap]())
    assert_equal(con.execute("SELECT list_total([1, 2, 3])").get[Int64]()[0], 6)
    var totals = con.execute("SELECT list_total(range(i)) FROM range(3000) t(i)").get[Int64]()
    assert_equal(totals[2999], 2999 * 2998 // 2)
    var lists = con.execute("SELECT range_list(i % 5) FROM range(3000) t(i)").get[List[String]]()
    assert_equal(len(lists[2999]), 4)
    assert_equal(lists[2999][3], "item number 3 with a long suffix")
    var p = con.execute("SELECT swap({'x': 1, 'y': 2})").fetchall().get[Point](col=0, row=0)
    assert_equal(p.x, 2)


def test_eight_arguments_and_overloads() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["sum8", sum8]())
    assert_equal(con.execute("SELECT sum8(1, 2, 3, 4, 5, 6, 7, 8)").get[Int64]()[0], 36)
    # Same name, other argument types: an overload.
    con.register(ScalarFunction.from_function["poly", add]())
    con.register(ScalarFunction.from_function["poly", greet]())
    assert_equal(con.execute("SELECT poly(1, 2)").get[Int64]()[0], 3)
    assert_equal(con.execute("SELECT poly('x')").get[String]()[0], "hello x")


def test_bind_data() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["scale", scale]())
    var r = con.execute("SELECT scale(i::DOUBLE, 2.5) FROM range(3000) t(i)").get[Float64]()
    assert_equal(r[2], 5.0)
    var message = String()
    try:
        _ = con.execute("SELECT scale(i::DOUBLE, i::DOUBLE) FROM range(3) t(i)").fetchall()
    except e:
        message = e.message
    assert_true("not a constant" in message, message)


def test_constant_nested_input_and_volatile() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["list_total", list_total]())
    var r = con.execute("SELECT list_total([1, 2, 3]) + i FROM range(3000) t(i)").get[Int64]()
    assert_equal(r[2999], 3005)
    con.register(ScalarFunction.from_function["v_add", add](volatile=True))
    assert_equal(con.execute("SELECT v_add(1, 2)").get[Int64]()[0], 3)


def test_simd_functions() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_simd_function["times_ten", DType.int64, DType.float64, times_ten]())
    con.register(ScalarFunction.from_simd_function["add_mixed", DType.int32, DType.float64, DType.float64, add_mixed]())
    con.register(ScalarFunction.from_simd_function["msqrt", DType.float64, std.math.sqrt]())
    var r = con.execute("SELECT times_ten(i) FROM range(5003) t(i)").get[Float64]()
    assert_equal(r[5002], 50020.0)
    # NULLs in a flat vector, and constant inputs.
    var n = con.execute("SELECT times_ten(CASE WHEN i % 2 = 0 THEN i END) FROM range(10) t(i)").get[Optional[Float64]]()
    assert_false(n[1].__bool__())
    assert_equal(n[2].value(), 20.0)
    assert_equal(con.execute("SELECT add_mixed(i::INTEGER, 0.5) FROM range(20) t(i)").get[Float64]()[19], 19.5)
    assert_equal(con.execute("SELECT msqrt(16.0)").get[Float64]()[0], 4.0)


def test_chunk_functions() raises:
    var con = Database().connect()
    con.register(
        ScalarFunction.from_chunk_function[double_it]("double_it", [LogicalType(TypeId.BIGINT)], LogicalType(TypeId.BIGINT))
    )
    var r = con.execute("SELECT double_it(CASE WHEN i > 0 THEN i END) FROM range(3000) t(i)").get[Optional[Int64]]()
    assert_false(r[0].__bool__())
    assert_equal(r[2999].value(), 5998)
    con.register(
        ScalarFunction.from_chunk_function[scaled_chunk](
            "scaled",
            [LogicalType(TypeId.DOUBLE), LogicalType(TypeId.DOUBLE)],
            LogicalType(TypeId.DOUBLE),
            bind=bind_callback[Scale](),
        )
    )
    assert_equal(con.execute("SELECT scaled(i::DOUBLE, 3.0) FROM range(10) t(i)").get[Float64]()[3], 9.0)


def test_thread_state() raises:
    var con = Database().connect()
    _ = con.execute("SET threads = 1")
    con.register(
        ScalarFunction.from_chunk_function[running_calls](
            "running_calls",
            [LogicalType(TypeId.BIGINT)],
            LogicalType(TypeId.BIGINT),
            init=init_callback[Scratch](),
            volatile=True,
        )
    )
    # One thread, 3 chunks: the state survives across chunks of one query.
    var r = con.execute("SELECT max(running_calls(i)) FROM range(3 * 2048) t(i)").get[Int64]()
    assert_equal(r[0], 3)
    # A new query starts with a new state.
    assert_equal(con.execute("SELECT max(running_calls(i)) FROM range(10) t(i)").get[Int64]()[0], 1)


def test_register_simd_math() raises:
    var con = Database().connect()
    register_simd_math(con)
    assert_equal(con.execute("SELECT mojo_sqrt(9.0)").get[Float64]()[0], 3.0)
    assert_equal(con.execute("SELECT round(mojo_log10(1000.0), 6)").get[Float64]()[0], 3.0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
