"""Tests for function signatures: named parameters, defaults, named-only
parameters, *args, ANY with computed result types, aggregate bind data."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb.v2 import *


def scale(x: Float64, factor: Float64) -> Float64:
    return x * factor


def describe(x: Int64, unit: String, verbose: Bool) -> String:
    if verbose:
        return String(x, " ", unit, " (verbose)")
    return String(x, unit)


def sum_all(mut args: ScalarArgs, mut out: VectorWriter) raises:
    """Adds every argument (any number of BIGINTs)."""
    var readers = List[ColumnReader[Int64, origin_of(args)]]()
    for i in range(len(args)):
        readers.append(args.column[Int64](i))
    for row in range(args.size()):
        var total: Int64 = 0
        var null = False
        for i in range(len(readers)):
            var v = readers[i].get(row)
            if v:
                total += v.value()
            else:
                null = True
        if null:
            out.write_null(row)
        else:
            out.write(row, total)


def first_arg(mut args: ScalarArgs, mut out: VectorWriter) raises:
    """Returns its first argument unchanged, whatever its type (copies values)."""
    var t = args.type(0)
    if t.id() == TypeId.VARCHAR:
        var x = args.column[String](0)
        for row in range(args.size()):
            var v = x.get(row)
            if v:
                out.write(row, v.value())
            else:
                out.write_null(row)
    else:
        var x = args.column[Int64](0)
        for row in range(args.size()):
            var v = x.get(row)
            if v:
                out.write(row, v.value())
            else:
                out.write_null(row)


def list_of_first(args: BindArgs) raises -> LogicalType:
    return LogicalType.list(args.type(0))


def wrap_in_list(mut args: ScalarArgs, mut out: VectorWriter) raises:
    var x = args.column[Int64](0)
    for row in range(args.size()):
        var l: List[Int64] = [x[row]]
        out.write(row, l)


@fieldwise_init
struct Sep(BindData, Copyable, Equatable):
    var text: String

    @staticmethod
    def bind(args: BindArgs) raises -> Self:
        var s = args.constant[String](1)
        return Self(s.value() if s else String(","))


struct Join(AggregateBind, AggregateState):
    comptime Result = String
    comptime Bind = Sep
    var sep: String
    var parts: List[String]

    def __init__(out self):
        self.sep = ""
        self.parts = []

    def init_from(mut self, bind: Sep):
        self.sep = bind.text

    def update(mut self, s: String, sep: String):
        self.parts.append(s)

    def combine(mut self, other: Self):
        self.parts.extend(other.parts.copy())

    def finalize(self) -> String:
        var p = self.parts.copy()
        sort(p)
        return self.sep.join(p)


def test_named_and_defaults() raises:
    var con = Database().connect()
    con.register(ScalarFunction.from_function["scale", scale]().named("x", "factor").default("factor", Float64(2.0)))
    assert_equal(con.execute("SELECT scale(3.0)").get[Float64]()[0], 6.0)
    assert_equal(con.execute("SELECT scale(3.0, 4.0)").get[Float64]()[0], 12.0)
    assert_equal(con.execute("SELECT scale(factor := 5.0, x := 2.0)").get[Float64]()[0], 10.0)
    assert_equal(con.execute("SELECT scale(i::DOUBLE) FROM range(3000) t(i)").get[Float64]()[2999], 5998.0)


def test_named_only() raises:
    var con = Database().connect()
    con.register(
        ScalarFunction.from_function["describe_len", describe]()
        .named("x", "unit", "verbose")
        .default("unit", String("m"))
        .default("verbose", False)
        .named_only("verbose")
    )
    assert_equal(con.execute("SELECT describe_len(5)").get[String]()[0], "5m")
    assert_equal(con.execute("SELECT describe_len(5, 'km', verbose := true)").get[String]()[0], "5 km (verbose)")
    var failed = False
    try:
        _ = con.execute("SELECT describe_len(5, 'km', true)").fetchall()  # verbose only by name
    except e:
        failed = True
    assert_true(failed)


def test_signature_errors() raises:
    var failed = False
    try:
        _ = ScalarFunction.from_function["scale", scale]().named("only_one")
    except e:
        failed = True
        assert_true("names for 2 parameters" in e.message, e.message)
    assert_true(failed)
    failed = False
    try:
        _ = ScalarFunction.from_function["scale", scale]().varargs("rest", LogicalType(TypeId.BIGINT))
    except e:
        failed = True
        assert_true("chunk-level" in e.message, e.message)
    assert_true(failed)


def test_varargs() raises:
    var con = Database().connect()
    con.register(
        ScalarFunction.from_chunk_function[sum_all]("sum_all", [LogicalType(TypeId.BIGINT)], LogicalType(TypeId.BIGINT))
        .varargs("rest", LogicalType(TypeId.BIGINT))
    )
    assert_equal(con.execute("SELECT sum_all(1)").get[Int64]()[0], 1)
    assert_equal(con.execute("SELECT sum_all(1, 2, 3, 4, 5)").get[Int64]()[0], 15)
    assert_false(con.execute("SELECT sum_all(1, NULL)").get[Optional[Int64]]()[0].__bool__())


def test_any_and_result_types() raises:
    var con = Database().connect()
    con.register(
        ScalarFunction.from_chunk_function[first_arg]("first_arg", [LogicalType(TypeId.ANY)], LogicalType(TypeId.ANY))
        .result_type_of(0)
    )
    assert_equal(con.execute("SELECT first_arg('a string longer than twelve')").get[String]()[0], "a string longer than twelve")
    assert_equal(con.execute("SELECT first_arg(42::BIGINT)").get[Int64]()[0], 42)
    assert_equal(String(con.execute("SELECT first_arg('x') AS v").columns()[0].type), "VARCHAR")
    con.register(
        ScalarFunction.from_chunk_function[wrap_in_list]("wrap", [LogicalType(TypeId.BIGINT)], LogicalType(TypeId.ANY))
        .resolve_type[list_of_first]()
    )
    # The resolve callback sees the argument types before the cast to the
    # parameter type (an INTEGER literal here), so pass a BIGINT.
    assert_equal(con.execute("SELECT wrap(7::BIGINT)").get[List[Int64]]()[0][0], 7)


def test_aggregate_bind_data() raises:
    var con = Database().connect()
    con.register(AggregateFunction.from_state["join_with", Join.update]())
    var r = con.execute(
        "SELECT join_with(s, ' | '), join_with(s, '-') FROM (VALUES ('b'), ('a'), ('c')) t(s)"
    ).fetchall()
    assert_equal(r.get[String](col=0, row=0), "a | b | c")
    assert_equal(r.get[String](col=1, row=0), "a-b-c")
    var g = con.execute(
        "SELECT k, join_with(s, '+') FROM (VALUES (1, 'x'), (2, 'y'), (1, 'z')) t(k, s) GROUP BY k ORDER BY k"
    ).fetchall()
    assert_equal(g.get[String](col=1, row=0), "x+z")


def test_table_named_parameters() raises:
    var con = Database().connect()
    con.register(
        TableFunction.from_impl[Repeat]("repeat_text", param_types[String]())
        .named("text")
        .add_named("times", Int64(3))
    )
    assert_equal(con.execute("SELECT count(*) FROM repeat_text('a')").get[Int64]()[0], 3)
    assert_equal(con.execute("SELECT count(*) FROM repeat_text('a', times := 10)").get[Int64]()[0], 10)


@fieldwise_init
struct Left(Movable):
    var n: Int64


@fieldwise_init
struct Repeat(TableFunctionImpl):
    comptime State = Left
    var text: String
    var times: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[String]("text")
        return Self(args.constant[String](0).value(), args.constant[Int64]("times").value())

    def init(self, shared: ScanShared) raises -> Left:
        return Left(self.times)

    def scan(self, mut state: Left, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.n > 0:
            out.write(rows, 0, self.text)
            state.n -= 1
            rows += 1
        return rows


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
