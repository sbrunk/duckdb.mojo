"""Tests for V2 table functions: bind, projection, parallel scans, nested
columns, rows as structs, errors."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb.v2 import *


@fieldwise_init
struct Counter(Movable):
    var next: Int64


@fieldwise_init
struct Squares(TableFunctionImpl):
    comptime State = Counter
    var n: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        var n = args.constant[Int64](0)
        if not n or n.value() < 0:
            raise Error("squares needs a non-negative count")
        columns.add[Int64]("i")
        columns.add[Int64]("square")
        columns.add[String]("label")
        columns.set_cardinality(Int(n.value()), exact=True)
        return Self(n.value())

    def init(self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(self, mut state: Counter, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < self.n:
            out.write(rows, 0, state.next)
            out.write(rows, 1, state.next * state.next)
            if out.wants(2):
                out.write(rows, 2, String("number ", state.next, " with a long label"))
            state.next += 1
            rows += 1
        return rows


@fieldwise_init
struct Item(Copyable, Defaultable, Movable):
    var id: Int64
    var tags: List[String]
    var score: Optional[Float64]

    def __init__(out self):
        self.id = 0
        self.tags = []
        self.score = None


@fieldwise_init
struct Items(TableFunctionImpl):
    comptime State = Counter
    var n: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add_row[Item]()
        return Self(args.constant[Int64](0).value())

    def init(self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(self, mut state: Counter, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < self.n:
            var tags = List[String]()
            for k in range(Int(state.next % 3)):
                tags.append(String("tag ", k))
            var score = Optional[Float64](None) if state.next % 2 == 1 else Optional[Float64](Float64(state.next))
            out.write_row(rows, Item(state.next, tags^, score))
            state.next += 1
            rows += 1
        return rows


comptime BLOCK = 1000


@fieldwise_init
struct ParallelState(Movable):
    var shared: ScanShared
    var block: Int
    var pos: Int


@fieldwise_init
struct ParallelRange(TableFunctionImpl):
    """0..n split into blocks that threads claim."""

    comptime State = ParallelState
    var n: Int

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("i")
        return Self(Int(args.constant[Int64](0).value()))

    def max_threads(self) -> Int:
        return 4

    def init(self, shared: ScanShared) raises -> ParallelState:
        return ParallelState(shared, -1, 0)

    def scan(self, mut state: ParallelState, mut out: TableOutput) raises -> Int:
        var rows = 0
        var data = out.data[Int64](0)
        while rows < out.capacity():
            if state.block < 0 or state.pos >= min((state.block + 1) * BLOCK, self.n):
                state.block = state.shared.claim()
                state.pos = state.block * BLOCK
                if state.pos >= self.n:
                    break
            if data:
                data.value()[unsafe_offset=rows] = Int64(state.pos)
            state.pos += 1
            rows += 1
        return rows


@fieldwise_init
struct Failing(TableFunctionImpl):
    comptime State = Counter
    var x: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("x")
        return Self(0)

    def init(self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(self, mut state: Counter, mut out: TableOutput) raises -> Int:
        raise Error("scan failed on purpose")


def test_basic_and_projection() raises:
    var con = Database().connect()
    con.register(TableFunction.from_impl[Squares]("squares", param_types[Int64]()))
    var r = con.execute("SELECT * FROM squares(5000)").fetchall()
    assert_equal(len(r), 5000)
    assert_equal(r.get[Int64](col=1, row=4999), 4999 * 4999)
    assert_equal(r.get[String](col=2, row=3), "number 3 with a long label")
    # Only some columns, in another order.
    var p = con.execute("SELECT square, i FROM squares(10) WHERE i > 7 ORDER BY i").fetchall()
    assert_equal(p.get[Int64](col=0, row=1), 81)
    assert_equal(con.execute("SELECT count(*) FROM squares(3000)").get[Int64]()[0], 3000)
    assert_equal(con.execute("SELECT count(*) FROM squares(0)").get[Int64]()[0], 0)


def test_bind_errors() raises:
    var con = Database().connect()
    con.register(TableFunction.from_impl[Squares]("squares", param_types[Int64]()))
    con.register(TableFunction.from_impl[Failing]("failing", param_types[]()))
    var message = String()
    try:
        _ = con.execute("SELECT * FROM squares(-1)").fetchall()
    except e:
        message = e.message
    assert_true("non-negative count" in message, message)
    message = String()
    try:
        _ = con.execute("SELECT * FROM failing()").fetchall()
    except e:
        message = e.message
    assert_true("scan failed on purpose" in message, message)


def test_rows_as_structs() raises:
    var con = Database().connect()
    con.register(TableFunction.from_impl[Items]("items", param_types[Int64]()))
    var items = con.execute("SELECT * FROM items(3000)").get[Item]()
    assert_equal(len(items), 3000)
    assert_equal(len(items[2].tags), 2)
    assert_false(items[1].score.__bool__())
    assert_equal(items[2998].score.value(), 2998.0)
    # Projection with write_row: fields of columns not asked for are skipped.
    assert_equal(con.execute("SELECT sum(len(tags)) FROM items(30)").get[Int128]()[0], 30)


def test_parallel_scan() raises:
    var con = Database().connect()
    _ = con.execute("SET threads = 4")
    con.register(TableFunction.from_impl[ParallelRange]("prange", param_types[Int64]()))
    var r = con.execute("SELECT count(*), sum(i), count(DISTINCT i) FROM prange(1000000)").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 1000000)
    assert_equal(r.get[Int128](col=1, row=0), 499999500000)
    assert_equal(r.get[Int64](col=2, row=0), 1000000)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
