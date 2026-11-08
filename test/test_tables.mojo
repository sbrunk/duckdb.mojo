"""Tests for table functions: bind, projection, parallel scans, nested
columns, rows as structs, errors."""

from std.testing import assert_equal, assert_false, assert_true
from std.os import abort
from std.testing.suite import TestSuite
from duckdb import *


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

    def init(ref self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(ref self, mut state: Counter, mut out: TableOutput) raises -> Int:
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

    def init(ref self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(ref self, mut state: Counter, mut out: TableOutput) raises -> Int:
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

    def max_threads(ref self) -> Int:
        return 4

    def init(ref self, shared: ScanShared) raises -> ParallelState:
        return ParallelState(shared, -1, 0)

    def scan(ref self, mut state: ParallelState, mut out: TableOutput) raises -> Int:
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
struct OrderedRange(OrderedBatches, ScanProgress, TableFunctionImpl):
    """Like ParallelRange, but numbers its batches, so rows keep their order."""

    comptime State = ParallelState
    var n: Int

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("i")
        return Self(Int(args.constant[Int64](0).value()))

    def max_threads(ref self) -> Int:
        return 4

    def progress(ref self, shared: ScanShared) -> Float64:
        return min(1.0, Float64(shared.claimed() * BLOCK) / Float64(max(self.n, 1)))

    def init(ref self, shared: ScanShared) raises -> ParallelState:
        return ParallelState(shared, -1, 0)

    def scan(ref self, mut state: ParallelState, mut out: TableOutput) raises -> Int:
        # One block per batch, so the block number orders the batches.
        if state.block < 0 or state.pos >= min((state.block + 1) * BLOCK, self.n):
            state.block = state.shared.claim()
            state.pos = state.block * BLOCK
            if state.pos >= self.n:
                return 0
        out.set_batch(state.block)
        var rows = 0
        var end = min((state.block + 1) * BLOCK, self.n)
        while rows < out.capacity() and state.pos < end:
            out.write(rows, 0, Int64(state.pos))
            state.pos += 1
            rows += 1
        return rows


@fieldwise_init
struct UnorderedRange(TableFunctionImpl):
    comptime State = ParallelState
    var n: Int

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("i")
        columns.set_unordered()
        return Self(Int(args.constant[Int64](0).value()))

    def max_threads(ref self) -> Int:
        return 4

    def init(ref self, shared: ScanShared) raises -> ParallelState:
        return ParallelState(shared, -1, 0)

    def scan(ref self, mut state: ParallelState, mut out: TableOutput) raises -> Int:
        return ParallelRange(self.n).scan(state, out)


@fieldwise_init
struct Failing(TableFunctionImpl):
    comptime State = Counter
    var x: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("x")
        return Self(0)

    def init(ref self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(ref self, mut state: Counter, mut out: TableOutput) raises -> Int:
        raise Error("scan failed on purpose")


from std.ffi import _Global


@fieldwise_init
struct Produced(Defaultable, Movable):
    var rows: Int

    def __init__(out self):
        self.rows = 0


comptime PRODUCED = _Global["test_v2_tables_produced", _new_produced]


def _new_produced() -> Produced:
    return Produced()


def produced() -> ref[MutUntrackedOrigin] Int:
    try:
        return PRODUCED.get_or_create_ptr()[].rows
    except:
        abort()


@fieldwise_init
struct Filtered(FilterPushdown, TableFunctionImpl):
    """0..n, applying `i < x`, `i <= x`, `i > x`, `i >= x` itself."""

    comptime State = Counter
    var low: Int64
    var high: Int64  # exclusive

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("i")
        columns.add[String]("label")
        return Self(0, args.constant[Int64](0).value())

    def pushdown(mut self, filter: Expression) raises -> Bool:
        var b = filter.between()
        if b and b.value().column == 0:
            # Bounds may be exclusive: narrow (inclusively), leave the exact filter to DuckDB.
            self.low = max(self.low, b.value().low.get[Int64]())
            self.high = min(self.high, b.value().high.get[Int64]() + 1)
            return False
        var c = filter.comparison()
        if not c or c.value().column != 0:
            return False
        ref cmp = c.value()
        var v = cmp.value.get_optional[Int64]()
        if not v:
            return False
        var x = v.value()
        if cmp.op == ExprKind.LESS:
            self.high = min(self.high, x)
        elif cmp.op == ExprKind.LESS_EQUAL:
            self.high = min(self.high, x + 1)
        elif cmp.op == ExprKind.GREATER:
            self.low = max(self.low, x + 1)
        elif cmp.op == ExprKind.GREATER_EQUAL:
            self.low = max(self.low, x)
        else:
            return False
        return True

    def init(ref self, shared: ScanShared) raises -> Counter:
        return Counter(self.low)

    def scan(ref self, mut state: Counter, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < self.high:
            out.write(rows, 0, state.next)
            out.write(rows, 1, String("row ", state.next))
            state.next += 1
            rows += 1
        produced() += rows
        return rows


def test_filter_pushdown() raises:
    var con = Database().connect()
    _ = con.execute("SET threads = 1")
    con.register(TableFunction.from_impl[Filtered]("filtered", param_types[Int64]()))
    produced() = 0
    var r = con.execute("SELECT count(*), min(i), max(i) FROM filtered(100000) WHERE i >= 500 AND i < 600").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 100)
    assert_equal(r.get[Int64](col=1, row=0), 500)
    assert_equal(r.get[Int64](col=2, row=0), 599)
    # A range arrives as BETWEEN, which only narrows: 500..600 inclusive.
    assert_equal(produced(), 101)
    # Single comparisons are taken over: exactly the matching rows are scanned.
    produced() = 0
    assert_equal(con.execute("SELECT count(*) FROM filtered(100000) WHERE i < 50").get[Int64]()[0], 50)
    assert_equal(produced(), 50)
    # The constant on the left, and a filter it does not take (left to DuckDB).
    produced() = 0
    var l = con.execute("SELECT count(*) FROM filtered(1000) WHERE 10 > i AND label <> 'row 3'").get[Int64]()
    assert_equal(l[0], 9)
    assert_equal(produced(), 10)
    # No filter: everything.
    assert_equal(con.execute("SELECT count(*) FROM filtered(5000)").get[Int64]()[0], 5000)


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


def test_order_and_progress() raises:
    var con = Database().connect()
    _ = con.execute("SET threads = 4")
    con.register(TableFunction.from_impl[OrderedRange]("ordered_range", param_types[Int64]()))
    con.register(TableFunction.from_impl[UnorderedRange]("unordered_range", param_types[Int64]()))
    # Numbered batches come back in order, even from 4 threads.
    var rows = con.execute("SELECT i FROM ordered_range(200000)").get[Int64]()
    assert_equal(len(rows), 200000)
    var in_order = True
    for k in range(len(rows)):
        if rows[k] != Int64(k):
            in_order = False
    assert_true(in_order)
    _ = con.execute("CREATE TABLE copy AS SELECT * FROM ordered_range(50000)")
    assert_equal(con.execute("SELECT count(*) FROM copy").get[Int64]()[0], 50000)
    var u = con.execute("SELECT count(*), sum(i) FROM unordered_range(200000)").fetchall()
    assert_equal(u.get[Int64](col=0, row=0), 200000)
    assert_equal(u.get[Int128](col=1, row=0), 19999900000)


def test_streaming_steps() raises:
    var con = Database().connect()
    var r = con.execute("SELECT * FROM range(10000) t(i)")
    var rows = 0
    var steps = 0
    while True:
        var st = r.step()
        steps += 1
        if st.status == StepStatus.CHUNK:
            rows += len(st.chunk.value())
        elif st.status == StepStatus.WAITING:
            r.wait()
        else:
            assert_true(st.status == StepStatus.FINISHED)
            break
    assert_equal(rows, 10000)
    assert_true(steps > 1)
    # A finished result keeps reporting FINISHED.
    assert_true(r.step().status == StepStatus.FINISHED)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
