"""POC benchmark: decoding rows into Mojo structs.

Compares, per scenario, the time spent turning result chunks into Mojo values:
- current: today's V1 typed API (`Chunk.get[T]`)
- emberserde: the POC reader (duckdb/_serde_poc.mojo)
- raw: a hand-written loop over V2 vector views (lower bound)

Only the decode is timed, not the query. Run:
    pixi run mojo build benchmark/poc_serde_bench.mojo -o /tmp/poc_serde_bench && /tmp/poc_serde_bench
"""

from std.time import perf_counter_ns
from duckdb import DuckDB
from duckdb._serde_poc import PocDatabase, PocResult, DuckDBCursor
from duckdb._libduckdb_v2 import *
from duckdb.duckdb_type import Date

comptime N = 1_000_000

comptime ROWS_SQL = (
    "SELECT range::INTEGER AS a, 'name number ' || range AS b,"
    " DATE '2000-01-01' + range::INTEGER % 10000 AS c FROM range(1000000)"
)
comptime STRUCT_SQL = (
    "SELECT {'a': range::INTEGER, 'b': 'name number ' || range,"
    " 'c': DATE '2000-01-01' + range::INTEGER % 10000} AS s FROM range(1000000)"
)
comptime LIST_SQL = "SELECT [range, range + 1, range + 2] AS l FROM range(1000000)"


@fieldwise_init
struct Rec(Copyable, Defaultable, Movable):
    var a: Int32
    var b: String
    var c: Date

    def __init__(out self):
        self.a = 0
        self.b = ""
        self.c = Date(0)


def _ms(ns: Int) -> String:
    return String(Float64(ns) / 1e6, " ms")


def _report(name: String, current: Int, serde: Int, raw: Int, checks: String):
    print(
        name,
        "| current:", _ms(current),
        "| emberserde:", _ms(serde),
        "| raw:", _ms(raw),
        "| emberserde/current:", Float64(serde) / Float64(current),
        "| emberserde/raw:", Float64(serde) / Float64(raw),
        "|", checks,
    )


# ---- current (V1) ----


def current_rows() raises -> Tuple[Int, Int]:
    var con = DuckDB.connect(":memory:")
    var res = con.execute(ROWS_SQL)
    var t = 0
    var check = 0
    for chunk in res.chunks():
        var t0 = perf_counter_ns()
        var rows = chunk.get[Rec]()
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def current_struct() raises -> Tuple[Int, Int]:
    var con = DuckDB.connect(":memory:")
    var res = con.execute(STRUCT_SQL)
    var t = 0
    var check = 0
    for chunk in res.chunks():
        var t0 = perf_counter_ns()
        var rows = chunk.get[Rec](col=0)
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def current_list() raises -> Tuple[Int, Int]:
    var con = DuckDB.connect(":memory:")
    var res = con.execute(LIST_SQL)
    var t = 0
    var check = 0
    for chunk in res.chunks():
        var t0 = perf_counter_ns()
        var rows = chunk.get[List[Int64]](col=0)
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1][2])
    return (t, check)


# ---- emberserde POC ----


def serde_rows() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(ROWS_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var rows = List[Rec]()
        var t0 = perf_counter_ns()
        chunk.value().append_rows[Rec](rows)
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def serde_struct() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(STRUCT_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var t0 = perf_counter_ns()
        var rows = chunk.value().column[Rec](0)
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def serde_list() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(LIST_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var t0 = perf_counter_ns()
        var rows = chunk.value().column[List[Int64]](0)
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1][2])
    return (t, check)


# ---- raw V2 views ----


@always_inline
def _read_varchar(data: Pointer[NoneType, ImmUntrackedOrigin], i: Int) -> String:
    var base = data.unsafe_bitcast[UInt8]().unsafe_offset(i * 16)
    var length = Int(base.unsafe_bitcast[UInt32]()[])
    var bytes: Pointer[UInt8, ImmUntrackedOrigin]
    if length <= 12:
        bytes = base.unsafe_offset(4)
    else:
        bytes = base.unsafe_offset(8).unsafe_bitcast[Pointer[UInt8, ImmUntrackedOrigin]]()[]
    return String(from_utf8_lossy=Span[UInt8, ImmUntrackedOrigin](unsafe_ptr=bytes, length=length))


def raw_rows() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(ROWS_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var t0 = perf_counter_ns()
        var tree = chunk.value()._tree()
        ref cols = tree.nodes[0].children
        var a = tree.nodes[cols[0]].data.unsafe_bitcast[Int32]()
        var b = tree.nodes[cols[1]].data
        var c = tree.nodes[cols[2]].data.unsafe_bitcast[Int32]()
        var rows = List[Rec](capacity=tree.size)
        for i in range(tree.size):
            rows.append(Rec(a[unsafe_offset=i], _read_varchar(b, i), Date(c[unsafe_offset=i])))
        t += Int(perf_counter_ns() - t0)
        _ = chunk^  # the tree points into the chunk; keep it alive until here
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def raw_struct() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(STRUCT_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var t0 = perf_counter_ns()
        var tree = chunk.value()._tree()
        ref kids = tree.nodes[tree.nodes[0].children[0]].children
        var a = tree.nodes[kids[0]].data.unsafe_bitcast[Int32]()
        var b = tree.nodes[kids[1]].data
        var c = tree.nodes[kids[2]].data.unsafe_bitcast[Int32]()
        var rows = List[Rec](capacity=tree.size)
        for i in range(tree.size):
            rows.append(Rec(a[unsafe_offset=i], _read_varchar(b, i), Date(c[unsafe_offset=i])))
        t += Int(perf_counter_ns() - t0)
        _ = chunk^  # the tree points into the chunk; keep it alive until here
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def raw_list() raises -> Tuple[Int, Int]:
    var db = PocDatabase()
    var res = db.query(LIST_SQL)
    var t = 0
    var check = 0
    while True:
        var chunk = res.fetch_chunk()
        if not chunk:
            break
        var t0 = perf_counter_ns()
        var tree = chunk.value()._tree()
        ref node = tree.nodes[tree.nodes[0].children[0]]
        var entries = node.data.unsafe_bitcast[duckdb_v2_list_entry]()
        var child = tree.nodes[node.children[0]].data.unsafe_bitcast[Int64]()
        var rows = List[List[Int64]](capacity=tree.size)
        for i in range(tree.size):
            ref e = entries[unsafe_offset=i]
            var l = List[Int64](capacity=Int(e.length))
            for j in range(Int(e.offset), Int(e.offset + e.length)):
                l.append(child[unsafe_offset=j])
            rows.append(l^)
        t += Int(perf_counter_ns() - t0)
        _ = chunk^  # the tree points into the chunk; keep it alive until here
        check += len(rows) + Int(rows[len(rows) - 1][2])
    return (t, check)


def main() raises:
    for _ in range(2):  # second round is the one to read (warm)
        var c1 = current_rows()
        var s1 = serde_rows()
        var r1 = raw_rows()
        _report("rows as struct (INT, VARCHAR, DATE)", c1[0], s1[0], r1[0], String(c1[1] == s1[1] and s1[1] == r1[1]))
        var c2 = current_struct()
        var s2 = serde_struct()
        var r2 = raw_struct()
        _report("STRUCT column                      ", c2[0], s2[0], r2[0], String(c2[1] == s2[1] and s2[1] == r2[1]))
        var c3 = current_list()
        var s3 = serde_list()
        var r3 = raw_list()
        _report("LIST(BIGINT) column                ", c3[0], s3[0], r3[0], String(c3[1] == s3[1] and s3[1] == r3[1]))
        print("---")
