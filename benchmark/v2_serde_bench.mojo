"""Benchmark: decoding rows into Mojo values and appending them, V2 API.

Reading times only the decode (chunk to Mojo values), not the query.
Writing times `Appender` end to end (writing chunks plus the INSERT). Run:

    pixi run mojo build benchmark/v2_serde_bench.mojo -o /tmp/v2_serde_bench && /tmp/v2_serde_bench
"""

from std.time import perf_counter_ns
from duckdb.v2 import *
from duckdb.duckdb_type import Date

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


def read_rows(con: Connection) raises -> Tuple[Int, Int]:
    var t = 0
    var check = 0
    for chunk in con.execute(ROWS_SQL).chunks():
        var t0 = perf_counter_ns()
        var rows = chunk.get[Rec]()
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def read_struct(con: Connection) raises -> Tuple[Int, Int]:
    var t = 0
    var check = 0
    for chunk in con.execute(STRUCT_SQL).chunks():
        var t0 = perf_counter_ns()
        var reader = chunk.column[Rec](0)
        var rows = List[Rec](capacity=len(reader))
        for i in range(len(reader)):
            rows.append(reader[i])
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1].a)
    return (t, check)


def read_list(con: Connection) raises -> Tuple[Int, Int]:
    var t = 0
    var check = 0
    for chunk in con.execute(LIST_SQL).chunks():
        var t0 = perf_counter_ns()
        var reader = chunk.column[List[Int64]](0)
        var rows = List[List[Int64]](capacity=len(reader))
        for i in range(len(reader)):
            rows.append(reader[i])
        t += Int(perf_counter_ns() - t0)
        check += len(rows) + Int(rows[len(rows) - 1][2])
    return (t, check)


def append_rows(con: Connection) raises -> Tuple[Int, Int]:
    _ = con.execute("CREATE OR REPLACE TABLE recs (a INTEGER, b VARCHAR, c DATE)")
    var t0 = perf_counter_ns()
    var a = Appender(con, "recs")
    for i in range(1_000_000):
        a.append_row(Rec(Int32(i), String("name number ", i), Date(Int32(i % 10000))))
    a.close()
    var t = Int(perf_counter_ns() - t0)
    return (t, Int(con.execute("SELECT count(*) FROM recs").get[Int64]()[0]))


def append_lists(con: Connection) raises -> Tuple[Int, Int]:
    _ = con.execute("CREATE OR REPLACE TABLE lists (l BIGINT[])")
    var t0 = perf_counter_ns()
    var a = Appender(con, "lists")
    for i in range(1_000_000):
        var l: List[Int64] = [Int64(i), Int64(i + 1), Int64(i + 2)]
        a.append_value(l)
        a.end_row()
    a.close()
    var t = Int(perf_counter_ns() - t0)
    return (t, Int(con.execute("SELECT count(*) FROM lists").get[Int64]()[0]))


def main() raises:
    var con = Database().connect()
    for _ in range(2):  # the first round warms up
        var r = read_rows(con)
        var s = read_struct(con)
        var l = read_list(con)
        print("rows as struct:", _ms(r[0]), "| STRUCT column:", _ms(s[0]), "| LIST column:", _ms(l[0]), "|", r[1], s[1], l[1])
    var w = append_rows(con)
    var wl = append_lists(con)
    print("append 1M rows (INTEGER, VARCHAR, DATE):", _ms(w[0]), "| append 1M BIGINT[3]:", _ms(wl[0]), "|", w[1], wl[1])
