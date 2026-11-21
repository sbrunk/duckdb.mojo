"""Tests for profiling, logging, log sinks, the file system and non-blocking steps."""

from std.ffi import _Global
from std.os import abort
from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb import *


struct _Seen(Defaultable, Movable):
    var messages: List[String]

    def __init__(out self):
        self.messages = []


comptime SEEN = _Global["test_v2_observe_seen", _new_seen]


def _new_seen() -> _Seen:
    return _Seen()


def seen() -> ref[MutUntrackedOrigin] List[String]:
    try:
        return SEEN.get_or_create_ptr()[].messages
    except:
        abort()


def collect(entry: LogEntry):
    if entry.type == "test_type":
        seen().append(String(entry.level, " ", entry.message))


def read_file(mut args: ScalarArgs, mut out: VectorWriter) raises:
    var fs = args.file_system()
    var paths = args.column[String](0)
    for row in range(args.size()):
        var data = fs.read_all(paths[row])
        out.write(row, String(from_utf8_lossy=Span(data)))


def test_profiling() raises:
    var con = Database().connect()
    con.enable_profiling()
    _ = con.execute("SELECT sum(i) FROM range(100000) t(i)").fetchall()
    var m = con.query_metrics()
    var root = m.root()
    assert_equal(root.name(), "QUERY")
    assert_true(root.child_count() > 0)
    var found_metric = len(root.metrics()) > 0
    for i in range(root.child_count()):
        found_metric = found_metric or len(root.child(i).metrics()) > 0
    assert_true(found_metric)
    assert_false(root.metric("no_such_metric").__bool__())


def test_logging_and_sinks() raises:
    var con = Database().connect()
    con.register(LogSink.from_function[collect]("collector"))
    _ = con.execute("SET enable_logging = true")
    _ = con.execute("SET logging_sink = 'collector'")
    con.log(LogLevel.INFO, "test_type", "first")
    con.log(LogLevel.WARNING, "test_type", "second")
    _ = con.execute("SELECT 1").fetchall()  # entries may be flushed later
    _ = con.execute("SET logging_sink = 'memory'")
    var msgs = seen().copy()
    assert_true(len(msgs) >= 2, String(len(msgs)))
    assert_equal(msgs[0], "INFO first")
    assert_equal(msgs[1], "WARNING second")


def test_file_system() raises:
    var con = Database().connect()
    var fs = con.file_system()
    var dir = String("/tmp/duckdb_mojo_v2_fs")
    _ = con.execute(String("COPY (SELECT 1) TO '", dir, "_probe.csv'"))  # makes sure /tmp is writable
    var path = dir + "_a.txt"
    fs.write_all(path, String("hello from mojo").as_bytes())
    assert_true(fs.exists(path))
    assert_false(fs.is_remote(path))
    var f = fs.open(path)
    assert_equal(f.size(), 15)
    assert_equal(String(from_utf8_lossy=Span(f.read(5))), "hello")
    assert_equal(f.tell(), 5)
    assert_equal(String(from_utf8_lossy=Span(f.read_at(4, 11))), "mojo")
    f.close()
    var g = fs.open(path, readable=False, append=True)
    _ = g.write(String("!").as_bytes())
    g.close()
    assert_equal(String(from_utf8_lossy=Span(fs.read_all(path))), "hello from mojo!")
    var matches = fs.glob(dir + "_*.txt")
    assert_true(len(matches) >= 1)
    # The same file through a function's file system.
    con.register(
        ScalarFunction.from_chunk_function[read_file]("read_text", [LogicalType(TypeId.VARCHAR)], LogicalType(TypeId.VARCHAR))
    )
    assert_equal(con.execute("SELECT read_text(?)", path).get[String]()[0], "hello from mojo!")
    fs.move(path, dir + "_b.txt")
    assert_false(fs.exists(path))
    assert_true(fs.remove(dir + "_b.txt"))
    assert_false(fs.remove(dir + "_b.txt"))
    assert_true(fs.remove(dir + "_probe.csv"))


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
