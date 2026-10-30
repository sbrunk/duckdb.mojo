"""Tests for custom COPY formats."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb.v2 import *


struct LinesWriter(CopyToFormat):
    """Writes each row as one line: the columns as text, joined by SEPARATOR."""

    comptime File = LinesFile
    var separator: String
    var columns: Int

    def __init__(out self, separator: String, columns: Int):
        self.separator = separator
        self.columns = columns

    @staticmethod
    def bind(info: CopyInfo) raises -> Self:
        var sep = info.option[String]("separator")
        return Self(sep.value() if sep else String(","), info.column_count())

    def open(self, path: String) raises -> LinesFile:
        return LinesFile(path)

    def write(self, mut file: LinesFile, rows: Chunk) raises:
        for r in range(len(rows)):
            var parts = List[String]()
            for c in range(self.columns):
                var v = rows.value(col=c, row=r)
                parts.append(String("") if v.is_null() else v.to_string())
            file.text += self.separator.join(parts) + "\n"

    def close(self, mut file: LinesFile) raises:
        with open(file.path, "w") as f:
            f.write(file.text)


@fieldwise_init
struct LinesFile(Movable):
    var path: String
    var text: String

    def __init__(out self, path: String):
        self.path = path
        self.text = String()


struct LinesReader(CopyFromFormat):
    comptime State = LinesPos
    var lines: List[String]
    var separator: String
    var columns: Int

    def __init__(out self, var lines: List[String], separator: String, columns: Int):
        self.lines = lines^
        self.separator = separator
        self.columns = columns

    @staticmethod
    def bind(info: CopyInfo) raises -> Self:
        var sep = info.option[String]("separator")
        var data = info.file_system().read_all(info.path)
        var text = String(from_utf8_lossy=Span(data))
        var lines = List[String]()
        for line in text.split("\n"):
            if line:
                lines.append(String(line))
        return Self(lines^, sep.value() if sep else String(","), info.column_count())

    def init(self) raises -> LinesPos:
        return LinesPos(0)

    def scan(self, mut state: LinesPos, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < len(self.lines):
            var parts = self.lines[state.next].split(self.separator)
            for c in range(self.columns):
                if c < len(parts) and parts[c]:
                    out.write(rows, c, String(parts[c]))  # DuckDB casts to the column type
                else:
                    out.write_null(rows, c)
            state.next += 1
            rows += 1
        return rows


@fieldwise_init
struct LinesPos(Movable):
    var next: Int


def test_copy_to_and_from() raises:
    var con = Database().connect()
    con.register(CopyFunction("lines", extension="txt").writes[LinesWriter]().reads[LinesReader]())
    _ = con.execute("CREATE TABLE t AS SELECT i::INTEGER AS id, 'name ' || i AS name FROM range(5000) t(i)")
    var path = String("/tmp/duckdb_mojo_v2_copy.txt")
    assert_equal(con.execute(String("COPY (SELECT * FROM t ORDER BY id) TO '", path, "' (FORMAT lines, SEPARATOR '|')")).rows_changed(), 5000)
    var text = open(path, "r").read()
    assert_true(text.startswith("0|name 0\n1|name 1\n"), String(text[byte=0:40]))
    _ = con.execute("CREATE TABLE back (id INTEGER, name VARCHAR)")
    assert_equal(con.execute(String("COPY back FROM '", path, "' (FORMAT lines, SEPARATOR '|')")).rows_changed(), 5000)
    var r = con.execute("SELECT count(*), sum(id), max(name) FROM back").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 5000)
    assert_equal(r.get[Int128](col=1, row=0), 12497500)
    assert_equal(r.get[String](col=2, row=0), "name 999")
    # Default separator, and NULLs.
    _ = con.execute(String("COPY (SELECT 1 AS a, NULL::VARCHAR AS b) TO '", path, "' (FORMAT lines)"))
    assert_equal(open(path, "r").read(), "1,\n")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
