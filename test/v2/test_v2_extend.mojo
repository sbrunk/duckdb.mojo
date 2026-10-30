"""Tests for casts, replacement scans, macros, pragmas and custom options."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb.v2 import *


def parse_flag(s: String) raises -> Bool:
    if s == "yes":
        return True
    if s == "no":
        return False
    raise Error(String("not a flag: ", s))


def int_to_label(x: Int64) -> String:
    return String("#", x)


def resolve(name: String) raises -> Optional[Replacement]:
    if name.startswith("numbers_"):
        var n = atol(name[byte=8:])
        return Replacement.function("range", [Value(Int64(n))]).with_alias("numbers")
    if name == "answer":
        return Replacement.query("SELECT 42 AS a")
    return None


def show_tables_like(args: BindArgs) raises -> String:
    var prefix = args.constant[String](0).value()
    return String("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE ", lit(prefix + "%"), " ORDER BY 1")


def test_casts() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TYPE flag AS VARCHAR")
    con.register(
        CastFunction.from_function[parse_flag](source=LogicalType.parse("flag", con.factory()), target=LogicalType(TypeId.BOOLEAN))
    )
    var r = con.execute("SELECT CAST('yes'::flag AS BOOLEAN), CAST(NULL::flag AS BOOLEAN), TRY_CAST('maybe'::flag AS BOOLEAN)").fetchall()
    assert_true(r.get[Bool](col=0, row=0))
    assert_false(r.get_optional[Bool](col=1, row=0).__bool__())
    assert_false(r.get_optional[Bool](col=2, row=0).__bool__())
    var message = String()
    try:
        _ = con.execute("SELECT CAST('maybe'::flag AS BOOLEAN)").fetchall()
    except e:
        message = e.message
    assert_true("not a flag: maybe" in message, message)


def test_replacement_scans() raises:
    var con = Database().connect()
    con.register(ReplacementScan.from_function[resolve]())
    assert_equal(con.execute("SELECT count(*) FROM numbers_5000").get[Int64]()[0], 5000)
    assert_equal(con.execute("SELECT max(numbers.range) FROM numbers_10").get[Int64]()[0], 9)
    assert_equal(con.execute("SELECT a FROM answer").get[Int32]()[0], 42)
    var failed = False
    try:
        _ = con.execute("SELECT * FROM unknown_table").fetchall()
    except e:
        failed = True
        assert_true("unknown_table" in e.message, e.message)
    assert_true(failed)


def test_macros_on_a_connection() raises:
    var con = Database().connect()
    con.register(MacroFunction.scalar("add_n", ["x", "n"], "x + n").default("n", Int64(1)))
    con.register(MacroFunction.table_macro("first_n", ["n"], "SELECT * FROM range(n)"))
    assert_equal(con.execute("SELECT add_n(41)::BIGINT").get[Int64]()[0], 42)
    assert_equal(con.execute("SELECT add_n(40, n := 2)::BIGINT").get[Int64]()[0], 42)
    assert_equal(con.execute("SELECT count(*) FROM first_n(7)").get[Int64]()[0], 7)


def test_pragmas() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE apples (x INTEGER); CREATE TABLE apricots (x INTEGER); CREATE TABLE bananas (x INTEGER)")
    con.register(PragmaFunction.query[show_tables_like]("tables_like", [LogicalType(TypeId.VARCHAR)]))
    var names = con.execute("PRAGMA tables_like('ap')").get[String]()
    assert_equal(len(names), 2)
    assert_equal(names[1], "apricots")


def test_custom_options() raises:
    var con = Database().connect()
    con.register(CustomOption.of("my_level", Int64(3), "how verbose"))
    assert_equal(con.execute("SELECT current_setting('my_level')").get[Int64]()[0], 3)
    _ = con.execute("SET my_level = 7")
    assert_equal(con.execute("SELECT current_setting('my_level')").get[Int64]()[0], 7)
    var failed = False
    try:
        _ = con.execute("SET my_level = 'not a number'")
    except e:
        failed = True
    assert_true(failed)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
