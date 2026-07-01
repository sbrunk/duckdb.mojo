from duckdb import *
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


def _write_csv(con: Connection[ApiLevel.CLIENT], path: String) raises:
    _ = con.execute(
        "CREATE OR REPLACE TABLE src AS SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) t(i,s)"
    )
    _ = con.execute(
        String("COPY src TO ", lit(path), " (FORMAT csv, HEADER)")
    )


def test_read_csv_returns_chainable_relation() raises:
    var con = DuckDB.connect(":memory:")
    var path = String("/tmp/duckdb_mojo_readers.csv")
    _write_csv(con, path)
    # A reader is a lazy relation: chain transforms then a terminal.
    var r = con.read_csv(path).filter("i >= 2").order("i").fetchall()
    assert_equal(len(r), 2)
    # read_csv's sniffer types integer columns as BIGINT.
    assert_equal(r.get[Int64](col=0, row=0), 2)
    var cols = con.read_csv(path).columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "i")


def test_read_csv_with_options() raises:
    var con = DuckDB.connect(":memory:")
    var path = String("/tmp/duckdb_mojo_readers_opt.csv")
    _write_csv(con, path)
    var opts = Dict[String, String]()
    opts["header"] = "true"
    var n = con.read_csv(path, opts).count().fetchone[Int64]()
    assert_true(Bool(n))
    assert_equal(n.value()[0], 3)


def test_module_read_csv_relation() raises:
    var con = DuckDB.connect(":memory:")
    var path = String("/tmp/duckdb_mojo_readers_mod.csv")
    _write_csv(con, path)
    # Module-level reader uses the default connection and is also lazy.
    var r = read_csv(path).fetchall()
    assert_equal(len(r), 3)


def test_remove_function() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE MACRO my_double(x) AS x * 2")
    assert_equal(
        con.execute("SELECT my_double(21)").fetchall().get[Int32](col=0, row=0),
        42,
    )
    con.remove_function("my_double")
    with assert_raises():
        _ = con.execute("SELECT my_double(1)")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
