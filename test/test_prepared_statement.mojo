from duckdb import *
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


def test_execute_positional_qmark() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT ? + ?", Int32(40), Int32(2)).fetchall()
    assert_equal(result.get[Int32](col=0, row=0), 42)


def test_execute_positional_dollar() raises:
    var con = DuckDB.connect(":memory:")
    # $1 reused twice plus $2
    var result = con.execute(
        "SELECT $1, $1, $2", String("duck"), String("goose")
    ).fetchall()
    assert_equal(result.get[String](col=0, row=0), "duck")
    assert_equal(result.get[String](col=1, row=0), "duck")
    assert_equal(result.get[String](col=2, row=0), "goose")


def test_execute_named() raises:
    var con = DuckDB.connect(":memory:")
    var params: Dict[String, Int32] = {"x": Int32(40), "y": Int32(2)}
    var result = con.execute_named("SELECT $x + $y", params).fetchall()
    assert_equal(result.get[Int32](col=0, row=0), 42)


def test_bind_optional_null() raises:
    var con = DuckDB.connect(":memory:")
    var some: Optional[Int32] = Int32(7)
    var none: Optional[Int32] = None
    var r1 = con.execute("SELECT ?", some).fetchall()
    assert_equal(r1.get[Int32](col=0, row=0), 7)
    var r2 = con.execute("SELECT ? IS NULL", none).fetchall()
    assert_true(r2.get[Bool](col=0, row=0))


def test_prepare_and_reuse() raises:
    var con = DuckDB.connect(":memory:")
    var stmt = con.prepare("SELECT $1 + $2")
    assert_equal(stmt.parameter_count(), 2)
    stmt.bind(1, Int32(40))
    stmt.bind(2, Int32(2))
    var r = stmt.execute().fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 42)


def test_executemany_insert() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    var rows: List[Tuple[Int32, String]] = [
        (Int32(1), String("a")),
        (Int32(2), String("b")),
        (Int32(3), String("c")),
    ]
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)
    var count = con.execute("SELECT count(*) FROM t").fetchall()
    assert_equal(count.get[Int64](col=0, row=0), 3)


def test_execute_no_params_unchanged() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT 42").fetchall()
    assert_equal(result.get[Int32](col=0, row=0), 42)


def test_prepare_error_raises() raises:
    var con = DuckDB.connect(":memory:")
    with assert_raises():
        _ = con.prepare("SELECT * FROM nonexistent_table_xyz")


def test_unknown_named_parameter_raises() raises:
    var con = DuckDB.connect(":memory:")
    var stmt = con.prepare("SELECT $x")
    with assert_raises():
        _ = stmt.parameter_index("nonexistent")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
