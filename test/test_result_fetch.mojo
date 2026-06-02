from duckdb import *
from std.testing import assert_equal, assert_true, assert_false
from std.testing.suite import TestSuite


def test_fetchone() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT 'a', 1")
    var row = result.fetchone[String, Int32]()
    assert_true(Bool(row))
    assert_equal(row.value()[0], "a")
    assert_equal(row.value()[1], 1)
    # Exhausted now
    var none = result.fetchone[String, Int32]()
    assert_false(Bool(none))


def test_fetchmany() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT * FROM range(5) t(i)")
    var first = result.fetchmany[Int64](size=3)
    assert_equal(len(first), 3)
    assert_equal(first[0][0], 0)
    assert_equal(first[2][0], 2)
    var rest = result.fetchmany[Int64](size=3)
    assert_equal(len(rest), 2)
    assert_equal(rest[0][0], 3)
    assert_equal(rest[1][0], 4)
    var empty = result.fetchmany[Int64](size=3)
    assert_equal(len(empty), 0)


def test_columns_names() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT 1 AS a, 'x' AS b")
    var names = result.columns()
    assert_equal(len(names), 2)
    assert_equal(names[0], "a")
    assert_equal(names[1], "b")


def test_description() raises:
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT 1 AS a, 'x' AS b")
    var desc = result.description()
    assert_equal(len(desc), 2)
    assert_equal(desc[0].name, "a")
    assert_equal(desc[0].index, 0)
    assert_equal(desc[1].name, "b")


def test_materialized_metadata() raises:
    var con = DuckDB.connect(":memory:")
    var mat = con.execute("SELECT 1 AS a, 2 AS b").fetchall()
    var names = mat.columns()
    assert_equal(names[0], "a")
    assert_equal(names[1], "b")
    var desc = mat.description()
    assert_equal(len(desc), 2)
    var types = mat.types()
    assert_equal(len(types), 2)


def test_show_smoke() raises:
    var con = DuckDB.connect(":memory:")
    var mat = con.execute(
        "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'yy' ORDER BY a"
    ).fetchall()
    var table = mat._render_table(max_rows=40, max_col_width=32)
    assert_true("a" in table)
    assert_true("b" in table)
    assert_true("yy" in table)
    # Header carries types
    assert_true("int32" in table)
    assert_true("varchar" in table)
    # Does not raise:
    mat.show()


def test_show_matches_cli_basic() raises:
    var con = DuckDB.connect(":memory:")
    var mat = con.execute(
        "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'yy' ORDER BY a"
    ).fetchall()
    var expected = String(
        "┌───────┬─────────┐\n"
        "│   a   │    b    │\n"
        "│ int32 │ varchar │\n"
        "├───────┼─────────┤\n"
        "│     1 │ x       │\n"
        "│     2 │ yy      │\n"
        "└───────┴─────────┘"
    )
    assert_equal(mat._render_table(max_rows=40, max_col_width=32), expected)


def test_show_truncation_footer() raises:
    var con = DuckDB.connect(":memory:")
    var mat = con.execute("SELECT i FROM range(60) t(i)").fetchall()
    var table = mat._render_table(max_rows=40, max_col_width=32)
    # 20 top + 3 dot rows + 20 bottom shown; footer reports totals.
    assert_true("·" in table)
    assert_true("60 rows" in table)
    assert_true("(40 shown)" in table)
    assert_true("19" in table)  # last top row
    assert_true("40" in table)  # first bottom row
    assert_false("29" in table)  # hidden middle row


def test_show_nulls_and_bool_multichunk() raises:
    var con = DuckDB.connect(":memory:")
    # UNION ALL yields two 1-row chunks — exercises cross-chunk row access.
    var mat = con.execute(
        "SELECT NULL::INTEGER AS i, true AS b UNION ALL SELECT 5, false"
    ).fetchall()
    var table = mat._render_table(max_rows=40, max_col_width=32)
    assert_true("NULL" in table)
    assert_true("true" in table)
    assert_true("false" in table)
    assert_false("True" in table)  # lowercase like the CLI


def test_show_no_truncation_within_threshold() raises:
    var con = DuckDB.connect(":memory:")
    # 43 == max_rows + 3 → DuckDB shows all, no footer / dots.
    var mat = con.execute("SELECT i FROM range(43) t(i)").fetchall()
    var table = mat._render_table(max_rows=40, max_col_width=32)
    assert_false("·" in table)
    assert_false("shown" in table)
    assert_true("42" in table)


def test_show_consuming() raises:
    var con = DuckDB.connect(":memory:")
    # Result.show consumes self; just assert it doesn't raise.
    con.execute("SELECT 42 AS answer").show()


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
