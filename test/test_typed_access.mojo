from duckdb import *
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


def _int_type() -> LogicalType[True, MutUntrackedOrigin]:
    return LogicalType[True, MutUntrackedOrigin](DuckDBType.integer)


# ── Name-based column access ───────────────────────────────────────


def test_get_by_name_single() raises:
    var con = DuckDB.connect(":memory:")
    var r = con.execute(
        "SELECT 1 AS a, 'x' AS b, CAST(2.5 AS DOUBLE) AS c"
    ).fetchall()
    assert_equal(r.get[Int32]("a", row=0), 1)
    assert_equal(r.get[String]("b", row=0), "x")
    assert_equal(r.get[Float64]("c", row=0), 2.5)


def test_get_by_name_column() raises:
    var con = DuckDB.connect(":memory:")
    var r = con.execute(
        "SELECT * FROM (VALUES (1),(2),(3)) t(a)"
    ).fetchall()
    var col = r.get[Int32]("a")
    assert_equal(len(col), 3)
    assert_equal(col[2], 3)


def test_column_index() raises:
    var con = DuckDB.connect(":memory:")
    var r = con.execute("SELECT 1 AS a, 2 AS b").fetchall()
    assert_equal(r.column_index("a"), 0)
    assert_equal(r.column_index("b"), 1)


def test_get_by_unknown_name_raises() raises:
    var con = DuckDB.connect(":memory:")
    var r = con.execute("SELECT 1 AS a").fetchall()
    with assert_raises():
        _ = r.get[Int32]("nope", row=0)


def test_get_by_name_case_insensitive() raises:
    var con = DuckDB.connect(":memory:")
    var r = con.execute("SELECT 1 AS price").fetchall()
    # DuckDB resolves names case-insensitively; so does name-based access.
    assert_equal(r.get[Int32]("PRICE", row=0), 1)
    assert_equal(r.column_index("Price"), 0)


# ── Type builders ──────────────────────────────────────────────────


def test_list_type() raises:
    var lt = list_type(_int_type())
    assert_equal(lt.get_type_id(), DuckDBType.list)
    assert_equal(lt.list_type_child_type().get_type_id(), DuckDBType.integer)


def test_array_type() raises:
    var at = array_type(_int_type(), 4)
    assert_equal(at.get_type_id(), DuckDBType.array)
    assert_equal(Int(at.array_type_array_size()), 4)
    assert_equal(at.array_type_child_type().get_type_id(), DuckDBType.integer)


def test_map_type() raises:
    var mt = map_type(
        LogicalType[True, MutUntrackedOrigin](DuckDBType.varchar),
        LogicalType[True, MutUntrackedOrigin](DuckDBType.bigint),
    )
    assert_equal(mt.get_type_id(), DuckDBType.map)
    assert_equal(mt.map_type_key_type().get_type_id(), DuckDBType.varchar)
    assert_equal(mt.map_type_value_type().get_type_id(), DuckDBType.bigint)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
