from duckdb import *
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


@fieldwise_init
struct Row2(Writable, Copyable, Movable):
    var k: String
    var v: Int32


def _con() raises -> Connection[ApiLevel.CLIENT]:
    var con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES ('a',1),('a',2),('b',3)) tbl(k,v)"
    )
    return con^


# ── Golden SQL (composition is pure string building) ───────────────


def test_golden_table() raises:
    var con = _con()
    assert_equal(con.table("t").sql_query(), 'SELECT * FROM "t"')


def test_golden_filter_nests() raises:
    var con = _con()
    assert_equal(
        con.table("t").filter("v > 1").sql_query(),
        'SELECT * FROM (SELECT * FROM "t") WHERE v > 1',
    )


def test_golden_select() raises:
    var con = _con()
    assert_equal(
        con.table("t").select("k", "v").sql_query(),
        'SELECT k, v FROM (SELECT * FROM "t")',
    )


def test_golden_order_limit_offset() raises:
    var con = _con()
    assert_equal(
        con.sql("SELECT * FROM t").order("v").limit(2, 1).sql_query(),
        "SELECT * FROM (SELECT * FROM (SELECT * FROM t) ORDER BY v) LIMIT 2"
        " OFFSET 1",
    )


def test_golden_sum_grouped() raises:
    var con = _con()
    assert_equal(
        con.table("t").sum("v", group="k").sql_query(),
        'SELECT k, sum(v) FROM (SELECT * FROM "t") GROUP BY k',
    )


def test_golden_union_parenthesized() raises:
    var con = _con()
    assert_equal(
        con.sql("SELECT 1").union(con.sql("SELECT 2")).sql_query(),
        "(SELECT 1) UNION (SELECT 2)",
    )


def test_golden_join_aliases() raises:
    var con = _con()
    var j = con.table("t").set_alias("l").join(
        con.sql("SELECT 1 AS k").set_alias("r"), on="l.k = r.k"
    )
    assert_equal(
        j.sql_query(),
        'SELECT * FROM (SELECT * FROM "t") AS "l" inner JOIN (SELECT 1 AS k)'
        ' AS "r" ON l.k = r.k',
    )


# ── Behavior ───────────────────────────────────────────────────────


def test_filter_fetchall() raises:
    var con = _con()
    var r = con.table("t").filter("v >= 2").fetchall()
    assert_equal(len(r), 2)


def test_get_struct() raises:
    var con = _con()
    var rows = con.table("t").order("k, v").get[Row2]()
    assert_equal(len(rows), 3)
    assert_equal(rows[0].k, "a")
    assert_equal(rows[0].v, 1)
    assert_equal(rows[2].k, "b")
    assert_equal(rows[2].v, 3)


def test_count_star() raises:
    var con = _con()
    var n = con.table("t").count().fetchone[Int64]()
    assert_true(Bool(n))
    assert_equal(n.value()[0], 3)


def test_aggregate_grouped() raises:
    var con = _con()
    var r = con.table("t").aggregate("k, sum(v) AS s", group="k").order(
        "k"
    ).fetchall()
    assert_equal(len(r), 2)
    assert_equal(r.get[String](col=0, row=0), "a")
    assert_equal(r.get[Int128](col=1, row=0), 3)  # sum of int32 is HUGEINT


def test_distinct() raises:
    var con = _con()
    var r = con.table("t").select("k").distinct().fetchall()
    assert_equal(len(r), 2)


def test_set_ops() raises:
    var con = _con()
    var u = con.sql("SELECT 1 AS x").union(con.sql("SELECT 2 AS x")).fetchall()
    assert_equal(len(u), 2)
    var i = con.sql("SELECT 1 AS x").intersect(
        con.sql("SELECT 1 AS x")
    ).fetchall()
    assert_equal(len(i), 1)


def test_columns() raises:
    var con = _con()
    var cols = con.table("t").columns()
    assert_equal(len(cols), 2)
    assert_equal(cols[0], "k")
    assert_equal(cols[1], "v")


def test_to_table_and_create_view() raises:
    var con = _con()
    con.table("t").filter("k = 'a'").create("t_a")
    var n = con.table("t_a").count().fetchone[Int64]()
    assert_equal(n.value()[0], 2)
    var v = con.table("t").create_view("v_t")
    assert_equal(len(v.fetchall()), 3)


def test_explain_nonempty() raises:
    var con = _con()
    var plan = con.table("t").explain()
    assert_true(plan.byte_length() > 0)


# ── Lifetime: a relation kept in a var must keep its connection alive ──


def test_named_relation_outlives_creation_use() raises:
    # Regression: Relation borrows the connection via an origin-tracked
    # pointer, so `con` is not destroyed early even when its last *direct*
    # use is the relation constructor.
    var con = _con()
    var rel = con.table("t").filter("v >= 2")
    # Intervening unrelated work; `con` must stay alive for `rel`.
    _ = con.execute("SELECT 1")
    var r = rel.fetchall()
    assert_equal(len(r), 2)


def test_relation_rerunnable() raises:
    var con = _con()
    var rel = con.table("t")
    assert_equal(len(rel.fetchall()), 3)
    assert_equal(len(rel.fetchall()), 3)  # re-executes cleanly


def test_module_level_sql() raises:
    _ = execute("CREATE TABLE IF NOT EXISTS rel_mod (i INTEGER)")
    _ = execute("DELETE FROM rel_mod")
    _ = execute("INSERT INTO rel_mod VALUES (10),(20)")
    var r = sql("SELECT * FROM rel_mod").order("i").fetchall()
    assert_equal(len(r), 2)
    assert_equal(r.get[Int32](col=0, row=0), 10)


def test_golden_qualified_table() raises:
    var con = _con()
    # Schema-qualified names split into quoted parts, not one literal identifier.
    assert_equal(
        con.table("myschema.t").sql_query(), 'SELECT * FROM "myschema"."t"'
    )


def test_negative_interval_rendering() raises:
    var con = _con()
    # Negative-month intervals decompose with truncation toward zero.
    var rendered = String(con.sql("SELECT INTERVAL '-14 months' AS i"))
    assert_true("-1 year -2 months" in rendered)


def test_relation_shape() raises:
    var con = _con()
    var s = con.table("t").shape()
    assert_equal(s[0], 3)  # rows
    assert_equal(s[1], 2)  # columns


def test_relation_types() raises:
    var con = _con()
    var ts = con.table("t").types()
    assert_equal(len(ts), 2)
    assert_equal(ts[0].get_type_id(), DuckDBType.varchar)
    assert_equal(ts[1].get_type_id(), DuckDBType.integer)


def test_materialized_shape() raises:
    var con = _con()
    var s = con.execute("SELECT * FROM t").fetchall().shape()
    assert_equal(s[0], 3)
    assert_equal(s[1], 2)


def test_rowcount_dml() raises:
    var con = _con()
    var r = con.execute("INSERT INTO t VALUES ('d', 4), ('e', 5)")
    assert_equal(r.rowcount(), 2)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
