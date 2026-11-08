"""Tests for relations (duckdb/relation.mojo)."""

from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite
from duckdb import *
from duckdb.expr import col, lit


def _con() raises -> Connection:
    var con = Database().connect()
    _ = con.execute(
        "CREATE TABLE trips AS SELECT range AS id, (range % 3)::VARCHAR AS payment, range * 1.5 AS fare FROM range(100)"
    )
    return con^


def test_compose_and_fetch() raises:
    var con = _con()
    var top = con.sql("FROM trips").filter("fare > 30").aggregate(
        "payment, count(*)::BIGINT AS n", group="payment"
    ).order("payment").limit(2)
    var r = top.fetchall()
    assert_equal(len(r), 2)
    assert_equal(r.get[String](col=0, row=0), "0")
    # Relations re-run.
    assert_equal(len(top.fetchall()), 2)


def test_table_select_and_expr() raises:
    var con = _con()
    var r = con.table("trips").filter(col("payment") + " = " + lit("1")).select("id", "fare").order("id").fetchall()
    assert_equal(r.column_count(), 2)
    assert_equal(r.get[Int64](col=0, row=0), 1)


def test_join_and_set_ops() raises:
    var con = _con()
    var a = con.sql("SELECT id FROM trips WHERE id < 5").set_alias("a")
    var b = con.sql("SELECT id, id * 10 AS ten FROM trips").set_alias("b")
    var joined = a.join(b, "a.id = b.id").fetchall()
    assert_equal(len(joined), 5)
    var u = con.sql("SELECT 1 AS x").union(con.sql("SELECT 1 AS x")).fetchall()
    assert_equal(len(u), 1)
    var ua = con.sql("SELECT 1 AS x").union_all(con.sql("SELECT 1 AS x")).fetchall()
    assert_equal(len(ua), 2)


def test_shape_columns_explain() raises:
    var con = _con()
    var rel = con.table("trips")
    var shape = rel.shape()
    assert_equal(shape[0], 100)
    assert_equal(shape[1], 3)
    assert_equal(rel.columns()[2].name, "fare")
    var plan = rel.explain()
    assert_true("Scan" in plan and "trips" in plan, plan)


def test_create_and_insert() raises:
    var con = _con()
    con.sql("SELECT * FROM trips WHERE id < 10").create("small")
    con.sql("SELECT * FROM trips WHERE id >= 90").insert_into("small")
    assert_equal(con.table("small").shape()[0], 20)


def test_aggregate_shorthands_and_render() raises:
    var con = _con()
    assert_true("4950" in con.table("trips").sum("id").render())


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
