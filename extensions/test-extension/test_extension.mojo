"""Integration tests for Mojo DuckDB extensions.

They load the test extensions (built by build.sh) into DuckDB and check:
- a V2 extension written against the raw V2 C API (`mojo_v2`), including a
  failing init function and the refused stable target,
- a V2 extension written with the high-level API (`mojo_v2_api`): scalar,
  aggregate and table functions, signatures, casts, macros, replacement scans
  and options.
"""

import duckdb
from duckdb import Connection
from std.testing import *
from std.testing.suite import TestSuite


def _open_unsigned() raises -> Connection:
    """An in-memory connection with allow_unsigned_extensions enabled."""
    return duckdb.connect(config={"allow_unsigned_extensions": "true"})


comptime V2_EXT_PATH = "extensions/test-extension/build/mojo_v2.duckdb_extension"
comptime V2_FAIL_EXT_PATH = "extensions/test-extension/build/mojo_v2_fail.duckdb_extension"
comptime V2_STABLE_EXT_PATH = "extensions/test-extension/build/mojo_v2_stable.duckdb_extension"


def _connect_v2() raises -> Connection:
    var conn = _open_unsigned()
    _ = conn.execute("LOAD '" + V2_EXT_PATH + "'")
    return conn^


def test_v2_scalar_function() raises:
    """A scalar function registered through the V2 API, with constant arguments and NULLs."""
    var conn = _connect_v2()
    var rows = conn.execute(
        "SELECT mojo_v2_add(40, 2) AS r, mojo_v2_add(x, 10) AS s FROM (VALUES (1, 1), (2, NULL), (3, 3)) t(k, x) ORDER BY k"
    ).fetchall()
    assert_equal(rows.get[Int64](col=0, row=0), 42)
    assert_equal(rows.get[Optional[Int64]](col=1, row=0), Optional[Int64](11))
    assert_true(rows.get[Optional[Int64]](col=1, row=1) is None)
    assert_equal(rows.get[Optional[Int64]](col=1, row=2), Optional[Int64](13))


def test_v2_vector_kinds() raises:
    """Sequence vectors (range) are flattened, table columns read as flat vectors."""
    var conn = _connect_v2()
    var seq = conn.execute("SELECT sum(mojo_v2_add(i, i))::BIGINT FROM range(100000) r(i)").fetchall()
    assert_equal(seq.get[Int64](col=0, row=0), 9999900000)
    _ = conn.execute("CREATE TABLE t AS SELECT (i % 3)::BIGINT AS c FROM range(5000) r(i)")
    var tbl = conn.execute("SELECT sum(mojo_v2_add(c, 1))::BIGINT FROM t").fetchall()
    assert_equal(tbl.get[Int64](col=0, row=0), 9999)


def test_v2_named_arguments() raises:
    """V2 signatures have named parameters."""
    var conn = _connect_v2()
    var rows = conn.execute("SELECT mojo_v2_add(b := 2, a := 40)").fetchall()
    assert_equal(rows.get[Int64](col=0, row=0), 42)


comptime V2_API_EXT_PATH = "extensions/test-extension/build/mojo_v2_api.duckdb_extension"


def _connect_v2_api() raises -> Connection:
    var conn = _open_unsigned()
    _ = conn.execute("LOAD '" + V2_API_EXT_PATH + "'")
    return conn^


def test_v2_api_extension_functions() raises:
    """Functions registered through the V2 high-level API from inside an extension."""
    var conn = _connect_v2_api()
    var r = conn.execute(
        "SELECT v2api_add(40, 2), v2api_greet('db'), v2api_total([1, 2, 3]), v2api_next_day(DATE '2024-02-28')::VARCHAR,"
        " v2api_year(DATE '1999-05-01'), v2api_offset(1, 41), v2api_negate(5), mojo_sqrt(16.0)"
    ).fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 42)
    assert_equal(r.get[String](col=1, row=0), "hello db, from a V2 extension")
    assert_equal(r.get[Int64](col=2, row=0), 6)
    assert_equal(r.get[String](col=3, row=0), "2024-02-29")
    assert_equal(r.get[Int64](col=4, row=0), 1999)
    assert_equal(r.get[Int64](col=5, row=0), 42)
    assert_equal(r.get[Int64](col=6, row=0), -5)
    assert_equal(r.get[Float64](col=7, row=0), 4.0)


def test_v2_api_extension_many_rows_and_nulls() raises:
    var conn = _connect_v2_api()
    var r = conn.execute(
        "SELECT sum(v2api_add(i, 1))::BIGINT, count(v2api_negate(CASE WHEN i % 2 = 0 THEN i END))::BIGINT,"
        " sum(v2api_total(range(i % 10)))::BIGINT FROM range(100000) t(i)"
    ).fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 5000050000)
    assert_equal(r.get[Int64](col=1, row=0), 50000)
    assert_equal(r.get[Int64](col=2, row=0), 1200000)


def test_v2_api_extension_aggregates_and_tables() raises:
    var conn = _connect_v2_api()
    var r = conn.execute(
        "SELECT v2api_longest(s), v2api_sum(x) FROM (VALUES ('a', 1.5), ('a longer string, over twelve', 2.5), ('ab', 3.0)) t(s, x)"
    ).fetchall()
    assert_equal(r.get[String](col=0, row=0), "a longer string, over twelve")
    assert_equal(r.get[Float64](col=1, row=0), 7.0)
    var t = conn.execute("SELECT count(*), sum(value)::BIGINT, max(value) FROM v2api_countdown(5000)").fetchall()
    assert_equal(t.get[Int64](col=0, row=0), 5000)
    assert_equal(t.get[Int64](col=1, row=0), 12502500)
    assert_equal(t.get[Int64](col=2, row=0), 5000)


def test_v2_api_extension_signatures_and_extras() raises:
    var conn = _connect_v2_api()
    var r = conn.execute(
        "SELECT v2api_scale(3.0), v2api_scale(factor := 4.0, x := 3.0), v2api_plus(1)::BIGINT,"
        " CAST(7::UBIGINT AS VARCHAR), current_setting('v2api_level')"
    ).fetchall()
    assert_equal(r.get[Float64](col=0, row=0), 6.0)
    assert_equal(r.get[Float64](col=1, row=0), 12.0)
    assert_equal(r.get[Int64](col=2, row=0), 11)
    assert_equal(r.get[String](col=3, row=0), "#7")
    assert_equal(r.get[Int64](col=4, row=0), 5)
    var t = conn.execute("SELECT count(*) FROM v2api_numbers_123").fetchall()
    assert_equal(t.get[Int64](col=0, row=0), 123)


def test_v2_init_error_is_reported() raises:
    """An error raised in the init function fails the load with its message."""
    var conn = _open_unsigned()
    with assert_raises(contains="intentional failure in the init function"):
        _ = conn.execute("LOAD '" + V2_FAIL_EXT_PATH + "'")


def test_v2_stable_target_refused() raises:
    """DuckDB 2.0.0 has no stable V2 functions, so it refuses stable V2 extensions.

    Once a DuckDB release stabilizes part of the V2 API, this extension should
    load there, and this test needs to change.
    """
    var conn = _open_unsigned()
    with assert_raises(contains="no stable surface"):
        _ = conn.execute("LOAD '" + V2_STABLE_EXT_PATH + "'")


# ===--------------------------------------------------------------------===#
# Main
# ===--------------------------------------------------------------------===#


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
