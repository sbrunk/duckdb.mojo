import duckdb
from duckdb import connect, sql, execute, read_csv
from duckdb._sql_util import _sql_quote
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


def test_connect_default() raises:
    var con = connect()
    var r = con.execute("SELECT 42").fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 42)


def test_connect_keyword_database() raises:
    var con = connect(database=":memory:")
    var r = con.execute("SELECT 1").fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 1)


def test_default_connection_persists() raises:
    # sql() and execute() must share one process-wide default connection.
    _ = execute("CREATE TABLE IF NOT EXISTS mod_t (i INTEGER)")
    _ = execute("DELETE FROM mod_t")
    _ = execute("INSERT INTO mod_t VALUES (1), (2), (3)")
    var r = sql("SELECT count(*) FROM mod_t").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 3)


def test_con_sql_alias() raises:
    var con = connect()
    var r = con.sql("SELECT 7").fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 7)


def test_sql_quote() raises:
    assert_equal(_sql_quote("plain"), "'plain'")
    assert_equal(_sql_quote("a'b"), "'a''b'")
    assert_equal(_sql_quote("o''reilly"), "'o''''reilly'")


comptime _CSV_FIXTURE = "test/data/train_services.csv"
"""Test asset, resolved relative to the repo root (the test working dir)."""


def test_read_csv() raises:
    var con = connect()
    var r = con.read_csv(_CSV_FIXTURE).fetchall()
    assert_true(len(r) > 0)
    var names = r.columns()
    assert_equal(names[0], "service_id")


def test_read_csv_module_level() raises:
    var r = read_csv(_CSV_FIXTURE).fetchall()
    assert_true(len(r) > 0)


def _make_ro_db(path: String) raises:
    # Separate function so the writable connection is dropped (file unlocked)
    # before we reopen read-only.
    var con = connect(path)
    _ = con.execute("CREATE TABLE IF NOT EXISTS t (i INTEGER)")
    _ = con.execute("INSERT INTO t VALUES (1)")


def test_read_only_rejects_writes() raises:
    var path = String("/tmp/test_duckdb_mojo_readonly.db")
    _make_ro_db(path)
    var con = connect(path, read_only=True)
    # Reads work
    var r = con.execute("SELECT count(*) FROM t").fetchall()
    assert_true(r.get[Int64](col=0, row=0) >= 1)
    # Writes are rejected
    with assert_raises():
        _ = con.execute("INSERT INTO t VALUES (2)")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
