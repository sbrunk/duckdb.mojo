from duckdb import *
from std.testing import assert_equal, assert_true, assert_raises
from std.testing.suite import TestSuite


def _count(con: Connection[ApiLevel.CLIENT], table: String) raises -> Int64:
    return con.execute(
        String("SELECT count(*) FROM ", table)
    ).fetchall().get[Int64](col=0, row=0)


def test_transaction_rollback() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (i INTEGER)")
    con.begin()
    _ = con.execute("INSERT INTO t VALUES (1)")
    con.rollback()
    assert_equal(_count(con, "t"), 0)


def test_transaction_commit() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (i INTEGER)")
    con.begin()
    _ = con.execute("INSERT INTO t VALUES (2)")
    con.commit()
    assert_equal(_count(con, "t"), 1)


def test_checkpoint() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (i INTEGER)")
    _ = con.execute("INSERT INTO t VALUES (1)")
    con.checkpoint()  # no-op for in-memory, but must not error
    assert_equal(_count(con, "t"), 1)


def test_cursor_shares_database() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (i INTEGER)")
    var cur = con.cursor()
    _ = cur.execute("INSERT INTO t VALUES (5)")
    # Auto-committed insert from the cursor is visible on the parent.
    assert_equal(_count(con, "t"), 1)


def test_close_is_idempotent() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("SELECT 1")
    con.close()
    # Destructor will disconnect again; the nulled handle makes it a no-op.
    assert_true(True)


def test_context_manager() raises:
    with DuckDB.connect(":memory:") as con:
        var r = con.execute("SELECT 42").fetchall()
        assert_equal(r.get[Int32](col=0, row=0), 42)


def test_load_unknown_extension_raises() raises:
    var con = DuckDB.connect(":memory:")
    with assert_raises():
        con.load_extension("definitely_not_a_real_extension_xyz")


def test_install_and_load_bundled_extension() raises:
    # `icu` is statically linked into libduckdb, so install/load succeed with
    # no network. Disabling autoinstall guarantees this never reaches out.
    var con = DuckDB.connect(":memory:")
    _ = con.execute("SET autoinstall_known_extensions=false")
    # Plain INSTALL of a statically-linked extension is a local no-op (no
    # network). FORCE INSTALL is deliberately not exercised here: it forces a
    # fresh download from the extension repository, which needs the network.
    con.install_extension("icu")
    con.load_extension("icu")
    var r = con.execute(
        "SELECT count(*) FROM duckdb_extensions()"
        " WHERE extension_name = 'icu' AND loaded"
    ).fetchall()
    assert_equal(r.get[Int64](col=0, row=0), Int64(1))


def test_interrupt_and_progress_idle() raises:
    var con = DuckDB.connect(":memory:")
    # No running query: interrupt is a no-op and progress is callable.
    con.interrupt()
    var p = con.query_progress()
    assert_true(p <= 100.0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
