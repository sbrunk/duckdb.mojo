"""Tests for queries and results: execution, parameters, prepared
statements, streaming and materialized results, rendering."""

from std.testing import assert_equal, assert_false, assert_true
from std.testing.suite import TestSuite
from duckdb import *


def _db() raises -> Connection:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    _ = con.execute("INSERT INTO users VALUES (1, 'Ada', 36), (2, 'Grace', 45), (3, 'Linus', NULL)")
    return con^


# ── Statements without rows ──────────────────────────────────────


def test_statements_without_rows_run_immediately() raises:
    var con = Database().connect()
    var r = con.execute("CREATE TABLE t (x INTEGER)")
    assert_true(r.statement_type() == StatementType.CREATE)
    var ins = con.execute("INSERT INTO t SELECT range FROM range(5)")
    assert_true(ins.result_type() == ResultType.CHANGED_ROWS)
    assert_equal(ins.rows_changed(), 5)
    # The INSERT result is still alive, but the connection is free.
    assert_equal(con.execute("UPDATE t SET x = x + 1 WHERE x < 2").rows_changed(), 2)
    assert_equal(con.execute("DELETE FROM t").rows_changed(), 5)
    _ = ins^


# ── Queries ──────────────────────────────────────────────────────


def test_fetchall_and_columns() raises:
    var con = _db()
    var r = con.execute("SELECT id, name, age FROM users ORDER BY id").fetchall()
    assert_equal(len(r), 3)
    assert_equal(r.column_count(), 3)
    var cols = r.columns()
    assert_equal(cols[1].name, "name")
    assert_equal(String(cols[1].type), "VARCHAR")
    assert_equal(r.get[Int32](col=0, row=1), 2)
    assert_equal(r.get[String](col=1, row=2), "Linus")
    assert_true(r.get_optional[Int32](col=2, row=2) is None)
    assert_equal(r.column_index("NAME"), 1)
    var names = r.get[String](col=1)
    assert_equal(len(names), 3)
    assert_equal(names[0], "Ada")
    var ages = r.get_optional[Int32](col=2)
    assert_equal(ages[1].value(), 45)
    assert_true(ages[2] is None)


def test_rows() raises:
    var con = _db()
    var r = con.execute("SELECT name, age FROM users ORDER BY id").fetchall()
    var seen = List[String]()
    for row in r.rows():
        seen.append(row.get[String](0))
    assert_equal(len(seen), 3)
    assert_equal(seen[2], "Linus")
    assert_true(r.row(2).get_optional[Int32](1) is None)


def test_streaming_chunks() raises:
    var con = Database().connect()
    var total = 0
    var chunks = 0
    for chunk in con.execute("SELECT range AS i FROM range(100000)").chunks():
        var col = chunk.column[Int64](0)
        for i in range(len(col)):
            total += Int(col[i])
        chunks += 1
    assert_true(chunks > 1)
    assert_equal(total, 99999 * 100000 // 2)


def _sum_chunks(sql: String) raises DuckDBError -> Int:
    var con = Database().connect()
    var total = 0
    for chunk in con.execute(sql).chunks():
        var col = chunk.column[Int64](0)
        for i in range(len(col)):
            total += Int(col[i])
    return total


def test_chunks_for_loop_raises() raises:
    """A DuckDB error while reading propagates out of a `for` loop over `chunks()`.

    Relies on the `__has_next__` loop protocol (see `ChunkIter`).
    """
    assert_equal(_sum_chunks("SELECT range FROM range(10)"), 45)
    var error = Optional[DuckDBError]()
    try:
        _ = _sum_chunks("SELECT 1 // (range - 5000) FROM range(10000)")
    except e:
        error = e^
    assert_true(error, "the error was swallowed by the for loop")
    assert_true("Division by zero" in error.value().message, error.value().message)


def test_try_chunks() raises:
    var con = Database().connect()
    var rows = 0
    for item in con.execute("SELECT range FROM range(5000)").try_chunks():
        assert_false(item.is_error())
        rows += len(item.get())
    var owned = List[Chunk]()
    for var item in con.execute("SELECT range FROM range(5000)").try_chunks():
        owned.append(item.take())
    assert_true(len(owned) > 1)
    assert_equal(rows, 5000)
    # An error arrives as the last element.
    var errors = 0
    var chunks = 0
    for item in con.execute("SELECT 1 // (range - 5000) FROM range(10000)").try_chunks():
        if item.is_error():
            errors += 1
            assert_true("Division by zero" in item.error().value().message)
        else:
            chunks += 1
    assert_equal(errors, 1)
    # The standard Iterator works with adapters.
    var n = 0
    for pair in enumerate(con.execute("SELECT range FROM range(3000)").try_chunks()):
        n = pair[0] + 1
    assert_true(n > 1)


def test_fetch_chunk_loop() raises:
    var con = Database().connect()
    var r = con.execute("SELECT range AS i FROM range(5000)")
    var rows = 0
    while True:
        var chunk = r.fetch_chunk()
        if not chunk:
            break
        rows += len(chunk.value())
    assert_equal(rows, 5000)


def test_open_result_blocks_connection() raises:
    """A result that is still being read keeps the connection busy."""
    var con = Database().connect()
    var r = con.execute("SELECT range FROM range(100000)")
    _ = r.fetch_chunk()
    var error = Optional[DuckDBError]()
    try:
        _ = con.execute("SELECT 1")
    except e:
        error = e^
    assert_true(error, "expected RESOURCE_IN_USE")
    assert_true(error.value().code == ErrorCode.RESOURCE_IN_USE, String(error.value()))
    _ = r^  # destroying the result frees the connection
    assert_equal(con.execute("SELECT 1::BIGINT").fetchall().get[Int64](col=0, row=0), 1)


def test_unused_result_frees_connection() raises:
    """Mojo destroys a result after its last use, which frees the connection."""
    var con = Database().connect()
    var r = con.execute("SELECT range FROM range(100000)")
    _ = r.fetch_chunk()  # last use of `r`
    assert_equal(con.execute("SELECT 2::BIGINT").fetchall().get[Int64](col=0, row=0), 2)


def test_runtime_error_surfaces_while_reading() raises:
    var con = Database().connect()
    var error = Optional[DuckDBError]()
    try:
        _ = con.execute("SELECT 1 // (range - 5) FROM range(10)").fetchall()
    except e:
        error = e^
    assert_true(error, "expected a division by zero error")
    assert_true("Division by zero" in error.value().message, error.value().message)


# ── Scripts ──────────────────────────────────────────────────────


def test_multi_statement_script() raises:
    var con = Database().connect()
    var r = con.execute(
        "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2); SELECT 99 AS ignored; SELECT sum(x)::BIGINT FROM t"
    ).fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 3)


def test_empty_sql_raises() raises:
    var con = Database().connect()
    var raised = False
    try:
        _ = con.execute("   ")
    except:
        raised = True
    assert_true(raised)


# ── Parameters ───────────────────────────────────────────────────


def test_positional_parameters() raises:
    var con = _db()
    var r = con.execute("SELECT name FROM users WHERE id = ? AND name <> ?", Int32(2), String("x")).fetchall()
    assert_equal(r.get[String](col=0, row=0), "Grace")
    _ = con.execute("INSERT INTO users VALUES ($1, $2, $3)", Int32(4), String("Barbara"), Int32(81))
    assert_equal(con.execute("SELECT age FROM users WHERE id = 4").fetchall().get[Int32](col=0, row=0), 81)


def test_named_parameters() raises:
    var con = _db()
    var p = Params()
    p.set("min_age", Int32(40))
    p.set("NAME", String("Grace"))
    var r = con.execute("SELECT id FROM users WHERE age >= $min_age AND name = $name", params=p^).fetchall()
    assert_equal(len(r), 1)
    assert_equal(r.get[Int32](col=0, row=0), 2)


def test_null_parameter() raises:
    var con = Database().connect()
    var p = Params()
    p.set_value("x", Value.null(LogicalType(TypeId.INTEGER)))
    var r = con.execute("SELECT $x IS NULL", params=p^).fetchall()
    assert_true(r.get[Bool](col=0, row=0))


def test_parameters_need_one_statement() raises:
    var con = Database().connect()
    var raised = False
    try:
        _ = con.execute("SELECT ?; SELECT 2", Int32(1))
    except e:
        raised = True
        assert_true("single statement" in e.message, e.message)
    assert_true(raised)


# ── Prepared statements and describe ─────────────────────────────


def test_prepared_statement() raises:
    var con = _db()
    var stmt = con.prepare("SELECT name FROM users WHERE id = $1")
    assert_equal(stmt.execute(Int32(1)).fetchall().get[String](col=0, row=0), "Ada")
    assert_equal(stmt.execute(Int32(3)).fetchall().get[String](col=0, row=0), "Linus")
    var plan = con.prepare("SELECT $1::INTEGER + 1")
    assert_true(plan.reuses_plan())
    assert_equal(plan.execute(Int32(41)).fetchall().get[Int32](col=0, row=0), 42)


def test_prepared_insert_loop() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (i INTEGER, s VARCHAR)")
    var stmt = con.prepare("INSERT INTO t VALUES (?, ?)")
    for i in range(100):
        assert_equal(stmt.execute(Int32(i), String("row ", i)).rows_changed(), 1)
    assert_equal(con.execute("SELECT count(*)::BIGINT FROM t").fetchall().get[Int64](col=0, row=0), 100)


def test_describe() raises:
    var con = _db()
    var d = con.describe("SELECT id, name FROM users WHERE age > $1")
    assert_equal(len(d.columns), 2)
    assert_equal(d.columns[1].name, "name")
    assert_equal(len(d.parameters), 1)
    assert_equal(String(d.parameters[0].type), "INTEGER")


# ── Transactions and rendering ───────────────────────────────────


def test_transactions() raises:
    var con = _db()
    con.begin()
    _ = con.execute("DELETE FROM users")
    con.rollback()
    assert_equal(con.execute("SELECT count(*)::BIGINT FROM users").fetchall().get[Int64](col=0, row=0), 3)
    con.begin()
    _ = con.execute("DELETE FROM users WHERE id = 1")
    con.commit()
    assert_equal(con.execute("SELECT count(*)::BIGINT FROM users").fetchall().get[Int64](col=0, row=0), 2)


def test_render() raises:
    var con = _db()
    var text = con.execute("SELECT id, name FROM users ORDER BY id").render()
    assert_true("Grace" in text, text)
    assert_true("name" in text, text)
    assert_true("┌" in text or "+" in text, text)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
