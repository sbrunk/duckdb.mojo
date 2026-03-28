"""Tests for table-to-struct deserialization and MaterializedResult struct access."""

from duckdb import *
from std.testing import assert_equal, assert_false, assert_raises, assert_true
from std.testing.suite import TestSuite


# ──────────────────────────────────────────────────────────────────
# Struct types for testing
# ──────────────────────────────────────────────────────────────────


@fieldwise_init
struct Point(Copyable, Movable):
    """Simple 2D point with two Float64 fields."""
    var x: Float64
    var y: Float64


@fieldwise_init
struct UserRecord(Copyable, Movable):
    """A user record for table-to-struct tests."""
    var name: String
    var age: Int64
    var active: Bool


@fieldwise_init
struct RecordWithList(Copyable, Movable):
    """A record with a list field for table-to-struct tests."""
    var name: String
    var scores: List[Float64]


@fieldwise_init
struct LabeledPoint(Copyable, Movable):
    """A labeled point for nested-struct table tests."""
    var label: String
    var pt: Point


# ──────────────────────────────────────────────────────────────────
# Table-to-struct deserialization tests
# ──────────────────────────────────────────────────────────────────


def test_table_struct_single_row() raises:
    """Deserialize a single table row into a struct."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES ('Bob', 25, false)")
    var result = con.execute("SELECT name, age, active FROM users")
    var chunk = result.fetch_chunk()

    var user = chunk.get[UserRecord](row=0)
    assert_equal(user.name, "Alice")
    assert_equal(user.age, 30)
    assert_equal(user.active, True)

    var user2 = chunk.get[UserRecord](row=1)
    assert_equal(user2.name, "Bob")
    assert_equal(user2.age, 25)
    assert_equal(user2.active, False)


def test_table_struct_all_rows() raises:
    """Deserialize all table rows into structs."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES ('Bob', 25, false)")
    _ = con.execute("INSERT INTO users VALUES ('Charlie', 35, true)")
    var result = con.execute("SELECT name, age, active FROM users")
    var chunk = result.fetch_chunk()

    var users = chunk.get[UserRecord]()
    assert_equal(len(users), 3)

    assert_equal(users[0].name, "Alice")
    assert_equal(users[0].age, 30)

    assert_equal(users[1].name, "Bob")
    assert_equal(users[1].age, 25)

    assert_equal(users[2].name, "Charlie")
    assert_equal(users[2].age, 35)
    assert_equal(users[2].active, True)


def test_table_struct_with_nulls() raises:
    """Rows with any NULL column value raise an error."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES (NULL, 25, false)")
    _ = con.execute("INSERT INTO users VALUES ('Charlie', NULL, true)")
    var result = con.execute("SELECT name, age, active FROM users")
    var chunk = result.fetch_chunk()

    # Row 0: all non-null -> succeeds
    var user0 = chunk.get[UserRecord](row=0)
    assert_equal(user0.name, "Alice")

    # Row 1: name is NULL -> raises
    with assert_raises():
        _ = chunk.get[UserRecord](row=1)

    # Row 2: age is NULL -> raises
    with assert_raises():
        _ = chunk.get[UserRecord](row=2)


def test_table_struct_all_rows_with_nulls() raises:
    """All-rows get raises when a non-Optional field is NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES (NULL, 25, false)")
    _ = con.execute("INSERT INTO users VALUES ('Charlie', 35, true)")
    var result = con.execute("SELECT name, age, active FROM users")
    var chunk = result.fetch_chunk()

    with assert_raises():
        _ = chunk.get[UserRecord]()


def test_table_struct_column_count_mismatch() raises:
    """Error when column count doesn't match struct field count."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, age BIGINT)")
    _ = con.execute("INSERT INTO t VALUES ('Alice', 30)")
    var result = con.execute("SELECT name, age FROM t")
    var chunk = result.fetch_chunk()

    # UserRecord has 3 fields but the query returns 2 columns
    with assert_raises(contains="Column count mismatch"):
        _ = chunk.get[UserRecord](row=0)


def test_table_struct_type_mismatch() raises:
    """Error when a column type doesn't match the struct field type."""
    con = DuckDB.connect(":memory:")
    # age should be BIGINT but we use VARCHAR
    _ = con.execute(
        "CREATE TABLE t (name VARCHAR, age VARCHAR, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO t VALUES ('Alice', '30', true)")
    var result = con.execute("SELECT name, age, active FROM t")
    var chunk = result.fetch_chunk()

    with assert_raises(contains="Type mismatch"):
        _ = chunk.get[UserRecord](row=0)


def test_table_struct_row_out_of_bounds() raises:
    """Error when row index is out of bounds."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO t VALUES ('Alice', 30, true)")
    var result = con.execute("SELECT name, age, active FROM t")
    var chunk = result.fetch_chunk()

    with assert_raises(contains="out of bounds"):
        _ = chunk.get[UserRecord](row=5)


def test_table_struct_with_list_field() raises:
    """Deserialize a table row with a List field."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, scores DOUBLE[])")
    _ = con.execute(
        "INSERT INTO t VALUES ('Alice', [90.0, 95.0, 100.0])"
    )
    _ = con.execute("INSERT INTO t VALUES ('Bob', [80.0, 85.0])")
    var result = con.execute("SELECT name, scores FROM t")
    var chunk = result.fetch_chunk()

    var rec = chunk.get[RecordWithList](row=0)
    assert_equal(rec.name, "Alice")
    assert_equal(len(rec.scores), 3)
    assert_equal(rec.scores[0], 90.0)
    assert_equal(rec.scores[1], 95.0)
    assert_equal(rec.scores[2], 100.0)

    var rec2 = chunk.get[RecordWithList](row=1)
    assert_equal(rec2.name, "Bob")
    assert_equal(len(rec2.scores), 2)


def test_table_struct_with_nested_struct() raises:
    """Deserialize a table row where a column is a DuckDB STRUCT."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (label VARCHAR, pt STRUCT(x DOUBLE, y DOUBLE))"
    )
    _ = con.execute("INSERT INTO t VALUES ('origin', {'x': 0.0, 'y': 0.0})")
    _ = con.execute(
        "INSERT INTO t VALUES ('offset', {'x': 1.5, 'y': 2.5})"
    )
    var result = con.execute("SELECT label, pt FROM t")
    var chunk = result.fetch_chunk()

    var row0 = chunk.get[LabeledPoint](row=0)
    assert_equal(row0.label, "origin")
    assert_equal(row0.pt.x, 0.0)
    assert_equal(row0.pt.y, 0.0)

    var row1 = chunk.get[LabeledPoint](row=1)
    assert_equal(row1.label, "offset")
    assert_equal(row1.pt.x, 1.5)
    assert_equal(row1.pt.y, 2.5)


# ──────────────────────────────────────────────────────────────────
# MaterializedResult struct deserialization
# ──────────────────────────────────────────────────────────────────


def test_materialized_struct_single_row() raises:
    """Deserialize a single row from a MaterializedResult into a struct."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES ('Bob', 25, false)")
    var result = con.execute("SELECT name, age, active FROM users").fetchall()

    var user0 = result.get[UserRecord](row=0)
    assert_equal(user0.name, "Alice")
    assert_equal(user0.age, 30)
    assert_equal(user0.active, True)

    var user1 = result.get[UserRecord](row=1)
    assert_equal(user1.name, "Bob")
    assert_equal(user1.age, 25)
    assert_equal(user1.active, False)


def test_materialized_struct_all_rows() raises:
    """Deserialize all rows from a MaterializedResult into structs."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES ('Bob', 25, false)")
    _ = con.execute("INSERT INTO users VALUES ('Charlie', 35, true)")
    var result = con.execute("SELECT name, age, active FROM users").fetchall()

    var users = result.get[UserRecord]()
    assert_equal(len(users), 3)

    assert_equal(users[0].name, "Alice")
    assert_equal(users[0].age, 30)
    assert_equal(users[0].active, True)

    assert_equal(users[1].name, "Bob")
    assert_equal(users[1].age, 25)
    assert_equal(users[1].active, False)

    assert_equal(users[2].name, "Charlie")
    assert_equal(users[2].age, 35)
    assert_equal(users[2].active, True)


def test_materialized_struct_with_nulls_raises() raises:
    """MaterializedResult.get[T]() raises when non-Optional field is NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE users (name VARCHAR, age BIGINT, active BOOLEAN)"
    )
    _ = con.execute("INSERT INTO users VALUES ('Alice', 30, true)")
    _ = con.execute("INSERT INTO users VALUES (NULL, 25, false)")
    var result = con.execute("SELECT name, age, active FROM users").fetchall()

    # Row 0 works fine
    var user = result.get[UserRecord](row=0)
    assert_equal(user.name, "Alice")

    # Row 1 has NULL name -> raises
    with assert_raises():
        _ = result.get[UserRecord](row=1)

    # get all rows also raises
    with assert_raises():
        _ = result.get[UserRecord]()


def test_materialized_struct_row_out_of_bounds() raises:
    """MaterializedResult.get[T](row=) raises on out-of-bounds row."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, age BIGINT, active BOOLEAN)")
    _ = con.execute("INSERT INTO t VALUES ('Alice', 30, true)")
    var result = con.execute("SELECT * FROM t").fetchall()

    with assert_raises():
        _ = result.get[UserRecord](row=5)


def test_materialized_struct_with_list_field() raises:
    """MaterializedResult struct deserialization with a List field."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, scores DOUBLE[])")
    _ = con.execute("INSERT INTO t VALUES ('Alice', [90.0, 95.0])")
    _ = con.execute("INSERT INTO t VALUES ('Bob', [80.0])")
    var result = con.execute("SELECT name, scores FROM t").fetchall()

    var all = result.get[RecordWithList]()
    assert_equal(len(all), 2)
    assert_equal(all[0].name, "Alice")
    assert_equal(len(all[0].scores), 2)
    assert_equal(all[0].scores[0], 90.0)
    assert_equal(all[1].name, "Bob")
    assert_equal(len(all[1].scores), 1)

    var rec = result.get[RecordWithList](row=0)
    assert_equal(rec.name, "Alice")
    assert_equal(rec.scores[1], 95.0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
