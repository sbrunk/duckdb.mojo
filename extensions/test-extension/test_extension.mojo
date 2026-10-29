"""Integration tests for the duckdb.mojo extension API.

Tests load the test extension shared library into DuckDB and verify:
- Extension loads successfully
- Scalar functions work (binary, unary, float)
- Aggregate functions work
- Multiple functions in one extension
- Functions work across multiple queries
- Extension functions work with table data
"""

from duckdb import *
from std.testing import *
from std.testing.suite import TestSuite

comptime EXT_PATH = "extensions/test-extension/build/mojo.duckdb_extension"


comptime BAD_API_EXT_PATH = "extensions/test-extension/build/bad_api.duckdb_extension"


def _open_unsigned() raises -> Database:
    """Open an in-memory database with allow_unsigned_extensions enabled."""
    var config = Config({"allow_unsigned_extensions": "true"})
    return Database(":memory:", config)


def _connect() raises -> Connection:
    """Create a connection with unsigned extensions enabled and the test
    extension loaded."""
    var db = _open_unsigned()
    var conn = Connection(db)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    return conn^


# ===--------------------------------------------------------------------===#
# Loading tests
# ===--------------------------------------------------------------------===#


def test_extension_loads() raises:
    """Extension loads without error."""
    var conn = _connect()
    # If we get here, the extension loaded successfully
    var result = conn.execute("SELECT 1 AS ok")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))


def test_extension_load_idempotent() raises:
    """Loading the same extension twice doesn't error (DuckDB deduplicates)."""
    var db = _open_unsigned()
    var conn = Connection(db)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    # Second load should be fine (DuckDB skips already-loaded extensions)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    var result = conn.execute("SELECT test_ext_add(1, 2)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(3))


# ===--------------------------------------------------------------------===#
# get_api tests
# ===--------------------------------------------------------------------===#


def test_get_api_invalid_version_returns_null() raises:
    """Calling get_api with an unsupported version prevents extension from loading."""
    var db = _open_unsigned()
    var conn = Connection(db)
    try:
        _ = conn.execute("LOAD '" + BAD_API_EXT_PATH + "'")
        raise Error("Expected LOAD to fail for invalid API version")
    except e:
        # DuckDB reports the unsupported version in the error message.
        assert_true(
            "v9999.0.0" in String(e),
            "Error should mention v9999.0.0, got: " + String(e),
        )


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_add (binary BIGINT)
# ===--------------------------------------------------------------------===#


def test_ext_add_basic() raises:
    """Binary add: basic addition."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(40, 2) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(42))


def test_ext_add_negative() raises:
    """Binary add: negative numbers."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(-10, 3) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(-7))


def test_ext_add_zeros() raises:
    """Binary add: zero arguments."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(0, 0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(0))


def test_ext_add_large() raises:
    """Binary add: large numbers."""
    var conn = _connect()
    var result = conn.execute(
        "SELECT test_ext_add(1000000000, 2000000000) AS result"
    )
    var chunk = result.fetch_chunk()
    assert_equal(
        chunk.get[Int64](col=0, row=0), Int64(3000000000)
    )


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_negate (unary BIGINT)
# ===--------------------------------------------------------------------===#


def test_ext_negate_positive() raises:
    """Negate: positive input."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(42) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(-42))


def test_ext_negate_negative() raises:
    """Negate: negative input (double negation)."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(-7) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(7))


def test_ext_negate_zero() raises:
    """Negate: zero."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(0))


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_multiply (binary DOUBLE)
# ===--------------------------------------------------------------------===#


def test_ext_multiply_basic() raises:
    """Multiply: basic multiplication."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(3.0, 7.0) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get[Float64](col=0, row=0), 21.0, atol=1e-10
    )


def test_ext_multiply_fractional() raises:
    """Multiply: fractional result."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(0.5, 0.25) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get[Float64](col=0, row=0), 0.125, atol=1e-10
    )


def test_ext_multiply_negative() raises:
    """Multiply: negative numbers."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(-2.0, 3.0) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get[Float64](col=0, row=0), -6.0, atol=1e-10
    )


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_double (unary BIGINT)
# ===--------------------------------------------------------------------===#


def test_ext_double_basic() raises:
    """Double: basic doubling."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(21) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(42))


def test_ext_double_zero() raises:
    """Double: zero."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(0))


def test_ext_double_negative() raises:
    """Double: negative input."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(-5) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(-10))


# ===--------------------------------------------------------------------===#
# Aggregate function tests: test_ext_sum
# ===--------------------------------------------------------------------===#


def test_ext_sum_basic() raises:
    """Aggregate sum: basic usage."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE t AS SELECT * FROM range(1, 11) t(x)")
    var result = conn.execute("SELECT test_ext_sum(x) FROM t")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(55))


def test_ext_sum_single() raises:
    """Aggregate sum: single value."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_sum(x) FROM (VALUES (42)) t(x)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(42))


def test_ext_sum_empty() raises:
    """Aggregate sum: empty table returns NULL."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE empty_t (x BIGINT)")
    var result = conn.execute("SELECT test_ext_sum(x) FROM empty_t")
    var chunk = result.fetch_chunk()
    # Empty aggregate returns NULL (no valid rows)
    var validity = chunk.get_vector(0).get_validity()
    assert_true(validity is not None)
    assert_false(Bool((validity.value()[unsafe_offset=0] >> 0) & 1))


# ===--------------------------------------------------------------------===#
# Table data tests (multiple rows)
# ===--------------------------------------------------------------------===#


def test_ext_add_table() raises:
    """Binary add over table data (batch processing)."""
    var conn = _connect()
    _ = conn.execute(
        "CREATE TABLE pairs AS SELECT x, x * 2 AS y"
        " FROM range(1, 101) t(x)"
    )
    var result = conn.execute(
        "SELECT CAST(SUM(test_ext_add(x, y)) AS BIGINT) AS total FROM pairs"
    )
    var chunk = result.fetch_chunk()
    # sum(x + 2x) = sum(3x) = 3 * sum(1..100) = 3 * 5050 = 15150
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(15150))


def test_ext_negate_table() raises:
    """Negate over table data."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE nums AS SELECT x FROM range(1, 6) t(x)")
    var result = conn.execute(
        "SELECT CAST(SUM(test_ext_negate(x)) AS BIGINT) AS total FROM nums"
    )
    var chunk = result.fetch_chunk()
    # -(1+2+3+4+5) = -15
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(-15))


# ===--------------------------------------------------------------------===#
# Composition tests (functions used together)
# ===--------------------------------------------------------------------===#


def test_ext_composition() raises:
    """Extension functions can be composed in a single query."""
    var conn = _connect()
    # negate(add(20, 22)) = negate(42) = -42
    var result = conn.execute(
        "SELECT test_ext_negate(test_ext_add(20, 22)) AS result"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(-42))


def test_ext_multiple_functions_one_query() raises:
    """Multiple extension functions in a single SELECT."""
    var conn = _connect()
    var result = conn.execute(
        "SELECT test_ext_add(1, 2) AS a,"
        " test_ext_negate(5) AS b,"
        " test_ext_double(21) AS c,"
        " test_ext_multiply(3.0, 4.0) AS d"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(3))
    assert_equal(chunk.get[Int64](col=1, row=0), Int64(-5))
    assert_equal(chunk.get[Int64](col=2, row=0), Int64(42))
    assert_almost_equal(
        chunk.get[Float64](col=3, row=0), 12.0, atol=1e-10
    )


# ===--------------------------------------------------------------------===#
# Multiple connections
# ===--------------------------------------------------------------------===#


def test_ext_across_connections() raises:
    """Extension functions accessible from multiple connections."""
    var db = _open_unsigned()
    var conn1 = Connection(db)
    _ = conn1.execute("LOAD '" + EXT_PATH + "'")

    # Create a second connection to the same database
    var conn2 = Connection(db)
    var result = conn2.execute("SELECT test_ext_add(10, 20)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(30))


# ===--------------------------------------------------------------------===#
# C API functions stabilized in v1.5.6
# ===--------------------------------------------------------------------===#


def test_ext_v1_5_6_api() raises:
    """`Extension.run` exposes functions that were unstable before DuckDB 1.5.6.

    The extension registers `test_ext_triple` only after creating and
    destroying a standalone vector, which uses functions from the v1.5.6 band
    of the extension API struct.
    """
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_triple(14) AS r")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int64](col=0, row=0), Int64(42))


# ===--------------------------------------------------------------------===#
# V2 C API extensions (duckdb.extension_v2)
# ===--------------------------------------------------------------------===#

comptime V2_EXT_PATH = "extensions/test-extension/build/mojo_v2.duckdb_extension"
comptime V2_FAIL_EXT_PATH = "extensions/test-extension/build/mojo_v2_fail.duckdb_extension"
comptime V2_STABLE_EXT_PATH = "extensions/test-extension/build/mojo_v2_stable.duckdb_extension"


def _connect_v2() raises -> Connection:
    var conn = Connection(_open_unsigned())
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
    var conn = Connection(_open_unsigned())
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


def test_v2_init_error_is_reported() raises:
    """An error raised in the init function fails the load with its message."""
    var conn = Connection(_open_unsigned())
    try:
        _ = conn.execute("LOAD '" + V2_FAIL_EXT_PATH + "'")
        raise Error("Expected LOAD to fail")
    except e:
        assert_true("intentional failure in the init function" in String(e), String(e))


def test_v2_stable_target_refused() raises:
    """DuckDB 2.0.0 has no stable V2 functions, so it refuses stable V2 extensions.

    Once a DuckDB release stabilizes part of the V2 API, this extension should
    load there, and this test needs to change.
    """
    var conn = Connection(_open_unsigned())
    try:
        _ = conn.execute("LOAD '" + V2_STABLE_EXT_PATH + "'")
        raise Error("Expected LOAD to fail")
    except e:
        assert_true("no stable surface" in String(e), String(e))


# ===--------------------------------------------------------------------===#
# Main
# ===--------------------------------------------------------------------===#


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
