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
from duckdb._libduckdb import *
from duckdb.api import DuckDB as _DuckDB
from testing import *
from testing.suite import TestSuite

comptime EXT_PATH = "test-extension/build/mojo.duckdb_extension"


comptime BAD_API_EXT_PATH = "test-extension/build/bad_api.duckdb_extension"


fn _open_unsigned() raises -> Database:
    """Open an in-memory database with allow_unsigned_extensions enabled."""
    ref libduckdb = _DuckDB().libduckdb()
    var cfg = duckdb_config()
    var cfg_addr = UnsafePointer(to=cfg)
    if libduckdb.duckdb_create_config(cfg_addr) == DuckDBError:
        raise Error("Failed to create config")
    var name = String("allow_unsigned_extensions")
    var val = String("true")
    if (
        libduckdb.duckdb_set_config(
            cfg,
            name.as_c_string_slice().unsafe_ptr(),
            val.as_c_string_slice().unsafe_ptr(),
        )
        == DuckDBError
    ):
        libduckdb.duckdb_destroy_config(cfg_addr)
        raise Error("Failed to set allow_unsigned_extensions")
    var db = duckdb_database()
    var db_addr = UnsafePointer(to=db)
    var out_error = alloc[UnsafePointer[c_char, MutAnyOrigin]](1)
    var path = String(":memory:")
    if (
        libduckdb.duckdb_open_ext(
            path.as_c_string_slice().unsafe_ptr(),
            db_addr,
            config=cfg,
            out_error=out_error,
        )
        == DuckDBError
    ):
        var error_ptr = out_error[]
        var error_msg = String(unsafe_from_utf8_ptr=error_ptr)
        libduckdb.duckdb_free(error_ptr.bitcast[NoneType]())
        libduckdb.duckdb_destroy_config(cfg_addr)
        raise Error(error_msg)
    libduckdb.duckdb_destroy_config(cfg_addr)
    return Database(_handle=db)


fn _connect() raises -> Connection:
    """Create a connection with unsigned extensions enabled and the test
    extension loaded."""
    var db = _open_unsigned()
    var conn = Connection(db^)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    return conn^


# ===--------------------------------------------------------------------===#
# Loading tests
# ===--------------------------------------------------------------------===#


def test_extension_loads():
    """Extension loads without error."""
    var conn = _connect()
    # If we get here, the extension loaded successfully
    var result = conn.execute("SELECT 1 AS ok")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(integer, col=0, row=0).value(), Int32(1))


def test_extension_load_idempotent():
    """Loading the same extension twice doesn't error (DuckDB deduplicates)."""
    var db = _open_unsigned()
    var conn = Connection(db^)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    # Second load should be fine (DuckDB skips already-loaded extensions)
    _ = conn.execute("LOAD '" + EXT_PATH + "'")
    var result = conn.execute("SELECT test_ext_add(1, 2)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(3))


# ===--------------------------------------------------------------------===#
# get_api tests
# ===--------------------------------------------------------------------===#


def test_get_api_invalid_version_returns_null():
    """get_api with an unsupported version prevents extension from loading."""
    var db = _open_unsigned()
    var conn = Connection(db^)
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


def test_ext_add_basic():
    """Binary add: basic addition."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(40, 2) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(42))


def test_ext_add_negative():
    """Binary add: negative numbers."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(-10, 3) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(-7))


def test_ext_add_zeros():
    """Binary add: zero arguments."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_add(0, 0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(0))


def test_ext_add_large():
    """Binary add: large numbers."""
    var conn = _connect()
    var result = conn.execute(
        "SELECT test_ext_add(1000000000, 2000000000) AS result"
    )
    var chunk = result.fetch_chunk()
    assert_equal(
        chunk.get(bigint, col=0, row=0).value(), Int64(3000000000)
    )


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_negate (unary BIGINT)
# ===--------------------------------------------------------------------===#


def test_ext_negate_positive():
    """Negate: positive input."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(42) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(-42))


def test_ext_negate_negative():
    """Negate: negative input (double negation)."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(-7) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(7))


def test_ext_negate_zero():
    """Negate: zero."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_negate(0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(0))


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_multiply (binary DOUBLE)
# ===--------------------------------------------------------------------===#


def test_ext_multiply_basic():
    """Multiply: basic multiplication."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(3.0, 7.0) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get(double, col=0, row=0).value(), 21.0, atol=1e-10
    )


def test_ext_multiply_fractional():
    """Multiply: fractional result."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(0.5, 0.25) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get(double, col=0, row=0).value(), 0.125, atol=1e-10
    )


def test_ext_multiply_negative():
    """Multiply: negative numbers."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_multiply(-2.0, 3.0) AS result")
    var chunk = result.fetch_chunk()
    assert_almost_equal(
        chunk.get(double, col=0, row=0).value(), -6.0, atol=1e-10
    )


# ===--------------------------------------------------------------------===#
# Scalar function tests: test_ext_double (unary BIGINT)
# ===--------------------------------------------------------------------===#


def test_ext_double_basic():
    """Double: basic doubling."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(21) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(42))


def test_ext_double_zero():
    """Double: zero."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(0) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(0))


def test_ext_double_negative():
    """Double: negative input."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_double(-5) AS result")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(-10))


# ===--------------------------------------------------------------------===#
# Aggregate function tests: test_ext_sum
# ===--------------------------------------------------------------------===#


def test_ext_sum_basic():
    """Aggregate sum: basic usage."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE t AS SELECT * FROM range(1, 11) t(x)")
    var result = conn.execute("SELECT test_ext_sum(x) FROM t")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(55))


def test_ext_sum_single():
    """Aggregate sum: single value."""
    var conn = _connect()
    var result = conn.execute("SELECT test_ext_sum(x) FROM (VALUES (42)) t(x)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(42))


def test_ext_sum_empty():
    """Aggregate sum: empty table returns NULL."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE empty_t (x BIGINT)")
    var result = conn.execute("SELECT test_ext_sum(x) FROM empty_t")
    var chunk = result.fetch_chunk()
    # Empty aggregate returns NULL (no valid rows)
    var validity = chunk.get_vector(0).get_validity()
    assert_not_equal(validity, UnsafePointer[UInt64, MutAnyOrigin]())
    assert_false(Bool((validity[0] >> 0) & 1))


# ===--------------------------------------------------------------------===#
# Table data tests (multiple rows)
# ===--------------------------------------------------------------------===#


def test_ext_add_table():
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
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(15150))


def test_ext_negate_table():
    """Negate over table data."""
    var conn = _connect()
    _ = conn.execute("CREATE TABLE nums AS SELECT x FROM range(1, 6) t(x)")
    var result = conn.execute(
        "SELECT CAST(SUM(test_ext_negate(x)) AS BIGINT) AS total FROM nums"
    )
    var chunk = result.fetch_chunk()
    # -(1+2+3+4+5) = -15
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(-15))


# ===--------------------------------------------------------------------===#
# Composition tests (functions used together)
# ===--------------------------------------------------------------------===#


def test_ext_composition():
    """Extension functions can be composed in a single query."""
    var conn = _connect()
    # negate(add(20, 22)) = negate(42) = -42
    var result = conn.execute(
        "SELECT test_ext_negate(test_ext_add(20, 22)) AS result"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(-42))


def test_ext_multiple_functions_one_query():
    """Multiple extension functions in a single SELECT."""
    var conn = _connect()
    var result = conn.execute(
        "SELECT test_ext_add(1, 2) AS a,"
        " test_ext_negate(5) AS b,"
        " test_ext_double(21) AS c,"
        " test_ext_multiply(3.0, 4.0) AS d"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(3))
    assert_equal(chunk.get(bigint, col=1, row=0).value(), Int64(-5))
    assert_equal(chunk.get(bigint, col=2, row=0).value(), Int64(42))
    assert_almost_equal(
        chunk.get(double, col=3, row=0).value(), 12.0, atol=1e-10
    )


# ===--------------------------------------------------------------------===#
# Multiple connections
# ===--------------------------------------------------------------------===#


def test_ext_across_connections():
    """Extension functions accessible from multiple connections."""
    var db = _open_unsigned()
    var conn1 = Connection(db)
    _ = conn1.execute("LOAD '" + EXT_PATH + "'")

    # Create a second connection to the same database
    var conn2 = Connection(db)
    var result = conn2.execute("SELECT test_ext_add(10, 20)")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), Int64(30))


# ===--------------------------------------------------------------------===#
# Main
# ===--------------------------------------------------------------------===#


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
