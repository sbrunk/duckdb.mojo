from duckdb import *
from std.testing import *
from std.testing.suite import TestSuite

def test_logical_type() raises:
    var bigint = LogicalType(DuckDBType.bigint)
    var list = bigint.create_list_type()
    var child = list.list_type_child_type()

    # Compare by type_id since child is borrowed and bigint is owned
    assert_equal(bigint.get_type_id(), child.get_type_id())
    assert_not_equal(bigint.get_type_id(), list.get_type_id())
    assert_not_equal(child.get_type_id(), list.get_type_id())

def test_decimal_type() raises:
    var dec = decimal_type(18, 3)
    assert_equal(dec.get_type_id(), DuckDBType.decimal)

def test_enum_type() raises:
    var names: List[String] = ["One", "Two", "Three"]

    var t = enum_type(names)
    assert_equal(t.get_type_id(), DuckDBType.enum)

def test_variant_type_id() raises:
    """A VARIANT column reports DuckDBType.variant (DuckDB 1.5+)."""
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT CAST('{\"a\": 1}' AS VARIANT) AS v")
    assert_equal(result.column_type(0).get_type_id(), DuckDBType.variant)

def test_geometry_type_id() raises:
    """A GEOMETRY column reports DuckDBType.geometry (DuckDB 1.5+)."""
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT CAST('POINT(0 1)' AS GEOMETRY) AS g")
    assert_equal(result.column_type(0).get_type_id(), DuckDBType.geometry)

def test_geometry_crs_none() raises:
    """A plain GEOMETRY (no CRS) returns None from geometry_type_crs()."""
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT CAST('POINT(0 1)' AS GEOMETRY) AS g")
    assert_false(Bool(result.column_type(0).geometry_type_crs()))

def test_geometry_crs_value() raises:
    """A GEOMETRY column with a CRS returns the CRS string."""
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (g GEOMETRY('OGC:CRS84'))")
    var result = con.execute("SELECT g FROM t")
    var crs = result.column_type(0).geometry_type_crs()
    assert_true(Bool(crs))
    assert_equal(crs.value(), "OGC:CRS84")

def test_geometry_crs_non_geometry() raises:
    """geometry_type_crs() returns None for a non-GEOMETRY type."""
    var bigint = LogicalType(DuckDBType.bigint)
    assert_false(Bool(bigint.geometry_type_crs()))

def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
