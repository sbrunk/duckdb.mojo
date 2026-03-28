"""Tests for MojoType descriptors and basic struct deserialization."""

from duckdb import *
from std.collections import Optional
from std.reflection import struct_field_count
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
struct IntPair(Copyable, Movable):
    """A pair of integers."""
    var a: Int32
    var b: Int64


# ──────────────────────────────────────────────────────────────────
# MojoType descriptor tests
# ──────────────────────────────────────────────────────────────────


def test_mojo_type_scalar() raises:
    """Test MojoType construction for scalar types."""
    var mt = MojoType(DuckDBType.bigint)
    assert_equal(String(mt), String(DuckDBType.bigint))
    assert_equal(mt.type_id, DuckDBType.bigint)
    assert_equal(len(mt.children), 0)
    assert_equal(len(mt.field_names), 0)


def test_mojo_type_list() raises:
    """Test MojoType.list_of() construction."""
    var element = MojoType(DuckDBType.integer)
    var list_mt = MojoType.list_of(element^)
    assert_equal(list_mt.type_id, DuckDBType.list)
    assert_equal(len(list_mt.children), 1)
    assert_equal(list_mt.children[0].type_id, DuckDBType.integer)
    assert_equal(String(list_mt), "list(integer)")


def test_mojo_type_struct() raises:
    """Test MojoType.struct_of() construction."""
    var names = List[String]()
    names.append("x")
    names.append("y")
    var types = List[MojoType]()
    types.append(MojoType(DuckDBType.double))
    types.append(MojoType(DuckDBType.double))
    var struct_mt = MojoType.struct_of(names^, types^)
    assert_equal(struct_mt.type_id, DuckDBType.struct_t)
    assert_equal(len(struct_mt.children), 2)
    assert_equal(len(struct_mt.field_names), 2)
    assert_equal(struct_mt.field_names[0], "x")
    assert_equal(struct_mt.field_names[1], "y")
    assert_equal(String(struct_mt), "struct(x double, y double)")


def test_mojo_logical_type_scalar() raises:
    """Test mojo_logical_type[T]() for scalar types."""
    var mt_int = mojo_logical_type[Int64]()
    assert_equal(mt_int.type_id, DuckDBType.bigint)
    var mt_str = mojo_logical_type[String]()
    assert_equal(mt_str.type_id, DuckDBType.varchar)
    var mt_f64 = mojo_logical_type[Float64]()
    assert_equal(mt_f64.type_id, DuckDBType.double)


def test_mojo_logical_type_struct() raises:
    """Test mojo_logical_type[T]() for struct types using reflection."""
    var mt = mojo_logical_type[Point]()
    assert_equal(mt.type_id, DuckDBType.struct_t)
    assert_equal(len(mt.field_names), 2)
    assert_equal(mt.field_names[0], "x")
    assert_equal(mt.field_names[1], "y")
    assert_equal(mt.children[0].type_id, DuckDBType.double)
    assert_equal(mt.children[1].type_id, DuckDBType.double)


def test_mojo_type_to_logical_type() raises:
    """Test MojoType.to_logical_type() conversion to DuckDB runtime type."""
    # Scalar
    var mt = MojoType(DuckDBType.bigint)
    var lt = mt.to_logical_type()
    assert_equal(lt.get_type_id(), DuckDBType.bigint)

    # Struct
    var struct_mt = mojo_logical_type[Point]()
    var struct_lt = struct_mt.to_logical_type()
    assert_equal(struct_lt.get_type_id(), DuckDBType.struct_t)
    assert_equal(Int(struct_lt.struct_type_child_count()), 2)
    assert_equal(struct_lt.struct_type_child_name(0), "x")
    assert_equal(struct_lt.struct_type_child_name(1), "y")


# ──────────────────────────────────────────────────────────────────
# Struct deserialization tests
# ──────────────────────────────────────────────────────────────────


def test_struct_deserialization_numeric() raises:
    """Test deserializing DuckDB STRUCT into a Mojo struct with numeric fields."""
    con = DuckDB.connect(":memory:")

    result = con.execute(
        "SELECT {'x': 1.5::DOUBLE, 'y': 2.5::DOUBLE} AS pt"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[Point](col=0, row=0)
    assert_equal(val.x, 1.5)
    assert_equal(val.y, 2.5)


def test_struct_deserialization_column() raises:
    """Test deserializing a column of DuckDB STRUCTs."""
    con = DuckDB.connect(":memory:")

    result = con.execute("""
        SELECT * FROM (VALUES
            ({'x': 0.0::DOUBLE, 'y': 0.0::DOUBLE}),
            ({'x': 1.0::DOUBLE, 'y': 2.0::DOUBLE}),
            ({'x': 3.0::DOUBLE, 'y': 4.0::DOUBLE})
        ) AS t(pt)
    """)
    var chunk = result.fetch_chunk()
    var points = chunk.get[Point](col=0)
    assert_equal(len(points), 3)

    assert_equal(points[0].x, 0.0)
    assert_equal(points[0].y, 0.0)

    assert_equal(points[1].x, 1.0)
    assert_equal(points[1].y, 2.0)

    assert_equal(points[2].x, 3.0)
    assert_equal(points[2].y, 4.0)


def test_struct_deserialization_mixed_int_types() raises:
    """Test struct with mixed integer types."""
    con = DuckDB.connect(":memory:")

    result = con.execute(
        "SELECT {'a': 10::INTEGER, 'b': 20::BIGINT} AS pair"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[IntPair](col=0, row=0)
    assert_equal(val.a, 10)
    assert_equal(val.b, 20)


def test_struct_null_handling() raises:
    """Test NULL handling for struct columns."""
    con = DuckDB.connect(":memory:")

    result = con.execute("""
        SELECT * FROM (VALUES
            ({'x': 1.0::DOUBLE, 'y': 2.0::DOUBLE}),
            (NULL),
            ({'x': 3.0::DOUBLE, 'y': 4.0::DOUBLE})
        ) AS t(pt)
    """)
    var chunk = result.fetch_chunk()
    var points = chunk.get[Optional[Point]](col=0)
    assert_equal(len(points), 3)

    assert_true(points[0])
    assert_equal(points[0].value().x, 1.0)

    assert_false(points[1])  # NULL row

    assert_true(points[2])
    assert_equal(points[2].value().x, 3.0)


def test_logical_type_struct_creation() raises:
    """Test creating struct LogicalType via the struct_type() function."""
    var names = List[String]()
    names.append("a")
    names.append("b")
    var types = List[LogicalType[True, MutExternalOrigin]]()
    types.append(LogicalType[True, MutExternalOrigin](DuckDBType.integer))
    types.append(LogicalType[True, MutExternalOrigin](DuckDBType.double))
    var lt = struct_type(names, types)
    assert_equal(lt.get_type_id(), DuckDBType.struct_t)
    assert_equal(Int(lt.struct_type_child_count()), 2)
    assert_equal(lt.struct_type_child_name(0), "a")
    assert_equal(lt.struct_type_child_name(1), "b")
    assert_equal(lt.struct_type_child_type(0).get_type_id(), DuckDBType.integer)
    assert_equal(lt.struct_type_child_type(1).get_type_id(), DuckDBType.double)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
