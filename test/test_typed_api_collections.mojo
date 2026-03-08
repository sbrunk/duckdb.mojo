"""Tests for the typed API — list deserialization, nested lists, ARRAY, MAP, MAP as List[Struct], UNION, UNION-as-Variant."""

from duckdb import *
from std.utils import Variant
from std.collections import Dict, Optional
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
struct MapEntryStrInt(Copyable, Movable):
    """Map entry struct matching MAP(VARCHAR, INTEGER) internal representation."""
    var key: String
    var value: Optional[Int32]


@fieldwise_init
struct NumOrStr(Copyable, Movable):
    """Union struct matching UNION(num INTEGER, str VARCHAR)."""
    var num: Optional[Int32]
    var str: Optional[String]


@fieldwise_init
struct NumOrStrOrBool(Copyable, Movable):
    """Union struct matching UNION(num INTEGER, str VARCHAR, flag BOOLEAN)."""
    var num: Optional[Int32]
    var str: Optional[String]
    var flag: Optional[Bool]


# ──────────────────────────────────────────────────────────────────
# List deserialization tests
# ──────────────────────────────────────────────────────────────────


def test_list_column_int32() raises:
    """Deserialize a LIST(INTEGER) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute("INSERT INTO t VALUES ([1, 2, 3]), ([4, 5]), ([6])")
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Int32]]](col=0)

    assert_equal(len(lists), 3)

    # Row 0: [1, 2, 3]
    var row0 = lists[0].copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)

    # Row 1: [4, 5]
    var row1 = lists[1].copy()
    assert_equal(len(row1), 2)
    assert_equal(row1[0].value(), 4)
    assert_equal(row1[1].value(), 5)

    # Row 2: [6]
    var row2 = lists[2].copy()
    assert_equal(len(row2), 1)
    assert_equal(row2[0].value(), 6)


def test_list_column_varchar() raises:
    """Deserialize a LIST(VARCHAR) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (words VARCHAR[])")
    _ = con.execute("INSERT INTO t VALUES (['hello', 'world']), (['mojo'])")
    result = con.execute("SELECT words FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[String]]](col=0)

    assert_equal(len(lists), 2)

    var row0 = lists[0].copy()
    assert_equal(len(row0), 2)
    assert_equal(row0[0].value(), "hello")
    assert_equal(row0[1].value(), "world")

    var row1 = lists[1].copy()
    assert_equal(len(row1), 1)
    assert_equal(row1[0].value(), "mojo")


def test_list_column_with_nulls() raises:
    """Deserialize a LIST column with NULL rows and NULL elements."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute(
        "INSERT INTO t VALUES ([1, NULL, 3]), (NULL), ([4])"
    )
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[Optional[List[Optional[Int32]]]](col=0)

    assert_equal(len(lists), 3)

    # Row 0: [1, NULL, 3]
    assert_true(lists[0])
    var row0 = lists[0].value().copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_false(row0[1])  # NULL element
    assert_equal(row0[2].value(), 3)

    # Row 1: NULL (entire row is NULL)
    assert_false(lists[1])

    # Row 2: [4]
    assert_true(lists[2])
    assert_equal(lists[2].value().copy()[0].value(), 4)


def test_list_column_empty_lists() raises:
    """Deserialize empty lists."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute("INSERT INTO t VALUES ([]), ([42])")
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Int32]]](col=0)

    assert_equal(len(lists), 2)

    # Row 0: empty list
    assert_equal(len(lists[0].copy()), 0)

    # Row 1: [42]
    assert_equal(lists[1].copy()[0].value(), 42)


def test_list_column_float64() raises:
    """Deserialize a LIST(DOUBLE) column."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT [1.5, 2.5, 3.5]::DOUBLE[] AS vals")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Float64]]](col=0)

    assert_equal(len(lists), 1)
    var row0 = lists[0].copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1.5)
    assert_equal(row0[1].value(), 2.5)
    assert_equal(row0[2].value(), 3.5)


# ──────────────────────────────────────────────────────────────────
# Unified API — get for list types
# ──────────────────────────────────────────────────────────────────


def test_list_via_get() raises:
    """Deserialize a LIST(INTEGER) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute("INSERT INTO t VALUES ([10, 20]), ([30])")
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()

    # T = List[Optional[Int32]] -> returns List[List[Optional[Int32]]]
    var lists = chunk.get[List[Optional[Int32]]](col=0)

    assert_equal(len(lists), 2)

    var row0 = lists[0].copy()
    assert_equal(len(row0), 2)
    assert_equal(row0[0].value(), 10)
    assert_equal(row0[1].value(), 20)

    var row1 = lists[1].copy()
    assert_equal(len(row1), 1)
    assert_equal(row1[0].value(), 30)


def test_list_via_get_with_nulls() raises:
    """LIST(INTEGER) column with NULLs."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute("INSERT INTO t VALUES ([1, NULL, 3]), (NULL), ([4])")
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[Optional[List[Optional[Int32]]]](col=0)

    assert_equal(len(lists), 3)

    # Row 0: [1, NULL, 3]
    assert_true(lists[0])
    var row0 = lists[0].value().copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_false(row0[1])  # NULL element
    assert_equal(row0[2].value(), 3)

    # Row 1: NULL row
    assert_false(lists[1])

    # Row 2: [4]
    assert_true(lists[2])
    assert_equal(lists[2].value().copy()[0].value(), 4)


def test_nested_list() raises:
    """Deserialize a LIST(LIST(INTEGER)) column — arbitrarily nested."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[][])")
    _ = con.execute(
        "INSERT INTO t VALUES ([[1, 2], [3]]), ([[4, 5, 6]])"
    )
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()

    # T = List[Optional[List[Optional[Int32]]]]
    var lists = chunk.get[
        List[Optional[List[Optional[Int32]]]]
    ](col=0)

    assert_equal(len(lists), 2)

    # Row 0: [[1, 2], [3]]
    var row0 = lists[0].copy()
    assert_equal(len(row0), 2)

    assert_true(row0[0])
    var inner0 = row0[0].value().copy()
    assert_equal(len(inner0), 2)
    assert_equal(inner0[0].value(), 1)
    assert_equal(inner0[1].value(), 2)

    assert_true(row0[1])
    var inner1 = row0[1].value().copy()
    assert_equal(len(inner1), 1)
    assert_equal(inner1[0].value(), 3)

    # Row 1: [[4, 5, 6]]
    var row1 = lists[1].copy()
    assert_equal(len(row1), 1)
    var inner2 = row1[0].value().copy()
    assert_equal(len(inner2), 3)
    assert_equal(inner2[0].value(), 4)
    assert_equal(inner2[1].value(), 5)
    assert_equal(inner2[2].value(), 6)


def test_nested_list_with_nulls() raises:
    """Nested LIST(LIST(INTEGER)) with NULLs at various levels."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[][])")
    _ = con.execute(
        "INSERT INTO t VALUES ([[1, NULL], NULL]), (NULL)"
    )
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[
        Optional[List[Optional[List[Optional[Int32]]]]]
    ](col=0)

    assert_equal(len(lists), 2)

    # Row 0: [[1, NULL], NULL]
    assert_true(lists[0])
    var row0 = lists[0].value().copy()
    assert_equal(len(row0), 2)

    assert_true(row0[0])
    var inner0 = row0[0].value().copy()
    assert_equal(len(inner0), 2)
    assert_equal(inner0[0].value(), 1)
    assert_false(inner0[1])  # NULL element

    assert_false(row0[1])  # NULL inner list

    # Row 1: NULL row
    assert_false(lists[1])


# ──────────────────────────────────────────────────────────────────
# ARRAY tests
# ──────────────────────────────────────────────────────────────────


def test_array_column_int32() raises:
    """Deserialize an ARRAY column of fixed-size INTEGER[3]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr INTEGER[3])")
    _ = con.execute(
        "INSERT INTO t VALUES ([1, 2, 3]), ([4, 5, 6]), ([7, 8, 9])"
    )
    result = con.execute("SELECT arr FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Int32]]](col=0)

    assert_equal(len(lists), 3)

    # Row 0: [1, 2, 3]
    var row0 = lists[0].copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)

    # Row 1: [4, 5, 6]
    var row1 = lists[1].copy()
    assert_equal(len(row1), 3)
    assert_equal(row1[0].value(), 4)
    assert_equal(row1[1].value(), 5)
    assert_equal(row1[2].value(), 6)

    # Row 2: [7, 8, 9]
    var row2 = lists[2].copy()
    assert_equal(len(row2), 3)
    assert_equal(row2[0].value(), 7)
    assert_equal(row2[1].value(), 8)
    assert_equal(row2[2].value(), 9)


def test_array_single_row() raises:
    """Deserialize a single ARRAY row."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT [10, 20]::INTEGER[2] AS arr")
    var chunk = result.fetch_chunk()
    var arr = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(arr), 2)
    assert_equal(arr[0].value(), 10)
    assert_equal(arr[1].value(), 20)


def test_array_column_varchar() raises:
    """Deserialize an ARRAY column of VARCHAR[2]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr VARCHAR[2])")
    _ = con.execute(
        "INSERT INTO t VALUES (['hello', 'world']), (['foo', 'bar'])"
    )
    result = con.execute("SELECT arr FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[String]]](col=0)

    assert_equal(len(lists), 2)
    var row0 = lists[0].copy()
    assert_equal(row0[0].value(), "hello")
    assert_equal(row0[1].value(), "world")

    var row1 = lists[1].copy()
    assert_equal(row1[0].value(), "foo")
    assert_equal(row1[1].value(), "bar")


def test_array_column_with_null_rows() raises:
    """Deserialize an ARRAY column where some rows are NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr INTEGER[2])")
    _ = con.execute("INSERT INTO t VALUES ([1, 2]), (NULL), ([3, 4])")
    result = con.execute("SELECT arr FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[Optional[List[Optional[Int32]]]](col=0)

    assert_equal(len(lists), 3)
    assert_true(lists[0])
    assert_equal(lists[0].value().copy()[0].value(), 1)
    assert_false(lists[1])  # NULL row
    assert_true(lists[2])
    assert_equal(lists[2].value().copy()[0].value(), 3)


def test_array_column_float64() raises:
    """Deserialize an ARRAY column of DOUBLE[2]."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT [1.5, 2.5]::DOUBLE[2] AS arr"
    )
    var chunk = result.fetch_chunk()
    var arr = chunk.get[List[Optional[Float64]]](col=0, row=0)
    assert_equal(len(arr), 2)
    assert_equal(arr[0].value(), 1.5)
    assert_equal(arr[1].value(), 2.5)


# ──────────────────────────────────────────────────────────────────
# MAP tests
# ──────────────────────────────────────────────────────────────────


def test_map_single_row() raises:
    """Deserialize a single MAP(VARCHAR, INTEGER) row as Dict."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': 2} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, Int32]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["a"], 1)
    assert_equal(d["b"], 2)


def test_map_column() raises:
    """Deserialize a column of MAPs as Dicts."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    _ = con.execute(
        "INSERT INTO t VALUES (MAP {'x': 10}), (MAP {'y': 20, 'z': 30})"
    )
    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var dicts = chunk.get[Dict[String, Int32]](col=0)

    assert_equal(len(dicts), 2)

    var d0 = dicts[0].copy()
    assert_equal(len(d0), 1)
    assert_equal(d0["x"], 10)

    var d1 = dicts[1].copy()
    assert_equal(len(d1), 2)
    assert_equal(d1["y"], 20)
    assert_equal(d1["z"], 30)


def test_map_with_null_values() raises:
    """Deserialize a MAP where some values are NULL using Optional."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': NULL::INTEGER} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, Optional[Int32]]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["a"].value(), 1)
    assert_false(d["b"])  # NULL value


def test_map_with_null_rows() raises:
    """Deserialize a MAP column where some rows are NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    _ = con.execute(
        "INSERT INTO t VALUES (MAP {'a': 1}), (NULL), (MAP {'c': 3})"
    )
    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var dicts = chunk.get[Optional[Dict[String, Int32]]](col=0)

    assert_equal(len(dicts), 3)
    assert_true(dicts[0])
    assert_equal(dicts[0].value()["a"], 1)
    assert_false(dicts[1])  # NULL row
    assert_true(dicts[2])
    assert_equal(dicts[2].value()["c"], 3)


def test_map_int_keys() raises:
    """Deserialize a MAP with integer keys."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {1: 'one', 2: 'two'} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[Int32, String]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d[Int32(1)], "one")
    assert_equal(d[Int32(2)], "two")


def test_map_with_mojo_int() raises:
    """Deserialize a MAP using Dict[String, Int] — Mojo's native integer type.

    Int is platform-dependent (64-bit on most modern systems). DuckDB's
    INTEGER is always 32-bit, so the values are widened on read.
    """
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': 2}::MAP(VARCHAR, BIGINT) AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, Int]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["a"], 1)
    assert_equal(d["b"], 2)


def test_map_with_mojo_uint() raises:
    """Deserialize a MAP using Dict[String, UInt] — Mojo's native unsigned integer.

    UInt is platform-dependent (64-bit on most modern systems). Maps to
    UBIGINT on 64-bit platforms.
    """
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'x': 10, 'y': 20}::MAP(VARCHAR, UBIGINT) AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, UInt]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["x"], 10)
    assert_equal(d["y"], 20)


# ──────────────────────────────────────────────────────────────────
# MAP as List[Struct] (alternative representation)
# ──────────────────────────────────────────────────────────────────


def test_map_as_list_struct_single_row() raises:
    """Deserialize a MAP as List[Struct] — the raw DuckDB representation.

    DuckDB MAPs are internally LIST(STRUCT(key K, value V)),
    so they can also be accessed as List[MapEntryStrInt].
    """
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': 2} AS m")
    var chunk = result.fetch_chunk()
    var entries = chunk.get[List[MapEntryStrInt]](col=0, row=0)

    assert_equal(len(entries), 2)
    assert_equal(entries[0].key, "a")
    assert_equal(entries[0].value.value(), 1)
    assert_equal(entries[1].key, "b")
    assert_equal(entries[1].value.value(), 2)


def test_map_as_list_struct_with_nulls() raises:
    """Deserialize a MAP as List[Struct] with NULL values."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': NULL::INTEGER} AS m")
    var chunk = result.fetch_chunk()
    var entries = chunk.get[List[MapEntryStrInt]](col=0, row=0)

    assert_equal(len(entries), 2)
    assert_equal(entries[0].key, "a")
    assert_equal(entries[0].value.value(), 1)
    assert_equal(entries[1].key, "b")
    assert_false(entries[1].value)  # NULL value


# ──────────────────────────────────────────────────────────────────
# UNION tests
# ──────────────────────────────────────────────────────────────────


def test_union_single_int_member() raises:
    """Deserialize a UNION value where the INTEGER member is active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(num := 42::INTEGER)::UNION(num INTEGER, str VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[NumOrStr](col=0, row=0)
    assert_equal(val.num.value(), 42)
    assert_false(val.str)  # inactive member is None


def test_union_single_str_member() raises:
    """Deserialize a UNION value where the VARCHAR member is active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(str := 'hello')::UNION(num INTEGER, str VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[NumOrStr](col=0, row=0)
    assert_false(val.num)  # inactive member is None
    assert_equal(val.str.value(), "hello")


def test_union_column() raises:
    """Deserialize a full column of UNION values with mixed active tags."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))")
    _ = con.execute("INSERT INTO t VALUES (1), ('two'), (3)")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[NumOrStr](col=0)

    assert_equal(len(vals), 3)

    # Row 0: tag=num, value=1
    assert_equal(vals[0].num.value(), 1)
    assert_false(vals[0].str)

    # Row 1: tag=str, value='two'
    assert_false(vals[1].num)
    assert_equal(vals[1].str.value(), "two")

    # Row 2: tag=num, value=3
    assert_equal(vals[2].num.value(), 3)
    assert_false(vals[2].str)


def test_union_three_members() raises:
    """Deserialize a UNION with three member types."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR, flag BOOLEAN))"
    )
    _ = con.execute("INSERT INTO t VALUES (42), ('hello'), (true)")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[NumOrStrOrBool](col=0)

    assert_equal(len(vals), 3)

    # Row 0: num active
    assert_equal(vals[0].num.value(), 42)
    assert_false(vals[0].str)
    assert_false(vals[0].flag)

    # Row 1: str active
    assert_false(vals[1].num)
    assert_equal(vals[1].str.value(), "hello")
    assert_false(vals[1].flag)

    # Row 2: flag active
    assert_false(vals[2].num)
    assert_false(vals[2].str)
    assert_equal(vals[2].flag.value(), True)


def test_union_with_null() raises:
    """Deserialize a UNION column where some rows are NULL."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))")
    _ = con.execute("INSERT INTO t VALUES (1), (NULL), ('three')")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[NumOrStr]](col=0)

    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(vals[0].value().num.value(), 1)
    assert_false(vals[1])  # NULL row
    assert_true(vals[2])
    assert_equal(vals[2].value().str.value(), "three")


# ──────────────────────────────────────────────────────────────────
# UNION-as-Variant tests
# ──────────────────────────────────────────────────────────────────


def test_variant_union_int_member() raises:
    """Deserialize a UNION value as Variant — INTEGER member active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(num := 42::INTEGER)::UNION(num INTEGER, str"
        " VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(val.isa[Int32]())
    assert_equal(val[Int32], 42)
    assert_false(val.isa[String]())


def test_variant_union_str_member() raises:
    """Deserialize a UNION value as Variant — VARCHAR member active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(str := 'hello')::UNION(num INTEGER, str"
        " VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_false(val.isa[Int32]())
    assert_true(val.isa[String]())
    assert_equal(val[String], "hello")


def test_variant_union_column() raises:
    """Deserialize a full column of UNION values as Variant."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))")
    _ = con.execute("INSERT INTO t VALUES (1), ('two'), (3)")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Variant[Int32, String]](col=0)

    assert_equal(len(vals), 3)

    # Row 0: tag=num, value=1
    assert_true(vals[0].isa[Int32]())
    assert_equal(vals[0][Int32], 1)

    # Row 1: tag=str, value='two'
    assert_true(vals[1].isa[String]())
    assert_equal(vals[1][String], "two")

    # Row 2: tag=num, value=3
    assert_true(vals[2].isa[Int32]())
    assert_equal(vals[2][Int32], 3)


def test_variant_union_three_members() raises:
    """Deserialize a UNION with three member types as Variant."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR, flag BOOLEAN))"
    )
    _ = con.execute("INSERT INTO t VALUES (42), ('hello'), (true)")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Variant[Int32, String, Bool]](col=0)

    assert_equal(len(vals), 3)

    # Row 0: num active
    assert_true(vals[0].isa[Int32]())
    assert_equal(vals[0][Int32], 42)

    # Row 1: str active
    assert_true(vals[1].isa[String]())
    assert_equal(vals[1][String], "hello")

    # Row 2: flag active
    assert_true(vals[2].isa[Bool]())
    assert_equal(vals[2][Bool], True)


def test_variant_union_with_null() raises:
    """Deserialize a UNION column as Optional[Variant] — NULL handling."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))")
    _ = con.execute("INSERT INTO t VALUES (1), (NULL), ('three')")
    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[Variant[Int32, String]]](col=0)

    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(vals[0].value()[Int32], 1)
    assert_false(vals[1])  # NULL row
    assert_true(vals[2])
    assert_equal(vals[2].value()[String], "three")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
