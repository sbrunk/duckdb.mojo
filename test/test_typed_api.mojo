"""Tests for the new typed API."""

from duckdb import *
from testing import assert_equal, assert_false, assert_raises, assert_true
from testing.suite import TestSuite
from reflection import struct_field_count


def test_scalar_types_new_api():
    """Test scalar types with the new get[T] API."""
    con = DuckDB.connect(":memory:")

    # Boolean
    result = con.execute("SELECT true")
    var chunk = result.fetch_chunk()
    var bool_val = chunk.get[Bool](col=0, row=0)
    assert_true(bool_val)
    assert_equal(bool_val, True)

    # Int types
    result = con.execute("SELECT -42::TINYINT")
    chunk = result.fetch_chunk()
    var tinyint_val = chunk.get[Int8](col=0, row=0)
    assert_equal(tinyint_val, -42)

    result = con.execute("SELECT 42::UTINYINT")
    chunk = result.fetch_chunk() 
    var utinyint_val = chunk.get[UInt8](col=0, row=0)
    assert_equal(utinyint_val, 42)

    result = con.execute("SELECT -42::SMALLINT")
    chunk = result.fetch_chunk()
    var smallint_val = chunk.get[Int16](col=0, row=0)
    assert_equal(smallint_val, -42)

    result = con.execute("SELECT 42::USMALLINT")
    chunk = result.fetch_chunk()
    var usmallint_val = chunk.get[UInt16](col=0, row=0)
    assert_equal(usmallint_val, 42)

    result = con.execute("SELECT -42::INTEGER")
    chunk = result.fetch_chunk()
    var int_val = chunk.get[Int32](col=0, row=0)
    assert_equal(int_val, -42)

    result = con.execute("SELECT 42::UINTEGER")
    chunk = result.fetch_chunk()
    var uint_val = chunk.get[UInt32](col=0, row=0)
    assert_equal(uint_val, 42)

    result = con.execute("SELECT -42::BIGINT")
    chunk = result.fetch_chunk()
    var bigint_val = chunk.get[Int64](col=0, row=0)
    assert_equal(bigint_val, -42)

    result = con.execute("SELECT 42::UBIGINT")
    chunk = result.fetch_chunk()
    var ubigint_val = chunk.get[UInt64](col=0, row=0)
    assert_equal(ubigint_val, 42)

    # Float types
    result = con.execute("SELECT 42.5::FLOAT")
    chunk = result.fetch_chunk()
    var float_val = chunk.get[Float32](col=0, row=0)
    assert_equal(float_val, 42.5)

    result = con.execute("SELECT 42.5::DOUBLE")
    chunk = result.fetch_chunk()
    var double_val = chunk.get[Float64](col=0, row=0)
    assert_equal(double_val, 42.5)

    # String
    result = con.execute("SELECT 'hello'")
    chunk = result.fetch_chunk()
    var str_val = chunk.get[String](col=0, row=0)
    assert_equal(str_val, "hello")

    result = con.execute("SELECT 'hello longer varchar'")
    chunk = result.fetch_chunk()
    var long_str_val = chunk.get[String](col=0, row=0)
    assert_equal(long_str_val, "hello longer varchar")


def test_date_time_types_new_api():
    """Test date/time types with the new API."""
    con = DuckDB.connect(":memory:")

    # Timestamp
    result = con.execute("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789'")
    var chunk = result.fetch_chunk()
    var ts_val = chunk.get[Timestamp](col=0, row=0)
    assert_equal(ts_val, Timestamp(716988600123456))

    # Date
    result = con.execute("SELECT DATE '1992-09-20'")
    chunk = result.fetch_chunk()
    var date_val = chunk.get[Date](col=0, row=0)
    assert_equal(date_val, Date(8298))

    # Time
    result = con.execute("SELECT TIME '1992-09-20 11:30:00.123456'")
    chunk = result.fetch_chunk()
    var time_val = chunk.get[Time](col=0, row=0)
    assert_equal(time_val, Time(41400123456))


def test_null_handling_new_api():
    """Test NULL handling with the new API."""
    con = DuckDB.connect(":memory:")

    # NULL integer
    result = con.execute("SELECT null::INT")
    var chunk = result.fetch_chunk()
    var null_val = chunk.get[Optional[Int32]](col=0, row=0)
    assert_false(null_val)

    # Mixed nulls and values
    result = con.execute("SELECT * FROM (VALUES (1), (null), (3)) AS t(x)")
    chunk = result.fetch_chunk()
    
    var val1 = chunk.get[Int32](col=0, row=0)
    assert_equal(val1, 1)
    
    var val2 = chunk.get[Optional[Int32]](col=0, row=1)
    assert_false(val2)
    
    var val3 = chunk.get[Int32](col=0, row=2)
    assert_equal(val3, 3)


def test_column_retrieval_new_api():
    """Test getting all values from a column."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)")
    var chunk = result.fetch_chunk()
    var values = chunk.get[Int32](col=0)
    
    assert_equal(len(values), 5)
    for i in range(5):
        assert_equal(values[i], Int32(i + 1))


def test_multiple_columns_new_api():
    """Test retrieving multiple columns with different types."""
    con = DuckDB.connect(":memory:")

    result = con.execute("""
        SELECT * FROM (VALUES 
            (1, 'alice', 25.5::DOUBLE),
            (2, 'bob', 30.0::DOUBLE),
            (3, 'charlie', 35.5::DOUBLE)
        ) AS t(id, name, score)
    """)
    var chunk = result.fetch_chunk()
    
    var ids = chunk.get[Int32](col=0)
    var names = chunk.get[String](col=1)
    var scores = chunk.get[Float64](col=2)
    
    assert_equal(len(ids), 3)
    assert_equal(len(names), 3)
    assert_equal(len(scores), 3)
    
    assert_equal(ids[0], 1)
    assert_equal(names[0], "alice")
    assert_equal(scores[0], 25.5)
    
    assert_equal(ids[2], 3)
    assert_equal(names[2], "charlie")
    assert_equal(scores[2], 35.5)


def test_type_mismatch_new_api():
    """Test that type mismatches raise errors."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 'hello'")
    var chunk = result.fetch_chunk()
    
    # Try to get a string as an int
    with assert_raises(contains="Type mismatch"):
        _ = chunk.get[Int32](col=0, row=0)


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
# MojoType descriptor tests
# ──────────────────────────────────────────────────────────────────


def test_mojo_type_scalar():
    """Test MojoType construction for scalar types."""
    var mt = MojoType(DuckDBType.bigint)
    assert_equal(String(mt), String(DuckDBType.bigint))
    assert_equal(mt.type_id, DuckDBType.bigint)
    assert_equal(len(mt.children), 0)
    assert_equal(len(mt.field_names), 0)


def test_mojo_type_list():
    """Test MojoType.list_of() construction."""
    var element = MojoType(DuckDBType.integer)
    var list_mt = MojoType.list_of(element^)
    assert_equal(list_mt.type_id, DuckDBType.list)
    assert_equal(len(list_mt.children), 1)
    assert_equal(list_mt.children[0].type_id, DuckDBType.integer)
    assert_equal(String(list_mt), "list(integer)")


def test_mojo_type_struct():
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


def test_mojo_logical_type_scalar():
    """Test mojo_logical_type[T]() for scalar types."""
    var mt_int = mojo_logical_type[Int64]()
    assert_equal(mt_int.type_id, DuckDBType.bigint)
    var mt_str = mojo_logical_type[String]()
    assert_equal(mt_str.type_id, DuckDBType.varchar)
    var mt_f64 = mojo_logical_type[Float64]()
    assert_equal(mt_f64.type_id, DuckDBType.double)


def test_mojo_logical_type_struct():
    """Test mojo_logical_type[T]() for struct types using reflection."""
    var mt = mojo_logical_type[Point]()
    assert_equal(mt.type_id, DuckDBType.struct_t)
    assert_equal(len(mt.field_names), 2)
    assert_equal(mt.field_names[0], "x")
    assert_equal(mt.field_names[1], "y")
    assert_equal(mt.children[0].type_id, DuckDBType.double)
    assert_equal(mt.children[1].type_id, DuckDBType.double)


def test_mojo_type_to_logical_type():
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


def test_struct_deserialization_numeric():
    """Test deserializing DuckDB STRUCT into a Mojo struct with numeric fields."""
    con = DuckDB.connect(":memory:")

    result = con.execute(
        "SELECT {'x': 1.5::DOUBLE, 'y': 2.5::DOUBLE} AS pt"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[Point](col=0, row=0)
    assert_equal(val.x, 1.5)
    assert_equal(val.y, 2.5)


def test_struct_deserialization_column():
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


def test_struct_deserialization_mixed_int_types():
    """Test struct with mixed integer types."""
    con = DuckDB.connect(":memory:")

    result = con.execute(
        "SELECT {'a': 10::INTEGER, 'b': 20::BIGINT} AS pair"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[IntPair](col=0, row=0)
    assert_equal(val.a, 10)
    assert_equal(val.b, 20)


def test_struct_null_handling():
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


def test_logical_type_struct_creation():
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


# ──────────────────────────────────────────────────────────────────
# List deserialization tests
# ──────────────────────────────────────────────────────────────────


def test_list_column_int32():
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


def test_list_column_varchar():
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


def test_list_column_with_nulls():
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


def test_list_column_empty_lists():
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


def test_list_column_float64():
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


def test_list_via_get():
    """Deserialize a LIST(INTEGER) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    _ = con.execute("INSERT INTO t VALUES ([10, 20]), ([30])")
    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()

    # T = List[Optional[Int32]] → returns List[List[Optional[Int32]]]
    var lists = chunk.get[List[Optional[Int32]]](col=0)

    assert_equal(len(lists), 2)

    var row0 = lists[0].copy()
    assert_equal(len(row0), 2)
    assert_equal(row0[0].value(), 10)
    assert_equal(row0[1].value(), 20)

    var row1 = lists[1].copy()
    assert_equal(len(row1), 1)
    assert_equal(row1[0].value(), 30)


def test_list_via_get_with_nulls():
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


def test_nested_list():
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


def test_nested_list_with_nulls():
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
# Table-to-struct deserialization tests
# ──────────────────────────────────────────────────────────────────


def test_table_struct_single_row():
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


def test_table_struct_all_rows():
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


def test_table_struct_with_nulls():
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

    # Row 0: all non-null → succeeds
    var user0 = chunk.get[UserRecord](row=0)
    assert_equal(user0.name, "Alice")

    # Row 1: name is NULL → raises
    with assert_raises():
        _ = chunk.get[UserRecord](row=1)

    # Row 2: age is NULL → raises
    with assert_raises():
        _ = chunk.get[UserRecord](row=2)


def test_table_struct_all_rows_with_nulls():
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


def test_table_struct_column_count_mismatch():
    """Error when column count doesn't match struct field count."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, age BIGINT)")
    _ = con.execute("INSERT INTO t VALUES ('Alice', 30)")
    var result = con.execute("SELECT name, age FROM t")
    var chunk = result.fetch_chunk()

    # UserRecord has 3 fields but the query returns 2 columns
    with assert_raises(contains="Column count mismatch"):
        _ = chunk.get[UserRecord](row=0)


def test_table_struct_type_mismatch():
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


def test_table_struct_row_out_of_bounds():
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


def test_table_struct_with_list_field():
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


def test_table_struct_with_nested_struct():
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


def test_materialized_struct_single_row():
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


def test_materialized_struct_all_rows():
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


def test_materialized_struct_with_nulls_raises():
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

    # Row 1 has NULL name → raises
    with assert_raises():
        _ = result.get[UserRecord](row=1)

    # get all rows also raises
    with assert_raises():
        _ = result.get[UserRecord]()


def test_materialized_struct_row_out_of_bounds():
    """MaterializedResult.get[T](row=) raises on out-of-bounds row."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (name VARCHAR, age BIGINT, active BOOLEAN)")
    _ = con.execute("INSERT INTO t VALUES ('Alice', 30, true)")
    var result = con.execute("SELECT * FROM t").fetchall()

    with assert_raises():
        _ = result.get[UserRecord](row=5)


def test_materialized_struct_with_list_field():
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


# ──────────────────────────────────────────────────────────────────
# Tuple deserialization
# ──────────────────────────────────────────────────────────────────


def test_tuple_basic():
    """Deserialize a row into a Tuple of basic types."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 42::INTEGER, 3.14::DOUBLE, 'hello'::VARCHAR"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Int32, Float64, String](row=0)
    assert_equal(t[0], 42)
    assert_equal(t[1], 3.14)
    assert_equal(t[2], "hello")


def test_tuple_all_rows():
    """Deserialize all rows into a List of Tuples."""
    var conn = DuckDB.connect(":memory:")
    var query = (
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
    )
    var chunk = conn.execute(query).fetch_chunk()
    var rows = chunk.get_tuple[Int32, String]()
    assert_equal(len(rows), 3)
    assert_equal(rows[0][0], 1)
    assert_equal(rows[0][1], "a")
    assert_equal(rows[1][0], 2)
    assert_equal(rows[1][1], "b")
    assert_equal(rows[2][0], 3)
    assert_equal(rows[2][1], "c")


def test_tuple_nullable():
    """Deserialize a Tuple with Optional elements for NULL handling."""
    var conn = DuckDB.connect(":memory:")
    var query = (
        "SELECT * FROM (VALUES (1, 'a'), (NULL, 'b'), (3, NULL))"
        " AS t(id, name)"
    )
    var chunk = conn.execute(query).fetch_chunk()

    var t0 = chunk.get_tuple[Optional[Int32], Optional[String]](row=0)
    assert_equal(t0[0].value(), 1)
    assert_equal(t0[1].value(), "a")

    var t1 = chunk.get_tuple[Optional[Int32], Optional[String]](row=1)
    assert_false(t1[0])
    assert_equal(t1[1].value(), "b")

    var t2 = chunk.get_tuple[Optional[Int32], Optional[String]](row=2)
    assert_equal(t2[0].value(), 3)
    assert_false(t2[1])


def test_tuple_from_row():
    """Deserialize a Tuple via Row.get_tuple inside a for loop."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT * FROM (VALUES (0, 'x'), (1, 'y'), (2, 'z')) AS t(id, label)"
    var expected_labels = ["x", "y", "z"]
    var idx = 0
    for row in conn.execute(query):
        var t = row.get_tuple[Int32, String]()
        assert_equal(t[0], Int32(idx))
        assert_equal(t[1], expected_labels[idx])
        idx += 1
    assert_equal(idx, 3)


def test_tuple_bigint_types():
    """Deserialize a Tuple with various integer widths."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 1::TINYINT, 2::SMALLINT, 3::INTEGER, 4::BIGINT"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Int8, Int16, Int32, Int64](row=0)
    assert_equal(t[0], Int8(1))
    assert_equal(t[1], Int16(2))
    assert_equal(t[2], Int32(3))
    assert_equal(t[3], Int64(4))


def test_tuple_boolean():
    """Deserialize a Tuple containing Bool values."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT TRUE::BOOLEAN, FALSE::BOOLEAN"
    var chunk = conn.execute(query).fetch_chunk()
    var t = chunk.get_tuple[Bool, Bool](row=0)
    assert_true(t[0])
    assert_false(t[1])


def test_tuple_column_count_mismatch():
    """Error when Tuple element count doesn't match column count."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT 1::INT, 2::INT, 3::INT"
    var chunk = conn.execute(query).fetch_chunk()
    
    # 2 elements for 3 columns
    with assert_raises():
        _ = chunk.get_tuple[Int32, Int32](row=0)


def test_tuple_null_non_optional_raises():
    """Error when NULL encountered for non-Optional Tuple element."""
    var conn = DuckDB.connect(":memory:")
    var query = "SELECT NULL::INTEGER"
    var chunk = conn.execute(query).fetch_chunk()
    
    with assert_raises():
        _ = chunk.get_tuple[Int32](row=0)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
