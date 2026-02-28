"""Tests for the new typed API."""

from duckdb import *
from utils import Variant
from collections import Dict
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

    # Mojo Int (platform-dependent width, maps to BIGINT on 64-bit)
    result = con.execute("SELECT 42::BIGINT")
    chunk = result.fetch_chunk()
    var mojo_int_val = chunk.get[Int](col=0, row=0)
    assert_equal(mojo_int_val, 42)

    # Mojo UInt (platform-dependent width, maps to UBIGINT on 64-bit)
    result = con.execute("SELECT 42::UBIGINT")
    chunk = result.fetch_chunk()
    var mojo_uint_val = chunk.get[UInt](col=0, row=0)
    assert_equal(mojo_uint_val, 42)


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


def test_timestamp_variants_typed_api():
    """Test TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS with get[T]."""
    con = DuckDB.connect(":memory:")

    # TIMESTAMP_S
    result = con.execute("SELECT TIMESTAMP_S '2021-01-01 00:00:00'")
    var chunk = result.fetch_chunk()
    var ts_s = chunk.get[TimestampS](col=0, row=0)
    assert_equal(ts_s, TimestampS(1609459200))

    # TIMESTAMP_MS
    result = con.execute("SELECT TIMESTAMP_MS '2021-01-01 00:00:00'")
    chunk = result.fetch_chunk()
    var ts_ms = chunk.get[TimestampMS](col=0, row=0)
    assert_equal(ts_ms, TimestampMS(1609459200000))

    # TIMESTAMP_NS
    result = con.execute("SELECT TIMESTAMP_NS '2021-01-01 00:00:00'")
    chunk = result.fetch_chunk()
    var ts_ns = chunk.get[TimestampNS](col=0, row=0)
    assert_equal(ts_ns, TimestampNS(1609459200000000000))


def test_timestamp_tz_typed_api():
    """Test TIMESTAMPTZ with get[T]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("SET TimeZone = 'UTC'")

    result = con.execute("SELECT TIMESTAMPTZ '2021-01-01 00:00:00+00'")
    var chunk = result.fetch_chunk()
    var ts_tz = chunk.get[TimestampTZ](col=0, row=0)
    assert_equal(ts_tz, TimestampTZ(1609459200000000))


def test_time_tz_typed_api():
    """Test TIMETZ with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT TIMETZ '12:30:00+02:00'")
    var chunk = result.fetch_chunk()
    var ttz = chunk.get[TimeTZ](col=0, row=0)

    # Build the expected value using the same helper
    var expected = TimeTZ(
        micros=Int64(12) * 3600 * 1_000_000 + Int64(30) * 60 * 1_000_000,
        offset=Int32(7200),
    )
    assert_equal(ttz, expected)


def test_uuid_typed_api():
    """Test UUID with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID")
    var chunk = result.fetch_chunk()
    var uuid = chunk.get[UUID](col=0, row=0)

    # Verify round-trip: read UUID, cast back to string via SQL
    _ = con.execute("CREATE TABLE t (id UUID)")
    var appender = Appender(con, "t")
    appender.append_value(uuid)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    assert_equal(
        chunk.get[String](col=0, row=0),
        "550e8400-e29b-41d4-a716-446655440000",
    )


def test_uuid_zero_typed_api():
    """Test UUID zero value with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT '00000000-0000-0000-0000-000000000000'::UUID")
    var chunk = result.fetch_chunk()
    var uuid = chunk.get[UUID](col=0, row=0)
    assert_equal(uuid, UUID(UInt128(0)))


def test_hugeint_typed_api():
    """Test HUGEINT (Int128) with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 123456789012345::HUGEINT")
    var chunk = result.fetch_chunk()
    var val = chunk.get[Int128](col=0, row=0)
    assert_equal(val, Int128(123456789012345))

    # Negative value
    result = con.execute("SELECT (-42)::HUGEINT")
    chunk = result.fetch_chunk()
    var neg = chunk.get[Int128](col=0, row=0)
    assert_equal(neg, Int128(-42))

    # Zero
    result = con.execute("SELECT 0::HUGEINT")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int128](col=0, row=0), Int128(0))


def test_uhugeint_typed_api():
    """Test UHUGEINT (UInt128) with get[T]."""
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT 999999999999999::UHUGEINT")
    var chunk = result.fetch_chunk()
    var val = chunk.get[UInt128](col=0, row=0)
    assert_equal(val, UInt128(999999999999999))

    # Zero
    result = con.execute("SELECT 0::UHUGEINT")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt128](col=0, row=0), UInt128(0))


def test_decimal_typed_api():
    """Test DECIMAL with get[T]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE d (val DECIMAL(10, 2))")
    _ = con.execute("INSERT INTO d VALUES (123.45)")
    _ = con.execute("INSERT INTO d VALUES (-99.99)")

    result = con.execute("SELECT val FROM d ORDER BY rowid")
    var chunk = result.fetch_chunk()

    var d1 = chunk.get[Decimal](col=0, row=0)
    assert_equal(d1.width, UInt8(10))
    assert_equal(d1.scale, UInt8(2))
    assert_equal(d1.value(), Int128(12345))

    var d2 = chunk.get[Decimal](col=0, row=1)
    assert_equal(d2.value(), Int128(-9999))


def test_timestamp_variants_column_api():
    """Test fetching a full column of timestamp variants."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE ts (a TIMESTAMP_S, b TIMESTAMP_MS, c TIMESTAMP_NS)")
    _ = con.execute("INSERT INTO ts VALUES (TIMESTAMP_S '2021-01-01', TIMESTAMP_MS '2021-01-01', TIMESTAMP_NS '2021-01-01')")
    _ = con.execute("INSERT INTO ts VALUES (TIMESTAMP_S '1970-01-01', TIMESTAMP_MS '1970-01-01', TIMESTAMP_NS '1970-01-01')")

    result = con.execute("SELECT a, b, c FROM ts ORDER BY rowid")
    var chunk = result.fetch_chunk()

    var col_s = chunk.get[TimestampS](col=0)
    assert_equal(len(col_s), 2)
    assert_equal(col_s[0], TimestampS(1609459200))
    assert_equal(col_s[1], TimestampS(0))

    var col_ms = chunk.get[TimestampMS](col=1)
    assert_equal(len(col_ms), 2)
    assert_equal(col_ms[0], TimestampMS(1609459200000))
    assert_equal(col_ms[1], TimestampMS(0))

    var col_ns = chunk.get[TimestampNS](col=2)
    assert_equal(len(col_ns), 2)
    assert_equal(col_ns[0], TimestampNS(1609459200000000000))
    assert_equal(col_ns[1], TimestampNS(0))


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


# ──────────────────────────────────────────────────────────────────
# ARRAY tests
# ──────────────────────────────────────────────────────────────────


def test_array_column_int32():
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


def test_array_single_row():
    """Deserialize a single ARRAY row."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT [10, 20]::INTEGER[2] AS arr")
    var chunk = result.fetch_chunk()
    var arr = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(arr), 2)
    assert_equal(arr[0].value(), 10)
    assert_equal(arr[1].value(), 20)


def test_array_column_varchar():
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


def test_array_column_with_null_rows():
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


def test_array_column_float64():
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


def test_map_single_row():
    """Deserialize a single MAP(VARCHAR, INTEGER) row as Dict."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': 2} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, Int32]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["a"], 1)
    assert_equal(d["b"], 2)


def test_map_column():
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


def test_map_with_null_values():
    """Deserialize a MAP where some values are NULL using Optional."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {'a': 1, 'b': NULL::INTEGER} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[String, Optional[Int32]]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d["a"].value(), 1)
    assert_false(d["b"])  # NULL value


def test_map_with_null_rows():
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


def test_map_int_keys():
    """Deserialize a MAP with integer keys."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT MAP {1: 'one', 2: 'two'} AS m")
    var chunk = result.fetch_chunk()
    var d = chunk.get[Dict[Int32, String]](col=0, row=0)

    assert_equal(len(d), 2)
    assert_equal(d[Int32(1)], "one")
    assert_equal(d[Int32(2)], "two")


def test_map_with_mojo_int():
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


def test_map_with_mojo_uint():
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


def test_map_as_list_struct_single_row():
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


def test_map_as_list_struct_with_nulls():
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


def test_union_single_int_member():
    """Deserialize a UNION value where the INTEGER member is active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(num := 42::INTEGER)::UNION(num INTEGER, str VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[NumOrStr](col=0, row=0)
    assert_equal(val.num.value(), 42)
    assert_false(val.str)  # inactive member is None


def test_union_single_str_member():
    """Deserialize a UNION value where the VARCHAR member is active."""
    con = DuckDB.connect(":memory:")
    result = con.execute(
        "SELECT union_value(str := 'hello')::UNION(num INTEGER, str VARCHAR) AS u"
    )
    var chunk = result.fetch_chunk()
    var val = chunk.get[NumOrStr](col=0, row=0)
    assert_false(val.num)  # inactive member is None
    assert_equal(val.str.value(), "hello")


def test_union_column():
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


def test_union_three_members():
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


def test_union_with_null():
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


def test_variant_union_int_member():
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


def test_variant_union_str_member():
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


def test_variant_union_column():
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


def test_variant_union_three_members():
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


def test_variant_union_with_null():
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


def test_blob_deserialize():
    """Read BLOB column as List[UInt8] via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BLOB)")
    _ = con.execute("INSERT INTO t VALUES ('\\x01\\x02\\x03'::BLOB), ('\\xDE\\xAD'::BLOB)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[UInt8]](col=0, row=0)
    assert_equal(len(row0), 3)
    assert_equal(row0[0], 1)
    assert_equal(row0[1], 2)
    assert_equal(row0[2], 3)
    var row1 = chunk.get[List[UInt8]](col=0, row=1)
    assert_equal(len(row1), 2)
    assert_equal(row1[0], 0xDE)
    assert_equal(row1[1], 0xAD)


def test_blob_deserialize_column():
    """Read BLOB column as full column of List[UInt8]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BLOB)")
    _ = con.execute("INSERT INTO t VALUES ('\\x01\\x02'::BLOB), (NULL)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[List[UInt8]]](col=0)
    assert_equal(len(vals), 2)
    assert_true(vals[0])
    assert_equal(len(vals[0].value()), 2)
    assert_false(vals[1])


def test_enum_deserialize():
    """Read ENUM column as String via typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
    _ = con.execute("CREATE TABLE t (m mood)")
    _ = con.execute("INSERT INTO t VALUES ('happy'), ('sad'), ('neutral')")
    result = con.execute("SELECT m FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "happy")
    assert_equal(chunk.get[String](col=0, row=1), "sad")
    assert_equal(chunk.get[String](col=0, row=2), "neutral")


def test_enum_deserialize_with_null():
    """Read ENUM column with NULL values as Optional[String]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
    _ = con.execute("CREATE TABLE t (c color)")
    _ = con.execute("INSERT INTO t VALUES ('red'), (NULL), ('blue')")
    result = con.execute("SELECT c FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[String]](col=0)
    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(vals[0].value(), "red")
    assert_false(vals[1])
    assert_true(vals[2])
    assert_equal(vals[2].value(), "blue")


def test_bit_deserialize():
    """Read BIT column as Bit."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    _ = con.execute("INSERT INTO t VALUES ('10110'::BIT), ('0'::BIT), ('11111111'::BIT)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var b0 = chunk.get[Bit](col=0, row=0)
    assert_equal(String(b0), "10110")
    assert_equal(len(b0), 5)
    var b1 = chunk.get[Bit](col=0, row=1)
    assert_equal(String(b1), "0")
    var b2 = chunk.get[Bit](col=0, row=2)
    assert_equal(String(b2), "11111111")
    assert_equal(len(b2), 8)


def test_bit_deserialize_column():
    """Read entire BIT column as List[Optional[Bit]]."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    _ = con.execute("INSERT INTO t VALUES ('101'::BIT), (NULL), ('0'::BIT)")
    result = con.execute("SELECT b FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Optional[Bit]](col=0)
    assert_equal(len(vals), 3)
    assert_true(vals[0])
    assert_equal(String(vals[0].value()), "101")
    assert_false(vals[1])
    assert_true(vals[2])
    assert_equal(String(vals[2].value()), "0")


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
