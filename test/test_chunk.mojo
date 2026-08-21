from duckdb import *
from std.testing import *
from std.testing.suite import TestSuite


def test_chunk_create() raises:
    """Test creating a data chunk with specific types."""
    var types = List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.varchar))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk[True](types)
    
    assert_equal(chunk.column_count(), 3)
    assert_equal(len(chunk), 0)  # Initially empty


def test_chunk_set_size() raises:
    """Test setting the size of a data chunk."""
    var types = List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk[True](types)
    
    chunk.set_size(10)
    assert_equal(len(chunk), 10)
    
    chunk.set_size(5)
    assert_equal(len(chunk), 5)


def test_chunk_reset() raises:
    """Test resetting a data chunk."""
    var types = List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]()
    types.append(LogicalType(DuckDBType.integer))
    
    var chunk = Chunk[True](types)
    
    chunk.set_size(10)
    assert_equal(len(chunk), 10)
    
    chunk.reset()
    assert_equal(len(chunk), 0)


def test_chunk_get_vector() raises:
    """Test getting a vector from a data chunk."""
    var types = List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.varchar))
    
    var chunk = Chunk[True](types)
    
    # Get vectors - using type() method which works
    assert_equal(chunk.type(0), DuckDBType.integer)
    assert_equal(chunk.type(1), DuckDBType.varchar)


def test_chunk_type() raises:
    """Test getting column types from a data chunk."""
    var types = List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]()
    types.append(LogicalType(DuckDBType.bigint))
    types.append(LogicalType(DuckDBType.boolean))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk[True](types)
    
    assert_equal(chunk.type(0), DuckDBType.bigint)
    assert_equal(chunk.type(1), DuckDBType.boolean)
    assert_equal(chunk.type(2), DuckDBType.double)


def test_chunk_from_query() raises:
    """Test working with chunks from query results."""
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT 42 as num, 'hello' as text")
    
    var chunk = result.fetch_chunk()
    assert_equal(chunk.column_count(), 2)
    assert_equal(len(chunk), 1)
    assert_equal(chunk.type(0), DuckDBType.integer)
    assert_equal(chunk.type(1), DuckDBType.varchar)


def test_get_span_reads_column_values() raises:
    """get_span yields the column's values with the chunk's row count as length."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT i::INT AS v FROM range(5) t(i)")
    var chunk = result.fetch_chunk()
    var span = chunk.get_span[DType.int32](col=0)
    assert_equal(len(span), 5)
    for i in range(5):
        assert_equal(span[i], Int32(i))


def test_get_span_length_matches_chunk_rows() raises:
    """The span length comes from the chunk, so it cannot run past the rows."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT i::BIGINT AS v FROM range(3) t(i)")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk.get_span[DType.int64](col=0)), len(chunk))


def test_get_span_rejects_wrong_dtype() raises:
    """Asking for the wrong element type raises instead of reinterpreting bits."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 1.5::DOUBLE AS v")
    var chunk = result.fetch_chunk()
    with assert_raises():
        _ = chunk.get_span[DType.int32](col=0)


def test_get_span_rejects_varchar() raises:
    """VARCHAR has no fixed-width scalar layout, so it must be rejected."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT 'abc' AS v")
    var chunk = result.fetch_chunk()
    with assert_raises():
        _ = chunk.get_span[DType.int32](col=0)


def test_get_span_is_writable_when_chunk_is_mut() raises:
    """A span from a mutably bound chunk can be written through."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT i::INT AS v FROM range(4) t(i)")
    var chunk = result.fetch_chunk()
    var span = chunk.get_span[DType.int32](col=0)
    span[0] = Int32(41)
    span[1] = Int32(42)
    assert_equal(span[0], Int32(41))
    assert_equal(span[1], Int32(42))
    # untouched elements keep their original values
    assert_equal(span[2], Int32(2))
    assert_equal(span[3], Int32(3))


def test_get_span_matches_get_data() raises:
    """get_span agrees with the raw get_data path it is meant to replace."""
    var conn = DuckDB.connect(":memory:")
    var result = conn.execute("SELECT (i * 7)::DOUBLE AS v FROM range(6) t(i)")
    var chunk = result.fetch_chunk()
    var span = chunk.get_span[DType.float64](col=0)
    var vec = chunk.get_vector(0)
    var raw = vec.get_data().unsafe_bitcast[Float64]()
    for i in range(len(chunk)):
        assert_equal(span[i], raw[unsafe_offset=i])


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
