from duckdb import *
from testing import *
from testing.suite import TestSuite


def test_chunk_create():
    """Test creating a data chunk with specific types."""
    var types = List[LogicalType]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.varchar))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk(types)
    
    assert_equal(chunk.column_count(), 3)
    assert_equal(len(chunk), 0)  # Initially empty


def test_chunk_set_size():
    """Test setting the size of a data chunk."""
    var types = List[LogicalType]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk(types)
    
    chunk.set_size(10)
    assert_equal(len(chunk), 10)
    
    chunk.set_size(5)
    assert_equal(len(chunk), 5)


def test_chunk_reset():
    """Test resetting a data chunk."""
    var types = List[LogicalType]()
    types.append(LogicalType(DuckDBType.integer))
    
    var chunk = Chunk(types)
    
    chunk.set_size(10)
    assert_equal(len(chunk), 10)
    
    chunk.reset()
    assert_equal(len(chunk), 0)


def test_chunk_get_vector():
    """Test getting a vector from a data chunk."""
    var types = List[LogicalType]()
    types.append(LogicalType(DuckDBType.integer))
    types.append(LogicalType(DuckDBType.varchar))
    
    var chunk = Chunk(types)
    
    var vec0 = chunk.get_vector(0)
    var vec1 = chunk.get_vector(1)
    
    assert_equal(vec0.get_column_type().get_type_id(), DuckDBType.integer)
    assert_equal(vec1.get_column_type().get_type_id(), DuckDBType.varchar)


def test_chunk_type():
    """Test getting column types from a data chunk."""
    var types = List[LogicalType]()
    types.append(LogicalType(DuckDBType.bigint))
    types.append(LogicalType(DuckDBType.boolean))
    types.append(LogicalType(DuckDBType.double))
    
    var chunk = Chunk(types)
    
    assert_equal(chunk.type(0), DuckDBType.bigint)
    assert_equal(chunk.type(1), DuckDBType.boolean)
    assert_equal(chunk.type(2), DuckDBType.double)


def test_chunk_from_query():
    """Test working with chunks from query results."""
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42 as num, 'hello' as text")
    
    var chunk = result.fetch_chunk()
    assert_equal(chunk.column_count(), 2)
    assert_equal(len(chunk), 1)
    assert_equal(chunk.type(0), DuckDBType.integer)
    assert_equal(chunk.type(1), DuckDBType.varchar)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
