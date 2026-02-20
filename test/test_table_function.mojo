from duckdb import *
from duckdb.table_function import (
    TableFunction,
    TableFunctionInfo,
    TableBindInfo,
    TableInitInfo,
)
from duckdb._libduckdb import *
from testing import *
from testing.suite import TestSuite
from memory.unsafe_pointer import alloc


# ===--------------------------------------------------------------------===#
# Bind data structs used by table functions
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct CounterBindData(Copyable, Movable):
    """Bind data for a simple counting table function.
    Stores how many rows to produce (limit) and tracks the current row offset.
    """

    var limit: Int
    var current_row: Int


@fieldwise_init
struct MultiColBindData(Copyable, Movable):
    """Bind data for a multi-column table function."""

    var num_rows: Int
    var current_row: Int


# ===--------------------------------------------------------------------===#
# Destroy callbacks
# ===--------------------------------------------------------------------===#


fn destroy_counter_bind_data(data: UnsafePointer[NoneType, MutAnyOrigin]):
    """Destroy callback for CounterBindData."""
    data.bitcast[CounterBindData]().destroy_pointee()
    data.bitcast[CounterBindData]().free()


fn destroy_multi_col_bind_data(data: UnsafePointer[NoneType, MutAnyOrigin]):
    """Destroy callback for MultiColBindData."""
    data.bitcast[MultiColBindData]().destroy_pointee()
    data.bitcast[MultiColBindData]().free()


# ===--------------------------------------------------------------------===#
# Simple table function: generate_series(n) -> produces integers 0..n-1
# ===--------------------------------------------------------------------===#


fn counter_bind(info: TableBindInfo):
    """Bind function: defines one INTEGER output column and stores the limit from the parameter.
    """
    info.add_result_column("i", LogicalType(DuckDBType.integer))
    var limit_val = info.get_parameter(0)
    var limit = Int(limit_val.as_int32())
    var bind_data = alloc[CounterBindData](1)
    bind_data.init_pointee_move(CounterBindData(limit=limit, current_row=0))
    info.set_bind_data(
        bind_data.bitcast[NoneType](),
        destroy_counter_bind_data,
    )


fn counter_init(info: TableInitInfo):
    """Init function: nothing to do, state lives in bind data."""
    pass


fn counter_function(info: TableFunctionInfo, mut output: Chunk):
    """Main function: produces rows in batches. Sets output size to 0 when done.
    """
    var bind_data = info.get_bind_data().bitcast[CounterBindData]()
    var current = bind_data[].current_row
    var limit = bind_data[].limit

    if current >= limit:
        output.set_size(0)
        return

    var remaining = limit - current
    var batch_size = min(remaining, 2048)

    var out_data = output.get_vector(0).get_data().bitcast[Int32]()
    for i in range(batch_size):
        out_data[i] = Int32(current + i)

    bind_data[].current_row = current + batch_size
    output.set_size(batch_size)


# ===--------------------------------------------------------------------===#
# Multi-column table function: produces (id INTEGER, name VARCHAR, score DOUBLE)
# ===--------------------------------------------------------------------===#


fn multi_col_bind(info: TableBindInfo):
    """Bind function with multiple output columns."""
    info.add_result_column("id", LogicalType(DuckDBType.integer))
    info.add_result_column("name", LogicalType(DuckDBType.varchar))
    info.add_result_column("score", LogicalType(DuckDBType.double))
    var bind_data = alloc[MultiColBindData](1)
    bind_data.init_pointee_move(MultiColBindData(num_rows=3, current_row=0))
    info.set_bind_data(
        bind_data.bitcast[NoneType](),
        destroy_multi_col_bind_data,
    )


fn multi_col_init(info: TableInitInfo):
    """Init function: nothing to do."""
    pass


fn multi_col_function(info: TableFunctionInfo, mut output: Chunk):
    """Produces 3 rows with (id, name, score) columns."""
    var bind_data = info.get_bind_data().bitcast[MultiColBindData]()
    var current = bind_data[].current_row
    var num_rows = bind_data[].num_rows

    if current >= num_rows:
        output.set_size(0)
        return

    var remaining = num_rows - current
    var batch_size = min(remaining, 2048)

    var id_data = output.get_vector(0).get_data().bitcast[Int32]()
    var name_vec = output.get_vector(1)
    var score_data = output.get_vector(2).get_data().bitcast[Float64]()

    # Names for our test data
    var names: List[String] = ["alice", "bob", "carol"]

    for i in range(batch_size):
        var row = current + i
        id_data[i] = Int32(row + 1)
        name_vec.assign_string_element(UInt64(i), names[row])
        score_data[i] = Float64(row) * 10.5

    bind_data[].current_row = current + batch_size
    output.set_size(batch_size)


# ===--------------------------------------------------------------------===#
# No-parameter table function: static_table() -> produces fixed 2 rows
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct StaticBindData(Copyable, Movable):
    var done: Bool


fn destroy_static_bind_data(data: UnsafePointer[NoneType, MutAnyOrigin]):
    data.bitcast[StaticBindData]().destroy_pointee()
    data.bitcast[StaticBindData]().free()


fn static_bind(info: TableBindInfo):
    """Bind function for a parameterless table function."""
    info.add_result_column("value", LogicalType(DuckDBType.integer))
    var bind_data = alloc[StaticBindData](1)
    bind_data.init_pointee_move(StaticBindData(done=False))
    info.set_bind_data(
        bind_data.bitcast[NoneType](),
        destroy_static_bind_data,
    )


fn static_init(info: TableInitInfo):
    pass


fn static_function(info: TableFunctionInfo, mut output: Chunk):
    """Produces exactly 2 rows in one call, then signals done."""
    var bind_data = info.get_bind_data().bitcast[StaticBindData]()
    if bind_data[].done:
        output.set_size(0)
        return

    var out_data = output.get_vector(0).get_data().bitcast[Int32]()
    out_data[0] = Int32(100)
    out_data[1] = Int32(200)

    bind_data[].done = True
    output.set_size(2)


# ===--------------------------------------------------------------------===#
# TableFunction Tests
# ===--------------------------------------------------------------------===#


def test_table_function_create():
    """Test creating a table function."""
    var func = TableFunction()
    _ = func^


def test_table_function_set_name():
    """Test setting the name of a table function."""
    var func = TableFunction()
    func.set_name("test_func")
    _ = func^


def test_table_function_add_parameter():
    """Test adding parameters to a table function."""
    var func = TableFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    _ = func^


def test_table_function_add_named_parameter():
    """Test adding a named parameter to a table function."""
    var func = TableFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.add_named_parameter("limit", int_type)
    _ = func^


def test_table_function_register_simple():
    """Test registering a simple table function."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()

    func.register(conn)


def test_table_function_execute_counter():
    """Test executing a counting table function that produces 5 rows."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM counter(5)")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 5)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 0)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 1)
    assert_equal(chunk.get(integer, col=0, row=2).value(), 2)
    assert_equal(chunk.get(integer, col=0, row=3).value(), 3)
    assert_equal(chunk.get(integer, col=0, row=4).value(), 4)


def test_table_function_execute_counter_zero():
    """Test counting table function with limit 0 produces no rows."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM counter(0)")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 0)


def test_table_function_execute_counter_single():
    """Test counting table function with limit 1."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM counter(1)")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 1)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 0)


def test_table_function_with_where_clause():
    """Test that table functions work with WHERE clauses."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT i FROM counter(10) WHERE i >= 7")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 7)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 8)
    assert_equal(chunk.get(integer, col=0, row=2).value(), 9)


def test_table_function_with_order_by():
    """Test that table functions work with ORDER BY."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT i FROM counter(5) ORDER BY i DESC")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 5)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 4)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 3)
    assert_equal(chunk.get(integer, col=0, row=2).value(), 2)
    assert_equal(chunk.get(integer, col=0, row=3).value(), 1)
    assert_equal(chunk.get(integer, col=0, row=4).value(), 0)


def test_table_function_with_aggregation():
    """Test that table function output can be aggregated."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    var result = conn.execute("SELECT SUM(i)::BIGINT as total FROM counter(5)")
    var chunk = result.fetch_chunk()
    # Sum of 0+1+2+3+4 = 10
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 10)


def test_table_function_no_params():
    """Test a table function with no parameters."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("static_table")
    func.set_bind[static_bind]()
    func.set_init[static_init]()
    func.set_function[static_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM static_table()")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 100)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 200)


def test_table_function_multi_column():
    """Test a table function that produces multiple columns."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("multi_col")
    func.set_bind[multi_col_bind]()
    func.set_init[multi_col_init]()
    func.set_function[multi_col_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM multi_col()")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)

    # Check id column
    assert_equal(chunk.get(integer, col=0, row=0).value(), 1)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 2)
    assert_equal(chunk.get(integer, col=0, row=2).value(), 3)

    # Check name column
    assert_equal(chunk.get(varchar, col=1, row=0).value(), "alice")
    assert_equal(chunk.get(varchar, col=1, row=1).value(), "bob")
    assert_equal(chunk.get(varchar, col=1, row=2).value(), "carol")

    # Check score column (DOUBLE = Float64)
    assert_equal(chunk.get(double, col=2, row=0).value(), 0.0)
    assert_equal(chunk.get(double, col=2, row=1).value(), 10.5)
    assert_equal(chunk.get(double, col=2, row=2).value(), 21.0)


def test_table_function_multi_column_select_specific():
    """Test selecting specific columns from a multi-column table function."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("multi_col")
    func.set_bind[multi_col_bind]()
    func.set_init[multi_col_init]()
    func.set_function[multi_col_function]()
    func.register(conn)

    var result = conn.execute(
        "SELECT name, score FROM multi_col() WHERE id > 1"
    )
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)
    assert_equal(chunk.get(varchar, col=0, row=0).value(), "bob")
    assert_equal(chunk.get(varchar, col=0, row=1).value(), "carol")


def test_table_function_in_join():
    """Test using a table function in a JOIN."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    # Create a regular table to join with
    _ = conn.execute("CREATE TABLE labels (id INTEGER, label VARCHAR)")
    _ = conn.execute(
        "INSERT INTO labels VALUES (0, 'zero'), (1, 'one'), (2, 'two')"
    )

    var result = conn.execute(
        "SELECT c.i, l.label FROM counter(3) c JOIN labels l ON c.i = l.id"
        " ORDER BY c.i"
    )
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 0)
    assert_equal(chunk.get(varchar, col=1, row=0).value(), "zero")
    assert_equal(chunk.get(integer, col=0, row=1).value(), 1)
    assert_equal(chunk.get(varchar, col=1, row=1).value(), "one")
    assert_equal(chunk.get(integer, col=0, row=2).value(), 2)
    assert_equal(chunk.get(varchar, col=1, row=2).value(), "two")


def test_table_function_supports_projection_pushdown():
    """Test setting projection pushdown on a table function."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter_pp")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.supports_projection_pushdown(True)
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    # Should still work correctly with projection pushdown enabled
    var result = conn.execute("SELECT i FROM counter_pp(3)")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 3)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 0)


def test_table_function_set_cardinality():
    """Test setting cardinality hint in bind phase."""
    var conn = DuckDB.connect(":memory:")

    fn cardinality_bind(info: TableBindInfo):
        info.add_result_column("value", LogicalType(DuckDBType.integer))
        info.set_cardinality(2, True)
        var bind_data = alloc[StaticBindData](1)
        bind_data.init_pointee_move(StaticBindData(done=False))
        info.set_bind_data(
            bind_data.bitcast[NoneType](),
            destroy_static_bind_data,
        )

    var func = TableFunction()
    func.set_name("cardinality_test")
    func.set_bind[cardinality_bind]()
    func.set_init[static_init]()
    func.set_function[static_function]()
    func.register(conn)

    var result = conn.execute("SELECT * FROM cardinality_test()")
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)
    assert_equal(chunk.get(integer, col=0, row=0).value(), 100)
    assert_equal(chunk.get(integer, col=0, row=1).value(), 200)


def test_table_function_large_result():
    """Test a table function producing more rows than a single vector (>2048),
    requiring multiple chunks to be produced."""
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    # 5000 rows should span multiple chunks (default vector size is 2048)
    var result = conn.execute("SELECT * FROM counter(5000)")
    var total_rows = 0
    var iter = result.chunk_iterator()
    while iter.__has_next__():
        var chunk = iter.__next__()
        total_rows += len(chunk)
    assert_true(
        total_rows == 5000, "expected 5000 total rows across all chunks"
    )


def test_table_function_large_result_correctness():
    """Test that values are correct across multiple chunks for a large table function.
    """
    var conn = DuckDB.connect(":memory:")

    var func = TableFunction()
    func.set_name("counter")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_bind[counter_bind]()
    func.set_init[counter_init]()
    func.set_function[counter_function]()
    func.register(conn)

    # Verify correctness via aggregation: SUM(0..4999) = 4999*5000/2 = 12497500
    var result = conn.execute(
        "SELECT COUNT(i)::BIGINT as cnt, SUM(i)::BIGINT as total,"
        " MIN(i)::BIGINT as mn, MAX(i)::BIGINT as mx FROM counter(5000)"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 5000)
    assert_equal(chunk.get(bigint, col=1, row=0).value(), 12497500)
    assert_equal(chunk.get(bigint, col=2, row=0).value(), 0)
    assert_equal(chunk.get(bigint, col=3, row=0).value(), 4999)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
