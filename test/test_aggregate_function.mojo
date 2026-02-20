from sys.info import size_of

from duckdb import *
from duckdb.aggregate_function import (
    AggregateFunction,
    AggregateFunctionSet,
    AggregateFunctionInfo,
    AggregateState,
    AggregateStateArray,
)
from duckdb._libduckdb import *
from testing import *
from testing.suite import TestSuite


# ===--------------------------------------------------------------------===#
# State struct for SUM aggregate
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct SumState(Copyable, Movable):
    """Accumulator state for a simple SUM."""

    var total: Int64


# ===--------------------------------------------------------------------===#
# SUM aggregate callback implementations
# ===--------------------------------------------------------------------===#


fn sum_state_size(info: AggregateFunctionInfo) -> idx_t:
    """Returns the size of the SUM state."""
    return idx_t(size_of[SumState]())


fn sum_state_init(info: AggregateFunctionInfo, state: AggregateState):
    """Initializes a SUM state to zero."""
    state.get_data().bitcast[SumState]().init_pointee_move(SumState(total=0))


fn sum_update(
    info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray
):
    """Updates SUM states with integer input values."""
    var size = len(input)
    var data = input.get_vector(0).get_data().bitcast[Int32]()
    for i in range(size):
        var s = states.get_state(i).get_data().bitcast[SumState]()
        s[].total += Int64(data[i])


fn sum_combine(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    target: AggregateStateArray,
    count: Int,
):
    """Combines SUM states for parallel aggregation."""
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[SumState]()
        var t = target.get_state(i).get_data().bitcast[SumState]()
        t[].total += s[].total


fn sum_finalize(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    result: Vector,
    count: Int,
    offset: Int,
):
    """Produces the final SUM result."""
    var out = result.get_data().bitcast[Int64]()
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[SumState]()
        out[offset + i] = s[].total


fn sum_destroy(states: AggregateStateArray):
    """Destroys SUM states."""
    for i in range(len(states)):
        states.get_state(i).get_data().bitcast[SumState]().destroy_pointee()


# ===--------------------------------------------------------------------===#
# State struct for COUNT aggregate
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct CountState(Copyable, Movable):
    """Accumulator state for a simple COUNT."""

    var count: Int64


# ===--------------------------------------------------------------------===#
# COUNT aggregate callback implementations
# ===--------------------------------------------------------------------===#


fn count_state_size(info: AggregateFunctionInfo) -> idx_t:
    return idx_t(size_of[CountState]())


fn count_state_init(info: AggregateFunctionInfo, state: AggregateState):
    state.get_data().bitcast[CountState]().init_pointee_move(
        CountState(count=0)
    )


fn count_update(
    info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray
):
    var size = len(input)
    for i in range(size):
        var s = states.get_state(i).get_data().bitcast[CountState]()
        s[].count += 1


fn count_combine(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    target: AggregateStateArray,
    count: Int,
):
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[CountState]()
        var t = target.get_state(i).get_data().bitcast[CountState]()
        t[].count += s[].count


fn count_finalize(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    result: Vector,
    count: Int,
    offset: Int,
):
    var out = result.get_data().bitcast[Int64]()
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[CountState]()
        out[offset + i] = s[].count


# ===--------------------------------------------------------------------===#
# State struct for AVG aggregate (using double sum)
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct AvgState(Copyable, Movable):
    """Accumulator state for AVG using Float64 sum and count."""

    var sum: Float64
    var count: Int64


fn avg_state_size(info: AggregateFunctionInfo) -> idx_t:
    return idx_t(size_of[AvgState]())


fn avg_state_init(info: AggregateFunctionInfo, state: AggregateState):
    state.get_data().bitcast[AvgState]().init_pointee_move(
        AvgState(sum=0.0, count=0)
    )


fn avg_update_double(
    info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray
):
    var size = len(input)
    var data = input.get_vector(0).get_data().bitcast[Float64]()
    for i in range(size):
        var s = states.get_state(i).get_data().bitcast[AvgState]()
        s[].sum += data[i]
        s[].count += 1


fn avg_combine(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    target: AggregateStateArray,
    count: Int,
):
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[AvgState]()
        var t = target.get_state(i).get_data().bitcast[AvgState]()
        t[].sum += s[].sum
        t[].count += s[].count


fn avg_finalize(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    result: Vector,
    count: Int,
    offset: Int,
):
    var out = result.get_data().bitcast[Float64]()
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[AvgState]()
        if s[].count > 0:
            out[offset + i] = s[].sum / Float64(s[].count)
        else:
            out[offset + i] = 0.0


# ===--------------------------------------------------------------------===#
# SUM for Float64 (used in function set tests)
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct SumDoubleState(Copyable, Movable):
    var total: Float64


fn sum_double_state_size(info: AggregateFunctionInfo) -> idx_t:
    return idx_t(size_of[SumDoubleState]())


fn sum_double_state_init(info: AggregateFunctionInfo, state: AggregateState):
    state.get_data().bitcast[SumDoubleState]().init_pointee_move(
        SumDoubleState(total=0.0)
    )


fn sum_double_update(
    info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray
):
    var size = len(input)
    var data = input.get_vector(0).get_data().bitcast[Float64]()
    for i in range(size):
        var s = states.get_state(i).get_data().bitcast[SumDoubleState]()
        s[].total += data[i]


fn sum_double_combine(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    target: AggregateStateArray,
    count: Int,
):
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[SumDoubleState]()
        var t = target.get_state(i).get_data().bitcast[SumDoubleState]()
        t[].total += s[].total


fn sum_double_finalize(
    info: AggregateFunctionInfo,
    source: AggregateStateArray,
    result: Vector,
    count: Int,
    offset: Int,
):
    var out = result.get_data().bitcast[Float64]()
    for i in range(count):
        var s = source.get_state(i).get_data().bitcast[SumDoubleState]()
        out[offset + i] = s[].total


# ===--------------------------------------------------------------------===#
# AggregateFunction Tests
# ===--------------------------------------------------------------------===#


def test_aggregate_function_create():
    """Test creating an aggregate function."""
    var func = AggregateFunction()
    _ = func^


def test_aggregate_function_set_name():
    """Test setting the name of an aggregate function."""
    var func = AggregateFunction()
    func.set_name("test_agg")
    _ = func^


def test_aggregate_function_add_parameter():
    """Test adding parameters to an aggregate function."""
    var func = AggregateFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    _ = func^


def test_aggregate_function_set_return_type():
    """Test setting the return type of an aggregate function."""
    var func = AggregateFunction()
    var bigint_type = LogicalType(DuckDBType.bigint)
    func.set_return_type(bigint_type)
    _ = func^


def test_aggregate_function_set_special_handling():
    """Test setting special handling for an aggregate function."""
    var func = AggregateFunction()
    func.set_special_handling()
    _ = func^


def test_aggregate_function_register_simple():
    """Test registering a simple aggregate function."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)


def test_aggregate_function_sum_single_value():
    """Test SUM aggregate with a single value."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    var result = conn.execute("SELECT my_sum(42) as answer")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 42)


def test_aggregate_function_sum_table():
    """Test SUM aggregate over a table of values."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE nums (x INTEGER)")
    _ = conn.execute("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")

    var result = conn.execute("SELECT my_sum(x) as total FROM nums")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 15)


def test_aggregate_function_sum_empty_table():
    """Test SUM aggregate on an empty table returns initialized state (0)."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE empty_nums (x INTEGER)")

    var result = conn.execute("SELECT my_sum(x) as total FROM empty_nums")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 0)


def test_aggregate_function_sum_with_groups():
    """Test SUM aggregate with GROUP BY."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE grouped (grp VARCHAR, val INTEGER)")
    _ = conn.execute(
        "INSERT INTO grouped VALUES ('a', 1), ('a', 2), ('b', 10), ('b', 20),"
        " ('b', 30)"
    )

    var result = conn.execute(
        "SELECT grp, my_sum(val) as total FROM grouped GROUP BY grp ORDER BY"
        " grp"
    )
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)
    # Group 'a': 1+2=3
    assert_equal(chunk.get(bigint, col=1, row=0).value(), 3)
    # Group 'b': 10+20+30=60
    assert_equal(chunk.get(bigint, col=1, row=1).value(), 60)


def test_aggregate_function_sum_large_input():
    """Test SUM aggregate with a larger dataset that spans multiple chunks."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    # SUM(1..5000) = 5000*5001/2 = 12502500
    var result = conn.execute(
        "SELECT my_sum(i::INTEGER) as total FROM range(1, 5001) t(i)"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 12502500)


def test_aggregate_function_count():
    """Test a custom COUNT aggregate."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_count")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        count_state_size,
        count_state_init,
        count_update,
        count_combine,
        count_finalize,
    ]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE count_data (x INTEGER)")
    _ = conn.execute("INSERT INTO count_data VALUES (10), (20), (30), (40)")

    var result = conn.execute("SELECT my_count(x) as cnt FROM count_data")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 4)


def test_aggregate_function_avg():
    """Test a custom AVG aggregate (DOUBLE input)."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_avg")
    func.add_parameter(LogicalType(DuckDBType.double))
    func.set_return_type(LogicalType(DuckDBType.double))
    func.set_functions[
        avg_state_size,
        avg_state_init,
        avg_update_double,
        avg_combine,
        avg_finalize,
    ]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE avg_data (x DOUBLE)")
    _ = conn.execute(
        "INSERT INTO avg_data VALUES (10.0), (20.0), (30.0), (40.0)"
    )

    var result = conn.execute("SELECT my_avg(x) as average FROM avg_data")
    var chunk = result.fetch_chunk()
    assert_almost_equal(chunk.get(double, col=0, row=0).value(), 25.0)


def test_aggregate_function_avg_with_groups():
    """Test custom AVG aggregate with GROUP BY."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_avg")
    func.add_parameter(LogicalType(DuckDBType.double))
    func.set_return_type(LogicalType(DuckDBType.double))
    func.set_functions[
        avg_state_size,
        avg_state_init,
        avg_update_double,
        avg_combine,
        avg_finalize,
    ]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE avg_grp (grp VARCHAR, val DOUBLE)")
    _ = conn.execute(
        "INSERT INTO avg_grp VALUES ('a', 2.0), ('a', 4.0), ('b', 100.0)"
    )

    var result = conn.execute(
        "SELECT grp, my_avg(val) as average FROM avg_grp GROUP BY grp ORDER BY"
        " grp"
    )
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 2)
    # Group 'a': (2+4)/2 = 3.0
    assert_almost_equal(chunk.get(double, col=1, row=0).value(), 3.0)
    # Group 'b': 100/1 = 100.0
    assert_almost_equal(chunk.get(double, col=1, row=1).value(), 100.0)


def test_aggregate_function_in_subquery():
    """Test using aggregate function in a subquery."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE sub_data (x INTEGER)")
    _ = conn.execute("INSERT INTO sub_data VALUES (1), (2), (3)")

    var result = conn.execute(
        "SELECT * FROM (SELECT my_sum(x) as total FROM sub_data) WHERE total"
        " > 5"
    )
    var chunk = result.fetch_chunk()
    assert_equal(len(chunk), 1)
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 6)


def test_aggregate_function_set_create():
    """Test creating an aggregate function set."""
    var func_set = AggregateFunctionSet("test_agg_set")
    _ = func_set^


def test_aggregate_function_set_register():
    """Test registering an aggregate function set with multiple overloads."""
    var conn = DuckDB.connect(":memory:")

    var func_set = AggregateFunctionSet("my_sum_set")

    # Integer overload
    var int_func = AggregateFunction()
    int_func.set_name("my_sum_set")
    int_func.add_parameter(LogicalType(DuckDBType.integer))
    int_func.set_return_type(LogicalType(DuckDBType.bigint))
    int_func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    int_func.set_destructor[sum_destroy]()
    func_set.add_function(int_func)

    # Double overload
    var dbl_func = AggregateFunction()
    dbl_func.set_name("my_sum_set")
    dbl_func.add_parameter(LogicalType(DuckDBType.double))
    dbl_func.set_return_type(LogicalType(DuckDBType.double))
    dbl_func.set_functions[
        sum_double_state_size,
        sum_double_state_init,
        sum_double_update,
        sum_double_combine,
        sum_double_finalize,
    ]()
    func_set.add_function(dbl_func)

    func_set.register(conn)

    # Test integer overload
    _ = conn.execute("CREATE TABLE int_vals (x INTEGER)")
    _ = conn.execute("INSERT INTO int_vals VALUES (1), (2), (3)")
    var result1 = conn.execute(
        "SELECT my_sum_set(x)::BIGINT as total FROM int_vals"
    )
    var chunk1 = result1.fetch_chunk()
    assert_equal(chunk1.get(bigint, col=0, row=0).value(), 6)

    # Test double overload
    _ = conn.execute("CREATE TABLE dbl_vals (x DOUBLE)")
    _ = conn.execute("INSERT INTO dbl_vals VALUES (1.5), (2.5), (3.0)")
    var result2 = conn.execute("SELECT my_sum_set(x) as total FROM dbl_vals")
    var chunk2 = result2.fetch_chunk()
    assert_almost_equal(chunk2.get(double, col=0, row=0).value(), 7.0)


def test_aggregate_function_sum_negative_values():
    """Test SUM aggregate with negative values."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute("CREATE TABLE neg_data (x INTEGER)")
    _ = conn.execute(
        "INSERT INTO neg_data VALUES (-5), (3), (-2), (10), (-6)"
    )

    var result = conn.execute("SELECT my_sum(x) as total FROM neg_data")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get(bigint, col=0, row=0).value(), 0)


def test_aggregate_function_sum_matches_builtin():
    """Test that our SUM aggregate matches DuckDB's built-in SUM."""
    var conn = DuckDB.connect(":memory:")

    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[
        sum_state_size,
        sum_state_init,
        sum_update,
        sum_combine,
        sum_finalize,
    ]()
    func.set_destructor[sum_destroy]()
    func.register(conn)

    _ = conn.execute(
        "CREATE TABLE cmp_data AS SELECT (i % 7)::INTEGER as x FROM range(100)"
        " t(i)"
    )

    var result = conn.execute(
        "SELECT my_sum(x) as my_total, SUM(x)::BIGINT as builtin_total FROM"
        " cmp_data"
    )
    var chunk = result.fetch_chunk()
    assert_equal(
        chunk.get(bigint, col=0, row=0).value(),
        chunk.get(bigint, col=1, row=0).value(),
    )


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
