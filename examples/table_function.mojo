"""A table function that generates the integers 0..n-1.

    pixi run mojo run examples/table_function.mojo
"""

from duckdb import *
from duckdb.table_function import (
    TableFunction,
    TableFunctionInfo,
    TableBindInfo,
    TableInitInfo,
)
from std.memory.alloc import unsafe_alloc


@fieldwise_init
struct CounterBindData(Copyable, Movable):
    var limit: Int
    var current_row: Int


def destroy_bind_data(data: Pointer[NoneType, MutAnyOrigin]) abi("C"):
    data.unsafe_bitcast[CounterBindData]().unsafe_deinit_pointee()
    data.unsafe_bitcast[CounterBindData]().unsafe_free()


def counter_bind(info: TableBindInfo):
    """Declare the output column and store the parameter."""
    info.add_result_column("i", LogicalType(DuckDBType.integer))
    var limit = Int(info.get_parameter(0).as_int32())
    var bind_data = unsafe_alloc[CounterBindData](1)
    bind_data.unsafe_write(CounterBindData(limit=limit, current_row=0))
    info.set_bind_data(bind_data.unsafe_bitcast[NoneType](), destroy_bind_data)


def counter_init(info: TableInitInfo):
    pass


def counter_function(info: TableFunctionInfo, mut output: Chunk):
    """Fill one output chunk. A size of 0 signals the end."""
    var bind_data = info.get_bind_data().unsafe_bitcast[CounterBindData]()
    var current = bind_data[].current_row
    var batch = min(bind_data[].limit - current, 2048)
    if batch <= 0:
        output.set_size(0)
        return
    var vec = output.get_vector(0)
    var out = vec.get_data().unsafe_bitcast[Int32]()
    for i in range(batch):
        out[unsafe_offset=i] = Int32(current + i)
    bind_data[].current_row = current + batch
    output.set_size(batch)


def main() raises:
    var conn = DuckDB.connect(":memory:")
    var tf = TableFunction()
    tf.set_name("generate_ints")
    tf.add_parameter(LogicalType(DuckDBType.integer))
    tf.set_bind[counter_bind]()
    tf.set_init[counter_init]()
    tf.set_function[counter_function]()
    tf.register(conn)

    conn.execute("SELECT sum(i) AS total FROM generate_ints(100)").show()
