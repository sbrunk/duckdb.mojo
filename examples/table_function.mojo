"""A table function that generates the integers 0..n-1 and their squares.

    pixi run mojo run examples/table_function.mojo
"""

import duckdb
from duckdb import BindArgs, ScanShared, TableColumns, TableFunction, TableFunctionImpl, TableOutput, param_types


@fieldwise_init
struct Position(Movable):
    var next: Int64


@fieldwise_init
struct Squares(TableFunctionImpl):
    # Per-scan state, created by `init`.
    comptime State = Position
    var n: Int64

    # Runs once per query: reads the arguments and declares the columns.
    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        var n = args.constant[Int64](0)
        if not n or n.value() < 0:
            raise Error("squares needs a non-negative count")
        columns.add[Int64]("i")
        columns.add[Int64]("square")
        columns.set_cardinality(Int(n.value()), exact=True)
        return Self(n.value())

    def init(self, shared: ScanShared) raises -> Position:
        return Position(0)

    # Fills one output chunk and returns its row count. 0 ends the scan.
    def scan(self, mut state: Position, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < self.n:
            out.write(rows, 0, state.next)
            out.write(rows, 1, state.next * state.next)
            state.next += 1
            rows += 1
        return rows


def main() raises:
    var con = duckdb.connect()
    con.register(TableFunction.from_impl[Squares]("squares", param_types[Int64]()))
    con.execute("SELECT count(*), sum(i), sum(square) FROM squares(100)").show()
