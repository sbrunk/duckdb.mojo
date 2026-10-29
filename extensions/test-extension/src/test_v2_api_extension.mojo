"""Test extension for the V2 high-level API (`duckdb.v2.Extension`).

Registers functions of every kind the API offers, so the extension tests can
check them in a DuckDB process that only loads this library.
"""

from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb.v2 import BindArgs, BindData, Extension, ScalarArgs, ScalarFunction, VectorWriter, register_simd_math
from duckdb.v2 import LogicalType, TypeId, bind_callback
from duckdb.duckdb_type import Date


def add(a: Int64, b: Int64) -> Int64:
    return a + b


def greet(name: String) -> String:
    return String("hello ", name, ", from a V2 extension")


def total(values: List[Int64]) -> Int64:
    var t: Int64 = 0
    for v in values:
        t += v
    return t


def next_day(d: Date) -> Date:
    return Date(d.days + 1)


def year_of(d: Date) -> Int64:
    return Int64(d.year())


@fieldwise_init
struct Offset(BindData, Copyable):
    var amount: Int64

    @staticmethod
    def bind(args: BindArgs) raises -> Self:
        var a = args.constant[Int64](1)
        return Self(a.value() if a else 0)


def offset(o: Offset, x: Int64, amount: Int64) -> Int64:
    return x + o.amount


def negate(mut args: ScalarArgs, mut out: VectorWriter) raises:
    var x = args.column[Int64](0)
    for row in range(args.size()):
        var v = x.get(row)
        if v:
            out.write(row, -v.value())
        else:
            out.write_null(row)


def init(ext: Extension) raises:
    ext.register(ScalarFunction.from_function["v2api_add", add]())
    ext.register(ScalarFunction.from_function["v2api_greet", greet]())
    ext.register(ScalarFunction.from_function["v2api_total", total]())
    ext.register(ScalarFunction.from_function["v2api_next_day", next_day]())
    ext.register(ScalarFunction.from_function["v2api_year", year_of]())
    ext.register(ScalarFunction.from_function["v2api_offset", offset]())
    ext.register(
        ScalarFunction.from_chunk_function[negate]("v2api_negate", [LogicalType(TypeId.BIGINT)], LogicalType(TypeId.BIGINT))
    )
    register_simd_math(ext)


@export("mojo_v2_api_init_c_api_v2")
def mojo_v2_api_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    Extension.run[init](input)
