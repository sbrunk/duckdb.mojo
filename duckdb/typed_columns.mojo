from collections import Optional
from utils import Variant
from duckdb.logical_type import LogicalType

trait AnyCol:
    fn type(self) -> LogicalType:
        pass

struct Col[T: CollectionElement, Builder: DuckDBValue](AnyCol):
    """Represents a typed column in a DuckDB result."""

    # using a variant here allows us to create aliases for simple types below
    # as we can't create a LogicalType at compile time due to calling into duckdb
    var _type: Variant[LogicalType, DuckDBType]

    fn __init__(inout self, duckdb_type: DuckDBType):
        self._type = duckdb_type

    fn __init__(inout self, logical_type: LogicalType):
        self._type = logical_type

    fn type(self) -> LogicalType:
        if self._type.isa[LogicalType]():
            return self._type[LogicalType]
        return LogicalType(self._type[DuckDBType])

alias bool_ = Col[Bool, BoolVal](DuckDBType.boolean)
alias int8 = Col[Int8, Int8Val](DuckDBType.tinyint)
alias int16 = Col[Int16, Int16Val](DuckDBType.smallint)
alias int32 = Col[Int32, Int32Val](DuckDBType.integer)
alias int64 = Col[Int64, Int64Val](DuckDBType.bigint)
alias uint8 = Col[UInt8, UInt8Val](DuckDBType.utinyint)
alias uint16 = Col[UInt16, UInt16Val](DuckDBType.usmallint)
alias uint32 = Col[UInt32, UInt32Val](DuckDBType.uinteger)
alias uint64 = Col[UInt64, UInt64Val](DuckDBType.ubigint)
alias float32 = Col[Float32, Float32Val](DuckDBType.float)
alias float64 = Col[Float64, Float64Val](DuckDBType.double)
alias timestamp = Col[Timestamp, DuckDBTimestamp](DuckDBType.timestamp)
alias date = Col[Date, DuckDBDate](DuckDBType.date)
alias time = Col[Time, DuckDBTime](DuckDBType.time)
alias interval = Col[Interval, DuckDBInterval](DuckDBType.interval)
alias string = Col[String, DuckDBString](DuckDBType.varchar)
"""A String column."""

# TODO remaining types

fn list[
    T: CollectionElement
](c: Col[T]) -> Col[List[Optional[T]], DuckDBList[c.Builder]]:
    return Col[List[Optional[T]], DuckDBList[c.Builder]](
        c.type().create_list_type()
    )


# fn map[
#     K: KeyElement, V: CollectionElement
# ](k: Col[K], v: Col[V]) -> Col[
#     Dict[K, Optional[V]], DuckDBMap[k.Builder, v.Builder]
# ]:
#     return Col[Dict[K, Optional[V]]](DBMapType(k.logical_type, v.logical_type))
