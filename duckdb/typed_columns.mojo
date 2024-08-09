from collections import Optional
from utils import Variant
from duckdb.logical_type import LogicalType

trait AnyCol:
    fn type(self) -> LogicalType:
        ...

@value
struct Col[T: CollectionElement, Builder: DuckDBValue](AnyCol):
    """Represents a typed column in a DuckDB result."""

    var logical_type: LogicalType

    fn type(self) -> LogicalType:
        return self.logical_type

var bool_ = Col[Bool, BoolVal](LogicalType(DuckDBType.boolean))
var int8 = Col[Int8, Int8Val](LogicalType(DuckDBType.tinyint))
var int16 = Col[Int16, Int16Val](LogicalType(DuckDBType.smallint))
var int32 = Col[Int32, Int32Val](LogicalType(DuckDBType.integer))
var int64 = Col[Int64, Int64Val](LogicalType(DuckDBType.bigint))
var uint8 = Col[UInt8, UInt8Val](LogicalType(DuckDBType.utinyint))
var uint16 = Col[UInt16, UInt16Val](LogicalType(DuckDBType.usmallint))
var uint32 = Col[UInt32, UInt32Val](LogicalType(DuckDBType.uinteger))
var uint64 = Col[UInt64, UInt64Val](LogicalType(DuckDBType.ubigint))
var float32 = Col[Float32, Float32Val](LogicalType(DuckDBType.float))
var float64 = Col[Float64, Float64Val](LogicalType(DuckDBType.double))
var timestamp = Col[Timestamp, DuckDBTimestamp](LogicalType(DuckDBType.timestamp))
var date = Col[Date, DuckDBDate](LogicalType(DuckDBType.date))
var time = Col[Time, DuckDBTime](LogicalType(DuckDBType.time))
var interval = Col[Interval, DuckDBInterval](LogicalType(DuckDBType.interval))
var string = Col[String, DuckDBString](LogicalType(DuckDBType.varchar))
"""A String column."""

# TODO remaining types

fn list[
    T: CollectionElement
](c: Col[T]) -> Col[List[Optional[T]], DuckDBList[c.Builder]]:
    return Col[List[Optional[T]], DuckDBList[c.Builder]](
        c.logical_type.create_list_type()
    )


# fn map[
#     K: KeyElement, V: CollectionElement
# ](k: Col[K], v: Col[V]) -> Col[
#     Dict[K, Optional[V]], DuckDBMap[k.Builder, v.Builder]
# ]:
#     return Col[Dict[K, Optional[V]]](DBMapType(k.logical_type, v.logical_type))
