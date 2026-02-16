from collections import Optional
from utils import Variant
from duckdb.logical_type import LogicalType
from duckdb.duckdb_wrapper import *


struct Col[T: Copyable & Movable, Builder: DuckDBWrapper](ImplicitlyCopyable & Movable):
    """Represents a typed column in a DuckDB result."""

    # using a variant here allows us to create aliases for simple types below
    # as we can't create a LogicalType at compile time due to calling into duckdb
    var _type: Variant[LogicalType[is_owned=True, origin=MutExternalOrigin], DuckDBType]

    fn __init__(out self, duckdb_type: DuckDBType):
        self._type = duckdb_type

    fn __init__(out self, logical_type: LogicalType[is_owned=True, origin=MutExternalOrigin]):
        self._type = logical_type

    fn type(self) -> LogicalType[is_owned=True, origin=MutExternalOrigin]:
        if self._type.isa[LogicalType[is_owned=True, origin=MutExternalOrigin]]():
            return self._type[LogicalType[is_owned=True, origin=MutExternalOrigin]]
        return LogicalType[is_owned=True, origin=MutExternalOrigin](self._type[DuckDBType])


comptime boolean = Col[Bool, BoolVal](DuckDBType.boolean)
comptime tinyint = Col[Int8, Int8Val](DuckDBType.tinyint)
comptime smallint = Col[Int16, Int16Val](DuckDBType.smallint)
comptime integer = Col[Int32, Int32Val](DuckDBType.integer)
comptime bigint = Col[Int64, Int64Val](DuckDBType.bigint)
comptime utinyint = Col[UInt8, UInt8Val](DuckDBType.utinyint)
comptime usmallint = Col[UInt16, UInt16Val](DuckDBType.usmallint)
comptime uinteger = Col[UInt32, UInt32Val](DuckDBType.uinteger)
comptime ubigint = Col[UInt64, UInt64Val](DuckDBType.ubigint)
comptime float_ = Col[Float32, Float32Val](DuckDBType.float)
"""A float32 column."""
comptime double = Col[Float64, Float64Val](DuckDBType.double)
"""A float64 column."""
comptime timestamp = Col[Timestamp, DuckDBTimestamp](DuckDBType.timestamp)
comptime date = Col[Date, DuckDBDate](DuckDBType.date)
comptime time = Col[Time, DuckDBTime](DuckDBType.time)
comptime interval = Col[Interval, DuckDBInterval](DuckDBType.interval)
comptime varchar = Col[String, DuckDBString](DuckDBType.varchar)
"""A String column."""


fn list[
    T: Copyable & Movable
](c: Col[T]) -> Col[List[Optional[T]], DuckDBList[c.Builder]]:
    return Col[List[Optional[T]], DuckDBList[c.Builder]](
        c.type().create_list_type()
    )


# TODO remaining types

# fn map[
#     K: KeyElement, V: Copyable & Movable
# ](k: Col[K], v: Col[V]) -> Col[
#     Dict[K, Optional[V]], DuckDBMap[k.Builder, v.Builder]
# ]:
#     return Col[Dict[K, Optional[V]]](DBMapType(k.logical_type, v.logical_type))
