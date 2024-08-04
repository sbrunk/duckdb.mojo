# we need a way to capture the expected type while building the Col[T] type.

alias DBType = Variant[DBListType, DBPrimitiveType, DBMapType]


fn get_duckdb_type(db_type: DBType) -> DuckDBType:
    if db_type.isa[DBPrimitiveType]():
        return db_type[DBPrimitiveType].duckdb_type
    elif db_type.isa[DBListType]():
        return DuckDBType.list
    elif db_type.isa[DBMapType]():
        return DuckDBType.map
    return DuckDBType.invalid


@value
struct DBPrimitiveType(Copyable):
    var duckdb_type: DuckDBType


# we can't just use DBType itself because the compiler complains about recursive types
alias ChildType = Variant[DBListType, DBPrimitiveType, DBMapType]


@value
struct DBListType(Copyable):
    alias duckdb_type = DuckDBType.list
    var _child: UnsafePointer[ChildType]

    fn __init__(inout self, child: ChildType):
        self._child = UnsafePointer[ChildType].alloc(1)
        self._child.init_pointee_copy(child)

    fn __copyinit__(inout self, other: Self):
        self._child = UnsafePointer[ChildType].alloc(1)
        self._child.init_pointee_copy(other.child())

    fn __moveinit__(inout self, owned other: Self):
        self._child = other._child

    fn child(self) -> ChildType:
        return self._child[]

    fn __del__(owned self):
        self._child.destroy_pointee()
        self._child.free()

alias KeyType = Variant[DBListType, DBPrimitiveType, DBMapType]
alias ValueType = KeyType


@value
struct DBMapType(Copyable):
    alias duckdb_type = DuckDBType.map
    var _key_type: UnsafePointer[KeyType]
    var _value_type: UnsafePointer[KeyType]

    fn __init__(inout self, key_type: KeyType, value_type: KeyType):
        self._key_type = UnsafePointer[DBType].alloc(1)
        self._key_type.init_pointee_copy(key_type)
        self._value_type = UnsafePointer[DBType].alloc(1)
        self._value_type.init_pointee_copy(value_type)

    fn __copyinit__(inout self, other: Self):
        self._key_type = UnsafePointer[DBType].alloc(1)
        self._key_type.init_pointee_copy(other.key_type())
        self._value_type = UnsafePointer[DBType].alloc(1)
        self._value_type.init_pointee_copy(other.value_type())

    fn __moveinit__(inout self, owned other: Self):
        self._key_type = other._key_type
        self._value_type = other._value_type

    fn key_type(self) -> DBType:
        return self._key_type[]

    fn value_type(self) -> DBType:
        return self._value_type[]

    fn __del__(owned self):
        self._key_type.destroy_pointee()
        self._key_type.free()
        self._value_type.destroy_pointee()
        self._value_type.free()

trait AnyCol(CollectionElement):
    fn type(self) -> DBType:
        ...


@value
struct Col[T: CollectionElement, Builder: DuckDBValue](AnyCol):
    """Represents a typed column in a DuckDB result."""

    var logical_type: DBType

    fn type(self) -> DBType:
        return self.logical_type

alias bool_ = Col[Bool, BoolVal](DBPrimitiveType(DuckDBType.boolean))
alias int8 = Col[Int8, Int8Val](DBPrimitiveType(DuckDBType.tinyint))
alias int16 = Col[Int16, Int16Val](DBPrimitiveType(DuckDBType.smallint))
alias int32 = Col[Int32, Int32Val](DBPrimitiveType(DuckDBType.integer))
alias int64 = Col[Int64, Int64Val](DBPrimitiveType(DuckDBType.bigint))
alias uint8 = Col[UInt8, UInt8Val](DBPrimitiveType(DuckDBType.utinyint))
alias uint16 = Col[UInt16, UInt16Val](DBPrimitiveType(DuckDBType.usmallint))
alias uint32 = Col[UInt32, UInt32Val](DBPrimitiveType(DuckDBType.uinteger))
alias uint64 = Col[UInt64, UInt64Val](DBPrimitiveType(DuckDBType.ubigint))
alias float32 = Col[Float32, Float32Val](DBPrimitiveType(DuckDBType.float))
alias float64 = Col[Float64, Float64Val](DBPrimitiveType(DuckDBType.double))
alias timestamp = Col[Timestamp, DuckDBTimestamp](DBPrimitiveType(DuckDBType.timestamp))
alias date = Col[Date, DuckDBDate](DBPrimitiveType(DuckDBType.date))
alias time = Col[Time, DuckDBTime](DBPrimitiveType(DuckDBType.time))
alias interval = Col[Interval, DuckDBInterval](DBPrimitiveType(DuckDBType.interval))
alias string = Col[String, DuckDBString](DBPrimitiveType(DuckDBType.varchar))
"""A String column."""

# TODO remaining types

fn list[
    T: CollectionElement
](c: Col[T]) -> Col[List[Optional[T]], DuckDBList[c.Builder]]:
    return Col[List[Optional[T]], DuckDBList[c.Builder]](
        DBListType(c.logical_type)
    )


# fn map[
#     K: KeyElement, V: CollectionElement
# ](k: Col[K], v: Col[V]) -> Col[
#     Dict[K, Optional[V]], DuckDBMap[k.Builder, v.Builder]
# ]:
#     return Col[Dict[K, Optional[V]]](DBMapType(k.logical_type, v.logical_type))
