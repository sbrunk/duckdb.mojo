# we need a way to capture the expected type while building the Col[T] type.

alias DBType = Variant[DBListType, DBPrimitiveType, DBMapType]

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
        self._value_type.destroy_pointee()

trait AnyCol(CollectionElement):
    fn type(self) -> DBType: ...

@value
struct Col[T: CollectionElement](AnyCol):
    """Represents a typed column in a DuckDB result."""
    var logical_type: DBType
    
    fn type(self) -> DBType:
        return self.logical_type

    fn __call__(self) raises -> List[Optional[T]]:
        # TODO check runtime type
        raise "Not implemented"

alias string = Col[String](DBPrimitiveType(DuckDBType.varchar))
"""A String column."""
alias bool = Col[Bool](DBPrimitiveType(DuckDBType.boolean))
alias int32 = Col[Int32](DBPrimitiveType(DuckDBType.integer))
alias int64 = Col[Int64](DBPrimitiveType(DuckDBType.bigint))

fn list[T: CollectionElement](c: Col[T]) -> Col[List[Optional[T]]]:
    return Col[List[Optional[T]]](DBListType(c.logical_type))

fn map[K: KeyElement, V: CollectionElement](k: Col[K], v: Col[V]) -> Col[Dict[K, Optional[V]]]:
    return Col[Dict[K, Optional[V]]](DBMapType(k.logical_type, v.logical_type))

# fn db_struct[name: String, T: CollectionElement]() -> Col[]
