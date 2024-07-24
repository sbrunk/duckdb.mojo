from duckdb._c_api.libduckdb import _impl
from duckdb.chunk import Chunk, _ChunkIter

@value
struct Column:
    var index: Int
    var name: String
    var type: DuckDBType

    fn format_to(self, inout writer: Formatter):
        writer.write(
            "Column(", self.index, ", ", self.name, ": ", self.type, ")"
        )

    fn __str__(self) -> String:
        return str(self.type)


struct Result(Stringable, Formattable):
    var _result: duckdb_result
    var columns: List[Column]

    fn __init__(inout self, result: duckdb_result):
        self._result = result
        self.columns = List[Column]()
        for i in range(self.column_count()):
            var col = Column(
                index=i, name=self.column_name(i), type=self.column_type(i)
            )
            self.columns.append(col)

    fn column_count(self) -> Int:
        return int(
            _impl().duckdb_column_count(UnsafePointer.address_of(self._result))
        )

    fn column_name(self, col: Int) -> String:
        return _impl().duckdb_column_name(
            UnsafePointer.address_of(self._result), col
        )

    fn column_types(self) -> List[DuckDBType]:
        var types = List[DuckDBType]()
        for i in range(self.column_count()):
            types.append(self.column_type(i))
        return types

    fn column_type(self, col: Int) -> DuckDBType:
        return int(
            _impl().duckdb_column_type(
                UnsafePointer.address_of(self._result), col
            )
        )

    fn format_to(self, inout writer: Formatter):
        for col in self.columns:
            writer.write(col[], ", ")

    fn __str__(self) -> String:
        return String.format_sequence(self)

    # fn __iter__(self) -> ResultIterator:
    #     return ResultIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        return Chunk(_impl().duckdb_fetch_chunk(self._result))

    fn chunk_iterator(self) raises -> _ChunkIter[__lifetime_of(self)]:
        return _ChunkIter(self)

    fn __del__(owned self):
        _impl().duckdb_destroy_result(UnsafePointer.address_of(self._result))

    fn __moveinit__(inout self, owned existing: Self):
        self._result = existing._result
        self.columns = existing.columns

@value
struct MaterializedResult[lifetime: ImmutableLifetime]:
    var chunks: List[Reference[Chunk, lifetime]]