from duckdb._c_api.libduckdb import _impl
from duckdb.chunk import Chunk, _ChunkIter
from collections import Optional


@value
struct Column(Writable, Stringable):
    var index: Int
    var name: String
    var type: LogicalType

    fn write_to[W: Writer](self, inout writer: W):
        writer.write(
            "Column(", self.index, ", ", self.name, ": ", self.type, ")"
        )

    fn __str__(self) -> String:
        return str(self.type)


struct Result(Writable, Stringable):
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

    fn column_types(self) -> List[LogicalType]:
        var types = List[LogicalType]()
        for i in range(self.column_count()):
            types.append(self.column_type(i))
        return types

    fn column_type(self, col: Int) -> LogicalType:
        return LogicalType(
            _impl().duckdb_column_logical_type(
                UnsafePointer.address_of(self._result), col
            )
        )

    fn write_to[W: Writer](self, inout writer: W):
        for col in self.columns:
            writer.write(col[], ", ")

    fn __str__(self) -> String:
        return String.write(self)

    # fn __iter__(self) -> ResultIterator:
    #     return ResultIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        return Chunk(_impl().duckdb_fetch_chunk(self._result))

    fn chunk_iterator(self) raises -> _ChunkIter[__origin_of(self)]:
        return _ChunkIter(self)

    fn fetch_all(owned self) raises -> MaterializedResult:
        return MaterializedResult(self^)

    fn __del__(owned self):
        _impl().duckdb_destroy_result(UnsafePointer.address_of(self._result))

    fn __moveinit__(inout self, owned existing: Self):
        self._result = existing._result
        self.columns = existing.columns


struct MaterializedResult(Sized):
    """A result with all rows fetched into memory."""

    var result: Result
    var chunks: List[UnsafePointer[Chunk]]
    var size: UInt

    fn __init__(inout self, owned result: Result) raises:
        self.result = result^
        self.chunks = List[UnsafePointer[Chunk]]()
        self.size = 0
        for chunk in self.result.chunk_iterator():
            self.size += len(chunk)
            var chunk_ptr = UnsafePointer[Chunk].alloc(1)
            chunk_ptr.init_pointee_move(chunk^)
            self.chunks.append(chunk_ptr)

    fn column_count(self) -> Int:
        return self.result.column_count()

    fn column_name(self, col: Int) -> String:
        return self.result.column_name(col)

    fn column_types(self) -> List[LogicalType]:
        return self.result.column_types()

    fn column_type(self, col: Int) -> LogicalType:
        return self.result.column_type(col)

    fn columns(self) -> List[Column]:
        return self.result.columns

    fn __len__(self) -> Int:
        return self.size

    fn get[
        T: CollectionElement, //
    ](self, type: Col[T], col: UInt) raises -> List[Optional[T]]:
        var result = List[Optional[T]](
            capacity=len(self.chunks) * int(_impl().duckdb_vector_size())
        )
        for chunk_ptr in self.chunks:
            result.extend(chunk_ptr[][].get(type, col))
        return result

    fn get[
        T: CollectionElement, //
    ](self, type: Col[T], col: UInt, row: UInt) raises -> Optional[T]:
        if row < 0 or row >= self.size:
            raise Error("Row index out of bounds")
        var chunk_idx = int(row // _impl().duckdb_vector_size())
        var chunk_offset = int(row % _impl().duckdb_vector_size())
        return self.chunks[chunk_idx][].get(type, col=col, row=chunk_offset)

    fn __del__(owned self):
        for chunk_ptr in self.chunks:
            chunk_ptr[].destroy_pointee()
            chunk_ptr[].free()
