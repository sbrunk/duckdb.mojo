from duckdb._libduckdb import *
from duckdb.vector import Vector
from duckdb.duckdb_type import *
from collections import Optional


struct Chunk(Movable & Sized):
    """Represents a DuckDB data chunk."""

    var _chunk: duckdb_data_chunk

    fn __init__(out self, chunk: duckdb_data_chunk):
        self._chunk = chunk

    fn __del__(owned self):
        duckdb_destroy_data_chunk(UnsafePointer(to=self._chunk))

    fn __moveinit__(out self, owned existing: Self):
        self._chunk = existing._chunk

    fn __len__(self) -> Int:
        return Int(duckdb_data_chunk_get_size(self._chunk))

    fn _get_vector(self, col: Int) -> Vector:
        return Vector(
            duckdb_data_chunk_get_vector(self._chunk, col),
            length=len(self),
        )

    @always_inline
    fn _check_bounds(self, col: Int) raises:
        if UInt64(col) >= duckdb_data_chunk_get_column_count(
            self._chunk
        ):
            raise Error(String("Column {} out of bounds.").format(col))

    @always_inline
    fn _check_bounds(self, col: Int, row: Int) raises:
        if row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))
        self._check_bounds(col)

    @always_inline
    fn type(self, col: Int) -> DuckDBType:
        return self._get_vector(col).get_column_type().get_type_id()

    fn is_null(self, *, col: Int) -> Bool:
        """Check if all values at the given and column are NULL."""
        var vector = self._get_vector(col)
        var validity_mask = vector._get_validity_mask()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        # TODO check validity mask doesn't contain any 0 bits
        return False

    fn is_null(self, *, col: Int, row: Int) -> Bool:
        """Check if the value at the given row and column is NULL."""
        var vector = self._get_vector(col)
        var validity_mask = vector._get_validity_mask()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
        return not is_valid

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], *, col: Int, row: Int) raises -> Optional[T]:
        self._check_bounds(col, row)
        if self.is_null(col=col, row=row):
            return None
        # TODO optimize single row access
        return self._get_vector(col).get(type)[row]

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], col: Int) raises -> List[Optional[T]]:
        self._check_bounds(col)
        if self.is_null(col=col):
            return List[Optional[T]](NoneType())
        return self._get_vector(col).get(type)

    # TODO remaining types


struct _ChunkIter[lifetime: ImmutableOrigin]:
    var _result: Pointer[Result, lifetime]
    var _next_chunk: duckdb_data_chunk

    fn __init__(out self, ref [lifetime]result: Result) raises:
        self._result = Pointer(to=result)
        self._next_chunk = duckdb_fetch_chunk(self._result[]._result)

    fn __del__(owned self):
        if self._next_chunk:
            _ = Chunk(self._next_chunk)

    fn __moveinit__(out self, owned existing: Self):
        self._result = existing._result
        self._next_chunk = existing._next_chunk

    fn __iter__(owned self) -> Self:
        return self^

    fn __next__(mut self) raises -> Chunk:
        if self._next_chunk:
            var current = self._next_chunk
            var next = duckdb_fetch_chunk(self._result[]._result)
            self._next_chunk = next
            return Chunk(current)
        else:
            raise Error("No more elements")

    @always_inline
    fn __has_next__(self) -> Bool:
        if self._next_chunk:
            return 1
        else:
            return 0


# struct ResultIterator:
#     var result: Result
#     var index: Int

#     fn __init__(out self, result: Result):
#         self.index = 0
#         self.result = result

#     # fn __index__(self) -> UInt64:
#     #     return self.index

#     # fn __len__(self) -> Int:
#     #     return Int(self.result.rows - self.index)  # TODO could overflow

#     fn __next__(out self) -> String:
#         self.index += 1
#         return String(self.index)
