from duckdb._libduckdb import *
from duckdb.vector import Vector
from duckdb.duckdb_type import *
from duckdb.logical_type import LogicalType
from collections import Optional
from memory import UnsafePointer
from memory.unsafe_pointer import alloc


struct Chunk(Movable & Sized):
    """Represents a DuckDB data chunk.
    
    Data chunks represent a horizontal slice of a table. They hold a number of vectors,
    that can each hold up to the VECTOR_SIZE rows (usually 2048).
    
    The Chunk can be either owning or non-owning (borrowed). When borrowed from DuckDB
    (e.g., in scalar function callbacks), the chunk will not be destroyed when it goes
    out of scope.
    """

    var _chunk: duckdb_data_chunk
    var _owned: Bool  # Tracks if this struct owns the underlying pointer

    fn __init__(out self, chunk: duckdb_data_chunk, take_ownership: Bool = False):
        """Creates a Chunk from a duckdb_data_chunk pointer.
        
        Args:
            chunk: The underlying duckdb_data_chunk pointer.
            take_ownership: Whether this Chunk owns the pointer (default: False for borrowed refs).
        """
        self._chunk = chunk
        self._owned = take_ownership

    fn __init__(out self, types: List[LogicalType]):
        """Creates an empty data chunk with the specified column types.
        
        This creates an owned chunk that will be destroyed when it goes out of scope.
        
        Args:
            types: A list of logical types for each column.
        """
        ref libduckdb = DuckDB().libduckdb()
        
        # Create array of duckdb_logical_type pointers
        var type_ptrs = alloc[duckdb_logical_type](len(types))
        for i in range(len(types)):
            type_ptrs[i] = types[i]._logical_type
        
        var chunk = libduckdb.duckdb_create_data_chunk(type_ptrs, UInt64(len(types)))
        type_ptrs.free()
        
        self._chunk = chunk
        self._owned = True  # This chunk is owned

    fn __del__(deinit self):
        """Destroys the chunk if it's owned by this instance."""
        if self._owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_data_chunk(UnsafePointer(to=self._chunk))

    fn __moveinit__(out self, deinit existing: Self):
        """Move constructor that transfers ownership."""
        self._chunk = existing._chunk
        self._owned = existing._owned

    fn __copyinit__(out self, existing: Self):
        """Copy constructor - creates a non-owning reference."""
        self._chunk = existing._chunk
        self._owned = False  # Copy doesn't own the resource

    fn __len__(self) -> Int:
        """Returns the current number of tuples (rows) in the data chunk.
        
        Returns:
            The number of tuples in the data chunk.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_data_chunk_get_size(self._chunk))

    fn column_count(self) -> Int:
        """Returns the number of columns in the data chunk.
        
        Returns:
            The number of columns in the data chunk.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_data_chunk_get_column_count(self._chunk))

    fn set_size(mut self, size: Int):
        """Sets the current number of tuples in the data chunk.
        
        Args:
            size: The number of tuples in the data chunk.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_data_chunk_set_size(self._chunk, UInt64(size))

    fn reset(mut self):
        """Resets the data chunk, clearing the validity masks and setting the cardinality to 0.
        
        After calling this method, you must call duckdb_vector_get_validity and 
        duckdb_vector_get_data to obtain current data and validity pointers.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_data_chunk_reset(self._chunk)

    fn get_vector(self, col: Int) -> Vector[origin_of(self)]:
        """Retrieves the vector at the specified column index in the data chunk.
        
        The pointer to the vector is valid for as long as the chunk is alive.
        It does NOT need to be destroyed.
        
        Args:
            col: The column index.
        
        Returns:
            The vector at the specified column.
        """
        ref libduckdb = DuckDB().libduckdb()
        return Vector(
            Pointer(to=self),
            libduckdb.duckdb_data_chunk_get_vector(self._chunk, UInt64(col)),
        )

    @always_inline
    fn _check_bounds(self, col: Int) raises:
        ref libduckdb = DuckDB().libduckdb()
        if UInt64(col) >= libduckdb.duckdb_data_chunk_get_column_count(
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
        """Returns the type of the specified column.
        
        Args:
            col: The column index.
        
        Returns:
            The DuckDBType of the column.
        """
        return self.get_vector(col).get_column_type().get_type_id()

    fn is_null(self, *, col: Int) -> Bool:
        """Check if all values at the given and column are NULL."""
        var validity_mask = self.get_vector(col).get_validity()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        # TODO check validity mask doesn't contain any 0 bits
        return False

    fn is_null(self, *, col: Int, row: Int) -> Bool:
        """Check if the value at the given row and column is NULL."""
        var validity_mask = self.get_vector(col).get_validity()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & UInt64((1 << idx_in_entry))
        return not is_valid

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], *, col: Int, row: Int) raises -> Optional[T]:
        self._check_bounds(col, row)
        if self.is_null(col=col, row=row):
            return None
        # TODO optimize single row access
        return self.get_vector(col).get(type, len(self))[row]

    fn get[
        T: Copyable & Movable, //
    ](self, type: Col[T], col: Int) raises -> List[Optional[T]]:
        self._check_bounds(col)
        if self.is_null(col=col):
            return [None]
        return self.get_vector(col).get(type, len(self))

    # TODO remaining types


struct _ChunkIter[lifetime: ImmutOrigin]:
    var _result: Pointer[Result, Self.lifetime]
    var _next_chunk: duckdb_data_chunk

    fn __init__(out self, ref [Self.lifetime]result: Result) raises:
        ref libduckdb = DuckDB().libduckdb()
        self._result = Pointer(to=result)
        self._next_chunk = libduckdb.duckdb_fetch_chunk(self._result[]._result)

    fn __del__(deinit self):
        if self._next_chunk:
            _ = Chunk(self._next_chunk)

    fn __moveinit__(out self, deinit existing: Self):
        self._result = existing._result
        self._next_chunk = existing._next_chunk

    fn __iter__(var self) -> Self:
        return self^

    fn __next__(mut self) raises -> Chunk:
        if self._next_chunk:
            var current = self._next_chunk
            ref libduckdb = DuckDB().libduckdb()
            var next = libduckdb.duckdb_fetch_chunk(self._result[]._result)
            self._next_chunk = next
            return Chunk(current)
        else:
            raise Error("No more elements")

    @always_inline
    fn __has_next__(self) -> Bool:
        if self._next_chunk:
            return True
        else:
            return False


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
