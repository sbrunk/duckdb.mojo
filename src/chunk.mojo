from duckdb._libduckdb import *
from duckdb.vector import Vector
from duckdb.duckdb_type import *
from duckdb.logical_type import LogicalType
from collections import Optional
from memory import UnsafePointer
from memory.unsafe_pointer import alloc


struct Chunk[is_owned: Bool](Movable, Sized):
    """Represents a DuckDB data chunk.
    
    Data chunks represent a horizontal slice of a table. They hold a number of vectors,
    that can each hold up to the VECTOR_SIZE rows (usually 2048).
    
    The Chunk can be either owning or non-owning (borrowed). Ownership is tracked
    at compile-time via the `is_owned` parameter:
    - `Chunk[is_owned=False]`: Borrowed from DuckDB, will not be destroyed
    - `Chunk[is_owned=True]`: Owned by us, will be destroyed when it goes out of scope
    
    Parameters:
        is_owned: Whether this Chunk owns its pointer and should destroy it (required).
    """

    var _chunk: duckdb_data_chunk

    fn __init__(out self: Chunk[is_owned=False], chunk: duckdb_data_chunk):
        """Creates a borrowed (non-owning) Chunk from a duckdb_data_chunk pointer.
        
        This constructor creates a `Chunk[False]` which will not destroy the underlying
        pointer when it goes out of scope. Use this for chunks obtained from DuckDB
        (e.g., in result sets or scalar function callbacks).
        
        Args:
            chunk: The underlying duckdb_data_chunk pointer.
        """
        self._chunk = chunk

    fn __init__(out self: Chunk[is_owned=True], var chunk: duckdb_data_chunk):
        """Takes ownership of an existing duckdb_data_chunk.
        
        This constructor creates a `Chunk[is_owned=True]` that takes ownership of a chunk
        returned by DuckDB (e.g., from duckdb_fetch_chunk). The chunk will be
        destroyed when this object goes out of scope.
        
        Args:
            chunk: The duckdb_data_chunk to take ownership of.
        """
        self._chunk = chunk

    fn __init__(out self: Chunk[is_owned=True], types: List[LogicalType[is_owned=True, origin=MutExternalOrigin]]):
        """Creates an empty data chunk with the specified column types.
        
        This creates an owned `Chunk[True]` that will be destroyed when it goes out of scope.
        Use this when you need to create a new chunk that you own.
        
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

    fn __del__(deinit self):
        """Destroys the chunk if owned.
        
        This uses compile-time conditional logic to only destroy owned chunks.
        """
        @parameter
        if Self.is_owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_data_chunk(UnsafePointer(to=self._chunk))

    fn __moveinit__(out self, deinit take: Self):
        self._chunk = take._chunk

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

    fn get_vector(self, col: Int) -> Vector[is_owned=False, origin=origin_of(self)]:
        """Retrieves a mutable vector at the specified column index in the data chunk.
        
        The pointer to the vector is valid for as long as the chunk is alive.
        It does NOT need to be destroyed. The returned vector extends the chunk's lifetime.
        
        Args:
            col: The column index.
        
        Returns:
            A borrowed mutable vector (not owned) at the specified column.  
        """
        ref libduckdb = DuckDB().libduckdb()
        return Vector[False, origin_of(self)](
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
        """Destroys the pending chunk if one exists."""
        if self._next_chunk:
            # Create an owned Chunk to properly destroy it
            _ = Chunk[is_owned=True](self._next_chunk)

    fn __moveinit__(out self, deinit take: Self):
        self._result = take._result
        self._next_chunk = take._next_chunk

    fn __iter__(var self) -> Self:
        return self^

    fn __next__(mut self) raises -> Chunk[is_owned=True]:
        """Returns the next owned chunk from the result set.
        
        The returned chunk must be destroyed (automatically via __del__).
        Per DuckDB C API: chunks from duckdb_fetch_chunk must be destroyed.
        """
        if self._next_chunk:
            var current = self._next_chunk
            ref libduckdb = DuckDB().libduckdb()
            var next = libduckdb.duckdb_fetch_chunk(self._result[]._result)
            self._next_chunk = next
            return Chunk[is_owned=True](current)
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
