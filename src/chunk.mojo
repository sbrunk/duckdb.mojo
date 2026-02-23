from duckdb._libduckdb import *
from duckdb.vector import Vector
from duckdb.duckdb_type import *
from duckdb.logical_type import LogicalType
from duckdb.typed_api import (
    mojo_type_to_duckdb_type,
    deserialize_from_vector,
    deserialize_list_column,
    _deserialize_table_field,
    _NullableColumn,
)
from collections import Optional
from memory import UnsafePointer
from memory.unsafe_pointer import alloc
from reflection import (
    struct_field_count,
    struct_field_types,
    struct_field_names,
    get_type_name,
)
from std.builtin.rebind import downcast


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
        T: Copyable & Movable
    ](self, *, col: Int, row: Int) raises -> T:
        """Get a single typed value from the chunk.

        When T is a plain type (e.g. Int64), raises on NULL.
        When T is Optional[X], returns None for NULL values.

        Parameters:
            T: The Mojo type to deserialize. Use Optional[T] for nullable values.

        Args:
            col: Column index.
            row: Row index.

        Returns:
            The deserialized value (or Optional[X] when T is Optional).

        Example:
            ```mojo
            var value = chunk.get[Int64](col=0, row=0)         # raises on NULL
            var maybe = chunk.get[Optional[Int64]](col=0, row=0)  # None on NULL
            ```
        """
        self._check_bounds(col, row)

        # Validate column type matches expected Mojo type
        var actual_type = self.type(col)

        @parameter
        if conforms_to(T, _NullableColumn):
            var expected = downcast[
                T, _NullableColumn
            ]._expected_duckdb_type()
            if actual_type != expected:
                raise Error(
                    "Type mismatch: expected "
                    + String(expected)
                    + " but column has "
                    + String(actual_type)
                )
            var val = downcast[
                T, _NullableColumn
            ]._deserialize_single_nullable(
                self.get_vector(col), row, self.is_null(col=col, row=row)
            )
            return rebind_var[T](val^)
        else:
            comptime expected_db_type = mojo_type_to_duckdb_type[T]()
            if actual_type != expected_db_type:
                raise Error(
                    "Type mismatch: expected "
                    + String(expected_db_type)
                    + " but column has "
                    + String(actual_type)
                )
            if self.is_null(col=col, row=row):
                raise Error(
                    "NULL value at col="
                    + String(col)
                    + ", row="
                    + String(row)
                    + ". Use get[Optional["
                    + String(get_type_name[T]())
                    + "]](col=, row=) for nullable values."
                )
            return _deserialize_table_field[T](self.get_vector(col), row)

    fn get[
        T: Copyable & Movable
    ](self, *, col: Int) raises -> List[T]:
        """Get all typed values from a column.

        When T is a plain type, raises if any value is NULL.
        When T is Optional[X], NULL entries become None.

        Parameters:
            T: The Mojo type to deserialize. Use Optional[T] for nullable values.

        Args:
            col: Column index.

        Returns:
            List[T] containing all values.

        Example:
            ```mojo
            var ints = chunk.get[Int64](col=0)               # raises on NULL
            var maybes = chunk.get[Optional[Int64]](col=0)   # None on NULL
            ```
        """
        self._check_bounds(col)

        # Validate column type matches expected Mojo type
        var actual_type = self.type(col)

        @parameter
        if conforms_to(T, _NullableColumn):
            var expected = downcast[
                T, _NullableColumn
            ]._expected_duckdb_type()
            if actual_type != expected:
                raise Error(
                    "Type mismatch: expected "
                    + String(expected)
                    + " but column has "
                    + String(actual_type)
                )
            var result = downcast[
                T, _NullableColumn
            ]._deserialize_column_nullable(
                self.get_vector(col), len(self), 0
            )
            return rebind_var[List[T]](result^)
        else:
            comptime expected_db_type = mojo_type_to_duckdb_type[T]()
            if actual_type != expected_db_type:
                raise Error(
                    "Type mismatch: expected "
                    + String(expected_db_type)
                    + " but column has "
                    + String(actual_type)
                )
            var opt_result = deserialize_from_vector[T](
                self.get_vector(col), len(self), 0
            )
            var result = List[T](capacity=len(opt_result))
            for i in range(len(opt_result)):
                if not opt_result[i]:
                    raise Error(
                        "NULL value at row "
                        + String(i)
                        + ". Use get[Optional["
                        + String(get_type_name[T]())
                        + "]](col=) for nullable values."
                    )
                result.append(opt_result[i].value().copy())
            return result^

    fn get[
        T: Copyable & Movable
    ](self, *, row: Int) raises -> T:
        """Deserialize a table row into a Mojo struct.

        Maps each column in the chunk to a field in T by position.
        Non-Optional fields raise on NULL; Optional fields become None.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            row: Row index.

        Returns:
            The deserialized struct.

        Example:
            ```mojo
            @fieldwise_init
            struct User(Copyable, Movable):
                var name: String
                var age: Optional[Int64]  # nullable

            var user = chunk.get[User](row=0)
            ```
        """
        constrained[
            mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t,
            "get[T](row=) is for struct types. For scalar/list values, use get[T](col=, row=).",
        ]()

        if row < 0 or row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))

        comptime field_count_ = struct_field_count[T]()

        # Validate column count matches field count
        if self.column_count() != field_count_:
            raise Error(
                "Column count mismatch: struct "
                + String(get_type_name[T]())
                + " has "
                + String(field_count_)
                + " fields but chunk has "
                + String(self.column_count())
                + " columns"
            )

        # Validate column types match field types
        @parameter
        for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]
            var actual_type = self.type(idx)

            @parameter
            if conforms_to(FT, _NullableColumn):
                var expected = downcast[
                    FT, _NullableColumn
                ]._expected_duckdb_type()
                if actual_type != expected:
                    comptime field_name = struct_field_names[T]()[idx]
                    raise Error(
                        "Type mismatch for field '"
                        + String(field_name)
                        + "': expected "
                        + String(expected)
                        + " but column has "
                        + String(actual_type)
                    )
            else:
                comptime expected_db_type = mojo_type_to_duckdb_type[FT]()
                if actual_type != expected_db_type:
                    comptime field_name = struct_field_names[T]()[idx]
                    raise Error(
                        "Type mismatch for field '"
                        + String(field_name)
                        + "': expected "
                        + String(expected_db_type)
                        + " but column has "
                        + String(actual_type)
                    )

        # Check non-Optional fields for NULL before allocating
        @parameter
        for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]

            @parameter
            if not conforms_to(FT, _NullableColumn):
                if self.is_null(col=idx, row=row):
                    comptime field_name = struct_field_names[T]()[idx]
                    raise Error(
                        "NULL value for non-optional field '"
                        + String(field_name)
                        + "'. Declare as Optional["
                        + String(get_type_name[FT]())
                        + "] to handle NULLs."
                    )

        # Deserialize each field from its column vector
        var ptr = alloc[T](1)

        @parameter
        for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]
            var vector = self.get_vector(idx)
            var dst = UnsafePointer(to=__struct_field_ref(idx, ptr[]))

            @parameter
            if conforms_to(FT, _NullableColumn):
                var val = downcast[
                    FT, _NullableColumn
                ]._deserialize_single_nullable(
                    vector, row, self.is_null(col=idx, row=row)
                )
                dst.bitcast[FT]().init_pointee_move(rebind_var[FT](val^))
            else:
                var val = _deserialize_table_field[FT](vector, row)
                dst.bitcast[FT]().init_pointee_move(val^)

        var result = ptr.take_pointee()
        ptr.free()
        return result^

    fn get[
        T: Copyable & Movable
    ](self) raises -> List[T]:
        """Deserialize all table rows into Mojo structs.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Returns:
            List[T] — one struct per row.

        Example:
            ```mojo
            @fieldwise_init
            struct User(Copyable, Movable):
                var name: String
                var age: Optional[Int64]  # nullable

            var users = chunk.get[User]()
            ```
        """
        constrained[
            mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t,
            "get[T]() is for struct types. For column values, use get[T](col=).",
        ]()

        var result = List[T](capacity=len(self))
        for row in range(len(self)):
            result.append(self.get[T](row=row))
        return result^


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
