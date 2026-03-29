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
    _is_list_compatible_type,
)
from std.collections import Optional
from std.memory import UnsafePointer
from std.memory.unsafe_pointer import alloc
from std.reflection import (
    struct_field_count,
    struct_field_types,
    struct_field_names,
    get_type_name,
)
from std.builtin.rebind import downcast, rebind_var
from std.iter import Iterator, Iterable, StopIteration


struct Chunk[is_owned: Bool](Movable, Sized, Iterable):
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

    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = _ChunkRowIter[Self.is_owned, ImmutOrigin(iterable_origin)]

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
        comptime if Self.is_owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_data_chunk(UnsafePointer(to=self._chunk))

    fn __moveinit__(out self, deinit take: Self):
        self._chunk = take._chunk

    fn __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        """Iterate over rows in this chunk.
        
        Returns:
            An iterator yielding `Row` proxies for each row.
        
        Example:
            ```mojo
            for row in chunk:
                var name = row.get[String](col=0)
            ```
        """
        return _ChunkRowIter(0, Pointer(to=self))

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

        comptime if conforms_to(T, _NullableColumn):
            var expected = downcast[
                T, _NullableColumn
            ]._expected_duckdb_type()
            if actual_type != expected:
                # Allow array/map when expected is list
                if expected == DuckDBType.list:
                    if not _is_list_compatible_type(actual_type):
                        raise Error(
                            "Type mismatch: expected list, array, or map"
                            " but column has "
                            + String(actual_type)
                        )
                # Allow union when expected is struct
                elif expected == DuckDBType.struct_t:
                    if actual_type != DuckDBType.union:
                        raise Error(
                            "Type mismatch: expected struct or union"
                            " but column has "
                            + String(actual_type)
                        )
                # Allow enum when expected is varchar
                elif expected == DuckDBType.varchar:
                    if actual_type != DuckDBType.enum:
                        raise Error(
                            "Type mismatch: expected "
                            + String(expected)
                            + " but column has "
                            + String(actual_type)
                        )
                else:
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
            comptime if expected_db_type == DuckDBType.list:
                if not _is_list_compatible_type(actual_type):
                    raise Error(
                        "Type mismatch: expected list, array, or map"
                        " but column has "
                        + String(actual_type)
                    )
            else:
                if actual_type != expected_db_type:
                    comptime if expected_db_type == DuckDBType.struct_t:
                        if actual_type != DuckDBType.union:
                            raise Error(
                                "Type mismatch: expected struct or union"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected_db_type == DuckDBType.varchar:
                        if actual_type != DuckDBType.enum:
                            raise Error(
                                "Type mismatch: expected "
                                + String(expected_db_type)
                                + " but column has "
                                + String(actual_type)
                            )
                    else:
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

        comptime if conforms_to(T, _NullableColumn):
            var expected = downcast[
                T, _NullableColumn
            ]._expected_duckdb_type()
            if actual_type != expected:
                # Allow array/map when expected is list
                if expected == DuckDBType.list:
                    if not _is_list_compatible_type(actual_type):
                        raise Error(
                            "Type mismatch: expected list, array, or map"
                            " but column has "
                            + String(actual_type)
                        )
                # Allow union when expected is struct
                elif expected == DuckDBType.struct_t:
                    if actual_type != DuckDBType.union:
                        raise Error(
                            "Type mismatch: expected struct or union"
                            " but column has "
                            + String(actual_type)
                        )
                # Allow enum when expected is varchar
                elif expected == DuckDBType.varchar:
                    if actual_type != DuckDBType.enum:
                        raise Error(
                            "Type mismatch: expected "
                            + String(expected)
                            + " but column has "
                            + String(actual_type)
                        )
                else:
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
            comptime if expected_db_type == DuckDBType.list:
                if not _is_list_compatible_type(actual_type):
                    raise Error(
                        "Type mismatch: expected list, array, or map"
                        " but column has "
                        + String(actual_type)
                    )
            else:
                if actual_type != expected_db_type:
                    comptime if expected_db_type == DuckDBType.struct_t:
                        if actual_type != DuckDBType.union:
                            raise Error(
                                "Type mismatch: expected struct or union"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected_db_type == DuckDBType.varchar:
                        if actual_type != DuckDBType.enum:
                            raise Error(
                                "Type mismatch: expected "
                                + String(expected_db_type)
                                + " but column has "
                                + String(actual_type)
                            )
                    else:
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
        comptime assert mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t, "get[T](row=) is for struct types. For scalar/list values, use get[T](col=, row=)."

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
        comptime for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]
            var actual_type = self.type(idx)

            comptime if conforms_to(FT, _NullableColumn):
                var expected = downcast[
                    FT, _NullableColumn
                ]._expected_duckdb_type()
                if actual_type != expected:
                    if expected == DuckDBType.list:
                        if not _is_list_compatible_type(actual_type):
                            comptime field_name = struct_field_names[T]()[idx]
                            raise Error(
                                "Type mismatch for field '"
                                + String(field_name)
                                + "': expected list, array, or map"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected == DuckDBType.struct_t:
                        if actual_type != DuckDBType.union:
                            comptime field_name = struct_field_names[T]()[idx]
                            raise Error(
                                "Type mismatch for field '"
                                + String(field_name)
                                + "': expected struct or union"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected == DuckDBType.varchar:
                        if actual_type != DuckDBType.enum:
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
                comptime if expected_db_type == DuckDBType.list:
                    if not _is_list_compatible_type(actual_type):
                        comptime field_name = struct_field_names[T]()[idx]
                        raise Error(
                            "Type mismatch for field '"
                            + String(field_name)
                            + "': expected list, array, or map"
                            " but column has "
                            + String(actual_type)
                        )
                else:
                    if actual_type != expected_db_type:
                        comptime if expected_db_type == DuckDBType.struct_t:
                            if actual_type != DuckDBType.union:
                                comptime field_name = struct_field_names[T]()[idx]
                                raise Error(
                                    "Type mismatch for field '"
                                    + String(field_name)
                                    + "': expected struct or union"
                                    " but column has "
                                    + String(actual_type)
                                )
                        elif expected_db_type == DuckDBType.varchar:
                            if actual_type != DuckDBType.enum:
                                comptime field_name = struct_field_names[T]()[idx]
                                raise Error(
                                    "Type mismatch for field '"
                                    + String(field_name)
                                    + "': expected "
                                    + String(expected_db_type)
                                    + " but column has "
                                    + String(actual_type)
                                )
                        else:
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
        comptime for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]

            comptime if not conforms_to(FT, _NullableColumn):
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

        comptime for idx in range(field_count_):
            comptime FieldType = struct_field_types[T]()[idx]
            comptime FT = downcast[FieldType, Copyable & Movable]
            var vector = self.get_vector(idx)
            var dst = UnsafePointer(to=__struct_field_ref(idx, ptr[]))

            comptime if conforms_to(FT, _NullableColumn):
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
        comptime assert mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t, "get[T]() is for struct types. For column values, use get[T](col=)."

        var result = List[T](capacity=len(self))
        for row in range(len(self)):
            result.append(self.get[T](row=row))
        return result^

    fn get_tuple[
        *Ts: Copyable & Movable
    ](self, *, row: Int) raises -> Tuple[*Ts]:
        """Deserialize a table row into a Mojo Tuple.

        Maps each column to the corresponding Tuple element by position.
        Non-Optional elements raise on NULL; Optional elements become None.

        Parameters:
            Ts: The Mojo types for each column.

        Args:
            row: Row index.

        Returns:
            A Tuple whose elements are deserialized from the columns.

        Example:
            ```mojo
            var t = chunk.get_tuple[String, Int64](row=0)
            print(t[0], t[1])
            ```
        """
        comptime T = Tuple[*Ts]
        comptime n = T.__len__()

        if row < 0 or row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))

        if self.column_count() != n:
            raise Error(
                "Column count mismatch: tuple has "
                + String(n)
                + " elements but chunk has "
                + String(self.column_count())
                + " columns"
            )

        # Validate types and NULL constraints
        comptime for idx in range(n):
            comptime ET = T.element_types[idx]
            comptime ETC = downcast[ET, Copyable & Movable]
            var actual_type = self.type(idx)

            comptime if conforms_to(ETC, _NullableColumn):
                var expected = downcast[
                    ETC, _NullableColumn
                ]._expected_duckdb_type()
                if actual_type != expected:
                    if expected == DuckDBType.list:
                        if not _is_list_compatible_type(actual_type):
                            raise Error(
                                "Type mismatch for tuple element "
                                + String(idx)
                                + ": expected list, array, or map"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected == DuckDBType.struct_t:
                        if actual_type != DuckDBType.union:
                            raise Error(
                                "Type mismatch for tuple element "
                                + String(idx)
                                + ": expected struct or union"
                                " but column has "
                                + String(actual_type)
                            )
                    elif expected == DuckDBType.varchar:
                        if actual_type != DuckDBType.enum:
                            raise Error(
                                "Type mismatch for tuple element "
                                + String(idx)
                                + ": expected "
                                + String(expected)
                                + " but column has "
                                + String(actual_type)
                            )
                    else:
                        raise Error(
                            "Type mismatch for tuple element "
                            + String(idx)
                            + ": expected "
                            + String(expected)
                            + " but column has "
                            + String(actual_type)
                        )
            else:
                comptime expected_db_type = mojo_type_to_duckdb_type[ETC]()
                comptime if expected_db_type == DuckDBType.list:
                    if not _is_list_compatible_type(actual_type):
                        raise Error(
                            "Type mismatch for tuple element "
                            + String(idx)
                            + ": expected list, array, or map"
                            " but column has "
                            + String(actual_type)
                        )
                else:
                    if actual_type != expected_db_type:
                        comptime if expected_db_type == DuckDBType.struct_t:
                            if actual_type != DuckDBType.union:
                                raise Error(
                                    "Type mismatch for tuple element "
                                    + String(idx)
                                    + ": expected struct or union"
                                    " but column has "
                                    + String(actual_type)
                                )
                        elif expected_db_type == DuckDBType.varchar:
                            if actual_type != DuckDBType.enum:
                                raise Error(
                                    "Type mismatch for tuple element "
                                    + String(idx)
                                    + ": expected "
                                    + String(expected_db_type)
                                    + " but column has "
                                    + String(actual_type)
                                )
                        else:
                            raise Error(
                                "Type mismatch for tuple element "
                                + String(idx)
                                + ": expected "
                                + String(expected_db_type)
                                + " but column has "
                                + String(actual_type)
                            )
                if self.is_null(col=idx, row=row):
                    raise Error(
                        "NULL value for non-optional tuple element "
                        + String(idx)
                        + ". Use Optional["
                        + String(get_type_name[ETC]())
                        + "] to handle NULLs."
                    )

        # Construct the tuple element by element
        var ptr = alloc[T](1)
        __mlir_op.`lit.ownership.mark_initialized`(
            __get_mvalue_as_litref(ptr[]._mlir_value)
        )

        comptime for idx in range(n):
            comptime ET = T.element_types[idx]
            comptime ETC = downcast[ET, Copyable & Movable]
            var vector = self.get_vector(idx)

            comptime if conforms_to(ETC, _NullableColumn):
                var val = downcast[
                    ETC, _NullableColumn
                ]._deserialize_single_nullable(
                    vector, row, self.is_null(col=idx, row=row)
                )
                UnsafePointer(to=ptr[][idx]).init_pointee_move(
                    rebind_var[ET](val^)
                )
            else:
                var val = _deserialize_table_field[ETC](vector, row)
                UnsafePointer(to=ptr[][idx]).init_pointee_move(
                    rebind_var[ET](val^)
                )

        var result = ptr.take_pointee()
        ptr.free()
        return result^

    fn get_tuple[
        *Ts: Copyable & Movable
    ](self) raises -> List[downcast[Tuple[*Ts], Copyable]]:
        """Deserialize all rows into Mojo Tuples.

        Parameters:
            Ts: The Mojo types for each column.

        Returns:
            List[Tuple[*Ts]] — one tuple per row.

        Example:
            ```mojo
            var rows = chunk.get_tuple[String, Int64]()
            for i in range(len(rows)):
                print(rows[i][0], rows[i][1])
            ```
        """
        var result = List[downcast[Tuple[*Ts], Copyable]](capacity=len(self))
        for row in range(len(self)):
            result.append(rebind_var[downcast[Tuple[*Ts], Copyable]](self.get_tuple[*Ts](row=row)))
        return result^


# ──────────────────────────────────────────────────────────────────
# Row proxy
# ──────────────────────────────────────────────────────────────────


struct Row(Movable, Copyable):
    """A lightweight proxy for accessing a single row in a chunk.

    Row does not own the underlying data — it holds a raw pointer to the
    chunk's memory and a row index.  It is only valid while the chunk it
    was created from is alive (guaranteed by the ``for`` loop contract).
    """

    var _chunk_ptr: duckdb_data_chunk
    var _row: Int
    var _num_cols: Int

    fn __init__(out self, chunk_ptr: duckdb_data_chunk, row: Int, num_cols: Int):
        self._chunk_ptr = chunk_ptr
        self._row = row
        self._num_cols = num_cols

    fn get[T: Copyable & Movable](self, *, col: Int) raises -> T:
        """Get a typed value from this row.

        Parameters:
            T: The Mojo type to deserialize.

        Args:
            col: Column index.

        Returns:
            The deserialized value.

        Example:
            ```mojo
            for row in chunk:
                var name = row.get[String](col=0)
                var age = row.get[Int64](col=1)
            ```
        """
        var chunk = Chunk[is_owned=False](self._chunk_ptr)
        return chunk.get[T](col=col, row=self._row)

    fn is_null(self, *, col: Int) -> Bool:
        """Check whether the value at the given column is NULL.

        Args:
            col: Column index.
        """
        var chunk = Chunk[is_owned=False](self._chunk_ptr)
        return chunk.is_null(col=col, row=self._row)

    fn column_count(self) -> Int:
        """Returns the number of columns."""
        return self._num_cols

    fn row_index(self) -> Int:
        """Returns the row index within the chunk."""
        return self._row

    fn get_tuple[
        *Ts: Copyable & Movable
    ](self) raises -> Tuple[*Ts]:
        """Deserialize this row into a Mojo Tuple.

        Parameters:
            Ts: The Mojo types for each column.

        Returns:
            A Tuple whose elements are deserialized from the columns.

        Example:
            ```mojo
            for row in chunk:
                var t = row.get_tuple[String, Int64]()
                print(t[0], t[1])
            ```
        """
        var chunk = Chunk[is_owned=False](self._chunk_ptr)
        return chunk.get_tuple[*Ts](row=self._row)


# ──────────────────────────────────────────────────────────────────
# Chunk row iterator — makes Chunk iterable over Row proxies
# ──────────────────────────────────────────────────────────────────


@fieldwise_init
struct _ChunkRowIter[
    is_owned: Bool, origin: ImmutOrigin
](ImplicitlyCopyable, Iterable, Iterator):
    """Iterates over rows within a single Chunk, yielding Row proxies."""

    comptime Element = Row
    comptime IteratorType[
        iterable_mut: Bool, //, iterable_origin: Origin[mut=iterable_mut]
    ]: Iterator = Self

    var _index: Int
    var _chunk: Pointer[Chunk[Self.is_owned], Self.origin]

    fn __iter__(ref self) -> Self.IteratorType[origin_of(self)]:
        return self.copy()

    fn __next__(mut self) raises StopIteration -> Row:
        if self._index >= len(self._chunk[]):
            raise StopIteration()
        var row = Row(self._chunk[]._chunk, self._index, self._chunk[].column_count())
        self._index += 1
        return row^

    fn bounds(self) -> Tuple[Int, Optional[Int]]:
        var remaining = len(self._chunk[]) - self._index
        return (remaining, {remaining})


