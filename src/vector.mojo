from duckdb._libduckdb import *
from duckdb.logical_type import *
from duckdb.duckdb_wrapper import *
from collections import Optional

from sys.intrinsics import _type_is_eq


struct Vector[is_owned: Bool, origin: ImmutOrigin]:
    """A wrapper around a DuckDB vector.
    
    Vectors can be borrowed from a Chunk or owned standalone. Ownership is tracked
    at compile-time via the `is_owned` parameter:
    - `Vector[False, ...]` : Borrowed from a Chunk or DuckDB, will not be destroyed
    - `Vector[True, ...]`: Owned standalone vector, will be destroyed when it goes out of scope
    
    The origin parameter tracks the lifetime dependency:
    - For standalone vectors: ImmutExternalOrigin (no dependency)
    - For vectors from chunks: origin_of(chunk) (extends chunk's lifetime)
    
    Parameters:
        is_owned: Whether this Vector owns its pointer and should destroy it.
        origin: The origin tracking lifetime dependencies.
    """
    var _vector: duckdb_vector

    fn __init__(out self: Vector[is_owned=False, origin=Self.origin], vector: duckdb_vector):
        """Creates a borrowed Vector (not owned).
        
        This creates a `Vector` that does not own the vector and will not
        destroy it when it goes out of scope. Used for vectors from chunks or managed by DuckDB.
        
        Args:
            vector: The duckdb_vector pointer.
        """
        self._vector = vector

    fn __init__(out self: Vector[is_owned=True, origin=MutExternalOrigin], type: LogicalType, capacity: idx_t):
        """Creates a standalone owned vector.
        
        This creates a `Vector` that owns the underlying duckdb_vector
        and will destroy it when it goes out of scope.

        Args:
            type: The logical type of the vector.
            capacity: The capacity of the vector.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._vector = libduckdb.duckdb_create_vector(type._logical_type, capacity)

    fn __del__(deinit self):
        """Destroys standalone owned vectors."""
        @parameter
        if Self.is_owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_vector(UnsafePointer(to=self._vector))

    fn get_column_type(ref [_]self: Self) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        """Retrieves the column type of the specified vector.

        * returns: The type of the vector
        """
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](libduckdb.duckdb_vector_get_column_type(self._vector))

    fn get_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the data pointer of the vector.

        The data pointer can be used to read or write values from the vector.
        How to read or write values depends on the type of the vector.

        * returns: The data pointer
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_get_data(self._vector)

    fn get_validity(self) -> UnsafePointer[UInt64, MutAnyOrigin]:
        """Retrieves the validity mask pointer of the specified vector.

        If all values are valid, this function MIGHT return NULL!

        The validity mask is a bitset that signifies null-ness within the data chunk.
        It is a series of UInt64 values, where each UInt64 value contains validity for 64 tuples.
        The bit is set to 1 if the value is valid (i.e. not NULL) or 0 if the value is invalid (i.e. NULL).

        Validity of a specific value can be obtained like this:

        idx_t entry_idx = row_idx / 64;
        idx_t idx_in_entry = row_idx % 64;
        Bool is_valid = validity_mask[entry_idx] & (1 << idx_in_entry);

        Alternatively, the (slower) duckdb_validity_row_is_valid function can be used.

        * returns: The pointer to the validity mask, or NULL if no validity mask is present
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_get_validity(self._vector)

    fn ensure_validity_writable(self) -> NoneType:
        """Ensures the validity mask is writable by allocating it.

        After this function is called, `get_validity` will ALWAYS return non-NULL.
        This allows null values to be written to the vector, regardless of whether a validity mask was present before.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_ensure_validity_writable(self._vector)

    fn assign_string_element(self, index: idx_t, str: String) -> NoneType:
        """Assigns a string element in the vector at the specified location.

        * index: The row position in the vector to assign the string to
        * str: The null-terminated string
        """
        var _str = str.copy()
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_assign_string_element(self._vector, index, _str.as_c_string_slice().unsafe_ptr())

    fn assign_string_element_len(self, index: idx_t, str: String, str_len: idx_t) -> NoneType:
        """Assigns a string element in the vector at the specified location. You may also use this function to assign BLOBs.

        * index: The row position in the vector to assign the string to
        * str: The string
        * str_len: The length of the string (in bytes)
        """
        var _str = str.copy()
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_assign_string_element_len(self._vector, index, _str.as_c_string_slice().unsafe_ptr(), str_len)

    fn list_get_child(self) -> Vector[is_owned=False, origin=origin_of(self)]:
        """Retrieves the child vector of a list vector.

        The resulting vector is valid as long as the parent vector is valid.

        * vector: The vector
        * returns: The child vector (borrowed, not owned)
        """
        ref libduckdb = DuckDB().libduckdb()
        return Vector[is_owned=False, origin=origin_of(self)](
            libduckdb.duckdb_list_vector_get_child(self._vector),
        )

    fn list_get_size(self) -> idx_t:
        """Returns the size of the child vector of the list.

        * returns: The size of the child list
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_list_vector_get_size(self._vector)

    fn list_set_size(self, size: idx_t) -> duckdb_state:
        """Sets the total size of the underlying child-vector of a list vector.

        * size: The size of the child list.
        * returns: The duckdb state. Returns DuckDBError if the vector is nullptr.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_list_vector_set_size(self._vector, size)

    fn list_reserve(self, required_capacity: idx_t) -> duckdb_state:
        """Sets the total capacity of the underlying child-vector of a list.

        * required_capacity: the total capacity to reserve.
        * returns: The duckdb state. Returns DuckDBError if the vector is nullptr.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_list_vector_reserve(self._vector, required_capacity)

    fn struct_get_child(self, index: idx_t) -> Vector[is_owned=False, origin=origin_of(self)]:
        """Retrieves the child vector of a struct vector.

        The resulting vector is valid as long as the parent vector is valid.

        * index: The child index
        * returns: The child vector (borrowed, not owned)
        """
        ref libduckdb = DuckDB().libduckdb()
        return Vector[False, origin_of(self)](
            libduckdb.duckdb_struct_vector_get_child(self._vector, index),
        )

    fn array_get_child(self) -> Vector[is_owned=False, origin=origin_of(self)]:
        """Retrieves the child vector of an array vector.

        The resulting vector is valid as long as the parent vector is valid.
        The resulting vector has the size of the parent vector multiplied by the array size.

        * returns: The child vector (borrowed, not owned)
        """
        ref libduckdb = DuckDB().libduckdb()
        var child_vector = libduckdb.duckdb_array_vector_get_child(self._vector)
        return Vector[False, origin_of(self)](
            child_vector,
        )

    fn slice(self, sel: duckdb_selection_vector, len: idx_t) -> NoneType:
        """Slice a vector with a selection vector.
        
        The length of the selection vector must be less than or equal to the length of the vector.
        Turns the vector into a dictionary vector.

        * sel: The selection vector.
        * len: The length of the selection vector.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_slice_vector(self._vector, sel, len)

    fn copy_sel(
        self,
        dst: Vector[_],
        sel: duckdb_selection_vector,
        src_count: idx_t,
        src_offset: idx_t,
        dst_offset: idx_t,
    ) -> NoneType:
        """Copy this vector to the dst with a selection vector that identifies which indices to copy.

        * dst: The vector to copy to.
        * sel: The selection vector. The length of the selection vector should not be more than the length of the src vector
        * src_count: The number of entries from selection vector to copy. Think of this as the effective length of the
        selection vector starting from index 0
        * src_offset: The offset in the selection vector to copy from (important: actual number of items copied =
        src_count - src_offset).
        * dst_offset: The offset in the dst vector to start copying to.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_copy_sel(self._vector, dst._vector, sel, src_count, src_offset, dst_offset)

    fn reference_value(self, value: duckdb_value) -> NoneType:
        """Copies the value from `value` to this vector.

        * value: The value to copy into the vector.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_reference_value(self._vector, value)

    fn reference_vector(self, from_vector: Vector[_]) -> NoneType:
        """Changes this vector to reference `from_vector`. After, the vectors share ownership of the data.

        * from_vector: The vector to reference.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_vector_reference_vector(self._vector, from_vector._vector)

    fn _check_type[db_is_owned: Bool, db_origin: ImmutOrigin](self, db_type: LogicalType[db_is_owned, db_origin]) raises:
        """Recursively check that the runtime type of the vector matches the expected type.
        """
        var self_type_id = self.get_column_type().get_type_id()
        var db_type_id = db_type.get_type_id()
        if self_type_id != db_type_id:
            raise "Expected type " + String(db_type) + " but got " + String(
                self_type_id
            )

    fn get[
        T: Copyable & Movable, //
    ](self, expected_type: Col[T], length: Int) raises -> List[Optional[T]]:
        """Convert the data from this vector into native Mojo data structures.
        """

        self._check_type(expected_type.type())

        var type = self.get_column_type().get_type_id()
        if type == DuckDBType.blob:
            raise Error("Blobs are not supported yet")
        if type == DuckDBType.decimal:
            raise Error("Decimals are not supported yet")
        if type == DuckDBType.timestamp_s:
            raise Error(
                "Timestamps with second precision are not supported yet"
            )
        if type == DuckDBType.timestamp_ms:
            raise Error(
                "Timestamps with millisecond precision are not supported yet"
            )
        if type == DuckDBType.timestamp_ns:
            raise Error(
                "Timestamps with nanosecond precision are not supported yet"
            )
        if type == DuckDBType.enum:
            raise Error("Enums are not supported yet")

        # Columns are essentially lists so we can use the same logic for getting the values.
        var result = DuckDBList[expected_type.Builder](
            self, length=length, offset=0
        )
        # The way we are building our Mojo representation of the data currently via the DuckDBWrapper
        # trait, with different __init__ implementations depending on the concrete type, means
        # that the types don't match.
        #
        # We can cast the result to the expected type though because
        # 1. We have ensured that the runtime type matches the expected type through _check_type
        # 2. The DuckDBWrapper implementations are all thin wrappers with conversion logic
        # around the underlying type we're converting into.
        return rebind[List[Optional[T]]](result).copy()
        
