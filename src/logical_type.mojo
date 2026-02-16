from duckdb._libduckdb import *
from collections import List



struct LogicalType[is_owned: Bool, origin: ImmutOrigin](ImplicitlyCopyable & Movable & Equatable & Writable & Stringable):
    """Represents a potentially nested DuckDB type.
    
    LogicalTypes can be borrowed from DuckDB structures or owned standalone. Ownership
    is tracked at compile-time via the `is_owned` parameter:
    - `LogicalType[False, ...]`: Borrowed from DuckDB, will not be destroyed
    - `LogicalType[True, ...]`: Owned standalone type, will be destroyed when it goes out of scope
    
    Parameters:
        is_owned: Whether this LogicalType owns its pointer and should destroy it.
        origin: The origin tracking lifetime dependencies.
    """
    var _logical_type: duckdb_logical_type


    fn __init__(out self: LogicalType[is_owned=True, origin=MutExternalOrigin], type_id: DuckDBType):
        """Creates an owned LogicalType from a standard primitive type."""
        ref libduckdb = DuckDB().libduckdb()
        self._logical_type = libduckdb.duckdb_create_logical_type(type_id.value)

    fn __init__(out self: LogicalType[is_owned=False, origin=Self.origin], logical_type: duckdb_logical_type):
        """Creates a borrowed LogicalType (not owned).
        
        This creates a `LogicalType` that does not own the logical type and will not
        destroy it when it goes out of scope. Used for types from DuckDB structures.
        
        Args:
            logical_type: The duckdb_logical_type pointer.
        """
        self._logical_type = logical_type

    fn __init__(out self: LogicalType[is_owned=True, origin=MutExternalOrigin], var logical_type: duckdb_logical_type):
        """Creates an owned LogicalType from a duckdb_logical_type pointer.
        
        This takes ownership of the logical type and will destroy it.
        
        Args:
            logical_type: The duckdb_logical_type pointer.
        """
        self._logical_type = logical_type

    fn __copyinit__(out self, other: Self):
        @parameter
        if Self.is_owned:
            if other.get_type_id() == DuckDBType.list:
                var child = other.list_type_child_type()
                var list_type = child.create_list_type()
                # Take ownership of the pointer before list_type's destructor runs
                self._logical_type = list_type._logical_type
                # Prevent list_type from destroying the pointer we just took
                list_type._logical_type = duckdb_logical_type()
            # TODO remaining nested types
            else:
                ref libduckdb = DuckDB().libduckdb()
                self._logical_type = libduckdb.duckdb_create_logical_type(other.get_type_id().value)
        else:
            self._logical_type = other._logical_type

    fn __moveinit__(out self, deinit other: Self):
        self._logical_type = other._logical_type

    fn __del__(deinit self):
        """Destroys owned LogicalTypes only."""
        @parameter
        if Self.is_owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_logical_type(
                UnsafePointer(to=self._logical_type)
            )

    fn create_list_type(self) -> LogicalType[is_owned=True, origin=MutExternalOrigin]:
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=True, origin=MutExternalOrigin](libduckdb.duckdb_create_list_type(self._logical_type))

    fn internal_ptr(self) -> duckdb_logical_type:
        return self._logical_type

    fn get_type_id(self) -> DuckDBType:
        """Retrieves the enum type class of this `LogicalType`.

        * type: The logical type object
        * returns: The type id
        """
        ref libduckdb = DuckDB().libduckdb()
        return DuckDBType(libduckdb.duckdb_get_type_id(self._logical_type))

    fn list_type_child_type(ref [_]self: Self) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        """Retrieves the child type of the given list type.
        
        The returned type is borrowed from this list type and should not be destroyed separately.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](libduckdb.duckdb_list_type_child_type(self._logical_type))

    fn map_type_key_type(ref [_]self: Self) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        """Retrieves the key type of the given map type.
        
        The returned type is borrowed from this map type and should not be destroyed separately.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](libduckdb.duckdb_map_type_key_type(self._logical_type))

    fn map_type_value_type(ref [_]self: Self) -> LogicalType[is_owned=False, origin=origin_of(self)]:
        """Retrieves the value type of the given map type.
        
        The returned type is borrowed from this map type and should not be destroyed separately.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return LogicalType[is_owned=False, origin=origin_of(self)](libduckdb.duckdb_map_type_value_type(self._logical_type))

    fn array_type_array_size(self) -> idx_t:
        """Retrieves the array size of the given array type.

        * returns: The fixed number of elements the values of this array type can store.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_array_type_array_size(self._logical_type)

    fn __eq__(self, other: Self) -> Bool:
        if self.get_type_id() != other.get_type_id():
            return False
        if self.get_type_id() == DuckDBType.list:
            # Compare child types by ID to avoid origin mismatch
            return self.list_type_child_type().get_type_id() == other.list_type_child_type().get_type_id()
        if self.get_type_id() == DuckDBType.map:
            # Compare child types by ID to avoid origin mismatch
            return (
                self.map_type_key_type().get_type_id() == other.map_type_key_type().get_type_id()
                and self.map_type_value_type().get_type_id() == other.map_type_value_type().get_type_id()
            )
        # TODO remaining nested types
        return True

    fn __ne__(self, other: Self) -> Bool:
        return not self == other

    fn __str__(self) -> String:
        if self.get_type_id() == DuckDBType.list:
            return "list(" + String(self.list_type_child_type()) + ")"
        if self.get_type_id() == DuckDBType.map:
            return (
                "map("
                + String(self.map_type_key_type())
                + ","
                + String(self.map_type_value_type())
                + ")"
            )
        # TODO remaining nested types
        return String(self.get_type_id())

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(String(self))

fn decimal_type(width: UInt8, scale: UInt8) -> LogicalType[True, MutExternalOrigin]:
    """Creates a decimal type.

    Args:
        width: The width of the decimal type.
        scale: The scale of the decimal type.

    Returns:
        A new LogicalType representing the decimal type.
    """
    ref libduckdb = DuckDB().libduckdb()
    return LogicalType[True, MutExternalOrigin](
        libduckdb.duckdb_create_decimal_type(width, scale)
    )

fn enum_type(mut names: List[String]) -> LogicalType[True, MutExternalOrigin]:
    """Creates an enum type.

    Args:
        names: The list of names for the enum.

    Returns:
        A new LogicalType representing the enum type.
    """
    ref libduckdb = DuckDB().libduckdb()
    var count = len(names)
    
    # Use List to manage array of pointers
    var c_names_list = List[UnsafePointer[c_char, ImmutAnyOrigin]]()
    c_names_list.reserve(count)
    
    # Use UnsafePointer to iterate without copying strings
    var base_ptr = names.unsafe_ptr()
    
    for i in range(count):
        var s = (base_ptr + i)[].as_c_string_slice()
        c_names_list.append(s.unsafe_ptr())
        
    # Get pointer to the array of pointers
    return LogicalType[True, MutExternalOrigin](
        libduckdb.duckdb_create_enum_type(c_names_list.unsafe_ptr(), UInt64(count))
    )

