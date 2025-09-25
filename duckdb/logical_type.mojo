from duckdb._libduckdb import *


struct LogicalType(ImplicitlyCopyable & Movable & EqualityComparable & Writable & Stringable):
    """Represents a potentially nested DuckDB type."""

    var _logical_type: duckdb_logical_type

    fn __init__(out self, type_id: DuckDBType):
        """Creates a `LogicalType` from a standard primitive type."""
        ref libduckdb = DuckDB().libduckdb()
        self._logical_type = libduckdb.duckdb_create_logical_type(type_id.value)

    fn __init__(out self, logical_type: duckdb_logical_type):
        self._logical_type = logical_type

    fn __copyinit__(out self, other: Self):
        if other.get_type_id() == DuckDBType.list:
            var child = other.list_type_child_type()
            self = child.create_list_type()
        # TODO remaining nested types
        # if not other.get_type_id().is_nested():
        else:
            self = Self(other.get_type_id())

    fn __moveinit__(out self, deinit other: Self):
        self._logical_type = other._logical_type

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_logical_type(
            UnsafePointer(to=self._logical_type)
        )

    fn create_list_type(self) -> Self:
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_create_list_type(self._logical_type))

    fn get_type_id(self) -> DuckDBType:
        """Retrieves the enum type class of this `LogicalType`.

        * type: The logical type object
        * returns: The type id
        """
        ref libduckdb = DuckDB().libduckdb()
        return DuckDBType(libduckdb.duckdb_get_type_id(self._logical_type))

    fn list_type_child_type(self) -> Self:
        """Retrieves the child type of the given list type.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_list_type_child_type(self._logical_type))

    fn map_type_key_type(self) -> Self:
        """Retrieves the key type of the given map type.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_map_type_key_type(self._logical_type))

    fn map_type_value_type(self) -> Self:
        """Retrieves the value type of the given map type.

        * type: The logical type object
        """
        ref libduckdb = DuckDB().libduckdb()
        return Self(libduckdb.duckdb_map_type_value_type(self._logical_type))

    fn __eq__(self, other: Self) -> Bool:
        if self.get_type_id() != other.get_type_id():
            return False
        if self.get_type_id() == DuckDBType.list:
            return self.list_type_child_type() == other.list_type_child_type()
        if self.get_type_id() == DuckDBType.map:
            return (
                self.map_type_key_type() == other.map_type_key_type()
                and self.map_type_value_type() == other.map_type_value_type()
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
