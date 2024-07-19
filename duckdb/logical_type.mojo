from duckdb.api import _impl


struct LogicalType:
    var _logical_type: duckdb_logical_type

    fn __init__(inout self, logical_type: duckdb_logical_type):
        self._logical_type = logical_type

    fn __del__(owned self):
        _impl().duckdb_destroy_logical_type(
            UnsafePointer.address_of(self._logical_type)
        )

    fn get_type_id(self) -> DuckDBType:
        """Retrieves the enum type class of a `duckdb_logical_type`.

        * type: The logical type object
        * returns: The type id
        """
        return _impl().duckdb_get_type_id(self._logical_type)

    fn list_type_child_type(self) -> LogicalType:
        """Retrieves the child type of the given list type.

        * type: The logical type object
        """
        return LogicalType(
            _impl().duckdb_list_type_child_type(self._logical_type)
        )
