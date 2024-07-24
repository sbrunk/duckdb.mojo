from duckdb._c_api.libduckdb import _impl
from duckdb._c_api.c_api import *
from duckdb.logical_type import *
from duckdb.duckdb_value import *

from sys.intrinsics import _type_is_eq

struct Vector[lifetime: ImmutableLifetime]:
    var _vector: duckdb_vector
    var _chunk: Reference[Chunk, lifetime]
    var length: UInt64

    fn __init__(
        inout self,
        vector: duckdb_vector,
        chunk: Reference[Chunk, lifetime],
        length: UInt64,
    ):
        self._vector = vector
        self._chunk = chunk
        self.length = length

    fn get_column_type(self) -> LogicalType:
        return _impl().duckdb_vector_get_column_type(self._vector)

    fn _get_data(self) -> UnsafePointer[NoneType]:
        return _impl().duckdb_vector_get_data(self._vector)

    fn _get_validity_mask(self) -> UnsafePointer[UInt64]:
        return _impl().duckdb_vector_get_validity(self._vector)

    fn list_get_child(self) -> Vector[lifetime]:
        """Retrieves the child vector of a list vector.

        The resulting vector is valid as long as the parent vector is valid.

        * vector: The vector
        * returns: The child vector
        """
        return Vector(
            _impl().duckdb_list_vector_get_child(self._vector),
            self._chunk,
            _impl().duckdb_list_vector_get_size(self._vector),
        )

    fn list_get_size(self) -> idx_t:
        """Returns the size of the child vector of the list.

        * vector: The vector
        * returns: The size of the child list
        """
        return _impl().duckdb_list_vector_get_size(self._vector)

    fn _check_type(self, db_type: DBType) raises:
        """Recursively check that the runtime type of the vector matches the expected type."""
        if db_type.isa[DBPrimitiveType]():
            if self.get_column_type().get_type_id() != get_duckdb_type(db_type):
                raise "Expected type " + str(get_duckdb_type(db_type)) + " but got " + str(self.get_column_type().get_type_id())
        if self.get_column_type().get_type_id() == DuckDBType.list:
            self.list_get_child()._check_type(db_type[DBListType].child())
        # TODO check remaining nested types
        # elif vector.get_column_type().get_type_id() == DuckDBType.map:

    fn get[T: CollectionElement, //](self, expected_type: Col[T]) raises -> List[Optional[T]]:
        """Convert the data from this vector into native Mojo data structures."""

        self._check_type(expected_type.logical_type)

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
        var result = ListVal[expected_type.Builder](self, length=int(self.length), offset=0).value
        # The way we are building our Mojo representation of the data currently via the DBVal
        # trait, with different __init__ implementations depending on the concrete type, means
        # that the types don't match.
        #
        # We can cast the result to the expected type though because
        # 1. We have ensured that the runtime type matches the expected type through _check_type
        # 2. The DBVal implementations are all thin wrappers with conversion logic
        # around the underlying type we're converting into.
        var converted_result = UnsafePointer.address_of(result).bitcast[List[Optional[T]]]()[]
        _ = result
        return converted_result

    fn get_fixed_size_values[
        T: CollectionElement
    ](self, length: Int, offset: Int) raises -> List[Optional[T]]:
        var data_ptr = self._get_data().bitcast[T]()
        var values = List[Optional[T]](capacity=int(length))
        var validity_mask = self._get_validity_mask()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            for row in range(length):
                if validity_mask[row]:
                    values.append(Optional(data_ptr[row + offset]))
            return values
        for row in range(length):
            var entry_idx = row // 64
            var idx_in_entry = row % 64
            var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
            if is_valid:
                values.append(Optional(data_ptr[row + offset]))
            else:
                values.append(None)
        return values
