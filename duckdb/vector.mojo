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

    fn get_values[T: DBVal](self) raises -> List[Optional[T]]:
        var type = self.get_column_type().get_type_id()
        if T.type() != type:
            raise "Expected type " + str(T.type()) + " but got " + str(type)

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

        # columns are essentially lists so we can use the same logic for getting the values
        return ListVal[T](self, length=int(self.length), offset=0).value

    fn get_value[T: DBVal](self, offset: Int) -> T:
        var data_ptr = self._get_data().bitcast[T]()
        return data_ptr[offset]

    fn get_fixed_size_values[
        T: DBVal
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

    @always_inline
    fn _get_string(
        self, row: Int, data_str_ptr: UnsafePointer[duckdb_string_t_pointer]
    ) raises -> String:
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = int(data_str_ptr[row].length)
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[row].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[
                duckdb_string_t_inlined
            ]()
            return StringRef(
                data_str_inlined[row].inlined.unsafe_ptr(), string_length
            )
        else:
            return StringRef(data_str_ptr[row].ptr, string_length)
