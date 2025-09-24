from duckdb._libduckdb import *
from duckdb.logical_type import *
from duckdb.duckdb_value import *
from collections import Optional

from sys.intrinsics import _type_is_eq


struct Vector[is_mutable: Bool, //, origin: Origin[is_mutable]]:
    var _chunk: Pointer[Chunk, origin] # keep a reference to the originating chunk to avoid premature destruction
    var _vector: duckdb_vector
    var length: UInt64

    fn __init__(
        out self,
        chunk: Pointer[Chunk, origin],
        vector: duckdb_vector,
        length: UInt64,
    ):
        self._vector = vector
        self.length = length
        self._chunk = chunk

    fn get_column_type(self) -> LogicalType:
        return LogicalType(duckdb_vector_get_column_type(self._vector))

    fn _get_data(self) -> UnsafePointer[NoneType]:
        return duckdb_vector_get_data(self._vector)

    fn _get_validity_mask(self) -> UnsafePointer[UInt64]:
        return duckdb_vector_get_validity(self._vector)

    fn list_get_child(self) -> Vector[origin]:
        """Retrieves the child vector of a list vector.

        The resulting vector is valid as long as the parent vector is valid.

        * vector: The vector
        * returns: The child vector
        """
        return Vector(
            self._chunk,
            duckdb_list_vector_get_child(self._vector),
            duckdb_list_vector_get_size(self._vector),
        )

    fn list_get_size(self) -> idx_t:
        """Returns the size of the child vector of the list.

        * vector: The vector
        * returns: The size of the child list
        """
        return duckdb_list_vector_get_size(self._vector)

    fn _check_type(self, db_type: LogicalType) raises:
        """Recursively check that the runtime type of the vector matches the expected type.
        """
        if self.get_column_type() != db_type:
            raise "Expected type " + String(db_type) + " but got " + String(
                self.get_column_type()
            )

    fn get[
        T: Copyable & Movable, //
    ](self, expected_type: Col[T]) raises -> List[Optional[T]]:
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
            self, length=Int(self.length), offset=0
        )
        # The way we are building our Mojo representation of the data currently via the DuckDBValue
        # trait, with different __init__ implementations depending on the concrete type, means
        # that the types don't match.
        #
        # We can cast the result to the expected type though because
        # 1. We have ensured that the runtime type matches the expected type through _check_type
        # 2. The DuckDBValue implementations are all thin wrappers with conversion logic
        # around the underlying type we're converting into.
        return rebind[List[Optional[T]]](result)
        
