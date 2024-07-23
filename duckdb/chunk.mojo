from duckdb._c_api.c_api import *
from duckdb._c_api.libduckdb import _impl
from duckdb.vector import Vector
from duckdb.duckdb_type import *

struct Chunk:
    """Represents a DuckDB data chunk."""

    var _chunk: duckdb_data_chunk

    fn __init__(inout self, chunk: duckdb_data_chunk):
        self._chunk = chunk

    fn __del__(owned self):
        _impl().duckdb_destroy_data_chunk(UnsafePointer.address_of(self._chunk))

    fn __moveinit__(inout self, owned existing: Self):
        self._chunk = existing._chunk

    fn __len__(self) -> Int:
        return int(_impl().duckdb_data_chunk_get_size(self._chunk))

    fn _get_vector(self, col: Int) -> Vector[__lifetime_of(self)]:
        return Vector(
            _impl().duckdb_data_chunk_get_vector(self._chunk, col),
            self,
            length=len(self),
        )

    @always_inline
    fn _check_bounds(self, col: Int) raises:
        if UInt64(col) >= _impl().duckdb_data_chunk_get_column_count(
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
        return self._get_vector(col).get_column_type().get_type_id()

    @always_inline
    fn _check_type(self, col: Int, expected: DuckDBType) raises:
        var type = self.type(col)
        if type != expected:
            raise Error(
                String("Column {} has type {}. Expected {}.").format(
                    col,
                    type,
                    expected,
                )
            )

    @always_inline
    fn _validate(self, col: Int, row: Int, expected_type: DuckDBType) raises:
        self._check_bounds(col, row)
        self._check_type(col, expected_type)

    @always_inline
    fn _get_value[
        T: StringableCollectionElement
    ](self, col: Int, row: Int, expected_type: DuckDBType) raises -> Optional[
        T
    ]:
        self._validate(col, row, expected_type)
        if self.is_null(col=col, row=row):
            return Optional[T](None)
        var data_ptr = self._get_vector(col)._get_data().bitcast[T]()
        return Optional[T](data_ptr[row])

    @always_inline
    fn _get_values[
        T: StringableCollectionElement
    ](self, col: Int, expected_type: DuckDBType) raises -> List[T]:
        self._check_type(col, expected_type)
        self._check_bounds(col)
        var vector = self._get_vector(col)
        var data_ptr = vector._get_data().bitcast[T]()
        var size = len(self)
        # we need a copy here as closing the chunk will free the original data
        var list_buffer = UnsafePointer[T].alloc(size)
        memcpy(dest=list_buffer, src=data_ptr, count=size)
        return List(unsafe_pointer=list_buffer, size=size, capacity=size)

    fn is_null(self, *, col: Int) -> Bool:
        """Check if all values at the given and column are NULL."""
        var vector = self._get_vector(col)
        var validity_mask = vector._get_validity_mask()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        # TODO check validity mask doesn't contain any 0 bits
        return False

    fn is_null(self, *, col: Int, row: Int) -> Bool:
        """Check if the value at the given row and column is NULL."""
        var vector = self._get_vector(col)
        var validity_mask = vector._get_validity_mask()
        if (
            not validity_mask
        ):  # validity mask can be null if there are no NULL values
            return False
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
        return not is_valid

    fn get[T: DBVal](self, *, col: Int, row: Int) raises -> Optional[T]:
        self._check_bounds(col, row)
        if self.is_null(col=col, row=row):
            return NoneType()
        return self._get_vector(col).get_value[T](offset=0)

    fn get[T: DBVal](self, col: Int) raises -> List[Optional[T]]:
        self._check_bounds(col)
        if self.is_null(col=col):
            return List[Optional[T]](NoneType())
        return self._get_vector(col).get_values[T]()

    fn get[type: DType](self, col: Int) raises -> List[Scalar[type]]:
        return self._get_values[Scalar[type]](
            col, expected_type=DuckDBType.from_dtype[type]()
        )

    fn get[
        type: DType
    ](self, col: Int, row: Int) raises -> Optional[Scalar[type]]:
        return self._get_value[Scalar[type]](
            col, row, expected_type=DuckDBType.from_dtype[type]()
        )

    fn get[T: __type_of(String)](self, col: Int) raises -> List[String]:
        self._check_bounds(col)
        self._check_type(col, DuckDBType.varchar)
        var string_data_ptr = self._get_vector(col)._get_data().bitcast[
            duckdb_string_t_pointer
        ]()
        var strings = List[String](capacity=len(self))
        for row in range(len(self)):
            strings.append(
                self._get_vector(col)._get_string(row, string_data_ptr)
            )
        return strings

    fn get[
        T: __type_of(String)
    ](self, col: Int, row: Int) raises -> Optional[String]:
        self._validate(col, row, DuckDBType.varchar)
        var vector = self._get_vector(col)
        var validity_mask = vector._get_validity_mask()
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
        if is_valid:
            var string_data_ptr = vector._get_data().bitcast[
                duckdb_string_t_pointer
            ]()
            return Optional(
                self._get_vector(col)._get_string(row, string_data_ptr)
            )
        return Optional[String](None)

    fn get[
        T: __type_of(Timestamp)
    ](self, col: Int, row: Int) raises -> Optional[Timestamp]:
        return self._get_value[Timestamp](col, row, DuckDBType.timestamp)

    fn get[T: __type_of(Timestamp)](self, col: Int) raises -> List[Timestamp]:
        return self._get_values[Timestamp](col, DuckDBType.timestamp)

    fn get[
        T: __type_of(Date)
    ](self, col: Int, row: Int) raises -> Optional[Date]:
        return self._get_value[Date](col, row, DuckDBType.date)

    fn get[T: __type_of(Date)](self, col: Int) raises -> List[Date]:
        return self._get_values[Date](col, DuckDBType.date)

    fn get[
        T: __type_of(Time)
    ](self, col: Int, row: Int) raises -> Optional[Time]:
        return self._get_value[Time](col, row, DuckDBType.time)

    fn get[T: __type_of(Time)](self, col: Int) raises -> List[Time]:
        return self._get_values[Time](col, DuckDBType.time)

    fn get[
        T: __type_of(Interval)
    ](self, col: Int, row: Int) raises -> Optional[Interval]:
        return self._get_value[Interval](col, row, DuckDBType.interval)

    fn get[T: __type_of(Interval)](self, col: Int) raises -> List[Interval]:
        return self._get_values[Interval](col, DuckDBType.interval)

    fn get[
        T: __type_of(Int128)
    ](self, col: Int, row: Int) raises -> Optional[Int128]:
        return self._get_value[Int128](col, row, DuckDBType.hugeint)

    fn get[T: __type_of(Int128)](self, col: Int) raises -> List[Int128]:
        return self._get_values[Int128](col, DuckDBType.hugeint)

    fn get[
        T: __type_of(UInt128)
    ](self, col: Int, row: Int) raises -> Optional[UInt128]:
        return self._get_value[UInt128](col, row, DuckDBType.uhugeint)

    fn get[T: __type_of(UInt128)](self, col: Int) raises -> List[UInt128]:
        return self._get_values[UInt128](col, DuckDBType.uhugeint)

    # TODO remaining types


struct _ChunkIter[lifetime: ImmutableLifetime]:
    var _result: Reference[Result, lifetime]
    var _next_chunk: duckdb_data_chunk

    fn __init__(inout self, ref [lifetime]result: Result) raises:
        self._result = result
        self._next_chunk = _impl().duckdb_fetch_chunk(self._result[]._result)

    fn __del__(owned self):
        if self._next_chunk:
            _ = Chunk(self._next_chunk)

    fn __moveinit__(inout self, owned existing: Self):
        self._result = existing._result
        self._next_chunk = existing._next_chunk

    fn __iter__(owned self) -> Self:
        return self^

    fn __next__(inout self) raises -> Chunk:
        if self._next_chunk:
            var current = self._next_chunk
            var next = _impl().duckdb_fetch_chunk(self._result[]._result)
            self._next_chunk = next
            return Chunk(current)
        else:
            raise Error("No more elements")

    # TODO this is not accurate as we don't know the length in advance but we currently
    # need it for the for syntax to work. It's done the same way for iterating over Python
    # objects in the Mojo stdlib currently:
    # https://github.com/modularml/mojo/blob/8bd1dbdf26c70c634768bfd4c014537f6fdb0fb2/stdlib/src/python/object.mojo#L90
    fn __len__(self) -> Int:
        if self._next_chunk:
            return 1
        else:
            return 0

# struct ResultIterator:
#     var result: Result
#     var index: Int

#     fn __init__(inout self, result: Result):
#         self.index = 0
#         self.result = result

#     # fn __index__(self) -> UInt64:
#     #     return self.index

#     # fn __len__(self) -> Int:
#     #     return int(self.result.rows - self.index)  # TODO could overflow

#     fn __next__(inout self) -> String:
#         self.index += 1
#         return str(self.index)
