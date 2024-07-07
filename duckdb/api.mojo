from duckdb._libduckdb import *
from sys.ffi import _get_global

alias Date = duckdb_date
"""Days are stored as days since 1970-01-01"""
alias Time = duckdb_time
"""Time is stored as microseconds since 00:00:00"""
alias Timestamp = duckdb_timestamp
"""Timestamps are stored as microseconds since 1970-01-01"""
alias Interval = duckdb_interval
alias Int128 = duckdb_hugeint
alias UInt128 = duckdb_uhugeint

fn _init_global(ignored: UnsafePointer[NoneType]) -> UnsafePointer[NoneType]:
    var ptr = UnsafePointer[LibDuckDB].alloc(1)
    ptr[] = LibDuckDB()
    return ptr.bitcast[NoneType]()


fn _destroy_global(duckdb: UnsafePointer[NoneType]):
    # var p = duckdb.bitcast[LibDuckDB]()
    # LibDuckDB.destroy(p[])
    duckdb.free()


@always_inline
fn _get_global_duckdb_itf() -> _DuckDBInterfaceImpl:
    var ptr = _get_global["DuckDB", _init_global, _destroy_global]()
    return ptr.bitcast[LibDuckDB]()


struct _DuckDBInterfaceImpl:
    var _libDuckDB: UnsafePointer[LibDuckDB]

    fn __init__(inout self, LibDuckDB: UnsafePointer[LibDuckDB]):
        self._libDuckDB = LibDuckDB

    fn __copyinit__(inout self, existing: Self):
        self._libDuckDB = existing._libDuckDB

    fn libDuckDB(self) -> LibDuckDB:
        return self._libDuckDB[]

fn _impl() -> LibDuckDB:
    return _get_global_duckdb_itf().libDuckDB()

struct DuckDB:
    @staticmethod
    fn connect(db_path: String) raises -> Connection:
        return Connection(db_path)


# TODO separate opening and connecting but add convenient functions to keep it simple
struct Connection:
    """A connection to a DuckDB database.

    Example:
    ```mojo
    from duckdb import DuckDB
    var con = DuckDB.connect(":memory:")
    var result = con.execute("SELECT lst, lst || 'duckdb' FROM range(10) tbl(lst)")
    ```
    """

    var _db: duckdb_database
    var __conn: duckdb_connection

    fn __init__(inout self, db_path: String) raises:
        self._db = UnsafePointer[duckdb_database.type]()
        var db_addr = UnsafePointer.address_of(self._db)
        if (
            _impl().duckdb_open(db_path.unsafe_cstr_ptr(), db_addr)
        ) == DuckDBError:
            raise Error(
                "Could not open database"
            )  ## TODO use duckdb_open_ext and return error message
        self.__conn = UnsafePointer[duckdb_connection.type]()
        if (
            _impl().duckdb_connect(
                self._db, UnsafePointer.address_of(self.__conn)
            )
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(owned self):
        _impl().duckdb_disconnect(UnsafePointer.address_of(self.__conn))
        _impl().duckdb_close(UnsafePointer.address_of(self._db))

    fn execute(self, query: String) raises -> Result:
        var result = duckdb_result()
        var result_ptr = UnsafePointer.address_of(result)
        if (
            _impl().duckdb_query(self.__conn, query.unsafe_cstr_ptr(), result_ptr)
            == DuckDBError
        ):
            raise Error(_impl().duckdb_result_error(result_ptr))
        return Result(result)


@value
struct Col:
    var index: Int
    var name: String
    var type: DuckDBType

    fn format_to(self, inout writer: Formatter) -> None:
        writer.write("Column(", self.index, ", ", self.name, ": ", self.type, ")")

    fn __str__(self) -> String: return str(self.type)

struct Result(Stringable, Formattable):
    var _result: duckdb_result
    var columns: List[Col]

    fn __init__(inout self, result: duckdb_result):
        self._result = result
        self.columns = List[Col]()
        for i in range(self.column_count()):
            var col = Col(
                index=i, name=self.column_name(i), type=self.column_type(i)
            )
            self.columns.append(col)

    fn column_count(self) -> Int:
        return int(
            _impl().duckdb_column_count(
                UnsafePointer.address_of(self._result)
            )
        )

    fn column_name(self, col: Int) -> String:
        return _impl().duckdb_column_name(
            UnsafePointer.address_of(self._result), col
        )

    fn column_types(self) -> List[DuckDBType]:
        var types = List[DuckDBType]()
        for i in range(self.column_count()):
            types.append(self.column_type(i))
        return types

    fn column_type(self, col: Int) -> DuckDBType:
        return int(
            _impl().duckdb_column_type(
                UnsafePointer.address_of(self._result), col
            )
        )

    fn format_to(self, inout writer: Formatter) -> None:
        for col in self.columns:
            writer.write(col[], ", ")

    fn __str__(self) -> String:
        return String.format_sequence(self)

    # fn __iter__(self) -> ResultIterator:
    #     return ResultIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        return Chunk(_impl().duckdb_fetch_chunk(self._result))

    fn chunk_iterator(self) raises -> _ChunkIter[__lifetime_of(self)]:
        return _ChunkIter(self)

    fn __del__(owned self):
        _impl().duckdb_destroy_result(UnsafePointer.address_of(self._result))

    fn __moveinit__(inout self, owned existing: Self):
        self._result = existing._result
        self.columns = existing.columns

struct _ChunkIter[lifetime: ImmutableLifetime]:
    var _result: Reference[Result, lifetime]
    var _next_chunk: duckdb_data_chunk

    fn __init__(inout self, ref [lifetime] result: Result) raises:
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

struct Chunk:
    var _chunk: duckdb_data_chunk

    fn __init__(inout self, chunk: duckdb_data_chunk):
        self._chunk = chunk

    fn __del__(owned self):
        _impl().duckdb_destroy_data_chunk(
            UnsafePointer.address_of(self._chunk)
        )

    fn __moveinit__(inout self, owned existing: Self):
        self._chunk = existing._chunk

    fn __len__(self) -> Int:
        return int(_impl().duckdb_data_chunk_get_size(self._chunk))

    fn _get_vector(self, col: Int) -> Vector:
        return Vector(_impl().duckdb_data_chunk_get_vector(self._chunk, col))

    @always_inline
    fn _check_bounds(self, col: Int) raises:
        if UInt64(col) >= _impl().duckdb_data_chunk_get_column_count(self._chunk):
            raise Error(String("Column {} out of bounds.").format(col)) 

    @always_inline
    fn _check_bounds(self, col: Int, row: Int) raises:
        if row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))
        self._check_bounds(col)

    @always_inline
    fn _check_type(self, col: Int, expected: DuckDBType) raises:
        var type = DuckDBType(_impl().duckdb_get_type_id(
            _impl().duckdb_vector_get_column_type(
                _impl().duckdb_data_chunk_get_vector(self._chunk, col)
            ))
        )
        if type != expected:
            raise Error(
                String("Column {} has type {}. Expected {}.").format(
                    col,
                    type,
                    expected,
                )
            )

    @always_inline
    fn _validate(
        self, col: Int, row: Int, expected_type: DuckDBType
    ) raises:
        self._check_bounds(col, row)
        self._check_type(col, expected_type)

    @always_inline
    fn _get_value[
        T: CollectionElement
    ](self, col: Int, row: Int, expected_type: DuckDBType) raises -> Optional[T]:
        self._validate(col, row, expected_type)
        var vector = self._get_vector(col)
        var validity_mask = _impl().duckdb_vector_get_validity(vector._vector)
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
        if is_valid:
            var data_ptr = vector._get_data().bitcast[T]()
            return Optional[T](data_ptr[row])
        return Optional[T](None)

    @always_inline
    fn _get_values[
        T: CollectionElement
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

    fn get_bool(self, col: Int, row: Int) raises -> Optional[Bool]:
        return self._get_value[Bool](col, row, DuckDBType.boolean)

    fn get_bool(self, col: Int) raises -> List[Bool]:
        return self._get_values[Bool](col, DuckDBType.boolean)

    fn get_int8(self, col: Int, row: Int) raises -> Optional[Int8]:
        return self._get_value[Int8](col, row, DuckDBType.tinyint)

    fn get_int8(self, col: Int) raises -> List[Int8]:
        return self._get_values[Int8](col, DuckDBType.tinyint)

    fn get_int16(self, col: Int, row: Int) raises -> Optional[Int16]:
        return self._get_value[Int16](col, row, DuckDBType.smallint)

    fn get_int16(self, col: Int) raises -> List[Int16]:
        return self._get_values[Int16](col, DuckDBType.smallint)

    fn get_int32(self, col: Int, row: Int) raises -> Optional[Int32]:
        return self._get_value[Int32](col, row, DuckDBType.integer)

    fn get_int32(self, col: Int) raises -> List[Int32]:
        return self._get_values[Int32](col, DuckDBType.integer)

    fn get_int64(self, col: Int, row: Int) raises -> Optional[Int64]:
        return self._get_value[Int64](col, row, DuckDBType.bigint)

    fn get_int64(self, col: Int) raises -> List[Int64]:
        return self._get_values[Int64](col, DuckDBType.bigint)

    fn get_uint8(self, col: Int, row: Int) raises -> Optional[UInt8]:
        return self._get_value[UInt8](col, row, DuckDBType.utinyint)

    fn get_uint8(self, col: Int) raises -> List[UInt8]:
        return self._get_values[UInt8](col, DuckDBType.utinyint)

    fn get_uint16(self, col: Int, row: Int) raises -> Optional[UInt16]:
        return self._get_value[UInt16](col, row, DuckDBType.usmallint)

    fn get_uint16(self, col: Int) raises -> List[UInt16]:
        return self._get_values[UInt16](col, DuckDBType.usmallint)

    fn get_uint32(self, col: Int, row: Int) raises -> Optional[UInt32]:
        return self._get_value[UInt32](col, row, DuckDBType.uinteger)

    fn get_uint32(self, col: Int) raises -> List[UInt32]:
        return self._get_values[UInt32](col, DuckDBType.uinteger)

    fn get_uint64(self, col: Int, row: Int) raises -> Optional[UInt64]:
        return self._get_value[UInt64](col, row, DuckDBType.ubigint)

    fn get_uint64(self, col: Int) raises -> List[UInt64]:
        return self._get_values[UInt64](col, DuckDBType.ubigint)

    fn get_float32(self, col: Int, row: Int) raises -> Optional[Float32]:
        return self._get_value[Float32](col, row, DuckDBType.float)

    fn get_float32(self, col: Int) raises -> List[Float32]:
        return self._get_values[Float32](col, DuckDBType.float)

    fn get_float64(self, col: Int, row: Int) raises -> Optional[Float64]:
        return self._get_value[Float64](col, row, DuckDBType.double)

    fn get_float64(self, col: Int) raises -> List[Float64]:
        return self._get_values[Float64](col, DuckDBType.double)

    fn get_timestamp(self, col: Int, row: Int) raises -> Optional[Timestamp]:
        return self._get_value[Timestamp](col, row, DuckDBType.timestamp)

    fn get_timestamp(self, col: Int) raises -> List[Timestamp]:
        return self._get_values[Timestamp](col, DuckDBType.timestamp)

    fn get_date(self, col: Int, row: Int) raises -> Optional[Date]:
        return self._get_value[Date](col, row, DuckDBType.date)

    fn get_date(self, col: Int) raises -> List[Date]:
        return self._get_values[Date](col, DuckDBType.date)

    fn get_time(self, col: Int, row: Int) raises -> Optional[Time]:
        return self._get_value[Time](col, row, DuckDBType.time)

    fn get_time(self, col: Int) raises -> List[Time]:
        return self._get_values[Time](col, DuckDBType.time)

    fn get_interval(self, col: Int, row: Int) raises -> Optional[Interval]:
        return self._get_value[Interval](col, row, DuckDBType.interval)

    fn get_interval(self, col: Int) raises -> List[Interval]:
        return self._get_values[Interval](col, DuckDBType.interval)

    fn get_int128(self, col: Int, row: Int) raises -> Optional[Int128]:
        return self._get_value[Int128](col, row, DuckDBType.hugeint)

    fn get_int128(self, col: Int) raises -> List[Int128]:
        return self._get_values[Int128](col, DuckDBType.hugeint)

    fn get_uint128(self, col: Int, row: Int) raises -> Optional[UInt128]:
        return self._get_value[UInt128](col, row, DuckDBType.uhugeint)

    fn get_uint128(self, col: Int) raises -> List[UInt128]:
        return self._get_values[UInt128](col, DuckDBType.uhugeint)

    @always_inline
    fn _get_string(self, row: Int, data_str_ptr: UnsafePointer[duckdb_string_t_pointer]) raises -> String:
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = int(data_str_ptr[row].length)
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[row].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[duckdb_string_t_inlined]()
            return StringRef(data_str_inlined[row].inlined.unsafe_ptr(), string_length)
        else:
            return StringRef(data_str_ptr[row].ptr, string_length)

    fn get_string(self, col: Int, row: Int) raises -> Optional[String]:
        self._validate(col, row, DuckDBType.varchar)
        var vector = self._get_vector(col)
        var validity_mask = _impl().duckdb_vector_get_validity(vector._vector)
        var entry_idx = row // 64
        var idx_in_entry = row % 64
        var is_valid = validity_mask[entry_idx] & (1 << idx_in_entry)
        if is_valid:
            var string_data_ptr = vector._get_data().bitcast[duckdb_string_t_pointer]()
            return Optional(self._get_string(row, string_data_ptr))
        return Optional[String](None)

    fn get_string(self, col: Int) raises -> List[String]:
        self._check_bounds(col)
        self._check_type(col, DuckDBType.varchar)
        var string_data_ptr = self._get_vector(col)._get_data().bitcast[duckdb_string_t_pointer]()
        var strings = List[String](capacity=len(self))
        for row in range(len(self)):
            strings.append(self._get_string(row, string_data_ptr))
        return strings

    # TODO remaining types


@value
struct Vector:
    var _vector: duckdb_vector

    fn _get_data(self) -> UnsafePointer[NoneType]:
        return _impl().duckdb_vector_get_data(self._vector)


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
