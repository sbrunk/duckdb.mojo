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


struct DuckDB:
    var impl: _DuckDBInterfaceImpl

    def __init__(inout self):
        self.impl = _get_global_duckdb_itf()

    @staticmethod
    def connect(db_path: String) -> Connection:
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

    var __db: duckdb_database
    var __conn: duckdb_connection

    fn __init__(inout self, db_path: String) raises:
        var impl = _get_global_duckdb_itf().libDuckDB()
        self.__db = UnsafePointer[duckdb_database.type]()
        var db_addr = UnsafePointer.address_of(self.__db)
        if (
            impl.duckdb_open(db_path.unsafe_cstr_ptr(), db_addr)
        ) == DuckDBError:
            raise Error(
                "Could not open database"
            )  ## TODO use duckdb_open_ext and return error message
        self.__conn = UnsafePointer[duckdb_connection.type]()
        if (
            impl.duckdb_connect(
                self.__db, UnsafePointer.address_of(self.__conn)
            )
        ) == DuckDBError:
            raise Error("Could not connect to database")

    fn __del__(owned self):
        var impl = _get_global_duckdb_itf().libDuckDB()
        impl.duckdb_disconnect(UnsafePointer.address_of(self.__conn))
        impl.duckdb_close(UnsafePointer.address_of(self.__db))

    fn execute(self, query: String) raises -> ResultSet:
        var impl = _get_global_duckdb_itf().libDuckDB()
        var result = duckdb_result()
        var result_ptr = UnsafePointer.address_of(result)
        if (
            impl.duckdb_query(self.__conn, query.unsafe_cstr_ptr(), result_ptr)
            == DuckDBError
        ):
            raise Error(impl.duckdb_result_error(result_ptr))
        return ResultSet(result)


struct ResultSet(Stringable):
    var __result: duckdb_result
    var impl: LibDuckDB
    # var rows: UInt64
    # var columns: UInt64

    fn __init__(inout self, result: duckdb_result):
        self.__result = result
        self.impl = _get_global_duckdb_itf().libDuckDB()
        # self.rows = self.__lib.duckdb_row_count(UnsafePointer.address_of(self.__result))
        # self.columns = duckdb_column_count(
        #     UnsafePointer.address_of(self.__result)
        # )

    # def get_string(
    #     self,
    #     col: UInt64,
    #     row: UInt64,
    # ) -> String:
    #     return String(
    #         self.__lib.duckdb_value_varchar(
    #             UnsafePointer.address_of(self.__result), col, row
    #         )
    #     )

    # fn __len__(self) -> UInt64:
    #     return self.rows

    fn column_count(self) -> Int:
        return int(
            self.impl.duckdb_column_count(
                UnsafePointer.address_of(self.__result)
            )
        )

    fn column_name(self, col: UInt64) -> String:
        return self.impl.duckdb_column_name(
            UnsafePointer.address_of(self.__result), col
        )

    fn column_types(self) -> List[Int]:
        var types = List[Int]()
        for i in range(self.column_count()):
            types.append(self.column_type(i))
        return types

    fn column_type(self, col: Int) -> Int:
        return int(
            self.impl.duckdb_column_type(
                UnsafePointer.address_of(self.__result), col
            )
        )

    fn __str__(self) -> String:
        var x: String
        try:
            x = String("Result set with {} columns\n").format(
                self.column_count()
            )
        except e:
            x = str(e)
        for i in range(self.column_count()):
            x = (
                x
                + self.column_name(i)
                + " "
                + type_names().get(self.column_type(i), "UNKNOWN")
                + ", "
            )
        return x

    # fn __iter__(self) -> ResultSetIterator:
    #     return ResultSetIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        return Chunk(self.impl.duckdb_fetch_chunk(self.__result), self)

    fn __del__(owned self):
        self.impl.duckdb_destroy_result(UnsafePointer.address_of(self.__result))

    fn __moveinit__(inout self, owned existing: Self):
        self.__result = existing.__result
        self.impl = existing.impl


@value
struct Chunk:
    var impl: LibDuckDB
    var __chunk: duckdb_data_chunk

    var column_count: Int
    var column_types: List[Int]

    def __init__(inout self, chunk: duckdb_data_chunk, result: ResultSet):
        self.impl = _get_global_duckdb_itf().libDuckDB()
        self.__chunk = chunk
        self.column_count = result.column_count()
        self.column_types = result.column_types()

    fn __del__(owned self):
        self.impl.duckdb_destroy_data_chunk(
            UnsafePointer.address_of(self.__chunk)
        )

    fn __len__(self) -> Int:
        return int(self.impl.duckdb_data_chunk_get_size(self.__chunk))

    fn __get_vector(self, col: UInt64) -> Vector:
        return Vector(self.impl.duckdb_data_chunk_get_vector(self.__chunk, col))

    fn _check_bounds(self, col: Int, row: Int) raises -> NoneType:
        if row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))
        if col >= self.column_count:
            raise Error(String("Column {} out of bounds.").format(col))

    fn _check_type(self, col: Int, expected: Int) raises -> NoneType:
        if self.column_types[col] != expected:
            raise Error(
                String("Column {} has type {}. Expected {}.").format(
                    col,
                    type_names().get(self.column_types[col], "UNKNOWN"),
                    type_names().get(expected, "UNKNOWN"),
                )
            )

    fn _validate(
        self, col: Int, row: Int, expected_type: Int
    ) raises -> NoneType:
        self._check_bounds(col, row)
        self._check_type(col, expected_type)

    fn _get_value[
        T: Copyable
    ](self, col: Int, row: Int, duckdb_type: Int) raises -> T:
        self._validate(col, row, duckdb_type)
        var vector = self.__get_vector(col)
        var data_ptr = vector.__get_data().bitcast[T]()
        return data_ptr[]

    fn get_bool(self, col: Int, row: Int) raises -> Bool:
        return self._get_value[Bool](col, row, DUCKDB_TYPE_BOOLEAN)

    fn get_int8(self, col: Int, row: Int) raises -> Int8:
        return self._get_value[Int8](col, row, DUCKDB_TYPE_TINYINT)

    fn get_int16(self, col: Int, row: Int) raises -> Int16:
        return self._get_value[Int16](col, row, DUCKDB_TYPE_SMALLINT)

    fn get_int32(self, col: Int, row: Int) raises -> Int32:
        return self._get_value[Int32](col, row, DUCKDB_TYPE_INTEGER)

    fn get_int64(self, col: Int, row: Int) raises -> Int64:
        return self._get_value[Int64](col, row, DUCKDB_TYPE_BIGINT)

    fn get_uint8(self, col: Int, row: Int) raises -> UInt8:
        return self._get_value[UInt8](col, row, DUCKDB_TYPE_UTINYINT)

    fn get_uint16(self, col: Int, row: Int) raises -> UInt16:
        return self._get_value[UInt16](col, row, DUCKDB_TYPE_USMALLINT)

    fn get_uint32(self, col: Int, row: Int) raises -> UInt32:
        return self._get_value[UInt32](col, row, DUCKDB_TYPE_UINTEGER)

    fn get_uint64(self, col: Int, row: Int) raises -> UInt64:
        return self._get_value[UInt64](col, row, DUCKDB_TYPE_UBIGINT)

    fn get_float32(self, col: Int, row: Int) raises -> Float32:
        return self._get_value[Float32](col, row, DUCKDB_TYPE_FLOAT)

    fn get_float64(self, col: Int, row: Int) raises -> Float64:
        return self._get_value[Float64](col, row, DUCKDB_TYPE_DOUBLE)

    fn get_timestamp(self, col: Int, row: Int) raises -> Timestamp:
        return self._get_value[Timestamp](col, row, DUCKDB_TYPE_TIMESTAMP)

    fn get_date(self, col: Int, row: Int) raises -> Date:
        return self._get_value[Date](col, row, DUCKDB_TYPE_DATE)

    fn get_time(self, col: Int, row: Int) raises -> Time:
        return self._get_value[Time](col, row, DUCKDB_TYPE_TIME)

    fn get_interval(self, col: Int, row: Int) raises -> Interval:
        return self._get_value[Interval](col, row, DUCKDB_TYPE_INTERVAL)

    fn get_int128(self, col: Int, row: Int) raises -> Int128:
        return self._get_value[Int128](col, row, DUCKDB_TYPE_HUGEINT)

    fn get_uint128(self, col: Int, row: Int) raises -> UInt128:
        return self._get_value[UInt128](col, row, DUCKDB_TYPE_UHUGEINT)

    fn get_string(self, col: Int, row: Int) raises -> String:
        self._validate(col, row, DUCKDB_TYPE_VARCHAR)
        var vector = self.__get_vector(col)
        # Short strings are inlined so need to check the length and then cast accordingly.
        var data_str_ptr = vector.__get_data().bitcast[
            duckdb_string_t_pointer
        ]()
        var string_value: String
        var string_length = int(data_str_ptr[row].length)
        if data_str_ptr[row].length <= 12:
            var data_str_inlined = vector.__get_data().bitcast[
                duckdb_string_t_inlined
            ]()
            string_value = StringRef(
                data_str_inlined[row].inlined.unsafe_ptr(), string_length
            )
        else:
            string_value = StringRef(data_str_ptr[row].ptr, string_length)
        return string_value

    # TODO remaining types


@value
struct Vector:
    var __vector: duckdb_vector

    fn __get_data(self) -> UnsafePointer[NoneType]:
        var impl = _get_global_duckdb_itf().libDuckDB()
        return impl.duckdb_vector_get_data(self.__vector)


# struct ResultSetIterator:
#     var result: ResultSet
#     var index: Int

#     fn __init__(inout self, result: ResultSet):
#         self.index = 0
#         self.result = result

#     # fn __index__(self) -> UInt64:
#     #     return self.index

#     # fn __len__(self) -> Int:
#     #     return int(self.result.rows - self.index)  # TODO could overflow

#     fn __next__(inout self) -> String:
#         self.index += 1
#         return str(self.index)
