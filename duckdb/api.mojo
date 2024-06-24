from duckdb.libduckdb import *
from sys.ffi import _get_global

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

struct Connection:
    var __db: duckdb_database
    var __conn: duckdb_connection

    fn __init__(inout self, db_path: String) raises:
        var impl = _get_global_duckdb_itf().libDuckDB()
        self.__db = UnsafePointer[duckdb_database.type]()
        var db_addr = UnsafePointer.address_of(self.__db)
        if (impl.duckdb_open(db_path.unsafe_cstr_ptr(), db_addr)) == DuckDBError:
            raise Error(
                "Could not open database"
            )  ## TODO use duckdb_open_ext and return error message
        self.__conn = UnsafePointer[duckdb_connection.type]()
        if (
            impl.duckdb_connect(self.__db, UnsafePointer.address_of(self.__conn))
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



@value
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

        return int(self.impl.duckdb_column_count(UnsafePointer.address_of(self.__result)))

    fn column_name(self, col: UInt64) -> String:
        return self.impl.duckdb_column_name(UnsafePointer.address_of(self.__result), col)

    fn column_type(self, col: UInt64) -> Int:
        return int(
            self.impl.duckdb_column_type(UnsafePointer.address_of(self.__result), col)
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

    fn __iter__(self) -> ResultSetIterator:
        return ResultSetIterator(self)

    fn fetch_chunk(self) raises -> Chunk:
        return Chunk(self.impl.duckdb_fetch_chunk(self.__result), self)

    # TODO del/destroy


@value
struct Chunk:
    var impl: LibDuckDB
    var __chunk: duckdb_data_chunk
    var result: ResultSet

    def __init__(inout self, chunk: duckdb_data_chunk, result: ResultSet):
        self.impl = _get_global_duckdb_itf().libDuckDB()
        self.__chunk = chunk
        self.result = result

    fn __len__(self) -> Int:
        return int(self.impl.duckdb_data_chunk_get_size(self.__chunk))

    fn __get_vector(self, col: UInt64) -> Vector:
        return Vector(self.impl.duckdb_data_chunk_get_vector(self.__chunk, col))

    fn get_string(self, col: Int, row: Int) raises -> String:
        if row >= len(self):
            raise Error(String("Row {} out of bounds.").format(row))
        if col >= self.result.column_count():
            raise Error(String("Column {} out of bounds.").format(col))
        if self.result.column_type(col) != DUCKDB_TYPE_VARCHAR:
            raise Error(
                String("Column {} has type {}. Expected string.").format(
                    col, type_names().get(self.result.column_type(col), "UNKNOWN")
                )
            )
        var vector = self.__get_vector(col)
        # Short strings are inlined so need to check the length and then cast accordingly
        var data_str_ptr = vector.__get_data().bitcast[
            duckdb_string_t_pointer
        ]()
        var data_str_inlined = vector.__get_data().bitcast[
            duckdb_string_t_inlined
        ]()
        var string_value: String
        if data_str_ptr[row].length <= 12:
            string_value = StringRef(
                data_str_inlined[row].inlined.unsafe_ptr(), 12
            )
        else:
            string_value = StringRef(
                data_str_ptr[row].ptr, int(data_str_ptr[row].length)
            )
        return string_value


@value
struct Vector:
    var __vector: duckdb_vector


    fn __get_data(self) -> UnsafePointer[NoneType]:
        var impl = _get_global_duckdb_itf().libDuckDB()
        return impl.duckdb_vector_get_data(self.__vector)


@value
struct ResultSetIterator:
    var result: ResultSet
    var index: Int

    fn __init__(inout self, result: ResultSet):
        self.index = 0
        self.result = result

    # fn __index__(self) -> UInt64:
    #     return self.index

    # fn __len__(self) -> Int:
    #     return int(self.result.rows - self.index)  # TODO could overflow

    fn __next__(inout self) -> String:
        self.index += 1
        return str(self.index)
