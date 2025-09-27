from sys.ffi import external_call, DLHandle, c_char
from utils import StaticTuple
from collections import InlineArray
from duckdb.duckdb_type import *
from sys.info import CompilationTarget
from os import abort
from pathlib import Path
from sys.ffi import _find_dylib
from sys.ffi import _get_dylib_function as _ffi_get_dylib_function
from sys.ffi import _Global, _OwnedDLHandle

# ===-----------------------------------------------------------------------===#
# FFI definitions for the DuckDB C API ported to Mojo.
# 
# Derived from
# https://github.com/duckdb/duckdb/blob/v1.0.0/src/include/duckdb.h
# 
# Once Mojo is able to generate these bindings automatically, we should switch
# to ease maintenance.
# ===-----------------------------------------------------------------------===#


# ===--------------------------------------------------------------------===#
# Enums
# ===--------------------------------------------------------------------===#

#! An enum over DuckDB's internal types.
alias duckdb_type = Int32
alias DUCKDB_TYPE_INVALID = 0
# bool
alias DUCKDB_TYPE_BOOLEAN = 1
# int8_t
alias DUCKDB_TYPE_TINYINT = 2
# int16_t
alias DUCKDB_TYPE_SMALLINT = 3
# int32_t
alias DUCKDB_TYPE_INTEGER = 4
# int64_t
alias DUCKDB_TYPE_BIGINT = 5
# uint8_t
alias DUCKDB_TYPE_UTINYINT = 6
# uint16_t
alias DUCKDB_TYPE_USMALLINT = 7
# uint32_t
alias DUCKDB_TYPE_UINTEGER = 8
# uint64_t
alias DUCKDB_TYPE_UBIGINT = 9
# float
alias DUCKDB_TYPE_FLOAT = 10
# double
alias DUCKDB_TYPE_DOUBLE = 11
# duckdb_timestamp, in microseconds
alias DUCKDB_TYPE_TIMESTAMP = 12
# duckdb_date
alias DUCKDB_TYPE_DATE = 13
# duckdb_time
alias DUCKDB_TYPE_TIME = 14
# duckdb_interval
alias DUCKDB_TYPE_INTERVAL = 15
# duckdb_hugeint
alias DUCKDB_TYPE_HUGEINT = 16
# duckdb_uhugeint
alias DUCKDB_TYPE_UHUGEINT = 32
# const char*
alias DUCKDB_TYPE_VARCHAR = 17
# duckdb_blob
alias DUCKDB_TYPE_BLOB = 18
# decimal
alias DUCKDB_TYPE_DECIMAL = 19
# duckdb_timestamp, in seconds
alias DUCKDB_TYPE_TIMESTAMP_S = 20
# duckdb_timestamp, in milliseconds
alias DUCKDB_TYPE_TIMESTAMP_MS = 21
# duckdb_timestamp, in nanoseconds
alias DUCKDB_TYPE_TIMESTAMP_NS = 22
# enum type, only useful as logical type
alias DUCKDB_TYPE_ENUM = 23
# list type, only useful as logical type
alias DUCKDB_TYPE_LIST = 24
# struct type, only useful as logical type
alias DUCKDB_TYPE_STRUCT = 25
# map type, only useful as logical type
alias DUCKDB_TYPE_MAP = 26
# duckdb_array, only useful as logical type
alias DUCKDB_TYPE_ARRAY = 33
# duckdb_hugeint
alias DUCKDB_TYPE_UUID = 27
# union type, only useful as logical type
alias DUCKDB_TYPE_UNION = 28
# duckdb_bit
alias DUCKDB_TYPE_BIT = 29
# duckdb_time_tz
alias DUCKDB_TYPE_TIME_TZ = 30
# duckdb_timestamp
alias DUCKDB_TYPE_TIMESTAMP_TZ = 31

#! An enum over the returned state of different functions.
alias duckdb_state = Int32
alias DuckDBSuccess = 0
alias DuckDBError = 1

#! An enum over the pending state of a pending query result.
alias duckdb_pending_state = Int32
alias DUCKDB_PENDING_RESULT_READY = 0
alias DUCKDB_PENDING_RESULT_NOT_READY = 1
alias DUCKDB_PENDING_ERROR = 2
alias DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3

#! An enum over DuckDB's different result types.
alias duckdb_result_type = Int32
alias DUCKDB_RESULT_TYPE_INVALID = 0
alias DUCKDB_RESULT_TYPE_CHANGED_ROWS = 1
alias DUCKDB_RESULT_TYPE_NOTHING = 2
alias DUCKDB_RESULT_TYPE_QUERY_RESULT = 3

#! An enum over DuckDB's different statement types.
alias duckdb_statement_type = Int32
alias DUCKDB_STATEMENT_TYPE_INVALID = 0
alias DUCKDB_STATEMENT_TYPE_SELECT = 1
alias DUCKDB_STATEMENT_TYPE_INSERT = 2
alias DUCKDB_STATEMENT_TYPE_UPDATE = 3
alias DUCKDB_STATEMENT_TYPE_EXPLAIN = 4
alias DUCKDB_STATEMENT_TYPE_DELETE = 5
alias DUCKDB_STATEMENT_TYPE_PREPARE = 6
alias DUCKDB_STATEMENT_TYPE_CREATE = 7
alias DUCKDB_STATEMENT_TYPE_EXECUTE = 8
alias DUCKDB_STATEMENT_TYPE_ALTER = 9
alias DUCKDB_STATEMENT_TYPE_TRANSACTION = 10
alias DUCKDB_STATEMENT_TYPE_COPY = 11
alias DUCKDB_STATEMENT_TYPE_ANALYZE = 12
alias DUCKDB_STATEMENT_TYPE_VARIABLE_SET = 13
alias DUCKDB_STATEMENT_TYPE_CREATE_FUNC = 14
alias DUCKDB_STATEMENT_TYPE_DROP = 15
alias DUCKDB_STATEMENT_TYPE_EXPORT = 16
alias DUCKDB_STATEMENT_TYPE_PRAGMA = 17
alias DUCKDB_STATEMENT_TYPE_VACUUM = 18
alias DUCKDB_STATEMENT_TYPE_CALL = 19
alias DUCKDB_STATEMENT_TYPE_SET = 20
alias DUCKDB_STATEMENT_TYPE_LOAD = 21
alias DUCKDB_STATEMENT_TYPE_RELATION = 22
alias DUCKDB_STATEMENT_TYPE_EXTENSION = 23
alias DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN = 24
alias DUCKDB_STATEMENT_TYPE_ATTACH = 25
alias DUCKDB_STATEMENT_TYPE_DETACH = 26
alias DUCKDB_STATEMENT_TYPE_MULTI = 27


# ===--------------------------------------------------------------------===#
# General type definitions
# ===--------------------------------------------------------------------===#


alias idx_t = UInt64

# ===--------------------------------------------------------------------===#
# Types (no explicit freeing)
# ===--------------------------------------------------------------------===#


#! Days are stored as days since 1970-01-01
#! Use the duckdb_from_date/duckdb_to_date function to extract individual information
alias duckdb_date = Date


@fieldwise_init
struct duckdb_date_struct(Copyable, Movable):
    var year: Int32
    var month: Int8
    var day: Int8


#! Time is stored as microseconds since 00:00:00
#! Use the duckdb_from_time/duckdb_to_time function to extract individual information
alias duckdb_time = Time


@fieldwise_init
struct duckdb_time_struct(Copyable, Movable):
    var hour: Int8
    var min: Int8
    var sec: Int8
    var micros: Int32


#! TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset
@fieldwise_init
struct duckdb_time_tz(Copyable, Movable):
    var bits: UInt64


@fieldwise_init
struct duckdb_time_tz_struct(Copyable, Movable):
    var time: duckdb_time_struct
    var offset: Int32


#! Timestamps are stored as microseconds since 1970-01-01
#! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information
alias duckdb_timestamp = Timestamp


@fieldwise_init
struct duckdb_timestamp_struct(Copyable, Movable):
    var date: duckdb_date_struct
    var time: duckdb_time_struct


#! Hugeints are composed of a (lower, upper) component
#! The value of the hugeint is upper * 2^64 + lower
#! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
alias duckdb_interval = Interval
alias duckdb_hugeint = Int128
alias duckdb_uhugeint = UInt128
#! Decimals are composed of a width and a scale, and are stored in a hugeint
alias duckdb_decimal = Decimal


@fieldwise_init
#! A type holding information about the query execution progress
struct duckdb_query_progress_type(Copyable, Movable):
    var percentage: Float64
    var rows_processed: UInt64
    var total_rows_to_process: UInt64


#! The internal representation of a VARCHAR (string_t). If the VARCHAR does not
#! exceed 12 characters, then we inline it. Otherwise, we inline a prefix for faster
#! string comparisons and store a pointer to the remaining characters. This is a non-
#! owning structure, i.e., it does not have to be freed.

# This is defined as a C union, which is not yet supported in Mojo so we use two structs and
# peek into length to determine which one is used.


@fieldwise_init
struct duckdb_string_t_pointer(Copyable, Movable):
    var length: UInt32
    var prefix: InlineArray[c_char, 4]
    var ptr: UnsafePointer[c_char]


@fieldwise_init
struct duckdb_string_t_inlined(Copyable, Movable):
    var length: UInt32
    var inlined: InlineArray[c_char, 12]


#! The internal representation of a list metadata entry contains the list's offset in
#! the child vector, and its length. The parent vector holds these metadata entries,
#! whereas the child vector holds the data
@fieldwise_init
struct duckdb_list_entry(ImplicitlyCopyable, Movable):
    var offset: UInt64
    var length: UInt64


@fieldwise_init
struct duckdb_column(Copyable, Movable):
    var __deprecated_data: UnsafePointer[NoneType]
    var __deprecated_nullmask: UnsafePointer[Bool]
    var __deprecated_type: Int32  # actually a duckdb_type enum
    var __deprecated_name: UnsafePointer[c_char]
    var internal_data: UnsafePointer[NoneType]

    fn __init__(out self):
        self.__deprecated_data = UnsafePointer[NoneType]()
        self.__deprecated_nullmask = UnsafePointer[Bool]()
        self.__deprecated_type = 0
        self.__deprecated_name = UnsafePointer[c_char]()
        self.internal_data = UnsafePointer[NoneType]()


struct _duckdb_vector:
    var __vctr: UnsafePointer[NoneType]


alias duckdb_vector = UnsafePointer[_duckdb_vector]

# ===--------------------------------------------------------------------===#
# Types (explicit freeing/destroying)
# ===--------------------------------------------------------------------===#


struct duckdb_string:
    var data: UnsafePointer[c_char]
    var size: idx_t


struct duckdb_blob:
    var data: UnsafePointer[NoneType]
    var size: idx_t


@fieldwise_init
struct duckdb_result(ImplicitlyCopyable & Movable):
    var __deprecated_column_count: idx_t
    var __deprecated_row_count: idx_t
    var __deprecated_rows_changed: idx_t
    var __deprecated_columns: UnsafePointer[duckdb_column]
    var __deprecated_error_message: UnsafePointer[c_char]
    var internal_data: UnsafePointer[NoneType]

    fn __init__(out self):
        self.__deprecated_column_count = 0
        self.__deprecated_row_count = 0
        self.__deprecated_rows_changed = 0
        self.__deprecated_columns = UnsafePointer[duckdb_column]()
        self.__deprecated_error_message = UnsafePointer[c_char]()
        self.internal_data = UnsafePointer[NoneType]()


struct _duckdb_database:
    var __db: UnsafePointer[NoneType]


alias duckdb_database = UnsafePointer[_duckdb_database]


struct _duckdb_connection:
    var __conn: UnsafePointer[NoneType]


alias duckdb_connection = UnsafePointer[_duckdb_connection]


struct _duckdb_prepared_statement:
    var __prep: UnsafePointer[NoneType]


alias duckdb_prepared_statement = UnsafePointer[_duckdb_prepared_statement]


struct _duckdb_extracted_statements:
    var __extrac: UnsafePointer[NoneType]


alias duckdb_extracted_statements = UnsafePointer[_duckdb_extracted_statements]


struct _duckdb_pending_result:
    var __pend: UnsafePointer[NoneType]


alias duckdb_pending_result = UnsafePointer[_duckdb_pending_result]


struct _duckdb_appender:
    var __appn: UnsafePointer[NoneType]


alias duckdb_appender = UnsafePointer[_duckdb_appender]


struct _duckdb_config:
    var __cnfg: UnsafePointer[NoneType]


alias duckdb_config = UnsafePointer[_duckdb_config]


struct _duckdb_logical_type:
    var __lglt: UnsafePointer[NoneType]


alias duckdb_logical_type = UnsafePointer[_duckdb_logical_type]


struct _duckdb_data_chunk:
    var __dtck: UnsafePointer[NoneType]


alias duckdb_data_chunk = UnsafePointer[_duckdb_data_chunk]


struct _duckdb_value:
    var __val: UnsafePointer[NoneType]


alias duckdb_value = UnsafePointer[_duckdb_value]


# ===-----------------------------------------------------------------------===#
# Library Load
# ===-----------------------------------------------------------------------===#

alias DUCKDB_LIBRARY_PATHS = List[Path](
    "libduckdb.so",
    "libduckdb.dylib",
)

alias DUCKDB_LIBRARY = _Global["DUCKDB_LIBRARY", _init_dylib]

fn _init_dylib() -> _OwnedDLHandle:
    return _find_dylib["libduckdb"](materialize[DUCKDB_LIBRARY_PATHS]())


@always_inline
fn _get_dylib_function[
    func_name: StaticString, result_type: AnyTrivialRegType
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_LIBRARY(),
        func_name,
        result_type,
    ]()


@register_passable("trivial")
struct _dylib_function[fn_name: StaticString, type: AnyTrivialRegType]:
    alias fn_type = type

    @staticmethod
    fn load() raises -> type:
        return _get_dylib_function[fn_name, type]()

alias DUCKDB_HELPERS_PATHS = List[Path](
    "libduckdb_mojo_helpers.so",
    "libduckdb_mojo_helpers.dylib",
)

alias DUCKDB_HELPERS_LIBRARY = _Global["DUCKDB_HELPERS_LIBRARY", _init_helper_dylib]

fn _init_helper_dylib() -> _OwnedDLHandle:
    return _find_dylib["libduckdb_mojo_helpers"](materialize[DUCKDB_HELPERS_PATHS]())

@always_inline
fn _get_dylib_helpers_function[
    func_name: StaticString, result_type: AnyTrivialRegType
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_HELPERS_LIBRARY(),
        func_name,
        result_type,
    ]()

@register_passable("trivial")
struct _dylib_helpers_function[fn_name: StaticString, type: AnyTrivialRegType]:
    alias fn_type = type

    @staticmethod
    fn load() raises -> type:
        return _get_dylib_helpers_function[fn_name, type]()

struct LibDuckDB(Movable):

    var _duckdb_open: _duckdb_open.fn_type
    var _duckdb_close: _duckdb_close.fn_type
    var _duckdb_connect: _duckdb_connect.fn_type
    var _duckdb_disconnect: _duckdb_disconnect.fn_type
    var _duckdb_query: _duckdb_query.fn_type
    var _duckdb_destroy_result: _duckdb_destroy_result.fn_type
    var _duckdb_column_name: _duckdb_column_name.fn_type
    var _duckdb_column_type: _duckdb_column_type.fn_type
    var _duckdb_result_statement_type: _duckdb_result_statement_type.fn_type
    var _duckdb_column_logical_type: _duckdb_column_logical_type.fn_type
    var _duckdb_column_count: _duckdb_column_count.fn_type
    var _duckdb_rows_changed: _duckdb_rows_changed.fn_type
    var _duckdb_result_error: _duckdb_result_error.fn_type
    var _duckdb_row_count: _duckdb_row_count.fn_type
    var _duckdb_result_return_type: _duckdb_result_return_type.fn_type
    var _duckdb_vector_size: _duckdb_vector_size.fn_type
    var _duckdb_create_data_chunk: _duckdb_create_data_chunk.fn_type
    var _duckdb_destroy_data_chunk: _duckdb_destroy_data_chunk.fn_type
    var _duckdb_data_chunk_reset: _duckdb_data_chunk_reset.fn_type
    var _duckdb_data_chunk_get_column_count: _duckdb_data_chunk_get_column_count.fn_type
    var _duckdb_data_chunk_get_vector: _duckdb_data_chunk_get_vector.fn_type
    var _duckdb_data_chunk_get_size: _duckdb_data_chunk_get_size.fn_type
    var _duckdb_data_chunk_set_size: _duckdb_data_chunk_set_size.fn_type
    var _duckdb_from_date: _duckdb_from_date.fn_type
    var _duckdb_to_date: _duckdb_to_date.fn_type
    var _duckdb_is_finite_date: _duckdb_is_finite_date.fn_type
    var _duckdb_from_time: _duckdb_from_time.fn_type
    var _duckdb_create_time_tz: _duckdb_create_time_tz.fn_type
    var _duckdb_from_time_tz: _duckdb_from_time_tz.fn_type
    var _duckdb_to_time: _duckdb_to_time.fn_type
    var _duckdb_to_timestamp: _duckdb_to_timestamp.fn_type
    var _duckdb_from_timestamp: _duckdb_from_timestamp.fn_type
    var _duckdb_is_finite_timestamp: _duckdb_is_finite_timestamp.fn_type
    var _duckdb_vector_get_column_type: _duckdb_vector_get_column_type.fn_type
    var _duckdb_vector_get_data: _duckdb_vector_get_data.fn_type
    var _duckdb_vector_get_validity: _duckdb_vector_get_validity.fn_type
    var _duckdb_vector_ensure_validity_writable: _duckdb_vector_ensure_validity_writable.fn_type
    var _duckdb_vector_assign_string_element: _duckdb_vector_assign_string_element.fn_type
    var _duckdb_vector_assign_string_element_len: _duckdb_vector_assign_string_element_len.fn_type
    var _duckdb_list_vector_get_child: _duckdb_list_vector_get_child.fn_type
    var _duckdb_list_vector_get_size: _duckdb_list_vector_get_size.fn_type
    var _duckdb_list_vector_set_size: _duckdb_list_vector_set_size.fn_type
    var _duckdb_list_vector_reserve: _duckdb_list_vector_reserve.fn_type
    var _duckdb_struct_vector_get_child: _duckdb_struct_vector_get_child.fn_type
    var _duckdb_array_vector_get_child: _duckdb_array_vector_get_child.fn_type
    var _duckdb_validity_row_is_valid: _duckdb_validity_row_is_valid.fn_type
    var _duckdb_validity_set_row_validity: _duckdb_validity_set_row_validity.fn_type
    var _duckdb_validity_set_row_invalid: _duckdb_validity_set_row_invalid.fn_type
    var _duckdb_validity_set_row_valid: _duckdb_validity_set_row_valid.fn_type
    var _duckdb_create_logical_type: _duckdb_create_logical_type.fn_type
    var _duckdb_create_list_type: _duckdb_create_list_type.fn_type
    var _duckdb_create_array_type: _duckdb_create_array_type.fn_type
    var _duckdb_create_map_type: _duckdb_create_map_type.fn_type
    var _duckdb_create_union_type: _duckdb_create_union_type.fn_type
    var _duckdb_create_struct_type: _duckdb_create_struct_type.fn_type
    var _duckdb_get_type_id: _duckdb_get_type_id.fn_type
    var _duckdb_list_type_child_type: _duckdb_list_type_child_type.fn_type
    var _duckdb_array_type_child_type: _duckdb_array_type_child_type.fn_type
    var _duckdb_map_type_key_type: _duckdb_map_type_key_type.fn_type
    var _duckdb_map_type_value_type: _duckdb_map_type_value_type.fn_type
    var _duckdb_destroy_logical_type: _duckdb_destroy_logical_type.fn_type
    var _duckdb_execution_is_finished: _duckdb_execution_is_finished.fn_type
    var _duckdb_fetch_chunk_ptr: _duckdb_fetch_chunk_ptr.fn_type

    fn __init__(out self):
        try:
            self._duckdb_open = _duckdb_open.load()
            self._duckdb_close = _duckdb_close.load()
            self._duckdb_connect = _duckdb_connect.load()
            self._duckdb_disconnect = _duckdb_disconnect.load()
            self._duckdb_query = _duckdb_query.load()
            self._duckdb_destroy_result = _duckdb_destroy_result.load()
            self._duckdb_column_name = _duckdb_column_name.load()
            self._duckdb_column_type = _duckdb_column_type.load()
            self._duckdb_result_statement_type = _duckdb_result_statement_type.load()
            self._duckdb_column_logical_type = _duckdb_column_logical_type.load()
            self._duckdb_column_count = _duckdb_column_count.load()
            self._duckdb_rows_changed = _duckdb_rows_changed.load()
            self._duckdb_result_error = _duckdb_result_error.load()
            self._duckdb_row_count = _duckdb_row_count.load()
            self._duckdb_result_return_type = _duckdb_result_return_type.load()
            self._duckdb_vector_size = _duckdb_vector_size.load()
            self._duckdb_create_data_chunk = _duckdb_create_data_chunk.load()
            self._duckdb_destroy_data_chunk = _duckdb_destroy_data_chunk.load()
            self._duckdb_data_chunk_reset = _duckdb_data_chunk_reset.load()
            self._duckdb_data_chunk_get_column_count = _duckdb_data_chunk_get_column_count.load()
            self._duckdb_data_chunk_get_vector = _duckdb_data_chunk_get_vector.load()
            self._duckdb_data_chunk_get_size = _duckdb_data_chunk_get_size.load()
            self._duckdb_data_chunk_set_size = _duckdb_data_chunk_set_size.load()
            self._duckdb_from_date = _duckdb_from_date.load()
            self._duckdb_to_date = _duckdb_to_date.load()
            self._duckdb_is_finite_date = _duckdb_is_finite_date.load()
            self._duckdb_from_time = _duckdb_from_time.load()
            self._duckdb_create_time_tz = _duckdb_create_time_tz.load()
            self._duckdb_from_time_tz = _duckdb_from_time_tz.load()
            self._duckdb_to_time = _duckdb_to_time.load()
            self._duckdb_to_timestamp = _duckdb_to_timestamp.load()
            self._duckdb_from_timestamp = _duckdb_from_timestamp.load()
            self._duckdb_is_finite_timestamp = _duckdb_is_finite_timestamp.load()
            self._duckdb_vector_get_column_type = _duckdb_vector_get_column_type.load()
            self._duckdb_vector_get_data = _duckdb_vector_get_data.load()
            self._duckdb_vector_get_validity = _duckdb_vector_get_validity.load()
            self._duckdb_vector_ensure_validity_writable = _duckdb_vector_ensure_validity_writable.load()
            self._duckdb_vector_assign_string_element = _duckdb_vector_assign_string_element.load()
            self._duckdb_vector_assign_string_element_len = _duckdb_vector_assign_string_element_len.load()
            self._duckdb_list_vector_get_child = _duckdb_list_vector_get_child.load()
            self._duckdb_list_vector_get_size = _duckdb_list_vector_get_size.load()
            self._duckdb_list_vector_set_size = _duckdb_list_vector_set_size.load()
            self._duckdb_list_vector_reserve = _duckdb_list_vector_reserve.load()
            self._duckdb_struct_vector_get_child = _duckdb_struct_vector_get_child.load()
            self._duckdb_array_vector_get_child = _duckdb_array_vector_get_child.load()
            self._duckdb_validity_row_is_valid = _duckdb_validity_row_is_valid.load()
            self._duckdb_validity_set_row_validity = _duckdb_validity_set_row_validity.load()
            self._duckdb_validity_set_row_invalid = _duckdb_validity_set_row_invalid.load()
            self._duckdb_validity_set_row_valid = _duckdb_validity_set_row_valid.load()
            self._duckdb_create_logical_type = _duckdb_create_logical_type.load()
            self._duckdb_create_list_type = _duckdb_create_list_type.load()
            self._duckdb_create_array_type = _duckdb_create_array_type.load()
            self._duckdb_create_map_type = _duckdb_create_map_type.load()
            self._duckdb_create_union_type = _duckdb_create_union_type.load()
            self._duckdb_create_struct_type = _duckdb_create_struct_type.load()
            self._duckdb_get_type_id = _duckdb_get_type_id.load()
            self._duckdb_list_type_child_type = _duckdb_list_type_child_type.load()
            self._duckdb_array_type_child_type = _duckdb_array_type_child_type.load()
            self._duckdb_map_type_key_type = _duckdb_map_type_key_type.load()
            self._duckdb_map_type_value_type = _duckdb_map_type_value_type.load()
            self._duckdb_destroy_logical_type = _duckdb_destroy_logical_type.load()
            self._duckdb_execution_is_finished = _duckdb_execution_is_finished.load()
            self._duckdb_fetch_chunk_ptr = _duckdb_fetch_chunk_ptr.load()
        except e:
            abort(String(e))
            __mlir_op.`lit.ownership.mark_initialized`(
                __get_mvalue_as_litref(self)
            )

    fn __moveinit__(out self, deinit existing: Self):
        self._duckdb_open = existing._duckdb_open
        self._duckdb_close = existing._duckdb_close
        self._duckdb_connect = existing._duckdb_connect
        self._duckdb_disconnect = existing._duckdb_disconnect
        self._duckdb_query = existing._duckdb_query
        self._duckdb_destroy_result = existing._duckdb_destroy_result
        self._duckdb_column_name = existing._duckdb_column_name
        self._duckdb_column_type = existing._duckdb_column_type
        self._duckdb_result_statement_type = existing._duckdb_result_statement_type
        self._duckdb_column_logical_type = existing._duckdb_column_logical_type
        self._duckdb_column_count = existing._duckdb_column_count
        self._duckdb_rows_changed = existing._duckdb_rows_changed
        self._duckdb_result_error = existing._duckdb_result_error
        self._duckdb_row_count = existing._duckdb_row_count
        self._duckdb_result_return_type = existing._duckdb_result_return_type
        self._duckdb_vector_size = existing._duckdb_vector_size
        self._duckdb_create_data_chunk = existing._duckdb_create_data_chunk
        self._duckdb_destroy_data_chunk = existing._duckdb_destroy_data_chunk
        self._duckdb_data_chunk_reset = existing._duckdb_data_chunk_reset
        self._duckdb_data_chunk_get_column_count = existing._duckdb_data_chunk_get_column_count
        self._duckdb_data_chunk_get_vector = existing._duckdb_data_chunk_get_vector
        self._duckdb_data_chunk_get_size = existing._duckdb_data_chunk_get_size
        self._duckdb_data_chunk_set_size = existing._duckdb_data_chunk_set_size
        self._duckdb_from_date = existing._duckdb_from_date
        self._duckdb_to_date = existing._duckdb_to_date
        self._duckdb_is_finite_date = existing._duckdb_is_finite_date
        self._duckdb_from_time = existing._duckdb_from_time
        self._duckdb_create_time_tz = existing._duckdb_create_time_tz
        self._duckdb_from_time_tz = existing._duckdb_from_time_tz
        self._duckdb_to_time = existing._duckdb_to_time
        self._duckdb_to_timestamp = existing._duckdb_to_timestamp
        self._duckdb_from_timestamp = existing._duckdb_from_timestamp
        self._duckdb_is_finite_timestamp = existing._duckdb_is_finite_timestamp
        self._duckdb_vector_get_column_type = existing._duckdb_vector_get_column_type
        self._duckdb_vector_get_data = existing._duckdb_vector_get_data
        self._duckdb_vector_get_validity = existing._duckdb_vector_get_validity
        self._duckdb_vector_ensure_validity_writable = existing._duckdb_vector_ensure_validity_writable
        self._duckdb_vector_assign_string_element = existing._duckdb_vector_assign_string_element
        self._duckdb_vector_assign_string_element_len = existing._duckdb_vector_assign_string_element_len
        self._duckdb_list_vector_get_child = existing._duckdb_list_vector_get_child
        self._duckdb_list_vector_get_size = existing._duckdb_list_vector_get_size
        self._duckdb_list_vector_set_size = existing._duckdb_list_vector_set_size
        self._duckdb_list_vector_reserve = existing._duckdb_list_vector_reserve
        self._duckdb_struct_vector_get_child = existing._duckdb_struct_vector_get_child
        self._duckdb_array_vector_get_child = existing._duckdb_array_vector_get_child
        self._duckdb_validity_row_is_valid = existing._duckdb_validity_row_is_valid
        self._duckdb_validity_set_row_validity = existing._duckdb_validity_set_row_validity
        self._duckdb_validity_set_row_invalid = existing._duckdb_validity_set_row_invalid
        self._duckdb_validity_set_row_valid = existing._duckdb_validity_set_row_valid
        self._duckdb_create_logical_type = existing._duckdb_create_logical_type
        self._duckdb_create_list_type = existing._duckdb_create_list_type
        self._duckdb_create_array_type = existing._duckdb_create_array_type
        self._duckdb_create_map_type = existing._duckdb_create_map_type
        self._duckdb_create_union_type = existing._duckdb_create_union_type
        self._duckdb_create_struct_type = existing._duckdb_create_struct_type
        self._duckdb_get_type_id = existing._duckdb_get_type_id
        self._duckdb_list_type_child_type = existing._duckdb_list_type_child_type
        self._duckdb_array_type_child_type = existing._duckdb_array_type_child_type
        self._duckdb_map_type_key_type = existing._duckdb_map_type_key_type
        self._duckdb_map_type_value_type = existing._duckdb_map_type_value_type
        self._duckdb_destroy_logical_type = existing._duckdb_destroy_logical_type
        self._duckdb_execution_is_finished = existing._duckdb_execution_is_finished
        self._duckdb_fetch_chunk_ptr = existing._duckdb_fetch_chunk_ptr


    # ===--------------------------------------------------------------------===#
    # Functions
    # ===--------------------------------------------------------------------===#

    # ===--------------------------------------------------------------------===#
    # Open/Connect
    # ===--------------------------------------------------------------------===#

    fn duckdb_open(
        self,
        path: UnsafePointer[c_char],
        out_database: UnsafePointer[duckdb_database],
    ) -> UInt32:
        """
        Creates a new database or opens an existing database file stored at the given path.
        If no path is given a new in-memory database is created instead.
        The instantiated database should be closed with 'duckdb_close'.

        * path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
        * out_database: The result database object.
        * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
        *
        """
        return self._duckdb_open(path, out_database)

    fn duckdb_close(self, database: UnsafePointer[duckdb_database]) -> NoneType:
        """
        Closes the specified database and de-allocates all memory allocated for that database.
        This should be called after you are done with any database allocated through `duckdb_open` or `duckdb_open_ext`.
        Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
        Still, it is recommended to always correctly close a database object after you are done with it.

        * database: The database object to shut down.
        """
        return self._duckdb_close(database)

    fn duckdb_connect(
        self,
        database: duckdb_database,
        out_connection: UnsafePointer[duckdb_connection],
    ) -> UInt32:
        """
        Opens a connection to a database. Connections are required to query the database, and store transactional state
        associated with the connection.
        The instantiated connection should be closed using 'duckdb_disconnect'.

        * database: The database file to connect to.
        * out_connection: The result connection object.
        * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
        """
        return self._duckdb_connect(database, out_connection)

    fn duckdb_disconnect(
        self,
        connection: UnsafePointer[duckdb_connection]
    ) -> NoneType:
        """
        Closes the specified connection and de-allocates all memory allocated for that connection.

        * connection: The connection to close.
        """
        return self._duckdb_disconnect(connection)

    # ===--------------------------------------------------------------------===#
    # Query Execution
    # ===--------------------------------------------------------------------===#

    fn duckdb_query(
        self,
        connection: duckdb_connection,
        query: UnsafePointer[c_char],
        out_result: UnsafePointer[duckdb_result],
    ) -> UInt32:
        """
        Executes a SQL query within a connection and stores the full (materialized) result in the out_result pointer.
        If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
        `duckdb_result_error`.

        Note that after running `duckdb_query`, `duckdb_destroy_result` must be called on the result object even if the
        query fails, otherwise the error stored within the result will not be freed correctly.

        * connection: The connection to perform the query in.
        * query: The SQL query to run.
        * out_result: The query result.
        * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
        """
        return self._duckdb_query(connection, query, out_result)

    fn duckdb_destroy_result(self, result: UnsafePointer[duckdb_result]) -> NoneType:
        """
        Closes the result and de-allocates all memory allocated for that connection.

        * result: The result to destroy.
        """
        return self._duckdb_destroy_result(result)

    fn duckdb_column_name(
        self, result: UnsafePointer[duckdb_result], col: idx_t
    ) -> UnsafePointer[c_char]:
        """
        Returns the column name of the specified column. The result should not need to be freed; the column names will
        automatically be destroyed when the result is destroyed.

        Returns `NULL` if the column is out of range.

        * result: The result object to fetch the column name from.
        * col: The column index.
        * returns: The column name of the specified column.
        """
        return self._duckdb_column_name(result, col)

    fn duckdb_column_type(
        self, result: UnsafePointer[duckdb_result], col: idx_t
    ) -> duckdb_type:
        """
        Returns the column type of the specified column.

        Returns `DUCKDB_TYPE_INVALID` if the column is out of range.

        * result: The result object to fetch the column type from.
        * col: The column index.
        * returns: The column type of the specified column.
        """
        return self._duckdb_column_type(result, col)

    fn duckdb_result_statement_type(self, result: duckdb_result) -> duckdb_statement_type:
        """
        Returns the statement type of the statement that was executed.

        * result: The result object to fetch the statement type from.
        * returns: duckdb_statement_type value or DUCKDB_STATEMENT_TYPE_INVALID
        """
        return self._duckdb_result_statement_type(result)

    fn duckdb_column_logical_type(
        self, result: UnsafePointer[duckdb_result], col: idx_t
    ) -> duckdb_logical_type:
        """
        Returns the logical column type of the specified column.

        The return type of this call should be destroyed with `duckdb_destroy_logical_type`.

        Returns `NULL` if the column is out of range.

        * result: The result object to fetch the column type from.
        * col: The column index.
        * returns: The logical column type of the specified column.
        """
        return self._duckdb_column_logical_type(result, col)

    fn duckdb_column_count(self, result: UnsafePointer[duckdb_result]) -> idx_t:
        """
        Returns the number of columns present in a the result object.

        * result: The result object.
        * returns: The number of columns present in the result object.
        """
        return self._duckdb_column_count(result)

    fn duckdb_rows_changed(self, result: UnsafePointer[duckdb_result]) -> idx_t:
        """
        Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
        queries. For other queries the rows_changed will be 0.

        * result: The result object.
        * returns: The number of rows changed.
        """
        return self._duckdb_rows_changed(result)

    fn duckdb_result_error(self, result: UnsafePointer[duckdb_result]) -> UnsafePointer[c_char]:
        """
        Returns the error message contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.

        The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_result` is called.

        * result: The result object to fetch the error from.
        * returns: The error of the result.
        """
        return self._duckdb_result_error(result)

    fn duckdb_row_count(self, result: UnsafePointer[duckdb_result]) -> idx_t:
        """Deprecated."""
        return self._duckdb_row_count(result)

    # ===--------------------------------------------------------------------===#
    # Result Functions
    # ===--------------------------------------------------------------------===#

    fn duckdb_result_return_type(self, result: duckdb_result) -> duckdb_result_type:
        """
        Returns the return_type of the given result, or DUCKDB_RETURN_TYPE_INVALID on error.

        * result: The result object
        * returns: The return_type
        """
        return self._duckdb_result_return_type(result)

    # ===--------------------------------------------------------------------===#
    # Helpers
    # ===--------------------------------------------------------------------===#

    fn duckdb_vector_size(self) -> idx_t:
        """The internal vector size used by DuckDB.
        This is the amount of tuples that will fit into a data chunk created by `duckdb_create_data_chunk`.

        * returns: The vector size.
        """
        return self._duckdb_vector_size()

    # ===--------------------------------------------------------------------===#
    # Data Chunk Interface
    # ===--------------------------------------------------------------------===#

    fn duckdb_create_data_chunk(
        self, types: UnsafePointer[duckdb_logical_type], column_count: idx_t
    ) -> duckdb_data_chunk:
        """
        Creates an empty DataChunk with the specified set of types.

        Note that the result must be destroyed with `duckdb_destroy_data_chunk`.

        * types: An array of types of the data chunk.
        * column_count: The number of columns.
        * returns: The data chunk.
        """
        return self._duckdb_create_data_chunk(types, column_count)

    fn duckdb_destroy_data_chunk(self, chunk: UnsafePointer[duckdb_data_chunk]) -> NoneType:
        """
        Destroys the data chunk and de-allocates all memory allocated for that chunk.

        * chunk: The data chunk to destroy.
        """
        return self._duckdb_destroy_data_chunk(chunk)

    fn duckdb_data_chunk_reset(self, chunk: duckdb_data_chunk) -> NoneType:
        """
        Resets a data chunk, clearing the validity masks and setting the cardinality of the data chunk to 0.

        * chunk: The data chunk to reset.
        """
        return self._duckdb_data_chunk_reset(chunk)

    fn duckdb_data_chunk_get_column_count(self, chunk: duckdb_data_chunk) -> idx_t:
        """
        Retrieves the number of columns in a data chunk.

        * chunk: The data chunk to get the data from
        * returns: The number of columns in the data chunk
        """
        return self._duckdb_data_chunk_get_column_count(chunk)

    fn duckdb_data_chunk_get_vector(
        self, chunk: duckdb_data_chunk, index: idx_t
    ) -> duckdb_vector:
        """
        Retrieves the vector at the specified column index in the data chunk.

        The pointer to the vector is valid for as long as the chunk is alive.
        It does NOT need to be destroyed.

        * chunk: The data chunk to get the data from
        * returns: The vector
        """
        return self._duckdb_data_chunk_get_vector(chunk, index)

    fn duckdb_data_chunk_get_size(self, chunk: duckdb_data_chunk) -> idx_t:
        """
        Retrieves the current number of tuples in a data chunk.

        * chunk: The data chunk to get the data from
        * returns: The number of tuples in the data chunk
        """
        return self._duckdb_data_chunk_get_size(chunk)

    fn duckdb_data_chunk_set_size(
        self, chunk: duckdb_data_chunk, size: idx_t
    ) -> NoneType:
        """
        Sets the current number of tuples in a data chunk.

        * chunk: The data chunk to set the size in
        * size: The number of tuples in the data chunk
        """
        return self._duckdb_data_chunk_set_size(chunk, size)

    fn duckdb_from_date(self, date: duckdb_date) -> duckdb_date_struct:
        """Decompose a `duckdb_date` object into year, month and date (stored as `duckdb_date_struct`).

        * date: The date object, as obtained from a `DUCKDB_TYPE_DATE` column.
        * returns: The `duckdb_date_struct` with the decomposed elements.
        """
        return self._duckdb_from_date(date)

    fn duckdb_to_date(self, date: duckdb_date_struct) -> duckdb_date:
        """Re-compose a `duckdb_date` from year, month and date (`duckdb_date_struct`).

        * date: The year, month and date stored in a `duckdb_date_struct`.
        * returns: The `duckdb_date` element.
        """
        return self._duckdb_to_date(date)

    fn duckdb_is_finite_date(self, date: duckdb_date) -> Bool:
        """Test a `duckdb_date` to see if it is a finite value.

        * date: The date object, as obtained from a `DUCKDB_TYPE_DATE` column.
        * returns: True if the date is finite, false if it is ±infinity.
        """
        return self._duckdb_is_finite_date(date)

    fn duckdb_from_time(self, time: duckdb_time) -> duckdb_time_struct:
        """Decompose a `duckdb_time` object into hour, minute, second and microsecond (stored as `duckdb_time_struct`).

        * time: The time object, as obtained from a `DUCKDB_TYPE_TIME` column.
        * returns: The `duckdb_time_struct` with the decomposed elements.
        """
        return self._duckdb_from_time(time)

    fn duckdb_create_time_tz(self, micros: Int64, offset: Int32) -> duckdb_time_tz:
        """Create a `duckdb_time_tz` object from micros and a timezone offset.

        * micros: The microsecond component of the time.
        * offset: The timezone offset component of the time.
        * returns: The `duckdb_time_tz` element.
        """
        return self._duckdb_create_time_tz(micros, offset)

    fn duckdb_from_time_tz(self, micros: duckdb_time_tz) -> duckdb_time_tz_struct:
        """Decompose a TIME_TZ objects into micros and a timezone offset.

        Use `duckdb_from_time` to further decompose the micros into hour, minute, second and microsecond.

        * micros: The time object, as obtained from a `DUCKDB_TYPE_TIME_TZ` column.
        * out_micros: The microsecond component of the time.
        * out_offset: The timezone offset component of the time.
        """
        return self._duckdb_from_time_tz(micros)

    fn duckdb_to_time(self, time: duckdb_time_struct) -> duckdb_time:
        """Re-compose a `duckdb_time` from hour, minute, second and microsecond (`duckdb_time_struct`).

        * time: The hour, minute, second and microsecond in a `duckdb_time_struct`.
        * returns: The `duckdb_time` element.
        """
        return self._duckdb_to_time(time)

    fn duckdb_to_timestamp(self, ts: duckdb_timestamp_struct) -> duckdb_timestamp:
        """Re-compose a `duckdb_timestamp` from a duckdb_timestamp_struct.

        * ts: The de-composed elements in a `duckdb_timestamp_struct`.
        * returns: The `duckdb_timestamp` element.
        """
        return self._duckdb_to_timestamp(ts)

    fn duckdb_from_timestamp(self, timestamp: duckdb_timestamp) -> duckdb_timestamp_struct:
        """Decompose a `duckdb_timestamp` object into a `duckdb_timestamp_struct`.

        * ts: The ts object, as obtained from a `DUCKDB_TYPE_TIMESTAMP` column.
        * returns: The `duckdb_timestamp_struct` with the decomposed elements.
        """
        return self._duckdb_from_timestamp(timestamp)

    fn duckdb_is_finite_timestamp(self, timestamp: duckdb_timestamp) -> Bool:
        """Test a `duckdb_timestamp` to see if it is a finite value.

        * ts: The timestamp object, as obtained from a `DUCKDB_TYPE_TIMESTAMP` column.
        * returns: True if the timestamp is finite, false if it is ±infinity.
        """
        return self._duckdb_is_finite_timestamp(timestamp)

    fn duckdb_vector_get_column_type(self, vector: duckdb_vector) -> duckdb_logical_type:
        """
        Retrieves the column type of the specified vector.

        The result must be destroyed with `duckdb_destroy_logical_type`.

        * vector: The vector get the data from
        * returns: The type of the vector
        """
        return self._duckdb_vector_get_column_type(vector)

    fn duckdb_vector_get_data(self, vector: duckdb_vector) -> UnsafePointer[NoneType]:
        """
        Retrieves the data pointer of the vector.

        The data pointer can be used to read or write values from the vector.
        How to read or write values depends on the type of the vector.

        * vector: The vector to get the data from
        * returns: The data pointer
        """
        return self._duckdb_vector_get_data(vector)

    fn duckdb_vector_get_validity(self, vector: duckdb_vector) -> UnsafePointer[UInt64]:
        """
        Retrieves the validity mask pointer of the specified vector.

        If all values are valid, this function MIGHT return NULL!

        The validity mask is a bitset that signifies null-ness within the data chunk.
        It is a series of UInt64 values, where each UInt64 value contains validity for 64 tuples.
        The bit is set to 1 if the value is valid (i.e. not NULL) or 0 if the value is invalid (i.e. NULL).

        Validity of a specific value can be obtained like this:

        idx_t entry_idx = row_idx / 64;
        idx_t idx_in_entry = row_idx % 64;
        Bool is_valid = validity_mask[entry_idx] & (1 << idx_in_entry);

        Alternatively, the (slower) duckdb_validity_row_is_valid function can be used.

        * vector: The vector to get the data from
        * returns: The pointer to the validity mask, or NULL if no validity mask is present
        """
        return self._duckdb_vector_get_validity(vector)

    fn duckdb_vector_ensure_validity_writable(self, vector: duckdb_vector) -> NoneType:
        """
        Ensures the validity mask is writable by allocating it.

        After this function is called, `duckdb_vector_get_validity` will ALWAYS return non-NULL.
        This allows null values to be written to the vector, regardless of whether a validity mask was present before.

        * vector: The vector to alter
        """
        return self._duckdb_vector_ensure_validity_writable(vector)

    fn duckdb_vector_assign_string_element(
        self, vector: duckdb_vector, index: idx_t, str: c_char
    ) -> NoneType:
        """
        Assigns a string element in the vector at the specified location.

        * vector: The vector to alter
        * index: The row position in the vector to assign the string to
        * str: The null-terminated string
        """
        return self._duckdb_vector_assign_string_element(vector, index, str)

    fn duckdb_vector_assign_string_element_len(
        self, vector: duckdb_vector, index: idx_t, str: c_char, str_len: idx_t
    ) -> NoneType:
        """
        Assigns a string element in the vector at the specified location. You may also use this function to assign BLOBs.

        * vector: The vector to alter
        * index: The row position in the vector to assign the string to
        * str: The string
        * str_len: The length of the string (in bytes)
        """
        return self._duckdb_vector_assign_string_element_len(vector, index, str, str_len)

    fn duckdb_list_vector_get_child(self, vector: duckdb_vector) -> duckdb_vector:
        """
        Retrieves the child vector of a list vector.

        The resulting vector is valid as long as the parent vector is valid.

        * vector: The vector
        * returns: The child vector
        """
        return self._duckdb_list_vector_get_child(vector)

    fn duckdb_list_vector_get_size(self, vector: duckdb_vector) -> idx_t:
        """
        Returns the size of the child vector of the list.

        * vector: The vector
        * returns: The size of the child list
        """
        return self._duckdb_list_vector_get_size(vector)

    fn duckdb_list_vector_set_size(
        self, vector: duckdb_vector, size: idx_t
    ) -> duckdb_state:
        """
        Sets the total size of the underlying child-vector of a list vector.

        * vector: The list vector.
        * size: The size of the child list.
        * returns: The duckdb state. Returns DuckDBError if the vector is nullptr.
        """
        return self._duckdb_list_vector_set_size(vector, size)

    fn duckdb_list_vector_reserve(
        self, vector: duckdb_vector, required_capacity: idx_t
    ) -> duckdb_state:
        """
        Sets the total capacity of the underlying child-vector of a list.

        * vector: The list vector.
        * required_capacity: the total capacity to reserve.
        * return: The duckdb state. Returns DuckDBError if the vector is nullptr.
        """
        return self._duckdb_list_vector_reserve(vector, required_capacity)

    fn duckdb_struct_vector_get_child(
        self, vector: duckdb_vector, index: idx_t
    ) -> duckdb_vector:
        """
        Retrieves the child vector of a struct vector.

        The resulting vector is valid as long as the parent vector is valid.

        * vector: The vector
        * index: The child index
        * returns: The child vector
        """
        return self._duckdb_struct_vector_get_child(vector, index)

    fn duckdb_array_vector_get_child(self, vector: duckdb_vector) -> duckdb_vector:
        """
        Retrieves the child vector of a array vector.

        The resulting vector is valid as long as the parent vector is valid.
        The resulting vector has the size of the parent vector multiplied by the array size.

        * vector: The vector
        * returns: The child vector
        """
        return self._duckdb_array_vector_get_child(vector)

    # ===--------------------------------------------------------------------===
    # Validity Mask Functions
    # ===--------------------------------------------------------------------===

    fn duckdb_validity_row_is_valid(
        self, validity: UnsafePointer[UInt64], row: idx_t
    ) -> Bool:
        """
        Returns whether or not a row is valid (i.e. not NULL) in the given validity mask.

        * validity: The validity mask, as obtained through `duckdb_vector_get_validity`
        * row: The row index
        * returns: true if the row is valid, false otherwise
        """
        return self._duckdb_validity_row_is_valid(validity, row)

    fn duckdb_validity_set_row_validity(
        self, validity: UnsafePointer[UInt64], row: idx_t, valid: Bool
    ) -> NoneType:
        """
        In a validity mask, sets a specific row to either valid or invalid.

        Note that `duckdb_vector_ensure_validity_writable` should be called before calling `duckdb_vector_get_validity`,
        to ensure that there is a validity mask to write to.

        * validity: The validity mask, as obtained through `duckdb_vector_get_validity`.
        * row: The row index
        * valid: Whether or not to set the row to valid, or invalid
        """
        return self._duckdb_validity_set_row_validity(validity, row, valid)

    fn duckdb_validity_set_row_invalid(
        self, validity: UnsafePointer[UInt64], row: idx_t
    ) -> NoneType:
        """
        In a validity mask, sets a specific row to invalid.

        Equivalent to `duckdb_validity_set_row_validity` with valid set to false.

        * validity: The validity mask
        * row: The row index
        """
        return self._duckdb_validity_set_row_invalid(validity, row)

    fn duckdb_validity_set_row_valid(
        self, validity: UnsafePointer[UInt64], row: idx_t
    ) -> NoneType:
        """
        In a validity mask, sets a specific row to valid.

        Equivalent to `duckdb_validity_set_row_validity` with valid set to true.

        * validity: The validity mask
        * row: The row index
        """
        return self._duckdb_validity_set_row_valid(validity, row)

    # ===--------------------------------------------------------------------===#
    # Logical Type Interface
    # ===--------------------------------------------------------------------===#

    fn duckdb_create_logical_type(self, type_id: duckdb_type) -> duckdb_logical_type:
        """Creates a `duckdb_logical_type` from a standard primitive type.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        This should not be used with `DUCKDB_TYPE_DECIMAL`.

        * type: The primitive type to create.
        * returns: The logical type.
        """
        return self._duckdb_create_logical_type(type_id)

    fn duckdb_create_list_type(self, type: duckdb_logical_type) -> duckdb_logical_type:
        """Creates a list type from its child type.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        * type: The child type of list type to create.
        * returns: The logical type.
        """
        return self._duckdb_create_list_type(type)

    fn duckdb_create_array_type(
        self, type: duckdb_logical_type, array_size: idx_t
    ) -> duckdb_logical_type:
        """Creates an array type from its child type.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        * type: The child type of array type to create.
        * array_size: The number of elements in the array.
        * returns: The logical type.
        """
        return self._duckdb_create_array_type(type, array_size)

    fn duckdb_create_map_type(
        self, key_type: duckdb_logical_type, value_type: duckdb_logical_type
    ) -> duckdb_logical_type:
        """Creates a map type from its key type and value type.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        * type: The key type and value type of map type to create.
        * returns: The logical type.
        """
        return self._duckdb_create_map_type(key_type, value_type)

    fn duckdb_create_union_type(
        self,
        member_types: UnsafePointer[duckdb_logical_type],
        member_names: UnsafePointer[UnsafePointer[c_char]],
        member_count: idx_t,
    ) -> duckdb_logical_type:
        """Creates a UNION type from the passed types array.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        * types: The array of types that the union should consist of.
        * type_amount: The size of the types array.
        * returns: The logical type.
        """
        return self._duckdb_create_union_type(member_types, member_names, member_count)

    fn duckdb_create_struct_type(
        self,
        member_types: UnsafePointer[duckdb_logical_type],
        member_names: UnsafePointer[UnsafePointer[c_char]],
        member_count: idx_t,
    ) -> duckdb_logical_type:
        """Creates a STRUCT type from the passed member name and type arrays.
        The resulting type should be destroyed with `duckdb_destroy_logical_type`.

        * member_types: The array of types that the struct should consist of.
        * member_names: The array of names that the struct should consist of.
        * member_count: The number of members that were specified for both arrays.
        * returns: The logical type.
        """
        return self._duckdb_create_struct_type(member_types, member_names, member_count)

    # fn duckdb_create_enum_type TODO
    # fn duckdb_create_decimal_type TODO

    fn duckdb_get_type_id(self, type: duckdb_logical_type) -> duckdb_type:
        """Retrieves the enum type class of a `duckdb_logical_type`.

        * type: The logical type object
        * returns: The type id
        """
        return self._duckdb_get_type_id(type)

    fn duckdb_list_type_child_type(self, type: duckdb_logical_type) -> duckdb_logical_type:
        """Retrieves the child type of the given list type.

        The result must be freed with `duckdb_destroy_logical_type`.

        * type: The logical type object
        * returns: The child type of the list type. Must be destroyed with `duckdb_destroy_logical_type`.
        """
        return self._duckdb_list_type_child_type(type)

    fn duckdb_array_type_child_type(self, type: duckdb_logical_type) -> duckdb_logical_type:
        """Retrieves the child type of the given array type.

        The result must be freed with `duckdb_destroy_logical_type`.

        * type: The logical type object
        * returns: The child type of the array type. Must be destroyed with `duckdb_destroy_logical_type`.
        """
        return self._duckdb_array_type_child_type(type)

    fn duckdb_map_type_key_type(self, type: duckdb_logical_type) -> duckdb_logical_type:
        """Retrieves the key type of the given map type.

        The result must be freed with `duckdb_destroy_logical_type`.

        * type: The logical type object
        * returns: The key type of the map type. Must be destroyed with `duckdb_destroy_logical_type`.
        """
        return self._duckdb_map_type_key_type(type)

    fn duckdb_map_type_value_type(self, type: duckdb_logical_type) -> duckdb_logical_type:
        """Retrieves the value type of the given map type.

        The result must be freed with `duckdb_destroy_logical_type`.

        * type: The logical type object
        * returns: The value type of the map type. Must be destroyed with `duckdb_destroy_logical_type`.
        """
        return self._duckdb_map_type_value_type(type)

    fn duckdb_destroy_logical_type(self, type: UnsafePointer[duckdb_logical_type]) -> None:
        """Destroys the logical type and de-allocates all memory allocated for that type.

        * type: The logical type to destroy.
        """
        return self._duckdb_destroy_logical_type(type)

    fn duckdb_execution_is_finished(self, con: duckdb_connection) -> Bool:
        return self._duckdb_execution_is_finished(con)

    fn duckdb_fetch_chunk(self, result: duckdb_result) -> duckdb_data_chunk:
        """
        Fetches a data chunk from a duckdb_result. This function should be called repeatedly until the result is exhausted.

        The result must be destroyed with `duckdb_destroy_data_chunk`.

        It is not known beforehand how many chunks will be returned by this result.

        * result: The result object to fetch the data chunk from.
        * returns: The resulting data chunk. Returns `NULL` if the result has an error.

        NOTE: Mojo cannot currently pass large structs by value correctly over the C ABI. We therefore call a helper
        wrapper that accepts a pointer to duckdb_result instead of passing it by value directly.
        """
        return self._duckdb_fetch_chunk_ptr(UnsafePointer(to=result))

alias _duckdb_open = _dylib_function["duckdb_open", 
    fn (UnsafePointer[c_char], UnsafePointer[duckdb_database]) -> UInt32
]

alias _duckdb_close = _dylib_function["duckdb_close",
    fn (UnsafePointer[duckdb_database]) -> NoneType
]

alias _duckdb_connect = _dylib_function[
    "duckdb_connect",
    fn (duckdb_database, UnsafePointer[duckdb_connection]) -> UInt32
]

alias _duckdb_disconnect = _dylib_function["duckdb_disconnect",
    fn (UnsafePointer[duckdb_connection]) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Query Execution
# ===--------------------------------------------------------------------===#

alias _duckdb_query = _dylib_function["duckdb_query",
    fn (duckdb_connection, UnsafePointer[c_char], UnsafePointer[duckdb_result]) -> UInt32
]

alias _duckdb_destroy_result = _dylib_function["duckdb_destroy_result",
    fn (UnsafePointer[duckdb_result]) -> NoneType
]

alias _duckdb_column_name = _dylib_function["duckdb_column_name",
    fn (UnsafePointer[duckdb_result], idx_t) -> UnsafePointer[c_char]
]

alias _duckdb_column_type = _dylib_function["duckdb_column_type",
    fn (UnsafePointer[duckdb_result], idx_t) -> duckdb_type
]

alias _duckdb_result_statement_type = _dylib_function["duckdb_result_statement_type",
    fn (duckdb_result) -> duckdb_statement_type
]

alias _duckdb_column_logical_type = _dylib_function["duckdb_column_logical_type",
    fn (UnsafePointer[duckdb_result], idx_t) -> duckdb_logical_type
]

alias _duckdb_column_count = _dylib_function["duckdb_column_count",
    fn (UnsafePointer[duckdb_result]) -> idx_t
]

alias _duckdb_rows_changed = _dylib_function["duckdb_rows_changed",
    fn (UnsafePointer[duckdb_result]) -> idx_t
]

alias _duckdb_result_error = _dylib_function["duckdb_result_error",
    fn (UnsafePointer[duckdb_result]) -> UnsafePointer[c_char]
]

alias _duckdb_row_count = _dylib_function["duckdb_row_count",
    fn (UnsafePointer[duckdb_result]) -> idx_t
]

# ===--------------------------------------------------------------------===#
# Result Functions
# ===--------------------------------------------------------------------===#

alias _duckdb_result_return_type = _dylib_function["duckdb_result_return_type",
    fn (duckdb_result) -> duckdb_result_type
]

# ===--------------------------------------------------------------------===#
# Helpers
# ===--------------------------------------------------------------------===#

alias _duckdb_vector_size = _dylib_function["duckdb_vector_size", fn () -> idx_t]

# ===--------------------------------------------------------------------===#
# Data Chunk Interface
# ===--------------------------------------------------------------------===#

alias _duckdb_create_data_chunk = _dylib_function["duckdb_create_data_chunk",
    fn (UnsafePointer[duckdb_logical_type], idx_t) -> duckdb_data_chunk
]

alias _duckdb_destroy_data_chunk = _dylib_function["duckdb_destroy_data_chunk",
    fn (UnsafePointer[duckdb_data_chunk]) -> NoneType
]

alias _duckdb_data_chunk_reset = _dylib_function["duckdb_data_chunk_reset",
    fn (duckdb_data_chunk) -> NoneType
]

alias _duckdb_data_chunk_get_column_count = _dylib_function["duckdb_data_chunk_get_column_count",
    fn (duckdb_data_chunk) -> idx_t
]

alias _duckdb_data_chunk_get_vector = _dylib_function["duckdb_data_chunk_get_vector",
    fn (duckdb_data_chunk, idx_t) -> duckdb_vector
]

alias _duckdb_data_chunk_get_size = _dylib_function["duckdb_data_chunk_get_size",
    fn (duckdb_data_chunk) -> idx_t
]

alias _duckdb_data_chunk_set_size = _dylib_function["duckdb_data_chunk_set_size",
    fn (duckdb_data_chunk, idx_t) -> NoneType
]

alias _duckdb_from_date = _dylib_function["duckdb_from_date",
    fn (duckdb_date) -> duckdb_date_struct
]

alias _duckdb_to_date = _dylib_function["duckdb_to_date",
    fn (duckdb_date_struct) -> duckdb_date
]

alias _duckdb_is_finite_date = _dylib_function["duckdb_is_finite_date",
    fn (duckdb_date) -> Bool
]

alias _duckdb_from_time = _dylib_function["duckdb_from_time",
    fn (duckdb_time) -> duckdb_time_struct
]

alias _duckdb_create_time_tz = _dylib_function["duckdb_create_time_tz",
    fn (Int64, Int32) -> duckdb_time_tz
]

alias _duckdb_from_time_tz = _dylib_function["duckdb_from_time_tz",
    fn (duckdb_time_tz) -> duckdb_time_tz_struct
]

alias _duckdb_to_time = _dylib_function["duckdb_to_time",
    fn (duckdb_time_struct) -> duckdb_time
]

alias _duckdb_to_timestamp = _dylib_function["duckdb_to_timestamp",
    fn (duckdb_timestamp_struct) -> duckdb_timestamp
]

alias _duckdb_from_timestamp = _dylib_function["duckdb_from_timestamp",
    fn (duckdb_timestamp) -> duckdb_timestamp_struct
]

alias _duckdb_is_finite_timestamp = _dylib_function["duckdb_is_finite_timestamp",
    fn (duckdb_timestamp) -> Bool
]

alias _duckdb_vector_get_column_type = _dylib_function["duckdb_vector_get_column_type",
    fn (duckdb_vector) -> duckdb_logical_type
]

alias _duckdb_vector_get_data = _dylib_function["duckdb_vector_get_data",
    fn (duckdb_vector) -> UnsafePointer[NoneType]
]

alias _duckdb_vector_get_validity = _dylib_function["duckdb_vector_get_validity",
    fn (duckdb_vector) -> UnsafePointer[UInt64]
]

alias _duckdb_vector_ensure_validity_writable = _dylib_function["duckdb_vector_ensure_validity_writable",
    fn (duckdb_vector) -> NoneType
]

alias _duckdb_vector_assign_string_element = _dylib_function["duckdb_vector_assign_string_element",
    fn (duckdb_vector, idx_t, c_char) -> NoneType
]

alias _duckdb_vector_assign_string_element_len = _dylib_function["duckdb_vector_assign_string_element_len",
    fn (duckdb_vector, idx_t, c_char, idx_t) -> NoneType
]

alias _duckdb_list_vector_get_child = _dylib_function["duckdb_list_vector_get_child",
    fn (duckdb_vector) -> duckdb_vector
]

alias _duckdb_list_vector_get_size = _dylib_function["duckdb_list_vector_get_size",
    fn (duckdb_vector) -> idx_t
]

alias _duckdb_list_vector_set_size = _dylib_function["duckdb_list_vector_set_size",
    fn (duckdb_vector, idx_t) -> duckdb_state
]

alias _duckdb_list_vector_reserve = _dylib_function["duckdb_list_vector_reserve",
    fn (duckdb_vector, idx_t) -> duckdb_state
]

alias _duckdb_struct_vector_get_child = _dylib_function["duckdb_struct_vector_get_child",
    fn (duckdb_vector, idx_t) -> duckdb_vector
]

alias _duckdb_array_vector_get_child = _dylib_function["duckdb_array_vector_get_child",
    fn (duckdb_vector) -> duckdb_vector
]

# ===--------------------------------------------------------------------===
# Validity Mask Functions
# ===--------------------------------------------------------------------===

alias _duckdb_validity_row_is_valid = _dylib_function["duckdb_validity_row_is_valid",
    fn (UnsafePointer[UInt64], idx_t) -> Bool
]

alias _duckdb_validity_set_row_validity = _dylib_function["duckdb_validity_set_row_validity",
    fn (UnsafePointer[UInt64], idx_t, Bool) -> NoneType
]

alias _duckdb_validity_set_row_invalid = _dylib_function["duckdb_validity_set_row_invalid",
    fn (UnsafePointer[UInt64], idx_t) -> NoneType
]

alias _duckdb_validity_set_row_valid = _dylib_function["duckdb_validity_set_row_valid",
    fn (UnsafePointer[UInt64], idx_t) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Logical Type Interface
# ===--------------------------------------------------------------------===#

alias _duckdb_create_logical_type = _dylib_function["duckdb_create_logical_type",
    fn (duckdb_type) -> duckdb_logical_type
]

alias _duckdb_create_list_type = _dylib_function["duckdb_create_list_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_create_array_type = _dylib_function["duckdb_create_array_type",
    fn (duckdb_logical_type, idx_t) -> duckdb_logical_type
]

alias _duckdb_create_map_type = _dylib_function["duckdb_create_map_type",
    fn (duckdb_logical_type, duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_create_union_type = _dylib_function["duckdb_create_union_type",
    fn (UnsafePointer[duckdb_logical_type], UnsafePointer[UnsafePointer[c_char]], idx_t) -> duckdb_logical_type
]

alias _duckdb_create_struct_type = _dylib_function["duckdb_create_struct_type",
    fn (UnsafePointer[duckdb_logical_type], UnsafePointer[UnsafePointer[c_char]], idx_t) -> duckdb_logical_type
]

# fn duckdb_create_enum_type TODO
# fn duckdb_create_decimal_type TODO

alias _duckdb_get_type_id = _dylib_function["duckdb_get_type_id",
    fn (duckdb_logical_type) -> duckdb_type
]

alias _duckdb_list_type_child_type = _dylib_function["duckdb_list_type_child_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_array_type_child_type = _dylib_function["duckdb_array_type_child_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_map_type_key_type = _dylib_function["duckdb_map_type_key_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_map_type_value_type = _dylib_function["duckdb_map_type_value_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

alias _duckdb_destroy_logical_type = _dylib_function["duckdb_destroy_logical_type",
    fn (UnsafePointer[duckdb_logical_type]) -> None
]

alias _duckdb_execution_is_finished = _dylib_function["duckdb_execution_is_finished",
    fn (duckdb_connection) -> Bool
]

alias _duckdb_fetch_chunk_ptr = _dylib_helpers_function["duckdb_fetch_chunk_ptr",
    fn (UnsafePointer[duckdb_result]) -> duckdb_data_chunk
]
