from ffi import external_call, c_char
from utils import StaticTuple
from collections import InlineArray
from duckdb.duckdb_type import *
from sys.info import CompilationTarget
from os import abort
from pathlib import Path
from ffi import _find_dylib
from ffi import _get_dylib_function as _ffi_get_dylib_function
from ffi import _Global, OwnedDLHandle

# ===-----------------------------------------------------------------------===#
# FFI definitions for the DuckDB C API ported to Mojo.
# 
# Derived from
# https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h
# 
# Once Mojo is able to generate these bindings automatically, we should switch
# to ease maintenance.
# ===-----------------------------------------------------------------------===#


# ===--------------------------------------------------------------------===#
# Enums
# ===--------------------------------------------------------------------===#

#! An enum over DuckDB's internal types.
comptime duckdb_type = Int32
comptime DUCKDB_TYPE_INVALID = 0
# bool
comptime DUCKDB_TYPE_BOOLEAN = 1
# int8_t
comptime DUCKDB_TYPE_TINYINT = 2
# int16_t
comptime DUCKDB_TYPE_SMALLINT = 3
# int32_t
comptime DUCKDB_TYPE_INTEGER = 4
# int64_t
comptime DUCKDB_TYPE_BIGINT = 5
# uint8_t
comptime DUCKDB_TYPE_UTINYINT = 6
# uint16_t
comptime DUCKDB_TYPE_USMALLINT = 7
# uint32_t
comptime DUCKDB_TYPE_UINTEGER = 8
# uint64_t
comptime DUCKDB_TYPE_UBIGINT = 9
# float
comptime DUCKDB_TYPE_FLOAT = 10
# double
comptime DUCKDB_TYPE_DOUBLE = 11
# duckdb_timestamp, in microseconds
comptime DUCKDB_TYPE_TIMESTAMP = 12
# duckdb_date
comptime DUCKDB_TYPE_DATE = 13
# duckdb_time
comptime DUCKDB_TYPE_TIME = 14
# duckdb_interval
comptime DUCKDB_TYPE_INTERVAL = 15
# duckdb_hugeint
comptime DUCKDB_TYPE_HUGEINT = 16
# duckdb_uhugeint
comptime DUCKDB_TYPE_UHUGEINT = 32
# const char*
comptime DUCKDB_TYPE_VARCHAR = 17
# duckdb_blob
comptime DUCKDB_TYPE_BLOB = 18
# decimal
comptime DUCKDB_TYPE_DECIMAL = 19
# duckdb_timestamp, in seconds
comptime DUCKDB_TYPE_TIMESTAMP_S = 20
# duckdb_timestamp, in milliseconds
comptime DUCKDB_TYPE_TIMESTAMP_MS = 21
# duckdb_timestamp, in nanoseconds
comptime DUCKDB_TYPE_TIMESTAMP_NS = 22
# enum type, only useful as logical type
comptime DUCKDB_TYPE_ENUM = 23
# list type, only useful as logical type
comptime DUCKDB_TYPE_LIST = 24
# struct type, only useful as logical type
comptime DUCKDB_TYPE_STRUCT = 25
# map type, only useful as logical type
comptime DUCKDB_TYPE_MAP = 26
# duckdb_array, only useful as logical type
comptime DUCKDB_TYPE_ARRAY = 33
# duckdb_hugeint
comptime DUCKDB_TYPE_UUID = 27
# union type, only useful as logical type
comptime DUCKDB_TYPE_UNION = 28
# duckdb_bit
comptime DUCKDB_TYPE_BIT = 29
# duckdb_time_tz
comptime DUCKDB_TYPE_TIME_TZ = 30
# duckdb_timestamp
comptime DUCKDB_TYPE_TIMESTAMP_TZ = 31

#! An enum over the returned state of different functions.
comptime duckdb_state = Int32
comptime DuckDBSuccess = 0
comptime DuckDBError = 1

#! An enum over the pending state of a pending query result.
comptime duckdb_pending_state = Int32
comptime DUCKDB_PENDING_RESULT_READY = 0
comptime DUCKDB_PENDING_RESULT_NOT_READY = 1
comptime DUCKDB_PENDING_ERROR = 2
comptime DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3

#! An enum over DuckDB's different result types.
comptime duckdb_result_type = Int32
comptime DUCKDB_RESULT_TYPE_INVALID = 0
comptime DUCKDB_RESULT_TYPE_CHANGED_ROWS = 1
comptime DUCKDB_RESULT_TYPE_NOTHING = 2
comptime DUCKDB_RESULT_TYPE_QUERY_RESULT = 3

#! An enum over DuckDB's different statement types.
comptime duckdb_statement_type = Int32
comptime DUCKDB_STATEMENT_TYPE_INVALID = 0
comptime DUCKDB_STATEMENT_TYPE_SELECT = 1
comptime DUCKDB_STATEMENT_TYPE_INSERT = 2
comptime DUCKDB_STATEMENT_TYPE_UPDATE = 3
comptime DUCKDB_STATEMENT_TYPE_EXPLAIN = 4
comptime DUCKDB_STATEMENT_TYPE_DELETE = 5
comptime DUCKDB_STATEMENT_TYPE_PREPARE = 6
comptime DUCKDB_STATEMENT_TYPE_CREATE = 7
comptime DUCKDB_STATEMENT_TYPE_EXECUTE = 8
comptime DUCKDB_STATEMENT_TYPE_ALTER = 9
comptime DUCKDB_STATEMENT_TYPE_TRANSACTION = 10
comptime DUCKDB_STATEMENT_TYPE_COPY = 11
comptime DUCKDB_STATEMENT_TYPE_ANALYZE = 12
comptime DUCKDB_STATEMENT_TYPE_VARIABLE_SET = 13
comptime DUCKDB_STATEMENT_TYPE_CREATE_FUNC = 14
comptime DUCKDB_STATEMENT_TYPE_DROP = 15
comptime DUCKDB_STATEMENT_TYPE_EXPORT = 16
comptime DUCKDB_STATEMENT_TYPE_PRAGMA = 17
comptime DUCKDB_STATEMENT_TYPE_VACUUM = 18
comptime DUCKDB_STATEMENT_TYPE_CALL = 19
comptime DUCKDB_STATEMENT_TYPE_SET = 20
comptime DUCKDB_STATEMENT_TYPE_LOAD = 21
comptime DUCKDB_STATEMENT_TYPE_RELATION = 22
comptime DUCKDB_STATEMENT_TYPE_EXTENSION = 23
comptime DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN = 24
comptime DUCKDB_STATEMENT_TYPE_ATTACH = 25
comptime DUCKDB_STATEMENT_TYPE_DETACH = 26
comptime DUCKDB_STATEMENT_TYPE_MULTI = 27

#! An enum over DuckDB's different error types.
comptime duckdb_error_type = Int32
comptime DUCKDB_ERROR_INVALID = 0
comptime DUCKDB_ERROR_OUT_OF_RANGE = 1
comptime DUCKDB_ERROR_CONVERSION = 2
comptime DUCKDB_ERROR_UNKNOWN_TYPE = 3
comptime DUCKDB_ERROR_DECIMAL = 4
comptime DUCKDB_ERROR_MISMATCH_TYPE = 5
comptime DUCKDB_ERROR_DIVIDE_BY_ZERO = 6
comptime DUCKDB_ERROR_OBJECT_SIZE = 7
comptime DUCKDB_ERROR_INVALID_TYPE = 8
comptime DUCKDB_ERROR_SERIALIZATION = 9
comptime DUCKDB_ERROR_TRANSACTION = 10
comptime DUCKDB_ERROR_NOT_IMPLEMENTED = 11
comptime DUCKDB_ERROR_EXPRESSION = 12
comptime DUCKDB_ERROR_CATALOG = 13
comptime DUCKDB_ERROR_PARSER = 14
comptime DUCKDB_ERROR_PLANNER = 15
comptime DUCKDB_ERROR_SCHEDULER = 16
comptime DUCKDB_ERROR_EXECUTOR = 17
comptime DUCKDB_ERROR_CONSTRAINT = 18
comptime DUCKDB_ERROR_INDEX = 19
comptime DUCKDB_ERROR_STAT = 20
comptime DUCKDB_ERROR_CONNECTION = 21
comptime DUCKDB_ERROR_SYNTAX = 22
comptime DUCKDB_ERROR_SETTINGS = 23
comptime DUCKDB_ERROR_BINDER = 24
comptime DUCKDB_ERROR_NETWORK = 25
comptime DUCKDB_ERROR_OPTIMIZER = 26
comptime DUCKDB_ERROR_NULL_POINTER = 27
comptime DUCKDB_ERROR_IO = 28
comptime DUCKDB_ERROR_INTERRUPT = 29
comptime DUCKDB_ERROR_FATAL = 30
comptime DUCKDB_ERROR_INTERNAL = 31
comptime DUCKDB_ERROR_INVALID_INPUT = 32
comptime DUCKDB_ERROR_OUT_OF_MEMORY = 33
comptime DUCKDB_ERROR_PERMISSION = 34
comptime DUCKDB_ERROR_PARAMETER_NOT_RESOLVED = 35
comptime DUCKDB_ERROR_PARAMETER_NOT_ALLOWED = 36
comptime DUCKDB_ERROR_DEPENDENCY = 37
comptime DUCKDB_ERROR_HTTP = 38
comptime DUCKDB_ERROR_MISSING_EXTENSION = 39
comptime DUCKDB_ERROR_AUTOLOAD = 40
comptime DUCKDB_ERROR_SEQUENCE = 41
comptime DUCKDB_INVALID_CONFIGURATION = 42


# ===--------------------------------------------------------------------===#
# General type definitions
# ===--------------------------------------------------------------------===#

#! DuckDB's index type.
comptime idx_t = UInt64

#! The callback that will be called to destroy data, e.g.,
#! bind data (if any), init data (if any), extra data for replacement scans (if any)
comptime duckdb_delete_callback_t = fn (UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType

#! The callback that will be called to copy bind data.
comptime duckdb_copy_callback_t = fn (UnsafePointer[NoneType, MutAnyOrigin]) -> UnsafePointer[NoneType, MutAnyOrigin]

# ===--------------------------------------------------------------------===#
# Types (no explicit freeing)
# ===--------------------------------------------------------------------===#

#! Days are stored as days since 1970-01-01
#! Use the duckdb_from_date/duckdb_to_date function to extract individual information
comptime duckdb_date = Date

@fieldwise_init
struct duckdb_date_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
    var year: Int32
    var month: Int8
    var day: Int8

#! Time is stored as microseconds since 00:00:00
#! Use the duckdb_from_time/duckdb_to_time function to extract individual information
comptime duckdb_time = Time

@fieldwise_init
struct duckdb_time_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
    var hour: Int8
    var min: Int8
    var sec: Int8
    var micros: Int32

#! TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset
@fieldwise_init
struct duckdb_time_tz(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
    var bits: UInt64


@fieldwise_init
struct duckdb_time_tz_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
    var time: duckdb_time_struct
    var offset: Int32


#! Timestamps are stored as microseconds since 1970-01-01
#! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information
comptime duckdb_timestamp = Timestamp


@fieldwise_init
struct duckdb_timestamp_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
    var date: duckdb_date_struct
    var time: duckdb_time_struct

# TODO hack to pass struct by value until https://github.com/modular/modular/issues/3144 is fixed
# Currently it only works with <= 2 struct values
struct duckdb_interval(TrivialRegisterPassable):
    var months_days: Int64
    var micros: Int64
# comptime duckdb_interval = Interval

#! Hugeints are composed of a (lower, upper) component
#! The value of the hugeint is upper * 2^64 + lower
#! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
comptime duckdb_hugeint = Int128
comptime duckdb_uhugeint = UInt128
#! Decimals are composed of a width and a scale, and are stored in a hugeint
comptime duckdb_decimal = Decimal


@fieldwise_init
#! A type holding information about the query execution progress
struct duckdb_query_progress_type(TrivialRegisterPassable, ImplicitlyCopyable, Movable):
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
    var ptr: UnsafePointer[c_char, MutExternalOrigin]


@fieldwise_init
struct duckdb_string_t_inlined(Copyable, Movable):
    var length: UInt32
    var inlined: InlineArray[c_char, 12]


#! DuckDB's LISTs are composed of a 'parent' vector holding metadata of each list,
#! and a child vector holding the entries of the lists.
#! The `duckdb_list_entry` struct contains the internal representation of a LIST metadata entry.
#! A metadata entry contains the length of the list, and its offset in the child vector.
@fieldwise_init
struct duckdb_list_entry(ImplicitlyCopyable, Movable):
    var offset: UInt64
    var length: UInt64


#! A column consists of a pointer to its internal data. Don't operate on this type directly.
#! Instead, use functions such as `duckdb_column_data`, `duckdb_nullmask_data`,
#! `duckdb_column_type`, and `duckdb_column_name`.
@fieldwise_init
struct duckdb_column(Copyable, Movable):
    var __deprecated_data: UnsafePointer[NoneType, MutExternalOrigin]
    var __deprecated_nullmask: UnsafePointer[Bool, MutExternalOrigin]
    var __deprecated_type: Int32  # actually a duckdb_type enum
    var __deprecated_name: UnsafePointer[c_char, ImmutExternalOrigin]
    var internal_data: UnsafePointer[NoneType, MutExternalOrigin]

    fn __init__(out self):
        self.__deprecated_data = UnsafePointer[NoneType, MutExternalOrigin]()
        self.__deprecated_nullmask = UnsafePointer[Bool, MutExternalOrigin]()
        self.__deprecated_type = 0
        self.__deprecated_name = UnsafePointer[c_char, ImmutExternalOrigin]()
        self.internal_data = UnsafePointer[NoneType, MutExternalOrigin]()


#! 1. A standalone vector that must be destroyed, or
#! 2. A vector to a column in a data chunk that lives as long as the data chunk lives.
struct _duckdb_vector:
    var __vctr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_vector = UnsafePointer[_duckdb_vector, MutExternalOrigin]

#! A selection vector is a vector of indices, which usually refer to values in a vector.
#! Can be used to slice vectors, changing their length and the order of their entries.
#! Standalone selection vectors must be destroyed.
struct _duckdb_selection_vector:
    var __sel: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_selection_vector = UnsafePointer[_duckdb_selection_vector, MutExternalOrigin]

# ===--------------------------------------------------------------------===#
# Types (explicit freeing/destroying)
# ===--------------------------------------------------------------------===#

struct duckdb_string:
    var data: UnsafePointer[c_char, MutExternalOrigin]
    var size: idx_t

struct duckdb_blob:
    var data: UnsafePointer[NoneType, MutExternalOrigin]
    var size: idx_t

# ===--------------------------------------------------------------------===#
# Function types
# ===--------------------------------------------------------------------===#

#! Additional function info.
#! When setting this info, it is necessary to pass a destroy-callback function.
struct _duckdb_function_info:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_function_info = UnsafePointer[_duckdb_function_info, MutExternalOrigin]

#! The bind info of a function.
#! When setting this info, it is necessary to pass a destroy-callback function.
struct _duckdb_bind_info:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_bind_info = UnsafePointer[_duckdb_bind_info, MutExternalOrigin]

#! An expression.
struct _duckdb_expression:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_expression = UnsafePointer[_duckdb_expression, MutExternalOrigin]

# ===--------------------------------------------------------------------===#
# Scalar function types
# ===--------------------------------------------------------------------===#

#! A scalar function. Must be destroyed with `duckdb_destroy_scalar_function`.
struct _duckdb_scalar_function:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_scalar_function = UnsafePointer[_duckdb_scalar_function, MutExternalOrigin]

#! A scalar function set. Must be destroyed with `duckdb_destroy_scalar_function_set`.
struct _duckdb_scalar_function_set:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_scalar_function_set = UnsafePointer[_duckdb_scalar_function_set, MutExternalOrigin]

#! The bind function of the scalar function.
comptime duckdb_scalar_function_bind_t = fn (duckdb_bind_info) -> NoneType

#! The main function of the scalar function.
comptime duckdb_scalar_function_t = fn (
    duckdb_function_info, duckdb_data_chunk, duckdb_vector
) -> NoneType

# ===--------------------------------------------------------------------===#
# Aggregate function types
# ===--------------------------------------------------------------------===#

#! An aggregate function. Must be destroyed with `duckdb_destroy_aggregate_function`.
struct _duckdb_aggregate_function:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_aggregate_function = UnsafePointer[_duckdb_aggregate_function, MutExternalOrigin]

#! A aggregate function set. Must be destroyed with `duckdb_destroy_aggregate_function_set`.
struct _duckdb_aggregate_function_set:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_aggregate_function_set = UnsafePointer[_duckdb_aggregate_function_set, MutExternalOrigin]

#! The state of an aggregate function.
struct _duckdb_aggregate_state:
    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_aggregate_state = UnsafePointer[_duckdb_aggregate_state, MutExternalOrigin]

#! A function to return the aggregate state's size.
comptime duckdb_aggregate_state_size = fn (duckdb_function_info) -> idx_t

#! A function to initialize an aggregate state.
comptime duckdb_aggregate_init_t = fn (duckdb_function_info, duckdb_aggregate_state) -> NoneType

#! An optional function to destroy an aggregate state.
comptime duckdb_aggregate_destroy_t = fn (UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], idx_t) -> NoneType

#! A function to update a set of aggregate states with new values.
comptime duckdb_aggregate_update_t = fn (
    duckdb_function_info, duckdb_data_chunk, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin]
) -> NoneType

#! A function to combine aggregate states.
comptime duckdb_aggregate_combine_t = fn (
    duckdb_function_info, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], 
    UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], idx_t
) -> NoneType

#! A function to finalize aggregate states into a result vector.
comptime duckdb_aggregate_finalize_t = fn (
    duckdb_function_info, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], 
    duckdb_vector, idx_t, idx_t
) -> NoneType


@fieldwise_init
struct duckdb_result(ImplicitlyCopyable & Movable):
    var __deprecated_column_count: idx_t
    var __deprecated_row_count: idx_t
    var __deprecated_rows_changed: idx_t
    var __deprecated_columns: UnsafePointer[duckdb_column, MutExternalOrigin]
    var __deprecated_error_message: UnsafePointer[c_char, ImmutExternalOrigin]
    var internal_data: UnsafePointer[NoneType, MutExternalOrigin]

    fn __init__(out self):
        self.__deprecated_column_count = 0
        self.__deprecated_row_count = 0
        self.__deprecated_rows_changed = 0
        self.__deprecated_columns = UnsafePointer[duckdb_column, MutExternalOrigin]()
        self.__deprecated_error_message = UnsafePointer[c_char, ImmutExternalOrigin]()
        self.internal_data = UnsafePointer[NoneType, MutExternalOrigin]()


struct _duckdb_database:
    var __db: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_database = UnsafePointer[_duckdb_database, MutExternalOrigin]


struct _duckdb_connection:
    var __conn: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_connection = UnsafePointer[_duckdb_connection, ImmutExternalOrigin]


struct _duckdb_prepared_statement:
    var __prep: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_prepared_statement = UnsafePointer[_duckdb_prepared_statement, MutExternalOrigin]


struct _duckdb_extracted_statements:
    var __extrac: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_extracted_statements = UnsafePointer[_duckdb_extracted_statements, MutExternalOrigin]


struct _duckdb_pending_result:
    var __pend: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_pending_result = UnsafePointer[_duckdb_pending_result, MutExternalOrigin]


struct _duckdb_appender:
    var __appn: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_appender = UnsafePointer[_duckdb_appender, MutExternalOrigin]


struct _duckdb_config:
    var __cnfg: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_config = UnsafePointer[_duckdb_config, MutExternalOrigin]


struct _duckdb_logical_type:
    var __lglt: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_logical_type = UnsafePointer[_duckdb_logical_type, MutExternalOrigin]


struct _duckdb_data_chunk:
    var __dtck: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_data_chunk = UnsafePointer[_duckdb_data_chunk, MutExternalOrigin]


struct _duckdb_value:
    var __val: UnsafePointer[NoneType, MutExternalOrigin]
comptime duckdb_value = UnsafePointer[_duckdb_value, MutExternalOrigin]


# ===-----------------------------------------------------------------------===#
# Library Load
# ===-----------------------------------------------------------------------===#

comptime DUCKDB_LIBRARY_PATHS: List[Path] = [
    "libduckdb.so",
    "libduckdb.dylib",
]

comptime DUCKDB_LIBRARY = _Global["DUCKDB_LIBRARY", _init_dylib]

fn _init_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb"](materialize[DUCKDB_LIBRARY_PATHS]())


@always_inline
fn _get_dylib_function[
    func_name: StaticString, result_type: __TypeOfAllTypes
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_LIBRARY(),
        func_name,
        result_type,
    ]()


struct _dylib_function[fn_name: StaticString, type: __TypeOfAllTypes](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    fn load() raises -> Self.type:
        return _get_dylib_function[Self.fn_name, Self.type]()

comptime DUCKDB_HELPERS_PATHS: List[Path] = [
    "libduckdb_mojo_helpers.so",
    "libduckdb_mojo_helpers.dylib",
]

comptime DUCKDB_HELPERS_LIBRARY = _Global["DUCKDB_HELPERS_LIBRARY", _init_helper_dylib]

fn _init_helper_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb_mojo_helpers"](materialize[DUCKDB_HELPERS_PATHS]())

@always_inline
fn _get_dylib_helpers_function[
    func_name: StaticString, result_type: __TypeOfAllTypes
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_HELPERS_LIBRARY(),
        func_name,
        result_type,
    ]()

struct _dylib_helpers_function[fn_name: StaticString, type: __TypeOfAllTypes](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    fn load() raises -> Self.type:
        return _get_dylib_helpers_function[Self.fn_name, Self.type]()

struct LibDuckDB(Movable):

    var _duckdb_open: _duckdb_open.fn_type
    var _duckdb_open_ext: _duckdb_open_ext.fn_type
    var _duckdb_close: _duckdb_close.fn_type
    var _duckdb_connect: _duckdb_connect.fn_type
    var _duckdb_disconnect: _duckdb_disconnect.fn_type
    var _duckdb_free: _duckdb_free.fn_type
    var _duckdb_query: _duckdb_query.fn_type
    var _duckdb_destroy_result: _duckdb_destroy_result.fn_type
    var _duckdb_column_name: _duckdb_column_name.fn_type
    var _duckdb_column_type: _duckdb_column_type.fn_type
    var _duckdb_result_statement_type_ptr: _duckdb_result_statement_type_ptr.fn_type
    var _duckdb_column_logical_type: _duckdb_column_logical_type.fn_type
    var _duckdb_column_count: _duckdb_column_count.fn_type
    var _duckdb_rows_changed: _duckdb_rows_changed.fn_type
    var _duckdb_result_error: _duckdb_result_error.fn_type
    var _duckdb_result_error_type: _duckdb_result_error_type.fn_type
    var _duckdb_prepare_error: _duckdb_prepare_error.fn_type
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
    var _duckdb_create_vector: _duckdb_create_vector.fn_type
    var _duckdb_destroy_vector: _duckdb_destroy_vector.fn_type
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
    var _duckdb_slice_vector: _duckdb_slice_vector.fn_type
    var _duckdb_vector_copy_sel: _duckdb_vector_copy_sel.fn_type
    var _duckdb_vector_reference_value: _duckdb_vector_reference_value.fn_type
    var _duckdb_vector_reference_vector: _duckdb_vector_reference_vector.fn_type
    var _duckdb_validity_row_is_valid: _duckdb_validity_row_is_valid.fn_type
    var _duckdb_validity_set_row_validity: _duckdb_validity_set_row_validity.fn_type
    var _duckdb_validity_set_row_invalid: _duckdb_validity_set_row_invalid.fn_type
    var _duckdb_validity_set_row_valid: _duckdb_validity_set_row_valid.fn_type
    var _duckdb_create_scalar_function: _duckdb_create_scalar_function.fn_type
    var _duckdb_destroy_scalar_function: _duckdb_destroy_scalar_function.fn_type
    var _duckdb_scalar_function_set_name: _duckdb_scalar_function_set_name.fn_type
    var _duckdb_scalar_function_set_varargs: _duckdb_scalar_function_set_varargs.fn_type
    var _duckdb_scalar_function_set_special_handling: _duckdb_scalar_function_set_special_handling.fn_type
    var _duckdb_scalar_function_set_volatile: _duckdb_scalar_function_set_volatile.fn_type
    var _duckdb_scalar_function_add_parameter: _duckdb_scalar_function_add_parameter.fn_type
    var _duckdb_scalar_function_set_return_type: _duckdb_scalar_function_set_return_type.fn_type
    var _duckdb_scalar_function_set_extra_info: _duckdb_scalar_function_set_extra_info.fn_type
    var _duckdb_scalar_function_set_bind: _duckdb_scalar_function_set_bind.fn_type
    var _duckdb_scalar_function_set_bind_data: _duckdb_scalar_function_set_bind_data.fn_type
    var _duckdb_scalar_function_set_bind_data_copy: _duckdb_scalar_function_set_bind_data_copy.fn_type
    var _duckdb_scalar_function_bind_set_error: _duckdb_scalar_function_bind_set_error.fn_type
    var _duckdb_scalar_function_set_function: _duckdb_scalar_function_set_function.fn_type
    var _duckdb_register_scalar_function: _duckdb_register_scalar_function.fn_type
    var _duckdb_scalar_function_get_extra_info: _duckdb_scalar_function_get_extra_info.fn_type
    var _duckdb_scalar_function_bind_get_extra_info: _duckdb_scalar_function_bind_get_extra_info.fn_type
    var _duckdb_scalar_function_get_bind_data: _duckdb_scalar_function_get_bind_data.fn_type
    var _duckdb_scalar_function_get_client_context: _duckdb_scalar_function_get_client_context.fn_type
    var _duckdb_scalar_function_set_error: _duckdb_scalar_function_set_error.fn_type
    var _duckdb_create_scalar_function_set: _duckdb_create_scalar_function_set.fn_type
    var _duckdb_destroy_scalar_function_set: _duckdb_destroy_scalar_function_set.fn_type
    var _duckdb_add_scalar_function_to_set: _duckdb_add_scalar_function_to_set.fn_type
    var _duckdb_register_scalar_function_set: _duckdb_register_scalar_function_set.fn_type
    var _duckdb_scalar_function_bind_get_argument_count: _duckdb_scalar_function_bind_get_argument_count.fn_type
    var _duckdb_scalar_function_bind_get_argument: _duckdb_scalar_function_bind_get_argument.fn_type
    var _duckdb_create_aggregate_function: _duckdb_create_aggregate_function.fn_type
    var _duckdb_destroy_aggregate_function: _duckdb_destroy_aggregate_function.fn_type
    var _duckdb_aggregate_function_set_name: _duckdb_aggregate_function_set_name.fn_type
    var _duckdb_aggregate_function_add_parameter: _duckdb_aggregate_function_add_parameter.fn_type
    var _duckdb_aggregate_function_set_return_type: _duckdb_aggregate_function_set_return_type.fn_type
    var _duckdb_aggregate_function_set_functions: _duckdb_aggregate_function_set_functions.fn_type
    var _duckdb_aggregate_function_set_destructor: _duckdb_aggregate_function_set_destructor.fn_type
    var _duckdb_register_aggregate_function: _duckdb_register_aggregate_function.fn_type
    var _duckdb_aggregate_function_get_extra_info: _duckdb_aggregate_function_get_extra_info.fn_type
    var _duckdb_aggregate_function_set_error: _duckdb_aggregate_function_set_error.fn_type
    var _duckdb_create_logical_type: _duckdb_create_logical_type.fn_type
    var _duckdb_create_list_type: _duckdb_create_list_type.fn_type
    var _duckdb_create_array_type: _duckdb_create_array_type.fn_type
    var _duckdb_create_map_type: _duckdb_create_map_type.fn_type
    var _duckdb_create_union_type: _duckdb_create_union_type.fn_type
    var _duckdb_create_struct_type: _duckdb_create_struct_type.fn_type
    var _duckdb_get_type_id: _duckdb_get_type_id.fn_type
    var _duckdb_list_type_child_type: _duckdb_list_type_child_type.fn_type
    var _duckdb_array_type_child_type: _duckdb_array_type_child_type.fn_type
    var _duckdb_array_type_array_size: _duckdb_array_type_array_size.fn_type
    var _duckdb_map_type_key_type: _duckdb_map_type_key_type.fn_type
    var _duckdb_map_type_value_type: _duckdb_map_type_value_type.fn_type
    var _duckdb_destroy_logical_type: _duckdb_destroy_logical_type.fn_type
    var _duckdb_execution_is_finished: _duckdb_execution_is_finished.fn_type
    var _duckdb_fetch_chunk_ptr: _duckdb_fetch_chunk_ptr.fn_type
    var _duckdb_destroy_value: _duckdb_destroy_value.fn_type
    var _duckdb_create_varchar: _duckdb_create_varchar.fn_type
    var _duckdb_create_varchar_length: _duckdb_create_varchar_length.fn_type
    var _duckdb_create_bool: _duckdb_create_bool.fn_type
    var _duckdb_create_int8: _duckdb_create_int8.fn_type
    var _duckdb_create_uint8: _duckdb_create_uint8.fn_type
    var _duckdb_create_int16: _duckdb_create_int16.fn_type
    var _duckdb_create_uint16: _duckdb_create_uint16.fn_type
    var _duckdb_create_int32: _duckdb_create_int32.fn_type
    var _duckdb_create_uint32: _duckdb_create_uint32.fn_type
    var _duckdb_create_int64: _duckdb_create_int64.fn_type
    var _duckdb_create_uint64: _duckdb_create_uint64.fn_type
    var _duckdb_create_float: _duckdb_create_float.fn_type
    var _duckdb_create_double: _duckdb_create_double.fn_type
    var _duckdb_create_date: _duckdb_create_date.fn_type
    var _duckdb_create_timestamp: _duckdb_create_timestamp.fn_type
    var _duckdb_create_interval: _duckdb_create_interval.fn_type
    var _duckdb_create_blob: _duckdb_create_blob.fn_type
    var _duckdb_create_null_value: _duckdb_create_null_value.fn_type
    var _duckdb_get_bool: _duckdb_get_bool.fn_type
    var _duckdb_get_int8: _duckdb_get_int8.fn_type
    var _duckdb_get_uint8: _duckdb_get_uint8.fn_type
    var _duckdb_get_int16: _duckdb_get_int16.fn_type
    var _duckdb_get_uint16: _duckdb_get_uint16.fn_type
    var _duckdb_get_int32: _duckdb_get_int32.fn_type
    var _duckdb_get_uint32: _duckdb_get_uint32.fn_type
    var _duckdb_get_int64: _duckdb_get_int64.fn_type
    var _duckdb_get_uint64: _duckdb_get_uint64.fn_type
    var _duckdb_get_float: _duckdb_get_float.fn_type
    var _duckdb_get_double: _duckdb_get_double.fn_type
    var _duckdb_get_date: _duckdb_get_date.fn_type
    var _duckdb_get_timestamp: _duckdb_get_timestamp.fn_type
    var _duckdb_get_interval: _duckdb_get_interval.fn_type
    var _duckdb_get_varchar: _duckdb_get_varchar.fn_type
    var _duckdb_get_value_type: _duckdb_get_value_type.fn_type
    var _duckdb_is_null_value: _duckdb_is_null_value.fn_type
    var _duckdb_value_to_string: _duckdb_value_to_string.fn_type

    fn __init__(out self):
        try:
            self._duckdb_open = _duckdb_open.load()
            self._duckdb_open_ext = _duckdb_open_ext.load()
            self._duckdb_close = _duckdb_close.load()
            self._duckdb_connect = _duckdb_connect.load()
            self._duckdb_disconnect = _duckdb_disconnect.load()
            self._duckdb_free = _duckdb_free.load()
            self._duckdb_query = _duckdb_query.load()
            self._duckdb_destroy_result = _duckdb_destroy_result.load()
            self._duckdb_column_name = _duckdb_column_name.load()
            self._duckdb_column_type = _duckdb_column_type.load()
            self._duckdb_result_statement_type_ptr = _duckdb_result_statement_type_ptr.load()
            self._duckdb_column_logical_type = _duckdb_column_logical_type.load()
            self._duckdb_column_count = _duckdb_column_count.load()
            self._duckdb_rows_changed = _duckdb_rows_changed.load()
            self._duckdb_result_error = _duckdb_result_error.load()
            self._duckdb_result_error_type = _duckdb_result_error_type.load()
            self._duckdb_prepare_error = _duckdb_prepare_error.load()
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
            self._duckdb_create_vector = _duckdb_create_vector.load()
            self._duckdb_destroy_vector = _duckdb_destroy_vector.load()
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
            self._duckdb_slice_vector = _duckdb_slice_vector.load()
            self._duckdb_vector_copy_sel = _duckdb_vector_copy_sel.load()
            self._duckdb_vector_reference_value = _duckdb_vector_reference_value.load()
            self._duckdb_vector_reference_vector = _duckdb_vector_reference_vector.load()
            self._duckdb_validity_row_is_valid = _duckdb_validity_row_is_valid.load()
            self._duckdb_validity_set_row_validity = _duckdb_validity_set_row_validity.load()
            self._duckdb_validity_set_row_invalid = _duckdb_validity_set_row_invalid.load()
            self._duckdb_validity_set_row_valid = _duckdb_validity_set_row_valid.load()
            self._duckdb_create_scalar_function = _duckdb_create_scalar_function.load()
            self._duckdb_destroy_scalar_function = _duckdb_destroy_scalar_function.load()
            self._duckdb_scalar_function_set_name = _duckdb_scalar_function_set_name.load()
            self._duckdb_scalar_function_set_varargs = _duckdb_scalar_function_set_varargs.load()
            self._duckdb_scalar_function_set_special_handling = _duckdb_scalar_function_set_special_handling.load()
            self._duckdb_scalar_function_set_volatile = _duckdb_scalar_function_set_volatile.load()
            self._duckdb_scalar_function_add_parameter = _duckdb_scalar_function_add_parameter.load()
            self._duckdb_scalar_function_set_return_type = _duckdb_scalar_function_set_return_type.load()
            self._duckdb_scalar_function_set_extra_info = _duckdb_scalar_function_set_extra_info.load()
            self._duckdb_scalar_function_set_bind = _duckdb_scalar_function_set_bind.load()
            self._duckdb_scalar_function_set_bind_data = _duckdb_scalar_function_set_bind_data.load()
            self._duckdb_scalar_function_set_bind_data_copy = _duckdb_scalar_function_set_bind_data_copy.load()
            self._duckdb_scalar_function_bind_set_error = _duckdb_scalar_function_bind_set_error.load()
            self._duckdb_scalar_function_set_function = _duckdb_scalar_function_set_function.load()
            self._duckdb_register_scalar_function = _duckdb_register_scalar_function.load()
            self._duckdb_scalar_function_get_extra_info = _duckdb_scalar_function_get_extra_info.load()
            self._duckdb_scalar_function_bind_get_extra_info = _duckdb_scalar_function_bind_get_extra_info.load()
            self._duckdb_scalar_function_get_bind_data = _duckdb_scalar_function_get_bind_data.load()
            self._duckdb_scalar_function_get_client_context = _duckdb_scalar_function_get_client_context.load()
            self._duckdb_scalar_function_set_error = _duckdb_scalar_function_set_error.load()
            self._duckdb_create_scalar_function_set = _duckdb_create_scalar_function_set.load()
            self._duckdb_destroy_scalar_function_set = _duckdb_destroy_scalar_function_set.load()
            self._duckdb_add_scalar_function_to_set = _duckdb_add_scalar_function_to_set.load()
            self._duckdb_register_scalar_function_set = _duckdb_register_scalar_function_set.load()
            self._duckdb_scalar_function_bind_get_argument_count = _duckdb_scalar_function_bind_get_argument_count.load()
            self._duckdb_scalar_function_bind_get_argument = _duckdb_scalar_function_bind_get_argument.load()
            self._duckdb_add_scalar_function_to_set = _duckdb_add_scalar_function_to_set.load()
            self._duckdb_register_scalar_function_set = _duckdb_register_scalar_function_set.load()
            self._duckdb_create_aggregate_function = _duckdb_create_aggregate_function.load()
            self._duckdb_destroy_aggregate_function = _duckdb_destroy_aggregate_function.load()
            self._duckdb_aggregate_function_set_name = _duckdb_aggregate_function_set_name.load()
            self._duckdb_aggregate_function_add_parameter = _duckdb_aggregate_function_add_parameter.load()
            self._duckdb_aggregate_function_set_return_type = _duckdb_aggregate_function_set_return_type.load()
            self._duckdb_aggregate_function_set_functions = _duckdb_aggregate_function_set_functions.load()
            self._duckdb_aggregate_function_set_destructor = _duckdb_aggregate_function_set_destructor.load()
            self._duckdb_register_aggregate_function = _duckdb_register_aggregate_function.load()
            self._duckdb_aggregate_function_get_extra_info = _duckdb_aggregate_function_get_extra_info.load()
            self._duckdb_aggregate_function_set_error = _duckdb_aggregate_function_set_error.load()
            self._duckdb_create_logical_type = _duckdb_create_logical_type.load()
            self._duckdb_create_list_type = _duckdb_create_list_type.load()
            self._duckdb_create_array_type = _duckdb_create_array_type.load()
            self._duckdb_create_map_type = _duckdb_create_map_type.load()
            self._duckdb_create_union_type = _duckdb_create_union_type.load()
            self._duckdb_create_struct_type = _duckdb_create_struct_type.load()
            self._duckdb_get_type_id = _duckdb_get_type_id.load()
            self._duckdb_list_type_child_type = _duckdb_list_type_child_type.load()
            self._duckdb_array_type_child_type = _duckdb_array_type_child_type.load()
            self._duckdb_array_type_array_size = _duckdb_array_type_array_size.load()
            self._duckdb_map_type_key_type = _duckdb_map_type_key_type.load()
            self._duckdb_map_type_value_type = _duckdb_map_type_value_type.load()
            self._duckdb_destroy_logical_type = _duckdb_destroy_logical_type.load()
            self._duckdb_execution_is_finished = _duckdb_execution_is_finished.load()
            self._duckdb_fetch_chunk_ptr = _duckdb_fetch_chunk_ptr.load()
            self._duckdb_destroy_value = _duckdb_destroy_value.load()
            self._duckdb_create_varchar = _duckdb_create_varchar.load()
            self._duckdb_create_varchar_length = _duckdb_create_varchar_length.load()
            self._duckdb_create_bool = _duckdb_create_bool.load()
            self._duckdb_create_int8 = _duckdb_create_int8.load()
            self._duckdb_create_uint8 = _duckdb_create_uint8.load()
            self._duckdb_create_int16 = _duckdb_create_int16.load()
            self._duckdb_create_uint16 = _duckdb_create_uint16.load()
            self._duckdb_create_int32 = _duckdb_create_int32.load()
            self._duckdb_create_uint32 = _duckdb_create_uint32.load()
            self._duckdb_create_int64 = _duckdb_create_int64.load()
            self._duckdb_create_uint64 = _duckdb_create_uint64.load()
            self._duckdb_create_float = _duckdb_create_float.load()
            self._duckdb_create_double = _duckdb_create_double.load()
            self._duckdb_create_date = _duckdb_create_date.load()
            self._duckdb_create_timestamp = _duckdb_create_timestamp.load()
            self._duckdb_create_interval = _duckdb_create_interval.load()
            self._duckdb_create_blob = _duckdb_create_blob.load()
            self._duckdb_create_null_value = _duckdb_create_null_value.load()
            self._duckdb_get_bool = _duckdb_get_bool.load()
            self._duckdb_get_int8 = _duckdb_get_int8.load()
            self._duckdb_get_uint8 = _duckdb_get_uint8.load()
            self._duckdb_get_int16 = _duckdb_get_int16.load()
            self._duckdb_get_uint16 = _duckdb_get_uint16.load()
            self._duckdb_get_int32 = _duckdb_get_int32.load()
            self._duckdb_get_uint32 = _duckdb_get_uint32.load()
            self._duckdb_get_int64 = _duckdb_get_int64.load()
            self._duckdb_get_uint64 = _duckdb_get_uint64.load()
            self._duckdb_get_float = _duckdb_get_float.load()
            self._duckdb_get_double = _duckdb_get_double.load()
            self._duckdb_get_date = _duckdb_get_date.load()
            self._duckdb_get_timestamp = _duckdb_get_timestamp.load()
            self._duckdb_get_interval = _duckdb_get_interval.load()
            self._duckdb_get_varchar = _duckdb_get_varchar.load()
            self._duckdb_get_value_type = _duckdb_get_value_type.load()
            self._duckdb_is_null_value = _duckdb_is_null_value.load()
            self._duckdb_value_to_string = _duckdb_value_to_string.load()
        except e:
            abort(String(e))

    fn __moveinit__(out self, deinit existing: Self):
        self._duckdb_open = existing._duckdb_open
        self._duckdb_open_ext = existing._duckdb_open_ext
        self._duckdb_close = existing._duckdb_close
        self._duckdb_connect = existing._duckdb_connect
        self._duckdb_disconnect = existing._duckdb_disconnect
        self._duckdb_free = existing._duckdb_free
        self._duckdb_query = existing._duckdb_query
        self._duckdb_destroy_result = existing._duckdb_destroy_result
        self._duckdb_column_name = existing._duckdb_column_name
        self._duckdb_column_type = existing._duckdb_column_type
        self._duckdb_result_statement_type_ptr = existing._duckdb_result_statement_type_ptr
        self._duckdb_column_logical_type = existing._duckdb_column_logical_type
        self._duckdb_column_count = existing._duckdb_column_count
        self._duckdb_rows_changed = existing._duckdb_rows_changed
        self._duckdb_result_error = existing._duckdb_result_error
        self._duckdb_result_error_type = existing._duckdb_result_error_type
        self._duckdb_prepare_error = existing._duckdb_prepare_error
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
        self._duckdb_create_vector = existing._duckdb_create_vector
        self._duckdb_destroy_vector = existing._duckdb_destroy_vector
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
        self._duckdb_slice_vector = existing._duckdb_slice_vector
        self._duckdb_vector_copy_sel = existing._duckdb_vector_copy_sel
        self._duckdb_vector_reference_value = existing._duckdb_vector_reference_value
        self._duckdb_vector_reference_vector = existing._duckdb_vector_reference_vector
        self._duckdb_validity_row_is_valid = existing._duckdb_validity_row_is_valid
        self._duckdb_validity_set_row_validity = existing._duckdb_validity_set_row_validity
        self._duckdb_validity_set_row_invalid = existing._duckdb_validity_set_row_invalid
        self._duckdb_validity_set_row_valid = existing._duckdb_validity_set_row_valid
        self._duckdb_create_scalar_function = existing._duckdb_create_scalar_function
        self._duckdb_destroy_scalar_function = existing._duckdb_destroy_scalar_function
        self._duckdb_scalar_function_set_name = existing._duckdb_scalar_function_set_name
        self._duckdb_scalar_function_set_varargs = existing._duckdb_scalar_function_set_varargs
        self._duckdb_scalar_function_set_special_handling = existing._duckdb_scalar_function_set_special_handling
        self._duckdb_scalar_function_set_volatile = existing._duckdb_scalar_function_set_volatile
        self._duckdb_scalar_function_add_parameter = existing._duckdb_scalar_function_add_parameter
        self._duckdb_scalar_function_set_return_type = existing._duckdb_scalar_function_set_return_type
        self._duckdb_scalar_function_set_extra_info = existing._duckdb_scalar_function_set_extra_info
        self._duckdb_scalar_function_set_bind = existing._duckdb_scalar_function_set_bind
        self._duckdb_scalar_function_set_bind_data = existing._duckdb_scalar_function_set_bind_data
        self._duckdb_scalar_function_set_bind_data_copy = existing._duckdb_scalar_function_set_bind_data_copy
        self._duckdb_scalar_function_bind_set_error = existing._duckdb_scalar_function_bind_set_error
        self._duckdb_scalar_function_set_function = existing._duckdb_scalar_function_set_function
        self._duckdb_register_scalar_function = existing._duckdb_register_scalar_function
        self._duckdb_scalar_function_get_extra_info = existing._duckdb_scalar_function_get_extra_info
        self._duckdb_scalar_function_bind_get_extra_info = existing._duckdb_scalar_function_bind_get_extra_info
        self._duckdb_scalar_function_get_bind_data = existing._duckdb_scalar_function_get_bind_data
        self._duckdb_scalar_function_get_client_context = existing._duckdb_scalar_function_get_client_context
        self._duckdb_scalar_function_set_error = existing._duckdb_scalar_function_set_error
        self._duckdb_create_scalar_function_set = existing._duckdb_create_scalar_function_set
        self._duckdb_destroy_scalar_function_set = existing._duckdb_destroy_scalar_function_set
        self._duckdb_add_scalar_function_to_set = existing._duckdb_add_scalar_function_to_set
        self._duckdb_register_scalar_function_set = existing._duckdb_register_scalar_function_set
        self._duckdb_scalar_function_bind_get_argument_count = existing._duckdb_scalar_function_bind_get_argument_count
        self._duckdb_scalar_function_bind_get_argument = existing._duckdb_scalar_function_bind_get_argument
        self._duckdb_create_aggregate_function = existing._duckdb_create_aggregate_function
        self._duckdb_destroy_aggregate_function = existing._duckdb_destroy_aggregate_function
        self._duckdb_aggregate_function_set_name = existing._duckdb_aggregate_function_set_name
        self._duckdb_aggregate_function_add_parameter = existing._duckdb_aggregate_function_add_parameter
        self._duckdb_aggregate_function_set_return_type = existing._duckdb_aggregate_function_set_return_type
        self._duckdb_aggregate_function_set_functions = existing._duckdb_aggregate_function_set_functions
        self._duckdb_aggregate_function_set_destructor = existing._duckdb_aggregate_function_set_destructor
        self._duckdb_register_aggregate_function = existing._duckdb_register_aggregate_function
        self._duckdb_aggregate_function_get_extra_info = existing._duckdb_aggregate_function_get_extra_info
        self._duckdb_aggregate_function_set_error = existing._duckdb_aggregate_function_set_error
        self._duckdb_create_logical_type = existing._duckdb_create_logical_type

        self._duckdb_add_scalar_function_to_set = existing._duckdb_add_scalar_function_to_set
        self._duckdb_register_scalar_function_set = existing._duckdb_register_scalar_function_set
        self._duckdb_create_logical_type = existing._duckdb_create_logical_type
        self._duckdb_create_list_type = existing._duckdb_create_list_type
        self._duckdb_create_array_type = existing._duckdb_create_array_type
        self._duckdb_create_map_type = existing._duckdb_create_map_type
        self._duckdb_create_union_type = existing._duckdb_create_union_type
        self._duckdb_create_struct_type = existing._duckdb_create_struct_type
        self._duckdb_get_type_id = existing._duckdb_get_type_id
        self._duckdb_list_type_child_type = existing._duckdb_list_type_child_type
        self._duckdb_array_type_child_type = existing._duckdb_array_type_child_type
        self._duckdb_array_type_array_size = existing._duckdb_array_type_array_size
        self._duckdb_map_type_key_type = existing._duckdb_map_type_key_type
        self._duckdb_map_type_value_type = existing._duckdb_map_type_value_type
        self._duckdb_destroy_logical_type = existing._duckdb_destroy_logical_type
        self._duckdb_execution_is_finished = existing._duckdb_execution_is_finished
        self._duckdb_fetch_chunk_ptr = existing._duckdb_fetch_chunk_ptr
        self._duckdb_destroy_value = existing._duckdb_destroy_value
        self._duckdb_create_varchar = existing._duckdb_create_varchar
        self._duckdb_create_varchar_length = existing._duckdb_create_varchar_length
        self._duckdb_create_bool = existing._duckdb_create_bool
        self._duckdb_create_int8 = existing._duckdb_create_int8
        self._duckdb_create_uint8 = existing._duckdb_create_uint8
        self._duckdb_create_int16 = existing._duckdb_create_int16
        self._duckdb_create_uint16 = existing._duckdb_create_uint16
        self._duckdb_create_int32 = existing._duckdb_create_int32
        self._duckdb_create_uint32 = existing._duckdb_create_uint32
        self._duckdb_create_int64 = existing._duckdb_create_int64
        self._duckdb_create_uint64 = existing._duckdb_create_uint64
        self._duckdb_create_float = existing._duckdb_create_float
        self._duckdb_create_double = existing._duckdb_create_double
        self._duckdb_create_date = existing._duckdb_create_date
        self._duckdb_create_timestamp = existing._duckdb_create_timestamp
        self._duckdb_create_interval = existing._duckdb_create_interval
        self._duckdb_create_blob = existing._duckdb_create_blob
        self._duckdb_create_null_value = existing._duckdb_create_null_value
        self._duckdb_get_bool = existing._duckdb_get_bool
        self._duckdb_get_int8 = existing._duckdb_get_int8
        self._duckdb_get_uint8 = existing._duckdb_get_uint8
        self._duckdb_get_int16 = existing._duckdb_get_int16
        self._duckdb_get_uint16 = existing._duckdb_get_uint16
        self._duckdb_get_int32 = existing._duckdb_get_int32
        self._duckdb_get_uint32 = existing._duckdb_get_uint32
        self._duckdb_get_int64 = existing._duckdb_get_int64
        self._duckdb_get_uint64 = existing._duckdb_get_uint64
        self._duckdb_get_float = existing._duckdb_get_float
        self._duckdb_get_double = existing._duckdb_get_double
        self._duckdb_get_date = existing._duckdb_get_date
        self._duckdb_get_timestamp = existing._duckdb_get_timestamp
        self._duckdb_get_interval = existing._duckdb_get_interval
        self._duckdb_get_varchar = existing._duckdb_get_varchar
        self._duckdb_get_value_type = existing._duckdb_get_value_type
        self._duckdb_is_null_value = existing._duckdb_is_null_value
        self._duckdb_value_to_string = existing._duckdb_value_to_string


    # ===--------------------------------------------------------------------===#
    # Functions
    # ===--------------------------------------------------------------------===#

    # ===--------------------------------------------------------------------===#
    # Open/Connect
    # ===--------------------------------------------------------------------===#

    fn duckdb_open(
        self,
        path: UnsafePointer[c_char, ImmutAnyOrigin],
        out_database: UnsafePointer[duckdb_database, MutAnyOrigin],
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

    fn duckdb_open_ext(
        self,
        path: UnsafePointer[c_char, ImmutAnyOrigin],
        out_database: UnsafePointer[duckdb_database, MutAnyOrigin],
        config: duckdb_config,
        out_error: UnsafePointer[UnsafePointer[c_char, MutAnyOrigin], MutAnyOrigin],
    ) -> UInt32:
        """
        Extended version of duckdb_open. Creates a new database or opens an existing database file stored at the given path.
        The database must be closed with 'duckdb_close'.

        * path: Path to the database file on disk. Both `nullptr` and `:memory:` open an in-memory database.
        * out_database: The result database object.
        * config: (Optional) configuration used to start up the database.
        * out_error: If set and the function returns `DuckDBError`, this contains the error message.
                     Note that the error message must be freed using `duckdb_free`.
        * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
        """
        return self._duckdb_open_ext(path, out_database, config, out_error)

    fn duckdb_close(self, database: UnsafePointer[duckdb_database, MutAnyOrigin]) -> NoneType:
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
        out_connection: UnsafePointer[duckdb_connection, MutAnyOrigin],
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
        connection: UnsafePointer[duckdb_connection, MutAnyOrigin]
    ) -> NoneType:
        """
        Closes the specified connection and de-allocates all memory allocated for that connection.

        * connection: The connection to close.
        """
        return self._duckdb_disconnect(connection)

    fn duckdb_free(self, ptr: UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType:
        """
        Free a value returned from `duckdb_malloc`, `duckdb_value_varchar`, `duckdb_value_blob`, or
        `duckdb_value_string`.

        * ptr: The memory region to de-allocate.
        """
        return self._duckdb_free(ptr)

    # ===--------------------------------------------------------------------===#
    # Query Execution
    # ===--------------------------------------------------------------------===#

    fn duckdb_query(
        self,
        connection: duckdb_connection,
        query: UnsafePointer[c_char, ImmutAnyOrigin],
        out_result: UnsafePointer[duckdb_result, MutAnyOrigin],
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

    fn duckdb_destroy_result(self, result: UnsafePointer[duckdb_result, MutAnyOrigin]) -> NoneType:
        """
        Closes the result and de-allocates all memory allocated for that connection.

        * result: The result to destroy.
        """
        return self._duckdb_destroy_result(result)

    fn duckdb_column_name(
        self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin], col: idx_t
    ) -> UnsafePointer[c_char, ImmutAnyOrigin]:
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
        self, result: UnsafePointer[duckdb_result, MutAnyOrigin], col: idx_t
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

        NOTE: Mojo cannot currently pass large structs by value correctly over the C ABI. We therefore call a helper
        wrapper that accepts a pointer to duckdb_result instead of passing it by value directly.
        """
        return self._duckdb_result_statement_type_ptr(UnsafePointer(to=result))

    fn duckdb_column_logical_type(
        self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin], col: idx_t
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

    fn duckdb_column_count(self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> idx_t:
        """
        Returns the number of columns present in a the result object.

        * result: The result object.
        * returns: The number of columns present in the result object.
        """
        return self._duckdb_column_count(result)

    fn duckdb_rows_changed(self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> idx_t:
        """
        Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
        queries. For other queries the rows_changed will be 0.

        * result: The result object.
        * returns: The number of rows changed.
        """
        return self._duckdb_rows_changed(result)

    fn duckdb_result_error(self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> UnsafePointer[c_char, ImmutExternalOrigin]:
        """
        Returns the error message contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.

        The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_result` is called.

        * result: The result object to fetch the error from.
        * returns: The error of the result.
        """
        return self._duckdb_result_error(result)

    fn duckdb_result_error_type(self, result: UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> duckdb_error_type:
        """
        Returns the result error type contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.

        * result: The result object to fetch the error from.
        * returns: The error type of the result.
        """
        return self._duckdb_result_error_type(result)

    fn duckdb_row_count(self, result: UnsafePointer[duckdb_result, MutAnyOrigin]) -> idx_t:
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

    #===--------------------------------------------------------------------===#
    # Prepared Statements
    #===--------------------------------------------------------------------===#

    fn duckdb_prepare_error(self, prepared_statement: duckdb_prepared_statement) -> UnsafePointer[c_char, ImmutExternalOrigin]:
        """
        Returns the error message associated with the given prepared statement.
        If the prepared statement has no error message, this returns `nullptr` instead.

        The error message should not be freed. It will be de-allocated when `duckdb_destroy_prepare` is called.

        * prepared_statement: The prepared statement to obtain the error from.
        * returns: The error message, or `nullptr` if there is none.
        """
        return self._duckdb_prepare_error(prepared_statement)

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
        self, types: UnsafePointer[duckdb_logical_type, ImmutAnyOrigin], column_count: idx_t
    ) -> duckdb_data_chunk:
        """
        Creates an empty DataChunk with the specified set of types.

        Note that the result must be destroyed with `duckdb_destroy_data_chunk`.

        * types: An array of types of the data chunk.
        * column_count: The number of columns.
        * returns: The data chunk.
        """
        return self._duckdb_create_data_chunk(types, column_count)

    fn duckdb_destroy_data_chunk(self, chunk: UnsafePointer[duckdb_data_chunk, MutAnyOrigin]) -> NoneType:
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

    # ===--------------------------------------------------------------------===#
    #  Vector Interface
    # ===--------------------------------------------------------------------===#

    fn duckdb_create_vector(self, type: duckdb_logical_type, capacity: idx_t) -> duckdb_vector:
        """
        Creates a flat vector. Must be destroyed with `duckdb_destroy_vector`.

        * type: The logical type of the vector.
        * capacity: The capacity of the vector.
        * returns: The vector.
        """
        return self._duckdb_create_vector(type, capacity)

    fn duckdb_destroy_vector(self, vector: UnsafePointer[duckdb_vector, MutAnyOrigin]) -> NoneType:
        """
        Destroys the vector and de-allocates its memory.

        * vector: A pointer to the vector.
        """
        return self._duckdb_destroy_vector(vector)

    fn duckdb_vector_get_column_type(self, vector: duckdb_vector) -> duckdb_logical_type:
        """
        Retrieves the column type of the specified vector.

        The result must be destroyed with `duckdb_destroy_logical_type`.

        * vector: The vector get the data from
        * returns: The type of the vector
        """
        return self._duckdb_vector_get_column_type(vector)

    fn duckdb_vector_get_data(self, vector: duckdb_vector) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """
        Retrieves the data pointer of the vector.

        The data pointer can be used to read or write values from the vector.
        How to read or write values depends on the type of the vector.

        * vector: The vector to get the data from
        * returns: The data pointer
        """
        return self._duckdb_vector_get_data(vector)

    fn duckdb_vector_get_validity(self, vector: duckdb_vector) -> UnsafePointer[UInt64, MutAnyOrigin]:
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
        self, vector: duckdb_vector, index: idx_t, str: UnsafePointer[c_char, ImmutAnyOrigin]
    ) -> NoneType:
        """
        Assigns a string element in the vector at the specified location.

        * vector: The vector to alter
        * index: The row position in the vector to assign the string to
        * str: The null-terminated string
        """
        return self._duckdb_vector_assign_string_element(vector, index, str)

    fn duckdb_vector_assign_string_element_len(
        self, vector: duckdb_vector, index: idx_t, str: UnsafePointer[c_char, ImmutAnyOrigin], str_len: idx_t
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

    fn duckdb_slice_vector(self, vector: duckdb_vector, sel: duckdb_selection_vector, len: idx_t) -> NoneType:
        """
        Slice a vector with a selection vector.
        The length of the selection vector must be less than or equal to the length of the vector.
        Turns the vector into a dictionary vector.

        * vector: The vector to slice.
        * sel: The selection vector.
        * len: The length of the selection vector.
        """
        return self._duckdb_slice_vector(vector, sel, len)

    fn duckdb_vector_copy_sel(
        self,
        src: duckdb_vector,
        dst: duckdb_vector,
        sel: duckdb_selection_vector,
        src_count: idx_t,
        src_offset: idx_t,
        dst_offset: idx_t,
    ) -> NoneType:
        """
        Copy the src vector to the dst with a selection vector that identifies which indices to copy.

        * src: The vector to copy from.
        * dst: The vector to copy to.
        * sel: The selection vector. The length of the selection vector should not be more than the length of the src vector
        * src_count: The number of entries from selection vector to copy. Think of this as the effective length of the
        selection vector starting from index 0
        * src_offset: The offset in the selection vector to copy from (important: actual number of items copied =
        src_count - src_offset).
        * dst_offset: The offset in the dst vector to start copying to.
        """
        return self._duckdb_vector_copy_sel(src, dst, sel, src_count, src_offset, dst_offset)

    fn duckdb_vector_reference_value(self, vector: duckdb_vector, value: duckdb_value) -> NoneType:
        """
        Copies the value from `value` to `vector`.

        * vector: The receiving vector.
        * value: The value to copy into the vector.
        """
        return self._duckdb_vector_reference_value(vector, value)

    fn duckdb_vector_reference_vector(self, to_vector: duckdb_vector, from_vector: duckdb_vector) -> NoneType:
        """
        Changes `to_vector` to reference `from_vector`. After, the vectors share ownership of the data.

        * to_vector: The receiving vector.
        * from_vector: The vector to reference.
        """
        return self._duckdb_vector_reference_vector(to_vector, from_vector)

    # ===--------------------------------------------------------------------===
    # Validity Mask Functions
    # ===--------------------------------------------------------------------===

    fn duckdb_validity_row_is_valid(
        self, validity: UnsafePointer[UInt64, MutExternalOrigin], row: idx_t
    ) -> Bool:
        """
        Returns whether or not a row is valid (i.e. not NULL) in the given validity mask.

        * validity: The validity mask, as obtained through `duckdb_vector_get_validity`
        * row: The row index
        * returns: true if the row is valid, false otherwise
        """
        return self._duckdb_validity_row_is_valid(validity, row)

    fn duckdb_validity_set_row_validity(
        self, validity: UnsafePointer[UInt64, MutExternalOrigin], row: idx_t, valid: Bool
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
        self, validity: UnsafePointer[UInt64, MutExternalOrigin], row: idx_t
    ) -> NoneType:
        """
        In a validity mask, sets a specific row to invalid.

        Equivalent to `duckdb_validity_set_row_validity` with valid set to false.

        * validity: The validity mask
        * row: The row index
        """
        return self._duckdb_validity_set_row_invalid(validity, row)

    fn duckdb_validity_set_row_valid(
        self, validity: UnsafePointer[UInt64, MutExternalOrigin], row: idx_t
    ) -> NoneType:
        """
        In a validity mask, sets a specific row to valid.

        Equivalent to `duckdb_validity_set_row_validity` with valid set to true.

        * validity: The validity mask
        * row: The row index
        """
        return self._duckdb_validity_set_row_valid(validity, row)

    # ===--------------------------------------------------------------------===#
    # Scalar Functions
    # ===--------------------------------------------------------------------===#

    fn duckdb_create_scalar_function(self) -> duckdb_scalar_function:
        """
        Creates a new empty scalar function.
        The return value must be destroyed with `duckdb_destroy_scalar_function`.
        * @return The scalar function object.
        """
        return self._duckdb_create_scalar_function()

    fn duckdb_destroy_scalar_function(self, scalar_function: UnsafePointer[duckdb_scalar_function, MutAnyOrigin]) -> NoneType:
        """
        Destroys the given scalar function object.

        * @param scalar_function The scalar function to destroy
        """
        return self._duckdb_destroy_scalar_function(scalar_function)

    fn duckdb_scalar_function_set_name(self, scalar_function: duckdb_scalar_function, name: UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType:
        """
        Sets the name of the given scalar function.

        * @param scalar_function The scalar function
        * @param name The name of the scalar function
        """
        return self._duckdb_scalar_function_set_name(scalar_function, name)

    fn duckdb_scalar_function_set_varargs(self, scalar_function: duckdb_scalar_function, type: duckdb_logical_type) -> NoneType:
        """
        Sets the parameters of the given scalar function to varargs. Does not require adding parameters with
        duckdb_scalar_function_add_parameter.
        * @param scalar_function The scalar function.
        * @param type The type of the arguments.
        * @return The parameter type. Cannot contain INVALID.
        """
        return self._duckdb_scalar_function_set_varargs(scalar_function, type)

    fn duckdb_scalar_function_set_special_handling(self, scalar_function: duckdb_scalar_function) -> NoneType:
        """
        Sets the scalar function's null-handling behavior to special.
        * @param scalar_function The scalar function.
        """
        return self._duckdb_scalar_function_set_special_handling(scalar_function)

    fn duckdb_scalar_function_set_volatile(self, scalar_function: duckdb_scalar_function) -> NoneType:
        """
        Sets the Function Stability of the scalar function to VOLATILE, indicating the function should be re-run for every row.
        This limits optimization that can be performed for the function.
        * @param scalar_function The scalar function.
        """
        return self._duckdb_scalar_function_set_volatile(scalar_function)

    fn duckdb_scalar_function_add_parameter(self, scalar_function: duckdb_scalar_function, type: duckdb_logical_type) -> NoneType:
        """
        Adds a parameter to the scalar function.
        * @param scalar_function The scalar function.
        * @param type The parameter type. Cannot contain INVALID.
        """
        return self._duckdb_scalar_function_add_parameter(scalar_function, type)

    fn duckdb_scalar_function_set_return_type(self, scalar_function: duckdb_scalar_function, type: duckdb_logical_type) -> NoneType:
        """
        Sets the return type of the scalar function.
        * @param scalar_function The scalar function
        * @param type Cannot contain INVALID or ANY.
        """
        return self._duckdb_scalar_function_set_return_type(scalar_function, type)

    fn duckdb_scalar_function_set_extra_info(self, scalar_function: duckdb_scalar_function, extra_info: UnsafePointer[NoneType, MutAnyOrigin], destroy: duckdb_delete_callback_t) -> NoneType:
        """
        Assigns extra information to the scalar function that can be fetched during binding, etc.

        * @param scalar_function The scalar function
        * @param extra_info The extra information
        * @param destroy The callback that will be called to destroy the extra information (if any)
        """
        return self._duckdb_scalar_function_set_extra_info(scalar_function, extra_info, destroy)

    fn duckdb_scalar_function_set_bind(self, scalar_function: duckdb_scalar_function, bind: duckdb_scalar_function_bind_t) -> NoneType:
        """
        Sets the (optional) bind function of the scalar function.

        * @param scalar_function The scalar function
        * @param bind The bind function
        """
        return self._duckdb_scalar_function_set_bind(scalar_function, bind)

    fn duckdb_scalar_function_set_bind_data(self, info: duckdb_bind_info, bind_data: UnsafePointer[NoneType, MutAnyOrigin], destroy: duckdb_delete_callback_t) -> NoneType:
        """
        Sets the user-provided bind data in the bind object of the scalar function.
        This object can be retrieved again during execution.
        * @param info The bind info of the scalar function.
        * @param bind_data The bind data object.
        * @param destroy The callback to destroy the bind data (if any).
        """
        return self._duckdb_scalar_function_set_bind_data(info, bind_data, destroy)

    fn duckdb_scalar_function_set_bind_data_copy(self, info: duckdb_bind_info, copy: duckdb_copy_callback_t) -> NoneType:
        """
        Sets the bind data copy function for the scalar function.
        This function is called to copy the bind data when needed.
        * @param info The bind info of the scalar function.
        * @param copy The callback to copy the bind data.
        """
        return self._duckdb_scalar_function_set_bind_data_copy(info, copy)

    fn duckdb_scalar_function_bind_set_error(self, info: duckdb_bind_info, error: UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType:
        """
        Report that an error has occurred while calling bind on a scalar function.

        * @param info The bind info object
        * @param error The error message
        """
        return self._duckdb_scalar_function_bind_set_error(info, error)

    fn duckdb_scalar_function_set_function(self, scalar_function: duckdb_scalar_function, function: duckdb_scalar_function_t) -> NoneType:
        """
        Sets the main function of the scalar function.

        * @param scalar_function The scalar function
        * @param function The function
        """
        return self._duckdb_scalar_function_set_function(scalar_function, function)

    fn duckdb_register_scalar_function(self, con: duckdb_connection, scalar_function: duckdb_scalar_function) -> duckdb_state:
        """
        Register the scalar function object within the given connection.
        The function requires at least a name, a function and a return type.
        If the function is incomplete or a function with this name already exists DuckDBError is returned.
        * @param con The connection to register it in.
        * @param scalar_function The function pointer
        * @return Whether or not the registration was successful.
        """
        return self._duckdb_register_scalar_function(con, scalar_function)

    fn duckdb_scalar_function_get_extra_info(self, info: duckdb_function_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """
        Retrieves the extra info of the function as set in `duckdb_scalar_function_set_extra_info`.
        * @param info The info object.
        * @return The extra info.
        """
        return self._duckdb_scalar_function_get_extra_info(info)

    fn duckdb_scalar_function_bind_get_extra_info(self, info: duckdb_bind_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """
        Retrieves the extra info of the function as set in `duckdb_scalar_function_set_extra_info` during bind.
        * @param info The bind info object.
        * @return The extra info.
        """
        return self._duckdb_scalar_function_bind_get_extra_info(info)

    fn duckdb_scalar_function_get_bind_data(self, info: duckdb_function_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """
        Gets the scalar function's bind data set by `duckdb_scalar_function_set_bind_data`.
        Note that the bind data is read-only.
        * @param info The function info.
        * @return The bind data object.
        """
        return self._duckdb_scalar_function_get_bind_data(info)

    fn duckdb_scalar_function_get_client_context(self, info: duckdb_bind_info, out_context: UnsafePointer[duckdb_connection, MutAnyOrigin]) -> NoneType:
        """
        Retrieves the client context of the bind info of a scalar function.
        * @param info The bind info object of the scalar function.
        * @param out_context The client context of the bind info. Must be destroyed with `duckdb_destroy_client_context`.
        """
        return self._duckdb_scalar_function_get_client_context(info, out_context)

    fn duckdb_scalar_function_set_error(self, info: duckdb_function_info, error: UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType:
        """
        Report that an error has occurred while executing the scalar function.

        * @param info The info object.
        * @param error The error message
        """
        return self._duckdb_scalar_function_set_error(info, error)

    fn duckdb_create_scalar_function_set(self, name: UnsafePointer[c_char, ImmutAnyOrigin]) -> duckdb_scalar_function_set:
        """
        Creates a new empty scalar function set.
        The return value must be destroyed with `duckdb_destroy_scalar_function_set`.
        * @return The scalar function set object.
        """
        return self._duckdb_create_scalar_function_set(name)

    fn duckdb_destroy_scalar_function_set(self, scalar_function_set: UnsafePointer[duckdb_scalar_function_set, MutAnyOrigin]) -> NoneType:
        """
        Destroys the given scalar function set object.
        """
        return self._duckdb_destroy_scalar_function_set(scalar_function_set)

    fn duckdb_add_scalar_function_to_set(self, set: duckdb_scalar_function_set, function: duckdb_scalar_function) -> duckdb_state:
        """
        Adds the scalar function as a new overload to the scalar function set.

        Returns DuckDBError if the function could not be added, for example if the overload already exists.
        * @param set The scalar function set
        * @param function The function to add
        """
        return self._duckdb_add_scalar_function_to_set(set, function)

    fn duckdb_register_scalar_function_set(self, con: duckdb_connection, set: duckdb_scalar_function_set) -> duckdb_state:
        """
        Register the scalar function set within the given connection.
        The set requires at least a single valid overload.
        If the set is incomplete or a function with this name already exists DuckDBError is returned.
        * @param con The connection to register it in.
        * @param set The function set to register
        * @return Whether or not the registration was successful.
        """
        return self._duckdb_register_scalar_function_set(con, set)

    fn duckdb_scalar_function_bind_get_argument_count(self, info: duckdb_bind_info) -> idx_t:
        """
        Gets the number of arguments passed to the scalar function during binding.
        * @param info The bind info object.
        * @return The number of arguments.
        """
        return self._duckdb_scalar_function_bind_get_argument_count(info)

    fn duckdb_scalar_function_bind_get_argument(self, info: duckdb_bind_info, index: idx_t) -> duckdb_expression:
        """
        Gets the argument expression at the specified index during binding.
        * @param info The bind info object.
        * @param index The index of the argument.
        * @return The expression at the specified index.
        """
        return self._duckdb_scalar_function_bind_get_argument(info, index)

    # ===--------------------------------------------------------------------===#
    # Aggregate Function Interface
    # ===--------------------------------------------------------------------===#

    fn duckdb_create_aggregate_function(self) -> duckdb_aggregate_function:
        """Creates a new empty aggregate function.
        The return value should be destroyed with `duckdb_destroy_aggregate_function`.

        * returns: The aggregate function object.
        """
        return self._duckdb_create_aggregate_function()

    fn duckdb_destroy_aggregate_function(
        self, aggregate_function: UnsafePointer[duckdb_aggregate_function, MutAnyOrigin]
    ) -> NoneType:
        """Destroys the given aggregate function object.

        * aggregate_function: The aggregate function to destroy.
        """
        return self._duckdb_destroy_aggregate_function(aggregate_function)

    fn duckdb_aggregate_function_set_name(
        self, aggregate_function: duckdb_aggregate_function, name: UnsafePointer[c_char, ImmutAnyOrigin]
    ) -> NoneType:
        """Sets the name of the given aggregate function.

        * aggregate_function: The aggregate function.
        * name: The name of the aggregate function.
        """
        return self._duckdb_aggregate_function_set_name(aggregate_function, name)

    fn duckdb_aggregate_function_add_parameter(
        self, aggregate_function: duckdb_aggregate_function, type: duckdb_logical_type
    ) -> NoneType:
        """Adds a parameter to the aggregate function.

        * aggregate_function: The aggregate function.
        * type: The parameter type.
        """
        return self._duckdb_aggregate_function_add_parameter(aggregate_function, type)

    fn duckdb_aggregate_function_set_return_type(
        self, aggregate_function: duckdb_aggregate_function, type: duckdb_logical_type
    ) -> NoneType:
        """Sets the return type of the aggregate function.

        * aggregate_function: The aggregate function.
        * type: The return type.
        """
        return self._duckdb_aggregate_function_set_return_type(aggregate_function, type)

    fn duckdb_aggregate_function_set_functions(
        self, 
        aggregate_function: duckdb_aggregate_function,
        state_size: duckdb_aggregate_state_size,
        state_init: duckdb_aggregate_init_t,
        update: duckdb_aggregate_update_t,
        combine: duckdb_aggregate_combine_t,
        finalize: duckdb_aggregate_finalize_t
    ) -> NoneType:
        """Sets all callback functions for the aggregate function.

        * aggregate_function: The aggregate function.
        * state_size: Function returning size of state in bytes.
        * state_init: State initialization function.
        * update: Update function called for each row.
        * combine: Combine function for parallel aggregation (optional, can be None).
        * finalize: Finalize function to produce result.
        """
        return self._duckdb_aggregate_function_set_functions(
            aggregate_function, state_size, state_init, update, combine, finalize
        )

    fn duckdb_aggregate_function_set_destructor(
        self, aggregate_function: duckdb_aggregate_function, destroy: duckdb_aggregate_destroy_t
    ) -> NoneType:
        """Sets the state destructor callback of the aggregate function (optional).

        * aggregate_function: The aggregate function.
        * destroy: State destroy callback.
        """
        return self._duckdb_aggregate_function_set_destructor(aggregate_function, destroy)

    fn duckdb_register_aggregate_function(
        self, con: duckdb_connection, aggregate_function: duckdb_aggregate_function
    ) -> duckdb_state:
        """Register the aggregate function object within the given connection.

        The function requires at minimum a name, a return type, and an update and finalize function.

        * con: The connection to register it in.
        * aggregate_function: The function to register.
        * returns: Whether or not the registration was successful.
        """
        return self._duckdb_register_aggregate_function(con, aggregate_function)

    fn duckdb_aggregate_function_get_extra_info(self, info: duckdb_function_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info of the function as set in `duckdb_aggregate_function_set_extra_info`.

        * info: The info object.
        * returns: The extra info.
        """
        return self._duckdb_aggregate_function_get_extra_info(info)

    fn duckdb_aggregate_function_set_error(
        self, info: duckdb_function_info, error: UnsafePointer[c_char, MutAnyOrigin]
    ) -> NoneType:
        """Report that an error has occurred while executing the aggregate function.

        * info: The info object.
        * error: The error message.
        """
        return self._duckdb_aggregate_function_set_error(info, error)

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
        member_types: UnsafePointer[duckdb_logical_type, ImmutAnyOrigin],
        member_names: UnsafePointer[UnsafePointer[c_char, ImmutAnyOrigin], ImmutAnyOrigin],
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
        member_types: UnsafePointer[duckdb_logical_type, ImmutAnyOrigin],
        member_names: UnsafePointer[UnsafePointer[c_char, ImmutAnyOrigin], ImmutAnyOrigin],
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

    fn duckdb_array_type_array_size(self, type: duckdb_logical_type) -> idx_t:
        """Retrieves the array size of the given array type.

        * type: The logical type object
        * returns: The fixed number of elements the values of this array type can store.
        """
        return self._duckdb_array_type_array_size(type)

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

    fn duckdb_destroy_logical_type(self, type: UnsafePointer[duckdb_logical_type, MutAnyOrigin]) -> None:
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

    # ===--------------------------------------------------------------------===#
    # Value Interface
    # ===--------------------------------------------------------------------===#

    fn duckdb_destroy_value(self, value: UnsafePointer[duckdb_value, MutAnyOrigin]) -> NoneType:
        """Destroys the value and de-allocates all memory allocated for that type.

        * value: The value to destroy.
        """
        return self._duckdb_destroy_value(value)

    fn duckdb_create_varchar(self, text: UnsafePointer[c_char, ImmutAnyOrigin]) -> duckdb_value:
        """Creates a value from a null-terminated string.

        * text: The null-terminated string
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_varchar(text)

    fn duckdb_create_varchar_length(self, text: UnsafePointer[c_char, ImmutAnyOrigin], length: idx_t) -> duckdb_value:
        """Creates a value from a string.

        * text: The text
        * length: The length of the text
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_varchar_length(text, length)

    fn duckdb_create_bool(self, input: Bool) -> duckdb_value:
        """Creates a value from a boolean.

        * input: The boolean value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_bool(input)

    fn duckdb_create_int8(self, input: Int8) -> duckdb_value:
        """Creates a value from an int8_t (a tinyint).

        * input: The tinyint value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_int8(input)

    fn duckdb_create_uint8(self, input: UInt8) -> duckdb_value:
        """Creates a value from a uint8_t (a utinyint).

        * input: The utinyint value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_uint8(input)

    fn duckdb_create_int16(self, input: Int16) -> duckdb_value:
        """Creates a value from an int16_t (a smallint).

        * input: The smallint value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_int16(input)

    fn duckdb_create_uint16(self, input: UInt16) -> duckdb_value:
        """Creates a value from a uint16_t (a usmallint).

        * input: The usmallint value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_uint16(input)

    fn duckdb_create_int32(self, input: Int32) -> duckdb_value:
        """Creates a value from an int32_t (an integer).

        * input: The integer value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_int32(input)

    fn duckdb_create_uint32(self, input: UInt32) -> duckdb_value:
        """Creates a value from a uint32_t (a uinteger).

        * input: The uinteger value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_uint32(input)

    fn duckdb_create_int64(self, input: Int64) -> duckdb_value:
        """Creates a value from an int64.

        * input: The int64 value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_int64(input)

    fn duckdb_create_uint64(self, input: UInt64) -> duckdb_value:
        """Creates a value from a uint64_t (a ubigint).

        * input: The ubigint value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_uint64(input)

    fn duckdb_create_float(self, input: Float32) -> duckdb_value:
        """Creates a value from a float.

        * input: The float value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_float(input)

    fn duckdb_create_double(self, input: Float64) -> duckdb_value:
        """Creates a value from a double.

        * input: The double value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_double(input)

    fn duckdb_create_date(self, input: duckdb_date) -> duckdb_value:
        """Creates a value from a date.

        * input: The date value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_date(input)

    fn duckdb_create_timestamp(self, input: duckdb_timestamp) -> duckdb_value:
        """Creates a TIMESTAMP value from a duckdb_timestamp.

        * input: The duckdb_timestamp value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_timestamp(input)

    fn duckdb_create_interval(self, input: duckdb_interval) -> duckdb_value:
        """Creates a value from an interval.

        * input: The interval value
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_interval(input)

    fn duckdb_create_blob(self, data: UnsafePointer[UInt8, ImmutAnyOrigin], length: idx_t) -> duckdb_value:
        """Creates a value from a blob.

        * data: The blob data
        * length: The length of the blob data
        * returns: The value. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_blob(data, length)

    fn duckdb_create_null_value(self) -> duckdb_value:
        """Creates a value of type SQLNULL.

        * returns: The duckdb_value representing SQLNULL. This must be destroyed with `duckdb_destroy_value`.
        """
        return self._duckdb_create_null_value()

    fn duckdb_get_bool(self, val: duckdb_value) -> Bool:
        """Returns the boolean value of the given value.

        * val: A duckdb_value containing a boolean
        * returns: A boolean, or false if the value cannot be converted
        """
        return self._duckdb_get_bool(val)

    fn duckdb_get_int8(self, val: duckdb_value) -> Int8:
        """Returns the int8_t value of the given value.

        * val: A duckdb_value containing a tinyint
        * returns: A int8_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_int8(val)

    fn duckdb_get_uint8(self, val: duckdb_value) -> UInt8:
        """Returns the uint8_t value of the given value.

        * val: A duckdb_value containing a utinyint
        * returns: A uint8_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_uint8(val)

    fn duckdb_get_int16(self, val: duckdb_value) -> Int16:
        """Returns the int16_t value of the given value.

        * val: A duckdb_value containing a smallint
        * returns: A int16_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_int16(val)

    fn duckdb_get_uint16(self, val: duckdb_value) -> UInt16:
        """Returns the uint16_t value of the given value.

        * val: A duckdb_value containing a usmallint
        * returns: A uint16_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_uint16(val)

    fn duckdb_get_int32(self, val: duckdb_value) -> Int32:
        """Returns the int32_t value of the given value.

        * val: A duckdb_value containing an integer
        * returns: A int32_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_int32(val)

    fn duckdb_get_uint32(self, val: duckdb_value) -> UInt32:
        """Returns the uint32_t value of the given value.

        * val: A duckdb_value containing a uinteger
        * returns: A uint32_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_uint32(val)

    fn duckdb_get_int64(self, val: duckdb_value) -> Int64:
        """Returns the int64_t value of the given value.

        * val: A duckdb_value containing a bigint
        * returns: A int64_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_int64(val)

    fn duckdb_get_uint64(self, val: duckdb_value) -> UInt64:
        """Returns the uint64_t value of the given value.

        * val: A duckdb_value containing a ubigint
        * returns: A uint64_t, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_uint64(val)

    fn duckdb_get_float(self, val: duckdb_value) -> Float32:
        """Returns the float value of the given value.

        * val: A duckdb_value containing a float
        * returns: A float, or NAN if the value cannot be converted
        """
        return self._duckdb_get_float(val)

    fn duckdb_get_double(self, val: duckdb_value) -> Float64:
        """Returns the double value of the given value.

        * val: A duckdb_value containing a double
        * returns: A double, or NAN if the value cannot be converted
        """
        return self._duckdb_get_double(val)

    fn duckdb_get_date(self, val: duckdb_value) -> duckdb_date:
        """Returns the date value of the given value.

        * val: A duckdb_value containing a date
        * returns: A duckdb_date, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_date(val)

    fn duckdb_get_timestamp(self, val: duckdb_value) -> duckdb_timestamp:
        """Returns the TIMESTAMP value of the given value.

        * val: A duckdb_value containing a TIMESTAMP
        * returns: A duckdb_timestamp, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_timestamp(val)

    fn duckdb_get_interval(self, val: duckdb_value) -> duckdb_interval:
        """Returns the interval value of the given value.

        * val: A duckdb_value containing a interval
        * returns: A duckdb_interval, or MinValue if the value cannot be converted
        """
        return self._duckdb_get_interval(val)

    fn duckdb_get_varchar(self, value: duckdb_value) -> UnsafePointer[c_char, MutExternalOrigin]:
        """Obtains a string representation of the given value.
        The result must be destroyed with `duckdb_free`.

        * value: The value
        * returns: The string value. This must be destroyed with `duckdb_free`.
        """
        return self._duckdb_get_varchar(value)

    fn duckdb_get_value_type(self, val: duckdb_value) -> duckdb_logical_type:
        """Returns the type of the given value. The type is valid as long as the value is
        not destroyed. The type itself must not be destroyed.

        * val: A duckdb_value
        * returns: A duckdb_logical_type.
        """
        return self._duckdb_get_value_type(val)

    fn duckdb_is_null_value(self, value: duckdb_value) -> Bool:
        """Returns whether the value's type is SQLNULL or not.

        * value: The value to check.
        * returns: True, if the value's type is SQLNULL, otherwise false.
        """
        return self._duckdb_is_null_value(value)

    fn duckdb_value_to_string(self, value: duckdb_value) -> UnsafePointer[c_char, MutExternalOrigin]:
        """Returns the SQL string representation of the given value.

        * value: A duckdb_value.
        * returns: The SQL string representation as a null-terminated string. The result must be freed with `duckdb_free`.
        """
        return self._duckdb_value_to_string(value)

comptime _duckdb_open = _dylib_function["duckdb_open", 
    fn (UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[duckdb_database, MutAnyOrigin]) -> UInt32
]

comptime _duckdb_open_ext = _dylib_function["duckdb_open_ext",
    fn (UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[duckdb_database, MutAnyOrigin], duckdb_config, UnsafePointer[UnsafePointer[c_char, MutAnyOrigin], MutAnyOrigin]) -> UInt32
]

comptime _duckdb_close = _dylib_function["duckdb_close",
    fn (UnsafePointer[duckdb_database, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_connect = _dylib_function[
    "duckdb_connect",
    fn (duckdb_database, UnsafePointer[duckdb_connection, MutAnyOrigin]) -> UInt32
]

comptime _duckdb_disconnect = _dylib_function["duckdb_disconnect",
    fn (UnsafePointer[duckdb_connection, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_free = _dylib_function["duckdb_free",
    fn (UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Query Execution
# ===--------------------------------------------------------------------===#

comptime _duckdb_query = _dylib_function["duckdb_query",
    fn (duckdb_connection, UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[duckdb_result, MutAnyOrigin]) -> UInt32
]

comptime _duckdb_destroy_result = _dylib_function["duckdb_destroy_result",
    fn (UnsafePointer[duckdb_result, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_column_name = _dylib_function["duckdb_column_name",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin], idx_t) -> UnsafePointer[c_char, ImmutExternalOrigin]
]

comptime _duckdb_column_type = _dylib_function["duckdb_column_type",
    fn (UnsafePointer[duckdb_result, MutAnyOrigin], idx_t) -> duckdb_type
]

comptime _duckdb_result_statement_type_ptr = _dylib_helpers_function["duckdb_result_statement_type_ptr",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> duckdb_statement_type
]

comptime _duckdb_column_logical_type = _dylib_function["duckdb_column_logical_type",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin], idx_t) -> duckdb_logical_type
]

comptime _duckdb_column_count = _dylib_function["duckdb_column_count",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> idx_t
]

comptime _duckdb_rows_changed = _dylib_function["duckdb_rows_changed",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> idx_t
]

comptime _duckdb_result_error = _dylib_function["duckdb_result_error",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> UnsafePointer[c_char, ImmutExternalOrigin]
]

comptime _duckdb_result_error_type = _dylib_function["duckdb_result_error_type",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> duckdb_error_type
]

comptime _duckdb_prepare_error = _dylib_function["duckdb_prepare_error",
    fn (duckdb_prepared_statement) -> UnsafePointer[c_char, ImmutExternalOrigin]
]

comptime _duckdb_row_count = _dylib_function["duckdb_row_count",
    fn (UnsafePointer[duckdb_result, MutAnyOrigin]) -> idx_t
]

# ===--------------------------------------------------------------------===#
# Result Functions
# ===--------------------------------------------------------------------===#

comptime _duckdb_result_return_type = _dylib_function["duckdb_result_return_type",
    fn (duckdb_result) -> duckdb_result_type
]

# ===--------------------------------------------------------------------===#
# Helpers
# ===--------------------------------------------------------------------===#

comptime _duckdb_vector_size = _dylib_function["duckdb_vector_size", fn () -> idx_t]

# ===--------------------------------------------------------------------===#
# Data Chunk Interface
# ===--------------------------------------------------------------------===#

comptime _duckdb_create_data_chunk = _dylib_function["duckdb_create_data_chunk",
    fn (UnsafePointer[duckdb_logical_type, ImmutAnyOrigin], idx_t) -> duckdb_data_chunk
]

comptime _duckdb_destroy_data_chunk = _dylib_function["duckdb_destroy_data_chunk",
    fn (UnsafePointer[duckdb_data_chunk, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_data_chunk_reset = _dylib_function["duckdb_data_chunk_reset",
    fn (duckdb_data_chunk) -> NoneType
]

comptime _duckdb_data_chunk_get_column_count = _dylib_function["duckdb_data_chunk_get_column_count",
    fn (duckdb_data_chunk) -> idx_t
]

comptime _duckdb_data_chunk_get_vector = _dylib_function["duckdb_data_chunk_get_vector",
    fn (duckdb_data_chunk, idx_t) -> duckdb_vector
]

comptime _duckdb_data_chunk_get_size = _dylib_function["duckdb_data_chunk_get_size",
    fn (duckdb_data_chunk) -> idx_t
]

comptime _duckdb_data_chunk_set_size = _dylib_function["duckdb_data_chunk_set_size",
    fn (duckdb_data_chunk, idx_t) -> NoneType
]

comptime _duckdb_from_date = _dylib_function["duckdb_from_date",
    fn (duckdb_date) -> duckdb_date_struct
]

comptime _duckdb_to_date = _dylib_function["duckdb_to_date",
    fn (duckdb_date_struct) -> duckdb_date
]

comptime _duckdb_is_finite_date = _dylib_function["duckdb_is_finite_date",
    fn (duckdb_date) -> Bool
]

comptime _duckdb_from_time = _dylib_function["duckdb_from_time",
    fn (duckdb_time) -> duckdb_time_struct
]

comptime _duckdb_create_time_tz = _dylib_function["duckdb_create_time_tz",
    fn (Int64, Int32) -> duckdb_time_tz
]

comptime _duckdb_from_time_tz = _dylib_function["duckdb_from_time_tz",
    fn (duckdb_time_tz) -> duckdb_time_tz_struct
]

comptime _duckdb_to_time = _dylib_function["duckdb_to_time",
    fn (duckdb_time_struct) -> duckdb_time
]

comptime _duckdb_to_timestamp = _dylib_function["duckdb_to_timestamp",
    fn (duckdb_timestamp_struct) -> duckdb_timestamp
]

comptime _duckdb_from_timestamp = _dylib_function["duckdb_from_timestamp",
    fn (duckdb_timestamp) -> duckdb_timestamp_struct
]

comptime _duckdb_is_finite_timestamp = _dylib_function["duckdb_is_finite_timestamp",
    fn (duckdb_timestamp) -> Bool
]

comptime _duckdb_create_vector = _dylib_function["duckdb_create_vector",
    fn (duckdb_logical_type, idx_t) -> duckdb_vector
]

comptime _duckdb_destroy_vector = _dylib_function["duckdb_destroy_vector",
    fn (UnsafePointer[duckdb_vector, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_vector_get_column_type = _dylib_function["duckdb_vector_get_column_type",
    fn (duckdb_vector) -> duckdb_logical_type
]

comptime _duckdb_vector_get_data = _dylib_function["duckdb_vector_get_data",
    fn (duckdb_vector) -> UnsafePointer[NoneType, MutExternalOrigin]
]

comptime _duckdb_vector_get_validity = _dylib_function["duckdb_vector_get_validity",
    fn (duckdb_vector) -> UnsafePointer[UInt64, MutExternalOrigin]
]

comptime _duckdb_vector_ensure_validity_writable = _dylib_function["duckdb_vector_ensure_validity_writable",
    fn (duckdb_vector) -> NoneType
]

comptime _duckdb_vector_assign_string_element = _dylib_function["duckdb_vector_assign_string_element",
    fn (duckdb_vector, idx_t, UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _duckdb_vector_assign_string_element_len = _dylib_function["duckdb_vector_assign_string_element_len",
    fn (duckdb_vector, idx_t, UnsafePointer[c_char, ImmutAnyOrigin], idx_t) -> NoneType
]

comptime _duckdb_list_vector_get_child = _dylib_function["duckdb_list_vector_get_child",
    fn (duckdb_vector) -> duckdb_vector
]

comptime _duckdb_list_vector_get_size = _dylib_function["duckdb_list_vector_get_size",
    fn (duckdb_vector) -> idx_t
]

comptime _duckdb_list_vector_set_size = _dylib_function["duckdb_list_vector_set_size",
    fn (duckdb_vector, idx_t) -> duckdb_state
]

comptime _duckdb_list_vector_reserve = _dylib_function["duckdb_list_vector_reserve",
    fn (duckdb_vector, idx_t) -> duckdb_state
]

comptime _duckdb_struct_vector_get_child = _dylib_function["duckdb_struct_vector_get_child",
    fn (duckdb_vector, idx_t) -> duckdb_vector
]

comptime _duckdb_array_vector_get_child = _dylib_function["duckdb_array_vector_get_child",
    fn (duckdb_vector) -> duckdb_vector
]

comptime _duckdb_slice_vector = _dylib_function["duckdb_slice_vector",
    fn (duckdb_vector, duckdb_selection_vector, idx_t) -> NoneType
]

comptime _duckdb_vector_copy_sel = _dylib_function["duckdb_vector_copy_sel",
    fn (duckdb_vector, duckdb_vector, duckdb_selection_vector, idx_t, idx_t, idx_t) -> NoneType
]

comptime _duckdb_vector_reference_value = _dylib_function["duckdb_vector_reference_value",
    fn (duckdb_vector, duckdb_value) -> NoneType
]

comptime _duckdb_vector_reference_vector = _dylib_function["duckdb_vector_reference_vector",
    fn (duckdb_vector, duckdb_vector) -> NoneType
]

# ===--------------------------------------------------------------------===
# Validity Mask Functions
# ===--------------------------------------------------------------------===

comptime _duckdb_validity_row_is_valid = _dylib_function["duckdb_validity_row_is_valid",
    fn (UnsafePointer[UInt64, MutExternalOrigin], idx_t) -> Bool
]

comptime _duckdb_validity_set_row_validity = _dylib_function["duckdb_validity_set_row_validity",
    fn (UnsafePointer[UInt64, MutExternalOrigin], idx_t, Bool) -> NoneType
]

comptime _duckdb_validity_set_row_invalid = _dylib_function["duckdb_validity_set_row_invalid",
    fn (UnsafePointer[UInt64, MutExternalOrigin], idx_t) -> NoneType
]

comptime _duckdb_validity_set_row_valid = _dylib_function["duckdb_validity_set_row_valid",
    fn (UnsafePointer[UInt64, MutExternalOrigin], idx_t) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Scalar Functions
# ===--------------------------------------------------------------------===#

comptime _duckdb_create_scalar_function = _dylib_function["duckdb_create_scalar_function",
    fn () -> duckdb_scalar_function
]

comptime _duckdb_destroy_scalar_function = _dylib_function["duckdb_destroy_scalar_function",
    fn (UnsafePointer[duckdb_scalar_function, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_scalar_function_set_name = _dylib_function["duckdb_scalar_function_set_name",
    fn (duckdb_scalar_function, UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _duckdb_scalar_function_set_varargs = _dylib_function["duckdb_scalar_function_set_varargs",
    fn (duckdb_scalar_function, duckdb_logical_type) -> NoneType
]

comptime _duckdb_scalar_function_set_special_handling = _dylib_function["duckdb_scalar_function_set_special_handling",
    fn (duckdb_scalar_function) -> NoneType
]

comptime _duckdb_scalar_function_set_volatile = _dylib_function["duckdb_scalar_function_set_volatile",
    fn (duckdb_scalar_function) -> NoneType
]

comptime _duckdb_scalar_function_add_parameter = _dylib_function["duckdb_scalar_function_add_parameter",
    fn (duckdb_scalar_function, duckdb_logical_type) -> NoneType
]

comptime _duckdb_scalar_function_set_return_type = _dylib_function["duckdb_scalar_function_set_return_type",
    fn (duckdb_scalar_function, duckdb_logical_type) -> NoneType
]

comptime _duckdb_scalar_function_set_extra_info = _dylib_function["duckdb_scalar_function_set_extra_info",
    fn (duckdb_scalar_function, UnsafePointer[NoneType, MutAnyOrigin], duckdb_delete_callback_t) -> NoneType
]

comptime _duckdb_scalar_function_set_bind = _dylib_function["duckdb_scalar_function_set_bind",
    fn (duckdb_scalar_function, duckdb_scalar_function_bind_t) -> NoneType
]

comptime _duckdb_scalar_function_set_bind_data = _dylib_function["duckdb_scalar_function_set_bind_data",
    fn (duckdb_bind_info, UnsafePointer[NoneType, MutAnyOrigin], duckdb_delete_callback_t) -> NoneType
]

comptime _duckdb_scalar_function_set_bind_data_copy = _dylib_function["duckdb_scalar_function_set_bind_data_copy",
    fn (duckdb_bind_info, duckdb_copy_callback_t) -> NoneType
]

comptime _duckdb_scalar_function_bind_set_error = _dylib_function["duckdb_scalar_function_bind_set_error",
    fn (duckdb_bind_info, UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _duckdb_scalar_function_set_function = _dylib_function["duckdb_scalar_function_set_function",
    fn (duckdb_scalar_function, duckdb_scalar_function_t) -> NoneType
]

comptime _duckdb_register_scalar_function = _dylib_function["duckdb_register_scalar_function",
    fn (duckdb_connection, duckdb_scalar_function) -> duckdb_state
]

comptime _duckdb_scalar_function_get_extra_info = _dylib_function["duckdb_scalar_function_get_extra_info",
    fn (duckdb_function_info) -> UnsafePointer[NoneType, MutExternalOrigin]
]

comptime _duckdb_scalar_function_bind_get_extra_info = _dylib_function["duckdb_scalar_function_bind_get_extra_info",
    fn (duckdb_bind_info) -> UnsafePointer[NoneType, MutExternalOrigin]
]

comptime _duckdb_scalar_function_get_bind_data = _dylib_function["duckdb_scalar_function_get_bind_data",
    fn (duckdb_function_info) -> UnsafePointer[NoneType, MutExternalOrigin]
]

comptime _duckdb_scalar_function_get_client_context = _dylib_function["duckdb_scalar_function_get_client_context",
    fn (duckdb_bind_info, UnsafePointer[duckdb_connection, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_scalar_function_set_error = _dylib_function["duckdb_scalar_function_set_error",
    fn (duckdb_function_info, UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _duckdb_create_scalar_function_set = _dylib_function["duckdb_create_scalar_function_set",
    fn (UnsafePointer[c_char, ImmutAnyOrigin]) -> duckdb_scalar_function_set
]

comptime _duckdb_destroy_scalar_function_set = _dylib_function["duckdb_destroy_scalar_function_set",
    fn (UnsafePointer[duckdb_scalar_function_set, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_add_scalar_function_to_set = _dylib_function["duckdb_add_scalar_function_to_set",
    fn (duckdb_scalar_function_set, duckdb_scalar_function) -> duckdb_state
]

comptime _duckdb_register_scalar_function_set = _dylib_function["duckdb_register_scalar_function_set",
    fn (duckdb_connection, duckdb_scalar_function_set) -> duckdb_state
]

comptime _duckdb_scalar_function_bind_get_argument_count = _dylib_function["duckdb_scalar_function_bind_get_argument_count",
    fn (duckdb_bind_info) -> idx_t
]

comptime _duckdb_scalar_function_bind_get_argument = _dylib_function["duckdb_scalar_function_bind_get_argument",
    fn (duckdb_bind_info, idx_t) -> duckdb_expression
]

# ===--------------------------------------------------------------------===#
# Aggregate Function Interface
# ===--------------------------------------------------------------------===#

comptime _duckdb_create_aggregate_function = _dylib_function["duckdb_create_aggregate_function",
    fn () -> duckdb_aggregate_function
]

comptime _duckdb_destroy_aggregate_function = _dylib_function["duckdb_destroy_aggregate_function",
    fn (UnsafePointer[duckdb_aggregate_function, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_aggregate_function_set_name = _dylib_function["duckdb_aggregate_function_set_name",
    fn (duckdb_aggregate_function, UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _duckdb_aggregate_function_add_parameter = _dylib_function["duckdb_aggregate_function_add_parameter",
    fn (duckdb_aggregate_function, duckdb_logical_type) -> NoneType
]

comptime _duckdb_aggregate_function_set_return_type = _dylib_function["duckdb_aggregate_function_set_return_type",
    fn (duckdb_aggregate_function, duckdb_logical_type) -> NoneType
]

comptime _duckdb_aggregate_function_set_functions = _dylib_function["duckdb_aggregate_function_set_functions",
    fn (
        duckdb_aggregate_function,
        duckdb_aggregate_state_size,
        duckdb_aggregate_init_t,
        duckdb_aggregate_update_t,
        duckdb_aggregate_combine_t,
        duckdb_aggregate_finalize_t
    ) -> NoneType
]

comptime _duckdb_aggregate_function_set_destructor = _dylib_function["duckdb_aggregate_function_set_destructor",
    fn (duckdb_aggregate_function, duckdb_aggregate_destroy_t) -> NoneType
]

comptime _duckdb_register_aggregate_function = _dylib_function["duckdb_register_aggregate_function",
    fn (duckdb_connection, duckdb_aggregate_function) -> duckdb_state
]

comptime _duckdb_aggregate_function_get_extra_info = _dylib_function["duckdb_aggregate_function_get_extra_info",
    fn (duckdb_function_info) -> UnsafePointer[NoneType, MutExternalOrigin]
]

comptime _duckdb_aggregate_function_set_error = _dylib_function["duckdb_aggregate_function_set_error",
    fn (duckdb_function_info, UnsafePointer[c_char, MutAnyOrigin]) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Logical Type Interface
# ===--------------------------------------------------------------------===#

comptime _duckdb_create_logical_type = _dylib_function["duckdb_create_logical_type",
    fn (duckdb_type) -> duckdb_logical_type
]

comptime _duckdb_create_list_type = _dylib_function["duckdb_create_list_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_create_array_type = _dylib_function["duckdb_create_array_type",
    fn (duckdb_logical_type, idx_t) -> duckdb_logical_type
]

comptime _duckdb_create_map_type = _dylib_function["duckdb_create_map_type",
    fn (duckdb_logical_type, duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_create_union_type = _dylib_function["duckdb_create_union_type",
    fn (UnsafePointer[duckdb_logical_type, ImmutAnyOrigin], UnsafePointer[UnsafePointer[c_char, ImmutAnyOrigin], ImmutAnyOrigin], idx_t) -> duckdb_logical_type
]

comptime _duckdb_create_struct_type = _dylib_function["duckdb_create_struct_type",
    fn (UnsafePointer[duckdb_logical_type, ImmutAnyOrigin], UnsafePointer[UnsafePointer[c_char, ImmutAnyOrigin], ImmutAnyOrigin], idx_t) -> duckdb_logical_type
]

# fn duckdb_create_enum_type TODO
# fn duckdb_create_decimal_type TODO

comptime _duckdb_get_type_id = _dylib_function["duckdb_get_type_id",
    fn (duckdb_logical_type) -> duckdb_type
]

comptime _duckdb_list_type_child_type = _dylib_function["duckdb_list_type_child_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_array_type_child_type = _dylib_function["duckdb_array_type_child_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_array_type_array_size = _dylib_function["duckdb_array_type_array_size",
    fn (duckdb_logical_type) -> idx_t
]

comptime _duckdb_map_type_key_type = _dylib_function["duckdb_map_type_key_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_map_type_value_type = _dylib_function["duckdb_map_type_value_type",
    fn (duckdb_logical_type) -> duckdb_logical_type
]

comptime _duckdb_destroy_logical_type = _dylib_function["duckdb_destroy_logical_type",
    fn (UnsafePointer[duckdb_logical_type, MutAnyOrigin]) -> None
]

comptime _duckdb_execution_is_finished = _dylib_function["duckdb_execution_is_finished",
    fn (duckdb_connection) -> Bool
]

comptime _duckdb_fetch_chunk_ptr = _dylib_helpers_function["duckdb_fetch_chunk_ptr",
    fn (UnsafePointer[duckdb_result, ImmutAnyOrigin]) -> duckdb_data_chunk
]

# ===--------------------------------------------------------------------===#
# Value Interface
# ===--------------------------------------------------------------------===#

comptime _duckdb_destroy_value = _dylib_function["duckdb_destroy_value",
    fn (UnsafePointer[duckdb_value, MutAnyOrigin]) -> NoneType
]

comptime _duckdb_create_varchar = _dylib_function["duckdb_create_varchar",
    fn (UnsafePointer[c_char, ImmutAnyOrigin]) -> duckdb_value
]

comptime _duckdb_create_varchar_length = _dylib_function["duckdb_create_varchar_length",
    fn (UnsafePointer[c_char, ImmutAnyOrigin], idx_t) -> duckdb_value
]

comptime _duckdb_create_bool = _dylib_function["duckdb_create_bool",
    fn (Bool) -> duckdb_value
]

comptime _duckdb_create_int8 = _dylib_function["duckdb_create_int8",
    fn (Int8) -> duckdb_value
]

comptime _duckdb_create_uint8 = _dylib_function["duckdb_create_uint8",
    fn (UInt8) -> duckdb_value
]

comptime _duckdb_create_int16 = _dylib_function["duckdb_create_int16",
    fn (Int16) -> duckdb_value
]

comptime _duckdb_create_uint16 = _dylib_function["duckdb_create_uint16",
    fn (UInt16) -> duckdb_value
]

comptime _duckdb_create_int32 = _dylib_function["duckdb_create_int32",
    fn (Int32) -> duckdb_value
]

comptime _duckdb_create_uint32 = _dylib_function["duckdb_create_uint32",
    fn (UInt32) -> duckdb_value
]

comptime _duckdb_create_int64 = _dylib_function["duckdb_create_int64",
    fn (Int64) -> duckdb_value
]

comptime _duckdb_create_uint64 = _dylib_function["duckdb_create_uint64",
    fn (UInt64) -> duckdb_value
]

comptime _duckdb_create_float = _dylib_function["duckdb_create_float",
    fn (Float32) -> duckdb_value
]

comptime _duckdb_create_double = _dylib_function["duckdb_create_double",
    fn (Float64) -> duckdb_value
]

comptime _duckdb_create_date = _dylib_function["duckdb_create_date",
    fn (duckdb_date) -> duckdb_value
]

comptime _duckdb_create_timestamp = _dylib_function["duckdb_create_timestamp",
    fn (duckdb_timestamp) -> duckdb_value
]

comptime _duckdb_create_interval = _dylib_function["duckdb_create_interval",
    fn (duckdb_interval) -> duckdb_value
]

comptime _duckdb_create_blob = _dylib_function["duckdb_create_blob",
    fn (UnsafePointer[UInt8, ImmutAnyOrigin], idx_t) -> duckdb_value
]

comptime _duckdb_create_null_value = _dylib_function["duckdb_create_null_value",
    fn () -> duckdb_value
]

comptime _duckdb_get_bool = _dylib_function["duckdb_get_bool",
    fn (duckdb_value) -> Bool
]

comptime _duckdb_get_int8 = _dylib_function["duckdb_get_int8",
    fn (duckdb_value) -> Int8
]

comptime _duckdb_get_uint8 = _dylib_function["duckdb_get_uint8",
    fn (duckdb_value) -> UInt8
]

comptime _duckdb_get_int16 = _dylib_function["duckdb_get_int16",
    fn (duckdb_value) -> Int16
]

comptime _duckdb_get_uint16 = _dylib_function["duckdb_get_uint16",
    fn (duckdb_value) -> UInt16
]

comptime _duckdb_get_int32 = _dylib_function["duckdb_get_int32",
    fn (duckdb_value) -> Int32
]

comptime _duckdb_get_uint32 = _dylib_function["duckdb_get_uint32",
    fn (duckdb_value) -> UInt32
]

comptime _duckdb_get_int64 = _dylib_function["duckdb_get_int64",
    fn (duckdb_value) -> Int64
]

comptime _duckdb_get_uint64 = _dylib_function["duckdb_get_uint64",
    fn (duckdb_value) -> UInt64
]

comptime _duckdb_get_float = _dylib_function["duckdb_get_float",
    fn (duckdb_value) -> Float32
]

comptime _duckdb_get_double = _dylib_function["duckdb_get_double",
    fn (duckdb_value) -> Float64
]

comptime _duckdb_get_date = _dylib_function["duckdb_get_date",
    fn (duckdb_value) -> duckdb_date
]

comptime _duckdb_get_timestamp = _dylib_function["duckdb_get_timestamp",
    fn (duckdb_value) -> duckdb_timestamp
]

comptime _duckdb_get_interval = _dylib_function["duckdb_get_interval",
    fn (duckdb_value) -> duckdb_interval
]

comptime _duckdb_get_varchar = _dylib_function["duckdb_get_varchar",
    fn (duckdb_value) -> UnsafePointer[c_char, MutExternalOrigin]
]

comptime _duckdb_get_value_type = _dylib_function["duckdb_get_value_type",
    fn (duckdb_value) -> duckdb_logical_type
]

comptime _duckdb_is_null_value = _dylib_function["duckdb_is_null_value",
    fn (duckdb_value) -> Bool
]

comptime _duckdb_value_to_string = _dylib_function["duckdb_value_to_string",
    fn (duckdb_value) -> UnsafePointer[c_char, MutExternalOrigin]
]
