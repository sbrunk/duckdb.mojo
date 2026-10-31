"""Errors raised by the package."""

from duckdb._libduckdb_v2 import *


@fieldwise_init
struct ErrorCode(Equatable, ImplicitlyCopyable, Writable):
    """A DuckDB V2 error code (`DUCKDB_V2_ERROR`), grouped by range.

    The thousands digit is the category: 1 IO, 2 input, 3 resource, 4 type,
    5 query, 6 database, 7 configuration, 8 runtime. `API` (1) is the generic
    "the call failed" code.
    """

    var value: DUCKDB_V2_ERROR

    comptime NONE = Self(DUCKDB_V2_ERROR_NONE)
    comptime API = Self(DUCKDB_V2_ERROR_API)
    comptime INPUT_INVALID = Self(DUCKDB_V2_ERROR_INPUT_INVALID)
    comptime RESOURCE_IN_USE = Self(DUCKDB_V2_ERROR_RESOURCE_IN_USE)
    comptime TYPE_MISMATCH = Self(DUCKDB_V2_ERROR_TYPE_MISMATCH)
    comptime TYPE_CONVERSION = Self(DUCKDB_V2_ERROR_TYPE_CONVERSION)
    comptime CATALOG = Self(DUCKDB_V2_ERROR_DATABASE_CATALOG)
    comptime INTERRUPT = Self(DUCKDB_V2_ERROR_RUNTIME_INTERRUPT)

    def category(self) -> Int:
        return Int(self.value) // 1000

    def is_io(self) -> Bool:
        return self.category() == 1

    def is_input(self) -> Bool:
        return self.category() == 2

    def is_resource(self) -> Bool:
        return self.category() == 3

    def is_type(self) -> Bool:
        return self.category() == 4

    def is_query(self) -> Bool:
        return self.category() == 5

    def is_database(self) -> Bool:
        return self.category() == 6

    def is_configuration(self) -> Bool:
        return self.category() == 7

    def is_runtime(self) -> Bool:
        return self.category() == 8

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ErrorCode(", self.value, ")")


struct DuckDBError(Copyable, Movable, Writable):
    """The error every call raises: a code and DuckDB's message."""

    var code: ErrorCode
    var message: String

    def __init__(out self, code: ErrorCode, var message: String):
        self.code = code
        self.message = message^

    def write_to(self, mut writer: Some[Writer]):
        writer.write(self.message)
