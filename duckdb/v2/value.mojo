"""`Value`: an owned DuckDB value."""

from std.os import abort
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info
from duckdb.v2._runtime import api_error, builtin_factory, check, lib
from duckdb.v2.error import DuckDBError
from duckdb.v2.serde import from_value, to_value
from duckdb.v2.types import LogicalType


struct Value(Copyable, Movable, Writable):
    """An owned DuckDB value of any type, including NULL.

    ```mojo
    var v = Value(Int64(42))
    var n = v.get[Int64]()
    var s = String(Value("text"))
    var l = Value([Int32(1), 2, 3]).get[List[Int32]]()
    ```

    Any mapped type works, nested ones included (lists, dicts, structs,
    `Optional`, `Variant`).
    """

    var _handle: duckdb_v2_value_handle

    def __init__(out self, *, var _owned: duckdb_v2_value_handle):
        self._handle = _owned

    def __init__[T: Movable & Deinitable](out self, value: T) raises DuckDBError:
        """A value from any mapped Mojo type, created with the built-in factory."""
        self._handle = to_value(value, builtin_factory())

    @staticmethod
    def null(type: LogicalType) raises DuckDBError -> Self:
        """A typed NULL."""
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_value_create_null(builtin_factory(), type.handle(), Pointer(to=v), Pointer(to=err)), err)
        return Self(_owned=v)

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __init__(out self, *, copy: Self):
        """A copy (DuckDB has no value copy; a cast to the value's own type makes one)."""
        self._handle = null[duckdb_v2_value_handle]()
        try:
            var t = copy.type()
            var err = null_error_info()
            check(lib().duckdb_v2_value_cast(builtin_factory(), copy._handle, t.handle(), Pointer(to=self._handle), Pointer(to=err)), err)
            _ = t^
        except e:
            abort(String("Failed to copy a value: ", e))

    def cast(self, type: LogicalType) raises DuckDBError -> Value:
        """This value cast to `type` (raises when DuckDB cannot convert it)."""
        var out = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_value_cast(builtin_factory(), self._handle, type.handle(), Pointer(to=out), Pointer(to=err)), err)
        return Value(_owned=out)

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_value_destroy(Pointer(to=self._handle))

    def handle(self) -> duckdb_v2_value_handle:
        """The underlying V2 handle (borrowed; valid while `self` lives)."""
        return self._handle

    def is_null(self) raises DuckDBError -> Bool:
        var out = False
        var err = null_error_info()
        check(lib().duckdb_v2_value_is_null(self._handle, Pointer(to=out), Pointer(to=err)), err)
        return out

    def type(self) raises DuckDBError -> LogicalType:
        var t = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_value_get_logical_type(self._handle, Pointer(to=t), Pointer(to=err)), err)
        return LogicalType(_owned=t)

    def get[T: Movable & Deinitable](self) raises DuckDBError -> T:
        """The value as `T`. Raises on NULL and when DuckDB cannot convert it."""
        if self.is_null():
            raise api_error("the value is NULL")
        return from_value[T](self._handle)

    def get_optional[T: Movable & Deinitable](self) raises DuckDBError -> Optional[T]:
        """The value as `T`, or `None` for NULL."""
        if self.is_null():
            return None
        return Optional[T](from_value[T](self._handle))

    def to_string(self) raises DuckDBError -> String:
        """DuckDB's text form of the value (diagnostic; `NULL` for NULL)."""
        var err = null_error_info()
        var needed: idx_t = 0
        check(
            lib().duckdb_v2_value_to_string(
                self._handle, null[Pointer[c_char, MutUntrackedOrigin]](), 0, Pointer(to=needed), Pointer(to=err)
            ),
            err,
        )
        var buf = List[UInt8](length=Int(needed) + 1, fill=0)
        check(
            lib().duckdb_v2_value_to_string(
                self._handle, buf.unsafe_ptr().unsafe_bitcast[c_char](), needed + 1, Pointer(to=needed), Pointer(to=err)
            ),
            err,
        )
        return String(from_utf8_lossy=Span(buf)[: Int(needed)])

    def write_to(self, mut writer: Some[Writer]):
        try:
            writer.write(self.to_string())
        except:
            writer.write("<value>")
