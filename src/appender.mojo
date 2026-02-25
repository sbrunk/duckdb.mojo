"""High-level Appender API for efficiently loading data into DuckDB.

Appenders are the most efficient way of loading data into DuckDB, and are
recommended for fast data loading. The appender is much faster than using
prepared statements or individual ``INSERT INTO`` statements.

This module provides a type-safe, idiomatic Mojo API that uses compile-time
reflection to map Mojo structs and tuples to DuckDB rows automatically.

Example — appending individual values:
```mojo
var con = DuckDB.connect(":memory:")
_ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")
var appender = Appender(con, "people")
appender.append_value(Int32(1))
appender.append_value("Mark")
appender.end_row()
appender.append_value(Int32(2))
appender.append_value("Hannes")
appender.end_row()
# appender is flushed and destroyed automatically
```

Example — appending structs:
```mojo
@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String

var appender = Appender(con, "people")
appender.append_row(Person(1, "Mark"))
appender.append_row(Person(2, "Hannes"))
```

Example — appending tuples:
```mojo
var appender = Appender(con, "people")
appender.append_tuple_row(Tuple(Int32(1), String("Mark")))
```

Example — appending from a list:
```mojo
var people = List[Person](Person(1, "Mark"), Person(2, "Hannes"))
var appender = Appender(con, "people")
appender.append_rows(people)
```

Example — nullable columns with Optional:
```mojo
@fieldwise_init
struct PersonOpt(Copyable, Movable):
    var id: Int32
    var name: Optional[String]

var appender = Appender(con, "people")
appender.append_row(PersonOpt(1, String("Mark")))
appender.append_row(PersonOpt(2, None))
```
"""

from sys.intrinsics import _type_is_eq
from sys.info import size_of
from reflection import (
    get_base_type_name,
    get_type_name,
    is_struct_type,
    struct_field_count,
    struct_field_names,
    struct_field_types,
)
from collections import Optional, List
from std.builtin.rebind import downcast, rebind_var, trait_downcast
from duckdb._libduckdb import *
from duckdb.duckdb_type import *
from duckdb.api import DuckDB, _get_duckdb_interface
from duckdb.connection import Connection
from duckdb.typed_api import (
    mojo_type_to_duckdb_type,
    _is_known_scalar_type,
    _scalar_type_to_duckdb,
)


# ──────────────────────────────────────────────────────────────────
# Appendable trait — high-level interface for appending typed values
# ──────────────────────────────────────────────────────────────────


trait Appendable:
    """Trait for types that can be appended to a DuckDB Appender.

    Implement this trait to make a type appendable via `Appender.append_value`.
    """

    fn append(ref self, mut appender: Appender) raises:
        """Append this value to the given appender."""
        ...


# ──────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────


fn _get_appender_error(raw: duckdb_appender) -> String:
    """Return the current appender error message, or empty string."""
    ref libduckdb = DuckDB().libduckdb()
    var error_data = libduckdb.duckdb_appender_error_data(raw)
    if not libduckdb.duckdb_error_data_has_error(error_data):
        libduckdb.duckdb_destroy_error_data(
            UnsafePointer(to=error_data)
        )
        return ""
    var msg_ptr = libduckdb.duckdb_error_data_message(error_data)
    var msg = String("")
    if msg_ptr:
        msg = String(unsafe_from_utf8_ptr=msg_ptr)
    libduckdb.duckdb_destroy_error_data(UnsafePointer(to=error_data))
    return msg


# ──────────────────────────────────────────────────────────────────
# Appendable extension conformances
# ──────────────────────────────────────────────────────────────────


__extension Bool(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_bool(appender._appender, self))


# Use SIMD extension to cover all numeric types at once:
# Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64
__extension SIMD(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        constrained[size == 1, "Only scalar SIMD (size=1) can be appended"]()
        ref libduckdb = DuckDB().libduckdb()
        var raw = appender._appender

        @parameter
        if _type_is_eq[Self, Int8]():
            appender._check(libduckdb.duckdb_append_int8(raw, rebind_var[Int8](self)))
        elif _type_is_eq[Self, Int16]():
            appender._check(libduckdb.duckdb_append_int16(raw, rebind_var[Int16](self)))
        elif _type_is_eq[Self, Int32]():
            appender._check(libduckdb.duckdb_append_int32(raw, rebind_var[Int32](self)))
        elif _type_is_eq[Self, Int64]():
            appender._check(libduckdb.duckdb_append_int64(raw, rebind_var[Int64](self)))
        elif _type_is_eq[Self, UInt8]():
            appender._check(libduckdb.duckdb_append_uint8(raw, rebind_var[UInt8](self)))
        elif _type_is_eq[Self, UInt16]():
            appender._check(libduckdb.duckdb_append_uint16(raw, rebind_var[UInt16](self)))
        elif _type_is_eq[Self, UInt32]():
            appender._check(libduckdb.duckdb_append_uint32(raw, rebind_var[UInt32](self)))
        elif _type_is_eq[Self, UInt64]():
            appender._check(libduckdb.duckdb_append_uint64(raw, rebind_var[UInt64](self)))
        elif _type_is_eq[Self, Float32]():
            appender._check(libduckdb.duckdb_append_float(raw, rebind_var[Float32](self)))
        elif _type_is_eq[Self, Float64]():
            appender._check(libduckdb.duckdb_append_double(raw, rebind_var[Float64](self)))
        else:
            constrained[False, "Unsupported SIMD DType for appender"]()


__extension String(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        var copy = String(self)
        appender._check(
            libduckdb.duckdb_append_varchar(
                appender._appender, copy.as_c_string_slice().unsafe_ptr()
            ),
        )


__extension Date(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_date(appender._appender, self))


__extension Time(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(libduckdb.duckdb_append_time(appender._appender, self))


__extension Timestamp(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        appender._check(
            libduckdb.duckdb_append_timestamp(appender._appender, self)
        )


__extension Interval(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        ref libduckdb = DuckDB().libduckdb()
        # Interval (months: Int32, days: Int32, micros: Int64) must be
        # reinterpreted as duckdb_interval (months_days: Int64, micros: Int64)
        # via memory bitcast, same pattern as src/value.mojo
        appender._check(
            libduckdb.duckdb_append_interval(
                appender._appender,
                UnsafePointer(to=self).bitcast[duckdb_interval]()[],
            ),
        )


__extension Optional(Appendable):
    fn append(ref self, mut appender: Appender) raises:
        if self:
            @parameter
            if conforms_to(Self.T, Appendable):
                trait_downcast[Appendable](self.value()).append(appender)
            else:
                raise Error(
                    "Unsupported inner type for Optional in appender"
                )
        else:
            appender.append_null()


# ──────────────────────────────────────────────────────────────────
# Appender — high-level API
# ──────────────────────────────────────────────────────────────────


struct Appender(Movable):
    """Efficiently load data into a DuckDB table.

    Appenders are the fastest way to insert data into DuckDB from Mojo.
    They support appending individual values, complete rows as structs or
    tuples, and bulk-appending from lists.

    ``Optional`` fields/values map to SQL ``NULL``.

    The appender is automatically flushed and destroyed when it goes out
    of scope.
    """

    var _appender: duckdb_appender

    # ── Construction / Destruction ────────────────────────────────

    fn __init__(out self, ref con: Connection, table: String, schema: String = "") raises:
        """Create an appender for the given table.

        Args:
            con: An open DuckDB connection.
            table: The table name to append to.
            schema: The schema name (empty string for default schema).
        """
        self._appender = duckdb_appender()
        var _table = table.copy()
        ref libduckdb = DuckDB().libduckdb()

        var state: duckdb_state
        if schema == "":
            state = libduckdb.duckdb_appender_create(
                con._conn,
                UnsafePointer[c_char, ImmutAnyOrigin](),
                _table.as_c_string_slice().unsafe_ptr(),
                UnsafePointer(to=self._appender),
            )
        else:
            var _schema = schema.copy()
            state = libduckdb.duckdb_appender_create(
                con._conn,
                _schema.as_c_string_slice().unsafe_ptr(),
                _table.as_c_string_slice().unsafe_ptr(),
                UnsafePointer(to=self._appender),
            )
        if state == DuckDBError:
            var err = _get_appender_error(self._appender)
            # Still need to destroy even on error
            _ = libduckdb.duckdb_appender_destroy(UnsafePointer(to=self._appender))
            raise Error("Failed to create appender: " + err)

    fn __moveinit__(out self, deinit take: Self):
        self._appender = take._appender

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        # duckdb_appender_destroy flushes, closes, and frees
        _ = libduckdb.duckdb_appender_destroy(UnsafePointer(to=self._appender))

    # ── Error handling ────────────────────────────────────────────

    fn _check(self, state: duckdb_state) raises:
        """Check a return state and raise on error."""
        if state == DuckDBError:
            raise Error("Appender error: " + _get_appender_error(self._appender))

    # ── Low-level value appending ─────────────────────────────────

    fn append_null(mut self) raises:
        """Append a NULL value for the current column."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_append_null(self._appender))

    fn append_default(mut self) raises:
        """Append the DEFAULT value for the current column."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_append_default(self._appender))

    fn append_value[T: Copyable & Movable](mut self, value: T) raises:
        """Append a single typed value to the current row.

        Supports all DuckDB-mappable scalar types, String, and Optional[T]
        (which appends NULL for None).

        Parameters:
            T: The Mojo type of the value.

        Args:
            value: The value to append.
        """
        @parameter
        if conforms_to(T, Appendable):
            trait_downcast[Appendable](value).append(self)
        else:
            raise Error(
                "Unsupported type for appender: "
                + String(get_type_name[T]())
            )

    # ── Row-level appending ───────────────────────────────────────

    fn end_row(mut self) raises:
        """Finish the current row and advance to the next."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_end_row(self._appender))

    fn begin_row(mut self) raises:
        """Begin a new row (optional — rows begin automatically)."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_begin_row(self._appender))

    fn append_row[T: Copyable & Movable](mut self, row: T) raises:
        """Append a struct as a complete table row.

        Each struct field is mapped to a column by position. Field types
        must be DuckDB-mappable. ``Optional`` fields become ``NULL``.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            row: The struct instance to append.

        Example:
            ```mojo
            @fieldwise_init
            struct Person(Copyable, Movable):
                var id: Int32
                var name: String

            appender.append_row(Person(1, "Mark"))
            ```
        """
        constrained[
            mojo_type_to_duckdb_type[T]() == DuckDBType.struct_t,
            "append_row[T] requires a struct type. For scalar values use append_value.",
        ]()
        self._append_struct_fields(row)
        self.end_row()

    fn _append_struct_fields[T: Copyable & Movable](mut self, ref row: T) raises:
        """Append all fields of a struct as values in the current row."""
        comptime field_count = struct_field_count[T]()

        @parameter
        for idx in range(field_count):
            comptime FieldType = struct_field_types[T]()[idx]

            @parameter
            if conforms_to(FieldType, Appendable):
                trait_downcast[Appendable](
                    __struct_field_ref(idx, row)
                ).append(self)
            else:
                comptime field_name = struct_field_names[T]()[idx]
                raise Error(
                    "Unsupported field type for appender: field '"
                    + String(field_name)
                    + "'"
                )

    fn append_tuple_row[*Ts: Copyable & Movable](mut self, row: Tuple[*Ts]) raises:
        """Append a tuple as a complete table row.

        Each tuple element maps to a column by position.

        Parameters:
            Ts: The types of the tuple elements.

        Args:
            row: The tuple to append.

        Example:
            ```mojo
            appender.append_tuple_row(Tuple(Int32(1), String("Mark")))
            ```
        """
        self._append_tuple_elements(row)
        self.end_row()

    fn _append_tuple_elements[
        *Ts: Copyable & Movable
    ](mut self, ref row: Tuple[*Ts]) raises:
        """Append all elements of a tuple as values in the current row."""
        comptime T = Tuple[*Ts]
        comptime n = T.__len__()

        @parameter
        for idx in range(n):
            comptime ET = T.element_types[idx]

            @parameter
            if conforms_to(ET, Appendable):
                trait_downcast[Appendable](row[idx]).append(
                    self
                )
            else:
                raise Error(
                    "Unsupported tuple element type at index "
                    + String(idx)
                )

    # ── Bulk appending ────────────────────────────────────────────

    fn append_rows[T: Copyable & Movable](mut self, rows: List[T]) raises:
        """Append multiple structs as table rows.

        Parameters:
            T: A Mojo struct whose fields correspond to table columns.

        Args:
            rows: A list of struct instances to append.

        Example:
            ```mojo
            var people = List[Person](Person(1, "Mark"), Person(2, "Hannes"))
            appender.append_rows(people)
            ```
        """
        for i in range(len(rows)):
            self.append_row(rows[i])

    # ── Flush / Close ─────────────────────────────────────────────

    fn flush(mut self) raises:
        """Flush the appender to the table, forcing the cache of the
        appender to be cleared. If flushing fails, the appender remains
        valid. Subsequent rows can still be appended after flushing.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_flush(self._appender))

    fn close(mut self) raises:
        """Close the appender. Unflushed data is flushed first.

        After closing, no more rows can be appended.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._check(libduckdb.duckdb_appender_close(self._appender))

    # ── Metadata ──────────────────────────────────────────────────

    fn column_count(self) -> Int:
        """Return the number of columns in the appender's target table."""
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_appender_column_count(self._appender))
