"""Typed API for converting DuckDB results to native Mojo types.

This module provides a type-safe API for extracting typed data from DuckDB
results. Calling ``chunk.get[T](col, row)`` returns a single value, while
``chunk.get[T](col)`` returns the entire column.

Scalar types:
```mojo
var val = chunk.get[Int64](col=0, row=0)   # Optional[Int64]
var col = chunk.get[Int64](col=0)          # List[Optional[Int64]]
```

Mojo native Int and UInt (platform-dependent width):
```mojo
result = con.execute("SELECT 42::BIGINT")
var chunk = result.fetch_chunk()
var v = chunk.get[Int](col=0, row=0)       # works on 64-bit platforms
```

Lists (DuckDB ``LIST``):
```mojo
result = con.execute("SELECT [1, 2, 3]::LIST(INTEGER)")
var chunk = result.fetch_chunk()
var lst = chunk.get[List[Int32]](col=0, row=0)
```

Structs — uses reflection to map fields from DuckDB ``STRUCT`` to Mojo struct:
```mojo
@fieldwise_init
struct Point(Copyable, Movable):
    var x: Float64
    var y: Float64

result = con.execute("SELECT {'x': 1.0, 'y': 2.0}::STRUCT(x DOUBLE, y DOUBLE)")
var chunk = result.fetch_chunk()
var pt = chunk.get[Point](col=0, row=0)
```

Arrays (DuckDB ``ARRAY`` — fixed-size lists):
```mojo
result = con.execute("SELECT [1, 2, 3]::INTEGER[3]")
var chunk = result.fetch_chunk()
var arr = chunk.get[List[Int32]](col=0, row=0)  # read as List
```

MAPs as Dict:
```mojo
result = con.execute("SELECT MAP {'a': 1, 'b': 2}")
var chunk = result.fetch_chunk()
var d = chunk.get[Dict[String, Int32]](col=0, row=0)
# d["a"] == 1, d["b"] == 2
```

UNIONs as Variant:
```mojo
result = con.execute("SELECT union_value(i := 42)::UNION(i INTEGER, s VARCHAR)")
var chunk = result.fetch_chunk()
var v = chunk.get[Variant[Int32, String]](col=0, row=0)
# v[Int32] == 42
```

Key features:
- Compile-time type mapping from Mojo types to DuckDB types
- Automatic null handling with Optional types
- Support for nested types (List, Dict, Variant, user-defined structs)
- Reflection-based struct deserialization
- Pure Mojo ``MojoType`` descriptor that mirrors DuckDB's LogicalType
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
from collections import Optional, List, Dict
from utils import Variant
from builtin.variadics import Variadic
from memory import alloc, memcpy
from std.builtin.rebind import downcast
from duckdb._libduckdb import *
from duckdb.duckdb_type import *
from duckdb.api import DuckDB
from duckdb.vector import Vector
from duckdb.logical_type import LogicalType, struct_type


# ──────────────────────────────────────────────────────────────────
# MojoType — a pure Mojo representation of DuckDB logical types
# ──────────────────────────────────────────────────────────────────


struct MojoType(Copyable, Movable, Writable, Stringable):
    """A pure-Mojo descriptor that mirrors DuckDB's LogicalType.

    This allows representing DuckDB types (including nested ones like LIST
    and STRUCT) entirely in Mojo, constructible at compile time from Mojo type
    parameters via `mojo_logical_type[T]()`.

    The descriptor can be converted to DuckDB's runtime LogicalType via
    `to_logical_type()` when needed for C-API interop.
    """

    var type_id: DuckDBType
    """The base DuckDB type id."""

    var children: List[MojoType]
    """Child types (list element, struct fields, etc.)."""

    var field_names: List[String]
    """Field names (for STRUCT types). Empty for non-struct types."""

    # -- Constructors ---------------------------------------------------

    fn __init__(out self, type_id: DuckDBType):
        """Create a scalar (leaf) MojoType."""
        self.type_id = type_id
        self.children = List[MojoType]()
        self.field_names = List[String]()

    @staticmethod
    fn list_of(var element: MojoType) -> MojoType:
        """Create a LIST type wrapping the given element type."""
        var mt = MojoType(DuckDBType.list)
        mt.children.append(element^)
        return mt^

    @staticmethod
    fn struct_of(var names: List[String], var types: List[MojoType]) -> MojoType:
        """Create a STRUCT type with the given field names and types.

        Args:
            names: Field names matching the struct's field order.
            types: Corresponding DuckDB types for each field.
        """
        var mt = MojoType(DuckDBType.struct_t)
        mt.field_names = names^
        mt.children = types^
        return mt^

    @staticmethod
    fn array_of(var element: MojoType, size: Int) -> MojoType:
        """Create an ARRAY type wrapping the given element type with fixed size.

        Args:
            element: The element type.
            size: The fixed number of elements per row.
        """
        var mt = MojoType(DuckDBType.array)
        mt.children.append(element^)
        # Encode the array size as a second child MojoType (type_id stores size)
        mt.field_names.append(String(size))
        return mt^

    @staticmethod
    fn map_of(var key_type: MojoType, var value_type: MojoType) -> MojoType:
        """Create a MAP type with the given key and value types.

        Args:
            key_type: The key type.
            value_type: The value type.
        """
        var mt = MojoType(DuckDBType.map)
        mt.children.append(key_type^)
        mt.children.append(value_type^)
        return mt^

    # -- Conversions ----------------------------------------------------

    fn to_logical_type(self) -> LogicalType[True, MutExternalOrigin]:
        """Convert this MojoType to a DuckDB runtime LogicalType.

        Returns a new *owned* LogicalType that the caller must manage.
        """
        if self.type_id == DuckDBType.list:
            var child_lt = self.children[0].to_logical_type()
            return child_lt.create_list_type()
        elif self.type_id == DuckDBType.array:
            var child_lt = self.children[0].to_logical_type()
            ref libduckdb = DuckDB().libduckdb()
            var size: Int
            try:
                size = atol(self.field_names[0])
            except:
                size = 0
            return LogicalType[True, MutExternalOrigin](
                libduckdb.duckdb_create_array_type(
                    child_lt.internal_ptr(), idx_t(size)
                )
            )
        elif self.type_id == DuckDBType.map:
            var key_lt = self.children[0].to_logical_type()
            var val_lt = self.children[1].to_logical_type()
            ref libduckdb = DuckDB().libduckdb()
            return LogicalType[True, MutExternalOrigin](
                libduckdb.duckdb_create_map_type(
                    key_lt.internal_ptr(), val_lt.internal_ptr()
                )
            )
        elif self.type_id == DuckDBType.struct_t:
            var child_types = List[LogicalType[True, MutExternalOrigin]]()
            for i in range(len(self.children)):
                child_types.append(self.children[i].to_logical_type())
            var names = List[String]()
            for i in range(len(self.field_names)):
                names.append(self.field_names[i])
            return struct_type(names, child_types)
        else:
            return LogicalType[True, MutExternalOrigin](self.type_id)

    # -- Display --------------------------------------------------------

    fn __str__(self) -> String:
        return String.write(self)

    fn write_to[W: Writer](self, mut writer: W):
        if self.type_id == DuckDBType.list:
            writer.write("list(", self.children[0], ")")
        elif self.type_id == DuckDBType.array:
            writer.write(
                "array(", self.children[0], ", ", self.field_names[0], ")"
            )
        elif self.type_id == DuckDBType.map:
            writer.write(
                "map(", self.children[0], ", ", self.children[1], ")"
            )
        elif self.type_id == DuckDBType.struct_t:
            writer.write("struct(")
            for i in range(len(self.field_names)):
                if i > 0:
                    writer.write(", ")
                writer.write(self.field_names[i], " ", self.children[i])
            writer.write(")")
        else:
            writer.write(self.type_id)


# ──────────────────────────────────────────────────────────────────
# Compile-time type mapping
# ──────────────────────────────────────────────────────────────────


fn _is_list_compatible_type(actual: DuckDBType) -> Bool:
    """Check if a DuckDB type is compatible with Mojo List[T].

    LIST, ARRAY, MAP, and BLOB columns can all be deserialized into List[T].
    """
    return (
        actual == DuckDBType.list
        or actual == DuckDBType.array
        or actual == DuckDBType.map
        or actual == DuckDBType.blob
    )


fn _is_known_scalar_type[T: Copyable & Movable]() -> Bool:
    """Returns True if T is one of the known DuckDB-mappable scalar types."""
    @parameter
    if (
        _type_is_eq[T, Bool]()
        or _type_is_eq[T, Int8]()
        or _type_is_eq[T, Int16]()
        or _type_is_eq[T, Int32]()
        or _type_is_eq[T, Int64]()
        or _type_is_eq[T, Int]()
        or _type_is_eq[T, UInt8]()
        or _type_is_eq[T, UInt16]()
        or _type_is_eq[T, UInt32]()
        or _type_is_eq[T, UInt64]()
        or _type_is_eq[T, UInt]()
        or _type_is_eq[T, Float32]()
        or _type_is_eq[T, Float64]()
        or _type_is_eq[T, String]()
        or _type_is_eq[T, Date]()
        or _type_is_eq[T, Time]()
        or _type_is_eq[T, Timestamp]()
        or _type_is_eq[T, Interval]()
        or _type_is_eq[T, Int128]()
        or _type_is_eq[T, UInt128]()
        or _type_is_eq[T, Decimal]()
        or _type_is_eq[T, TimestampS]()
        or _type_is_eq[T, TimestampMS]()
        or _type_is_eq[T, TimestampNS]()
        or _type_is_eq[T, TimestampTZ]()
        or _type_is_eq[T, TimeTZ]()
        or _type_is_eq[T, UUID]()
        or _type_is_eq[T, TimeNS]()
    ):
        return True
    else:
        return False


fn mojo_type_to_duckdb_type[T: Copyable & Movable]() -> DuckDBType:
    """Maps a Mojo type to its corresponding DuckDB type at compile time.

    Supports scalar types, Date/Time/Timestamp/Interval, String, List[T],
    and user-defined structs (mapped to DuckDBType.struct_t).

    Parameters:
        T: The Mojo type to map.

    Returns:
        The corresponding DuckDBType.
    """
    @parameter
    if _type_is_eq[T, Bool]():
        return DuckDBType.boolean
    elif _type_is_eq[T, Int8]():
        return DuckDBType.tinyint
    elif _type_is_eq[T, Int16]():
        return DuckDBType.smallint
    elif _type_is_eq[T, Int32]():
        return DuckDBType.integer
    elif _type_is_eq[T, Int64]():
        return DuckDBType.bigint
    elif _type_is_eq[T, Int]():
        # Int is platform-dependent: 32-bit or 64-bit
        @parameter
        if size_of[Int]() == 4:
            return DuckDBType.integer
        else:
            return DuckDBType.bigint
    elif _type_is_eq[T, UInt8]():
        return DuckDBType.utinyint
    elif _type_is_eq[T, UInt16]():
        return DuckDBType.usmallint
    elif _type_is_eq[T, UInt32]():
        return DuckDBType.uinteger
    elif _type_is_eq[T, UInt64]():
        return DuckDBType.ubigint
    elif _type_is_eq[T, UInt]():
        # UInt is platform-dependent: 32-bit or 64-bit
        @parameter
        if size_of[UInt]() == 4:
            return DuckDBType.uinteger
        else:
            return DuckDBType.ubigint
    elif _type_is_eq[T, Float32]():
        return DuckDBType.float
    elif _type_is_eq[T, Float64]():
        return DuckDBType.double
    elif _type_is_eq[T, String]():
        return DuckDBType.varchar
    elif _type_is_eq[T, Date]():
        return DuckDBType.date
    elif _type_is_eq[T, Time]():
        return DuckDBType.time
    elif _type_is_eq[T, Timestamp]():
        return DuckDBType.timestamp
    elif _type_is_eq[T, Interval]():
        return DuckDBType.interval
    elif _type_is_eq[T, Int128]():
        return DuckDBType.hugeint
    elif _type_is_eq[T, UInt128]():
        return DuckDBType.uhugeint
    elif _type_is_eq[T, Decimal]():
        return DuckDBType.decimal
    elif _type_is_eq[T, TimestampS]():
        return DuckDBType.timestamp_s
    elif _type_is_eq[T, TimestampMS]():
        return DuckDBType.timestamp_ms
    elif _type_is_eq[T, TimestampNS]():
        return DuckDBType.timestamp_ns
    elif _type_is_eq[T, TimestampTZ]():
        return DuckDBType.timestamp_tz
    elif _type_is_eq[T, TimeTZ]():
        return DuckDBType.time_tz
    elif _type_is_eq[T, UUID]():
        return DuckDBType.uuid
    elif _type_is_eq[T, TimeNS]():
        return DuckDBType.time_ns
    else:
        comptime base_name = get_base_type_name[T]()
        @parameter
        if base_name == "List":
            return DuckDBType.list
        elif base_name == "Dict":
            return DuckDBType.map
        elif base_name == "Variant":
            return DuckDBType.union
        elif base_name == "Optional":
            constrained[
                False,
                "Optional types should be unwrapped before mapping."
                " Use the element type directly.",
            ]()
            return DuckDBType.invalid
        else:
            # Treat any remaining struct type as DuckDB STRUCT
            return DuckDBType.struct_t


fn _scalar_type_to_duckdb[T: AnyType]() -> DuckDBType:
    """Map a field type (from struct_field_types) to DuckDBType.

    Used during struct reflection where field types come from
    `struct_field_types` and may not satisfy Copyable & Movable.

    Parameters:
        T: The field type.

    Returns:
        The DuckDBType for this field.
    """
    @parameter
    if _type_is_eq[T, Bool]():
        return DuckDBType.boolean
    elif _type_is_eq[T, Int8]():
        return DuckDBType.tinyint
    elif _type_is_eq[T, Int16]():
        return DuckDBType.smallint
    elif _type_is_eq[T, Int32]():
        return DuckDBType.integer
    elif _type_is_eq[T, Int64]():
        return DuckDBType.bigint
    elif _type_is_eq[T, Int]():
        @parameter
        if size_of[Int]() == 4:
            return DuckDBType.integer
        else:
            return DuckDBType.bigint
    elif _type_is_eq[T, UInt8]():
        return DuckDBType.utinyint
    elif _type_is_eq[T, UInt16]():
        return DuckDBType.usmallint
    elif _type_is_eq[T, UInt32]():
        return DuckDBType.uinteger
    elif _type_is_eq[T, UInt64]():
        return DuckDBType.ubigint
    elif _type_is_eq[T, UInt]():
        @parameter
        if size_of[UInt]() == 4:
            return DuckDBType.uinteger
        else:
            return DuckDBType.ubigint
    elif _type_is_eq[T, Float32]():
        return DuckDBType.float
    elif _type_is_eq[T, Float64]():
        return DuckDBType.double
    elif _type_is_eq[T, String]():
        return DuckDBType.varchar
    elif _type_is_eq[T, Date]():
        return DuckDBType.date
    elif _type_is_eq[T, Time]():
        return DuckDBType.time
    elif _type_is_eq[T, Timestamp]():
        return DuckDBType.timestamp
    elif _type_is_eq[T, Interval]():
        return DuckDBType.interval
    elif _type_is_eq[T, Int128]():
        return DuckDBType.hugeint
    elif _type_is_eq[T, UInt128]():
        return DuckDBType.uhugeint
    elif _type_is_eq[T, Decimal]():
        return DuckDBType.decimal
    elif _type_is_eq[T, TimestampS]():
        return DuckDBType.timestamp_s
    elif _type_is_eq[T, TimestampMS]():
        return DuckDBType.timestamp_ms
    elif _type_is_eq[T, TimestampNS]():
        return DuckDBType.timestamp_ns
    elif _type_is_eq[T, TimestampTZ]():
        return DuckDBType.timestamp_tz
    elif _type_is_eq[T, TimeTZ]():
        return DuckDBType.time_tz
    elif _type_is_eq[T, UUID]():
        return DuckDBType.uuid
    elif _type_is_eq[T, TimeNS]():
        return DuckDBType.time_ns
    else:
        comptime base_name = get_base_type_name[T]()
        @parameter
        if base_name == "List":
            return DuckDBType.list
        elif base_name == "Dict":
            return DuckDBType.map
        elif base_name == "Variant":
            return DuckDBType.union
        else:
            return DuckDBType.struct_t


fn mojo_logical_type[T: Copyable & Movable]() -> MojoType:
    """Build a pure-Mojo MojoType descriptor from a Mojo type parameter.

    For scalar types this returns a leaf MojoType.
    For List types it returns `MojoType.list_of(...)`.
    For user-defined structs it reflects over fields and returns a
    `MojoType.struct_of(...)` with recursive child types.

    Parameters:
        T: The Mojo type to describe.

    Returns:
        A MojoType descriptor.
    """
    @parameter
    if _is_known_scalar_type[T]():
        return MojoType(mojo_type_to_duckdb_type[T]())
    else:
        comptime base_name = get_base_type_name[T]()
        @parameter
        if base_name == "List":
            # We know it's a list but can't yet extract T from List[T]
            # automatically. Return a bare list descriptor.
            # Users can call MojoType.list_of() directly for full precision.
            return MojoType(DuckDBType.list)
        elif base_name == "Dict":
            return MojoType(DuckDBType.map)
        elif base_name == "Variant":
            return MojoType(DuckDBType.union)
        else:
            # User-defined struct — reflect over fields
            comptime field_count = struct_field_count[T]()
            comptime field_type_arr = struct_field_types[T]()

            var names = List[String]()
            var types = List[MojoType]()

            @parameter
            for idx in range(field_count):
                # Extract individual field name at compile time (avoids
                # materialising the whole InlineArray[StaticString, N])
                comptime field_name = struct_field_names[T]()[idx]
                names.append(String(field_name))
                comptime ft = field_type_arr[idx]
                types.append(MojoType(_scalar_type_to_duckdb[ft]()))

            return MojoType.struct_of(names^, types^)


# ──────────────────────────────────────────────────────────────────
# Deserialization — scalar
# ──────────────────────────────────────────────────────────────────


fn _deserialize_scalar[T: Copyable & Movable](vector: Vector, offset: Int) raises -> T:
    """Deserialize a scalar value from a vector.

    Parameters:
        T: The scalar type to deserialize.

    Args:
        vector: The vector to read from.
        offset: The offset in the vector.

    Returns:
        The deserialized scalar value.
    """
    comptime expected_type = mojo_type_to_duckdb_type[T]()

    @parameter
    if expected_type == DuckDBType.varchar:
        var data_str_ptr = vector.get_data().bitcast[duckdb_string_t_pointer]()
        var string_length = Int(data_str_ptr[offset].length)

        var result: String
        if data_str_ptr[offset].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[duckdb_string_t_inlined]()
            var ptr = data_str_inlined[offset].inlined.unsafe_ptr().bitcast[Byte]()
            result = String(unsafe_uninit_length=string_length)
            memcpy(dest=result.unsafe_ptr_mut(), src=ptr, count=string_length)
        else:
            var ptr = data_str_ptr[offset].ptr.bitcast[UInt8]()
            result = String(unsafe_uninit_length=string_length)
            memcpy(dest=result.unsafe_ptr_mut(), src=ptr, count=string_length)

        return rebind_var[T](result^)
    elif expected_type == DuckDBType.decimal:
        # Decimal values are stored as their internal integer representation
        # (Int16/Int32/Int64/Int128) depending on the DECIMAL width.
        # We must read the width/scale from the logical type and reconstruct
        # the Decimal struct.
        ref libduckdb = DuckDB().libduckdb()
        var col_type = vector.get_column_type()
        var width = libduckdb.duckdb_decimal_width(col_type.internal_ptr())
        var scale = libduckdb.duckdb_decimal_scale(col_type.internal_ptr())
        var internal = libduckdb.duckdb_decimal_internal_type(col_type.internal_ptr())

        var value: Int128
        if internal == DuckDBType.smallint.value:
            value = vector.get_data().bitcast[Int16]()[offset].cast[DType.int128]()
        elif internal == DuckDBType.integer.value:
            value = vector.get_data().bitcast[Int32]()[offset].cast[DType.int128]()
        elif internal == DuckDBType.bigint.value:
            value = vector.get_data().bitcast[Int64]()[offset].cast[DType.int128]()
        else:
            # hugeint (width > 18)
            value = vector.get_data().bitcast[Int128]()[offset]

        var dec = Decimal(width, scale, value)
        return rebind_var[T](dec)
    elif expected_type == DuckDBType.uuid:
        # UUIDs are stored as Int128 in vectors with a special encoding.
        # Convert from internal format to canonical UInt128.
        var raw = vector.get_data().bitcast[Int128]()[offset]
        var uuid = UUID(internal=raw)
        return rebind_var[T](uuid)
    elif _type_is_eq[T, Int]():
        # Int is platform-dependent — read the matching fixed-width type
        @parameter
        if size_of[Int]() == 4:
            var val = Int(vector.get_data().bitcast[Int32]()[offset])
            return rebind_var[T](val)
        else:
            var val = Int(vector.get_data().bitcast[Int64]()[offset])
            return rebind_var[T](val)
    elif _type_is_eq[T, UInt]():
        @parameter
        if size_of[UInt]() == 4:
            var val = UInt(vector.get_data().bitcast[UInt32]()[offset])
            return rebind_var[T](val)
        else:
            var val = UInt(vector.get_data().bitcast[UInt64]()[offset])
            return rebind_var[T](val)
    else:
        var data_ptr = vector.get_data().bitcast[T]()
        return data_ptr[offset].copy()


# ──────────────────────────────────────────────────────────────────
# Deserialization — BLOB
# ──────────────────────────────────────────────────────────────────


fn _deserialize_blob(vector: Vector, offset: Int) -> List[UInt8]:
    """Deserialize a BLOB value from a vector into a List[UInt8].

    BLOBs are stored using the same duckdb_string_t inline/pointer
    format as VARCHAR.  This reads the raw bytes without any encoding.

    Args:
        vector: The BLOB vector.
        offset: Row offset.

    Returns:
        A List[UInt8] containing the raw binary data.
    """
    var data_str_ptr = vector.get_data().bitcast[duckdb_string_t_pointer]()
    var blob_length = Int(data_str_ptr[offset].length)
    var result = List[UInt8](capacity=blob_length)

    if data_str_ptr[offset].length <= 12:
        var data_str_inlined = data_str_ptr.bitcast[duckdb_string_t_inlined]()
        var ptr = data_str_inlined[offset].inlined.unsafe_ptr().bitcast[UInt8]()
        for i in range(blob_length):
            result.append(ptr[i])
    else:
        var ptr = data_str_ptr[offset].ptr.bitcast[UInt8]()
        for i in range(blob_length):
            result.append(ptr[i])

    return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — ENUM
# ──────────────────────────────────────────────────────────────────


fn _deserialize_enum_value(vector: Vector, offset: Int) raises -> String:
    """Deserialize a single ENUM value from a vector as a String.

    ENUMs are stored internally as small integers (UInt8, UInt16, or UInt32)
    indexing into a dictionary.  This reads the integer and looks up the
    corresponding dictionary string.

    Args:
        vector: The ENUM vector.
        offset: Row offset.

    Returns:
        The enum member name as a String.
    """
    ref libduckdb = DuckDB().libduckdb()
    var col_type = vector.get_column_type()
    var internal_type = DuckDBType(libduckdb.duckdb_enum_internal_type(col_type.internal_ptr()))

    var enum_idx: idx_t
    if internal_type == DuckDBType.utinyint:
        enum_idx = idx_t(vector.get_data().bitcast[UInt8]()[offset])
    elif internal_type == DuckDBType.usmallint:
        enum_idx = idx_t(vector.get_data().bitcast[UInt16]()[offset])
    elif internal_type == DuckDBType.uinteger:
        enum_idx = idx_t(vector.get_data().bitcast[UInt32]()[offset])
    else:
        raise Error("Unexpected ENUM internal type: " + String(internal_type))

    var char_ptr = libduckdb.duckdb_enum_dictionary_value(col_type.internal_ptr(), enum_idx)
    var result = String(unsafe_from_utf8_ptr=char_ptr)
    libduckdb.duckdb_free(char_ptr.bitcast[NoneType]())
    return result^


fn _deserialize_enum_column[
    T: Copyable & Movable
](vector: Vector, length: Int, offset: Int) raises -> List[Optional[T]]:
    """Deserialize an ENUM column into a list of Optional[String].

    Parameters:
        T: Expected to be String.

    Args:
        vector: The ENUM vector.
        length: Number of rows.
        offset: Starting row offset.

    Returns:
        A list of optional string values.
    """
    var result = List[Optional[T]](capacity=length)
    var validity_mask = vector.get_validity()

    for idx in range(length):
        if _is_valid(validity_mask, offset + idx):
            var s = _deserialize_enum_value(vector, offset + idx)
            result.append(Optional(rebind_var[T](s^)))
        else:
            result.append(None)

    return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — struct (reflection-based)
# ──────────────────────────────────────────────────────────────────


fn _is_valid(validity_mask: UnsafePointer[UInt64, MutAnyOrigin], idx: Int) -> Bool:
    """Check if a value at idx is valid (non-NULL) given a validity mask.

    If the mask is null, all values are valid.
    """
    if not validity_mask:
        return True
    var entry_idx = idx // 64
    var idx_in_entry = idx % 64
    return Bool(validity_mask[entry_idx] & UInt64(1 << idx_in_entry))


fn _deserialize_struct_field[
    FieldType: Copyable & Movable
](child_vector: Vector, offset: Int) raises -> FieldType:
    """Deserialize a single field value from a struct child vector.

    Parameters:
        FieldType: The Mojo type of the field.

    Args:
        child_vector: The child vector for this struct field.
        offset: Row offset.

    Returns:
        The deserialized field value.
    """
    comptime db_type = mojo_type_to_duckdb_type[FieldType]()

    @parameter
    if db_type == DuckDBType.struct_t:
        # Nested struct — recurse
        return _deserialize_struct_row[FieldType](child_vector, offset)
    else:
        # Scalar field
        return _deserialize_scalar[FieldType](child_vector, offset)


fn _deserialize_struct_row[
    T: Copyable & Movable
](vector: Vector, offset: Int) raises -> T:
    """Deserialize a single DuckDB STRUCT row into a Mojo struct T.

    Uses compile-time reflection to iterate over T's fields and read
    each one from the corresponding DuckDB struct child vector.

    Parameters:
        T: A Mojo struct whose fields map to DuckDB struct children by position.

    Args:
        vector: The struct vector.
        offset: Row index.

    Returns:
        An instance of T populated from the vector.
    """
    comptime field_count = struct_field_count[T]()

    # Allocate uninitialised memory — we fill every field below.
    var ptr = alloc[T](1)

    @parameter
    for idx in range(field_count):
        var child_vec = vector.struct_get_child(idx_t(idx))
        comptime FieldType = struct_field_types[T]()[idx]
        comptime FT = downcast[FieldType, Copyable & Movable]

        # Get raw pointer to the field's memory slot
        var dst = UnsafePointer(to=__struct_field_ref(idx, ptr[]))

        @parameter
        if conforms_to(FT, _NullableColumn):
            # Optional field — check child vector validity for this row
            var child_validity = child_vec.get_validity()
            var is_null = not _is_valid(child_validity, offset)
            var val = downcast[
                FT, _NullableColumn
            ]._deserialize_single_nullable(child_vec, offset, is_null)
            dst.bitcast[FT]().init_pointee_move(rebind_var[FT](val^))
        else:
            comptime db_type = _scalar_type_to_duckdb[FieldType]()

            @parameter
            if db_type == DuckDBType.varchar:
                # String requires special construction
                var data_str_ptr = child_vec.get_data().bitcast[duckdb_string_t_pointer]()
                var string_length = Int(data_str_ptr[offset].length)
                var result: String
                if data_str_ptr[offset].length <= 12:
                    var data_str_inlined = data_str_ptr.bitcast[duckdb_string_t_inlined]()
                    var p = data_str_inlined[offset].inlined.unsafe_ptr().bitcast[Byte]()
                    result = String(unsafe_uninit_length=string_length)
                    memcpy(dest=result.unsafe_ptr_mut(), src=p, count=string_length)
                else:
                    var p = data_str_ptr[offset].ptr.bitcast[UInt8]()
                    result = String(unsafe_uninit_length=string_length)
                    memcpy(dest=result.unsafe_ptr_mut(), src=p, count=string_length)
                dst.bitcast[String]().init_pointee_move(result^)
            else:
                # Fixed-size type: bitwise copy from the vector data
                comptime field_size = size_of[FieldType]()
                var src = child_vec.get_data().bitcast[Byte]() + offset * field_size
                memcpy(dest=dst.bitcast[Byte](), src=src, count=field_size)

    var result = ptr.take_pointee()
    ptr.free()
    return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — union (struct with all-Optional fields)
# ──────────────────────────────────────────────────────────────────
#
# A DuckDB UNION(name1 TYPE1, name2 TYPE2, ...) is stored internally as
# STRUCT(tag UTINYINT, name1 TYPE1, name2 TYPE2, ...).
#
# The tag (child 0) is a UInt8 indicating which member is active for
# each row.  To deserialize, we read the tag, then read the active
# member's value from the corresponding child vector (children 1..N).
#
# The target Mojo struct must have all Optional fields:
#
#   @fieldwise_init
#   struct MyUnion(Copyable, Movable):
#       var name: Optional[String]    # member 0
#       var id: Optional[Int32]       # member 1
#
# Only the active member gets a value; all others are None.
# ──────────────────────────────────────────────────────────────────


fn _deserialize_union_row[
    T: Copyable & Movable
](vector: Vector, offset: Int) raises -> T:
    """Deserialize a single DuckDB UNION row into a Mojo struct T.

    The struct must have all Optional fields. Only the active member
    (indicated by the union tag) gets a value.

    Parameters:
        T: A Mojo struct with all Optional fields.

    Args:
        vector: The union/struct vector (UNION is stored as STRUCT internally).
        offset: Row index.

    Returns:
        An instance of T with the active member set.
    """
    comptime field_count = struct_field_count[T]()

    # Read the tag from child 0 (UTINYINT)
    var tag_vec = vector.struct_get_child(0)
    var tag = Int(tag_vec.get_data().bitcast[UInt8]()[offset])

    # Allocate uninitialised memory — we fill every field
    var ptr = alloc[T](1)

    @parameter
    for idx in range(field_count):
        var dst = UnsafePointer(to=__struct_field_ref(idx, ptr[]))
        comptime FieldType = struct_field_types[T]()[idx]
        comptime FT = downcast[FieldType, Copyable & Movable]

        # Each field must be Optional. Child vectors are 1-indexed
        # (child 0 is the tag).
        if tag == idx:
            # Active member — deserialize from child vector (idx + 1)
            var child_vec = vector.struct_get_child(idx_t(idx + 1))

            @parameter
            if conforms_to(FT, _NullableColumn):
                var child_validity = child_vec.get_validity()
                var is_null = not _is_valid(child_validity, offset)
                var val = downcast[
                    FT, _NullableColumn
                ]._deserialize_single_nullable(child_vec, offset, is_null)
                dst.bitcast[FT]().init_pointee_move(rebind_var[FT](val^))
            else:
                # Non-Optional field in union — deserialize directly
                var val = _deserialize_table_field[FT](child_vec, offset)
                dst.bitcast[FT]().init_pointee_move(val)
        else:
            # Inactive member — set to None if Optional
            @parameter
            if conforms_to(FT, _NullableColumn):
                # Initialize as None by re-using the nullable deserializer
                var child_vec = vector.struct_get_child(idx_t(idx + 1))
                var val = downcast[
                    FT, _NullableColumn
                ]._deserialize_single_nullable(child_vec, offset, True)
                dst.bitcast[FT]().init_pointee_move(rebind_var[FT](val^))
            else:
                # Non-Optional inactive member — zero-init
                var zero = alloc[Byte](size_of[FT]())
                for i in range(size_of[FT]()):
                    zero[i] = 0
                memcpy(dest=dst.bitcast[Byte](), src=zero, count=size_of[FT]())
                zero.free()

    var result = ptr.take_pointee()
    ptr.free()
    return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — union as Variant
# ──────────────────────────────────────────────────────────────────
#
# Alternative to the struct-with-Optional approach: represent a
# DuckDB UNION as a Mojo `Variant[T1, T2, ...]`.
#
# Positional mapping: Variant's type index i corresponds to UNION
# member i.  Member names are NOT checked — only types by position.
#
# Example:
#
#   -- SQL:  UNION(num INTEGER, str VARCHAR)
#   -- Mojo: Variant[Int32, String]
#
#   var val = chunk.get[Variant[Int32, String]](col=0, row=0)
#   if val.isa[Int32]():
#       print(val[Int32])   # active member
#   elif val.isa[String]():
#       print(val[String])
# ──────────────────────────────────────────────────────────────────


trait _VariantUnionDeserializable(_DBase):
    """Deserialize a DuckDB UNION row into a Mojo Variant."""

    @staticmethod
    fn _from_union_vector(vector: Vector, offset: Int) raises -> Self:
        """Construct a Variant from a DuckDB UNION vector at the given row."""
        ...


__extension Variant(_VariantUnionDeserializable):
    @staticmethod
    fn _from_union_vector(vector: Vector, offset: Int) raises -> Self:
        # Read the tag from child 0 (UTINYINT)
        var tag_vec = vector.struct_get_child(0)
        var tag = Int(tag_vec.get_data().bitcast[UInt8]()[offset])

        # Iterate over Variant's type parameters at compile time.
        # Each iteration generates a runtime branch for the matching tag.
        comptime for i in range(Variadic.size(Self.Ts)):
            if tag == i:
                comptime MemberType = Self.Ts[i]
                comptime MT = downcast[MemberType, Copyable & Movable]
                var child_vec = vector.struct_get_child(idx_t(i + 1))
                var val = _deserialize_table_field[MT](child_vec, offset)
                return Self(val^)

        raise Error("Invalid union tag: " + String(tag))


# ──────────────────────────────────────────────────────────────────
# List type decomposition — traits + extensions for recursive types
# ──────────────────────────────────────────────────────────────────
#
# Problem: inside a generic `fn foo[T: Copyable & Movable]()` we
# cannot access `T.T` to decompose `List[Optional[X]]` into X.
#
# Solution (inspired by EmberJson): use `__extension` blocks where
# `Self` IS the concrete type, so `Self.T` resolves to the actual
# parameter.  Two extensions cooperate:
#
#   Optional._deser_as_list_elements  — unwraps Optional and calls
#       _deserialize_list with the leaf type.
#
#   List._from_list_child  — delegates to Optional's method if the
#       element type conforms, otherwise strips Optional from the
#       result of _deserialize_list for plain element types.
#
# The mutual recursion terminates when the leaf type is a non-list
# scalar and _deserialize_list handles it directly.
# ──────────────────────────────────────────────────────────────────

comptime _DBase = Copyable & Movable


trait _InnerListDeserializer(_DBase):
    """For Optional[X]: deserialize child-vector elements as List[Self].

    Since _deserialize_list[X] returns List[Optional[X]] and
    Self = Optional[X], the result type List[Optional[X]] = List[Self].
    """

    @staticmethod
    fn _deser_as_list_elements(
        child_vector: Vector, length: Int, offset: Int
    ) raises -> List[Self]:
        ...


__extension Optional(_InnerListDeserializer):
    @staticmethod
    fn _deser_as_list_elements(
        child_vector: Vector, length: Int, offset: Int
    ) raises -> List[Self]:
        # Self = Optional[X],  Self.T = X
        # _deserialize_list[X] returns List[Optional[X]] == List[Self]
        var inner = _deserialize_list[downcast[Self.T, _DBase]](
            child_vector, length, offset
        )
        return rebind_var[List[Self]](inner^)


trait _VectorListConstructible(_DBase):
    """Construct a List value from a DuckDB child-vector region.

    Implemented via `__extension` for `List`.  Enables arbitrary
    nesting: List[Optional[List[Optional[Int32]]]], etc.
    """

    @staticmethod
    fn _from_list_child(
        child_vector: Vector, length: Int, offset: Int
    ) raises -> Self:
        """Construct one Self from a child-vector region."""
        ...


__extension List(_VectorListConstructible):
    @staticmethod
    fn _from_list_child(
        child_vector: Vector, length: Int, offset: Int
    ) raises -> Self:
        # If Self.T conforms to _InnerListDeserializer (i.e. it is Optional),
        # delegate to its _deser_as_list_elements which returns List[Self.T] = Self.
        comptime if conforms_to(Self.T, _InnerListDeserializer):
            var inner = downcast[
                Self.T, _InnerListDeserializer
            ]._deser_as_list_elements(
                child_vector, length, offset
            )
            return rebind_var[Self](inner^)
        else:
            # Self = List[X] where X is NOT Optional.
            # Deserialize as List[Optional[X]] then unwrap, raising on NULLs.
            var deserialized = _deserialize_list[downcast[Self.T, _DBase]](
                child_vector, length, offset
            )
            var result = Self(capacity=length)
            for i in range(len(deserialized)):
                if deserialized[i]:
                    result.append(
                        rebind_var[Self.T](deserialized[i].value().copy())
                    )
                else:
                    raise Error(
                        "NULL in DuckDB list but target element type is not"
                        " Optional. Use List[Optional[...]] to handle NULLs."
                    )
            return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — MAP as Dict
# ──────────────────────────────────────────────────────────────────
#
# A DuckDB MAP(K, V) is stored internally as LIST(STRUCT(key K, value V)).
# When the user requests Dict[K, V], we iterate the MAP entries, read each
# key and value from the struct child vectors, and populate a Dict.
#
# NULL values are supported when V is Optional[T]; otherwise a NULL value
# raises an error.  MAP keys cannot be NULL in DuckDB.
# ──────────────────────────────────────────────────────────────────


trait _DictMapDeserializable(_DBase):
    """Construct a Dict from a DuckDB MAP child-vector region."""

    @staticmethod
    fn _from_map_child(
        struct_vec: Vector, length: Int, offset: Int
    ) raises -> Self:
        """Build a Dict from the STRUCT(key, value) child of a MAP vector.

        Args:
            struct_vec: The struct child vector (child of the MAP/LIST vector).
            length: Number of entries.
            offset: Starting offset.
        """
        ...


__extension Dict(_DictMapDeserializable):
    @staticmethod
    fn _from_map_child(
        struct_vec: Vector, length: Int, offset: Int
    ) raises -> Self:
        var key_vec = struct_vec.struct_get_child(0)
        var val_vec = struct_vec.struct_get_child(1)
        var val_validity = val_vec.get_validity()
        var result = Self()

        comptime KT = downcast[
            Self.K, Copyable & Movable & ImplicitlyDestructible
        ]
        comptime VT = downcast[Self.V, _DBase]

        for i in range(length):
            var is_null = not _is_valid(val_validity, offset + i)

            @parameter
            if conforms_to(VT, _NullableColumn):
                var key = _deserialize_scalar[KT](key_vec, offset + i)
                var val = downcast[
                    VT, _NullableColumn
                ]._deserialize_single_nullable(
                    val_vec, offset + i, is_null
                )
                result[rebind_var[Self.K](key^)] = rebind_var[
                    Self.V
                ](val^)
            else:
                if is_null:
                    raise Error(
                        "NULL value in MAP but Dict value type is not"
                        " Optional. Use Dict[K, Optional[V]] to handle"
                        " NULLs."
                    )
                var key = _deserialize_scalar[KT](key_vec, offset + i)
                var val = _deserialize_table_field[VT](
                    val_vec, offset + i
                )
                result[rebind_var[Self.K](key^)] = rebind_var[Self.V](
                    val^
                )

        return result^


# ──────────────────────────────────────────────────────────────────
# Deserialization — list elements
# ──────────────────────────────────────────────────────────────────


fn _deserialize_list[
    ElementType: Copyable & Movable
](vector: Vector, length: Int, offset: Int) raises -> List[Optional[ElementType]]:
    """Deserialize list elements from a vector.

    Parameters:
        ElementType: The type of list elements.

    Args:
        vector: The vector to read from.
        length: Number of elements.
        offset: Starting offset.

    Returns:
        A list of optional element values.
    """
    comptime element_db_type = mojo_type_to_duckdb_type[ElementType]()

    var result = List[Optional[ElementType]](capacity=length)
    var validity_mask = vector.get_validity()

    @parameter
    if element_db_type == DuckDBType.struct_t:
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                result.append(
                    Optional(
                        _deserialize_struct_row[ElementType](vector, offset + idx)
                    )
                )
            else:
                result.append(None)
    elif element_db_type.is_fixed_size():
        var data_ptr = vector.get_data().bitcast[ElementType]()
        if not validity_mask:
            for idx in range(length):
                result.append(Optional(data_ptr[offset + idx].copy()))
        else:
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    result.append(Optional(data_ptr[offset + idx].copy()))
                else:
                    result.append(None)
    elif element_db_type == DuckDBType.varchar:
        if not validity_mask:
            for idx in range(length):
                result.append(
                    Optional(_deserialize_scalar[ElementType](vector, offset + idx))
                )
        else:
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    result.append(
                        Optional(
                            _deserialize_scalar[ElementType](vector, offset + idx)
                        )
                    )
                else:
                    result.append(None)
    elif element_db_type == DuckDBType.list:
        # Nested list — use the _VectorListConstructible extension to
        # decompose ElementType (a List[...]) and recurse.
        var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
        var child_vec = vector.list_get_child()
        if not validity_mask:
            for idx in range(length):
                var entry = list_entries[offset + idx]
                var inner = downcast[
                    ElementType, _VectorListConstructible
                ]._from_list_child(
                    child_vec, Int(entry.length), Int(entry.offset)
                )
                result.append(Optional(rebind_var[ElementType](inner^)))
        else:
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    var entry = list_entries[offset + idx]
                    var inner = downcast[
                        ElementType, _VectorListConstructible
                    ]._from_list_child(
                        child_vec, Int(entry.length), Int(entry.offset)
                    )
                    result.append(Optional(rebind_var[ElementType](inner^)))
                else:
                    result.append(None)
    elif element_db_type == DuckDBType.union:
        # Variant union elements in a list
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                var inner = downcast[
                    ElementType, _VariantUnionDeserializable
                ]._from_union_vector(vector, offset + idx)
                result.append(Optional(rebind_var[ElementType](inner^)))
            else:
                result.append(None)
    else:
        raise Error("Unsupported list element type: " + String(element_db_type))

    return result^


# ──────────────────────────────────────────────────────────────────
# List column deserialization
# ──────────────────────────────────────────────────────────────────


fn deserialize_list_column[
    ElementType: Copyable & Movable
](
    vector: Vector, length: Int, offset: Int = 0
) raises -> List[Optional[List[Optional[ElementType]]]]:
    """Deserialize a LIST column from a DuckDB vector.

    Each row in the column is a variable-length list of `ElementType` values.
    Uses `_deserialize_list` internally — a column *is* essentially a list,
    so nested lists reuse the same machinery.

    Parameters:
        ElementType: The element type inside each list
                     (e.g., `Int32` for a `LIST(INTEGER)` column).

    Args:
        vector: The DuckDB list vector to read from.
        length: Number of rows to read.
        offset: Starting row offset (default 0).

    Returns:
        A list of optional lists, one per row.  Each inner list contains
        `Optional[ElementType]` to represent per-element NULLs.

    Example:
        ```mojo
        # For a column created as LIST(INTEGER):
        var lists = deserialize_list_column[Int32](vector, row_count)
        for row in lists:
            if row[]:
                for elem in row[].value():
                    if elem[]:
                        print(elem[].value())
        ```
    """
    var actual_type = vector.get_column_type().get_type_id()
    if actual_type == DuckDBType.array:
        # ARRAY column: fixed-size per row
        var array_size = Int(
            vector.get_column_type().array_type_array_size()
        )
        var child_vec = vector.array_get_child()
        var result = List[Optional[List[Optional[ElementType]]]](
            capacity=length
        )
        var validity_mask = vector.get_validity()
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                var inner = _deserialize_list[ElementType](
                    child_vec,
                    array_size,
                    (offset + idx) * array_size,
                )
                result.append(Optional(inner^))
            else:
                result.append(None)
        return result^

    if actual_type != DuckDBType.list and actual_type != DuckDBType.map:
        raise Error(
            "Type mismatch: expected LIST, ARRAY, or MAP column but got "
            + String(actual_type)
        )

    var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
    var child_vec = vector.list_get_child()
    var result = List[Optional[List[Optional[ElementType]]]](capacity=length)
    var validity_mask = vector.get_validity()

    for idx in range(length):
        if _is_valid(validity_mask, offset + idx):
            var entry = list_entries[offset + idx]
            var inner = _deserialize_list[ElementType](
                child_vec, Int(entry.length), Int(entry.offset)
            )
            result.append(Optional(inner^))
        else:
            result.append(None)

    return result^


# ──────────────────────────────────────────────────────────────────
# Table-to-struct field deserialization
# ──────────────────────────────────────────────────────────────────


fn _deserialize_table_field[
    T: Copyable & Movable
](vector: Vector, row: Int) raises -> T:
    """Deserialize a single non-null value from a column vector at a given row.

    Handles scalars, strings, lists, and nested structs. Used for
    table-to-struct deserialization where each column maps to a struct field.

    Parameters:
        T: The Mojo type to deserialize.

    Args:
        vector: The DuckDB column vector.
        row: Row index.

    Returns:
        The deserialized value.
    """
    comptime db_type = mojo_type_to_duckdb_type[T]()
    comptime base_name = get_base_type_name[T]()

    @parameter
    if base_name == "List":
        # List/Array/Map field — dispatch by actual vector type
        var actual_type = vector.get_column_type().get_type_id()
        if actual_type == DuckDBType.blob:
            # BLOB column → List[UInt8]
            var blob_list = _deserialize_blob(vector, row)
            return rebind_var[T](blob_list^)
        if actual_type == DuckDBType.array:
            # ARRAY: fixed-size, use array_get_child
            var array_size = Int(
                vector.get_column_type().array_type_array_size()
            )
            var child_vec = vector.array_get_child()
            var inner = downcast[
                T, _VectorListConstructible
            ]._from_list_child(
                child_vec, array_size, row * array_size
            )
            return rebind_var[T](inner^)
        else:
            # LIST or MAP: both use duckdb_list_entry layout
            var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
            var entry = list_entries[row]
            var child_vec = vector.list_get_child()
            var inner = downcast[
                T, _VectorListConstructible
            ]._from_list_child(
                child_vec, Int(entry.length), Int(entry.offset)
            )
            return rebind_var[T](inner^)
    elif base_name == "Dict":
        # MAP column → Dict[K, V]
        var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
        var entry = list_entries[row]
        var child_vec = vector.list_get_child()
        var inner = downcast[
            T, _DictMapDeserializable
        ]._from_map_child(child_vec, Int(entry.length), Int(entry.offset))
        return rebind_var[T](inner^)
    elif db_type == DuckDBType.union:
        # Variant union — use the _VariantUnionDeserializable extension
        var inner = downcast[
            T, _VariantUnionDeserializable
        ]._from_union_vector(vector, row)
        return rebind_var[T](inner^)
    elif db_type == DuckDBType.struct_t:
        # Could be STRUCT or UNION — check actual vector type
        var actual_type = vector.get_column_type().get_type_id()
        if actual_type == DuckDBType.union:
            return _deserialize_union_row[T](vector, row)
        else:
            return _deserialize_struct_row[T](vector, row)
    else:
        # Scalar (including String) — use existing scalar deserialization
        # ENUM columns can be deserialized as String
        @parameter
        if db_type == DuckDBType.varchar:
            var actual_type = vector.get_column_type().get_type_id()
            if actual_type == DuckDBType.enum:
                var s = _deserialize_enum_value(vector, row)
                return rebind_var[T](s^)
        return _deserialize_scalar[T](vector, row)


# ──────────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────────


fn deserialize_from_vector[
    T: Copyable & Movable
](vector: Vector, length: Int, offset: Int = 0) raises -> List[Optional[T]]:
    """Deserialize values from a DuckDB vector into native Mojo types.

    Handles scalar types, List[T], and user-defined structs (via reflection).

    Parameters:
        T: The target Mojo type.

    Args:
        vector: The DuckDB vector to read from.
        length: Number of elements to read.
        offset: Starting offset (default 0).

    Returns:
        A list of optional values of type T.

    Example:
        ```mojo
        var ints = deserialize_from_vector[Int64](vector, 100)
        var strings = deserialize_from_vector[String](vector, 50)
        var points = deserialize_from_vector[Point](vector, 10)
        ```
    """
    comptime db_type = mojo_type_to_duckdb_type[T]()
    comptime base_name = get_base_type_name[T]()

    @parameter
    if base_name == "List":
        # List/Array/Map deserialization — use the _VectorListConstructible
        # extension to decompose T (a List[...]) and build each row's list.
        var actual_type = vector.get_column_type().get_type_id()

        # BLOB columns → List[UInt8]: read raw bytes from varchar-style storage
        if actual_type == DuckDBType.blob:
            var result = List[Optional[T]](capacity=length)
            var validity_mask = vector.get_validity()
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    var blob_list = _deserialize_blob(vector, offset + idx)
                    result.append(Optional(rebind_var[T](blob_list^)))
                else:
                    result.append(None)
            return result^

        if actual_type == DuckDBType.array:
            # ARRAY: fixed-size per row. Child vector has rows × array_size
            # elements; entry n at offset n * array_size, length = array_size.
            var array_size = Int(
                vector.get_column_type().array_type_array_size()
            )
            var child_vec = vector.array_get_child()
            var result = List[Optional[T]](capacity=length)
            var validity_mask = vector.get_validity()
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    var inner = downcast[
                        T, _VectorListConstructible
                    ]._from_list_child(
                        child_vec,
                        array_size,
                        (offset + idx) * array_size,
                    )
                    result.append(Optional(rebind_var[T](inner^)))
                else:
                    result.append(None)
            return result^

        if actual_type != DuckDBType.list and actual_type != DuckDBType.map:
            raise Error(
                "Type mismatch: Expected type list, array, or map but got "
                + String(actual_type)
            )
        # LIST and MAP both use duckdb_list_entry layout
        var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
        var child_vec = vector.list_get_child()
        var result = List[Optional[T]](capacity=length)
        var validity_mask = vector.get_validity()
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                var entry = list_entries[offset + idx]
                var inner = downcast[
                    T, _VectorListConstructible
                ]._from_list_child(
                    child_vec, Int(entry.length), Int(entry.offset)
                )
                result.append(Optional(rebind_var[T](inner^)))
            else:
                result.append(None)
        return result^

    @parameter
    if base_name == "Dict":
        # MAP column → Dict[K, V]
        var actual_type = vector.get_column_type().get_type_id()
        if actual_type != DuckDBType.map:
            raise Error(
                "Type mismatch: Expected type map but got "
                + String(actual_type)
            )
        var list_entries = vector.get_data().bitcast[duckdb_list_entry]()
        var child_vec = vector.list_get_child()
        var result = List[Optional[T]](capacity=length)
        var validity_mask = vector.get_validity()
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                var entry = list_entries[offset + idx]
                var inner = downcast[
                    T, _DictMapDeserializable
                ]._from_map_child(
                    child_vec, Int(entry.length), Int(entry.offset)
                )
                result.append(Optional(rebind_var[T](inner^)))
            else:
                result.append(None)
        return result^

    @parameter
    if db_type == DuckDBType.union:
        # Variant union deserialization
        var actual_type = vector.get_column_type().get_type_id()
        if actual_type != DuckDBType.union:
            raise Error(
                "Type mismatch: Expected type union but got "
                + String(actual_type)
            )
        var result = List[Optional[T]](capacity=length)
        var validity_mask = vector.get_validity()
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                var inner = downcast[
                    T, _VariantUnionDeserializable
                ]._from_union_vector(vector, offset + idx)
                result.append(Optional(rebind_var[T](inner^)))
            else:
                result.append(None)
        return result^

    @parameter
    if db_type == DuckDBType.struct_t:
        # Struct or Union deserialization via reflection
        var actual_type = vector.get_column_type().get_type_id()
        if actual_type == DuckDBType.union:
            # UNION: tag + members stored as STRUCT internally
            var result = List[Optional[T]](capacity=length)
            var validity_mask = vector.get_validity()
            for idx in range(length):
                if _is_valid(validity_mask, offset + idx):
                    result.append(
                        Optional(
                            _deserialize_union_row[T](vector, offset + idx)
                        )
                    )
                else:
                    result.append(None)
            return result^
        elif actual_type != DuckDBType.struct_t:
            raise Error(
                "Type mismatch: Expected type struct or union but got "
                + String(actual_type)
            )
        var result = List[Optional[T]](capacity=length)
        var validity_mask = vector.get_validity()
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                result.append(
                    Optional(
                        _deserialize_struct_row[T](vector, offset + idx)
                    )
                )
            else:
                result.append(None)
        return result^

    # Scalar types
    var actual_type = vector.get_column_type().get_type_id()
    if actual_type != db_type:
        # ENUM columns can be deserialized to String
        @parameter
        if db_type == DuckDBType.varchar:
            if actual_type == DuckDBType.enum:
                return _deserialize_enum_column[T](vector, length, offset)
        raise Error(
            "Type mismatch: Expected type " + String(db_type) + " but got "
            + String(actual_type)
        )

    var result = List[Optional[T]](capacity=length)
    var validity_mask = vector.get_validity()

    if not validity_mask:
        for idx in range(length):
            result.append(
                Optional(_deserialize_scalar[T](vector, offset + idx))
            )
    else:
        for idx in range(length):
            if _is_valid(validity_mask, offset + idx):
                result.append(
                    Optional(_deserialize_scalar[T](vector, offset + idx))
                )
            else:
                result.append(None)

    return result^


# ──────────────────────────────────────────────────────────────────
# Nullable column support — Optional[T] extension
# ──────────────────────────────────────────────────────────────────
#
# These types allow `get[Optional[T]]` to return None for NULL values
# instead of raising a runtime error.  Without Optional, a NULL triggers
# an error — making null-avoidance the default.
#
#   get[Int64](col=0, row=0)            → Int64          (raises on NULL)
#   get[Optional[Int64]](col=0, row=0)  → Optional[Int64] (None on NULL)
# ──────────────────────────────────────────────────────────────────


trait _NullableColumn(_DBase):
    """Marker for types that accept NULL as None (i.e., Optional[T])."""

    @staticmethod
    fn _expected_duckdb_type() -> DuckDBType:
        """DuckDB type of the wrapped inner type."""
        ...

    @staticmethod
    fn _deserialize_single_nullable(
        vector: Vector, row: Int, is_null: Bool
    ) raises -> Self:
        """Deserialize one value, returning None when is_null is True."""
        ...

    @staticmethod
    fn _deserialize_column_nullable(
        vector: Vector, count: Int, offset: Int
    ) raises -> List[Self]:
        """Deserialize a full column with None for NULL entries."""
        ...


__extension Optional(_NullableColumn):
    @staticmethod
    fn _expected_duckdb_type() -> DuckDBType:
        return mojo_type_to_duckdb_type[downcast[Self.T, _DBase]]()

    @staticmethod
    fn _deserialize_single_nullable(
        vector: Vector, row: Int, is_null: Bool
    ) raises -> Self:
        if is_null:
            return None
        var val = _deserialize_table_field[downcast[Self.T, _DBase]](
            vector, row
        )
        return rebind_var[Self](Optional(val^))

    @staticmethod
    fn _deserialize_column_nullable(
        vector: Vector, count: Int, offset: Int
    ) raises -> List[Self]:
        var inner = deserialize_from_vector[downcast[Self.T, _DBase]](
            vector, count, offset
        )
        return rebind_var[List[Self]](inner^)