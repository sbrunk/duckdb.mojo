from duckdb.vector import Vector
from collections import Dict, Optional
from collections.string import StringSlice, StaticString
from memory import memcpy


trait DuckDBValue(Copyable & Movable & Stringable):
    """Represents a DuckDB value of any supported type.

    Implementations are thin wrappers around native Mojo types
    but implement a type specific __init__ method to convert from a DuckDB vector.
    """
    comptime Type: DuckDBType

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        ...

trait DuckDBKeyElement(DuckDBValue, KeyElement):
    pass


@fieldwise_init
struct DTypeValue[duckdb_type: DuckDBType](DuckDBKeyElement & Hashable & TrivialRegisterPassable):
    comptime Type = Self.duckdb_type

    var value: Scalar[Self.Type.to_dtype()]

    fn __str__(self) -> String:
        return self.value.__str__()

    fn __hash__[H: Hasher](self, mut hasher: H):
        hasher.update(self.value)

    fn __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: Self) -> Bool:
        return self.value != other.value

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != Self.duckdb_type:
            raise "Expected type " + String(Self.duckdb_type) + " but got " + String(
                vector.get_column_type().get_type_id()
            )

        self = vector.get_data().bitcast[Self]()[offset=offset]

comptime BoolVal = DTypeValue[DuckDBType.boolean]
comptime Int8Val = DTypeValue[DuckDBType.tinyint]
comptime Int16Val = DTypeValue[DuckDBType.smallint]
comptime Int32Val = DTypeValue[DuckDBType.integer]
comptime Int64Val = DTypeValue[DuckDBType.bigint]
comptime UInt8Val = DTypeValue[DuckDBType.utinyint]
comptime UInt16Val = DTypeValue[DuckDBType.usmallint]
comptime UInt32Val = DTypeValue[DuckDBType.uinteger]
comptime UInt64Val = DTypeValue[DuckDBType.ubigint]
comptime Float32Val = DTypeValue[DuckDBType.float]
comptime Float64Val = DTypeValue[DuckDBType.double]


@fieldwise_init
struct FixedSizeValue[
    duckdb_type: DuckDBType, underlying: Stringable & Writable & ImplicitlyCopyable & Movable
](DuckDBValue & ImplicitlyCopyable):
    comptime Type = Self.duckdb_type
    var value: Self.underlying

    fn write_to[W: Writer](self, mut writer: W):
        self.value.write_to(writer)

    fn __str__(self) -> String:
        return String(self.value)

    # fn __hash__(self) -> UInt:
    #     return self.value.__hash__()

    # fn __eq__(self, other: Self) -> Bool:
    #     return self.value == other.value

    # fn __ne__(self, other: Self) -> Bool:
    #     return self.value != other.value

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != Self.duckdb_type:
            raise "Expected type " + String(Self.duckdb_type) + " but got " + String(
                vector.get_column_type().get_type_id()
            )

        self = vector.get_data().bitcast[Self]()[offset=offset]

    fn __copyinit__(out self, other: Self):
        self.value = other.value

comptime DuckDBTimestamp = FixedSizeValue[DuckDBType.timestamp, Timestamp]
comptime DuckDBDate = FixedSizeValue[DuckDBType.date, Date]
comptime DuckDBTime = FixedSizeValue[DuckDBType.time, Time]
comptime DuckDBInterval = FixedSizeValue[DuckDBType.interval, Time]


@fieldwise_init
struct DuckDBString(DuckDBValue):
    comptime Type = DuckDBType.varchar
    var value: String

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != DuckDBType.varchar:
            raise "Expected type " + String(
                DuckDBType.varchar
            ) + " but got " + String(vector.get_column_type().get_type_id())
        var data_str_ptr = vector.get_data().bitcast[duckdb_string_t_pointer]()
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = Int(data_str_ptr[offset].length)
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[offset].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[
                duckdb_string_t_inlined
            ]()
            var ptr=data_str_inlined[offset].inlined.unsafe_ptr().bitcast[Byte]()
            self.value = String(unsafe_uninit_length=string_length)
            memcpy(dest=self.value.unsafe_ptr_mut(), src=ptr, count=string_length)
        else:
            ptr=data_str_ptr[offset].ptr.bitcast[UInt8]()
            self.value = String(unsafe_uninit_length=string_length)
            memcpy(dest=self.value.unsafe_ptr_mut(), src=ptr, count=string_length)

    fn __str__(self) -> String:
        return self.value


@fieldwise_init
struct DuckDBList[T: DuckDBValue & Movable](DuckDBValue & Copyable & Movable):
    """A DuckDB list."""
    comptime Type = DuckDBType.list

    comptime expected_element_type = Self.T.Type
    var value: List[Optional[Self.T]]

    fn __str__(self) -> String:
        return "DuckDBList"  # TODO

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        var runtime_element_type = vector.get_column_type().get_type_id()
        if runtime_element_type != Self.expected_element_type:
            raise "Expected type " + String(
                Self.expected_element_type
            ) + " but got " + String(runtime_element_type)
        self.value = List[Optional[Self.T]](capacity=length)

        var data_ptr = vector.get_data().bitcast[Self.T]()
        var validity_mask = vector.get_validity()

        # TODO factor out the validity mask check into a higher-order function to avoid repetition

        if Self.expected_element_type.is_fixed_size():
            # if the element type is fixed size, we can directly get all values from the vector
            # that way we can avoid calling the constructor for each element

            # validity mask can be null if there are no NULL values
            if not validity_mask:
                for idx in range(length):
                    self.value.append(Optional(data_ptr[idx + offset].copy()))
            else:  # otherwise we have to check the validity mask for each element
                for idx in range(length):
                    var entry_idx = idx // 64
                    var idx_in_entry = idx % 64
                    var is_valid = validity_mask[entry_idx] & UInt64((
                        1 << idx_in_entry
                    ))
                    if is_valid:
                        self.value.append(Optional(data_ptr[idx + offset].copy()))
                    else:
                        self.value.append(None)
        elif Self.expected_element_type == DuckDBType.varchar:
            # validity mask can be null if there are no NULL values
            if not validity_mask:
                for idx in range(length):
                    self.value.append(Optional(Self.T(vector, length=1, offset=offset + idx)))
            else:  # otherwise we have to check the validity mask for each element
                for idx in range(length):
                    var entry_idx = idx // 64
                    var idx_in_entry = idx % 64
                    var is_valid = validity_mask[entry_idx] & UInt64((
                        1 << idx_in_entry
                    ))
                    if is_valid:
                        self.value.append(Optional(Self.T(vector, length=1, offset=offset + idx)))
                    else:
                        self.value.append(None)
        elif Self.expected_element_type == DuckDBType.list:
            # if the subtype is a list itself, we need to call the constructor for each element recursively

            # pointer to list metadata (length and offset) that allows us to get the
            # correct positions of the actual data in the child vector
            var data_ptr = data_ptr.bitcast[duckdb_list_entry]()
            # The child vector holds the actual list data in variable size entries (list_entry.length)
            var child_vector = vector.list_get_child()

            # validity mask can be null if there are no NULL values
            if not validity_mask:
                for idx in range(length):
                    var list_entry = data_ptr[offset + idx]
                    self.value.append(
                        Optional(
                            Self.T(
                                child_vector,
                                length=Int(list_entry.length),
                                offset=Int(list_entry.offset),
                            )
                        )
                    )
            else:  # otherwise we have to check the validity mask for each element
                for idx in range(length):
                    var entry_idx = idx // 64
                    var idx_in_entry = idx % 64
                    var is_valid = validity_mask[entry_idx] & UInt64((
                        1 << idx_in_entry
                    ))
                    if is_valid:
                        var list_entry = data_ptr[offset + idx]
                        self.value.append(
                            Optional(
                                Self.T(
                                    child_vector,
                                    length=Int(list_entry.length),
                                    offset=Int(list_entry.offset),
                                )
                            )
                        )
                    else:
                        self.value.append(None)
        else:
            raise Error(
                "Unsupported or invalid type: " + String(runtime_element_type)
            )

# @fieldwise_init
# struct DuckDBMap[K: DuckDBKeyElement, V: DuckDBValue](DuckDBValue):
#     comptime Type = DuckDBType.map
#     var value: Dict[K, V]

#     fn __str__(self) -> String:
#         return "DuckDBMap"  # TODO

#     fn __init__(
#         out self, vector: Vector, length: Int, offset: Int = 0
#     ) raises:
#         self.value = Dict[K, V]()
#         raise "Not implemented"
#         # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
#         # Via https://duckdb.org/docs/internals/vector#map-vectors
#         # TODO fill dict
#         # for i in range(length):
#         #     self.key = K()
#         #     self.value = V()
