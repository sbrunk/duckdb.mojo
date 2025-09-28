from duckdb.vector import Vector
from collections import Dict, Optional
from collections.string import StringSlice, StaticString
from memory import memcpy


trait DuckDBValue(Copyable & Movable & Stringable):
    """Represents a DuckDB value of any supported type.

    Implementations are thin wrappers around native Mojo types
    but implement a type specific __init__ method to convert from a DuckDB vector.
    """
    alias Type: DuckDBType

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        pass

trait DuckDBKeyElement(DuckDBValue, KeyElement):
    pass


@fieldwise_init
@register_passable("trivial")
struct DTypeValue[duckdb_type: DuckDBType](DuckDBKeyElement & Hashable):
    alias Type = duckdb_type

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
        if vector.get_column_type().get_type_id() != duckdb_type:
            raise "Expected type " + String(duckdb_type) + " but got " + String(
                vector.get_column_type().get_type_id()
            )

        self = vector._get_data().bitcast[Self]()[offset=offset]

alias BoolVal = DTypeValue[DuckDBType.boolean]
alias Int8Val = DTypeValue[DuckDBType.tinyint]
alias Int16Val = DTypeValue[DuckDBType.smallint]
alias Int32Val = DTypeValue[DuckDBType.integer]
alias Int64Val = DTypeValue[DuckDBType.bigint]
alias UInt8Val = DTypeValue[DuckDBType.utinyint]
alias UInt16Val = DTypeValue[DuckDBType.usmallint]
alias UInt32Val = DTypeValue[DuckDBType.uinteger]
alias UInt64Val = DTypeValue[DuckDBType.ubigint]
alias Float32Val = DTypeValue[DuckDBType.float]
alias Float64Val = DTypeValue[DuckDBType.double]


@fieldwise_init
struct FixedSizeValue[
    duckdb_type: DuckDBType, underlying: Stringable & Writable & ImplicitlyCopyable & Movable
](DuckDBValue & ImplicitlyCopyable):
    alias Type = duckdb_type
    var value: underlying

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
        if vector.get_column_type().get_type_id() != duckdb_type:
            raise "Expected type " + String(duckdb_type) + " but got " + String(
                vector.get_column_type().get_type_id()
            )

        self = vector._get_data().bitcast[Self]()[offset=offset]

    fn __copyinit__(out self, other: Self):
        self.value = other.value

alias DuckDBTimestamp = FixedSizeValue[DuckDBType.timestamp, Timestamp]
alias DuckDBDate = FixedSizeValue[DuckDBType.date, Date]
alias DuckDBTime = FixedSizeValue[DuckDBType.time, Time]
alias DuckDBInterval = FixedSizeValue[DuckDBType.interval, Time]


@fieldwise_init
struct DuckDBString(DuckDBValue):
    alias Type = DuckDBType.varchar
    var value: String

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != DuckDBType.varchar:
            raise "Expected type " + String(
                DuckDBType.varchar
            ) + " but got " + String(vector.get_column_type().get_type_id())
        var data_str_ptr = vector._get_data().bitcast[duckdb_string_t_pointer]()
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = UInt(Int(data_str_ptr[offset].length))
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[offset].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[
                duckdb_string_t_inlined
            ]()
            var ptr=data_str_inlined[offset].inlined.unsafe_ptr().bitcast[Byte]()
            self.value = String(unsafe_uninit_length=string_length)
            memcpy(self.value.unsafe_ptr_mut(), ptr, string_length)
        else:
            ptr=data_str_ptr[offset].ptr.bitcast[UInt8]()
            self.value = String(unsafe_uninit_length=string_length)
            memcpy(self.value.unsafe_ptr_mut(), ptr, string_length)

    fn __str__(self) -> String:
        return self.value


@fieldwise_init
struct DuckDBList[T: DuckDBValue & Movable](DuckDBValue & Copyable & Movable):
    """A DuckDB list."""
    alias Type = DuckDBType.list

    alias expected_element_type = T.Type
    var value: List[Optional[T]]

    fn __str__(self) -> String:
        return "DuckDBList"  # TODO

    fn __init__(out self, vector: Vector, length: Int, offset: Int) raises:
        var runtime_element_type = vector.get_column_type().get_type_id()
        if runtime_element_type != Self.expected_element_type:
            raise "Expected type " + String(
                Self.expected_element_type
            ) + " but got " + String(runtime_element_type)
        self.value = List[Optional[T]](capacity=length)

        var data_ptr = vector._get_data().bitcast[T]()
        var validity_mask = vector._get_validity_mask()

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
                    var is_valid = validity_mask[entry_idx] & (
                        1 << idx_in_entry
                    )
                    if is_valid:
                        self.value.append(Optional(data_ptr[idx + offset].copy()))
                    else:
                        self.value.append(None)
        elif Self.expected_element_type == DuckDBType.varchar:
            # validity mask can be null if there are no NULL values
            if not validity_mask:
                for idx in range(length):
                    self.value.append(Optional(T(vector, length=1, offset=offset + idx)))
            else:  # otherwise we have to check the validity mask for each element
                for idx in range(length):
                    var entry_idx = idx // 64
                    var idx_in_entry = idx % 64
                    var is_valid = validity_mask[entry_idx] & (
                        1 << idx_in_entry
                    )
                    if is_valid:
                        self.value.append(Optional(T(vector, length=1, offset=offset + idx)))
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
                            T(
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
                    var is_valid = validity_mask[entry_idx] & (
                        1 << idx_in_entry
                    )
                    if is_valid:
                        var list_entry = data_ptr[offset + idx]
                        self.value.append(
                            Optional(
                                T(
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
#     alias Type = DuckDBType.map
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
