from duckdb.vector import Vector

trait DBVal(CollectionElement, Stringable):
    """Represents a DuckDB value of any supported type."""

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        pass

    @staticmethod
    fn type() -> DuckDBType:
        pass


trait KeyElementVal(DBVal, KeyElement):
    pass

@value
@register_passable("trivial")
struct DTypeVal[duckdb_type: DuckDBType](DBVal, KeyElementVal):
    var value: Scalar[duckdb_type.to_dtype()]

    fn __str__(self) -> String:
        return self.value.__str__()

    fn __hash__(self) -> UInt:
        return self.value.__hash__()

    fn __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: Self) -> Bool:
        return self.value != other.value

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != duckdb_type:
            raise "Expected type " + str(duckdb_type) + " but got " + str(
                vector.get_column_type().get_type_id()
            )
        
        self = vector._get_data().bitcast[Self]()[offset=offset]

    @staticmethod
    fn type() -> DuckDBType:
        return duckdb_type


alias BoolVal = DTypeVal[DuckDBType.boolean]
alias Int8Val = DTypeVal[DuckDBType.tinyint]
alias Int16Val = DTypeVal[DuckDBType.smallint]
alias Int32Val = DTypeVal[DuckDBType.integer]
alias Int64Val = DTypeVal[DuckDBType.bigint]
alias UInt8Val = DTypeVal[DuckDBType.utinyint]
alias UInt16Val = DTypeVal[DuckDBType.usmallint]
alias UInt32Val = DTypeVal[DuckDBType.uinteger]
alias UInt64Val = DTypeVal[DuckDBType.ubigint]
alias Float32Val = DTypeVal[DuckDBType.float]
alias Float64Val = DTypeVal[DuckDBType.double]

@value
struct FixedSizeVal[duckdb_type: DuckDBType, underlying: StringableCollectionElement](DBVal):
    var value: underlying

    fn __str__(self) -> String:
        return self.value.__str__()

    # fn __hash__(self) -> UInt:
    #     return self.value.__hash__()

    # fn __eq__(self, other: Self) -> Bool:
    #     return self.value == other.value

    # fn __ne__(self, other: Self) -> Bool:
    #     return self.value != other.value

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != duckdb_type:
            raise "Expected type " + str(duckdb_type) + " but got " + str(
                vector.get_column_type().get_type_id()
            )

        self = vector._get_data().bitcast[Self]()[offset=offset]

    @staticmethod
    fn type() -> DuckDBType:
        return duckdb_type


alias TimestampVal = FixedSizeVal[DuckDBType.timestamp, Timestamp]
alias DateVal = FixedSizeVal[DuckDBType.date, Date]
alias TimeVal = FixedSizeVal[DuckDBType.time, Time]
alias IntervalVal = FixedSizeVal[DuckDBType.interval, Time]

@value
struct StringVal(DBVal):
    var value: String

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        if vector.get_column_type().get_type_id() != DuckDBType.varchar:
            raise "Expected type " + str(
                DuckDBType.varchar
            ) + " but got " + str(vector.get_column_type().get_type_id())
        var data_str_ptr = vector._get_data().bitcast[duckdb_string_t_pointer]()
        # Short strings are inlined so need to check the length and then cast accordingly.
        var string_length = int(data_str_ptr[offset].length)
        # TODO use duckdb_string_is_inlined helper instead
        if data_str_ptr[offset].length <= 12:
            var data_str_inlined = data_str_ptr.bitcast[
                duckdb_string_t_inlined
            ]()
            self.value = StringRef(
                data_str_inlined[offset].inlined.unsafe_ptr(), string_length
            )
        else:
            self.value = StringRef(data_str_ptr[offset].ptr, string_length)

    fn __str__(self) -> String:
        return self.value

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.varchar


@value
struct ListVal[T: DBVal](DBVal):
    """A DuckDB list."""

    alias expected_element_type = T.type()
    var value: List[Optional[T]]

    fn __str__(self) -> String:
        return "ListVal"  # TODO

    fn __init__(inout self, vector: Vector, length: Int, offset: Int) raises:
        var runtime_element_type = vector.get_column_type().get_type_id()
        if runtime_element_type != Self.expected_element_type:
            raise "Expected type " + str(
                Self.expected_element_type
            ) + " but got " + str(runtime_element_type)
        self.value = List[Optional[T]](capacity=length)
        if Self.expected_element_type.is_fixed_size():
            # if the element type is fixed size, we can directly get all values from the vector
            # that way we can avoid calling the constructor for each element
            self.value = vector.get_fixed_size_values[T](
                length=length, offset=offset
            )
            # for i in range(length):
            #     self.value.append(T(vector, length=1, offset=int(offset + i)))
        elif Self.expected_element_type == DuckDBType.varchar:
            for i in range(length):
                self.value.append(T(vector, length=1, offset=offset + i))
        elif Self.expected_element_type == DuckDBType.list:
            # pointer to list metadata (length and offset) that allows us to get the
            # correct positions of the actual data in the child vector
            var data_ptr = vector._get_data().bitcast[duckdb_list_entry]()
            # The child vector holds the actual list data in variable size entries (list_entry.length)
            var child_vector = vector.list_get_child()
            for i in range(length):
                var list_entry = data_ptr[offset + i]
                # if the subtime is a list itself, we need to call the constructor for each element recursively
                self.value.append(
                    Optional(
                        T(
                            child_vector,
                            length=int(list_entry.length),
                            offset=int(list_entry.offset),
                        )
                    )
                )
        else:
            raise Error(
                "Unsupported or invalid type: " + str(runtime_element_type)
            )

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.list


@value
struct MapVal[K: KeyElementVal, V: DBVal](DBVal):
    var value: Dict[K, V]

    fn __str__(self) -> String:
        return "MapVal"  # TODO

    fn __init__(
        inout self, vector: Vector, length: Int, offset: Int = 0
    ) raises:
        self.value = Dict[K, V]()
        raise "Not implemented"
        # Internally map vectors are stored as a LIST[STRUCT(key KEY_TYPE, value VALUE_TYPE)].
        # Via https://duckdb.org/docs/internals/vector#map-vectors
        # TODO fill dict
        # for i in range(length):
        #     self.key = K()
        #     self.value = V()

    @staticmethod
    fn type() -> DuckDBType:
        return DuckDBType.map
