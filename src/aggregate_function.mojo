from sys.info import size_of

from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.logical_type import LogicalType
from duckdb.connection import Connection
from duckdb.duckdb_type import dtype_to_duckdb_type


struct AggregateFunctionInfo:
    """Provides access to function information during aggregate function execution.

    This struct wraps the `duckdb_function_info` pointer passed to aggregate function callbacks.
    It's a non-owning wrapper - the underlying pointer is managed by DuckDB.

    Example:
    ```mojo
    from duckdb.aggregate_function import AggregateFunctionInfo

    fn my_update(info: AggregateFunctionInfo, input: Chunk, states: AggregateStateArray):
        var extra = info.get_extra_info()
        # ... process input and update states
    ```
    """

    var _info: duckdb_function_info

    fn __init__(out self, info: duckdb_function_info):
        """Creates an AggregateFunctionInfo from a duckdb_function_info pointer.

        This is a non-owning wrapper - the pointer is managed by DuckDB.

        Args:
            info: The duckdb_function_info pointer from the callback.
        """
        self._info = info

    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `AggregateFunction.set_extra_info()`.

        Returns:
            Pointer to the extra info data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_aggregate_function_get_extra_info(self._info)

    fn set_error(self, error: String):
        """Reports an error during aggregate function execution.

        This should be called when the function encounters an error.
        After calling this, the function should return.

        Args:
            error: The error message to report.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_error(
            self._info,
            error_copy.as_c_string_slice().unsafe_ptr(),
        )


struct AggregateState:
    """A wrapper around a single DuckDB aggregate state pointer.

    This is a non-owning wrapper around `duckdb_aggregate_state`.
    The underlying memory is managed by DuckDB.

    Example:
    ```mojo
    from duckdb.aggregate_function import AggregateState

    fn my_init(info: AggregateFunctionInfo, state: AggregateState):
        var data = state.get_data().bitcast[Int64]()
        data[] = 0  # Initialize state
    ```
    """

    var _state: duckdb_aggregate_state

    fn __init__(out self, state: duckdb_aggregate_state):
        """Creates an AggregateState wrapper.

        Args:
            state: The duckdb_aggregate_state pointer.
        """
        self._state = state

    fn get_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Returns a pointer to the state's internal data.

        The `duckdb_aggregate_state` pointer directly points to user state data.
        DuckDB allocates `state_size()` bytes and reinterpret_casts the pointer
        as `duckdb_aggregate_state`. The actual user state lives at the same address.

        Returns:
            Pointer to the state's data (to be bitcast to the actual state type).
        """
        # duckdb_aggregate_state is UnsafePointer[_duckdb_aggregate_state].
        # The state data lives at the address self._state points to — NOT
        # at self._state[].internal_ptr.  DuckDB reinterpret_casts the raw
        # allocation to duckdb_aggregate_state, so the user struct IS the
        # memory at that address.
        return self._state.bitcast[NoneType]()


struct AggregateStateArray(Sized):
    """A wrapper around an array of DuckDB aggregate state pointers.

    This is passed to update, combine, finalize, and destroy callbacks.
    Each element is a `duckdb_aggregate_state` pointer.

    Example:
    ```mojo
    from duckdb import Chunk
    from duckdb.aggregate_function import AggregateFunctionInfo, AggregateStateArray

    fn my_update(info: AggregateFunctionInfo, input: Chunk, states: AggregateStateArray):
        var size = len(input)
        for i in range(size):
            var state_data = states.get_state(i).get_data().bitcast[Int64]()
            # ... update state
    ```
    """

    var _states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin]
    var _count: Int

    fn __init__(out self, states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], count: Int = 0):
        """Creates an AggregateStateArray wrapper.

        Args:
            states: Pointer to the array of aggregate states.
            count: The number of states (0 if unknown, e.g., for update where count comes from the chunk).
        """
        self._states = states
        self._count = count

    fn get_state(self, index: Int) -> AggregateState:
        """Gets the state at the specified index.

        Args:
            index: The index of the state to retrieve.

        Returns:
            The aggregate state at the given index.
        """
        return AggregateState(self._states[index])

    fn __len__(self) -> Int:
        """Returns the number of states in this array.

        Returns:
            The number of states.
        """
        return self._count


struct _ReduceState[D: DType](Movable):
    """Internal state for reduction-based aggregate functions.

    Stores the accumulated value and a count to track whether any
    non-NULL input was seen (for proper NULL result on empty groups).
    """

    var value: Scalar[Self.D]
    var count: Int64

    fn __init__(out self, *, value: Scalar[Self.D], count: Int64):
        self.value = value
        self.count = count

    fn __moveinit__(out self, deinit take: Self):
        self.value = take.value
        self.count = take.count


struct AggregateFunction(Movable):
    """An aggregate function that can be registered in DuckDB.

    Aggregate functions process multiple rows and produce a single result value
    per group. They require four callback functions:

    1. **state_size**: Returns the size of the aggregate state in bytes.
    2. **state_init**: Initializes a new aggregate state.
    3. **update**: Called for each input row to update the state.
    4. **combine**: Merges two states (for parallel aggregation).
    5. **finalize**: Produces the final result from the state.

    Optionally, a **destroy** callback can clean up state resources.

    Example:
    ```mojo
    from duckdb import Connection, DuckDBType, Chunk
    from duckdb.aggregate_function import (
        AggregateFunction, AggregateFunctionInfo, AggregateState, AggregateStateArray,
    )
    from duckdb.logical_type import LogicalType
    from duckdb.vector import Vector

    # Simple SUM aggregate for integers
    fn my_state_size(info: AggregateFunctionInfo) -> idx_t:
        return size_of[Int64]()

    fn my_state_init(info: AggregateFunctionInfo, state: AggregateState):
        state.get_data().bitcast[Int64]().init_pointee_move(0)

    fn my_update(info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray):
        var size = len(input)
        var data = input.get_vector(0).get_data().bitcast[Int32]()
        for i in range(size):
            var s = states.get_state(i).get_data().bitcast[Int64]()
            s[] += Int64(data[i])

    fn my_combine(info: AggregateFunctionInfo, source: AggregateStateArray,
                  target: AggregateStateArray, count: Int):
        for i in range(count):
            var s = source.get_state(i).get_data().bitcast[Int64]()
            var t = target.get_state(i).get_data().bitcast[Int64]()
            t[] += s[]

    fn my_finalize(info: AggregateFunctionInfo, source: AggregateStateArray,
                   result: Vector, count: Int, offset: Int):
        var out = result.get_data().bitcast[Int64]()
        for i in range(count):
            var s = source.get_state(i).get_data().bitcast[Int64]()
            out[offset + i] = s[]

    var conn = Connection(":memory:")
    var func = AggregateFunction()
    func.set_name("my_sum")
    func.add_parameter(LogicalType(DuckDBType.integer))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_functions[my_state_size, my_state_init, my_update, my_combine, my_finalize]()
    func.register(conn)
    ```
    """

    var _function: duckdb_aggregate_function
    var _owned: Bool

    fn __init__(out self):
        """Creates a new aggregate function.

        The function must be destroyed with `__del__` or by letting it go out of scope.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._function = libduckdb.duckdb_create_aggregate_function()
        self._owned = True

    fn __moveinit__(out self, deinit take: Self):
        """Move constructor that transfers ownership."""
        self._function = take._function
        self._owned = take._owned

    fn __copyinit__(out self, copy: Self):
        """Copy constructor - shares the pointer but doesn't own it."""
        self._function = copy._function
        self._owned = False

    fn __del__(deinit self):
        """Destroys the aggregate function and deallocates all memory."""
        if self._owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_aggregate_function(
                UnsafePointer(to=self._function)
            )

    fn set_name(self, name: String):
        """Sets the name of the aggregate function.

        Args:
            name: The name of the aggregate function.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_name(
            self._function,
            name_copy.as_c_string_slice().unsafe_ptr(),
        )

    fn add_parameter(self, type: LogicalType):
        """Adds a parameter to the aggregate function.

        Args:
            type: The type of the parameter to add.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_add_parameter(
            self._function, type._logical_type
        )

    fn set_return_type(self, type: LogicalType):
        """Sets the return type of the aggregate function.

        Args:
            type: The return type of the aggregate function. Cannot contain INVALID or ANY.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_return_type(
            self._function, type._logical_type
        )

    fn set_special_handling(self):
        """Sets the NULL handling of the aggregate function to SPECIAL_HANDLING.

        When enabled, NULL values are not automatically filtered and are passed
        to the update function.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_special_handling(self._function)

    fn set_extra_info(
        self,
        extra_info: UnsafePointer[NoneType, MutAnyOrigin],
        destroy: duckdb_delete_callback_t,
    ):
        """Assigns extra information to the aggregate function.

        This information can be fetched during execution using
        `AggregateFunctionInfo.get_extra_info()`.

        Args:
            extra_info: The extra information pointer.
            destroy: The callback that will be called to destroy the extra information.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_extra_info(
            self._function, extra_info, destroy
        )

    fn set_functions[
        state_size_fn: fn (AggregateFunctionInfo) -> idx_t,
        state_init_fn: fn (AggregateFunctionInfo, AggregateState) -> None,
        update_fn: fn (AggregateFunctionInfo, mut Chunk, AggregateStateArray) -> None,
        combine_fn: fn (AggregateFunctionInfo, AggregateStateArray, AggregateStateArray, Int) -> None,
        finalize_fn: fn (AggregateFunctionInfo, AggregateStateArray, Vector, Int, Int) -> None,
    ](self):
        """Sets all callback functions for the aggregate function using high-level Mojo types.

        Automatically creates wrappers that convert low-level FFI types to
        high-level Mojo types before calling your functions.

        Parameters:
            state_size_fn: Returns the size of the aggregate state in bytes.
            state_init_fn: Initializes a new aggregate state.
            update_fn: Updates aggregate states with new input values.
            combine_fn: Combines source states into target states.
            finalize_fn: Produces final results from aggregate states.

        Example:
        ```mojo
        from duckdb.aggregate_function import (
            AggregateFunction, AggregateFunctionInfo, AggregateState,
            AggregateStateArray,
        )
        from duckdb import Chunk
        from duckdb.vector import Vector

        fn size(info: AggregateFunctionInfo) -> idx_t:
            return size_of[Int64]()

        fn init(info: AggregateFunctionInfo, state: AggregateState):
            state.get_data().bitcast[Int64]().init_pointee_move(0)

        fn update(info: AggregateFunctionInfo, mut input: Chunk, states: AggregateStateArray):
            var n = len(input)
            var data = input.get_vector(0).get_data().bitcast[Int32]()
            for i in range(n):
                var s = states.get_state(i).get_data().bitcast[Int64]()
                s[] += Int64(data[i])

        fn combine(info: AggregateFunctionInfo, source: AggregateStateArray,
                   target: AggregateStateArray, count: Int):
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[Int64]()
                var t = target.get_state(i).get_data().bitcast[Int64]()
                t[] += s[]

        fn finalize(info: AggregateFunctionInfo, source: AggregateStateArray,
                    result: Vector, count: Int, offset: Int):
            var out = result.get_data().bitcast[Int64]()
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[Int64]()
                out[offset + i] = s[]

        var func = AggregateFunction()
        func.set_functions[size, init, update, combine, finalize]()
        ```
        """

        fn raw_state_size(raw_info: duckdb_function_info) -> idx_t:
            var info = AggregateFunctionInfo(raw_info)
            return state_size_fn(info)

        fn raw_state_init(
            raw_info: duckdb_function_info, raw_state: duckdb_aggregate_state
        ):
            var info = AggregateFunctionInfo(raw_info)
            var state = AggregateState(raw_state)
            state_init_fn(info, state)

        fn raw_update(
            raw_info: duckdb_function_info,
            raw_input: duckdb_data_chunk,
            raw_states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin],
        ):
            var info = AggregateFunctionInfo(raw_info)
            var input_chunk = Chunk[is_owned=False](raw_input)
            var states = AggregateStateArray(raw_states)
            update_fn(info, input_chunk, states)

        fn raw_combine(
            raw_info: duckdb_function_info,
            raw_source: UnsafePointer[
                duckdb_aggregate_state, MutExternalOrigin
            ],
            raw_target: UnsafePointer[
                duckdb_aggregate_state, MutExternalOrigin
            ],
            count: idx_t,
        ):
            var info = AggregateFunctionInfo(raw_info)
            var source = AggregateStateArray(raw_source, Int(count))
            var target = AggregateStateArray(raw_target, Int(count))
            combine_fn(info, source, target, Int(count))

        fn raw_finalize(
            raw_info: duckdb_function_info,
            raw_source: UnsafePointer[
                duckdb_aggregate_state, MutExternalOrigin
            ],
            raw_result: duckdb_vector,
            count: idx_t,
            offset: idx_t,
        ):
            var info = AggregateFunctionInfo(raw_info)
            var source = AggregateStateArray(raw_source, Int(count))
            var result = Vector[False, MutExternalOrigin](raw_result)
            finalize_fn(info, source, result, Int(count), Int(offset))

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_functions(
            self._function,
            raw_state_size,
            raw_state_init,
            raw_update,
            raw_combine,
            raw_finalize,
        )

    fn set_destructor[
        destroy_fn: fn (AggregateStateArray) -> None,
    ](self):
        """Sets an optional state destructor callback.

        The destructor is called when aggregate states are no longer needed.
        Use this to free any resources allocated in the state.

        Parameters:
            destroy_fn: Function to destroy aggregate states.

        Example:
        ```mojo
        from duckdb.aggregate_function import AggregateFunction, AggregateStateArray

        fn my_destroy(states: AggregateStateArray):
            for i in range(len(states)):
                var s = states.get_state(i).get_data().bitcast[Int64]()
                s.destroy_pointee()

        var func = AggregateFunction()
        func.set_destructor[my_destroy]()
        ```
        """

        fn raw_destroy(
            raw_states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin],
            count: idx_t,
        ):
            var states = AggregateStateArray(raw_states, Int(count))
            destroy_fn(states)

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_aggregate_function_set_destructor(
            self._function, raw_destroy
        )

    fn register(mut self, conn: Connection) raises:
        """Registers the aggregate function within the given connection.

        The function requires at least a name, a return type,
        and an update and finalize function.

        This method releases ownership to transfer it to DuckDB.

        Args:
            conn: The connection to register the function in.

        Raises:
            Error if the registration was unsuccessful.
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_register_aggregate_function(
            conn._conn, self._function
        )
        if status != DuckDBSuccess:
            raise Error("Failed to register aggregate function")
        self._owned = False

    # ===--------------------------------------------------------------------===#
    # High-level reduction-based aggregate constructors
    # ===--------------------------------------------------------------------===#

    @staticmethod
    fn from_reduce[
        name: StringLiteral,
        D: DType,
        reduce_fn: fn[width: Int] (
            SIMD[D, width], SIMD[D, width]
        ) -> SIMD[D, width],
        init_fn: fn () -> Scalar[D],
    ](conn: Connection) raises:
        """Create and register a unary aggregate from a SIMD reduction function.

        Auto-generates all aggregate callbacks (state_size, init, update,
        combine, finalize, destroy) from a single binary SIMD reduce function
        and an identity element function.  Input and output share the same DType.

        The reduce function must be associative and commutative.  The init function
        returns the identity element (e.g. 0 for sum, MIN for max).

        Empty groups (no non-NULL input) correctly produce NULL in the output.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.
            reduce_fn: A binary SIMD function for combining values.
            init_fn: Returns the identity element for the reduction.

        Example:
        ```mojo
        fn my_add[w: Int](a: SIMD[DType.float64, w], b: SIMD[DType.float64, w]) -> SIMD[DType.float64, w]:
            return a + b
        fn zero() -> Scalar[DType.float64]: return 0.0

        AggregateFunction.from_reduce["my_sum", DType.float64, my_add, zero](conn)
        ```
        """

        fn _state_size(info: AggregateFunctionInfo) -> idx_t:
            return idx_t(size_of[_ReduceState[D]]())

        fn _state_init(info: AggregateFunctionInfo, state: AggregateState):
            state.get_data().bitcast[_ReduceState[D]]().init_pointee_move(
                _ReduceState[D](value=init_fn(), count=0)
            )

        fn _update(
            info: AggregateFunctionInfo,
            mut input: Chunk,
            states: AggregateStateArray,
        ):
            var n = len(input)
            var data = input.get_vector(0).get_data().bitcast[Scalar[D]]()
            for i in range(n):
                var s = states.get_state(i).get_data().bitcast[_ReduceState[D]]()
                s[].value = reduce_fn[1](s[].value, data[i])
                s[].count += 1

        fn _combine(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            target: AggregateStateArray,
            count: Int,
        ):
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                var t = target.get_state(i).get_data().bitcast[_ReduceState[D]]()
                t[].value = reduce_fn[1](t[].value, s[].value)
                t[].count += s[].count

        fn _finalize(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            result: Vector,
            count: Int,
            offset: Int,
        ):
            var out = result.get_data().bitcast[Scalar[D]]()
            result.ensure_validity_writable()
            var validity = result.get_validity()
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                if s[].count > 0:
                    out[offset + i] = s[].value
                else:
                    var row = offset + i
                    validity[row // 64] = validity[row // 64] & ~(
                        UInt64(1) << UInt64(row % 64)
                    )

        fn _destroy(states: AggregateStateArray):
            for i in range(len(states)):
                states.get_state(i).get_data().bitcast[
                    _ReduceState[D]
                ]().destroy_pointee()

        var func = AggregateFunction()
        func.set_name(name)
        func.add_parameter(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_return_type(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_functions[
            _state_size, _state_init, _update, _combine, _finalize
        ]()
        func.set_destructor[_destroy]()
        func.register(conn)

    @staticmethod
    fn from_reduce[
        name: StringLiteral,
        In: DType,
        Out: DType,
        reduce_fn: fn[width: Int] (
            SIMD[Out, width], SIMD[Out, width]
        ) -> SIMD[Out, width],
        init_fn: fn () -> Scalar[Out],
    ](conn: Connection) raises:
        """Create and register a unary aggregate with separate input/output types.

        Input values are cast to ``Out`` before accumulation.  The reduce
        function and identity operate on the output type.

        Parameters:
            name: The SQL function name.
            In: The input DType.
            Out: The output / accumulator DType.
            reduce_fn: A binary SIMD function for combining values.
            init_fn: Returns the identity element for the reduction.

        Example:
        ```mojo
        fn add[w: Int](a: SIMD[DType.int64, w], b: SIMD[DType.int64, w]) -> SIMD[DType.int64, w]:
            return a + b
        fn zero() -> Scalar[DType.int64]: return 0

        # Sum int32 values into a int64 accumulator
        AggregateFunction.from_reduce["wide_sum", DType.int32, DType.int64, add, zero](conn)
        ```
        """

        fn _state_size(info: AggregateFunctionInfo) -> idx_t:
            return idx_t(size_of[_ReduceState[Out]]())

        fn _state_init(info: AggregateFunctionInfo, state: AggregateState):
            state.get_data().bitcast[_ReduceState[Out]]().init_pointee_move(
                _ReduceState[Out](value=init_fn(), count=0)
            )

        fn _update(
            info: AggregateFunctionInfo,
            mut input: Chunk,
            states: AggregateStateArray,
        ):
            var n = len(input)
            var data = input.get_vector(0).get_data().bitcast[Scalar[In]]()
            for i in range(n):
                var s = states.get_state(i).get_data().bitcast[_ReduceState[Out]]()
                s[].value = reduce_fn[1](s[].value, data[i].cast[Out]())
                s[].count += 1

        fn _combine(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            target: AggregateStateArray,
            count: Int,
        ):
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[Out]]()
                var t = target.get_state(i).get_data().bitcast[_ReduceState[Out]]()
                t[].value = reduce_fn[1](t[].value, s[].value)
                t[].count += s[].count

        fn _finalize(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            result: Vector,
            count: Int,
            offset: Int,
        ):
            var out = result.get_data().bitcast[Scalar[Out]]()
            result.ensure_validity_writable()
            var validity = result.get_validity()
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[Out]]()
                if s[].count > 0:
                    out[offset + i] = s[].value
                else:
                    var row = offset + i
                    validity[row // 64] = validity[row // 64] & ~(
                        UInt64(1) << UInt64(row % 64)
                    )

        fn _destroy(states: AggregateStateArray):
            for i in range(len(states)):
                states.get_state(i).get_data().bitcast[
                    _ReduceState[Out]
                ]().destroy_pointee()

        var func = AggregateFunction()
        func.set_name(name)
        func.add_parameter(LogicalType(dtype_to_duckdb_type[In]()))
        func.set_return_type(LogicalType(dtype_to_duckdb_type[Out]()))
        func.set_functions[
            _state_size, _state_init, _update, _combine, _finalize
        ]()
        func.set_destructor[_destroy]()
        func.register(conn)

    @staticmethod
    fn from_reduce[
        name: StringLiteral,
        D: DType,
        reduce_fn: fn[dtype: DType, width: Int] (
            SIMD[dtype, width], SIMD[dtype, width]
        ) -> SIMD[dtype, width],
        init_fn: fn () -> Scalar[D],
    ](conn: Connection) raises:
        """Create and register a unary aggregate from a stdlib-compatible function.

        Accepts functions with the standard library signature
        ``fn[dtype: DType, width: Int](SIMD[dtype, width], SIMD[dtype, width]) -> SIMD[dtype, width]``
        so you can pass stdlib math helpers directly.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.
            reduce_fn: A stdlib-compatible binary SIMD function.
            init_fn: Returns the identity element for the reduction.

        Example:
        ```mojo
        import math
        fn zero() -> Scalar[DType.float64]: return 0.0
        AggregateFunction.from_reduce["my_sum", DType.float64, math.add, zero](conn)
        ```
        """

        fn _state_size(info: AggregateFunctionInfo) -> idx_t:
            return idx_t(size_of[_ReduceState[D]]())

        fn _state_init(info: AggregateFunctionInfo, state: AggregateState):
            state.get_data().bitcast[_ReduceState[D]]().init_pointee_move(
                _ReduceState[D](value=init_fn(), count=0)
            )

        fn _update(
            info: AggregateFunctionInfo,
            mut input: Chunk,
            states: AggregateStateArray,
        ):
            var n = len(input)
            var data = input.get_vector(0).get_data().bitcast[Scalar[D]]()
            for i in range(n):
                var s = states.get_state(i).get_data().bitcast[_ReduceState[D]]()
                s[].value = reduce_fn[D, 1](s[].value, data[i])
                s[].count += 1

        fn _combine(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            target: AggregateStateArray,
            count: Int,
        ):
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                var t = target.get_state(i).get_data().bitcast[_ReduceState[D]]()
                t[].value = reduce_fn[D, 1](t[].value, s[].value)
                t[].count += s[].count

        fn _finalize(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            result: Vector,
            count: Int,
            offset: Int,
        ):
            var out = result.get_data().bitcast[Scalar[D]]()
            result.ensure_validity_writable()
            var validity = result.get_validity()
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                if s[].count > 0:
                    out[offset + i] = s[].value
                else:
                    var row = offset + i
                    validity[row // 64] = validity[row // 64] & ~(
                        UInt64(1) << UInt64(row % 64)
                    )

        fn _destroy(states: AggregateStateArray):
            for i in range(len(states)):
                states.get_state(i).get_data().bitcast[
                    _ReduceState[D]
                ]().destroy_pointee()

        var func = AggregateFunction()
        func.set_name(name)
        func.add_parameter(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_return_type(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_functions[
            _state_size, _state_init, _update, _combine, _finalize
        ]()
        func.set_destructor[_destroy]()
        func.register(conn)

    # ===--------------------------------------------------------------------===#
    # Convenience aggregate constructors
    # ===--------------------------------------------------------------------===#

    @staticmethod
    fn from_sum[name: StringLiteral, D: DType](conn: Connection) raises:
        """Create and register a SUM aggregate.

        Computes the sum of all non-NULL input values.  Returns NULL for
        empty groups.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.

        Example:
        ```mojo
        AggregateFunction.from_sum["my_sum", DType.float64](conn)
        # SELECT my_sum(x) FROM t
        ```
        """

        fn _add[w: Int](a: SIMD[D, w], b: SIMD[D, w]) -> SIMD[D, w]:
            return a + b

        fn _zero() -> Scalar[D]:
            return 0

        AggregateFunction.from_reduce[name, D, _add, _zero](conn)

    @staticmethod
    fn from_product[name: StringLiteral, D: DType](conn: Connection) raises:
        """Create and register a PRODUCT aggregate.

        Computes the product of all non-NULL input values.  Returns NULL for
        empty groups.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.

        Example:
        ```mojo
        AggregateFunction.from_product["my_product", DType.float64](conn)
        ```
        """

        fn _mul[w: Int](a: SIMD[D, w], b: SIMD[D, w]) -> SIMD[D, w]:
            return a * b

        fn _one() -> Scalar[D]:
            return 1

        AggregateFunction.from_reduce[name, D, _mul, _one](conn)

    @staticmethod
    fn from_max[name: StringLiteral, D: DType](conn: Connection) raises:
        """Create and register a MAX aggregate.

        Returns the maximum non-NULL input value, or NULL for empty groups.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.

        Example:
        ```mojo
        AggregateFunction.from_max["my_max", DType.float64](conn)
        ```
        """

        fn _max[w: Int](a: SIMD[D, w], b: SIMD[D, w]) -> SIMD[D, w]:
            return max(a, b)

        fn _init() -> Scalar[D]:
            return Scalar[D].MIN

        AggregateFunction.from_reduce[name, D, _max, _init](conn)

    @staticmethod
    fn from_min[name: StringLiteral, D: DType](conn: Connection) raises:
        """Create and register a MIN aggregate.

        Returns the minimum non-NULL input value, or NULL for empty groups.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output.

        Example:
        ```mojo
        AggregateFunction.from_min["my_min", DType.float64](conn)
        ```
        """

        fn _min[w: Int](a: SIMD[D, w], b: SIMD[D, w]) -> SIMD[D, w]:
            return min(a, b)

        fn _init() -> Scalar[D]:
            return Scalar[D].MAX

        AggregateFunction.from_reduce[name, D, _min, _init](conn)

    @staticmethod
    fn from_mean[name: StringLiteral, D: DType](conn: Connection) raises:
        """Create and register a MEAN (average) aggregate.

        Computes the arithmetic mean of all non-NULL input values.
        Returns NULL for empty groups.  D must be a floating-point type.

        Parameters:
            name: The SQL function name.
            D: The DType for input and output (should be floating-point).

        Example:
        ```mojo
        AggregateFunction.from_mean["my_avg", DType.float64](conn)
        ```
        """

        fn _state_size(info: AggregateFunctionInfo) -> idx_t:
            return idx_t(size_of[_ReduceState[D]]())

        fn _state_init(info: AggregateFunctionInfo, state: AggregateState):
            state.get_data().bitcast[_ReduceState[D]]().init_pointee_move(
                _ReduceState[D](value=Scalar[D](0), count=0)
            )

        fn _update(
            info: AggregateFunctionInfo,
            mut input: Chunk,
            states: AggregateStateArray,
        ):
            var n = len(input)
            var data = input.get_vector(0).get_data().bitcast[Scalar[D]]()
            for i in range(n):
                var s = states.get_state(i).get_data().bitcast[_ReduceState[D]]()
                s[].value += data[i]
                s[].count += 1

        fn _combine(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            target: AggregateStateArray,
            count: Int,
        ):
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                var t = target.get_state(i).get_data().bitcast[_ReduceState[D]]()
                t[].value += s[].value
                t[].count += s[].count

        fn _finalize(
            info: AggregateFunctionInfo,
            source: AggregateStateArray,
            result: Vector,
            count: Int,
            offset: Int,
        ):
            var out = result.get_data().bitcast[Scalar[D]]()
            result.ensure_validity_writable()
            var validity = result.get_validity()
            for i in range(count):
                var s = source.get_state(i).get_data().bitcast[_ReduceState[D]]()
                if s[].count > 0:
                    out[offset + i] = s[].value / Scalar[D](s[].count)
                else:
                    var row = offset + i
                    validity[row // 64] = validity[row // 64] & ~(
                        UInt64(1) << UInt64(row % 64)
                    )

        fn _destroy(states: AggregateStateArray):
            for i in range(len(states)):
                states.get_state(i).get_data().bitcast[
                    _ReduceState[D]
                ]().destroy_pointee()

        var func = AggregateFunction()
        func.set_name(name)
        func.add_parameter(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_return_type(LogicalType(dtype_to_duckdb_type[D]()))
        func.set_functions[
            _state_size, _state_init, _update, _combine, _finalize
        ]()
        func.set_destructor[_destroy]()
        func.register(conn)


struct AggregateFunctionSet(Movable):
    """A set of aggregate function overloads with the same name but different signatures.

    This allows registering multiple versions of an aggregate function that handle
    different parameter types or counts.

    Example:
    ```mojo
    from duckdb import Connection
    from duckdb.aggregate_function import AggregateFunction, AggregateFunctionSet

    var func_set = AggregateFunctionSet("my_agg")

    # Add overload for INTEGER -> BIGINT
    var func1 = AggregateFunction()
    # ... configure func1 ...
    func_set.add_function(func1)

    # Add overload for DOUBLE -> DOUBLE
    var func2 = AggregateFunction()
    # ... configure func2 ...
    func_set.add_function(func2)

    var conn = Connection(":memory:")
    func_set.register(conn)
    ```
    """

    var _function_set: duckdb_aggregate_function_set
    var _owned: Bool

    fn __init__(out self, name: String):
        """Creates a new aggregate function set.

        Args:
            name: The name for all functions in this set.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        self._function_set = libduckdb.duckdb_create_aggregate_function_set(
            name_copy.as_c_string_slice().unsafe_ptr()
        )
        self._owned = True

    fn __moveinit__(out self, deinit take: Self):
        """Move constructor that transfers ownership."""
        self._function_set = take._function_set
        self._owned = take._owned

    fn __copyinit__(out self, copy: Self):
        """Copy constructor - shares the pointer but doesn't own it."""
        self._function_set = copy._function_set
        self._owned = False

    fn __del__(deinit self):
        """Destroys the aggregate function set and deallocates all memory."""
        if self._owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_aggregate_function_set(
                UnsafePointer(to=self._function_set)
            )

    fn add_function(self, function: AggregateFunction) raises:
        """Adds an aggregate function as a new overload to the function set.

        Args:
            function: The function to add. Must have matching name.

        Raises:
            Error if the function could not be added (e.g., duplicate signature).
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_add_aggregate_function_to_set(
            self._function_set, function._function
        )
        if status != DuckDBSuccess:
            raise Error(
                "Failed to add function to set - overload may already exist"
            )

    fn register(mut self, conn: Connection) raises:
        """Registers the aggregate function set within the given connection.

        The set requires at least one valid overload.

        Args:
            conn: The connection to register the function set in.

        Raises:
            Error if the registration was unsuccessful.
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_register_aggregate_function_set(
            conn._conn, self._function_set
        )
        if status != DuckDBSuccess:
            raise Error("Failed to register aggregate function set")
        self._owned = False
