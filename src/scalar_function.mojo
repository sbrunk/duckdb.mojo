from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.logical_type import LogicalType
from duckdb.connection import Connection


struct FunctionInfo:
    """Provides access to function information during scalar function execution.
    
    This struct wraps the `duckdb_function_info` pointer passed to scalar function callbacks.
    It's a non-owning wrapper - the underlying pointer is managed by DuckDB.
    
    Example:
    ```mojo
    from duckdb import Connection, DuckDBType, Chunk
    from duckdb.scalar_function import ScalarFunction, FunctionInfo
    from duckdb.logical_type import LogicalType
    from duckdb.vector import Vector
    
    fn my_function(info: FunctionInfo, input: Chunk, output: Vector):
        # Access extra info
        var extra = info.get_extra_info()
        
        # Access bind data
        var bind_data = info.get_bind_data()
        
        # Report errors if something goes wrong
        if len(input) == 0:
            info.set_error("Input chunk is empty")
            return
        
        # Process data...
        var size = len(input)
        # ... your processing logic here
    ```
    """
    
    var _info: duckdb_function_info
    
    fn __init__(out self, info: duckdb_function_info):
        """Creates a FunctionInfo from a duckdb_function_info pointer.
        
        This is a non-owning wrapper - the pointer is managed by DuckDB.
        
        Args:
            info: The duckdb_function_info pointer from the callback.
        """
        self._info = info
    
    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `ScalarFunction.set_extra_info()`.
        
        Returns:
            Pointer to the extra info data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_get_extra_info(self._info)
    
    fn get_bind_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the bind data set during the bind phase.
        
        Note that the bind data is read-only during execution.
        
        Returns:
            Pointer to the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_get_bind_data(self._info)
    
    fn set_error(self, error: String):
        """Reports an error during function execution.
        
        This should be called when the function encounters an error.
        After calling this, the function should return without setting output values.
        
        Args:
            error: The error message to report.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_error(
            self._info,
            error_copy.as_c_string_slice().unsafe_ptr()
        )


struct ScalarFunction(Movable):
    """A scalar function that can be registered in DuckDB.

    Functions are written using high-level Mojo types (FunctionInfo, Chunk, Vector)
    which provide better ergonomics and type safety. The API automatically handles
    conversion from low-level FFI types.
    
    Example:
    ```mojo
    from duckdb import Connection, DuckDBType
    from duckdb.scalar_function import ScalarFunction, FunctionInfo
    from duckdb.logical_type import LogicalType
    from duckdb import Chunk
    from duckdb.vector import Vector
    
    fn add_one(info: FunctionInfo, input: Chunk, output: Vector):
        var size = len(input)
        var in_vec = input.get_vector(0)
        var in_data = in_vec.get_data().bitcast[Int32]()
        var out_data = output.get_data().bitcast[Int32]()
        
        for i in range(size):
            out_data[i] = in_data[i] + 1
        
    var conn = Connection(":memory:")
    var func = ScalarFunction()
    func.set_name("add_one")
    var int_type = LogicalType(DuckDBType.integer)
    func.add_parameter(int_type)
    func.set_return_type(int_type)
    func.set_function[add_one]()  # Pass function as compile-time parameter
    func.register(conn)
    ```
    """

    var _function: duckdb_scalar_function
    var _owned: Bool  # Tracks if this struct owns the underlying pointer

    fn __init__(out self):
        """Creates a new scalar function.
        
        The function must be destroyed with `__del__` or by letting it go out of scope.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._function = libduckdb.duckdb_create_scalar_function()
        self._owned = True

    fn __moveinit__(out self, deinit take: Self):
        """Move constructor that transfers ownership."""
        self._function = take._function
        self._owned = take._owned

    fn __copyinit__(out self, copy: Self):
        """Copy constructor - shares the pointer but doesn't own it."""
        self._function = copy._function
        self._owned = False  # Copy doesn't own the resource

    fn __del__(deinit self):
        """Destroys the scalar function and deallocates all memory."""
        if self._owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_scalar_function(UnsafePointer(to=self._function))

    fn _release_ownership(mut self):
        """Releases ownership of the underlying pointer.
        
        After calling this, the destructor will not destroy the underlying DuckDB function.
        This is used internally when adding functions to sets or registering them.
        """
        self._owned = False

    fn set_name(self, name: String):
        """Sets the name of the scalar function.

        * name: The name of the scalar function.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_name(
            self._function, 
            name_copy.as_c_string_slice().unsafe_ptr()
        )

    fn set_varargs(self, type: LogicalType):
        """Sets the scalar function as varargs.
        
        This allows the function to accept a variable number of arguments of the specified type.

        * type: The type of the variable arguments.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_varargs(self._function, type._logical_type)

    fn set_special_handling(self):
        """Sets the scalar function to use special handling.
        
        This is used for functions that require special handling during binding or execution.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_special_handling(self._function)

    fn set_volatile(self):
        """Sets the scalar function as volatile.
        
        Volatile functions can return different results for the same input (e.g., random(), now()).
        This prevents the optimizer from constant-folding calls to this function.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_volatile(self._function)

    fn add_parameter(self, type: LogicalType):
        """Adds a parameter to the scalar function.

        * type: The type of the parameter to add.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_add_parameter(self._function, type._logical_type)

    fn set_return_type(self, type: LogicalType):
        """Sets the return type of the scalar function.

        * type: The return type of the scalar function.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_return_type(self._function, type._logical_type)

    fn set_extra_info(
        self, 
        extra_info: UnsafePointer[NoneType, MutAnyOrigin], 
        destroy: duckdb_delete_callback_t
    ):
        """Assigns extra information to the scalar function.
        
        This information can be fetched during binding, execution, etc using `get_extra_info`.

        * extra_info: The extra information pointer.
        * destroy: The callback that will be called to destroy the extra information.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_extra_info(
            self._function, 
            extra_info, 
            destroy
        )

    fn set_bind(self, bind: duckdb_scalar_function_bind_t):
        """Sets the bind function of the scalar function.
        
        The bind function is called during query planning and can be used to:
        - Validate arguments
        - Set the return type dynamically
        - Store bind data for use during execution

        * bind: The bind function callback.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_bind(self._function, bind)

    fn set_function[
        func: fn(FunctionInfo, mut Chunk, Vector) -> None
    ](self):
        """Sets the main execution function using high-level Mojo types.
        
        Automatically creates a wrapper that converts all low-level FFI types
        to high-level Mojo types (FunctionInfo, Chunk, Vector) before calling your function.
        
        Example:
        ```mojo
        from duckdb import Chunk, Vector
        from duckdb.scalar_function import FunctionInfo, ScalarFunction
        fn my_add_one(info: FunctionInfo, mut input: Chunk, output: Vector):
            var size = len(input)
            var in_vec = input.get_vector(0)
            var in_data = in_vec.get_data().bitcast[Int32]()
            var out_data = output.get_data().bitcast[Int32]()
            
            for i in range(size):
                out_data[i] = in_data[i] + 1
        
        var func = ScalarFunction()
        func.set_function[my_add_one]()  # Pass function as compile-time parameter
        ```
        
        * func: Your function with signature fn(FunctionInfo, mut Chunk, Vector)..
        """
        # Create a wrapper function that converts FFI types to high-level types
        fn wrapper(raw_info: duckdb_function_info, 
                   raw_input: duckdb_data_chunk, 
                   raw_output: duckdb_vector):
            # Wrap FFI types in high-level non-owning wrappers
            var info = FunctionInfo(raw_info)
            var input_chunk = Chunk[is_owned=False](raw_input)
            # Output vector doesn't need chunk reference - DuckDB manages its lifetime
            var output_vec = Vector[False, MutExternalOrigin](raw_output)
            
            # Call the user's high-level function
            func(info, input_chunk, output_vec)
        
        # Register the wrapper with DuckDB
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_function(self._function, wrapper)

    fn register(mut self, conn: Connection) raises:
        """Registers the scalar function within the given connection.

        The function requires at least a name, a function, and a return type.
        This method releases ownership to transfer it to DuckDB.
        The function will be managed by the connection and should not be destroyed manually.
        
        * conn: The connection to register the function in.
        * raises: Error if the registration was unsuccessful.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_scalar_function(conn._conn, self._function)
        # Release ownership - DuckDB now manages this
        self._owned = False

    @staticmethod
    fn get_extra_info(info: duckdb_function_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `set_extra_info`.
        
        This can be called during function execution.

        * info: The function info object.
        * returns: The extra info pointer.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_get_extra_info(info)

    @staticmethod
    fn get_bind_data(info: duckdb_function_info) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the bind data set during binding.
        
        This can be called during function execution to access data stored during binding.
        Note that the bind data is read-only.

        * info: The function info object.
        * returns: The bind data pointer.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_get_bind_data(info)

    @staticmethod
    fn set_error(info: duckdb_function_info, error: String):
        """Reports that an error has occurred during function execution.

        * info: The function info object.
        * error: The error message.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_error(info, error_copy.as_c_string_slice().unsafe_ptr().bitcast[c_char]())


struct BindInfo:
    """Provides access to binding information for scalar functions.
    
    This struct wraps the `duckdb_bind_info` type and provides methods
    for working with function binding.
    """

    var _info: duckdb_bind_info

    fn __init__(out self, info: duckdb_bind_info):
        """Creates a BindInfo wrapper.

        * info: The bind info pointer from DuckDB.
        """
        self._info = info

    fn set_bind_data(
        self, 
        bind_data: UnsafePointer[NoneType, MutAnyOrigin], 
        destroy: duckdb_delete_callback_t
    ):
        """Sets user-provided bind data.
        
        This data can be retrieved during function execution using `ScalarFunction.get_bind_data`.

        * bind_data: The bind data pointer.
        * destroy: The callback to destroy the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_bind_data(self._info, bind_data, destroy)

    fn set_bind_data_copy(self, copy: duckdb_copy_callback_t):
        """Sets the bind data copy function.
        
        This function is called to copy the bind data when needed (e.g., for parallel execution).

        * copy: The callback to copy the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_set_bind_data_copy(self._info, copy)

    fn set_error(self, error: String):
        """Reports that an error has occurred during binding.

        * error: The error message.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_scalar_function_bind_set_error(self._info, error_copy.as_c_string_slice().unsafe_ptr())

    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `ScalarFunction.set_extra_info`.

        * returns: The extra info pointer.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_bind_get_extra_info(self._info)

    fn get_argument_count(self) -> idx_t:
        """Gets the number of arguments passed to the scalar function.

        * returns: The number of arguments.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_bind_get_argument_count(self._info)

    fn get_argument(self, index: idx_t) -> duckdb_expression:
        """Gets the argument expression at the specified index.

        * index: The index of the argument.
        * returns: The expression at the specified index.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_scalar_function_bind_get_argument(self._info, index)


struct ScalarFunctionSet(Movable):
    """A set of scalar function overloads with the same name but different signatures.

    This allows registering multiple versions of a function that handle different
    parameter types or counts.
    
    Example:
    ```mojo
    from duckdb import Connection
    from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet
    from duckdb.logical_type import LogicalType
    from duckdb._libduckdb import *
    from duckdb.api import DuckDB
    
    var func_set = ScalarFunctionSet("my_func")
    ref lib = DuckDB().libduckdb()
    
    # Add overload for (FLOAT) -> FLOAT
    var func1 = ScalarFunction()
    var float_type = LogicalType(DuckDBType.float)
    func1.add_parameter(float_type)
    func1.set_return_type(float_type)
    # func1.set_function(my_float_impl)  # Provide your implementation
    func_set.add_function(func1)
    
    # Add overload for (INTEGER) -> INTEGER  
    var func2 = ScalarFunction()
    var int_type = LogicalType(DuckDBType.integer)
    func2.add_parameter(int_type)
    func2.set_return_type(int_type)
    # func2.set_function(my_int_impl)  # Provide your implementation
    func_set.add_function(func2)
    
    var conn = Connection(":memory:")
    func_set.register(conn)
    ```
    """

    var _function_set: duckdb_scalar_function_set
    var _owned: Bool  # Tracks if this struct owns the underlying pointer

    fn __init__(out self, name: String):
        """Creates a new scalar function set.

        * name: The name for all functions in this set.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        self._function_set = libduckdb.duckdb_create_scalar_function_set(name_copy.as_c_string_slice().unsafe_ptr())
        self._owned = True

    fn __moveinit__(out self, deinit take: Self):
        """Move constructor that transfers ownership."""
        self._function_set = take._function_set
        self._owned = take._owned

    fn __copyinit__(out self, copy: Self):
        """Copy constructor - shares the pointer but doesn't own it."""
        self._function_set = copy._function_set
        self._owned = False  # Copy doesn't own the resource

    fn __del__(deinit self):
        """Destroys the scalar function set and deallocates all memory."""
        if self._owned:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_destroy_scalar_function_set(UnsafePointer(to=self._function_set))

    fn add_function(self, function: ScalarFunction) raises:
        """Adds a scalar function as a new overload to the function set.

        IMPORTANT: The function must have its name set to match the function set's name
        using set_name() before being added to the set.

        The function pointer is added to the set. The caller must ensure the function
        remains alive until after the set is registered with the connection.

        * function: The function to add (passed by reference). Must have matching name.
        * raises: Error if the function could not be added (e.g., duplicate signature).
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_add_scalar_function_to_set(
            self._function_set, 
            function._function
        )
        if status != DuckDBSuccess:
            raise Error("Failed to add function to set - overload may already exist")

    fn register(mut self, conn: Connection) raises:
        """Registers the scalar function set within the given connection.

        The set requires at least one valid overload.
        This method releases ownership to transfer it to DuckDB.
        The function set will be managed by the connection and should not be destroyed manually.

        * conn: The connection to register the function set in.
        * raises: Error if the registration was unsuccessful.
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_register_scalar_function_set(conn._conn, self._function_set)
        if status != DuckDBSuccess:
            raise Error("Failed to register scalar function set")
        # Release ownership - DuckDB now manages this
        self._owned = False
