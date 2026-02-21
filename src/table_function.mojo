from duckdb._libduckdb import *
from duckdb.api import _get_duckdb_interface
from duckdb.logical_type import LogicalType
from duckdb.connection import Connection
from duckdb.duckdb_type import dtype_to_duckdb_type
from duckdb.value import DuckDBValue


struct TableFunctionInfo:
    """Provides access to function information during table function execution.

    This struct wraps the `duckdb_function_info` pointer passed to the table function
    main callback. It's a non-owning wrapper - the underlying pointer is managed by DuckDB.

    Example:
    ```mojo
    from duckdb import Chunk
    from duckdb.table_function import TableFunctionInfo

    fn my_function(info: TableFunctionInfo, output: Chunk):
        var bind_data = info.get_bind_data()
        var init_data = info.get_init_data()
        # ... produce rows into output chunk
    ```
    """

    var _info: duckdb_function_info

    fn __init__(out self, info: duckdb_function_info):
        """Creates a TableFunctionInfo from a duckdb_function_info pointer.

        Args:
            info: The duckdb_function_info pointer from the callback.
        """
        self._info = info

    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `TableFunction.set_extra_info()`.

        Returns:
            Pointer to the extra info data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_function_get_extra_info(self._info)

    fn get_bind_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the bind data set during the bind phase.

        Note that the bind data is read-only during execution.

        Returns:
            Pointer to the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_function_get_bind_data(self._info)

    fn get_init_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the init data set during the init phase.

        Returns:
            Pointer to the init data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_function_get_init_data(self._info)

    fn get_local_init_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the thread-local init data set during the local init phase.

        Returns:
            Pointer to the local init data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_function_get_local_init_data(self._info)

    fn set_error(self, error: String):
        """Reports an error during function execution.

        This should be called when the function encounters an error.
        After calling this, the function should return without setting output values.

        Args:
            error: The error message to report.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_function_set_error(
            self._info, error_copy.as_c_string_slice().unsafe_ptr()
        )


struct TableBindInfo:
    """Provides access to binding information for table functions.

    This struct wraps the `duckdb_bind_info` type and provides methods
    for defining the output schema and accessing parameters during the bind phase.

    Example:
    ```mojo
    from duckdb import DuckDBType
    from duckdb.table_function import TableBindInfo
    from duckdb.logical_type import LogicalType

    fn my_bind(info: TableBindInfo):
        info.add_result_column("id", LogicalType(DuckDBType.integer))
        info.add_result_column("name", LogicalType(DuckDBType.varchar))
    ```
    """

    var _info: duckdb_bind_info

    fn __init__(out self, info: duckdb_bind_info):
        """Creates a TableBindInfo wrapper.

        Args:
            info: The bind info pointer from DuckDB.
        """
        self._info = info

    fn add_result_column(self, name: String, type: LogicalType):
        """Adds a result column to the output of the table function.

        Args:
            name: The column name.
            type: The logical column type.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_bind_add_result_column(
            self._info,
            name_copy.as_c_string_slice().unsafe_ptr(),
            type._logical_type,
        )

    fn get_parameter_count(self) -> idx_t:
        """Retrieves the number of regular (non-named) parameters.

        Returns:
            The number of parameters.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_bind_get_parameter_count(self._info)

    fn get_parameter(self, index: idx_t) -> DuckDBValue:
        """Retrieves the parameter at the given index.

        Args:
            index: The index of the parameter to get.

        Returns:
            The value of the parameter.
        """
        ref libduckdb = DuckDB().libduckdb()
        return DuckDBValue(
            libduckdb.duckdb_bind_get_parameter(self._info, index)
        )

    fn get_named_parameter(self, name: String) -> DuckDBValue:
        """Retrieves a named parameter with the given name.

        Args:
            name: The name of the parameter.

        Returns:
            The value of the parameter.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        return DuckDBValue(
            libduckdb.duckdb_bind_get_named_parameter(
                self._info,
                name_copy.as_c_string_slice().unsafe_ptr(),
            )
        )

    fn set_bind_data(
        self,
        bind_data: UnsafePointer[NoneType, MutAnyOrigin],
        destroy: duckdb_delete_callback_t,
    ):
        """Sets user-provided bind data.

        This data can be retrieved during init and execution using
        `TableInitInfo.get_bind_data()` and `TableFunctionInfo.get_bind_data()`.

        Args:
            bind_data: The bind data pointer.
            destroy: The callback to destroy the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_bind_set_bind_data(self._info, bind_data, destroy)

    fn set_cardinality(self, cardinality: idx_t, is_exact: Bool):
        """Sets the cardinality estimate for the table function, used for optimization.

        Args:
            cardinality: The cardinality estimate.
            is_exact: Whether or not the cardinality estimate is exact.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_bind_set_cardinality(self._info, cardinality, is_exact)

    fn set_error(self, error: String):
        """Reports that an error has occurred during binding.

        Args:
            error: The error message.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_bind_set_error(
            self._info,
            error_copy.as_c_string_slice().unsafe_ptr(),
        )

    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `TableFunction.set_extra_info`.

        Returns:
            The extra info pointer.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_bind_get_extra_info(self._info)


struct TableInitInfo:
    """Provides access to init information for table functions.

    This struct wraps the `duckdb_init_info` type and provides methods
    for setting up per-thread state before table function execution.

    Example:
    ```mojo
    from duckdb.table_function import TableInitInfo

    fn my_init(info: TableInitInfo):
        # Optionally set init data and max threads
        info.set_max_threads(4)
    ```
    """

    var _info: duckdb_init_info

    fn __init__(out self, info: duckdb_init_info):
        """Creates a TableInitInfo wrapper.

        Args:
            info: The init info pointer from DuckDB.
        """
        self._info = info

    fn get_extra_info(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Retrieves the extra info set via `TableFunction.set_extra_info`.

        Returns:
            Pointer to the extra info data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_init_get_extra_info(self._info)

    fn get_bind_data(self) -> UnsafePointer[NoneType, MutAnyOrigin]:
        """Gets the bind data set during the bind phase.

        Returns:
            Pointer to the bind data.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_init_get_bind_data(self._info)

    fn set_init_data(
        self,
        init_data: UnsafePointer[NoneType, MutAnyOrigin],
        destroy: duckdb_delete_callback_t,
    ):
        """Sets the user-provided init data.

        This data can be retrieved during execution using
        `TableFunctionInfo.get_init_data()`.

        Args:
            init_data: The init data object.
            destroy: The callback to destroy the init data.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_init_set_init_data(self._info, init_data, destroy)

    fn get_column_count(self) -> idx_t:
        """Returns the number of projected columns.

        This function must be used if projection pushdown is enabled.

        Returns:
            The number of projected columns.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_init_get_column_count(self._info)

    fn get_column_index(self, column_index: idx_t) -> idx_t:
        """Returns the column index of the projected column at the specified position.

        This function must be used if projection pushdown is enabled.

        Args:
            column_index: The index at which to get the projected column index.

        Returns:
            The column index of the projected column.
        """
        ref libduckdb = DuckDB().libduckdb()
        return libduckdb.duckdb_init_get_column_index(self._info, column_index)

    fn set_max_threads(self, max_threads: idx_t):
        """Sets how many threads can process this table function in parallel.

        Args:
            max_threads: The maximum amount of threads.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_init_set_max_threads(self._info, max_threads)

    fn set_error(self, error: String):
        """Reports that an error has occurred during init.

        Args:
            error: The error message.
        """
        var error_copy = error.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_init_set_error(
            self._info,
            error_copy.as_c_string_slice().unsafe_ptr(),
        )


struct TableFunction(Movable):
    """A table function that can be registered in DuckDB and called in the FROM clause.

    Table functions produce rows of data. They require a bind function (to define the
    output schema), an init function (to initialize per-thread state), and a main
    function (to produce output chunks).

    The result schema is defined in the bind function by calling `add_result_column`.
    The main function is called repeatedly to produce output chunks. When done,
    set the chunk size to 0 to signal completion.

    Example:
    ```mojo
    from duckdb import Connection, DuckDBType, Chunk
    from duckdb.table_function import TableFunction, TableFunctionInfo, TableBindInfo, TableInitInfo
    from duckdb.logical_type import LogicalType

    fn my_bind(info: TableBindInfo):
        info.add_result_column("i", LogicalType(DuckDBType.integer))

    fn my_init(info: TableInitInfo):
        pass  # No per-thread state needed

    fn my_function(info: TableFunctionInfo, mut output: Chunk):
        # Produce up to VECTOR_SIZE rows
        var data = output.get_vector(0).get_data().bitcast[Int32]()
        for i in range(5):
            data[i] = Int32(i)
        output.set_size(5)

    var conn = Connection(":memory:")
    var func = TableFunction()
    func.set_name("my_table")
    func.set_bind[my_bind]()
    func.set_init[my_init]()
    func.set_function[my_function]()
    func.register(conn)

    # Use in SQL: SELECT * FROM my_table()
    ```
    """

    var _function: duckdb_table_function

    fn __init__(out self):
        """Creates a new table function.

        The function must be destroyed with `__del__` or by letting it go out of scope.
        """
        ref libduckdb = DuckDB().libduckdb()
        self._function = libduckdb.duckdb_create_table_function()

    fn __moveinit__(out self, deinit take: Self):
        """Move constructor that transfers ownership."""
        self._function = take._function

    fn __del__(deinit self):
        """Destroys the table function and deallocates all memory."""
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_table_function(
            UnsafePointer(to=self._function)
        )

    fn set_name(self, name: String):
        """Sets the name of the table function.

        Args:
            name: The name of the table function.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_name(
            self._function,
            name_copy.as_c_string_slice().unsafe_ptr(),
        )

    fn add_parameter(self, type: LogicalType):
        """Adds a parameter to the table function.

        Args:
            type: The type of the parameter to add.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_add_parameter(
            self._function, type._logical_type
        )

    fn add_named_parameter(self, name: String, type: LogicalType):
        """Adds a named parameter to the table function.

        Args:
            name: The parameter name.
            type: The type of the parameter.
        """
        var name_copy = name.copy()
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_add_named_parameter(
            self._function,
            name_copy.as_c_string_slice().unsafe_ptr(),
            type._logical_type,
        )

    fn set_extra_info(
        self,
        extra_info: UnsafePointer[NoneType, MutAnyOrigin],
        destroy: duckdb_delete_callback_t,
    ):
        """Assigns extra information to the table function.

        This information can be fetched during binding, init, and execution.

        Args:
            extra_info: The extra information pointer.
            destroy: The callback that will be called to destroy the extra information.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_extra_info(
            self._function, extra_info, destroy
        )

    fn set_bind[
        func: fn(TableBindInfo) -> None,
    ](self):
        """Sets the bind function using the high-level `TableBindInfo` type.

        The bind function is called once before execution and defines the output schema
        by adding result columns.

        Parameters:
            func: Your bind function with signature `fn(TableBindInfo) -> None`.
        """

        fn wrapper(raw_info: duckdb_bind_info):
            var info = TableBindInfo(raw_info)
            func(info)

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_bind(self._function, wrapper)

    fn set_bind_raw(self, bind: duckdb_table_function_bind_t):
        """Sets the bind function using the raw FFI callback type.

        Args:
            bind: The bind function callback.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_bind(self._function, bind)

    fn set_init[
        func: fn(TableInitInfo) -> None,
    ](self):
        """Sets the init function using the high-level `TableInitInfo` type.

        The init function is called once per thread before execution starts.

        Parameters:
            func: Your init function with signature `fn(TableInitInfo) -> None`.
        """

        fn wrapper(raw_info: duckdb_init_info):
            var info = TableInitInfo(raw_info)
            func(info)

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_init(self._function, wrapper)

    fn set_init_raw(self, init: duckdb_table_function_init_t):
        """Sets the init function using the raw FFI callback type.

        Args:
            init: The init function callback.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_init(self._function, init)

    fn set_local_init[
        func: fn(TableInitInfo) -> None,
    ](self):
        """Sets the thread-local init function using the high-level `TableInitInfo` type.

        Parameters:
            func: Your init function with signature `fn(TableInitInfo) -> None`.
        """

        fn wrapper(raw_info: duckdb_init_info):
            var info = TableInitInfo(raw_info)
            func(info)

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_local_init(self._function, wrapper)

    fn set_local_init_raw(self, init: duckdb_table_function_init_t):
        """Sets the thread-local init function using the raw FFI callback type.

        Args:
            init: The init function callback.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_local_init(self._function, init)

    fn set_function[
        func: fn(TableFunctionInfo, mut Chunk) -> None,
    ](self):
        """Sets the main execution function using high-level Mojo types.

        The function is called repeatedly to produce output chunks. When done
        producing data, set the chunk size to 0 to signal completion.

        Parameters:
            func: Your function with signature `fn(TableFunctionInfo, mut Chunk) -> None`.

        Example:
        ```mojo
        from duckdb import Chunk
        from duckdb.table_function import TableFunctionInfo, TableFunction

        fn my_function(info: TableFunctionInfo, mut output: Chunk):
            var data = output.get_vector(0).get_data().bitcast[Int32]()
            for i in range(5):
                data[i] = Int32(i)
            output.set_size(5)

        var tf = TableFunction()
        tf.set_function[my_function]()
        ```
        """

        fn wrapper(
            raw_info: duckdb_function_info, raw_output: duckdb_data_chunk
        ):
            var info = TableFunctionInfo(raw_info)
            var output_chunk = Chunk[is_owned=False](raw_output)
            func(info, output_chunk)

        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_function(self._function, wrapper)

    fn set_function_raw(self, function: duckdb_table_function_t):
        """Sets the main function using the raw FFI callback type.

        Args:
            function: The function callback.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_set_function(self._function, function)

    fn supports_projection_pushdown(self, pushdown: Bool):
        """Sets whether the table function supports projection pushdown.

        If enabled, the system will provide a list of all required columns in the init
        stage through `TableInitInfo.get_column_count()` and `TableInitInfo.get_column_index()`.

        Args:
            pushdown: True if the table function supports projection pushdown.
        """
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_table_function_supports_projection_pushdown(
            self._function, pushdown
        )

    fn register(self, conn: Connection) raises:
        """Registers the table function within the given connection.

        The function requires at least a name, a bind function, an init function,
        and a main function.

        DuckDB copies the function internally during registration, so the
        function handle remains valid and will be cleaned up normally when
        this struct goes out of scope.

        Args:
            conn: The connection to register the function in.

        Raises:
            Error if the registration was unsuccessful.
        """
        ref libduckdb = DuckDB().libduckdb()
        var status = libduckdb.duckdb_register_table_function(
            conn._conn, self._function
        )
        if status != DuckDBSuccess:
            raise Error("Failed to register table function")
