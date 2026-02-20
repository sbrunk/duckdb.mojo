"""DuckDB Extension support for Mojo.

This module provides the types and utilities needed to create DuckDB extensions
in Mojo using the C Extension API.

A DuckDB extension is a shared library (.so/.dylib) that exports a C-compatible
entry point function. When loaded by DuckDB, the entry point receives access to
the DuckDB API to register new functions, types, etc.

## Quick Start

To create an extension, you need to:
1. Write your functions using the existing duckdb.mojo API (ScalarFunction, etc.)
2. Create an init function that registers them via `ExtensionConnection`
3. Export the entry point using `@export`

Example:
```mojo
from duckdb.extension import (
    duckdb_extension_access,
    ExtensionConnection,
)
from duckdb._libduckdb import duckdb_extension_info, DuckDBError
from duckdb import ScalarFunction, DuckDBType
from duckdb.scalar_function import FunctionInfo
from duckdb.chunk import Chunk
from duckdb.vector import Vector
from duckdb.logical_type import LogicalType

fn add_numbers(info: FunctionInfo, mut input: Chunk, output: Vector):
    var size = len(input)
    var a = input.get_vector(0).get_data().bitcast[Int64]()
    var b = input.get_vector(1).get_data().bitcast[Int64]()
    var result = output.get_data().bitcast[Int64]()
    for i in range(size):
        result[i] = a[i] + b[i]

@export("my_extension_init_c_api", ABI="C")
fn my_extension_init(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access],
) -> Bool:
    var ext_conn = ExtensionConnection(info, access)
    if not ext_conn:
        return False
    var func = ScalarFunction()
    func.set_name("add_numbers")
    func.add_parameter(LogicalType(DuckDBType.bigint))
    func.add_parameter(LogicalType(DuckDBType.bigint))
    func.set_return_type(LogicalType(DuckDBType.bigint))
    func.set_function[add_numbers]()
    ext_conn.register(func)
    return True
```

Then build with:
```sh
mojo build my_extension.mojo --emit shared-lib -o my_extension.duckdb_extension
```

And load in DuckDB:
```sql
LOAD 'my_extension.duckdb_extension';
SELECT add_numbers(1, 2);
```
"""

from duckdb._libduckdb import *
from duckdb.api import DuckDB
from duckdb.scalar_function import ScalarFunction, ScalarFunctionSet
from duckdb.aggregate_function import AggregateFunction, AggregateFunctionSet
from duckdb.table_function import TableFunction


# ===--------------------------------------------------------------------===#
# Extension C API types
# ===--------------------------------------------------------------------===#

@fieldwise_init
struct duckdb_extension_access(ImplicitlyCopyable, Movable):
    """The C struct passed to extension entry points by DuckDB.

    Contains function pointers for:
    - `set_error`: Report an error during extension initialization.
    - `get_database`: Get the database handle to register functions with.
    - `get_api`: Get the versioned C API function pointer struct.
    """

    var set_error: fn (
        duckdb_extension_info, UnsafePointer[c_char, ImmutAnyOrigin]
    ) -> NoneType
    var get_database: fn (
        duckdb_extension_info,
    ) -> UnsafePointer[duckdb_database, MutExternalOrigin]
    var get_api: fn (
        duckdb_extension_info, UnsafePointer[c_char, ImmutAnyOrigin]
    ) -> UnsafePointer[NoneType, ImmutExternalOrigin]


# ===--------------------------------------------------------------------===#
# ExtensionConnection
# ===--------------------------------------------------------------------===#


struct ExtensionConnection(Movable, Boolable):
    """A connection to a DuckDB database obtained from within an extension.

    Unlike `Connection`, this does not own the database. The connection is
    created during extension initialization and automatically closed when
    this object goes out of scope.

    Use this to register functions, types, and other extension functionality.

    Example:
    ```mojo
    @export("my_ext_init_c_api", ABI="C")
    fn my_ext_init(
        info: duckdb_extension_info,
        access: UnsafePointer[duckdb_extension_access],
    ) -> Bool:
        var ext_conn = ExtensionConnection(info, access)
        if not ext_conn:
            return False
        # Register functions using ext_conn
        return True
    ```
    """

    var _conn: duckdb_connection
    var _valid: Bool
    var _info: duckdb_extension_info
    var _access: UnsafePointer[duckdb_extension_access, MutExternalOrigin]

    fn __init__(
        out self,
        info: duckdb_extension_info,
        access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
    ):
        """Create an extension connection from the DuckDB-provided info and access.

        Gets the database handle from DuckDB and opens a connection to it.
        If the connection fails, the object will be invalid (bool(self) == False).

        Args:
            info: The extension info handle from DuckDB.
            access: Pointer to the extension access struct from DuckDB.
        """
        self._info = info
        self._access = access
        self._valid = False

        # Get the database handle from DuckDB
        var db_ptr = access[].get_database(info)
        var db = db_ptr[]

        # Open a connection
        ref libduckdb = DuckDB().libduckdb()
        self._conn = UnsafePointer[duckdb_connection.type, MutExternalOrigin]()
        if (
            libduckdb.duckdb_connect(db, UnsafePointer(to=self._conn))
        ) == DuckDBError:
            self._set_error("Failed to open connection to database")
            return
        self._valid = True

    fn __moveinit__(out self, deinit take: Self):
        self._conn = take._conn
        self._valid = take._valid
        self._info = take._info
        self._access = take._access

    fn __del__(deinit self):
        """Close the connection if valid."""
        if self._valid:
            ref libduckdb = DuckDB().libduckdb()
            libduckdb.duckdb_disconnect(UnsafePointer(to=self._conn))

    fn __bool__(self) -> Bool:
        """Check if the connection is valid."""
        return self._valid

    fn _set_error(self, error: String):
        """Report an error back to DuckDB."""
        var error_copy = error.copy()
        self._access[].set_error(
            self._info, error_copy.as_c_string_slice().unsafe_ptr()
        )

    fn register(self, mut func: ScalarFunction):
        """Register a scalar function with DuckDB.

        Args:
            func: The scalar function to register. Ownership is transferred to DuckDB.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_scalar_function(
            self._conn, func._function
        )
        func._owned = False

    fn register(self, mut func_set: ScalarFunctionSet):
        """Register a scalar function set with DuckDB.

        Args:
            func_set: The scalar function set to register.
                Ownership is transferred to DuckDB.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_scalar_function_set(
            self._conn, func_set._function_set
        )
        func_set._owned = False

    fn register(self, mut func: AggregateFunction):
        """Register an aggregate function with DuckDB.

        Args:
            func: The aggregate function to register.
                Ownership is transferred to DuckDB.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_aggregate_function(
            self._conn, func._function
        )
        func._owned = False

    fn register(self, mut func_set: AggregateFunctionSet):
        """Register an aggregate function set with DuckDB.

        Args:
            func_set: The aggregate function set to register.
                Ownership is transferred to DuckDB.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_aggregate_function_set(
            self._conn, func_set._function_set
        )
        func_set._owned = False

    fn register(self, mut func: TableFunction):
        """Register a table function with DuckDB.

        Args:
            func: The table function to register.
                Ownership is transferred to DuckDB.
        """
        ref libduckdb = DuckDB().libduckdb()
        _ = libduckdb.duckdb_register_table_function(
            self._conn, func._function
        )
        func._owned = False
