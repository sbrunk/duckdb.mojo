"""DuckDB Extension support for Mojo.

This module provides the types and utilities needed to create DuckDB extensions
in Mojo using the C Extension API.

A DuckDB extension is a shared library (.so/.dylib) that exports a C-compatible
entry point function. When loaded by DuckDB, the entry point receives access to
the DuckDB API to register new functions, types, etc.

## Quick Start

To create an extension, you need to:
1. Write your functions using the existing duckdb.mojo API (ScalarFunction, etc.)
2. Create an init function that registers them via a `Connection` to the
   extension's `Database`
3. Export the entry point using `@export`

Example:
```mojo
from duckdb.extension import Extension, duckdb_extension_access
from duckdb._libduckdb import duckdb_extension_info
from duckdb import Connection, ScalarFunction

fn add_one(x: Int64) -> Int64:
    return x + 1

fn init(conn: Connection) raises:
    ScalarFunction.from_function[
        "add_one", DType.int64, DType.int64, add_one
    ](conn)

@export("my_extension_init_c_api", ABI="C")
fn my_extension_init(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access],
) -> Bool:
    return Extension.run[init](info, access)
```

Then build with:
```sh
mojo build my_extension.mojo --emit shared-lib -o my_extension.duckdb_extension
```

And load in DuckDB:
```sql
LOAD 'my_extension.duckdb_extension';
SELECT add_one(41);  -- Returns 42
```
"""

from duckdb._libduckdb import *
from duckdb.database import Database
from duckdb.connection import Connection
from duckdb.api import DuckDB


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
# Extension
# ===--------------------------------------------------------------------===#


struct Extension(Movable):
    """Access to a DuckDB database from within an extension.

    Provides a non-owning `Database` handle and convenience `connect()` method
    for creating connections and registering functions, types, etc.

    Example:
    ```mojo
    @export("my_ext_init_c_api", ABI="C")
    fn my_ext_init(
        info: duckdb_extension_info,
        access: UnsafePointer[duckdb_extension_access],
    ) -> Bool:
        var ext = Extension(info, access)
        try:
            var conn = ext.connect()
            # Register functions via conn ...
        except:
            return False
        return True
    ```
    """

    var _info: duckdb_extension_info
    var _access: UnsafePointer[duckdb_extension_access, MutExternalOrigin]

    fn __init__(
        out self,
        info: duckdb_extension_info,
        access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
    ):
        """Create an Extension from the DuckDB-provided info and access.

        Args:
            info: The extension info handle from DuckDB.
            access: Pointer to the extension access struct from DuckDB.
        """
        self._info = info
        self._access = access

    fn database(self) -> Database:
        """Return a non-owning Database handle for this extension's database."""
        var db_ptr = self._access[].get_database(self._info)
        return Database(_handle=db_ptr[])

    fn connect(self) raises -> Connection:
        """Create a connection to the extension's database.

        Example:
        ```mojo
        var conn = ext.connect()
        ```
        """
        return Connection(self.database())

    fn set_error(self, error: String):
        """Report an error back to DuckDB."""
        var error_copy = error.copy()
        self._access[].set_error(
            self._info, error_copy.as_c_string_slice().unsafe_ptr()
        )

    @staticmethod
    fn run[
        init_fn: fn (conn: Connection) raises -> None
    ](
        info: duckdb_extension_info,
        access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
    ) -> Bool:
        """Run an extension init function with automatic error handling.

        Creates an `Extension`, connects to the database, calls `init_fn`,
        and reports any errors back to DuckDB. This eliminates the
        boilerplate of writing an extension entry point.

        Parameters:
            init_fn: A function that receives a `Connection` and registers
                extension functionality. Raise on failure.

        Args:
            info: The extension info handle from DuckDB.
            access: Pointer to the extension access struct from DuckDB.

        Returns:
            True on success, False on failure.

        Example:
        ```mojo
        fn init(conn: Connection) raises:
            ScalarFunction.from_function[
                "add_one", DType.int64, DType.int64, add_one
            ](conn)

        @export("my_ext_init_c_api", ABI="C")
        fn my_ext_init(
            info: duckdb_extension_info,
            access: UnsafePointer[duckdb_extension_access],
        ) -> Bool:
            return Extension.run[init](info, access)
        ```
        """
        var ext = Extension(info, access)
        try:
            var conn = ext.connect()
            init_fn(conn)
        except e:
            ext.set_error(String(e))
            return False
        return True
