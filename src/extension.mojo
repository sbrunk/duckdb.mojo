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
from duckdb.api import DuckDB, _set_ext_api_ptr, _set_ext_api_unstable_ptr

# ===--------------------------------------------------------------------===#
# Extension API version
# ===--------------------------------------------------------------------===#

comptime EXTENSION_API_VERSION = "v1.2.0"
"""The C Extension API version this library targets.

This corresponds to `DUCKDB_EXTENSION_API_VERSION_STRING` in duckdb_extension.h.
When calling `get_api`, pass this version to request the stable v1.2.0 API.
"""

# Default API struct for stable usage
comptime ExtApi = duckdb_ext_api_v1
"""The default (stable) extension API struct type."""

comptime ExtApiUnstable = duckdb_ext_api_v1_unstable
"""The full extension API struct type, including unstable functions."""


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

    fn get_api(
        self, version: String = EXTENSION_API_VERSION
    ) -> UnsafePointer[NoneType, ImmutExternalOrigin]:
        """Request the DuckDB C API function pointer struct (untyped).

        Returns an opaque pointer that can be bitcast to the appropriate
        ``duckdb_ext_api_v1`` or ``duckdb_ext_api_v1_unstable`` struct.

        Args:
            version: The semver API version string to request (e.g. "v1.2.0").
                Defaults to `EXTENSION_API_VERSION`.

        Returns:
            An opaque pointer to the API struct, or null if unsupported.
        """
        var version_copy = version.copy()
        return self._access[].get_api(
            self._info, version_copy.as_c_string_slice().unsafe_ptr()
        )

    fn get_api_typed[
        ApiStruct: AnyType = ExtApi
    ](
        self, version: String = EXTENSION_API_VERSION
    ) -> UnsafePointer[ApiStruct, ImmutExternalOrigin]:
        """Request the DuckDB C API as a typed struct pointer.

        Returns a pointer to the API struct with the expected struct layout.
        Use ``ExtApi`` (default) for the stable API, or ``ExtApiUnstable``
        for the full API including unstable functions.

        Parameters:
            ApiStruct: The struct type to cast to. Defaults to ``ExtApi``
                (same as ``duckdb_ext_api_v1``).

        Args:
            version: The semver API version string to request.

        Returns:
            A typed pointer to the API struct, or null if unsupported.

        Example:
        ```mojo
        var api = ext.get_api_typed[ExtApi]()
        if not api:
            ext.set_error("Unsupported API version")
            return False
        # Access function pointers via api[].duckdb_open(...)
        ```
        """
        var raw = self.get_api(version)
        return raw.bitcast[ApiStruct]()

    @staticmethod
    fn run[
        init_fn: fn (conn: Connection) raises -> None,
        unstable: Bool = False,
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
            unstable: If True, request the unstable API (all functions
                available). If False (default), request only the stable API.

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

        # Seed the global so DuckDB() constructs LibDuckDB from the API
        # struct instead of dlopen/dlsym. Must happen before any DuckDB()
        # usage (e.g. ext.connect() -> Connection -> DuckDB().libduckdb()).
        @parameter
        if unstable:
            var api_ptr = ext.get_api_typed[ExtApiUnstable]()
            if not api_ptr:
                ext.set_error(
                    "Incompatible DuckDB C API version (requested "
                    + EXTENSION_API_VERSION
                    + ")"
                )
                return False
            _set_ext_api_unstable_ptr(api_ptr)
        else:
            var api_ptr = ext.get_api_typed[ExtApi]()
            if not api_ptr:
                ext.set_error(
                    "Incompatible DuckDB C API version (requested "
                    + EXTENSION_API_VERSION
                    + ")"
                )
                return False
            _set_ext_api_ptr(api_ptr)

        try:
            var conn = ext.connect()
            init_fn(conn)
        except e:
            ext.set_error(String(e))
            return False
        return True
