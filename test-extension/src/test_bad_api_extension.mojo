"""Minimal extension that requests an invalid C API version.

Used by the test harness to verify that get_api returns null (and DuckDB
reports a load error) when the requested version is unsupported.
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension, EXTENSION_API_VERSION
from duckdb.api_level import ApiLevel
from duckdb.connection import Connection


fn get_invalid_version() -> String:
    """Return the invalid version string to test.
    
    Using a function to construct the string helps avoid PIC issues
    with string literals in minimal files on Linux.
    """
    return "v9999.0.0"


fn get_error_message() -> String:
    """Construct the error message dynamically.
    
    Using a function to build the string helps avoid PIC issues
    with string literals in minimal files on Linux.
    """
    var base = String("get_api correctly returned null for ")
    return base + get_invalid_version()


@export("bad_api_init_c_api", ABI="C")
fn bad_api_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    """Entry point that deliberately requests an unsupported API version."""
    var ext = Extension(info, access)
    var api = ext.get_api(get_invalid_version())
    if not api:
        ext.set_error(get_error_message())
        return False
    # Should never reach here — if it does, something is wrong.
    return True
