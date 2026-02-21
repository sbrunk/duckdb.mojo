"""Minimal extension that requests an invalid C API version.

Used by the test harness to verify that get_api returns null (and DuckDB
reports a load error) when the requested version is unsupported.
"""

from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension


@export("bad_api_init_c_api", ABI="C")
fn bad_api_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    """Entry point that deliberately requests an unsupported API version."""
    var ext = Extension(info, access)
    var api = ext.get_api("v9999.0.0")
    if not api:
        ext.set_error("get_api correctly returned null for v9999.0.0")
        return False
    # Should never reach here — if it does, something is wrong.
    return True
