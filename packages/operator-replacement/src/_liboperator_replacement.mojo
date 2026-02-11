from ffi import external_call, c_char
from pathlib import Path
from ffi import _find_dylib
from ffi import _Global, OwnedDLHandle

# ===-----------------------------------------------------------------------===#
# FFI definitions for the DuckDB Operator Replacement C API.
# 
# This is based on internal DuckDB APIs and is NOT part of the stable API.
# Use at your own risk - may break between DuckDB versions.
# ===-----------------------------------------------------------------------===#

# Opaque type from duckdb.h
comptime duckdb_connection = UnsafePointer[NoneType]

# ===--------------------------------------------------------------------===#
# Dynamic library loading
# ===--------------------------------------------------------------------===#

comptime OPERATOR_REPLACEMENT_PATHS: List[Path] = [
    "libduckdb_operator_replacement.so",
    "libduckdb_operator_replacement.dylib",
]

comptime OPERATOR_REPLACEMENT_LIBRARY = _Global["OPERATOR_REPLACEMENT_LIBRARY", _init_dylib]

fn _init_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb_operator_replacement"](materialize[OPERATOR_REPLACEMENT_PATHS]())

# ===--------------------------------------------------------------------===#
# Public API
# ===--------------------------------------------------------------------===#

fn register_function_replacement(var original_name: String, var replacement_name: String) raises:
    """Register a function/operator replacement.
    
    Maps an original function/operator name to a replacement function that must
    be registered in the DuckDB catalog.
    
    Args:
        original_name: The function/operator to replace (e.g., "*", "sqrt", "+").
        replacement_name: The name of the replacement function in the catalog.
    """
    var orig = original_name.as_c_string_slice()
    var repl = replacement_name.as_c_string_slice()
    _ = external_call[
        "register_function_replacement",
        NoneType,
    ](OPERATOR_REPLACEMENT_LIBRARY(), orig.unsafe_ptr(), repl.unsafe_ptr())

fn register_operator_replacement(connection: UnsafePointer[NoneType]) raises:
    """Activate all registered function/operator replacements.
    
    This registers the optimizer extension that performs the replacements.
    Must be called after registering all replacement mappings.
    
    Args:
        connection: The DuckDB connection handle.
    """
    _ = external_call[
        "register_operator_replacement",
        NoneType,
    ](OPERATOR_REPLACEMENT_LIBRARY(), connection)
