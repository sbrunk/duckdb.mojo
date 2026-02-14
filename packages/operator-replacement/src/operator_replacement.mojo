from ffi import c_char
from pathlib import Path
from ffi import _find_dylib
from ffi import _Global, OwnedDLHandle
from ffi import _get_dylib_function as _ffi_get_dylib_function

# ===-----------------------------------------------------------------------===#
# FFI definitions for the DuckDB Operator Replacement C API.
# 
# This is based on internal DuckDB APIs and is NOT part of the stable API.
# Use at your own risk - may break between DuckDB versions.
# ===-----------------------------------------------------------------------===#

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

@always_inline
fn _get_dylib_function[
    func_name: StaticString, result_type: __TypeOfAllTypes
]() raises -> result_type:
    return _ffi_get_dylib_function[
        OPERATOR_REPLACEMENT_LIBRARY(),
        func_name,
        result_type,
    ]()

struct _dylib_function[fn_name: StaticString, type: __TypeOfAllTypes](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    fn load() raises -> Self.type:
        return _get_dylib_function[Self.fn_name, Self.type]()

# ===--------------------------------------------------------------------===#
# Function bindings
# ===--------------------------------------------------------------------===#

comptime _register_function_replacement = _dylib_function["register_function_replacement",
    fn (UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType
]

comptime _register_operator_replacement = _dylib_function["register_operator_replacement",
    fn (UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType
]

# ===--------------------------------------------------------------------===#
# Public API  Wrapper
# ===--------------------------------------------------------------------===#

struct OperatorReplacementLib:
    """Wrapper for DuckDB Operator Replacement library functions."""
    
    var _register_function_replacement: _register_function_replacement.fn_type
    var _register_operator_replacement: _register_operator_replacement.fn_type
    
    fn __init__(out self) raises:
        self._register_function_replacement = _register_function_replacement.load()
        self._register_operator_replacement = _register_operator_replacement.load()
    
    fn register_function_replacement(self, var original_name: String, var replacement_name: String):
        """Register a function/operator replacement.
        
        Maps an original function/operator name to a replacement function that must
        be registered in the DuckDB catalog.
        
        Args:
            original_name: The function/operator to replace (e.g., "*", "sqrt", "+").
            replacement_name: The name of the replacement function in the catalog.
        """
        var orig = original_name.as_c_string_slice()
        var repl = replacement_name.as_c_string_slice()
        _ = self._register_function_replacement(orig.unsafe_ptr(), repl.unsafe_ptr())
    
    fn register_operator_replacement(self, connection: UnsafePointer[NoneType, MutAnyOrigin]):
        """Activate all registered function/operator replacements.
        
        This registers the optimizer extension that performs the replacements.
        Must be called after registering all replacement mappings.
        
        Args:
            connection: The DuckDB connection handle (pass conn._conn[].__conn).
        """
        _ = self._register_operator_replacement(connection)
