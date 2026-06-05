from std.ffi import c_char
from std.pathlib import Path
from std.ffi import _find_dylib, _Global, OwnedDLHandle

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

def _init_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb_operator_replacement"](materialize[OPERATOR_REPLACEMENT_PATHS]())

struct _dylib_function[fn_name: StaticString, type: TrivialRegisterPassable](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    def load() raises -> Self.type:
        return OPERATOR_REPLACEMENT_LIBRARY.get_or_create_ptr()[]
            .borrow()._get_function[Self.fn_name, Self.type]()

# ===--------------------------------------------------------------------===#
# Function bindings
# ===--------------------------------------------------------------------===#

comptime _register_function_replacement = _dylib_function["register_function_replacement",
    def(UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[c_char, ImmutAnyOrigin]) thin abi("C") -> NoneType
]

comptime _register_operator_replacement = _dylib_function["register_operator_replacement",
    def(UnsafePointer[NoneType, MutAnyOrigin]) thin abi("C") -> NoneType
]

# ===--------------------------------------------------------------------===#
# Public API  Wrapper
# ===--------------------------------------------------------------------===#

struct OperatorReplacementLib:
    """Wrapper for DuckDB Operator Replacement library functions."""
    
    var _register_function_replacement: _register_function_replacement.fn_type
    var _register_operator_replacement: _register_operator_replacement.fn_type
    
    def __init__(out self) raises:
        self._register_function_replacement = _register_function_replacement.load()
        self._register_operator_replacement = _register_operator_replacement.load()
    
    def register_function_replacement(self, var original_name: String, var replacement_name: String):
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
    
    def register_operator_replacement(self, connection: UnsafePointer[NoneType, MutAnyOrigin]):
        """Activate all registered function/operator replacements.
        
        This registers the optimizer extension that performs the replacements.
        Must be called after registering all replacement mappings.
        
        Args:
            connection: The DuckDB connection handle (pass conn._conn[].__conn).
        """
        _ = self._register_operator_replacement(connection)
