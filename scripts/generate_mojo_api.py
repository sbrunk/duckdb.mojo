#!/usr/bin/env python3
"""Generate Mojo bindings for the DuckDB C API.

This script generates libduckdb.mojo from the same JSON definition files used
for duckdb.h header generation in the DuckDB codebase.

Usage:
    python scripts/generate_mojo_api.py [--duckdb-dir <path>]

The --duckdb-dir flag defaults to 'duckdb' (the submodule in this repo).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths relative to the DuckDB source tree
# ---------------------------------------------------------------------------
CAPI_FUNCTION_DEFINITION_FILES = "src/include/duckdb/main/capi/header_generation/functions/**/*.json"
BASE_HEADER_TEMPLATE = "src/include/duckdb/main/capi/header_generation/header_base.hpp.template"

# Output file (relative to the workspace root)
OUTPUT_FILE = "src/libduckdb.mojo"

# Groups in the order they appear in duckdb.h – maintained for easy diffing.
ORIGINAL_FUNCTION_GROUP_ORDER = [
    "open_connect",
    "configuration",
    "error_data",
    "query_execution",
    "safe_fetch_functions",
    "helpers",
    "date_time_timestamp_helpers",
    "hugeint_and_uhugeint_helpers",
    "decimal_helpers",
    "prepared_statements",
    "bind_values_to_prepared_statements",
    "execute_prepared_statements",
    "extract_statements",
    "pending_result_interface",
    "value_interface",
    "logical_type_interface",
    "data_chunk_interface",
    "vector_interface",
    "validity_mask_functions",
    "scalar_functions",
    "selection_vector_interface",
    "aggregate_functions",
    "table_functions",
    "table_function_bind",
    "table_function_init",
    "table_function",
    "replacement_scans",
    "profiling_info",
    "appender",
    "table_description",
    "arrow_interface",
    "threading_information",
    "streaming_result_interface",
    "cast_functions",
    "expression_interface",
    "file_system_interface",
    "config_options_interface",
    "copy_functions",
    "catalog_interface",
    "logging",
]

# ---------------------------------------------------------------------------
# C-type  ->  Mojo-type  mappings for parameters and return values.
#
# Order matters: longest / most specific patterns first.
# ---------------------------------------------------------------------------

# These are "opaque pointer" types – in the C API they are
#   typedef struct _duckdb_xxx { void *internal_ptr; } *duckdb_xxx;
# In Mojo we model them as  UnsafePointer[_duckdb_xxx, MutExternalOrigin].
OPAQUE_HANDLE_TYPES: set[str] = {
    "duckdb_database",
    "duckdb_connection",
    "duckdb_prepared_statement",
    "duckdb_extracted_statements",
    "duckdb_pending_result",
    "duckdb_appender",
    "duckdb_config",
    "duckdb_logical_type",
    "duckdb_create_type_info",
    "duckdb_data_chunk",
    "duckdb_vector",
    "duckdb_value",
    "duckdb_profiling_info",
    "duckdb_table_function",
    "duckdb_scalar_function",
    "duckdb_scalar_function_set",
    "duckdb_aggregate_function",
    "duckdb_aggregate_function_set",
    "duckdb_aggregate_state",
    "duckdb_function_info",
    "duckdb_bind_info",
    "duckdb_init_info",
    "duckdb_expression",
    "duckdb_selection_vector",
    "duckdb_replacement_scan_info",
    "duckdb_task_state",
    "duckdb_arrow",
    "duckdb_arrow_stream",
    "duckdb_arrow_schema",
    "duckdb_arrow_array",
    "duckdb_arrow_converted_schema",
    "duckdb_arrow_options",
    "duckdb_instance_cache",
    "duckdb_client_context",
    "duckdb_table_description",
    "duckdb_error_data",
    "duckdb_extension_info",
    "duckdb_cast_function",
    "duckdb_copy_function",
    "duckdb_copy_function_bind_info",
    "duckdb_copy_function_global_init_info",
    "duckdb_copy_function_sink_info",
    "duckdb_copy_function_finalize_info",
    "duckdb_file_open_options",
    "duckdb_file_system",
    "duckdb_file_handle",
    "duckdb_catalog",
    "duckdb_catalog_entry",
    "duckdb_config_option",
    "duckdb_log_storage",
}

# Function-pointer / callback typedefs defined in the header template.
# We map these to their Mojo `fn(...)` equivalents.
CALLBACK_TYPES: dict[str, str] = {
    "duckdb_delete_callback_t": "fn (UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType",
    "duckdb_copy_callback_t": "fn (UnsafePointer[NoneType, MutAnyOrigin]) -> UnsafePointer[NoneType, MutAnyOrigin]",
    "duckdb_scalar_function_bind_t": "fn (duckdb_bind_info) -> NoneType",
    "duckdb_scalar_function_init_t": "fn (duckdb_init_info) -> NoneType",
    "duckdb_scalar_function_t": "fn (duckdb_function_info, duckdb_data_chunk, duckdb_vector) -> NoneType",
    "duckdb_table_function_bind_t": "fn (duckdb_bind_info) -> NoneType",
    "duckdb_table_function_init_t": "fn (duckdb_init_info) -> NoneType",
    "duckdb_table_function_t": "fn (duckdb_function_info, duckdb_data_chunk) -> NoneType",
    "duckdb_aggregate_state_size": "fn (duckdb_function_info) -> idx_t",
    "duckdb_aggregate_init_t": "fn (duckdb_function_info, duckdb_aggregate_state) -> NoneType",
    "duckdb_aggregate_destroy_t": "fn (UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], idx_t) -> NoneType",
    "duckdb_aggregate_update_t": "fn (duckdb_function_info, duckdb_data_chunk, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin]) -> NoneType",
    "duckdb_aggregate_combine_t": "fn (duckdb_function_info, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], idx_t) -> NoneType",
    "duckdb_aggregate_finalize_t": "fn (duckdb_function_info, UnsafePointer[duckdb_aggregate_state, MutExternalOrigin], duckdb_vector, idx_t, idx_t) -> NoneType",
    "duckdb_replacement_callback_t": "fn (duckdb_replacement_scan_info, UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType",
    "duckdb_cast_function_t": "fn (duckdb_function_info, idx_t, duckdb_vector, duckdb_vector) -> Bool",
    "duckdb_copy_function_bind_t": "fn (duckdb_copy_function_bind_info) -> NoneType",
    "duckdb_copy_function_global_init_t": "fn (duckdb_copy_function_global_init_info) -> NoneType",
    "duckdb_copy_function_sink_t": "fn (duckdb_copy_function_sink_info, duckdb_data_chunk) -> NoneType",
    "duckdb_copy_function_finalize_t": "fn (duckdb_copy_function_finalize_info) -> NoneType",
    "duckdb_logger_write_log_entry_t": "fn (UnsafePointer[NoneType, MutAnyOrigin], UnsafePointer[duckdb_timestamp, MutAnyOrigin], UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[c_char, ImmutAnyOrigin], UnsafePointer[c_char, ImmutAnyOrigin]) -> NoneType",
}

# Scalar / small struct C-types whose Mojo equivalents differ.
SIMPLE_TYPE_MAP: dict[str, str] = {
    "void": "NoneType",
    "bool": "Bool",
    "int8_t": "Int8",
    "int16_t": "Int16",
    "int32_t": "Int32",
    "int64_t": "Int64",
    "uint8_t": "UInt8",
    "uint16_t": "UInt16",
    "uint32_t": "UInt32",
    "uint64_t": "UInt64",
    "idx_t": "idx_t",
    "sel_t": "UInt32",
    "size_t": "UInt",
    "float": "Float32",
    "double": "Float64",
    "char": "c_char",
    # DuckDB enum types (stored as Int32) 
    "duckdb_type": "duckdb_type",
    "DUCKDB_TYPE": "duckdb_type",
    "duckdb_state": "duckdb_state",
    "duckdb_pending_state": "duckdb_pending_state",
    "duckdb_result_type": "duckdb_result_type",
    "duckdb_statement_type": "duckdb_statement_type",
    "duckdb_error_type": "duckdb_error_type",
    "duckdb_cast_mode": "duckdb_cast_mode",
    "duckdb_file_flag": "duckdb_file_flag",
    "duckdb_config_option_scope": "duckdb_config_option_scope",
    "duckdb_catalog_entry_type": "duckdb_catalog_entry_type",
    # Small value structs passed by value (already Mojo types)
    "duckdb_date": "duckdb_date",
    "duckdb_date_struct": "duckdb_date_struct",
    "duckdb_time": "duckdb_time",
    "duckdb_time_struct": "duckdb_time_struct",
    "duckdb_time_ns": "duckdb_time_ns",
    "duckdb_time_tz": "duckdb_time_tz",
    "duckdb_time_tz_struct": "duckdb_time_tz_struct",
    "duckdb_timestamp": "duckdb_timestamp",
    "duckdb_timestamp_struct": "duckdb_timestamp_struct",
    "duckdb_timestamp_s": "duckdb_timestamp_s",
    "duckdb_timestamp_ms": "duckdb_timestamp_ms",
    "duckdb_timestamp_ns": "duckdb_timestamp_ns",
    "duckdb_interval": "duckdb_interval",
    "duckdb_hugeint": "duckdb_hugeint",
    "duckdb_uhugeint": "duckdb_uhugeint",
    "duckdb_decimal": "duckdb_decimal",
    "duckdb_query_progress_type": "duckdb_query_progress_type",
    "duckdb_string_t": "duckdb_string_t",
    "duckdb_list_entry": "duckdb_list_entry",
    "duckdb_blob": "duckdb_blob",
    "duckdb_bit": "duckdb_bit",
    "duckdb_bignum": "duckdb_bignum",
    "duckdb_string": "duckdb_string",
    "duckdb_result": "duckdb_result",
}

# Large structs that have ABI issues when passed by value.
# When a function takes these by value, we need helper wrappers.
LARGE_BYVAL_STRUCTS = {"duckdb_result"}

# Mojo reserved keywords that cannot be used as parameter names.
MOJO_RESERVED_PARAM_NAMES = {
    "type", "set", "input", "function", "init", "out", "ref", "mut",
    "owned", "borrowed", "inout", "self", "Self", "fn", "var", "let",
    "struct", "trait", "alias", "comptime", "raises", "async",
}


def _strip(s: str) -> str:
    """Strip whitespace but also remove trailing `*` qualification markers."""
    return s.strip()


# ---------------------------------------------------------------------------
# C-type → Mojo-type conversion
# ---------------------------------------------------------------------------

def c_type_to_mojo(c_type: str, *, is_return: bool = False) -> str:
    """Convert a C type string to the corresponding Mojo type string.

    Handles pointers, const qualifiers, opaque handles, callbacks, and
    simple scalar types.
    """
    t = c_type.strip()

    # ---- callback / function-pointer types --------------------------------
    if t in CALLBACK_TYPES:
        return CALLBACK_TYPES[t]

    # ---- struct pointer types (ArrowSchema, ArrowArray) ------
    if t == "struct ArrowSchema *":
        return "UnsafePointer[NoneType, MutAnyOrigin]"
    if t == "struct ArrowArray *":
        return "UnsafePointer[NoneType, MutAnyOrigin]"

    # ---- pointer-to-pointer:  char ** → UnsafePointer[UnsafePointer[c_char, …], …]
    if t in ("char **",):
        return "UnsafePointer[UnsafePointer[c_char, MutAnyOrigin], MutAnyOrigin]"

    # ---- const pointer patterns -------------------------------------------
    # const char *
    if t == "const char *":
        return "UnsafePointer[c_char, ImmutAnyOrigin]"
    # const char **
    if t == "const char **":
        return "UnsafePointer[UnsafePointer[c_char, ImmutAnyOrigin], MutAnyOrigin]"
    # const uint8_t *
    if t == "const uint8_t *":
        return "UnsafePointer[UInt8, ImmutAnyOrigin]"

    # ---- opaque handle pointers  e.g. "duckdb_database *" -----------------
    for h in OPAQUE_HANDLE_TYPES:
        if t == f"{h} *":
            return f"UnsafePointer[{h}, ImmutAnyOrigin]"

    # ---- misc pointer patterns -------------------------------------------
    # Plain void *
    if t == "void *":
        if is_return:
            return "UnsafePointer[NoneType, MutExternalOrigin]"
        return "UnsafePointer[NoneType, MutAnyOrigin]"

    # uint64_t *  (validity masks)
    if t == "uint64_t *":
        return "UnsafePointer[UInt64, MutExternalOrigin]"

    # bool *  (deprecated nullmask)
    if t == "bool *":
        return "UnsafePointer[Bool, MutExternalOrigin]"

    # duckdb_logical_type *  (arrays of logical types)
    if t == "duckdb_logical_type *":
        return "UnsafePointer[duckdb_logical_type, ImmutAnyOrigin]"

    # duckdb_value * (arrays of values)
    if t == "duckdb_value *":
        return "UnsafePointer[duckdb_value, MutAnyOrigin]"

    # duckdb_result *
    if t == "duckdb_result *":
        return "UnsafePointer[duckdb_result, ImmutAnyOrigin]"

    # duckdb_string_t *
    if t == "duckdb_string_t *":
        return "UnsafePointer[duckdb_string_t, MutAnyOrigin]"

    # char *  (return type = mutable external origin)
    if t == "char *":
        if is_return:
            return "UnsafePointer[c_char, MutExternalOrigin]"
        return "UnsafePointer[c_char, MutAnyOrigin]"

    # ---- generic pointer fallback -----------------------------------------
    m = re.match(r"^(const\s+)?(\w+)\s*\*$", t)
    if m:
        base = m.group(2)
        mojo_base = SIMPLE_TYPE_MAP.get(base, base)
        if m.group(1):  # const
            return f"UnsafePointer[{mojo_base}, ImmutAnyOrigin]"
        if is_return:
            return f"UnsafePointer[{mojo_base}, MutExternalOrigin]"
        return f"UnsafePointer[{mojo_base}, MutAnyOrigin]"

    # ---- opaque handles passed by value -----------------------------------
    if t in OPAQUE_HANDLE_TYPES:
        return t

    # ---- simple / scalar types --------------------------------------------
    if t in SIMPLE_TYPE_MAP:
        return SIMPLE_TYPE_MAP[t]

    # ---- struct ArrowArray / ArrowSchema (forward decls) ------------------
    if t.startswith("struct "):
        return t  # leave as-is; rarely used directly

    print(f"WARNING: unknown C type '{t}' — passing through as-is", file=sys.stderr)
    return t


# ---------------------------------------------------------------------------
# Parsing JSON definitions (same logic as generate_c_api.py)
# ---------------------------------------------------------------------------

def parse_capi_function_definitions(duckdb_dir: str):
    """Parse all function group JSON files and return (groups_ordered, function_map)."""
    pattern = os.path.join(duckdb_dir, CAPI_FUNCTION_DEFINITION_FILES)
    function_files = sorted(glob.glob(pattern, recursive=True))

    function_groups: list[dict] = []
    function_map: dict[str, dict] = {}

    for fpath in function_files:
        with open(fpath, "r") as f:
            data = json.load(f)
        function_groups.append(data)
        for entry in data.get("entries", []):
            function_map[entry["name"]] = entry

    # Re-order to match ORIGINAL_FUNCTION_GROUP_ORDER
    groups_by_name = {g["group"]: g for g in function_groups}
    ordered: list[dict] = []
    for name in ORIGINAL_FUNCTION_GROUP_ORDER:
        if name in groups_by_name:
            ordered.append(groups_by_name[name])
    # Append any groups not in the original order
    seen = set(ORIGINAL_FUNCTION_GROUP_ORDER)
    for g in function_groups:
        if g["group"] not in seen:
            ordered.append(g)

    return ordered, function_map


# ---------------------------------------------------------------------------
# Deprecation helpers
# ---------------------------------------------------------------------------

def is_group_deprecated(group: dict) -> bool:
    return group.get("deprecated", False)


def is_function_deprecated(entry: dict) -> bool:
    comment = entry.get("comment", {})
    if comment.get("deprecated", False):
        return True
    desc = comment.get("description", "")
    if "DEPRECATION NOTICE" in desc or "scheduled for removal" in desc:
        return True
    return False


# ---------------------------------------------------------------------------
# Identify functions that need helper wrappers (large struct by-val)
# ---------------------------------------------------------------------------

def needs_byval_helper(entry: dict) -> bool:
    """Return True if a function passes a large struct by value."""
    for param in entry.get("params", []):
        ptype = param["type"].strip()
        if ptype in LARGE_BYVAL_STRUCTS:
            return True
    return False


def takes_result_byval(entry: dict) -> bool:
    """Whether the function takes duckdb_result by value (not pointer)."""
    for param in entry.get("params", []):
        ptype = param["type"].strip()
        if ptype == "duckdb_result":
            return True
    return False


# ---------------------------------------------------------------------------
# Code generation helpers
# ---------------------------------------------------------------------------

def mojo_fn_type(entry: dict, *, use_ptr_helper: bool = False) -> str:
    """Build the Mojo fn(...) -> ... type for a comptime _dylib_function."""
    params = entry.get("params", [])
    ret = entry["return_type"]

    mojo_params: list[str] = []
    for p in params:
        ptype = p["type"].strip()
        if use_ptr_helper and ptype == "duckdb_result":
            mojo_params.append("UnsafePointer[duckdb_result, ImmutAnyOrigin]")
        else:
            mojo_params.append(c_type_to_mojo(ptype))
    
    mojo_ret = c_type_to_mojo(ret, is_return=True)

    if mojo_params:
        return f"fn ({', '.join(mojo_params)}) -> {mojo_ret}"
    return f"fn () -> {mojo_ret}"


def mojo_method_signature(entry: dict, *, use_ptr_helper: bool = False) -> str:
    """Build the fn method_name(self, <params>) -> <ret>: line."""
    name = entry["name"]
    params = entry.get("params", [])
    ret = entry["return_type"]

    parts = [f"    fn {name}(\n        self"]
    for p in params:
        ptype = p["type"].strip()
        pname = p["name"]
        # Avoid Mojo keyword collisions
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        if use_ptr_helper and ptype == "duckdb_result":
            parts.append(f",\n        {pname}: UnsafePointer[duckdb_result, ImmutAnyOrigin]")
        else:
            mojo_type = c_type_to_mojo(ptype)
            parts.append(f",\n        {pname}: {mojo_type}")

    mojo_ret = c_type_to_mojo(ret, is_return=True)
    sig = "".join(parts) + f",\n    ) -> {mojo_ret}:"
    return sig


def mojo_method_body(entry: dict, *, use_ptr_helper: bool = False) -> str:
    """Build the body of a LibDuckDB method: docstring + return self._name(args)."""
    name = entry["name"]
    params = entry.get("params", [])
    comment = entry.get("comment", {})

    lines: list[str] = []

    # Docstring
    desc = comment.get("description", "").strip()
    if desc:
        lines.append('        """')
        for line in desc.split("\n"):
            lines.append(f"        {line.rstrip()}")
        lines.append('        """')

    # Call
    args: list[str] = []
    for p in params:
        pname = p["name"]
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        ptype = p["type"].strip()
        if use_ptr_helper and ptype == "duckdb_result":
            # The helper wrapper takes pointer
            args.append(pname)
        else:
            args.append(pname)
    
    call = f"        return self._{name}({', '.join(args)})"
    lines.append(call)

    return "\n".join(lines)


def format_comment_header(title: str) -> str:
    return f"""
    # ===--------------------------------------------------------------------===#
    # {title}
    # ===--------------------------------------------------------------------===#
"""


def format_section_header(title: str) -> str:
    return f"""
# ===--------------------------------------------------------------------===#
# {title}
# ===--------------------------------------------------------------------===#
"""


def headline_capitalize(s: str) -> str:
    """Convert snake_case group name to Title Case."""
    return " ".join(w.capitalize() for w in s.split("_"))


# ---------------------------------------------------------------------------
# Which functions does the existing _libduckdb.mojo use helper wrappers for?
# We need to know this so we can generate compatible code.
# ---------------------------------------------------------------------------

# Functions that take duckdb_result by value and need a ptr helper wrapper
# (loaded from the helper library via _dylib_helpers_function).
HELPER_WRAPPER_FUNCTIONS = {
    "duckdb_result_statement_type": "duckdb_result_statement_type_ptr",
    "duckdb_fetch_chunk": "duckdb_fetch_chunk_ptr",
}

# Functions that are replaced by Mojo helper library functions.
# The original C functions take/return duckdb_decimal by value, which doesn't
# work across the Mojo FFI. The helpers accept/return via pointer.
DECIMAL_HELPER_FUNCTIONS = {
    "duckdb_create_decimal",
    "duckdb_get_decimal",
}

# ---------------------------------------------------------------------------
# Main code generation
# ---------------------------------------------------------------------------

def generate_mojo(duckdb_dir: str, workspace_dir: str) -> str:
    """Generate the complete libduckdb.mojo content."""
    groups, function_map = parse_capi_function_definitions(duckdb_dir)

    # Collect non-deprecated functions we want to generate
    all_entries: list[dict] = []
    grouped_entries: list[tuple[str, str, list[dict]]] = []  # (group_name, description, entries)

    for group in groups:
        if is_group_deprecated(group):
            continue
        entries = [e for e in group.get("entries", []) if not is_function_deprecated(e)]
        if not entries:
            continue
        all_entries.extend(entries)
        grouped_entries.append((group["group"], group.get("description", ""), entries))

    out = []
    
    # ---- File header ----
    out.append(_generate_header())
    out.append("")

    # ---- Enums (from template) ----
    out.append(_generate_enums(duckdb_dir))
    out.append("")

    # ---- Type definitions (from template) ----
    out.append(_generate_types(duckdb_dir))
    out.append("")

    # ---- Library Load ----
    out.append(_generate_library_load())
    out.append("")

    # ---- LibDuckDB struct ----
    out.append(_generate_libduckdb_struct(grouped_entries))
    out.append("")

    # ---- comptime _dylib_function declarations ----
    out.append(_generate_dylib_declarations(grouped_entries))

    return "\n".join(out)


def _generate_header() -> str:
    return """from ffi import external_call, c_char
from utils import StaticTuple
from collections import InlineArray
from duckdb.duckdb_type import *
from sys.info import CompilationTarget
from os import abort
from pathlib import Path
from ffi import _get_dylib_function as _ffi_get_dylib_function
from ffi import _find_dylib, _Global, OwnedDLHandle, UnsafeUnion
from memory import UnsafePointer

# ===--------------------------------------------------------------------===#
# FFI definitions for the DuckDB C API ported to Mojo.
#
# WARNING: this file is autogenerated by scripts/generate_mojo_api.py
# Manual changes will be overwritten!
# ===-----------------------------------------------------------------------===#"""


def _read_template(duckdb_dir: str) -> str:
    """Read the header_base.hpp.template file."""
    template_path = os.path.join(duckdb_dir, BASE_HEADER_TEMPLATE)
    with open(template_path, "r") as f:
        return f.read()


def _generate_enums(duckdb_dir: str) -> str:
    """Generate Mojo enum definitions from the C header template."""
    content = _read_template(duckdb_dir)
    lines: list[str] = []

    lines.append("")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Enums")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")

    # Match typedef enum blocks with an optional immediately preceding //! comment.
    # We use a two-pass approach: first find all enum blocks, then look for
    # preceding comments.
    enum_pattern = re.compile(
        r"typedef\s+enum\s+(\w+)\s*\{(.*?)\}\s*(\w+)\s*;",
        re.DOTALL,
    )

    for m in enum_pattern.finditer(content):
        enum_name = m.group(3).strip()
        body = m.group(2)

        # Look for //! comment lines immediately before this typedef
        # Walk backwards from the match start to find consecutive //! lines
        pre = content[:m.start()].rstrip()
        comment_lines: list[str] = []
        for candidate in reversed(pre.split("\n")):
            stripped = candidate.strip()
            if stripped.startswith("//!"):
                comment_lines.insert(0, stripped[3:].strip())
            elif stripped.startswith("//") and not stripped.startswith("//="):
                # Allow plain // comment lines in the block too
                comment_lines.insert(0, stripped[2:].strip())
            elif stripped == "":
                break
            else:
                break

        if comment_lines:
            lines.append(f"#! {' '.join(comment_lines)}")
        lines.append(f"comptime {enum_name} = Int32")

        # Parse each member: handle both "// comment\n MEMBER = N" and "MEMBER = N"
        member_pat = re.compile(r"(?://\s*([^\n]*)\n\s*)?(\w+)\s*=\s*(\d+)")
        for mm in member_pat.finditer(body):
            member_comment = mm.group(1)
            member_name = mm.group(2)
            member_value = mm.group(3)
            if member_comment:
                lines.append(f"# {member_comment.strip()}")
            lines.append(f"comptime {member_name} = {member_value}")
        lines.append("")

    return "\n".join(lines)


def _generate_types(duckdb_dir: str) -> str:
    """Generate Mojo type definitions from the C header template."""
    content = _read_template(duckdb_dir)
    lines: list[str] = []

    # ---- General type definitions ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# General type definitions")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("#! DuckDB's index type.")
    lines.append("comptime idx_t = UInt64")
    lines.append("")
    lines.append("#! The callback that will be called to destroy data, e.g.,")
    lines.append("#! bind data (if any), init data (if any), extra data for replacement scans (if any)")
    lines.append("comptime duckdb_delete_callback_t = fn (UnsafePointer[NoneType, MutAnyOrigin]) -> NoneType")
    lines.append("")
    lines.append("#! The callback that will be called to copy bind data.")
    lines.append("comptime duckdb_copy_callback_t = fn (UnsafePointer[NoneType, MutAnyOrigin]) -> UnsafePointer[NoneType, MutAnyOrigin]")
    lines.append("")

    # ---- Types (no explicit freeing) ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Types (no explicit freeing)")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")

    lines.append("#! Days are stored as days since 1970-01-01")
    lines.append("#! Use the duckdb_from_date/duckdb_to_date function to extract individual information")
    lines.append("comptime duckdb_date = Date")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_date_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var year: Int32")
    lines.append("    var month: Int8")
    lines.append("    var day: Int8")
    lines.append("")

    lines.append("#! Time is stored as microseconds since 00:00:00")
    lines.append("#! Use the duckdb_from_time/duckdb_to_time function to extract individual information")
    lines.append("comptime duckdb_time = Time")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_time_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var hour: Int8")
    lines.append("    var min: Int8")
    lines.append("    var sec: Int8")
    lines.append("    var micros: Int32")
    lines.append("")

    lines.append("#! TIME_NS is stored as nanoseconds since 00:00:00.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_time_ns(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var nanos: Int64")
    lines.append("")

    lines.append("#! TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_time_tz(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var bits: UInt64")
    lines.append("")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_time_tz_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var time: duckdb_time_struct")
    lines.append("    var offset: Int32")
    lines.append("")

    lines.append("#! Timestamps are stored as microseconds since 1970-01-01")
    lines.append("#! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information")
    lines.append("comptime duckdb_timestamp = Timestamp")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_timestamp_struct(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var date: duckdb_date_struct")
    lines.append("    var time: duckdb_time_struct")
    lines.append("")

    lines.append("#! TIMESTAMP_S is stored as seconds since 1970-01-01.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_timestamp_s(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var seconds: Int64")
    lines.append("")

    lines.append("#! TIMESTAMP_MS is stored as milliseconds since 1970-01-01.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_timestamp_ms(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var millis: Int64")
    lines.append("")

    lines.append("#! TIMESTAMP_NS is stored as nanoseconds since 1970-01-01.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_timestamp_ns(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var nanos: Int64")
    lines.append("")

    # duckdb_interval: using the hack from the original file
    lines.append("# TODO hack to pass struct by value until https://github.com/modular/modular/issues/3144 is fixed")
    lines.append("# Currently it only works with <= 2 struct values")
    lines.append("struct duckdb_interval(TrivialRegisterPassable):")
    lines.append("    var months_days: Int64")
    lines.append("    var micros: Int64")
    lines.append("# comptime duckdb_interval = Interval")
    lines.append("")

    lines.append("#! Hugeints are composed of a (lower, upper) component")
    lines.append("#! The value of the hugeint is upper * 2^64 + lower")
    lines.append("#! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended")
    lines.append("comptime duckdb_hugeint = Int128")
    lines.append("comptime duckdb_uhugeint = UInt128")
    lines.append("#! Decimals are composed of a width and a scale, and are stored in a hugeint")
    lines.append("comptime duckdb_decimal = Decimal")
    lines.append("")

    lines.append("@fieldwise_init")
    lines.append("#! A type holding information about the query execution progress")
    lines.append("struct duckdb_query_progress_type(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var percentage: Float64")
    lines.append("    var rows_processed: UInt64")
    lines.append("    var total_rows_to_process: UInt64")
    lines.append("")

    # duckdb_string_t
    lines.append("#! The internal representation of a VARCHAR (string_t). If the VARCHAR does not")
    lines.append("#! exceed 12 characters, then we inline it. Otherwise, we inline a prefix for faster")
    lines.append("#! string comparisons and store a pointer to the remaining characters. This is a non-")
    lines.append("#! owning structure, i.e., it does not have to be freed.")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_string_t_pointer(Copyable, Movable):")
    lines.append("    var length: UInt32")
    lines.append("    var prefix: InlineArray[c_char, 4]")
    lines.append("    var ptr: UnsafePointer[c_char, MutExternalOrigin]")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_string_t_inlined(Copyable, Movable):")
    lines.append("    var length: UInt32")
    lines.append("    var inlined: InlineArray[c_char, 12]")
    lines.append("")
    lines.append("comptime duckdb_string_t = UnsafeUnion[duckdb_string_t_pointer, duckdb_string_t_inlined]")
    lines.append("")

    # duckdb_list_entry
    lines.append("#! The metadata entry for a LIST.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_list_entry(ImplicitlyCopyable, Movable):")
    lines.append("    var offset: UInt64")
    lines.append("    var length: UInt64")
    lines.append("")

    # duckdb_column
    lines.append("#! A column consists of a pointer to its internal data. Don't operate on this type directly.")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_column(Copyable, Movable):")
    lines.append("    var __deprecated_data: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("    var __deprecated_nullmask: UnsafePointer[Bool, MutExternalOrigin]")
    lines.append("    var __deprecated_type: Int32  # actually a duckdb_type enum")
    lines.append("    var __deprecated_name: UnsafePointer[c_char, ImmutExternalOrigin]")
    lines.append("    var internal_data: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("")
    lines.append("    fn __init__(out self):")
    lines.append("        self.__deprecated_data = UnsafePointer[NoneType, MutExternalOrigin]()")
    lines.append("        self.__deprecated_nullmask = UnsafePointer[Bool, MutExternalOrigin]()")
    lines.append("        self.__deprecated_type = 0")
    lines.append("        self.__deprecated_name = UnsafePointer[c_char, ImmutExternalOrigin]()")
    lines.append("        self.internal_data = UnsafePointer[NoneType, MutExternalOrigin]()")
    lines.append("")

    # Opaque handle types: struct + comptime alias
    opaque_handles = [
        ("duckdb_vector", "__vctr", "MutExternalOrigin"),
        ("duckdb_selection_vector", "__sel", "MutExternalOrigin"),
    ]
    for name, field, origin in opaque_handles:
        lines.append(f"struct _{name}:")
        lines.append(f"    var {field}: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, {origin}]")
        lines.append("")

    # ---- Types (explicit freeing) ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Types (explicit freeing/destroying)")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("struct duckdb_string:")
    lines.append("    var data: UnsafePointer[c_char, MutExternalOrigin]")
    lines.append("    var size: idx_t")
    lines.append("")
    lines.append("struct duckdb_blob:")
    lines.append("    var data: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("    var size: idx_t")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_bit(TrivialRegisterPassable, ImplicitlyCopyable, Movable):")
    lines.append("    var data: UnsafePointer[UInt8, MutExternalOrigin]")
    lines.append("    var size: idx_t")
    lines.append("")
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_bignum(ImplicitlyCopyable, Movable):")
    lines.append("    var data: UnsafePointer[UInt8, MutExternalOrigin]")
    lines.append("    var size: idx_t")
    lines.append("    var is_negative: Bool")
    lines.append("")

    # duckdb_result
    lines.append("@fieldwise_init")
    lines.append("struct duckdb_result(ImplicitlyCopyable & Movable):")
    lines.append("    var __deprecated_column_count: idx_t")
    lines.append("    var __deprecated_row_count: idx_t")
    lines.append("    var __deprecated_rows_changed: idx_t")
    lines.append("    var __deprecated_columns: UnsafePointer[duckdb_column, MutExternalOrigin]")
    lines.append("    var __deprecated_error_message: UnsafePointer[c_char, ImmutExternalOrigin]")
    lines.append("    var internal_data: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("")
    lines.append("    fn __init__(out self):")
    lines.append("        self.__deprecated_column_count = 0")
    lines.append("        self.__deprecated_row_count = 0")
    lines.append("        self.__deprecated_rows_changed = 0")
    lines.append("        self.__deprecated_columns = UnsafePointer[duckdb_column, MutExternalOrigin]()")
    lines.append("        self.__deprecated_error_message = UnsafePointer[c_char, ImmutExternalOrigin]()")
    lines.append("        self.internal_data = UnsafePointer[NoneType, MutExternalOrigin]()")
    lines.append("")

    # Opaque pointer types that need explicit destroy
    opaque_destroy_types = [
        ("duckdb_instance_cache", "__ic"),
        ("duckdb_database", "__db"),
        ("duckdb_connection", "__conn"),
        ("duckdb_client_context", "__ctx"),
        ("duckdb_prepared_statement", "__prep"),
        ("duckdb_extracted_statements", "__extrac"),
        ("duckdb_pending_result", "__pend"),
        ("duckdb_appender", "__appn"),
        ("duckdb_table_description", "__td"),
        ("duckdb_config", "__cnfg"),
        ("duckdb_config_option", "__copt"),
        ("duckdb_logical_type", "__lglt"),
        ("duckdb_create_type_info", "__cti"),
        ("duckdb_data_chunk", "__dtck"),
        ("duckdb_value", "__val"),
        ("duckdb_profiling_info", "__prof"),
        ("duckdb_error_data", "__err"),
        ("duckdb_expression", "__expr"),
        ("duckdb_extension_info", "__ext"),
    ]
    for name, field in opaque_destroy_types:
        lines.append(f"struct _{name}:")
        lines.append(f"    var {field}: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    # ---- Function types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")

    fn_info_types = [
        ("duckdb_function_info", "__fi"),
        ("duckdb_bind_info", "__bi"),
        ("duckdb_init_info", "__ii"),
    ]
    for name, field in fn_info_types:
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    # ---- Scalar function types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Scalar function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name, field_comment in [
        ("duckdb_scalar_function", "A scalar function"),
        ("duckdb_scalar_function_set", "A scalar function set"),
    ]:
        lines.append(f"#! {field_comment}. Must be destroyed with `duckdb_destroy_{name.replace('duckdb_', '')}`.")
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    lines.append("#! The bind function of the scalar function.")
    lines.append(f"comptime duckdb_scalar_function_bind_t = {CALLBACK_TYPES['duckdb_scalar_function_bind_t']}")
    lines.append("")
    lines.append("#! The main function of the scalar function.")
    lines.append(f"comptime duckdb_scalar_function_t = {CALLBACK_TYPES['duckdb_scalar_function_t']}")
    lines.append("")

    # ---- Table function types ----  
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Table function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("#! A table function. Must be destroyed with `duckdb_destroy_table_function`.")
    lines.append("struct _duckdb_table_function:")
    lines.append("    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("comptime duckdb_table_function = UnsafePointer[_duckdb_table_function, MutExternalOrigin]")
    lines.append("")
    lines.append("#! The bind function of the table function.")
    lines.append(f"comptime duckdb_table_function_bind_t = {CALLBACK_TYPES['duckdb_table_function_bind_t']}")
    lines.append("")
    lines.append("#! The possibly thread-local initialization function of the table function.")
    lines.append(f"comptime duckdb_table_function_init_t = {CALLBACK_TYPES['duckdb_table_function_init_t']}")
    lines.append("")
    lines.append("#! The function to generate an output chunk during table function execution.")
    lines.append(f"comptime duckdb_table_function_t = {CALLBACK_TYPES['duckdb_table_function_t']}")
    lines.append("")

    # ---- Aggregate function types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Aggregate function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name, comment in [
        ("duckdb_aggregate_function", "An aggregate function"),
        ("duckdb_aggregate_function_set", "A aggregate function set"),
        ("duckdb_aggregate_state", "The state of an aggregate function"),
    ]:
        lines.append(f"#! {comment}.")
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    agg_callbacks = [
        ("duckdb_aggregate_state_size", "A function to return the aggregate state's size."),
        ("duckdb_aggregate_init_t", "A function to initialize an aggregate state."),
        ("duckdb_aggregate_destroy_t", "An optional function to destroy an aggregate state."),
        ("duckdb_aggregate_update_t", "A function to update a set of aggregate states with new values."),
        ("duckdb_aggregate_combine_t", "A function to combine aggregate states."),
        ("duckdb_aggregate_finalize_t", "A function to finalize aggregate states into a result vector."),
    ]
    for cb_name, cb_comment in agg_callbacks:
        lines.append(f"#! {cb_comment}")
        lines.append(f"comptime {cb_name} = {CALLBACK_TYPES[cb_name]}")
        lines.append("")
    
    # ---- Replacement scan types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Replacement scan types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("struct _duckdb_replacement_scan_info:")
    lines.append("    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("comptime duckdb_replacement_scan_info = UnsafePointer[_duckdb_replacement_scan_info, MutExternalOrigin]")
    lines.append("")
    lines.append(f"comptime duckdb_replacement_callback_t = {CALLBACK_TYPES['duckdb_replacement_callback_t']}")
    lines.append("")

    # ---- Cast function types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Cast function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("struct _duckdb_cast_function:")
    lines.append("    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("comptime duckdb_cast_function = UnsafePointer[_duckdb_cast_function, MutExternalOrigin]")
    lines.append("")
    lines.append(f"comptime duckdb_cast_function_t = {CALLBACK_TYPES['duckdb_cast_function_t']}")
    lines.append("")

    # ---- Copy function types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Copy function types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name in ["duckdb_copy_function", "duckdb_copy_function_bind_info",
                  "duckdb_copy_function_global_init_info", "duckdb_copy_function_sink_info",
                  "duckdb_copy_function_finalize_info"]:
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")
    
    for cb_name in ["duckdb_copy_function_bind_t", "duckdb_copy_function_global_init_t",
                     "duckdb_copy_function_sink_t", "duckdb_copy_function_finalize_t"]:
        lines.append(f"comptime {cb_name} = {CALLBACK_TYPES[cb_name]}")
    lines.append("")

    # ---- Arrow types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Arrow-related types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name in ["duckdb_arrow", "duckdb_arrow_stream", "duckdb_arrow_schema",
                  "duckdb_arrow_array", "duckdb_arrow_converted_schema", "duckdb_arrow_options"]:
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    # ---- File system types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Virtual File System types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name in ["duckdb_file_open_options", "duckdb_file_system", "duckdb_file_handle"]:
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    # ---- Catalog types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Catalog Interface types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    for name in ["duckdb_catalog", "duckdb_catalog_entry"]:
        lines.append(f"struct _{name}:")
        lines.append(f"    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
        lines.append(f"comptime {name} = UnsafePointer[_{name}, MutExternalOrigin]")
        lines.append("")

    # ---- Logging types ----
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("# Logging types")
    lines.append("# ===--------------------------------------------------------------------===#")
    lines.append("")
    lines.append("struct _duckdb_log_storage:")
    lines.append("    var internal_ptr: UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("comptime duckdb_log_storage = UnsafePointer[_duckdb_log_storage, MutExternalOrigin]")
    lines.append("")
    lines.append(f"comptime duckdb_logger_write_log_entry_t = {CALLBACK_TYPES['duckdb_logger_write_log_entry_t']}")
    lines.append("")

    # ---- Task state type ----
    lines.append("comptime duckdb_task_state = UnsafePointer[NoneType, MutExternalOrigin]")
    lines.append("")

    return "\n".join(lines)


def _generate_library_load() -> str:
    return """# ===-----------------------------------------------------------------------===#
# Library Load
# ===-----------------------------------------------------------------------===#

comptime DUCKDB_LIBRARY_PATHS: List[Path] = [
    "libduckdb.so",
    "libduckdb.dylib",
]

comptime DUCKDB_LIBRARY = _Global["DUCKDB_LIBRARY", _init_dylib]

fn _init_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb"](materialize[DUCKDB_LIBRARY_PATHS]())


@always_inline
fn _get_dylib_function[
    func_name: StaticString, result_type: __TypeOfAllTypes
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_LIBRARY(),
        func_name,
        result_type,
    ]()


struct _dylib_function[fn_name: StaticString, type: __TypeOfAllTypes](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    fn load() raises -> Self.type:
        return _get_dylib_function[Self.fn_name, Self.type]()

comptime DUCKDB_HELPERS_PATHS: List[Path] = [
    "libduckdb_mojo_helpers.so",
    "libduckdb_mojo_helpers.dylib",
]

comptime DUCKDB_HELPERS_LIBRARY = _Global["DUCKDB_HELPERS_LIBRARY", _init_helper_dylib]

fn _init_helper_dylib() -> OwnedDLHandle:
    return _find_dylib["libduckdb_mojo_helpers"](materialize[DUCKDB_HELPERS_PATHS]())

@always_inline
fn _get_dylib_helpers_function[
    func_name: StaticString, result_type: __TypeOfAllTypes
]() raises -> result_type:
    return _ffi_get_dylib_function[
        DUCKDB_HELPERS_LIBRARY(),
        func_name,
        result_type,
    ]()

struct _dylib_helpers_function[fn_name: StaticString, type: __TypeOfAllTypes](TrivialRegisterPassable):
    comptime fn_type = Self.type

    @staticmethod
    fn load() raises -> Self.type:
        return _get_dylib_helpers_function[Self.fn_name, Self.type]()"""


def _generate_libduckdb_struct(grouped_entries: list[tuple[str, str, list[dict]]]) -> str:
    """Generate the LibDuckDB struct with vars, __init__, __moveinit__, and methods."""
    lines: list[str] = []

    # Collect all function names for the struct
    all_fn_names: list[str] = []
    for group_name, desc, entries in grouped_entries:
        for entry in entries:
            all_fn_names.append(entry["name"])

    # Mojo helper function names (loaded from the helper library)
    MOJO_HELPER_FN_NAMES = sorted(["duckdb_mojo_get_decimal", "duckdb_mojo_create_decimal"])

    lines.append("struct LibDuckDB(Movable):")
    lines.append("")

    # ---- var declarations ----
    for name in all_fn_names:
        if name in DECIMAL_HELPER_FUNCTIONS:
            # Skipped: replaced by Mojo helper functions
            continue
        if name in HELPER_WRAPPER_FUNCTIONS:
            # For by-value result functions, we use the helper wrapper
            helper_name = HELPER_WRAPPER_FUNCTIONS[name]
            lines.append(f"    var _{helper_name}: _{helper_name}.fn_type")
        else:
            lines.append(f"    var _{name}: _{name}.fn_type")

    # Add Mojo helper vars
    for name in MOJO_HELPER_FN_NAMES:
        lines.append(f"    var _{name}: _{name}.fn_type")
    lines.append("")

    # ---- __init__ ----
    lines.append("    fn __init__(out self):")
    lines.append("        try:")
    for name in all_fn_names:
        if name in DECIMAL_HELPER_FUNCTIONS:
            continue
        if name in HELPER_WRAPPER_FUNCTIONS:
            helper_name = HELPER_WRAPPER_FUNCTIONS[name]
            lines.append(f"            self._{helper_name} = _{helper_name}.load()")
        else:
            lines.append(f"            self._{name} = _{name}.load()")
    for name in MOJO_HELPER_FN_NAMES:
        lines.append(f"            self._{name} = _{name}.load()")
    lines.append("        except e:")
    lines.append("            abort(String(e))")
    lines.append("")

    # ---- __moveinit__ ----
    lines.append("    fn __moveinit__(out self, deinit take: Self):")
    for name in all_fn_names:
        if name in DECIMAL_HELPER_FUNCTIONS:
            continue
        if name in HELPER_WRAPPER_FUNCTIONS:
            helper_name = HELPER_WRAPPER_FUNCTIONS[name]
            lines.append(f"        self._{helper_name} = take._{helper_name}")
        else:
            lines.append(f"        self._{name} = take._{name}")
    for name in MOJO_HELPER_FN_NAMES:
        lines.append(f"        self._{name} = take._{name}")
    lines.append("")

    # ---- Methods ----
    lines.append("    # ===--------------------------------------------------------------------===#")
    lines.append("    # Functions")
    lines.append("    # ===--------------------------------------------------------------------===#")

    for group_name, desc, entries in grouped_entries:
        title = headline_capitalize(group_name)
        lines.append(format_comment_header(title))

        for entry in entries:
            name = entry["name"]
            
            # Special handling for functions that take duckdb_result by value
            if name in HELPER_WRAPPER_FUNCTIONS:
                helper_name = HELPER_WRAPPER_FUNCTIONS[name]
                lines.append(_generate_byval_helper_method(entry, helper_name))
            elif name == "duckdb_create_decimal":
                lines.append(_generate_decimal_create_method(entry))
            elif name == "duckdb_get_decimal":
                lines.append(_generate_decimal_get_method(entry))
            else:
                lines.append(_generate_normal_method(entry))
            lines.append("")

    return "\n".join(lines)


def _generate_normal_method(entry: dict) -> str:
    """Generate a normal LibDuckDB method."""
    name = entry["name"]
    params = entry.get("params", [])
    ret = entry["return_type"]
    comment = entry.get("comment", {})

    lines: list[str] = []

    # Signature
    sig_parts = [f"    fn {name}(\n        self"]
    for p in params:
        ptype = p["type"].strip()
        pname = p["name"]
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        mojo_type = c_type_to_mojo(ptype)
        sig_parts.append(f",\n        {pname}: {mojo_type}")
    mojo_ret = c_type_to_mojo(ret, is_return=True)
    lines.append("".join(sig_parts) + f",\n    ) -> {mojo_ret}:")

    # Docstring
    desc = comment.get("description", "").strip()
    if desc:
        lines.append('        """')
        for line in desc.split("\n"):
            lines.append(f"        {line.rstrip()}")
        lines.append('        """')

    # Call body
    args: list[str] = []
    for p in params:
        pname = p["name"]
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        args.append(pname)
    lines.append(f"        return self._{name}({', '.join(args)})")

    return "\n".join(lines)


def _generate_byval_helper_method(entry: dict, helper_name: str) -> str:
    """Generate a method that uses a pointer-based helper wrapper for large by-value structs."""
    name = entry["name"]
    params = entry.get("params", [])
    ret = entry["return_type"]
    comment = entry.get("comment", {})

    lines: list[str] = []

    # Signature – use pointer params for duckdb_result
    sig_parts = [f"    fn {name}(\n        self"]
    for p in params:
        ptype = p["type"].strip()
        pname = p["name"]
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        if ptype == "duckdb_result":
            # Take by value in the public API but convert internally
            sig_parts.append(f",\n        {pname}: {ptype}")
        else:
            mojo_type = c_type_to_mojo(ptype)
            sig_parts.append(f",\n        {pname}: {mojo_type}")
    mojo_ret = c_type_to_mojo(ret, is_return=True)
    lines.append("".join(sig_parts) + f",\n    ) -> {mojo_ret}:")

    # Docstring
    desc = comment.get("description", "").strip()
    if desc:
        lines.append('        """')
        for line in desc.split("\n"):
            lines.append(f"        {line.rstrip()}")
        lines.append("")
        lines.append("        NOTE: Mojo cannot currently pass large structs by value correctly over the C ABI.")
        lines.append("        We therefore call a helper wrapper that accepts a pointer instead.")
        lines.append('        """')

    # Call body – wrap the by-value arg in UnsafePointer
    args: list[str] = []
    for p in params:
        ptype = p["type"].strip()
        pname = p["name"]
        if pname in MOJO_RESERVED_PARAM_NAMES:
            pname = pname + "_"
        if ptype == "duckdb_result":
            args.append(f"UnsafePointer(to={pname})")
        else:
            args.append(pname)
    lines.append(f"        return self._{helper_name}({', '.join(args)})")

    return "\n".join(lines)


def _generate_decimal_create_method(entry: dict) -> str:
    """Generate the special create_decimal method that uses the helper."""
    lines = [
        "    fn duckdb_create_decimal(",
        "        self,",
        "        input_: duckdb_decimal,",
        "    ) -> duckdb_value:",
        '        """Creates a DECIMAL value from the given decimal value.',
        "",
        "        * input: The decimal value.",
        "        * returns: A duckdb_value containing the decimal.",
        '        """',
        "        return self._duckdb_mojo_create_decimal(UnsafePointer(to=input_).unsafe_origin_cast[ImmutAnyOrigin]())",
    ]
    return "\n".join(lines)


def _generate_decimal_get_method(entry: dict) -> str:
    """Generate the special get_decimal method that uses the helper."""
    lines = [
        "    fn duckdb_get_decimal(",
        "        self,",
        "        val: duckdb_value,",
        "    ) -> duckdb_decimal:",
        '        """Returns the decimal value of the given value.',
        "",
        "        * val: A duckdb_value containing a decimal",
        "        * returns: A decimal, or 0 if the value cannot be converted",
        '        """',
        "        var result = duckdb_decimal(width=0, scale=0, value=0)",
        "        self._duckdb_mojo_get_decimal(val, UnsafePointer(to=result).unsafe_origin_cast[MutExternalOrigin]())",
        "        return result",
    ]
    return "\n".join(lines)


def _generate_dylib_declarations(grouped_entries: list[tuple[str, str, list[dict]]]) -> str:
    """Generate the comptime _dylib_function declarations."""
    lines: list[str] = []

    for group_name, desc, entries in grouped_entries:
        title = headline_capitalize(group_name)
        lines.append(format_section_header(title))

        for entry in entries:
            name = entry["name"]
            
            if name in HELPER_WRAPPER_FUNCTIONS:
                # Generate the helper function declaration
                helper_name = HELPER_WRAPPER_FUNCTIONS[name]
                # Build a modified entry with pointer params
                modified_params = []
                for p in entry.get("params", []):
                    if p["type"].strip() == "duckdb_result":
                        modified_params.append({"type": "duckdb_result *", "name": p["name"]})
                    else:
                        modified_params.append(p)
                # Generate the ptr version using helpers library
                helper_entry = dict(entry)
                helper_entry["params"] = modified_params
                fn_type = mojo_fn_type(helper_entry)
                # Use ImmutAnyOrigin for the result pointer param in helpers
                fn_type = fn_type.replace(
                    "UnsafePointer[duckdb_result, MutAnyOrigin]",
                    "UnsafePointer[duckdb_result, ImmutAnyOrigin]"
                )
                lines.append(f'comptime _{helper_name} = _dylib_helpers_function["{helper_name}",')
                lines.append(f"    {fn_type}")
                lines.append("]")
                lines.append("")
            elif name == "duckdb_create_decimal":
                # Skip - handled via helper
                pass
            elif name == "duckdb_get_decimal":
                # Skip - handled via helper
                pass
            else:
                fn_type = mojo_fn_type(entry)
                lines.append(f'comptime _{name} = _dylib_function["{name}",')
                lines.append(f"    {fn_type}")
                lines.append("]")
                lines.append("")

    # Add the Mojo helper declarations
    lines.append(format_section_header("Mojo Helper Functions"))
    lines.append('comptime _duckdb_mojo_get_decimal = _dylib_helpers_function["duckdb_mojo_get_decimal",')
    lines.append("    fn(duckdb_value, UnsafePointer[duckdb_decimal, MutExternalOrigin]) -> NoneType")
    lines.append("]")
    lines.append("")
    lines.append('comptime _duckdb_mojo_create_decimal = _dylib_helpers_function["duckdb_mojo_create_decimal",')
    lines.append("    fn(UnsafePointer[duckdb_decimal, ImmutAnyOrigin]) -> duckdb_value")
    lines.append("]")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Mojo bindings for the DuckDB C API")
    parser.add_argument(
        "--duckdb-dir",
        default="duckdb",
        help="Path to the DuckDB source tree (default: duckdb)",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help=f"Output file path (default: {OUTPUT_FILE})",
    )
    args = parser.parse_args()

    duckdb_dir = os.path.abspath(args.duckdb_dir)
    workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if not os.path.isdir(os.path.join(duckdb_dir, "src")):
        print(f"ERROR: DuckDB source directory not found at {duckdb_dir}", file=sys.stderr)
        sys.exit(1)

    output = generate_mojo(duckdb_dir, workspace_dir)
    
    output_path = os.path.join(workspace_dir, args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(output)
        f.write("\n")
    
    print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
