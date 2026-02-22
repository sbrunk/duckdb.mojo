#include "duckdb.h"

// Workaround wrappers for Mojo FFI limitations with by-value struct passing.
// Mojo currently cannot safely pass large structs by value over the C ABI,
// so these pointer-based shims work around the issue.
// Build this into a shared library (libduckdb_mojo_helpers.{so,dylib}).

duckdb_data_chunk workaround_fetch_chunk_ptr(duckdb_result *result) {
    if (!result) {
        return (duckdb_data_chunk) { 0 };
    }
    return duckdb_fetch_chunk(*result);
}

// Workaround for duckdb_result_statement_type - same issue with passing by value
duckdb_statement_type workaround_result_statement_type_ptr(duckdb_result *result) {
    if (!result) {
        return DUCKDB_STATEMENT_TYPE_INVALID;
    }
    return duckdb_result_statement_type(*result);
}

// Workaround for duckdb_get_decimal to avoid returning large structs by value
void workaround_get_decimal_ptr(duckdb_value val, duckdb_decimal *out_decimal) {
    if (out_decimal) {
        *out_decimal = duckdb_get_decimal(val);
    }
}

// Workaround for duckdb_create_decimal to avoid passing large structs by value from Mojo
duckdb_value workaround_create_decimal_ptr(duckdb_decimal *decimal) {
    if (!decimal) {
        return NULL; 
    }
    return duckdb_create_decimal(*decimal);
}

