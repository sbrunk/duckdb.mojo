#include "duckdb.h"

// Helper wrapper: duckdb_fetch_chunk expects duckdb_result by value. Mojo currently
// cannot safely pass large structs by value, so we expose a pointer-based shim.
// Build this into a shared library (libduckdb_mojo_helpers.{so,dylib}).

duckdb_data_chunk duckdb_fetch_chunk_ptr(duckdb_result *result) {
    if (!result) {
        return (duckdb_data_chunk) { 0 };
    }
    return duckdb_fetch_chunk(*result);
}

// Helper wrapper for duckdb_result_statement_type - same issue with passing by value
duckdb_statement_type duckdb_result_statement_type_ptr(duckdb_result *result) {
    if (!result) {
        return DUCKDB_STATEMENT_TYPE_INVALID;
    }
    return duckdb_result_statement_type(*result);
}

// Helper wrapper for duckdb_get_decimal to avoid returning large structs by value
void duckdb_mojo_get_decimal(duckdb_value val, duckdb_decimal *out_decimal) {
    if (out_decimal) {
        *out_decimal = duckdb_get_decimal(val);
    }
}

// Helper wrapper for duckdb_create_decimal to avoid passing large structs by value from Mojo
duckdb_value duckdb_mojo_create_decimal(duckdb_decimal *decimal) {
    if (!decimal) {
        return NULL; 
    }
    return duckdb_create_decimal(*decimal);
}

