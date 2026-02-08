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
