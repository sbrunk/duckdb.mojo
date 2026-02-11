#ifndef DUCKDB_OPERATOR_REPLACEMENT_H
#define DUCKDB_OPERATOR_REPLACEMENT_H

#include "duckdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// Register a function/operator replacement
// original_name: the function/operator to replace (e.g., "*", "sqrt", "+")
// replacement_name: the name of the replacement function in the catalog
void register_function_replacement(const char *original_name, const char *replacement_name);

// Register the operator replacement extension - activates all registered replacements
void register_operator_replacement(duckdb_connection con);

#ifdef __cplusplus
}
#endif

#endif
