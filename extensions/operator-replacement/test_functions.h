#ifndef TEST_FUNCTIONS_H
#define TEST_FUNCTIONS_H

#include "duckdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// Example custom function implementations for testing

// Register custom_multiply function (doubles first argument instead of multiplying)
void register_custom_multiply(duckdb_connection con);

// Register custom_sqrt function (adds 100 instead of taking square root)
void register_custom_sqrt(duckdb_connection con);

#ifdef __cplusplus
}
#endif

#endif
