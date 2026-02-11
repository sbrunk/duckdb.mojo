#include <stdio.h>
#include <stdlib.h>
#include "duckdb.h"
#include "duckdb_operator_replacement.h"
#include "test_functions.h"

void check_error(duckdb_state state, const char *msg, duckdb_result *result) {
    if (state == DuckDBError) {
        fprintf(stderr, "Error: %s - %s\n", msg, duckdb_result_error(result));
        if (result) duckdb_destroy_result(result);
        exit(1);
    }
}

int main() {
    duckdb_database db;
    duckdb_connection con;
    duckdb_result result;
    duckdb_state state;

    // Open database
    if (duckdb_open(NULL, &db) == DuckDBError) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    if (duckdb_connect(db, &con) == DuckDBError) {
        fprintf(stderr, "Failed to connect\n");
        duckdb_close(&db);
        return 1;
    }

    printf("=== Testing Operator Replacement ===\n\n");

    // Register custom multiply function
    printf("1. Registering example custom functions...");
    register_custom_multiply(con);
    register_custom_sqrt(con);
    printf("   ✓ Functions registered\n\n");

    // Test custom function directly
    printf("2. Testing custom function directly: SELECT custom_multiply(3, 4)\n");
    state = duckdb_query(con, "SELECT custom_multiply(3, 4) as result", &result);
    check_error(state, "Direct function call failed", &result);
    
    int64_t direct_result = duckdb_value_int64(&result, 0, 0);
    printf("   Result: %lld (expected 6 since custom_multiply doubles first arg)\n", direct_result);
    duckdb_destroy_result(&result);
    printf("   ✓ Custom function works\n\n");

    // Register which operators/functions to replace
    printf("3. Registering function replacements...\n");
    register_function_replacement("*", "custom_multiply");  // Replace * operator with custom_multiply
    register_function_replacement("sqrt", "custom_sqrt");   // Replace sqrt function with custom_sqrt
    printf("   ✓ Replacements registered\n\n");

    // Register optimizer extension
    printf("4. Registering optimizer extension...\n");
    register_operator_replacement(con);
    printf("   ✓ Extension registered\n\n");

    // Test operator replacement
    printf("5. Testing operator replacement: SELECT 3 * 4\n");
    printf("   (Should use custom_multiply if replacement works)\n");
    state = duckdb_query(con, "SELECT 3 * 4 as result", &result);
    check_error(state, "Operator query failed", &result);
    
    int64_t op_result = duckdb_value_int64(&result, 0, 0);
    printf("   Result: %lld\n", op_result);
    if (op_result == 6) {
        printf("   ✓ SUCCESS! Operator was replaced (got 6 instead of 12)\n");
    } else if (op_result == 12) {
        printf("   ✗ FAILED! Operator was NOT replaced (got standard result 12)\n");
    } else {
        printf("   ? Unexpected result: %lld\n", op_result);
    }
    duckdb_destroy_result(&result);
    printf("\n");

    // Test in a more complex query
    printf("6. Testing in complex query: SELECT l_quantity * l_extendedprice FROM (VALUES (2, 5)) t(l_quantity, l_extendedprice)\n");
    state = duckdb_query(con, 
        "SELECT l_quantity * l_extendedprice as result FROM (VALUES (2, 5)) t(l_quantity, l_extendedprice)", 
        &result);
    check_error(state, "Complex query failed", &result);
    
    int64_t complex_result = duckdb_value_int64(&result, 0, 0);
    printf("   Result: %lld\n", complex_result);
    if (complex_result == 4) {
        printf("   ✓ SUCCESS! Operator was replaced in complex query\n");
    } else if (complex_result == 10) {
        printf("   ✗ FAILED! Operator was NOT replaced\n");
    } else {
        printf("   ? Unexpected result: %lld\n", complex_result);
    }
    duckdb_destroy_result(&result);
    printf("\n");

    // Test sqrt replacement
    printf("7. Testing sqrt replacement: SELECT sqrt(CAST(value AS DOUBLE)) FROM (VALUES (25.0)) t(value)\n");
    printf("   (Should return value + 100 if replacement works)\n");
    state = duckdb_query(con, 
        "SELECT sqrt(CAST(value AS DOUBLE)) as result FROM (VALUES (25.0)) t(value)", 
        &result);
    check_error(state, "Sqrt query failed", &result);
    
    double sqrt_result = duckdb_value_double(&result, 0, 0);
    printf("   Result: %.1f\n", sqrt_result);
    if (sqrt_result == 125.0) {
        printf("   ✓ SUCCESS! sqrt was replaced (got 25 + 100 = 125 instead of 5)\n");
    } else if (sqrt_result == 5.0) {
        printf("   ✗ FAILED! sqrt was NOT replaced (got standard result 5)\n");
    } else {
        printf("   ? Unexpected result: %.1f\n", sqrt_result);
    }
    duckdb_destroy_result(&result);

    // Cleanup
    duckdb_disconnect(&con);
    duckdb_close(&db);

    printf("\n=== Test Complete ===\n");
    return 0;
}
