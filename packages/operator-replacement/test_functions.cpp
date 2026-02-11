// Example custom function implementations for testing operator replacement
// These are test stubs that demonstrate the operator replacement mechanism

#include "test_functions.h"
#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

using namespace duckdb;

// Custom multiply function implementation - works with any integer type
template<class T>
static void custom_multiply_impl(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &left = args.data[0];
    BinaryExecutor::Execute<T, T, T>(
        left, args.data[1], result, args.size(),
        [](T left_val, T right_val) {
            // Double the left value to show it's our custom function
            // For real use, replace with: return left_val * right_val;
            return left_val * 2;
        });
}

// Wrapper that dispatches to the right template based on type
static void custom_multiply_func(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &left_type = args.data[0].GetType();
    switch (left_type.InternalType()) {
        case PhysicalType::INT8:
            custom_multiply_impl<int8_t>(args, state, result);
            break;
        case PhysicalType::INT16:
            custom_multiply_impl<int16_t>(args, state, result);
            break;
        case PhysicalType::INT32:
            custom_multiply_impl<int32_t>(args, state, result);
            break;
        case PhysicalType::INT64:
            custom_multiply_impl<int64_t>(args, state, result);
            break;
        default:
            throw NotImplementedException("Unsupported type for custom_multiply");
    }
}

// Custom sqrt function - returns input + 100 to show it's custom
static void custom_sqrt_func(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    UnaryExecutor::Execute<double, double>(input, result, args.size(), [](double val) {
        // Add 100 to show this is our custom function (not real sqrt)
        // For real use, replace with: return std::sqrt(val);
        return val + 100.0;
    });
}

extern "C" {

void register_custom_multiply(duckdb_connection con) {
    auto connection = reinterpret_cast<Connection*>(con);
    
    // Register it within a transaction
    connection->context->RunFunctionInTransaction([&]() {
        auto &catalog = Catalog::GetSystemCatalog(*connection->context);
        
        // Create a function set with overloads for different types
        ScalarFunctionSet func_set("custom_multiply");
        
        // Register for common integer types
        for (auto &type : {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER, LogicalType::BIGINT}) {
            ScalarFunction func({type, type}, type, custom_multiply_func);
            func_set.AddFunction(func);
        }
        
        CreateScalarFunctionInfo info(func_set);
        catalog.CreateFunction(*connection->context, info);
    });
}

void register_custom_sqrt(duckdb_connection con) {
    auto connection = reinterpret_cast<Connection*>(con);
    
    connection->context->RunFunctionInTransaction([&]() {
        auto &catalog = Catalog::GetSystemCatalog(*connection->context);
        
        ScalarFunction sqrt_func("custom_sqrt", {LogicalType::DOUBLE}, LogicalType::DOUBLE, custom_sqrt_func);
        CreateScalarFunctionInfo sqrt_info(sqrt_func);
        catalog.CreateFunction(*connection->context, sqrt_info);
    });
}

}
