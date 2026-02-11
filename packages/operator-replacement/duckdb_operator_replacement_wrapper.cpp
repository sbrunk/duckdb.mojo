#include "duckdb_operator_replacement.h"
#include "duckdb_operator_replacement.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

using namespace duckdb;

extern "C" {

void register_function_replacement(const char *original_name, const char *replacement_name) {
    OperatorReplacementExtension::RegisterReplacement(original_name, replacement_name);
}

void register_operator_replacement(duckdb_connection con) {
    auto connection = reinterpret_cast<Connection*>(con);
    
    // Create the optimizer extension
    OptimizerExtension extension;
    extension.optimize_function = OperatorReplacementExtension::Optimize;
    
    // Version-specific registration:
    // - v1.4.4: Direct push_back to optimizer_extensions vector
    // - v1.5.0+: Use OptimizerExtension::Register() static method
    
    // For DuckDB v1.4.4:
    auto &config = DBConfig::GetConfig(*connection->context);
    config.optimizer_extensions.push_back(extension);
    
    // For DuckDB v1.5.0-dev and later, uncomment this line and comment out the above:
    // OptimizerExtension::Register(DBConfig::GetConfig(*connection->context), extension);
}

}
