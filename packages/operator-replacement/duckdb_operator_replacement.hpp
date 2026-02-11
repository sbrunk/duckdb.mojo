#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include <unordered_map>
#include <string>

namespace duckdb {

class OperatorReplacementExtension {
public:
    // Register a function/operator replacement
    // original_name: the function/operator to replace (e.g., "*", "sqrt", "+")
    // replacement_name: the name of the replacement function in the catalog
    static void RegisterReplacement(const string &original_name, const string &replacement_name);
    
    // Clear all registered replacements
    static void ClearReplacements();
    
    // Get all registered replacements
    static const std::unordered_map<string, string>& GetReplacements();
    
    // The optimizer function called by DuckDB
    static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
    
private:
    static void ReplaceOperators(ClientContext &context, unique_ptr<LogicalOperator> &op);
    
    // Registry mapping original function names to replacement function names
    static std::unordered_map<string, string> replacement_registry;
};

} // namespace duckdb
