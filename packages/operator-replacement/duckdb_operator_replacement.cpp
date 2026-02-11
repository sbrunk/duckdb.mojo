#include "duckdb_operator_replacement.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

// Initialize static registry
std::unordered_map<string, string> OperatorReplacementExtension::replacement_registry;

void OperatorReplacementExtension::RegisterReplacement(const string &original_name, const string &replacement_name) {
    replacement_registry[original_name] = replacement_name;
    printf("[DEBUG] Registered replacement: '%s' -> '%s'\n", original_name.c_str(), replacement_name.c_str());
}

void OperatorReplacementExtension::ClearReplacements() {
    replacement_registry.clear();
}

const std::unordered_map<string, string>& OperatorReplacementExtension::GetReplacements() {
    return replacement_registry;
}

void OperatorReplacementExtension::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (replacement_registry.empty()) {
        // No replacements registered, skip optimization
        return;
    }
    printf("[DEBUG] Optimize called with %zu registered replacements\n", replacement_registry.size());
    ReplaceOperators(input.context, plan);
    printf("[DEBUG] Optimize done!\n");
}

void OperatorReplacementExtension::ReplaceOperators(ClientContext &context, unique_ptr<LogicalOperator> &op) {
    if (!op) {
        return;
    }

    // Traverse all expressions in this operator using LogicalOperatorVisitor
    LogicalOperatorVisitor::EnumerateExpressions(*op, [&](unique_ptr<Expression> *expr_ptr) {
        if (!expr_ptr || !*expr_ptr) {
            return;
        }
        
        auto &expr = **expr_ptr;
        
        // Look for BoundFunctionExpression and check if it's in our replacement registry
        if (expr.expression_class == ExpressionClass::BOUND_FUNCTION) {
            auto &func_expr = expr.Cast<BoundFunctionExpression>();
            
            // Check if this function is registered for replacement
            auto it = replacement_registry.find(func_expr.function.name);
            if (it != replacement_registry.end()) {
                const string &replacement_name = it->second;
                // Get the catalog to look up the replacement function
                auto &catalog = Catalog::GetSystemCatalog(context);
                EntryLookupInfo lookup_info(CatalogType::SCALAR_FUNCTION_ENTRY, replacement_name);
                auto func_entry = catalog.GetEntry(context, DEFAULT_SCHEMA, lookup_info, 
                                                  OnEntryNotFound::RETURN_NULL);
                
                if (func_entry && func_entry->type == CatalogType::SCALAR_FUNCTION_ENTRY) {
                    auto &scalar_func = func_entry->Cast<ScalarFunctionCatalogEntry>();
                    
                    // Get argument types from the original function
                    vector<LogicalType> arg_types;
                    for (auto &child : func_expr.children) {
                        arg_types.push_back(child->return_type);
                    }
                    
                    // Get the matching function overload
                    auto replacement_func = scalar_func.functions.GetFunctionByArguments(context, arg_types);
                    
                    // Replace the function with custom version
                    vector<unique_ptr<Expression>> children;
                    for (auto &child : func_expr.children) {
                        children.push_back(std::move(child));
                    }
                    
                    *expr_ptr = make_uniq<BoundFunctionExpression>(
                        expr.return_type,
                        replacement_func,
                        std::move(children),
                        nullptr
                    );
                }
            }
        }
    });

    // Recursively process children
    for (auto &child : op->children) {
        ReplaceOperators(context, child);
    }
}

} // namespace duckdb
