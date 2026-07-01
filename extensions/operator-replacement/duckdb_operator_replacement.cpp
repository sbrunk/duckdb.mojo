#include "duckdb_operator_replacement.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Initialize static registry
std::unordered_map<string, string> OperatorReplacementExtension::replacement_registry;

void OperatorReplacementExtension::RegisterReplacement(const string &original_name, const string &replacement_name) {
    replacement_registry[original_name] = replacement_name;
    // printf("[DEBUG] Registered replacement: '%s' -> '%s'\n", original_name.c_str(), replacement_name.c_str());
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
    // printf("[DEBUG] Optimize called with %zu registered replacements\n", replacement_registry.size());
    ReplaceOperators(input.context, plan);
    // printf("[DEBUG] Optimize done!\n");
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
                    
                    // Try to get the matching function overload.
                    // Skip replacement if no compatible overload exists (e.g., when
                    // intermediate DECIMAL types are wider than our registered functions
                    // can handle, or when types like DATE/INTERVAL don't match).
                    try {
                        auto replacement_func = scalar_func.functions.GetFunctionByArguments(context, arg_types);

                        // Save the original expression return type before modification.
                        // Parent expressions were bound expecting this type.
                        auto original_return_type = func_expr.return_type;

                        // Insert BoundCastExpression wrappers on children whose types
                        // don't match the replacement function's declared parameter types.
                        // E.g., DECIMAL(15,2) -> DECIMAL(18,4) scale adjustment.
                        // AddCastToType is a no-op when source == target type.
                        for (idx_t i = 0; i < func_expr.children.size() && i < replacement_func.arguments.size(); i++) {
                            if (func_expr.children[i]->return_type != replacement_func.arguments[i]) {
                                func_expr.children[i] = BoundCastExpression::AddCastToType(
                                    context, std::move(func_expr.children[i]), replacement_func.arguments[i]);
                            }
                        }

                        // Replace the function and update the expression's return type
                        // so the execution engine allocates the correct output vector.
                        func_expr.function = replacement_func;
                        func_expr.return_type = replacement_func.return_type;

                        // Re-bind the function to create proper bind_info for the
                        // replacement function. The original bind_info was created by
                        // the built-in function's bind callback and is incompatible
                        // with the C API wrapper that our replacement uses (which
                        // expects CScalarFunctionInfo). Without re-binding, the
                        // execution wrapper interprets the wrong memory layout → crash.
                        if (func_expr.function.bind) {
                            func_expr.bind_info = func_expr.function.bind(context, func_expr.function, func_expr.children);
                        } else {
                            func_expr.bind_info = nullptr;
                        }

                        // If the return type changed, wrap the whole expression in a
                        // cast back to the original type so parent expressions (which
                        // were bound expecting the original type) remain compatible.
                        // E.g., our DECIMAL(18,4) result gets cast to DECIMAL(34,6)
                        // that the parent SUM() was bound with.
                        // NOTE: after this std::move, func_expr/expr refs are invalid.
                        if (original_return_type != replacement_func.return_type) {
                            *expr_ptr = BoundCastExpression::AddCastToType(
                                context, std::move(*expr_ptr), original_return_type);
                        }
                    } catch (...) {
                        // No compatible overload found — keep the original function
                    }
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
