#include "duckdb_operator_replacement.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_binder.hpp"

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
        
        // Look for BoundFunctionExpression and check if it's in our replacement registry.
        // In DuckDB 2.0 comparisons and casts are function expressions too, so
        // registering e.g. "=" works as well.
        if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
            return;
        }
        auto &func_expr = expr.Cast<BoundFunctionExpression>();
        auto it = replacement_registry.find(func_expr.Function().GetName().GetIdentifierName());
        if (it == replacement_registry.end()) {
            return;
        }

        // Look up the replacement function in the system catalog
        auto &catalog = Catalog::GetSystemCatalog(context);
        auto func_entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, Identifier::DefaultSchema(),
                                           Identifier(it->second), OnEntryNotFound::RETURN_NULL);
        if (!func_entry || func_entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
            return;
        }
        auto &scalar_func = func_entry->Cast<ScalarFunctionCatalogEntry>();

        // Bind the replacement on copies of the arguments, so the original
        // expression stays intact if binding fails. The binder picks the
        // overload, adds casts on arguments whose types differ (e.g. a
        // DECIMAL(15,2) to DECIMAL(18,4) scale change) and runs the
        // function's bind callback. Skip the replacement if no compatible
        // overload exists (e.g. intermediate DECIMAL types wider than our
        // registered functions handle, or DATE/INTERVAL arguments).
        vector<unique_ptr<Expression>> children;
        for (auto &child : func_expr.GetChildren()) {
            children.push_back(child->Copy());
        }
        ErrorData error;
        unique_ptr<Expression> replacement;
        try {
            replacement = FunctionBinder(context).BindScalarFunction(scalar_func, std::move(children), error,
                                                                     func_expr.IsOperator());
        } catch (...) {
            return;
        }
        if (!replacement || error.HasError()) {
            return;
        }

        // Parent expressions were bound expecting the original return type, so
        // cast back if it changed. E.g. our DECIMAL(18,4) result gets cast to
        // the DECIMAL(34,6) that the parent SUM() was bound with.
        // AddCastToType is a no-op when the types already match.
        auto original_return_type = func_expr.GetReturnType();
        *expr_ptr = BoundCastExpression::AddCastToType(context, std::move(replacement), original_return_type);
    });

    // Recursively process children
    for (auto &child : op->children) {
        ReplaceOperators(context, child);
    }
}

} // namespace duckdb
