#pragma once

#include "rtbot_sql/api/types.h"
#include "rtbot_sql/catalog/catalog.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::planner {

// Classify a SELECT statement into an execution tier.
// Throws std::runtime_error for invalid queries (e.g., unbounded stream scan).
SelectTier classify_select(const parser::ast::SelectStmt& stmt,
                           const catalog::Catalog& catalog);

// Query introspection helpers (exposed for testing).
bool has_aggregates(const parser::ast::SelectStmt& stmt);
bool has_windowed_functions(const parser::ast::SelectStmt& stmt);
bool has_group_by(const parser::ast::SelectStmt& stmt);
bool has_where(const parser::ast::SelectStmt& stmt);
bool has_expressions_in_select(const parser::ast::SelectStmt& stmt);
bool is_simple_read(const parser::ast::SelectStmt& stmt);

}  // namespace rtbot_sql::planner
