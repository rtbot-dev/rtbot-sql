#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rtbot_sql/api/types.h"
#include "rtbot_sql/catalog/catalog.h"
#include "rtbot_sql/parser/ast.h"
#include "rtbot_sql/planner/evaluator.h"

namespace rtbot_sql::planner {

struct SelectPlan {
  SelectTier tier;

  // Tier 1
  std::string read_stream;
  int limit = -1;
  std::optional<double> key_filter;

  // Tier 2
  std::unique_ptr<CompiledExpr> where_predicate;
  std::vector<std::unique_ptr<CompiledExpr>> select_exprs;
  std::map<std::string, int> field_map;
  std::string scan_stream;

  // Tier 3
  bool needs_compilation = false;
};

// Build an execution plan for a SELECT statement.
SelectPlan plan_select(const parser::ast::SelectStmt& stmt,
                       const catalog::Catalog& catalog);

}  // namespace rtbot_sql::planner
