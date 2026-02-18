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

// Describes a single aggregate to compute across all rows (cross-key Tier 2).
struct CrossKeyAgg {
  std::string func;    // "SUM", "COUNT", "AVG", "MIN", "MAX"
  int col_index = -1;  // source column in the view's schema (-1 for COUNT(*))
  std::string alias;   // output field name
};

struct SelectPlan {
  SelectTier tier;

  // Tier 1
  std::string read_stream;
  int limit = -1;
  std::optional<double> key_filter;

  // Tier 2 (row scan)
  std::unique_ptr<CompiledExpr> where_predicate;
  std::vector<std::unique_ptr<CompiledExpr>> select_exprs;
  std::map<std::string, int> field_map;
  std::string scan_stream;

  // Tier 2 (cross-key aggregation over a keyed materialized view)
  bool is_cross_key = false;
  std::vector<CrossKeyAgg> cross_key_aggs;

  // Tier 3
  bool needs_compilation = false;
};

// Build an execution plan for a SELECT statement.
SelectPlan plan_select(const parser::ast::SelectStmt& stmt,
                       const catalog::Catalog& catalog);

}  // namespace rtbot_sql::planner
