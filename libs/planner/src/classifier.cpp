#include "rtbot_sql/planner/classifier.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/function_compiler.h"

namespace rtbot_sql::planner {

namespace {

// Check if an expression tree contains any aggregate/windowed function call.
bool expr_has_aggregate(const parser::ast::Expr& expr) {
  if (auto* func_ptr =
          std::get_if<std::unique_ptr<parser::ast::FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    if (compiler::is_aggregate_or_windowed(func.name)) return true;
    for (const auto& arg : func.args) {
      if (expr_has_aggregate(arg)) return true;
    }
    return false;
  }
  if (auto* bin = std::get_if<std::unique_ptr<parser::ast::BinaryExpr>>(&expr)) {
    return expr_has_aggregate((*bin)->left) || expr_has_aggregate((*bin)->right);
  }
  if (auto* cmp = std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&expr)) {
    return expr_has_aggregate((*cmp)->left) || expr_has_aggregate((*cmp)->right);
  }
  if (auto* log = std::get_if<std::unique_ptr<parser::ast::LogicalExpr>>(&expr)) {
    return expr_has_aggregate((*log)->left) || expr_has_aggregate((*log)->right);
  }
  if (auto* not_e = std::get_if<std::unique_ptr<parser::ast::NotExpr>>(&expr)) {
    return expr_has_aggregate((*not_e)->operand);
  }
  return false;
}

// Check if an expression is anything other than a plain column reference.
bool is_computed(const parser::ast::Expr& expr) {
  return !std::holds_alternative<parser::ast::ColumnRef>(expr);
}

// Check if a windowed function (not cumulative aggregate) is present.
bool expr_has_windowed(const parser::ast::Expr& expr) {
  if (auto* func_ptr =
          std::get_if<std::unique_ptr<parser::ast::FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    std::string upper = func.name;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    if (upper == "MOVING_AVERAGE" || upper == "MOVING_SUM" ||
        upper == "MOVING_COUNT" || upper == "MOVING_STD" ||
        upper == "FIR" || upper == "IIR" || upper == "RESAMPLE" ||
        upper == "PEAK_DETECT") {
      return true;
    }
    for (const auto& arg : func.args) {
      if (expr_has_windowed(arg)) return true;
    }
  }
  if (auto* bin = std::get_if<std::unique_ptr<parser::ast::BinaryExpr>>(&expr)) {
    return expr_has_windowed((*bin)->left) || expr_has_windowed((*bin)->right);
  }
  return false;
}

// Extract a key equality filter: WHERE key_col = <constant>
// Returns the constant value if found, nullopt otherwise.
std::optional<double> extract_key_filter(
    const parser::ast::Expr& where_expr,
    const std::string& key_col_name) {
  auto* cmp_ptr =
      std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&where_expr);
  if (!cmp_ptr) return std::nullopt;
  const auto& cmp = **cmp_ptr;
  if (cmp.op != "=") return std::nullopt;

  // col = constant
  auto* left_col = std::get_if<parser::ast::ColumnRef>(&cmp.left);
  auto* right_const = std::get_if<parser::ast::Constant>(&cmp.right);
  if (left_col && right_const && left_col->column_name == key_col_name) {
    return right_const->value;
  }

  // constant = col
  auto* left_const = std::get_if<parser::ast::Constant>(&cmp.left);
  auto* right_col = std::get_if<parser::ast::ColumnRef>(&cmp.right);
  if (left_const && right_col && right_col->column_name == key_col_name) {
    return left_const->value;
  }

  return std::nullopt;
}

}  // namespace

bool has_aggregates(const parser::ast::SelectStmt& stmt) {
  for (const auto& item : stmt.select_list) {
    if (expr_has_aggregate(item.expr)) return true;
  }
  return false;
}

bool has_windowed_functions(const parser::ast::SelectStmt& stmt) {
  for (const auto& item : stmt.select_list) {
    if (expr_has_windowed(item.expr)) return true;
  }
  return false;
}

bool has_group_by(const parser::ast::SelectStmt& stmt) {
  return !stmt.group_by.empty();
}

bool has_where(const parser::ast::SelectStmt& stmt) {
  return stmt.where_clause.has_value();
}

bool has_expressions_in_select(const parser::ast::SelectStmt& stmt) {
  if (stmt.select_list.empty()) return false;  // SELECT *
  for (const auto& item : stmt.select_list) {
    if (is_computed(item.expr)) return true;
  }
  return false;
}

bool is_simple_read(const parser::ast::SelectStmt& stmt) {
  // Simple reads have no WHERE, no GROUP BY, no HAVING
  if (stmt.where_clause.has_value() || !stmt.group_by.empty() ||
      stmt.having.has_value()) {
    return false;
  }

  // SELECT * with LIMIT
  if (stmt.select_list.empty() && stmt.limit.has_value()) return true;

  // Column subset only (no expressions) with LIMIT
  if (!stmt.select_list.empty() && stmt.limit.has_value()) {
    bool all_cols = true;
    for (const auto& item : stmt.select_list) {
      if (!std::holds_alternative<parser::ast::ColumnRef>(item.expr)) {
        all_cols = false;
        break;
      }
    }
    if (all_cols) return true;
  }

  // No aggregates, no GROUP BY, no expressions — just a passthrough
  if (stmt.select_list.empty()) return true;

  return false;
}

SelectTier classify_select(const parser::ast::SelectStmt& stmt,
                           const catalog::Catalog& catalog) {
  auto entity_type = catalog.resolve_entity(stmt.from_table);
  if (!entity_type.has_value()) {
    throw std::runtime_error("unknown source: " + stmt.from_table);
  }

  auto type = *entity_type;

  // TABLE → Tier 1
  if (type == EntityType::TABLE) {
    return SelectTier::TIER1_READ;
  }

  // VIEW → always Tier 3
  if (type == EntityType::VIEW) {
    return SelectTier::TIER3_EPHEMERAL;
  }

  // STREAM without LIMIT → error
  if (type == EntityType::STREAM && !stmt.limit.has_value()) {
    throw std::runtime_error("stream '" + stmt.from_table +
                             "' requires LIMIT or WHERE time bounds");
  }

  // MATERIALIZED_VIEW or STREAM from here on
  if (type == EntityType::MATERIALIZED_VIEW) {
    auto view_meta = catalog.lookup_view(stmt.from_table);
    if (view_meta.has_value() && view_meta->view_type == ViewType::KEYED) {
      // Keyed mat. view + aggregate, no GROUP BY → Tier 2 cross-key agg
      if (has_aggregates(stmt) && !has_group_by(stmt)) {
        return SelectTier::TIER2_SCAN;
      }
      // Key lookup: WHERE key_col = X
      if (stmt.where_clause.has_value() && !view_meta->field_map.empty()) {
        // Find key column name from field_map (index == key_index)
        for (const auto& [name, idx] : view_meta->field_map) {
          if (idx == view_meta->key_index) {
            auto key_val =
                extract_key_filter(*stmt.where_clause, name);
            if (key_val.has_value()) {
              return SelectTier::TIER1_READ;
            }
            break;
          }
        }
      }
    }
  }

  // Simple read
  if (is_simple_read(stmt)) {
    return SelectTier::TIER1_READ;
  }

  // Stateful: aggregates, windowed, GROUP BY
  if (has_aggregates(stmt) || has_windowed_functions(stmt) ||
      has_group_by(stmt)) {
    return SelectTier::TIER3_EPHEMERAL;
  }

  // Stateless filters/expressions
  if (has_where(stmt) || has_expressions_in_select(stmt)) {
    return SelectTier::TIER2_SCAN;
  }

  // Fallback
  return SelectTier::TIER1_READ;
}

}  // namespace rtbot_sql::planner
