#include "rtbot_sql/planner/planner.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/planner/classifier.h"

namespace rtbot_sql::planner {

namespace {

// Resolve the schema for a source entity.
StreamSchema resolve_schema(const std::string& source,
                            const catalog::Catalog& catalog) {
  auto stream = catalog.lookup_stream(source);
  if (stream.has_value()) return *stream;

  auto view = catalog.lookup_view(source);
  if (view.has_value()) {
    // Construct a StreamSchema from the view's field_map.
    StreamSchema schema;
    schema.name = view->name;
    for (const auto& [name, idx] : view->field_map) {
      schema.columns.push_back({name, idx});
    }
    return schema;
  }

  auto table = catalog.lookup_table(source);
  if (table.has_value()) return {table->name, table->columns};

  throw std::runtime_error("cannot resolve schema for: " + source);
}

// Default alias for an expression.
std::string default_alias(const parser::ast::Expr& expr, int index) {
  if (auto* col = std::get_if<parser::ast::ColumnRef>(&expr)) {
    return col->column_name;
  }
  return "expr_" + std::to_string(index);
}

}  // namespace

SelectPlan plan_select(const parser::ast::SelectStmt& stmt,
                       const catalog::Catalog& catalog) {
  SelectPlan plan;
  plan.tier = classify_select(stmt, catalog);

  switch (plan.tier) {
    case SelectTier::TIER1_READ: {
      plan.read_stream = stmt.from_table;
      plan.limit = stmt.limit.value_or(-1);

      // Validate column names in SELECT list
      if (!stmt.select_list.empty()) {
        auto schema = resolve_schema(stmt.from_table, catalog);
        for (size_t i = 0; i < stmt.select_list.size(); ++i) {
          auto* col = std::get_if<parser::ast::ColumnRef>(
              &stmt.select_list[i].expr);
          if (col) {
            auto idx = schema.column_index(col->column_name);
            if (!idx.has_value()) {
              std::string avail;
              for (const auto& c : schema.columns) {
                if (!avail.empty()) avail += ", ";
                avail += c.name;
              }
              throw std::runtime_error(
                  "Column '" + col->column_name + "' not found in '" +
                  stmt.from_table + "'. Available: " + avail);
            }
            std::string alias = stmt.select_list[i].alias.value_or(
                col->column_name);
            plan.field_map[alias] = *idx;
          }
        }
      }

      // Check for key filter (WHERE key = X on keyed views)
      if (stmt.where_clause.has_value()) {
        auto view = catalog.lookup_view(stmt.from_table);
        if (view.has_value() && view->view_type == ViewType::KEYED) {
          for (const auto& [name, idx] : view->field_map) {
            if (idx == view->key_index) {
              auto* cmp_ptr = std::get_if<
                  std::unique_ptr<parser::ast::ComparisonExpr>>(
                  &*stmt.where_clause);
              if (cmp_ptr) {
                const auto& cmp = **cmp_ptr;
                if (cmp.op == "=") {
                  auto* left_col =
                      std::get_if<parser::ast::ColumnRef>(&cmp.left);
                  auto* right_const =
                      std::get_if<parser::ast::Constant>(&cmp.right);
                  if (left_col && right_const &&
                      left_col->column_name == name) {
                    plan.key_filter = right_const->value;
                  }
                  auto* left_const =
                      std::get_if<parser::ast::Constant>(&cmp.left);
                  auto* right_col =
                      std::get_if<parser::ast::ColumnRef>(&cmp.right);
                  if (left_const && right_col &&
                      right_col->column_name == name) {
                    plan.key_filter = left_const->value;
                  }
                }
              }
              break;
            }
          }
        }
      }
      break;
    }

    case SelectTier::TIER2_SCAN: {
      plan.scan_stream = stmt.from_table;
      auto schema = resolve_schema(stmt.from_table, catalog);

      // Check if this is a cross-key aggregation over a keyed materialized view.
      auto view_meta = catalog.lookup_view(stmt.from_table);
      bool is_keyed_view_agg = view_meta.has_value() &&
                               view_meta->view_type == ViewType::KEYED &&
                               !stmt.select_list.empty();

      if (is_keyed_view_agg) {
        // Validate all SELECT items are aggregate functions.
        plan.is_cross_key = true;
        for (size_t i = 0; i < stmt.select_list.size(); ++i) {
          const auto& item = stmt.select_list[i];
          auto* func_ptr =
              std::get_if<std::unique_ptr<parser::ast::FuncCall>>(&item.expr);
          if (!func_ptr) {
            throw std::runtime_error(
                "cross-key aggregation: SELECT items must be aggregate functions");
          }
          const auto& func = **func_ptr;
          std::string upper = func.name;
          std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

          if (upper != "SUM" && upper != "COUNT" && upper != "AVG" &&
              upper != "MIN" && upper != "MAX") {
            throw std::runtime_error(
                "cross-key aggregation: unsupported function " + func.name +
                " (use SUM, COUNT, AVG, MIN, MAX)");
          }

          CrossKeyAgg agg;
          agg.func = upper;
          agg.alias = item.alias.value_or(
              default_alias(item.expr, static_cast<int>(i)));

          // Resolve column index for the aggregate argument.
          if (upper == "COUNT") {
            agg.col_index = -1;  // COUNT(*) — no column needed
          } else {
            if (func.args.size() != 1) {
              throw std::runtime_error(upper + " requires exactly 1 argument");
            }
            auto* col_ref =
                std::get_if<parser::ast::ColumnRef>(&func.args[0]);
            if (!col_ref) {
              throw std::runtime_error(
                  upper + ": argument must be a column reference");
            }
            auto idx = schema.column_index(col_ref->column_name);
            if (!idx.has_value()) {
              throw std::runtime_error("unknown column: " + col_ref->column_name);
            }
            agg.col_index = *idx;
          }

          plan.field_map[agg.alias] = static_cast<int>(i);
          plan.cross_key_aggs.push_back(std::move(agg));
        }
      } else {
        // Standard row scan.

        // Compile WHERE predicate
        if (stmt.where_clause.has_value()) {
          plan.where_predicate =
              compile_for_eval(*stmt.where_clause, schema);
        }

        // Compile SELECT expressions
        if (stmt.select_list.empty()) {
          // SELECT * — identity, no compiled exprs needed
          for (const auto& col : schema.columns) {
            plan.field_map[col.name] = col.index;
          }
        } else {
          for (size_t i = 0; i < stmt.select_list.size(); ++i) {
            const auto& item = stmt.select_list[i];
            plan.select_exprs.push_back(
                compile_for_eval(item.expr, schema));
            std::string alias =
                item.alias.value_or(default_alias(item.expr, static_cast<int>(i)));
            plan.field_map[alias] = static_cast<int>(i);
          }
        }
      }

      plan.limit = stmt.limit.value_or(-1);
      break;
    }

    case SelectTier::TIER3_EPHEMERAL: {
      plan.needs_compilation = true;
      break;
    }
  }

  return plan;
}

}  // namespace rtbot_sql::planner
