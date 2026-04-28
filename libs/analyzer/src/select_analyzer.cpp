#include "rtbot_sql/analyzer/select_analyzer.h"

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <variant>

#include "rtbot_sql/analyzer/expr_validator.h"
#include "rtbot_sql/analyzer/scope.h"

namespace rtbot_sql::analyzer {

namespace {

// Mirrors lookup_schema in libs/api/src/compiler.cpp — resolves a source
// name (stream / view / table) into a StreamSchema for column resolution.
// Returns nullopt if the source is not in the catalog.
std::optional<StreamSchema> lookup_schema(const std::string& source,
                                          const CatalogSnapshot& catalog) {
  auto it_stream = catalog.streams.find(source);
  if (it_stream != catalog.streams.end()) return it_stream->second;
  auto it_view = catalog.views.find(source);
  if (it_view != catalog.views.end()) {
    StreamSchema schema;
    schema.name = it_view->second.name;
    std::vector<std::pair<std::string, int>> sorted_entries(
        it_view->second.field_map.begin(), it_view->second.field_map.end());
    std::sort(sorted_entries.begin(), sorted_entries.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    for (int i = 0; i < static_cast<int>(sorted_entries.size()); ++i) {
      schema.columns.push_back({sorted_entries[i].first, i});
    }
    return schema;
  }
  auto it_table = catalog.tables.find(source);
  if (it_table != catalog.tables.end()) {
    return StreamSchema{it_table->second.name, it_table->second.columns};
  }
  return std::nullopt;
}

// Build a Scope for an analyze_select pass. Returns nullopt when no scope
// can be built — caller skips column-resolution checks (the compiler will
// surface them via its own paths if anything breaks).
std::optional<Scope> build_select_scope(const parser::ast::SelectStmt& stmt,
                                        const CatalogSnapshot& catalog) {
  Scope scope;
  // Multi-FROM (cross-select): register every source.
  if (stmt.from_tables.size() > 1) {
    for (const auto& src : stmt.from_tables) {
      auto schema = lookup_schema(src.table_name, catalog);
      if (!schema) return std::nullopt;
      scope.register_stream(src.table_name, *schema,
                            src.alias.empty() ? "" : src.alias);
    }
    return scope;
  }
  // Single FROM (with optional JOINs). Register only the left/main source —
  // mirroring the scope built in compile_table_join. JOIN target columns are
  // pattern-matched in the ON condition (never resolved through Scope), and
  // skipping ON-condition resolution in analyze_select keeps wording
  // consistent with the compiler's behaviour.
  if (stmt.from_table.empty()) return std::nullopt;
  auto schema = lookup_schema(stmt.from_table, catalog);
  if (!schema) return std::nullopt;
  scope.register_stream(stmt.from_table, *schema,
                        stmt.from_alias.empty() ? "" : stmt.from_alias);
  return scope;
}

// True if `name` is one of the entity types that needs LIMIT/time bounds
// when used as a stream source. Mirrors the EntityType::STREAM branch in
// libs/planner/src/classifier.cpp.
bool source_is_stream(const std::string& name, const CatalogSnapshot& catalog) {
  return catalog.streams.find(name) != catalog.streams.end();
}

// True if `name` resolves to a non-materialized VIEW. Mirrors the
// EntityType::VIEW branch in classify_select.
bool source_is_view(const std::string& name, const CatalogSnapshot& catalog) {
  auto it = catalog.views.find(name);
  if (it == catalog.views.end()) return false;
  return it->second.entity_type == EntityType::VIEW;
}

// Mirrors compiler::is_aggregate_name in alias_expander.cpp. Used to detect
// SUM/COUNT/AVG/MIN/MAX inside WHERE clauses.
bool is_aggregate_name(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return upper == "SUM" || upper == "COUNT" || upper == "AVG" ||
         upper == "MIN" || upper == "MAX";
}

// True for windowed function names. Mirrors the windowed branch of
// compiler::is_aggregate_or_windowed in libs/compiler/src/function_compiler.cpp.
bool is_windowed_function_name(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return upper == "MOVING_AVERAGE" || upper == "MOVING_SUM" ||
         upper == "MOVING_COUNT" || upper == "MOVING_STD" ||
         upper == "STDDEV" || upper == "MOVING_MIN" ||
         upper == "MOVING_MAX" || upper == "FIR" || upper == "IIR" ||
         upper == "RESAMPLE" || upper == "PEAK_DETECT";
}

// Returns true if any windowed function call appears anywhere in the
// expression tree. Mirrors compiler::expr_has_windowed.
bool expr_has_windowed(const parser::ast::Expr& expr) {
  using namespace parser::ast;
  return std::visit(
      [](const auto& v) -> bool {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          if (!v) return false;
          if (is_windowed_function_name(v->name)) return true;
          for (const auto& arg : v->args) {
            if (expr_has_windowed(arg)) return true;
          }
          return false;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BinaryExpr>> ||
                             std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>> ||
                             std::is_same_v<T, std::unique_ptr<LogicalExpr>>) {
          return v && (expr_has_windowed(v->left) ||
                        expr_has_windowed(v->right));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          return v && expr_has_windowed(v->operand);
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BetweenExpr>>) {
          return v && (expr_has_windowed(v->expr) ||
                        expr_has_windowed(v->low) ||
                        expr_has_windowed(v->high));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          if (!v) return false;
          for (const auto& wc : v->when_clauses) {
            if (expr_has_windowed(wc.condition) ||
                expr_has_windowed(wc.result))
              return true;
          }
          if (v->else_result.has_value()) {
            return expr_has_windowed(*v->else_result);
          }
          return false;
        } else {
          (void)v;
          return false;
        }
      },
      expr);
}

// Map of SELECT alias name → defining expression. Used to detect
// aggregates that are reachable only through SELECT-list alias expansion
// (e.g. `WHERE my_sum > 0` where `my_sum = SUM(x)`).
using AliasMap = std::map<std::string, const parser::ast::Expr*>;

bool expr_has_aggregate_resolving_aliases(
    const parser::ast::Expr& expr, const AliasMap& aliases,
    std::set<std::string>& in_progress);

// Recursive worker. Visits every sub-expression. For unqualified
// ColumnRefs whose name matches a SELECT alias, follows the alias's
// defining expression (with cycle detection). Mirrors what
// compiler::expand_aliases + compiler::expr_has_aggregate produce together.
bool expr_has_aggregate_resolving_aliases(
    const parser::ast::Expr& expr, const AliasMap& aliases,
    std::set<std::string>& in_progress) {
  using namespace parser::ast;
  return std::visit(
      [&](const auto& v) -> bool {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, ColumnRef>) {
          // Aliases live in the unqualified namespace; `t.col` is always
          // a real column, never an alias.
          if (!v.table_alias.empty()) return false;
          auto it = aliases.find(v.column_name);
          if (it == aliases.end()) return false;
          // Cycle break: if we're already resolving this alias, the chain
          // is malformed — treat as no-aggregate to avoid infinite recursion.
          if (in_progress.count(v.column_name)) return false;
          in_progress.insert(v.column_name);
          bool result = expr_has_aggregate_resolving_aliases(
              *it->second, aliases, in_progress);
          in_progress.erase(v.column_name);
          return result;
        } else if constexpr (std::is_same_v<T, Constant> ||
                             std::is_same_v<T, StringConstant> ||
                             std::is_same_v<T, ArrayLiteral>) {
          (void)v;
          return false;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          if (!v) return false;
          if (is_aggregate_name(v->name)) return true;
          for (const auto& arg : v->args) {
            if (expr_has_aggregate_resolving_aliases(arg, aliases, in_progress))
              return true;
          }
          return false;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BinaryExpr>> ||
                             std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>> ||
                             std::is_same_v<T, std::unique_ptr<LogicalExpr>>) {
          return v && (expr_has_aggregate_resolving_aliases(
                           v->left, aliases, in_progress) ||
                        expr_has_aggregate_resolving_aliases(
                            v->right, aliases, in_progress));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          return v && expr_has_aggregate_resolving_aliases(
                          v->operand, aliases, in_progress);
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BetweenExpr>>) {
          return v &&
                 (expr_has_aggregate_resolving_aliases(v->expr, aliases,
                                                        in_progress) ||
                  expr_has_aggregate_resolving_aliases(v->low, aliases,
                                                        in_progress) ||
                  expr_has_aggregate_resolving_aliases(v->high, aliases,
                                                        in_progress));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          if (!v) return false;
          for (const auto& wc : v->when_clauses) {
            if (expr_has_aggregate_resolving_aliases(wc.condition, aliases,
                                                      in_progress) ||
                expr_has_aggregate_resolving_aliases(wc.result, aliases,
                                                      in_progress))
              return true;
          }
          if (v->else_result.has_value()) {
            return expr_has_aggregate_resolving_aliases(*v->else_result,
                                                         aliases, in_progress);
          }
          return false;
        }
        return false;
      },
      expr);
}

// Mirrors compiler::expr_has_aggregate in alias_expander.cpp. Returns true
// if any SUM/COUNT/AVG/MIN/MAX appears anywhere in the expression tree.
bool expr_has_aggregate(const parser::ast::Expr& expr) {
  using namespace parser::ast;
  return std::visit(
      [](const auto& v) -> bool {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, ColumnRef> ||
                      std::is_same_v<T, Constant> ||
                      std::is_same_v<T, StringConstant> ||
                      std::is_same_v<T, ArrayLiteral>) {
          (void)v;
          return false;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          if (!v) return false;
          if (is_aggregate_name(v->name)) return true;
          for (const auto& arg : v->args) {
            if (expr_has_aggregate(arg)) return true;
          }
          return false;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BinaryExpr>>) {
          return v && (expr_has_aggregate(v->left) ||
                        expr_has_aggregate(v->right));
        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>>) {
          return v && (expr_has_aggregate(v->left) ||
                        expr_has_aggregate(v->right));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<LogicalExpr>>) {
          return v && (expr_has_aggregate(v->left) ||
                        expr_has_aggregate(v->right));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          return v && expr_has_aggregate(v->operand);
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BetweenExpr>>) {
          return v && (expr_has_aggregate(v->expr) ||
                        expr_has_aggregate(v->low) ||
                        expr_has_aggregate(v->high));
        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          if (!v) return false;
          for (const auto& wc : v->when_clauses) {
            if (expr_has_aggregate(wc.condition) ||
                expr_has_aggregate(wc.result))
              return true;
          }
          if (v->else_result.has_value()) {
            return expr_has_aggregate(*v->else_result);
          }
          return false;
        }
        return false;
      },
      expr);
}

}  // namespace

namespace {

// Check whether a name resolves to any catalog entity (stream/view/table).
// Mirrors catalog::Catalog::resolve_entity used in planner/classifier.cpp.
bool source_exists(const std::string& name, const CatalogSnapshot& catalog) {
  return catalog.streams.find(name) != catalog.streams.end() ||
         catalog.views.find(name) != catalog.views.end() ||
         catalog.tables.find(name) != catalog.tables.end();
}

}  // namespace

void analyze_select(const parser::ast::SelectStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag,
                    bool top_level, std::string_view sql) {
  // Source existence: catch "unknown source" at the top-level FROM before
  // any expression validation so users see the most relevant error first.
  // Mirrors libs/planner/src/classifier.cpp:163. Use the FROM source's
  // location if available — SelectStmt itself has no libpg_query location.
  if (!stmt.from_table.empty() && stmt.from_tables.size() <= 1 &&
      !source_exists(stmt.from_table, catalog)) {
    parser::ast::SourceLocation from_loc =
        stmt.from_tables.empty() ? stmt.loc : stmt.from_tables[0].loc;
    bag.error("unknown source: " + stmt.from_table, from_loc);
  }

  // Build a Scope for column resolution. nullopt when sources can't be
  // resolved (e.g. unknown source — already reported above).
  auto scope_opt = build_select_scope(stmt, catalog);
  const Scope* scope = scope_opt ? &*scope_opt : nullptr;

  // Collect SELECT-list alias names. The compiler's alias-expander
  // substitutes references to these in WHERE / HAVING / GROUP BY / ORDER BY
  // before column resolution, so the analyzer must skip resolving them.
  // Item 4: also detect duplicates — `SELECT x AS y, z AS y FROM t` would
  // silently shadow the first `y`, almost always a typo.
  std::set<std::string> alias_set;
  std::set<std::string> seen_aliases;
  for (const auto& item : stmt.select_list) {
    if (!item.alias.has_value()) continue;
    if (seen_aliases.count(*item.alias)) {
      bag.error("duplicate alias '" + *item.alias + "' in SELECT list",
                item.loc);
    } else {
      seen_aliases.insert(*item.alias);
    }
    alias_set.insert(*item.alias);
  }
  const std::set<std::string>* aliases =
      alias_set.empty() ? nullptr : &alias_set;

  // SELECT items can reference other items' aliases (alias chaining), but
  // not their own (to prevent self-reference). Mirrors the `excl` parameter
  // in compiler::expand_aliases.
  for (const auto& item : stmt.select_list) {
    std::set<std::string> item_aliases = alias_set;
    if (item.alias.has_value()) item_aliases.erase(*item.alias);
    const std::set<std::string>* item_aliases_ptr =
        item_aliases.empty() ? nullptr : &item_aliases;
    validate_expression(item.expr, bag, scope, item_aliases_ptr, sql);
  }

  // JOIN ON conditions: validate the expression tree (function arity etc.)
  // but skip column resolution — the JOIN target's columns aren't in the
  // analyzer's left-only scope, and the compiler resolves them by pattern-
  // matching the ON expression structure rather than via Scope.
  for (const auto& join : stmt.join_clauses) {
    if (join.on_condition) {
      validate_expression(*join.on_condition, bag, /*scope=*/nullptr,
                          /*aliases=*/nullptr, sql);
    }
  }

  // WHERE — predicate-shape rules apply (NOT/BETWEEN/two-constant comparison).
  // Aggregates (SUM/COUNT/AVG/MIN/MAX) belong in HAVING, not WHERE — both
  // raw (`WHERE SUM(x) > 0`) and reachable through SELECT-list alias
  // expansion (`SELECT SUM(x) AS s FROM t WHERE s > 0`).
  // Mirrors libs/api/src/compiler.cpp:179, 276.
  if (stmt.where_clause) {
    AliasMap alias_map;
    for (const auto& item : stmt.select_list) {
      if (item.alias.has_value()) alias_map[*item.alias] = &item.expr;
    }
    std::set<std::string> in_progress;
    if (expr_has_aggregate_resolving_aliases(*stmt.where_clause, alias_map,
                                              in_progress)) {
      bag.error("aggregate function not allowed in WHERE clause; use HAVING",
                parser::ast::loc_of(*stmt.where_clause));
    }
    validate_predicate(*stmt.where_clause, bag, "WHERE", scope, aliases, sql);
  }

  // GROUP BY
  for (const auto& g : stmt.group_by) {
    validate_expression(g, bag, scope, aliases, sql);
  }

  // GROUP BY with multi-FROM is not yet supported.
  // Mirrors libs/api/src/compiler.cpp:181-182.
  if (!stmt.group_by.empty() && stmt.from_tables.size() > 1) {
    bag.error("GROUP BY with multiple FROM sources is not yet supported",
              parser::ast::loc_of(stmt.group_by[0]));
  }

  // JOIN target tables must exist as TABLE entities.
  // Mirrors libs/api/src/compiler.cpp:697 ("JOIN: unknown table") and
  // libs/api/src/compiler.cpp:823 ("JOIN: unsupported join target type").
  for (const auto& jc : stmt.join_clauses) {
    if (jc.table_name.empty()) continue;
    auto it_table = catalog.tables.find(jc.table_name);
    if (it_table != catalog.tables.end()) {
      // Valid TABLE target. Validate the ON condition.
      if (jc.on_condition.has_value()) {
        const auto* cmp = std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(
            &*jc.on_condition);

        auto left_schema = lookup_schema(stmt.from_table, catalog);
        const auto& target_schema = it_table->second.columns;

        // Check whether a ColumnRef belongs to the left stream and the
        // referenced column actually exists there. If qualified with the
        // wrong alias, considered "not left".
        auto belongs_left = [&](const parser::ast::ColumnRef* ref) -> bool {
          if (!ref) return false;
          if (!ref->table_alias.empty() &&
              ref->table_alias != stmt.from_alias &&
              ref->table_alias != stmt.from_table) {
            return false;
          }
          if (!left_schema) return false;
          return left_schema->column_index(ref->column_name).has_value();
        };
        auto belongs_target = [&](const parser::ast::ColumnRef* ref) -> bool {
          if (!ref) return false;
          // Must be qualified with the JOIN target's name or alias.
          if (ref->table_alias != jc.table_name &&
              ref->table_alias != jc.table_alias) {
            return false;
          }
          for (const auto& col : target_schema) {
            if (col.name == ref->column_name) return true;
          }
          return false;
        };

        bool structural_ok = false;
        if (cmp && (*cmp)->op == "=") {
          const auto* lref =
              std::get_if<parser::ast::ColumnRef>(&(*cmp)->left);
          const auto* rref =
              std::get_if<parser::ast::ColumnRef>(&(*cmp)->right);

          // For a valid ON, one side resolves to left, the other side
          // resolves to the JOIN target (or is unqualified — defer).
          structural_ok = belongs_left(lref) || belongs_left(rref);

          // Item 1: validate qualified refs that point at the JOIN target.
          // If a side is qualified `target_name.col` or `target_alias.col`,
          // verify the column exists in the target schema.
          auto check_target_qualified =
              [&](const parser::ast::ColumnRef* ref) {
                if (!ref) return;
                if (ref->table_alias.empty()) return;
                if (ref->table_alias != jc.table_name &&
                    ref->table_alias != jc.table_alias) {
                  return;
                }
                for (const auto& col : target_schema) {
                  if (col.name == ref->column_name) return;
                }
                bag.error(
                    "JOIN: column '" + ref->table_alias + "." +
                        ref->column_name + "' not found in '" + jc.table_name +
                        "'",
                    ref->loc);
              };
          check_target_qualified(lref);
          check_target_qualified(rref);

          // Symmetric: if a side is qualified with the LEFT stream's
          // name/alias, verify the column exists in the left schema too.
          auto check_left_qualified =
              [&](const parser::ast::ColumnRef* ref) {
                if (!ref) return;
                if (ref->table_alias.empty()) return;
                if (ref->table_alias != stmt.from_table &&
                    ref->table_alias != stmt.from_alias) {
                  return;
                }
                if (!left_schema) return;
                if (left_schema->column_index(ref->column_name).has_value())
                  return;
                bag.error(
                    "JOIN: column '" + ref->table_alias + "." +
                        ref->column_name + "' not found in '" +
                        stmt.from_table + "'",
                    ref->loc);
              };
          check_left_qualified(lref);
          check_left_qualified(rref);
          (void)belongs_target;  // reserved for future use
        }
        if (!structural_ok) {
          bag.error(
              "JOIN: could not resolve join column from ON condition for table: " +
                  jc.table_name,
              jc.on_condition ? parser::ast::loc_of(*jc.on_condition) : jc.loc);
        }
      }
      continue;
    }
    if (catalog.streams.find(jc.table_name) != catalog.streams.end() ||
        catalog.views.find(jc.table_name) != catalog.views.end()) {
      // Resolves to something, but not a TABLE — only TABLE joins supported.
      bag.error("JOIN: unsupported join target type for: " + jc.table_name +
                    " (only TABLE joins are supported)",
                jc.loc);
    } else {
      bag.error("JOIN: unknown table: " + jc.table_name, jc.loc);
    }
  }

  // HAVING — predicate-shape rules apply with HAVING-context wording.
  if (stmt.having) {
    validate_predicate(*stmt.having, bag, "HAVING", scope, aliases, sql);
  }

  // ORDER BY items
  for (const auto& ob : stmt.order_by) {
    validate_expression(ob.expr, bag, scope, aliases, sql);
  }

  // SELECT FROM VIEW (non-materialized) requires LIMIT.
  // Mirrors libs/api/src/compiler.cpp:858. Top-level only — view
  // definitions can wrap unbounded SELECTs.
  if (top_level && !stmt.from_table.empty() && stmt.from_tables.size() <= 1 &&
      stmt.join_clauses.empty() &&
      source_is_view(stmt.from_table, catalog) && !stmt.limit.has_value()) {
    parser::ast::SourceLocation from_loc =
        stmt.from_tables.empty() ? stmt.loc : stmt.from_tables[0].loc;
    bag.error("SELECT FROM VIEW '" + stmt.from_table +
                  "' requires LIMIT or WHERE time bounds",
              from_loc);
  }

  // Stream sources require LIMIT (or aggregates / windowed / GROUP BY /
  // ORDER BY which create stateful pipelines that don't need bounding).
  // Mirrors libs/planner/src/classifier.cpp:185-188 — only enforced for
  // top-level SELECTs (CREATE [MATERIALIZED] VIEW wraps stateful pipelines
  // and doesn't go through classify_select).
  if (top_level && !stmt.from_table.empty() && stmt.from_tables.size() <= 1 &&
      stmt.join_clauses.empty() &&
      source_is_stream(stmt.from_table, catalog) && !stmt.limit.has_value() &&
      stmt.group_by.empty() && stmt.order_by.empty()) {
    bool has_agg = false;
    bool has_win = false;
    for (const auto& item : stmt.select_list) {
      if (expr_has_aggregate(item.expr)) has_agg = true;
      if (expr_has_windowed(item.expr)) has_win = true;
    }
    if (!has_agg && !has_win) {
      parser::ast::SourceLocation from_loc =
          stmt.from_tables.empty() ? stmt.loc : stmt.from_tables[0].loc;
      bag.error("stream '" + stmt.from_table +
                    "' requires LIMIT or WHERE time bounds",
                from_loc);
    }
  }

  // ORDER BY requires LIMIT in streaming context.
  // Mirrors libs/api/src/compiler.cpp:177, 301.
  if (!stmt.order_by.empty() && !stmt.limit.has_value()) {
    bag.error("ORDER BY requires LIMIT in streaming context",
              stmt.order_by[0].loc);
  }

  // Item 9: LIMIT must be a positive integer. LIMIT 0 is pointless;
  // LIMIT -1 would silently cast to a huge unsigned at runtime.
  if (stmt.limit.has_value() && *stmt.limit <= 0) {
    parser::ast::SourceLocation loc = stmt.limit_loc;
    if (loc.line == -1) loc = stmt.loc;
    bag.error("LIMIT must be a positive integer", loc);
  }

  // ORDER BY column must be a ColumnRef whose name is in SELECT (alias or
  // unaliased default). Mirrors libs/api/src/compiler.cpp:198, 370.
  // Only fires when LIMIT is present (so the TopK path actually runs).
  if (!stmt.order_by.empty() && stmt.limit.has_value() &&
      !stmt.select_list.empty()) {
    const auto& ob = stmt.order_by[0];
    auto* col = std::get_if<parser::ast::ColumnRef>(&ob.expr);
    if (col) {
      bool found = false;
      for (const auto& item : stmt.select_list) {
        std::string item_name;
        if (item.alias.has_value()) {
          item_name = *item.alias;
        } else if (auto* ic = std::get_if<parser::ast::ColumnRef>(&item.expr)) {
          item_name = ic->column_name;
        }
        if (item_name == col->column_name) {
          found = true;
          break;
        }
      }
      if (!found) {
        bag.error("ORDER BY column not found in SELECT list", ob.loc);
      }
    }
  }
}

}  // namespace rtbot_sql::analyzer
