#include "rtbot_sql/analyzer/dml_analyzer.h"

#include <string>
#include <vector>

#include "rtbot_sql/analyzer/expr_validator.h"

namespace rtbot_sql::analyzer {

void analyze_insert(const parser::ast::InsertStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag) {
  // Walk every value expression so unknown-function / arity diagnostics in
  // VALUES (e.g. INSERT INTO t VALUES (NOT_A_FUNC(x))) get surfaced.
  for (const auto& v : stmt.values) {
    validate_expression(v, bag);
  }

  // Look up columns from streams or tables (mirrors handle_insert lookup).
  std::vector<ColumnDef> columns;
  auto it_stream = catalog.streams.find(stmt.table_name);
  if (it_stream != catalog.streams.end()) {
    columns = it_stream->second.columns;
  } else {
    auto it_table = catalog.tables.find(stmt.table_name);
    if (it_table != catalog.tables.end()) {
      columns = it_table->second.columns;
    } else {
      bag.error("INSERT: unknown stream or table: " + stmt.table_name,
                stmt.loc);
      return;
    }
  }

  // Item 2: when INSERT specifies an explicit column list, every named
  // column must exist in the schema. Value count must match the number of
  // *named* columns (not the full schema).
  if (!stmt.columns.empty()) {
    bool all_known = true;
    for (const auto& name : stmt.columns) {
      bool found = false;
      for (const auto& col : columns) {
        if (col.name == name) {
          found = true;
          break;
        }
      }
      if (!found) {
        bag.error(
            "INSERT: column '" + name + "' not found in '" + stmt.table_name +
                "'",
            stmt.loc);
        all_known = false;
      }
    }
    if (!all_known) return;
    if (stmt.values.size() != stmt.columns.size()) {
      bag.error("INSERT: value count mismatch (" +
                    std::to_string(stmt.values.size()) + " values for " +
                    std::to_string(stmt.columns.size()) + " named columns)",
                stmt.loc);
      return;
    }
  } else if (stmt.values.size() != columns.size()) {
    bag.error("INSERT: value count mismatch (" +
                  std::to_string(stmt.values.size()) + " values for " +
                  std::to_string(columns.size()) + " columns)",
              stmt.loc);
    return;
  }

  for (std::size_t i = 0; i < stmt.values.size(); ++i) {
    const auto& col = columns[i];
    const auto& value = stmt.values[i];
    parser::ast::SourceLocation vloc = parser::ast::loc_of(value);

    if (auto* c = std::get_if<parser::ast::Constant>(&value)) {
      (void)c;
      if (col.type == ColumnType::TEXT) {
        bag.error("INSERT: column '" + col.name +
                      "' is TEXT but got a numeric value",
                  vloc);
      }
    } else if (auto* sc = std::get_if<parser::ast::StringConstant>(&value)) {
      (void)sc;
      if (col.type != ColumnType::TEXT) {
        bag.error("INSERT: column '" + col.name +
                      "' is DOUBLE but got a string value",
                  vloc);
      }
    } else {
      bag.error("INSERT values must be constants", vloc);
    }
  }
}

void analyze_delete(const parser::ast::DeleteStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag) {
  // Walk WHERE expression so any unknown-function / arity diagnostics fire.
  if (stmt.where_clause) {
    validate_expression(*stmt.where_clause, bag);
  }

  auto it = catalog.tables.find(stmt.table_name);
  if (it == catalog.tables.end()) {
    bag.error("DELETE: unknown table: " + stmt.table_name, stmt.loc);
    return;
  }
  const auto& table = it->second;
  if (table.key_columns.empty()) {
    bag.error("DELETE: table has no primary key: " + stmt.table_name,
              stmt.loc);
    return;
  }

  if (!stmt.where_clause.has_value()) {
    bag.error("DELETE requires WHERE key_column = value", stmt.loc);
    return;
  }

  const auto* cmp = std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(
      &*stmt.where_clause);
  if (!cmp || (*cmp)->op != "=") {
    bag.error("DELETE WHERE must be key_column = constant",
              parser::ast::loc_of(*stmt.where_clause));
    return;
  }

  const auto* key_const = std::get_if<parser::ast::Constant>(&(*cmp)->right);
  if (!key_const) {
    key_const = std::get_if<parser::ast::Constant>(&(*cmp)->left);
  }
  if (!key_const) {
    bag.error("DELETE WHERE value must be a constant", (*cmp)->loc);
  }
}

void analyze_drop(const parser::ast::DropStmt& stmt,
                  const CatalogSnapshot& catalog, DiagnosticBag& bag) {
  // Mirrors the dependency check at libs/api/src/compiler.cpp:919-941.
  std::vector<std::string> dependents;
  for (const auto& [view_name, view_meta] : catalog.views) {
    if (view_name == stmt.name) continue;
    for (const auto& src : view_meta.source_streams) {
      if (src == stmt.name) {
        dependents.push_back(view_name);
        break;
      }
    }
  }
  if (!dependents.empty()) {
    std::string dep_list;
    for (std::size_t i = 0; i < dependents.size(); ++i) {
      if (i > 0) dep_list += ", ";
      dep_list += dependents[i];
    }
    bag.error("Cannot drop '" + stmt.name + "': referenced by: " + dep_list,
              stmt.loc);
  }
}

}  // namespace rtbot_sql::analyzer
