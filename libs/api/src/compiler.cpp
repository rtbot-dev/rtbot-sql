#include "rtbot_sql/api/compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <variant>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/compiler/group_by_compiler.h"
#include "rtbot_sql/compiler/select_compiler.h"
#include "rtbot_sql/compiler/where_compiler.h"
#include "rtbot_sql/parser/ast.h"
#include "rtbot_sql/parser/ast_converter.h"
#include "rtbot_sql/parser/parser.h"
#include "rtbot_sql/planner/classifier.h"
#include "rtbot_sql/planner/planner.h"

namespace rtbot_sql::api {

namespace {

CompilationResult make_error(const std::string& msg) {
  CompilationResult r{};
  r.errors.push_back({msg, -1, -1});
  return r;
}

// Build a Scope from a CatalogSnapshot for a given source table.
analyzer::Scope build_scope(const std::string& source,
                            const CatalogSnapshot& catalog) {
  analyzer::Scope scope;

  auto it_stream = catalog.streams.find(source);
  if (it_stream != catalog.streams.end()) {
    scope.register_stream(source, it_stream->second);
    return scope;
  }

  auto it_view = catalog.views.find(source);
  if (it_view != catalog.views.end()) {
    // Build a StreamSchema from the view's field_map
    StreamSchema schema;
    schema.name = it_view->second.name;
    for (const auto& [name, idx] : it_view->second.field_map) {
      schema.columns.push_back({name, idx});
    }
    scope.register_stream(source, schema);
    return scope;
  }

  // TODO: Review table-as-source approach. Currently tables are treated
  // identically to streams at compile time, but at runtime they need a
  // KeyedVariable operator (or similar) to materialize lookups from table
  // state. The compilation produces a valid program graph, but the execution
  // engine cannot wire it correctly yet.
  auto it_table = catalog.tables.find(source);
  if (it_table != catalog.tables.end()) {
    StreamSchema schema{it_table->second.name, it_table->second.columns};
    scope.register_stream(source, schema);
    return scope;
  }

  throw std::runtime_error("unknown source: " + source);
}

// Build a Catalog object from a snapshot (needed by planner).
catalog::Catalog snapshot_to_catalog(const CatalogSnapshot& snap) {
  catalog::Catalog cat;
  for (const auto& [name, schema] : snap.streams) {
    cat.register_stream(name, schema);
  }
  for (const auto& [name, meta] : snap.views) {
    cat.register_view(name, meta);
  }
  for (const auto& [name, schema] : snap.tables) {
    cat.register_table(name, schema);
  }
  return cat;
}

// Compile a SELECT query that needs a full operator graph (Tier 3 or GROUP BY).
CompilationResult compile_select_to_program(
    const parser::ast::SelectStmt& stmt, const CatalogSnapshot& catalog) {
  CompilationResult result{};

  auto scope = build_scope(stmt.from_table, catalog);
  compiler::GraphBuilder builder;

  // Input operator
  builder.add_operator("input_0", "Input");
  compiler::Endpoint current{"input_0", "o1"};

  // WHERE clause
  if (stmt.where_clause.has_value()) {
    current = compiler::compile_where(*stmt.where_clause, current, scope,
                                      builder);
  }

  compiler::FieldMap field_map;

  // GROUP BY
  if (!stmt.group_by.empty()) {
    auto [ep, fm] = compiler::compile_group_by(
        stmt.select_list, stmt.group_by, stmt.having, current, scope, builder);
    current = ep;
    field_map = fm;
  } else {
    // SELECT projection
    auto [ep, fm] =
        compiler::compile_select_projection(stmt.select_list, current, scope,
                                            builder);
    current = ep;
    field_map = fm;
  }

  // Output operator
  builder.add_operator("output_0", "Output");
  builder.connect(current, {"output_0", "i1"});

  result.program_json = builder.to_json();
  result.field_map = field_map;
  result.source_streams = {stmt.from_table};

  // Determine view type
  if (!stmt.group_by.empty()) {
    result.view_type = ViewType::KEYED;
    // key_index from the GROUP BY column
    auto* key_col =
        std::get_if<parser::ast::ColumnRef>(&stmt.group_by[0]);
    if (key_col) {
      auto binding = scope.resolve(*key_col);
      if (auto* b = std::get_if<analyzer::ColumnBinding>(&binding)) {
        result.key_index = b->index;
      }
    }
  } else {
    result.view_type = ViewType::SCALAR;
    result.key_index = -1;
  }

  return result;
}

// Handle CREATE STREAM (via CREATE TABLE).
CompilationResult handle_create_stream(
    const parser::ast::CreateStreamStmt& stmt) {
  CompilationResult result{};
  result.statement_type = StatementType::CREATE_STREAM;
  result.entity_name = stmt.name;

  StreamSchema schema;
  schema.name = stmt.name;
  for (size_t i = 0; i < stmt.columns.size(); ++i) {
    schema.columns.push_back({stmt.columns[i].name, static_cast<int>(i)});
  }
  result.stream_schema = schema;
  return result;
}

// Handle INSERT.
CompilationResult handle_insert(const parser::ast::InsertStmt& stmt) {
  CompilationResult result{};
  result.statement_type = StatementType::INSERT;
  result.entity_name = stmt.table_name;

  for (const auto& val_expr : stmt.values) {
    if (auto* c = std::get_if<parser::ast::Constant>(&val_expr)) {
      result.insert_payload.push_back(c->value);
    } else {
      return make_error("INSERT values must be constants");
    }
  }
  return result;
}

// Handle SELECT.
CompilationResult handle_select(const parser::ast::SelectStmt& stmt,
                                const CatalogSnapshot& catalog) {
  CompilationResult result{};
  result.statement_type = StatementType::SELECT;

  try {
    auto cat = snapshot_to_catalog(catalog);
    auto tier = planner::classify_select(stmt, cat);
    result.select_tier = tier;

    if (tier == SelectTier::TIER3_EPHEMERAL) {
      auto compiled = compile_select_to_program(stmt, catalog);
      result.program_json = compiled.program_json;
      result.field_map = compiled.field_map;
      result.source_streams = compiled.source_streams;
      result.view_type = compiled.view_type;
      result.key_index = compiled.key_index;
    } else {
      // Tier 1/2: populate basic plan info
      auto plan = planner::plan_select(stmt, cat);
      result.field_map = plan.field_map;
      result.source_streams = {stmt.from_table};
    }
  } catch (const std::runtime_error& e) {
    return make_error(e.what());
  }

  return result;
}

// Handle CREATE MATERIALIZED VIEW.
CompilationResult handle_create_mat_view(
    const parser::ast::CreateViewStmt& stmt,
    const CatalogSnapshot& catalog) {
  CompilationResult result{};

  try {
    auto compiled = compile_select_to_program(stmt.query, catalog);
    result = compiled;
    result.statement_type = stmt.materialized
                                ? StatementType::CREATE_MATERIALIZED_VIEW
                                : StatementType::CREATE_VIEW;
    result.entity_name = stmt.name;
  } catch (const std::runtime_error& e) {
    return make_error(e.what());
  }

  return result;
}

// Handle DROP.
CompilationResult handle_drop(const parser::ast::DropStmt& stmt) {
  CompilationResult result{};
  result.statement_type = StatementType::DROP;
  result.drop_entity_name = stmt.name;

  if (stmt.entity_type == "STREAM" || stmt.entity_type == "TABLE")
    result.drop_entity_type = EntityType::STREAM;
  else if (stmt.entity_type == "VIEW")
    result.drop_entity_type = EntityType::VIEW;
  else if (stmt.entity_type == "MATERIALIZED_VIEW")
    result.drop_entity_type = EntityType::MATERIALIZED_VIEW;
  else
    result.drop_entity_type = EntityType::STREAM;

  return result;
}

}  // namespace

CompilationResult compile_sql(const std::string& sql,
                              const CatalogSnapshot& catalog) {
  // Step 1: Parse
  auto parse_result = parser::parse(sql);
  if (!parse_result.ok()) {
    CompilationResult r{};
    for (const auto& err : parse_result.errors) {
      r.errors.push_back({err, -1, -1});
    }
    parser::free_result(parse_result);
    return r;
  }

  // Step 2: Get JSON parse tree and convert to AST
  auto json_result = pg_query_parse(sql.c_str());
  if (json_result.error) {
    auto r = make_error(json_result.error->message
                            ? json_result.error->message
                            : "parse error");
    pg_query_free_parse_result(json_result);
    parser::free_result(parse_result);
    return r;
  }

  parser::ast::Statement stmt;
  try {
    stmt = parser::convert_parse_tree(json_result.parse_tree);
  } catch (const std::exception& e) {
    pg_query_free_parse_result(json_result);
    parser::free_result(parse_result);
    return make_error(e.what());
  }

  pg_query_free_parse_result(json_result);
  parser::free_result(parse_result);

  // Step 3: Dispatch on statement type
  try {
    if (auto* s = std::get_if<parser::ast::SelectStmt>(&stmt)) {
      return handle_select(*s, catalog);
    }
    if (auto* s = std::get_if<parser::ast::CreateStreamStmt>(&stmt)) {
      return handle_create_stream(*s);
    }
    if (auto* s = std::get_if<parser::ast::CreateViewStmt>(&stmt)) {
      return handle_create_mat_view(*s, catalog);
    }
    if (auto* s = std::get_if<parser::ast::InsertStmt>(&stmt)) {
      return handle_insert(*s);
    }
    if (auto* s = std::get_if<parser::ast::DropStmt>(&stmt)) {
      return handle_drop(*s);
    }
    return make_error("unsupported statement type");
  } catch (const std::exception& e) {
    return make_error(e.what());
  }
}

}  // namespace rtbot_sql::api
