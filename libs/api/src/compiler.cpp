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

  // Validate the generated graph before serializing
  auto validation_errors = builder.validate();
  if (!validation_errors.empty()) {
    CompilationResult err_result{};
    for (const auto& msg : validation_errors) {
      err_result.errors.push_back({"graph validation: " + msg, -1, -1});
    }
    return err_result;
  }

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

// Forward declaration — defined below after handle_create_mat_view.
CompilationResult handle_select_from_view(const parser::ast::SelectStmt& stmt,
                                          const ViewMeta& view_meta,
                                          const CatalogSnapshot& catalog);

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
      // SELECT FROM VIEW: augment the stored graph instead of compiling fresh.
      auto entity_type = cat.resolve_entity(stmt.from_table);
      if (entity_type == EntityType::VIEW) {
        auto view_meta_opt = cat.lookup_view(stmt.from_table);
        if (!view_meta_opt.has_value()) {
          return make_error("VIEW '" + stmt.from_table +
                            "' not found in catalog");
        }
        return handle_select_from_view(stmt, *view_meta_opt, catalog);
      }

      auto compiled = compile_select_to_program(stmt, catalog);
      result.program_json = compiled.program_json;
      result.field_map = compiled.field_map;
      result.source_streams = compiled.source_streams;
      result.view_type = compiled.view_type;
      result.key_index = compiled.key_index;
      result.select_limit = stmt.limit.value_or(-1);
    } else {
      // Tier 1/2: populate basic plan info
      auto plan = planner::plan_select(stmt, cat);
      result.field_map = plan.field_map;
      result.source_streams = {stmt.from_table};
      result.select_limit = plan.limit;
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

// Handle SELECT FROM VIEW: load stored graph, augment, return Tier 3 result.
CompilationResult handle_select_from_view(const parser::ast::SelectStmt& stmt,
                                          const ViewMeta& view_meta,
                                          const CatalogSnapshot& catalog) {
  // SELECT FROM VIEW always requires LIMIT (no unbounded ephemeral replays).
  if (!stmt.limit.has_value()) {
    return make_error("SELECT FROM VIEW '" + stmt.from_table +
                      "' requires LIMIT or WHERE time bounds");
  }

  // Load the VIEW's stored graph, dropping the Output connection.
  auto [builder, pre_output_ep] =
      compiler::GraphBuilder::from_json_for_augmentation(view_meta.program_json);

  // Build scope from the VIEW's field_map (reusing the existing helper).
  auto scope = build_scope(stmt.from_table, catalog);

  compiler::Endpoint current = pre_output_ep;

  // Apply additional WHERE clause (if any).
  if (stmt.where_clause.has_value()) {
    current =
        compiler::compile_where(*stmt.where_clause, current, scope, builder);
  }

  // Apply SELECT projection (if not SELECT *).
  compiler::FieldMap field_map = view_meta.field_map;
  if (!stmt.select_list.empty()) {
    auto [ep, fm] = compiler::compile_select_projection(
        stmt.select_list, current, scope, builder);
    current = ep;
    field_map = fm;
  }

  // Re-wire Output operator.
  std::string output_id;
  for (const auto& op : builder.operators()) {
    if (op.type == "Output") {
      output_id = op.id;
      break;
    }
  }
  builder.connect(current, {output_id, "i1"});

  // Validate the augmented graph.
  auto validation_errors = builder.validate();
  if (!validation_errors.empty()) {
    CompilationResult err{};
    for (const auto& msg : validation_errors) {
      err.errors.push_back({"graph validation: " + msg, -1, -1});
    }
    return err;
  }

  CompilationResult result{};
  result.statement_type = StatementType::SELECT;
  result.select_tier = SelectTier::TIER3_EPHEMERAL;
  result.program_json = builder.to_json();
  result.field_map = field_map;
  result.source_streams = view_meta.source_streams;
  result.select_limit = *stmt.limit;
  return result;
}

// Handle DROP with dependency checking.
CompilationResult handle_drop(const parser::ast::DropStmt& stmt,
                              const CatalogSnapshot& catalog) {
  // Check whether any registered view depends on the entity being dropped.
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
    for (size_t i = 0; i < dependents.size(); ++i) {
      if (i > 0) dep_list += ", ";
      dep_list += dependents[i];
    }
    return make_error("Cannot drop '" + stmt.name +
                      "': referenced by: " + dep_list);
  }

  CompilationResult result{};
  result.statement_type = StatementType::DROP;
  result.drop_entity_name = stmt.name;

  if (stmt.entity_type == "STREAM")
    result.drop_entity_type = EntityType::STREAM;
  else if (stmt.entity_type == "TABLE")
    result.drop_entity_type = EntityType::TABLE;
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
      return handle_drop(*s, catalog);
    }
    return make_error("unsupported statement type");
  } catch (const std::exception& e) {
    return make_error(e.what());
  }
}

Tier2FilterResult apply_tier2_filter(
    const std::string& sql, const CatalogSnapshot& catalog,
    const std::vector<std::vector<double>>& input_rows, int limit) {
  Tier2FilterResult out;

  // Parse and identify the statement
  auto json_result = pg_query_parse(sql.c_str());
  if (json_result.error) {
    pg_query_free_parse_result(json_result);
    out.rows = input_rows;
    return out;
  }

  parser::ast::Statement stmt;
  try {
    stmt = parser::convert_parse_tree(json_result.parse_tree);
  } catch (...) {
    pg_query_free_parse_result(json_result);
    out.rows = input_rows;
    return out;
  }
  pg_query_free_parse_result(json_result);

  auto* select = std::get_if<parser::ast::SelectStmt>(&stmt);
  if (!select) {
    out.rows = input_rows;
    return out;
  }

  // Build the execution plan
  auto cat = snapshot_to_catalog(catalog);
  planner::SelectPlan plan;
  try {
    plan = planner::plan_select(*select, cat);
  } catch (...) {
    out.rows = input_rows;
    return out;
  }

  out.field_map = plan.field_map;

  if (plan.tier != SelectTier::TIER2_SCAN) {
    // Not Tier 2 — return rows unchanged
    out.rows = input_rows;
    return out;
  }

  int effective_limit = (limit > 0) ? limit : plan.limit;

  for (const auto& row : input_rows) {
    // Apply WHERE predicate
    if (plan.where_predicate) {
      if (!planner::evaluate_where(*plan.where_predicate, row)) {
        continue;
      }
    }

    // Apply SELECT projection
    if (!plan.select_exprs.empty()) {
      out.rows.push_back(
          planner::evaluate_select(plan.select_exprs, row));
    } else {
      out.rows.push_back(row);
    }

    // Check limit
    if (effective_limit > 0 &&
        static_cast<int>(out.rows.size()) >= effective_limit) {
      break;
    }
  }

  return out;
}

}  // namespace rtbot_sql::api
