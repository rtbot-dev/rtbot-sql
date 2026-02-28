#include "rtbot_sql/api/compiler.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <variant>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/alias_expander.h"
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

StreamSchema lookup_schema(const std::string& source,
                           const CatalogSnapshot& catalog) {
  auto it_stream = catalog.streams.find(source);
  if (it_stream != catalog.streams.end()) {
    return it_stream->second;
  }

  auto it_view = catalog.views.find(source);
  if (it_view != catalog.views.end()) {
    // Build a StreamSchema from the view's field_map
    StreamSchema schema;
    schema.name = it_view->second.name;
    for (const auto& [name, idx] : it_view->second.field_map) {
      schema.columns.push_back({name, idx});
    }
    return schema;
  }

  // TODO: Review table-as-source approach. Currently tables are treated
  // identically to streams at compile time, but at runtime they need a
  // KeyedVariable operator (or similar) to materialize lookups from table
  // state. The compilation produces a valid program graph, but the execution
  // engine cannot wire it correctly yet.
  auto it_table = catalog.tables.find(source);
  if (it_table != catalog.tables.end()) {
    return StreamSchema{it_table->second.name, it_table->second.columns};
  }

  throw std::runtime_error("unknown source: " + source);
}

// Build a Scope from a CatalogSnapshot for a given source table.
analyzer::Scope build_scope(const std::string& source,
                            const CatalogSnapshot& catalog) {
  analyzer::Scope scope;
  scope.register_stream(source, lookup_schema(source, catalog));
  return scope;
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

CompilationResult compile_stream_cross_select(
    const parser::ast::SelectStmt& stmt,
    const CatalogSnapshot& catalog) {
  CompilationResult result{};

  int n_sources = static_cast<int>(stmt.from_tables.size());
  if (n_sources <= 1) {
    return make_error("cross-select requires multiple FROM sources");
  }

  compiler::GraphBuilder builder;
  builder.add_operator("input_0", "Input",
                       {{"numInputPorts", static_cast<double>(n_sources)}});

  analyzer::Scope scope;
  std::map<std::string, compiler::Endpoint> source_ports;
  for (int i = 0; i < n_sources; ++i) {
    const auto& src = stmt.from_tables[i];
    auto scope_schema = lookup_schema(src.table_name, catalog);
    scope.register_stream(src.table_name, scope_schema,
                          src.alias.empty() ? "" : src.alias);
    source_ports[src.table_name] = {"input_0", "o" + std::to_string(i + 1)};
  }

  compiler::Endpoint current{"input_0", "o1"};

  auto alias_map = compiler::build_alias_map(stmt.select_list);

  std::vector<parser::ast::SelectItem> expanded_select;
  expanded_select.reserve(stmt.select_list.size());
  for (const auto& item : stmt.select_list) {
    expanded_select.push_back(
        {compiler::expand_aliases(item.expr, alias_map), item.alias});
  }

  std::optional<parser::ast::Expr> expanded_where;
  if (stmt.where_clause.has_value()) {
    expanded_where = compiler::expand_aliases(*stmt.where_clause, alias_map);
    if (compiler::expr_has_aggregate(*expanded_where)) {
      return make_error(
          "aggregate function not allowed in WHERE clause; use HAVING");
    }
  }

  if (expanded_where.has_value()) {
    current = compiler::compile_where(*expanded_where, current, scope, builder,
                                      &source_ports);
  }

  if (!stmt.order_by.empty() && !stmt.limit.has_value()) {
    return make_error("ORDER BY requires LIMIT in streaming context");
  }

  if (!stmt.group_by.empty()) {
    return make_error("GROUP BY with multiple FROM sources is not yet supported");
  }

  auto [ep, field_map] = compiler::compile_select_projection(
      expanded_select, current, scope, builder, &source_ports);
  current = ep;

  if (!stmt.order_by.empty() && stmt.limit.has_value()) {
    const auto& ob = stmt.order_by[0];
    int score_index = -1;
    if (auto* col = std::get_if<parser::ast::ColumnRef>(&ob.expr)) {
      auto it = field_map.find(col->column_name);
      if (it != field_map.end()) {
        score_index = it->second;
      }
    }
    if (score_index < 0) {
      return make_error("ORDER BY column not found in SELECT list");
    }
    auto topk_id = builder.next_id("topk");
    builder.add_operator(topk_id, "TopK",
                         {{"k", static_cast<double>(*stmt.limit)},
                          {"score_index", static_cast<double>(score_index)}},
                         {{"descending", ob.descending ? "true" : "false"}});
    builder.connect(current, {topk_id, "i1"});
    current = {topk_id, "o1"};
  }

  builder.add_operator("output_0", "Output");
  builder.connect(current, {"output_0", "i1"});

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
  result.source_streams.clear();
  for (const auto& src : stmt.from_tables) {
    result.source_streams.push_back(src.table_name);
  }
  result.view_type = ViewType::SCALAR;
  result.key_index = -1;
  return result;
}

// Compile a SELECT query that needs a full operator graph (Tier 3 or GROUP BY).
CompilationResult compile_select_to_program(
    const parser::ast::SelectStmt& stmt, const CatalogSnapshot& catalog) {
  CompilationResult result{};

  if (stmt.from_tables.size() > 1) {
    return compile_stream_cross_select(stmt, catalog);
  }

  auto scope = build_scope(stmt.from_table, catalog);
  compiler::GraphBuilder builder;

  // Input operator
  builder.add_operator("input_0", "Input");
  compiler::Endpoint current{"input_0", "o1"};

  // ── Alias expansion ──────────────────────────────────────────────────────
  auto alias_map = compiler::build_alias_map(stmt.select_list);

  // Produce expanded SELECT list for projection / GROUP BY compilers
  std::vector<parser::ast::SelectItem> expanded_select;
  expanded_select.reserve(stmt.select_list.size());
  for (const auto& item : stmt.select_list) {
    expanded_select.push_back(
        {compiler::expand_aliases(item.expr, alias_map), item.alias});
  }

  // Expand WHERE; reject aggregate aliases (those belong in HAVING)
  std::optional<parser::ast::Expr> expanded_where;
  if (stmt.where_clause.has_value()) {
    expanded_where = compiler::expand_aliases(*stmt.where_clause, alias_map);
    if (compiler::expr_has_aggregate(*expanded_where)) {
      return make_error(
          "aggregate function not allowed in WHERE clause; use HAVING");
    }
  }

  // Expand HAVING
  std::optional<parser::ast::Expr> expanded_having;
  if (stmt.having.has_value()) {
    expanded_having = compiler::expand_aliases(*stmt.having, alias_map);
  }
  // ─────────────────────────────────────────────────────────────────────────

  // WHERE clause
  if (expanded_where.has_value()) {
    current = compiler::compile_where(*expanded_where, current, scope, builder);
  }

  // Validate ORDER BY + LIMIT early
  if (!stmt.order_by.empty() && !stmt.limit.has_value()) {
    return make_error("ORDER BY requires LIMIT in streaming context");
  }

  // Compute input column count for composite GROUP BY support
  int num_input_cols = 0;
  {
    auto it = catalog.streams.find(stmt.from_table);
    if (it != catalog.streams.end()) {
      num_input_cols = static_cast<int>(it->second.columns.size());
    } else {
      auto it_v = catalog.views.find(stmt.from_table);
      if (it_v != catalog.views.end()) {
        num_input_cols = static_cast<int>(it_v->second.field_map.size());
      } else {
        auto it_t = catalog.tables.find(stmt.from_table);
        if (it_t != catalog.tables.end()) {
          num_input_cols = static_cast<int>(it_t->second.columns.size());
        }
      }
    }
  }

  compiler::FieldMap field_map;

  // GROUP BY
  if (!stmt.group_by.empty()) {
    auto [ep, fm] = compiler::compile_group_by(
        expanded_select, stmt.group_by, expanded_having, current, scope,
        builder, num_input_cols);
    current = ep;
    field_map = fm;
  } else {
    // SELECT projection
    auto [ep, fm] =
        compiler::compile_select_projection(expanded_select, current, scope,
                                            builder);
    current = ep;
    field_map = fm;
  }

  // ORDER BY + LIMIT → TopK
  if (!stmt.order_by.empty() && stmt.limit.has_value()) {
    const auto& ob = stmt.order_by[0];
    int score_index = -1;
    if (auto* col = std::get_if<parser::ast::ColumnRef>(&ob.expr)) {
      auto it = field_map.find(col->column_name);
      if (it != field_map.end()) {
        score_index = it->second;
      }
    }
    if (score_index < 0) {
      return make_error("ORDER BY column not found in SELECT list");
    }
    auto topk_id = builder.next_id("topk");
    builder.add_operator(topk_id, "TopK",
                         {{"k", static_cast<double>(*stmt.limit)},
                          {"score_index", static_cast<double>(score_index)}},
                         {{"descending", ob.descending ? "true" : "false"}});
    builder.connect(current, {topk_id, "i1"});
    current = {topk_id, "o1"};
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
    if (stmt.group_by.size() == 1) {
      // Single-key GROUP BY outputs the key at position 0 in the keyed view
      // payload (see compile_group_by field_map contract). Runtime key tracking
      // and tier-1 keyed reads must use the keyed-view output index, not the
      // original source-stream column index.
      result.key_index = 0;
    } else {
      // Composite key: routing is via hash — no single key_index
      result.key_index = -1;
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

// Handle CREATE TABLE (with PRIMARY KEY).
CompilationResult handle_create_table(const parser::ast::CreateStreamStmt& stmt) {
  CompilationResult result{};
  result.statement_type = StatementType::CREATE_TABLE;
  result.entity_name = stmt.name;

  std::vector<int> key_cols;
  for (size_t i = 0; i < stmt.columns.size(); ++i) {
    if (stmt.columns[i].primary_key) {
      key_cols.push_back(static_cast<int>(i));
    }
  }
  if (key_cols.empty()) {
    return make_error("CREATE TABLE requires a PRIMARY KEY column");
  }
  if (key_cols.size() > 1) {
    return make_error("Composite table keys are not yet supported");
  }

  TableSchema schema;
  schema.name = stmt.name;
  for (size_t i = 0; i < stmt.columns.size(); ++i) {
    schema.columns.push_back({stmt.columns[i].name, static_cast<int>(i)});
  }
  schema.key_columns = key_cols;
  schema.changelog_stream = "rtbot:sql:table:" + stmt.name + ":changelog";

  result.table_schema = schema;
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

// Handle DELETE FROM TABLE.
CompilationResult handle_delete(const parser::ast::DeleteStmt& stmt,
                                const CatalogSnapshot& catalog) {
  CompilationResult result{};
  result.statement_type = StatementType::DELETE;
  result.entity_name = stmt.table_name;

  auto it = catalog.tables.find(stmt.table_name);
  if (it == catalog.tables.end()) {
    return make_error("DELETE: unknown table: " + stmt.table_name);
  }
  const auto& table = it->second;
  if (table.key_columns.empty()) {
    return make_error("DELETE: table has no primary key: " + stmt.table_name);
  }

  if (!stmt.where_clause.has_value()) {
    return make_error("DELETE requires WHERE key_column = value");
  }

  const auto* cmp =
      std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&*stmt.where_clause);
  if (!cmp || (*cmp)->op != "=") {
    return make_error("DELETE WHERE must be key_column = constant");
  }

  const auto* key_const = std::get_if<parser::ast::Constant>(&(*cmp)->right);
  if (!key_const) {
    // Try left side (constant = col)
    key_const = std::get_if<parser::ast::Constant>(&(*cmp)->left);
  }
  if (!key_const) {
    return make_error("DELETE WHERE value must be a constant");
  }

  // Payload: [key, NaN] — NaN signals deletion to KeyedVariable
  result.delete_payload = {key_const->value,
                           std::numeric_limits<double>::quiet_NaN()};
  return result;
}

// Forward declarations — defined below.
CompilationResult handle_select_from_view(const parser::ast::SelectStmt& stmt,
                                          const ViewMeta& view_meta,
                                          const CatalogSnapshot& catalog);
CompilationResult compile_joined_select(const parser::ast::SelectStmt& stmt,
                                        const CatalogSnapshot& catalog);

// Handle SELECT.
CompilationResult handle_select(const parser::ast::SelectStmt& stmt,
                                const CatalogSnapshot& catalog) {
  CompilationResult result{};
  result.statement_type = StatementType::SELECT;

  try {
    // JOINs always require full graph compilation (TIER3_EPHEMERAL)
    if (!stmt.join_clauses.empty()) {
      result.select_tier = SelectTier::TIER3_EPHEMERAL;
      auto compiled = compile_joined_select(stmt, catalog);
      if (compiled.has_errors()) return compiled;
      result.program_json = compiled.program_json;
      result.field_map = compiled.field_map;
      result.source_streams = compiled.source_streams;
      result.view_type = compiled.view_type;
      result.key_index = compiled.key_index;
      result.select_limit = stmt.limit.value_or(-1);
      return result;
    }

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
      if (compiled.has_errors()) return compiled;
      result.program_json = compiled.program_json;
      result.field_map = compiled.field_map;
      result.source_streams = compiled.source_streams;
      result.view_type = compiled.view_type;
      result.key_index = compiled.key_index;
      result.select_limit = stmt.limit.value_or(-1);
    } else {
      // Tier 1/2: expand aliases then plan
      // (group_by, order_by, having, join_clauses are empty for Tier 1/2)
      auto alias_map_t12 = compiler::build_alias_map(stmt.select_list);
      parser::ast::SelectStmt expanded_stmt_t12;
      expanded_stmt_t12.from_table = stmt.from_table;
      expanded_stmt_t12.from_alias = stmt.from_alias;
      expanded_stmt_t12.from_tables = stmt.from_tables;
      expanded_stmt_t12.limit = stmt.limit;
      for (const auto& item : stmt.select_list) {
        expanded_stmt_t12.select_list.push_back(
            {compiler::expand_aliases(item.expr, alias_map_t12), item.alias});
      }
      if (stmt.where_clause.has_value()) {
        expanded_stmt_t12.where_clause =
            compiler::expand_aliases(*stmt.where_clause, alias_map_t12);
      }
      auto plan = planner::plan_select(expanded_stmt_t12, cat);
      result.field_map = plan.field_map;
      result.source_streams = {stmt.from_table};
      result.select_limit = plan.limit;
    }
  } catch (const std::runtime_error& e) {
    return make_error(e.what());
  }

  return result;
}

// Compile a Stream–TABLE JOIN using KeyedVariable pattern.
//
// Graph:
//   Input(numInputPorts=2)
//     o1 → VectorExtract(join_col) → kv.c1 (query)
//     o1 → VectorExtract [fan-out]  → kv.c2 (heartbeat)
//     o2 → kv.i1 (changelog data)
//     o1 → dmux.i1 (full left tuple)
//   kv.o1 → dmux.c1 (boolean gate)
//   dmux.o1 → [WHERE] → [SELECT projection] → Output
CompilationResult compile_table_join(
    const parser::ast::SelectStmt& stmt,
    const parser::ast::JoinClause& join,
    const CatalogSnapshot& catalog) {
  // Look up left stream
  auto it_left = catalog.streams.find(stmt.from_table);
  if (it_left == catalog.streams.end()) {
    return make_error("JOIN: unknown left stream: " + stmt.from_table);
  }
  const StreamSchema& left_schema = it_left->second;

  // Look up right table (verify existence)
  if (catalog.tables.find(join.table_name) == catalog.tables.end()) {
    return make_error("JOIN: unknown table: " + join.table_name);
  }

  // Resolve join column index from ON condition (left side)
  int join_col_idx = -1;
  if (join.on_condition.has_value()) {
    const auto* cmp = std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(
        &*join.on_condition);
    if (cmp && (*cmp)->op == "=") {
      auto try_left_col = [&](const parser::ast::Expr& e) -> int {
        const auto* ref = std::get_if<parser::ast::ColumnRef>(&e);
        if (!ref) return -1;
        // Belongs to left stream if alias matches (or empty/no alias)
        if (!ref->table_alias.empty() &&
            ref->table_alias != stmt.from_alias &&
            ref->table_alias != stmt.from_table) {
          return -1;
        }
        auto idx = left_schema.column_index(ref->column_name);
        return idx.value_or(-1);
      };
      join_col_idx = try_left_col((*cmp)->left);
      if (join_col_idx == -1) join_col_idx = try_left_col((*cmp)->right);
    }
  }
  if (join_col_idx == -1) {
    return make_error(
        "JOIN: could not resolve join column from ON condition for table: " +
        join.table_name);
  }

  // Build graph
  compiler::GraphBuilder builder;

  // Input with 2 ports: i1 = main stream, i2 = table changelog
  builder.add_operator("input_0", "Input", {{"numInputPorts", 2.0}});

  // VectorExtract for join column (shared for both c1 query and c2 heartbeat)
  std::string ve_id = builder.next_id("ve");
  builder.add_operator(ve_id, "VectorExtract",
                       {{"index", static_cast<double>(join_col_idx)}});
  builder.connect({"input_0", "o1"}, {ve_id, "i1"});

  // KeyedVariable (exists mode: key presence check)
  std::string kv_id = builder.next_id("kv");
  builder.add_operator(kv_id, "KeyedVariable", {}, {{"mode", "exists"}});
  builder.connect({ve_id, "o1"}, {kv_id, "c1"});  // query port
  builder.connect({ve_id, "o1"}, {kv_id, "c2"});  // heartbeat port
  builder.connect({"input_0", "o2"}, {kv_id, "i1"});  // changelog data

  // Demultiplexer (boolean gate on the full left tuple)
  std::string dmux_id = builder.next_id("dmux");
  builder.add_operator(dmux_id, "Demultiplexer",
                       {{"numPorts", 1.0}}, {{"portType", "vector_number"}});
  builder.connect({"input_0", "o1"}, {dmux_id, "i1"});
  builder.connect({kv_id, "o1"}, {dmux_id, "c1"});

  // Build scope for subsequent WHERE / SELECT projection
  analyzer::Scope scope;
  scope.register_stream(stmt.from_table, left_schema,
                        stmt.from_alias.empty() ? "" : stmt.from_alias);

  compiler::Endpoint current{dmux_id, "o1"};

  // Optional additional WHERE clause
  if (stmt.where_clause.has_value()) {
    current = compiler::compile_where(*stmt.where_clause, current, scope, builder);
  }

  // SELECT projection (or pass-through for SELECT *)
  compiler::FieldMap field_map;
  if (!stmt.select_list.empty()) {
    auto [ep, fm] = compiler::compile_select_projection(
        stmt.select_list, current, scope, builder);
    current = ep;
    field_map = fm;
  } else {
    for (const auto& col : left_schema.columns) {
      field_map[col.name] = col.index;
    }
  }

  // Output
  builder.add_operator("output_0", "Output");
  builder.connect(current, {"output_0", "i1"});

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
  // source_streams: left stream + table entity name (for dependency checking)
  result.source_streams = {stmt.from_table, join.table_name};
  result.view_type = ViewType::SCALAR;
  result.key_index = -1;
  return result;
}

// Dispatch JOIN compilation based on join target type.
CompilationResult compile_joined_select(const parser::ast::SelectStmt& stmt,
                                        const CatalogSnapshot& catalog) {
  if (stmt.join_clauses.empty()) {
    return compile_select_to_program(stmt, catalog);
  }

  const auto& join = stmt.join_clauses[0];

  auto cat = snapshot_to_catalog(catalog);
  auto entity_type = cat.resolve_entity(join.table_name);

  if (!entity_type.has_value()) {
    return make_error("JOIN: unknown join target: " + join.table_name);
  }
  if (*entity_type == EntityType::TABLE) {
    return compile_table_join(stmt, join, catalog);
  }

  return make_error("JOIN: unsupported join target type for: " + join.table_name +
                    " (only TABLE joins are supported)");
}

// Handle CREATE MATERIALIZED VIEW.
CompilationResult handle_create_mat_view(
    const parser::ast::CreateViewStmt& stmt,
    const CatalogSnapshot& catalog) {
  CompilationResult result{};

  try {
    CompilationResult compiled;
    if (stmt.query.from_tables.size() > 1) {
      compiled = compile_stream_cross_select(stmt.query, catalog);
    } else if (!stmt.query.join_clauses.empty()) {
      compiled = compile_joined_select(stmt.query, catalog);
    } else {
      compiled = compile_select_to_program(stmt.query, catalog);
    }
    if (compiled.has_errors()) return compiled;
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
      // Route to CREATE TABLE if any column has PRIMARY KEY
      bool has_pk = std::any_of(s->columns.begin(), s->columns.end(),
                                [](const parser::ast::ColumnDefAST& c) {
                                  return c.primary_key;
                                });
      return has_pk ? handle_create_table(*s) : handle_create_stream(*s);
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
    if (auto* s = std::get_if<parser::ast::DeleteStmt>(&stmt)) {
      return handle_delete(*s, catalog);
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

  // Cross-key aggregation: reduce all rows to a single output row.
  if (plan.is_cross_key) {
    out.rows.push_back(
        planner::evaluate_cross_key_agg(plan.cross_key_aggs, input_rows));
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
