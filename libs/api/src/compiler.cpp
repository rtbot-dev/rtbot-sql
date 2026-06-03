#include "rtbot_sql/api/compiler.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <regex>
#include <set>
#include <stdexcept>
#include <string>
#include <variant>

#include "rtbot_sql/analyzer/analyzer.h"
#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/api/preprocessor.h"
#include "rtbot_sql/api/unreachable.h"
#include "rtbot_sql/catalog/catalog.h"
#include "rtbot_sql/catalog/string_dictionary.h"
#include "rtbot_sql/compiler/alias_expander.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/compiler/group_by_compiler.h"
#include "rtbot_sql/compiler/select_compiler.h"
#include "rtbot_sql/compiler/where_compiler.h"
#include "rtbot_sql/parser/ast.h"
#include "rtbot_sql/parser/ast_converter.h"
#include "rtbot_sql/parser/parser.h"
#include "rtbot_sql/parser/source_text.h"
#include "rtbot_sql/planner/classifier.h"
#include "rtbot_sql/planner/planner.h"

namespace rtbot_sql::api {

namespace {

std::string normalize_sql(const std::string& sql) {
  std::string out = sql;
  // Strip optional FROM "..." [TYPE x] [WINDOW n] source metadata so
  // pg_query never sees it. The anchor on ) ensures we only match after a
  // column-list closing paren, not in SELECT ... FROM clauses.
  static const std::regex kStreamFrom(
      R"(\)\s+FROM\s+"[^"]*"(?:\s+TYPE\s+\w+)?(?:\s+WINDOW\s+\d+)?\s*;?\s*$)",
      std::regex::icase);
  out = std::regex_replace(out, kStreamFrom, ")");
  static const std::regex kCreateStream(R"(\bSELECT\s+STREAM\b)",
                                        std::regex::icase);
  out = std::regex_replace(out, kCreateStream, "CREATE TABLE");
  static const std::regex kDropStream(R"(\bDROP\s+STREAM\b)",
                                      std::regex::icase);
  out = std::regex_replace(out, kDropStream, "DROP TABLE");
  return out;
}

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
    // Build a StreamSchema from the view's field_map.
    //
    // The field_map stores the *physical* position of each column in the
    // operator output vector.  For composite GROUP BY views, KeyedPipeline
    // prepends an internal hash key at index 0, so the published columns
    // start at index 1 (e.g. device_id→1, channel_id→2, …).
    //
    // Downstream views, however, receive a *logical* vector that contains
    // only the published columns (the hash key is stripped during the
    // decode→re-encode cycle in the JavaScript and Java runtimes).
    // Therefore we must re-map the field_map indices to contiguous 0-based
    // positions ordered by their original physical index.
    StreamSchema schema;
    schema.name = it_view->second.name;

    // Sort entries by physical index to preserve column order.
    std::vector<std::pair<std::string, int>> sorted_entries(
        it_view->second.field_map.begin(), it_view->second.field_map.end());
    std::sort(sorted_entries.begin(), sorted_entries.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    for (int i = 0; i < static_cast<int>(sorted_entries.size()); ++i) {
      schema.columns.push_back({sorted_entries[i].first, i});
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

  // Precondition: analyzer::analyze_select rejects unknown sources before
  // lookup_schema runs.
  unreachable();
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
  for (const auto& [key, entries] : snap.dictionaries) {
    catalog::StringDictionary dict;
    dict.restore(entries);
    cat.register_dictionary(key, dict);
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
    std::string excl = item.alias.has_value() ? *item.alias : "";
    expanded_select.push_back(
        {compiler::expand_aliases(item.expr, alias_map, excl), item.alias});
  }

  // Preconditions enforced by analyzer::analyze_select before compilation:
  //   * no aggregate in WHERE (raw or via SELECT-list alias expansion),
  //   * ORDER BY accompanied by LIMIT,
  //   * GROUP BY not combined with multiple FROM sources,
  //   * ORDER BY column resolves into the SELECT list.

  std::optional<parser::ast::Expr> expanded_where;
  if (stmt.where_clause.has_value()) {
    expanded_where = compiler::expand_aliases(*stmt.where_clause, alias_map);
  }

  if (expanded_where.has_value()) {
    current = compiler::compile_where(*expanded_where, current, scope, builder,
                                      &source_ports);
  }

  auto [ep, field_map, _seg] = compiler::compile_select_projection(
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
    if (score_index < 0) unreachable();
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
    // Exclude the item's own alias to prevent self-referencing expansion
    // (e.g., RESAMPLE_CONSTANT(value, 1000) AS value would otherwise replace
    // the inner ColumnRef "value" with the alias definition, creating a nested
    // duplicate).
    std::string excl = item.alias.has_value() ? *item.alias : "";
    expanded_select.push_back(
        {compiler::expand_aliases(item.expr, alias_map, excl), item.alias});
  }

  // Expand WHERE. The analyzer rejects aggregates in WHERE — both raw and
  // those reachable through SELECT-list alias expansion — before
  // compilation runs.
  std::optional<parser::ast::Expr> expanded_where;
  if (stmt.where_clause.has_value()) {
    expanded_where = compiler::expand_aliases(*stmt.where_clause, alias_map);
  }

  // Expand HAVING
  std::optional<parser::ast::Expr> expanded_having;
  if (stmt.having.has_value()) {
    expanded_having = compiler::expand_aliases(*stmt.having, alias_map);
  }

  // Expand GROUP BY items (resolve SELECT aliases — MySQL-style resolution)
  std::vector<parser::ast::Expr> expanded_group_by;
  expanded_group_by.reserve(stmt.group_by.size());
  for (const auto& gb : stmt.group_by) {
    expanded_group_by.push_back(compiler::expand_aliases(gb, alias_map));
  }
  // ─────────────────────────────────────────────────────────────────────────

  // WHERE clause. Defer the compile_where emission when there's no GROUP BY:
  // the select projection may be able to absorb the predicate into a single
  // FusedExpressionVector via GATE, eliminating the VectorExtract + CompareGT
  // + Demultiplexer chain. When GROUP BY is present WHERE must run first so
  // segment detection sees only the filtered rows.
  const bool try_absorb_where =
      expanded_where.has_value() && expanded_group_by.empty();
  if (expanded_where.has_value() && !try_absorb_where) {
    current = compiler::compile_where(*expanded_where, current, scope, builder);
  }

  // ORDER BY without LIMIT is rejected by analyzer::analyze_select.

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
  bool is_segment_only_group_by = false;
  if (!expanded_group_by.empty()) {
    auto [ep, fm, seg_only] = compiler::compile_group_by(
        expanded_select, expanded_group_by, expanded_having, current, scope,
        builder, num_input_cols);
    current = ep;
    field_map = fm;
    is_segment_only_group_by = seg_only;
  } else {
    // SELECT projection. When try_absorb_where is on, we haven't yet emitted
    // compile_where — give the projection a shot at folding it into the FEV.
    const parser::ast::Expr* where_ptr =
        try_absorb_where ? &*expanded_where : nullptr;
    bool where_absorbed = false;
    auto [ep, fm, _seg2] = compiler::compile_select_projection(
        expanded_select, current, scope, builder,
        /*source_endpoints=*/nullptr, where_ptr, &where_absorbed);
    // Absorption is best-effort. If the projection fell back (multi-source
    // or unfusable SELECT), it silently ignored where_ptr — emit the
    // standalone compile_where and re-run the projection.
    if (try_absorb_where && !where_absorbed) {
      current = compiler::compile_where(*expanded_where, current, scope,
                                         builder);
      auto retry = compiler::compile_select_projection(expanded_select, current,
                                                        scope, builder);
      current = retry.endpoint;
      field_map = retry.field_map;
    } else {
      current = ep;
      field_map = fm;
    }
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
    // analyzer::analyze_select rejects ORDER BY columns absent from the
    // SELECT list; score_index is guaranteed >= 0 here.
    if (score_index < 0) unreachable();
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
  if (!stmt.group_by.empty() && !is_segment_only_group_by) {
    result.view_type = ViewType::KEYED;
    // Count only persistent partition keys (bare column references), ignoring
    // segment expressions. Mixed GROUP BY (e.g., device_id, ABS(x) > 0) has
    // group_by.size() > 1 but only 1 persistent key.
    auto classification =
        compiler::classify_group_by(expanded_group_by, scope);
    int num_persistent = classification.persistent_key_count();
    if (num_persistent == 1) {
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
    // No GROUP BY, or segment-only GROUP BY (no persistent partition key)
    result.view_type = ViewType::SCALAR;
    result.key_index = -1;
  }

  return result;
}

// Handle SELECT STREAM (via CREATE TABLE).
CompilationResult handle_create_stream(
    const parser::ast::CreateStreamStmt& stmt) {
  CompilationResult result{};
  result.statement_type = StatementType::SELECT_STREAM;
  result.entity_name = stmt.name;

  StreamSchema schema;
  schema.name = stmt.name;
  for (size_t i = 0; i < stmt.columns.size(); ++i) {
    ColumnType col_type = ColumnType::DOUBLE;
    const auto& tn = stmt.columns[i].type_name;
    if (tn == "text" || tn == "varchar" || tn == "bpchar") {
      col_type = ColumnType::TEXT;
    }
    schema.columns.push_back(
        {stmt.columns[i].name, static_cast<int>(i), col_type});
  }
  schema.source = stmt.source;
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
  // Single-PRIMARY-KEY shape enforced by analyzer::analyze_create_stream.
  // The dispatch in compile_sql only routes here when key_cols is non-empty,
  // and composite keys are rejected upstream.
  if (key_cols.empty() || key_cols.size() > 1) unreachable();

  TableSchema schema;
  schema.name = stmt.name;
  for (size_t i = 0; i < stmt.columns.size(); ++i) {
    ColumnType col_type = ColumnType::DOUBLE;
    const auto& tn = stmt.columns[i].type_name;
    if (tn == "text" || tn == "varchar" || tn == "bpchar") {
      col_type = ColumnType::TEXT;
    }
    schema.columns.push_back(
        {stmt.columns[i].name, static_cast<int>(i), col_type});
  }
  schema.key_columns = key_cols;
  schema.changelog_stream = "rtbot:sql:table:" + stmt.name + ":changelog";

  result.table_schema = schema;
  return result;
}

// Handle INSERT.
CompilationResult handle_insert(const parser::ast::InsertStmt& stmt,
                                catalog::Catalog& cat,
                                const CatalogSnapshot& catalog) {
  CompilationResult result{};
  result.statement_type = StatementType::INSERT;
  result.entity_name = stmt.table_name;

  // Preconditions enforced by analyzer::analyze_insert: the table exists,
  // value count matches column count, each value is a constant of the
  // matching type.
  std::vector<ColumnDef> columns;
  auto it_stream = catalog.streams.find(stmt.table_name);
  if (it_stream != catalog.streams.end()) {
    columns = it_stream->second.columns;
  } else {
    auto it_table = catalog.tables.find(stmt.table_name);
    if (it_table == catalog.tables.end()) unreachable();
    columns = it_table->second.columns;
  }

  if (stmt.values.size() != columns.size()) unreachable();

  for (size_t i = 0; i < stmt.values.size(); ++i) {
    const auto& col = columns[i];

    if (auto* c = std::get_if<parser::ast::Constant>(&stmt.values[i])) {
      result.insert_payload.push_back(c->value);
    } else if (auto* sc =
                   std::get_if<parser::ast::StringConstant>(&stmt.values[i])) {
      // Encode string via dictionary.
      const std::string dict_key =
          stmt.table_name + "." + col.name;
      auto& dict = cat.get_or_create_dictionary(dict_key);
      double id = dict.encode(sc->value);
      result.insert_payload.push_back(id);
      // Record the dictionary update.
      result.dictionary_updates[dict_key] = dict.all_entries();
    } else {
      unreachable();
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

  // Preconditions enforced by analyzer::analyze_delete: table exists, has
  // a primary key, WHERE is a `key_column = constant` comparison.
  auto it = catalog.tables.find(stmt.table_name);
  if (it == catalog.tables.end()) unreachable();
  const auto& table = it->second;
  if (table.key_columns.empty()) unreachable();
  if (!stmt.where_clause.has_value()) unreachable();

  const auto* cmp =
      std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&*stmt.where_clause);
  if (!cmp || (*cmp)->op != "=") unreachable();

  const auto* key_const = std::get_if<parser::ast::Constant>(&(*cmp)->right);
  if (!key_const) {
    // analyzer accepts both `col = const` and `const = col` shapes.
    key_const = std::get_if<parser::ast::Constant>(&(*cmp)->left);
  }
  if (!key_const) unreachable();

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
        std::string excl = item.alias.has_value() ? *item.alias : "";
        expanded_stmt_t12.select_list.push_back(
            {compiler::expand_aliases(item.expr, alias_map_t12, excl),
             item.alias});
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
  // Preconditions enforced by analyzer::analyze_select: the left stream
  // exists in the catalog (covered by source-existence check) and the
  // JOIN target is a TABLE.
  auto it_left = catalog.streams.find(stmt.from_table);
  if (it_left == catalog.streams.end()) unreachable();
  const StreamSchema& left_schema = it_left->second;

  if (catalog.tables.find(join.table_name) == catalog.tables.end()) {
    unreachable();
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
  // Precondition: analyzer::analyze_select rejects ON conditions that
  // don't resolve a join column from the left stream.
  if (join_col_idx == -1) unreachable();

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
    auto [ep, fm, _seg3] = compiler::compile_select_projection(
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

  // Preconditions enforced by analyzer::analyze_select: the JOIN target
  // resolves to a TABLE entity. Stream/View targets and unknown names are
  // rejected before this dispatch runs.
  auto cat = snapshot_to_catalog(catalog);
  auto entity_type = cat.resolve_entity(join.table_name);
  if (!entity_type.has_value() || *entity_type != EntityType::TABLE) {
    unreachable();
  }
  return compile_table_join(stmt, join, catalog);
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
  // SELECT FROM VIEW always requires LIMIT (no unbounded ephemeral
  // replays). Enforced by analyzer::analyze_select before compilation.
  if (!stmt.limit.has_value()) unreachable();

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
    auto [ep, fm, _seg4] = compiler::compile_select_projection(
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

// Handle DROP. Dependency check (entity not referenced by any view) is
// enforced by analyzer::analyze_drop before compilation.
CompilationResult handle_drop(const parser::ast::DropStmt& stmt,
                              const CatalogSnapshot& /*catalog*/) {
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
  // Parse the optional FROM "..." [TYPE x] [WINDOW n] source metadata before
  // normalization strips it. Validation rules:
  //   * `SELECT STREAM ... ) FROM`            → "FROM requires a quoted source name"
  //   * `... FROM "<unknown TYPE>`            → "unknown TYPE '...'"
  //   * `... FROM "src" TYPE csv_burst` (no WINDOW)
  //                                            → "TYPE csv_burst requires WINDOW"
  //   * `... FROM "src" WINDOW <not-a-number>` → "WINDOW requires a positive integer"
  //   * `... FROM "src" <non-`;` after metadata>` → "expected `;` after source name"
  //   * Otherwise accepted; source/type/window populated.
  std::optional<Source> stream_source;
  {
    static const std::regex kSelectStream(R"(\bSELECT\s+STREAM\b)",
                                          std::regex::icase);
    static const std::regex kStreamFromAnywhere(
        R"re(\)\s+FROM\s+"([^"]*)")re", std::regex::icase);
    static const std::regex kBareFrom(R"re(\)\s+(FROM)\b)re",
                                      std::regex::icase);

    bool is_select_stream = std::regex_search(sql, kSelectStream);
    std::smatch m;

    auto make_loc_error = [&](int byte_offset,
                              const std::string& msg) -> CompilationResult {
      auto src = parser::make_source_text(sql);
      auto loc = parser::compute_location(src, byte_offset);
      CompilationResult r{};
      r.errors.push_back(
          {msg, loc.line, loc.column, loc.end_line, loc.end_column});
      return r;
    };

    if (is_select_stream && std::regex_search(sql, m, kStreamFromAnywhere)) {
      Source s;
      s.name = m[1].str();
      auto src = parser::make_source_text(sql);
      // Span the `"..."` token (both quotes) via find_token_end.
      int quote_offset = static_cast<int>(m.position(1)) - 1;
      auto loc = parser::compute_location(src, quote_offset);
      s.line = loc.line;
      s.column = loc.column;
      s.end_line = loc.end_line;
      s.end_column = loc.end_column;

      // Cursor for parsing TYPE and WINDOW clauses after the closing quote.
      std::size_t i = static_cast<std::size_t>(m.position(0)) +
                       static_cast<std::size_t>(m.length(0));
      auto skip_ws = [&]() {
        while (i < sql.size() &&
               std::isspace(static_cast<unsigned char>(sql[i])))
          ++i;
      };
      auto match_keyword = [&](const std::string& kw) -> bool {
        if (i + kw.size() > sql.size()) return false;
        for (std::size_t k = 0; k < kw.size(); ++k) {
          if (std::tolower(static_cast<unsigned char>(sql[i + k])) !=
              std::tolower(static_cast<unsigned char>(kw[k])))
            return false;
        }
        if (i + kw.size() < sql.size()) {
          unsigned char next =
              static_cast<unsigned char>(sql[i + kw.size()]);
          if (std::isalnum(next) || next == '_') return false;
        }
        return true;
      };
      auto read_word = [&]() -> std::string {
        std::size_t start = i;
        while (i < sql.size() &&
               (std::isalnum(static_cast<unsigned char>(sql[i])) ||
                sql[i] == '_'))
          ++i;
        return sql.substr(start, i - start);
      };

      skip_ws();

      // Optional TYPE clause.
      if (match_keyword("TYPE")) {
        i += 4;
        skip_ws();
        std::size_t id_start = i;
        std::string type_str = read_word();
        if (type_str.empty()) {
          return make_loc_error(
              static_cast<int>(id_start),
              "SELECT STREAM: TYPE requires a value (scalar or csv_burst)");
        }
        std::string type_lower = type_str;
        std::transform(type_lower.begin(), type_lower.end(),
                       type_lower.begin(), ::tolower);
        SourceTypeClause tc;
        tc.raw = type_str;
        auto type_loc = parser::compute_location(
            src, static_cast<int>(id_start));
        tc.line = type_loc.line;
        tc.column = type_loc.column;
        tc.end_line = type_loc.end_line;
        tc.end_column = type_loc.end_column;
        if (type_lower == "scalar") {
          tc.value = SourceType::SCALAR;
        } else if (type_lower == "csv_burst") {
          tc.value = SourceType::CSV_BURST;
        } else {
          // Preserve the raw identifier; the analyzer will flag UNKNOWN.
          tc.value = SourceType::UNKNOWN;
        }
        s.type = tc;
        skip_ws();
        // Reject a second TYPE clause. Span over the duplicate keyword.
        if (match_keyword("TYPE")) {
          return make_loc_error(
              static_cast<int>(i),
              "SELECT STREAM: duplicate TYPE clause");
        }
      }

      // Optional WINDOW clause.
      if (match_keyword("WINDOW")) {
        std::size_t window_offset = i;
        i += 6;
        skip_ws();
        std::size_t num_start = i;
        while (i < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i])))
          ++i;
        if (num_start == i) {
          return make_loc_error(
              static_cast<int>(window_offset),
              "SELECT STREAM: WINDOW requires a positive integer");
        }
        SourceWindowClause wc;
        wc.value = std::stoi(sql.substr(num_start, i - num_start));
        auto window_loc = parser::compute_location(
            src, static_cast<int>(num_start));
        wc.line = window_loc.line;
        wc.column = window_loc.column;
        wc.end_line = window_loc.end_line;
        wc.end_column = window_loc.end_column;
        s.window = wc;
        skip_ws();
        // Reject a second WINDOW clause. Span over the duplicate keyword.
        if (match_keyword("WINDOW")) {
          return make_loc_error(
              static_cast<int>(i),
              "SELECT STREAM: duplicate WINDOW clause");
        }
      }

      // Semantic checks (unknown TYPE, csv_burst↔WINDOW correlation) live
      // in the analyzer (libs/analyzer/src/ddl_analyzer.cpp).

      // After source / TYPE / WINDOW: first non-whitespace must be `;` or
      // end-of-input.
      if (i < sql.size() && sql[i] != ';') {
        return make_loc_error(
            static_cast<int>(i),
            "SELECT STREAM: expected `;` after source name");
      }

      stream_source = std::move(s);
    } else if (is_select_stream &&
               std::regex_search(sql, m, kBareFrom)) {
      return make_loc_error(
          static_cast<int>(m.position(1)),
          "SELECT STREAM: FROM requires a quoted source name");
    }
  }

  const std::string normalized = normalize_sql(sql);

  // Step 1: Parse
  auto parse_result = parser::parse(normalized);
  if (!parse_result.ok()) {
    CompilationResult r{};
    for (const auto& err : parse_result.errors) {
      r.errors.push_back({err.message, err.loc.line, err.loc.column,
                          err.loc.end_line, err.loc.end_column});
    }
    parser::free_result(parse_result);
    return r;
  }

  // Step 2: Get JSON parse tree and convert to AST
  auto json_result = pg_query_parse(normalized.c_str());
  if (json_result.error) {
    auto src = parser::make_source_text(normalized);
    // cursorpos is 1-based (PostgreSQL convention); compute_location
    // expects 0-based byte offsets — convert here.
    int cursorpos = json_result.error->cursorpos;
    int byte_offset = cursorpos > 0 ? cursorpos - 1 : -1;
    auto loc = parser::compute_location(src, byte_offset);
    CompilationResult r{};
    r.errors.push_back({json_result.error->message
                            ? json_result.error->message
                            : "parse error",
                        loc.line, loc.column, loc.end_line, loc.end_column});
    pg_query_free_parse_result(json_result);
    parser::free_result(parse_result);
    return r;
  }

  parser::ast::Statement stmt;
  try {
    stmt = parser::convert_parse_tree(normalized, json_result.parse_tree);
  } catch (const parser::ConverterError& e) {
    // Converter throws carry a SourceLocation — propagate the full span.
    pg_query_free_parse_result(json_result);
    parser::free_result(parse_result);
    CompilationResult r{};
    r.errors.push_back({e.what(), e.loc().line, e.loc().column,
                        e.loc().end_line, e.loc().end_column});
    return r;
  } catch (const std::exception& e) {
    pg_query_free_parse_result(json_result);
    parser::free_result(parse_result);
    return make_error(e.what());
  }

  pg_query_free_parse_result(json_result);
  parser::free_result(parse_result);

  // Attach the extracted FROM "..." / TYPE / WINDOW metadata to the AST so
  // the analyzer can validate it alongside other CREATE STREAM rules.
  if (stream_source.has_value()) {
    if (auto* s = std::get_if<parser::ast::CreateStreamStmt>(&stmt)) {
      s->source = *stream_source;
    }
  }

  // Step 2.5: Semantic analysis. Returns diagnostics with source locations.
  // If the analyzer reports errors, surface them and short-circuit before
  // compilation runs. As Phase C migrations land, more checks fire here
  // (with locations) instead of throwing during compilation.
  {
    auto diags = analyzer::analyze_statement(stmt, catalog, normalized);
    if (!diags.empty()) {
      CompilationResult r{};
      for (const auto& d : diags) {
        r.errors.push_back({d.message, d.loc.line, d.loc.column,
                            d.loc.end_line, d.loc.end_column});
      }
      return r;
    }
  }

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
      if (has_pk) return handle_create_table(*s);
      return handle_create_stream(*s);
    }
    if (auto* s = std::get_if<parser::ast::CreateViewStmt>(&stmt)) {
      return handle_create_mat_view(*s, catalog);
    }
    if (auto* s = std::get_if<parser::ast::InsertStmt>(&stmt)) {
      auto cat = snapshot_to_catalog(catalog);
      return handle_insert(*s, cat, catalog);
    }
    if (auto* s = std::get_if<parser::ast::DropStmt>(&stmt)) {
      return handle_drop(*s, catalog);
    }
    if (auto* s = std::get_if<parser::ast::DeleteStmt>(&stmt)) {
      return handle_delete(*s, catalog);
    }
    // Statement is a closed std::variant — every alternative is handled
    // above. Reaching this point would mean a new variant was added
    // without a matching dispatch arm.
    unreachable();
  } catch (const std::exception& e) {
    return make_error(e.what());
  }
}

// ---------------------------------------------------------------------------
// compile_session_program: consolidate all views in the catalog into one
// rtbot Program. Each view's per-view program_json (still produced by
// compile_sql at DDL time) is parsed, its Input/Output operators are
// stripped, the body is inlined under a view-specific operator-id prefix,
// and view-to-view edges are wired directly across the join. Exactly one
// Input operator is retained per base stream. Materialized views are
// exposed as named outputs so Program attaches a Collector to each.
// ---------------------------------------------------------------------------

namespace {

// Topologically order views by their FROM dependencies (views-on-views).
// Returns a vector of view names in dependency order. If the graph has a
// cycle, returns the subset that could be ordered and sets `cycle` to true.
std::vector<std::string> topo_sort_views(const CatalogSnapshot& catalog,
                                         bool& cycle) {
  cycle = false;
  std::map<std::string, int> indeg;
  std::map<std::string, std::vector<std::string>> children;
  for (const auto& [name, _] : catalog.views) {
    indeg[name] = 0;
  }
  for (const auto& [name, view] : catalog.views) {
    for (const auto& src : view.source_streams) {
      if (catalog.views.count(src)) {
        // src is an upstream view — src must be emitted before `name`.
        indeg[name]++;
        children[src].push_back(name);
      }
    }
  }
  std::vector<std::string> order;
  std::vector<std::string> queue;
  for (const auto& [name, deg] : indeg) {
    if (deg == 0) queue.push_back(name);
  }
  while (!queue.empty()) {
    std::string cur = queue.front();
    queue.erase(queue.begin());
    order.push_back(cur);
    for (const auto& ch : children[cur]) {
      if (--indeg[ch] == 0) queue.push_back(ch);
    }
  }
  if (order.size() != catalog.views.size()) cycle = true;
  return order;
}

}  // namespace

SessionCompilationResult compile_session_program(
    const CatalogSnapshot& catalog) {
  SessionCompilationResult out{};

  // 1. Topological order of views.
  bool cycle = false;
  auto order = topo_sort_views(catalog, cycle);
  if (cycle) {
    out.errors.push_back({"view dependency graph has a cycle", -1, -1});
    return out;
  }

  // 2. Collect external sources referenced across all views, in a
  //    deterministic (topo-visit) order. Both streams and tables are
  //    treated as session-level inputs: each gets its own port on the
  //    shared session Input operator. Views that JOIN a table (whose
  //    per-view program has a secondary port consuming table changelog
  //    messages) wire that port to the table's session-level entry, so
  //    INSERT INTO table_name routes through the same consolidated
  //    Program as stream inserts.
  std::vector<std::string> base_streams;
  {
    std::set<std::string> seen;
    for (const auto& vname : order) {
      const auto& view = catalog.views.at(vname);
      for (const auto& src : view.source_streams) {
        if (seen.count(src)) continue;
        if (catalog.streams.count(src) || catalog.tables.count(src)) {
          seen.insert(src);
          base_streams.push_back(src);
        }
      }
    }
  }
  if (base_streams.empty()) {
    out.errors.push_back(
        {"session has no base streams — cannot emit entry", -1, -1});
    return out;
  }

  compiler::GraphBuilder merged;

  // 3. Single session-level Input with one port per base stream. The
  //    runtime addresses each stream via its port id (i1, i2, …).
  const std::string session_input_id = "input__session";
  {
    std::map<std::string, double> params;
    if (base_streams.size() > 1) {
      params["numInputPorts"] =
          static_cast<double>(base_streams.size());
    }
    merged.add_operator(session_input_id, "Input", params);
  }
  std::map<std::string, int> stream_port_index;
  for (std::size_t i = 0; i < base_streams.size(); ++i) {
    stream_port_index[base_streams[i]] = static_cast<int>(i);
    out.base_stream_inputs[base_streams[i]] = session_input_id;
    out.base_stream_ports[base_streams[i]] =
        "i" + std::to_string(i + 1);
  }

  // view name → endpoint feeding its terminal in the merged graph.
  std::map<std::string, compiler::Endpoint> view_terminal;
  // terminal operator id → list of output ports to expose as Program
  // outputs (currently always one port per materialized view).
  std::map<std::string, std::vector<std::string>> named_outputs;

  // 4. Merge each view's subgraph in topological order. Views may have
  //    multiple sources (cross-select); each source_streams[i]
  //    corresponds to the i-th port on the view-local Input operator
  //    and is wired to the session-level endpoint for that source.
  for (const auto& vname : order) {
    const auto& view = catalog.views.at(vname);

    if (view.program_json.empty()) {
      out.errors.push_back(
          {"view '" + vname + "' has no program_json", -1, -1});
      return out;
    }

    // Resolve each source to a merged-graph endpoint. Stream sources
    // map to their session-Input port; view sources map to the upstream
    // view's terminal. Table sources (Stream JOIN Table) are not yet
    // supported — the changelog-stream wiring needs its own pass.
    std::vector<compiler::Endpoint> source_endpoints;
    source_endpoints.reserve(view.source_streams.size());
    for (const auto& src : view.source_streams) {
      auto stream_it = stream_port_index.find(src);
      if (stream_it != stream_port_index.end()) {
        source_endpoints.push_back(
            {session_input_id,
             "o" + std::to_string(stream_it->second + 1)});
        continue;
      }
      auto view_it = view_terminal.find(src);
      if (view_it != view_terminal.end()) {
        source_endpoints.push_back(view_it->second);
        continue;
      }
      out.errors.push_back(
          {"view '" + vname + "' references unknown source '" + src + "'",
           -1, -1});
      return out;
    }

    // Parse the view's stored program, strip the Output connection.
    compiler::GraphBuilder per_view;
    compiler::Endpoint pre_output_ep;
    try {
      auto parsed = compiler::GraphBuilder::from_json_for_augmentation(
          view.program_json);
      per_view = std::move(parsed.first);
      pre_output_ep = std::move(parsed.second);
    } catch (const std::exception& e) {
      out.errors.push_back(
          {"failed to parse view '" + vname + "' program: " + e.what(),
           -1, -1});
      return out;
    }

    // Locate the view-local Input operator (to strip and rewire).
    std::string local_input;
    for (const auto& op : per_view.operators()) {
      if (op.type == "Input") {
        local_input = op.id;
        break;
      }
    }

    const std::string prefix = vname + "__";

    // Clone prototypes with view-prefixed ids (operator refs below get
    // rewritten to the new prototype id). Inner operators in the prototype
    // are in a separate scope and do not need renaming.
    std::map<std::string, std::string> proto_rename;
    for (const auto& proto : per_view.prototypes()) {
      compiler::PrototypeDef cloned = proto;
      cloned.id = prefix + proto.id;
      proto_rename[proto.id] = cloned.id;
      merged.add_prototype(cloned);
    }

    // Copy operators (skip Input/Output), prefixing ids and rewriting
    // any prototype reference.
    for (const auto& op : per_view.operators()) {
      if (op.type == "Input" || op.type == "Output") continue;
      compiler::OperatorDef cloned = op;
      cloned.id = prefix + op.id;
      auto proto_it = cloned.string_params.find("prototype");
      if (proto_it != cloned.string_params.end()) {
        auto r = proto_rename.find(proto_it->second);
        if (r != proto_rename.end()) proto_it->second = r->second;
      }
      merged.add_operator(cloned.id, cloned.type, cloned.params,
                          cloned.string_params, cloned.double_array_params,
                          cloned.int_array_params);
    }

    // Copy connections, skipping those originating from the view's local
    // Input (rewired below) and those to the dropped Output (already
    // absent from per_view.connections()).
    for (const auto& c : per_view.connections()) {
      if (c.from_id == local_input) continue;
      merged.connect({prefix + c.from_id, c.from_port},
                     {prefix + c.to_id, c.to_port});
    }

    // Wire each local-Input outgoing edge from its corresponding source
    // endpoint. Port "oN" on the local Input corresponds to
    // source_streams[N-1]; we use that to index into source_endpoints.
    for (const auto& c : per_view.connections()) {
      if (c.from_id != local_input) continue;
      int port_idx = -1;
      if (c.from_port.size() >= 2 && c.from_port[0] == 'o') {
        try {
          port_idx = std::stoi(c.from_port.substr(1)) - 1;
        } catch (...) {
        }
      }
      if (port_idx < 0 ||
          port_idx >= static_cast<int>(source_endpoints.size())) {
        out.errors.push_back(
            {"view '" + vname + "' input port '" + c.from_port +
                 "' has no corresponding source_streams entry",
             -1, -1});
        return out;
      }
      merged.connect(source_endpoints[static_cast<std::size_t>(port_idx)],
                     {prefix + c.to_id, c.to_port});
    }

    // Record terminal for dependents and session-level outputs.
    compiler::Endpoint merged_terminal{prefix + pre_output_ep.operator_id,
                                       pre_output_ep.port};
    view_terminal[vname] = merged_terminal;
    out.view_terminals[vname] = merged_terminal.operator_id;
    out.view_terminal_ports[vname] = merged_terminal.port;

    // Every view — plain or materialized — gets its terminal exposed
    // as a Program output. Subscribers on plain views (e.g. observers
    // upstream of further views) need the same Collector-sink wiring
    // that materialized views get. Collectors are passive taps that
    // don't steal data from downstream operators, so this does not
    // change dataflow semantics.
    named_outputs[merged_terminal.operator_id].push_back(
        merged_terminal.port);
    if (view.entity_type == EntityType::MATERIALIZED_VIEW) {
      out.materialized_views.push_back(vname);
    }
  }

  // 5. Entry is always the single session-level Input (one op, multi-port).
  const std::string entry_op_id = session_input_id;

  // 7. Validate the merged graph (without requiring Input/Output presence;
  //    exactly one Input is always present here, but no Output is).
  auto verrors = merged.validate_session();
  if (!verrors.empty()) {
    for (const auto& e : verrors) {
      out.errors.push_back({"graph: " + e, -1, -1});
    }
    return out;
  }

  // 8. Emit JSON with explicit entry + named outputs.
  out.program_json = merged.to_json_session(entry_op_id, named_outputs);
  return out;
}

// ---------------------------------------------------------------------------
// compile_sql_expanded: preprocess + compile loop with catalog updates
// ---------------------------------------------------------------------------

namespace {

// Update catalog snapshot from a successful CompilationResult so that
// subsequent statements in the same expansion can resolve names.
void apply_result_to_catalog(CatalogSnapshot& catalog,
                             const CompilationResult& r) {
  switch (r.statement_type) {
    case StatementType::SELECT_STREAM: {
      catalog.streams[r.entity_name] = r.stream_schema;
      break;
    }
    case StatementType::CREATE_VIEW:
    case StatementType::CREATE_MATERIALIZED_VIEW: {
      ViewMeta vm;
      vm.name = r.entity_name;
      vm.entity_type = (r.statement_type == StatementType::CREATE_VIEW)
                            ? EntityType::VIEW
                            : EntityType::MATERIALIZED_VIEW;
      vm.view_type = r.view_type;
      vm.field_map = r.field_map;
      vm.source_streams = r.source_streams;
      vm.program_json = r.program_json;
      vm.key_index = r.key_index;
      catalog.views[r.entity_name] = vm;
      break;
    }
    case StatementType::CREATE_TABLE: {
      catalog.tables[r.entity_name] = r.table_schema;
      break;
    }
    case StatementType::DROP: {
      catalog.streams.erase(r.drop_entity_name);
      catalog.views.erase(r.drop_entity_name);
      catalog.tables.erase(r.drop_entity_name);
      break;
    }
    default:
      break;
  }
}

}  // namespace

ExpandedCompilationResult compile_sql_expanded(
    const std::string& sql, const CatalogSnapshot& catalog,
    int64_t ts_units_per_second) {
  ExpandedCompilationResult out;

  // Step 1: Preprocess (expand sugar, handle SET TIMESCALE)
  auto pp = preprocess_sql(sql, ts_units_per_second);

  if (pp.new_ts_units_per_second > 0) {
    out.new_ts_units_per_second = pp.new_ts_units_per_second;
    return out;  // SET TIMESCALE — no statements to compile
  }

  // Step 2: Compile each expanded statement, updating catalog between steps
  CatalogSnapshot evolving_catalog = catalog;

  for (const auto& stmt : pp.statements) {
    auto result = compile_sql(stmt, evolving_catalog);
    if (result.has_errors()) {
      // Stop on first error — return just this error result
      out.results.push_back(std::move(result));
      return out;
    }
    apply_result_to_catalog(evolving_catalog, result);
    out.results.push_back(std::move(result));
  }

  return out;
}

Tier2FilterResult apply_tier2_filter(
    const std::string& sql, const CatalogSnapshot& catalog,
    const std::vector<std::vector<double>>& input_rows, int limit) {
  Tier2FilterResult out;

  const std::string normalized = normalize_sql(sql);

  // Parse and identify the statement
  auto json_result = pg_query_parse(normalized.c_str());
  if (json_result.error) {
    pg_query_free_parse_result(json_result);
    out.rows = input_rows;
    return out;
  }

  parser::ast::Statement stmt;
  try {
    stmt = parser::convert_parse_tree(normalized, json_result.parse_tree);
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
