#include <emscripten/bind.h>

#include <nlohmann/json.hpp>
#include <regex>
#include <string>

#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/preprocessor.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/parser.h"

using json = nlohmann::json;
using namespace rtbot_sql;

namespace {

// Normalize RTBot SQL dialect keywords to their PostgreSQL equivalents so that
// libpg_query (a PostgreSQL 17 grammar wrapper) can parse them successfully.
// CREATE STREAM → CREATE TABLE, DROP STREAM → DROP TABLE.
std::string normalize_sql(const std::string& sql) {
  std::string out = sql;
  static const std::regex kCreateStream(R"(\bCREATE\s+STREAM\b)",
                                        std::regex::icase);
  out = std::regex_replace(out, kCreateStream, "CREATE TABLE");
  static const std::regex kDropStream(R"(\bDROP\s+STREAM\b)",
                                      std::regex::icase);
  out = std::regex_replace(out, kDropStream, "DROP TABLE");
  return out;
}

// --- JSON serialization for CatalogSnapshot (input) ---

ColumnDef column_def_from_json(const json& j) {
  ColumnType col_type = ColumnType::DOUBLE;
  if (j.contains("type") && j["type"].is_string()) {
    if (j["type"] == "TEXT") col_type = ColumnType::TEXT;
  }
  return {j.at("name").get<std::string>(), j.at("index").get<int>(), col_type};
}

StreamSchema stream_schema_from_json(const json& j) {
  StreamSchema s;
  s.name = j.at("name").get<std::string>();
  for (const auto& col : j.at("columns")) {
    s.columns.push_back(column_def_from_json(col));
  }
  return s;
}

ViewMeta view_meta_from_json(const json& j) {
  ViewMeta v;
  v.name = j.at("name").get<std::string>();

  auto et = j.at("entity_type").get<std::string>();
  if (et == "STREAM")
    v.entity_type = EntityType::STREAM;
  else if (et == "VIEW")
    v.entity_type = EntityType::VIEW;
  else if (et == "MATERIALIZED_VIEW")
    v.entity_type = EntityType::MATERIALIZED_VIEW;
  else
    v.entity_type = EntityType::TABLE;

  auto vt = j.at("view_type").get<std::string>();
  if (vt == "KEYED")
    v.view_type = ViewType::KEYED;
  else if (vt == "TOPK")
    v.view_type = ViewType::TOPK;
  else
    v.view_type = ViewType::SCALAR;

  v.field_map = j.at("field_map").get<std::map<std::string, int>>();
  v.source_streams =
      j.at("source_streams").get<std::vector<std::string>>();
  v.program_json = j.value("program_json", "");
  v.output_stream = j.value("output_stream", "");
  v.per_key_prefix = j.value("per_key_prefix", "");
  v.known_keys = j.value("known_keys", std::vector<double>{});
  v.key_index = j.value("key_index", -1);
  return v;
}

TableSchema table_schema_from_json(const json& j) {
  TableSchema t;
  t.name = j.at("name").get<std::string>();
  for (const auto& col : j.at("columns")) {
    t.columns.push_back(column_def_from_json(col));
  }
  t.changelog_stream = j.value("changelog_stream", "");
  t.key_columns = j.value("key_columns", std::vector<int>{});
  return t;
}

CatalogSnapshot catalog_from_json(const std::string& catalog_json) {
  CatalogSnapshot snap;
  if (catalog_json.empty()) return snap;

  auto j = json::parse(catalog_json);

  if (j.contains("streams")) {
    for (const auto& [name, val] : j["streams"].items()) {
      snap.streams[name] = stream_schema_from_json(val);
    }
  }
  if (j.contains("views")) {
    for (const auto& [name, val] : j["views"].items()) {
      snap.views[name] = view_meta_from_json(val);
    }
  }
  if (j.contains("tables")) {
    for (const auto& [name, val] : j["tables"].items()) {
      snap.tables[name] = table_schema_from_json(val);
    }
  }
  if (j.contains("dictionaries") && j["dictionaries"].is_object()) {
    for (auto& [key, entries] : j["dictionaries"].items()) {
      std::map<double, std::string> dict_entries;
      for (auto& [id_str, str_val] : entries.items()) {
        dict_entries[std::stod(id_str)] = str_val.get<std::string>();
      }
      snap.dictionaries[key] = dict_entries;
    }
  }
  return snap;
}

// --- JSON serialization for CompilationResult (output) ---

std::string statement_type_str(StatementType t) {
  switch (t) {
    case StatementType::CREATE_STREAM:
      return "CREATE_STREAM";
    case StatementType::CREATE_VIEW:
      return "CREATE_VIEW";
    case StatementType::CREATE_MATERIALIZED_VIEW:
      return "CREATE_MATERIALIZED_VIEW";
    case StatementType::CREATE_TABLE:
      return "CREATE_TABLE";
    case StatementType::INSERT:
      return "INSERT";
    case StatementType::SELECT:
      return "SELECT";
    case StatementType::SUBSCRIBE:
      return "SUBSCRIBE";
    case StatementType::DROP:
      return "DROP";
    case StatementType::DELETE:
      return "DELETE";
  }
  return "UNKNOWN";
}

std::string view_type_str(ViewType t) {
  switch (t) {
    case ViewType::SCALAR:
      return "SCALAR";
    case ViewType::KEYED:
      return "KEYED";
    case ViewType::TOPK:
      return "TOPK";
  }
  return "UNKNOWN";
}

std::string entity_type_str(EntityType t) {
  switch (t) {
    case EntityType::STREAM:
      return "STREAM";
    case EntityType::VIEW:
      return "VIEW";
    case EntityType::MATERIALIZED_VIEW:
      return "MATERIALIZED_VIEW";
    case EntityType::TABLE:
      return "TABLE";
  }
  return "UNKNOWN";
}

std::string select_tier_str(SelectTier t) {
  switch (t) {
    case SelectTier::TIER1_READ:
      return "TIER1_READ";
    case SelectTier::TIER2_SCAN:
      return "TIER2_SCAN";
    case SelectTier::TIER3_EPHEMERAL:
      return "TIER3_EPHEMERAL";
  }
  return "UNKNOWN";
}

json result_to_json(const CompilationResult& r) {
  json j;

  // Errors
  json errs = json::array();
  for (const auto& e : r.errors) {
    errs.push_back({{"message", e.message},
                    {"line", e.line},
                    {"column", e.column},
                    {"end_line", e.end_line},
                    {"end_column", e.end_column}});
  }
  j["errors"] = errs;

  if (r.has_errors()) return j;

  j["statement_type"] = statement_type_str(r.statement_type);
  j["entity_name"] = r.entity_name;
  j["program_json"] = r.program_json;
  j["field_map"] = r.field_map;
  j["source_streams"] = r.source_streams;
  j["view_type"] = view_type_str(r.view_type);
  j["key_index"] = r.key_index;
  j["select_tier"] = select_tier_str(r.select_tier);
  j["insert_payload"] = r.insert_payload;

  // stream_schema
  json schema;
  schema["name"] = r.stream_schema.name;
  json cols = json::array();
  for (const auto& c : r.stream_schema.columns) {
    cols.push_back({{"name", c.name}, {"index", c.index},
                    {"type", (c.type == ColumnType::TEXT) ? "TEXT" : "DOUBLE"}});
  }
  schema["columns"] = cols;
  if (r.stream_schema.source.has_value())
    schema["source"] = r.stream_schema.source.value();
  j["stream_schema"] = schema;

  // table_schema
  json tschema;
  tschema["name"] = r.table_schema.name;
  json tcols = json::array();
  for (const auto& c : r.table_schema.columns) {
    tcols.push_back({{"name", c.name}, {"index", c.index},
                     {"type", (c.type == ColumnType::TEXT) ? "TEXT" : "DOUBLE"}});
  }
  tschema["columns"] = tcols;
  tschema["changelog_stream"] = r.table_schema.changelog_stream;
  tschema["key_columns"] = r.table_schema.key_columns;
  j["table_schema"] = tschema;

  // drop info
  j["drop_entity_name"] = r.drop_entity_name;
  j["drop_entity_type"] = entity_type_str(r.drop_entity_type);

  // dictionary_updates
  if (!r.dictionary_updates.empty()) {
    json dict_json = json::object();
    for (const auto& [key, entries] : r.dictionary_updates) {
      json entry_json = json::object();
      for (const auto& [id, str] : entries) {
        entry_json[std::to_string(static_cast<int>(id))] = str;
      }
      dict_json[key] = entry_json;
    }
    j["dictionary_updates"] = dict_json;
  }

  return j;
}

// --- Exported functions ---

std::string compile_sql_json(const std::string& sql,
                             const std::string& catalog_json,
                             int ts_units_per_second) {
  try {
    auto catalog = catalog_from_json(catalog_json);
    auto expanded = api::compile_sql_expanded(
        sql, catalog, static_cast<int64_t>(ts_units_per_second));
    json j;
    json results_arr = json::array();
    for (const auto& r : expanded.results) {
      results_arr.push_back(result_to_json(r));
    }
    j["results"] = results_arr;
    j["new_ts_units_per_second"] = expanded.new_ts_units_per_second;
    return j.dump();
  } catch (const std::exception& e) {
    // Best-effort: try to parse so we can report a real source span.
    int line = -1, column = -1, end_line = -1, end_column = -1;
    try {
      auto pr = parser::parse(normalize_sql(sql));
      if (!pr.errors.empty()) {
        const auto& loc = pr.errors[0].loc;
        line = loc.line;
        column = loc.column;
        end_line = loc.end_line;
        end_column = loc.end_column;
      }
      parser::free_result(pr);
    } catch (...) {
      // Ignore — fall back to sentinel locations.
    }
    json j;
    json results_arr = json::array();
    results_arr.push_back({{"errors",
                            {{{"message", e.what()},
                              {"line", line},
                              {"column", column},
                              {"end_line", end_line},
                              {"end_column", end_column}}}}});
    j["results"] = results_arr;
    j["new_ts_units_per_second"] = -1;
    return j.dump();
  }
}

std::string validate_sql(const std::string& sql) {
  try {
    auto parse_result = parser::parse(normalize_sql(sql));
    json j;
    j["valid"] = parse_result.ok();
    json errs = json::array();
    for (const auto& e : parse_result.errors) {
      errs.push_back({{"message", e.message},
                      {"line", e.loc.line},
                      {"column", e.loc.column},
                      {"end_line", e.loc.end_line},
                      {"end_column", e.loc.end_column}});
    }
    j["errors"] = errs;
    parser::free_result(parse_result);
    return j.dump();
  } catch (const std::exception& e) {
    json j;
    j["valid"] = false;
    j["errors"] = {{{"message", e.what()},
                    {"line", -1},
                    {"column", -1},
                    {"end_line", -1},
                    {"end_column", -1}}};
    return j.dump();
  }
}

std::string compile_session_json(const std::string& catalog_json) {
  try {
    auto catalog = catalog_from_json(catalog_json);
    auto r = api::compile_session_program(catalog);
    json j;
    j["program_json"] = r.program_json;
    j["view_terminals"] = r.view_terminals;
    j["view_terminal_ports"] = r.view_terminal_ports;
    j["materialized_views"] = r.materialized_views;
    j["base_stream_inputs"] = r.base_stream_inputs;
    j["base_stream_ports"] = r.base_stream_ports;
    json errs = json::array();
    for (const auto& e : r.errors) {
      errs.push_back({{"message", e.message},
                      {"line", e.line},
                      {"column", e.column},
                      {"end_line", e.end_line},
                      {"end_column", e.end_column}});
    }
    j["errors"] = errs;
    return j.dump();
  } catch (const std::exception& e) {
    json j;
    json errs = json::array();
    errs.push_back({{"message", e.what()}, {"line", -1}, {"column", -1}});
    j["errors"] = errs;
    return j.dump();
  }
}

std::string preprocess_sql_json(const std::string& sql,
                                int ts_units_per_second) {
  try {
    auto result = api::preprocess_sql(sql,
                                      static_cast<int64_t>(ts_units_per_second));
    json j;
    json stmts = json::array();
    for (const auto& s : result.statements) {
      stmts.push_back(s);
    }
    j["statements"] = stmts;
    j["new_ts_units_per_second"] = result.new_ts_units_per_second;
    return j.dump();
  } catch (const std::exception& e) {
    json err;
    err["error"] = e.what();
    return err.dump();
  }
}

}  // namespace

EMSCRIPTEN_BINDINGS(RtBotSql) {
  emscripten::function("compileSqlJson", &compile_sql_json);
  emscripten::function("compileSessionJson", &compile_session_json);
  emscripten::function("validateSql", &validate_sql);
  emscripten::function("preprocessSqlJson", &preprocess_sql_json);
}
