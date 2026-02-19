#include <emscripten/bind.h>

#include <nlohmann/json.hpp>
#include <string>

#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/parser.h"

using json = nlohmann::json;
using namespace rtbot_sql;

namespace {

// --- JSON serialization for CatalogSnapshot (input) ---

ColumnDef column_def_from_json(const json& j) {
  return {j.at("name").get<std::string>(), j.at("index").get<int>()};
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
    errs.push_back(
        {{"message", e.message}, {"line", e.line}, {"column", e.column}});
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
    cols.push_back({{"name", c.name}, {"index", c.index}});
  }
  schema["columns"] = cols;
  j["stream_schema"] = schema;

  // table_schema
  json tschema;
  tschema["name"] = r.table_schema.name;
  json tcols = json::array();
  for (const auto& c : r.table_schema.columns) {
    tcols.push_back({{"name", c.name}, {"index", c.index}});
  }
  tschema["columns"] = tcols;
  tschema["changelog_stream"] = r.table_schema.changelog_stream;
  tschema["key_columns"] = r.table_schema.key_columns;
  j["table_schema"] = tschema;

  // drop info
  j["drop_entity_name"] = r.drop_entity_name;
  j["drop_entity_type"] = entity_type_str(r.drop_entity_type);

  return j;
}

// --- Exported functions ---

std::string compile_sql_json(const std::string& sql,
                             const std::string& catalog_json) {
  try {
    auto catalog = catalog_from_json(catalog_json);
    auto result = api::compile_sql(sql, catalog);
    return result_to_json(result).dump();
  } catch (const std::exception& e) {
    json err;
    err["errors"] = {{{"message", e.what()}, {"line", -1}, {"column", -1}}};
    return err.dump();
  }
}

std::string validate_sql(const std::string& sql) {
  try {
    auto parse_result = parser::parse(sql);
    json j;
    j["valid"] = parse_result.ok();
    json errs = json::array();
    for (const auto& e : parse_result.errors) {
      errs.push_back(
          {{"message", e}, {"line", -1}, {"column", -1}});
    }
    j["errors"] = errs;
    parser::free_result(parse_result);
    return j.dump();
  } catch (const std::exception& e) {
    json j;
    j["valid"] = false;
    j["errors"] = {{{"message", e.what()}, {"line", -1}, {"column", -1}}};
    return j.dump();
  }
}

}  // namespace

EMSCRIPTEN_BINDINGS(RtBotSql) {
  emscripten::function("compileSqlJson", &compile_sql_json);
  emscripten::function("validateSql", &validate_sql);
}
