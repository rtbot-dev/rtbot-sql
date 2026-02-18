#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "args.h"
#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/types.h"

using namespace rtbot_sql;
using namespace rtbot_sql::cli;
using json = nlohmann::json;

namespace {

// Load a CatalogSnapshot from a JSON file.
CatalogSnapshot load_catalog(const std::string& path) {
  CatalogSnapshot catalog;
  if (path.empty()) return catalog;

  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    throw std::runtime_error("Cannot open catalog file: " + path);
  }

  auto j = json::parse(ifs);

  if (j.contains("streams")) {
    for (auto& [name, stream_j] : j["streams"].items()) {
      StreamSchema schema;
      schema.name = name;
      if (stream_j.is_array()) {
        // Simple format: {"trades": ["instrument_id", "price", "quantity"]}
        for (int i = 0; i < static_cast<int>(stream_j.size()); ++i) {
          schema.columns.push_back({stream_j[i].get<std::string>(), i});
        }
      } else if (stream_j.contains("columns")) {
        // Detailed format: {"trades": {"columns": [{"name": "price", "index": 0}]}}
        for (const auto& col_j : stream_j["columns"]) {
          schema.columns.push_back(
              {col_j["name"].get<std::string>(), col_j["index"].get<int>()});
        }
      }
      catalog.streams[name] = schema;
    }
  }

  return catalog;
}

// Serialize a CompilationResult to JSON.
json result_to_json(const CompilationResult& r) {
  json j;

  // Statement type
  static const char* stmt_names[] = {
      "CREATE_STREAM", "CREATE_VIEW", "CREATE_MATERIALIZED_VIEW",
      "CREATE_TABLE", "INSERT", "SELECT", "SUBSCRIBE", "DROP"};
  j["statement_type"] = stmt_names[static_cast<int>(r.statement_type)];

  if (!r.entity_name.empty()) {
    j["entity_name"] = r.entity_name;
  }

  // Errors
  if (r.has_errors()) {
    json errors = json::array();
    for (const auto& err : r.errors) {
      json e;
      e["message"] = err.message;
      if (err.line >= 0) e["line"] = err.line;
      if (err.column >= 0) e["column"] = err.column;
      errors.push_back(e);
    }
    j["errors"] = errors;
    return j;
  }

  // Statement-specific fields
  switch (r.statement_type) {
    case StatementType::CREATE_STREAM: {
      json cols = json::array();
      for (const auto& col : r.stream_schema.columns) {
        cols.push_back({{"name", col.name}, {"index", col.index}});
      }
      j["schema"] = cols;
      break;
    }
    case StatementType::CREATE_TABLE: {
      json cols = json::array();
      for (const auto& col : r.table_schema.columns) {
        cols.push_back({{"name", col.name}, {"index", col.index}});
      }
      j["schema"] = cols;
      j["key_columns"] = r.table_schema.key_columns;
      j["changelog_stream"] = r.table_schema.changelog_stream;
      break;
    }
    case StatementType::DELETE: {
      j["delete_payload"] = r.delete_payload;
      break;
    }
    case StatementType::INSERT: {
      j["values"] = r.insert_payload;
      break;
    }
    case StatementType::SELECT: {
      static const char* tier_names[] = {"TIER1_READ", "TIER2_SCAN",
                                         "TIER3_EPHEMERAL"};
      j["tier"] = tier_names[static_cast<int>(r.select_tier)];
      if (!r.field_map.empty()) j["field_map"] = r.field_map;
      if (!r.program_json.empty()) j["program"] = json::parse(r.program_json);
      if (!r.source_streams.empty()) j["source_streams"] = r.source_streams;
      break;
    }
    case StatementType::CREATE_MATERIALIZED_VIEW:
    case StatementType::CREATE_VIEW: {
      if (!r.field_map.empty()) j["field_map"] = r.field_map;
      if (!r.program_json.empty()) j["program"] = json::parse(r.program_json);
      if (!r.source_streams.empty()) j["source_streams"] = r.source_streams;
      static const char* vt_names[] = {"SCALAR", "KEYED", "TOPK"};
      j["view_type"] = vt_names[static_cast<int>(r.view_type)];
      if (r.key_index >= 0) j["key_index"] = r.key_index;
      break;
    }
    case StatementType::DROP: {
      j["drop_entity_name"] = r.drop_entity_name;
      static const char* et_names[] = {"STREAM", "VIEW", "MATERIALIZED_VIEW",
                                       "TABLE"};
      j["drop_entity_type"] = et_names[static_cast<int>(r.drop_entity_type)];
      break;
    }
    default:
      break;
  }

  return j;
}

void handle_compile(const CLIArguments& args) {
  auto catalog = load_catalog(args.catalog_file);
  auto result = api::compile_sql(args.sql, catalog);

  json output = result_to_json(result);

  int indent = (args.format == OutputFormat::JSON) ? 2 : -1;
  std::string json_str = output.dump(indent);

  if (!args.output_file.empty()) {
    std::ofstream ofs(args.output_file);
    if (!ofs.is_open()) {
      throw std::runtime_error("Cannot open output file: " + args.output_file);
    }
    ofs << json_str << "\n";
    if (args.verbose) {
      std::cerr << "Output written to " << args.output_file << std::endl;
    }
  } else {
    std::cout << json_str << std::endl;
  }

  if (result.has_errors()) {
    exit(1);
  }
}

void handle_repl(const CLIArguments& /*args*/) {
  std::cerr << "REPL mode not yet implemented. Coming in a future batch."
            << std::endl;
  exit(1);
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    auto args = CLIArguments::parse(argc, argv);

    switch (args.mode) {
      case Mode::COMPILE:
        handle_compile(args);
        break;
      case Mode::REPL:
        handle_repl(args);
        break;
    }

    return 0;
  } catch (const ArgumentException& e) {
    std::cerr << "Argument error: " << e.what() << std::endl;
    CLIArguments::print_usage();
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
