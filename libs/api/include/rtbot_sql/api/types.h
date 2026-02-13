#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace rtbot_sql {

enum class ViewType { SCALAR, KEYED, TOPK };

enum class EntityType { STREAM, VIEW, MATERIALIZED_VIEW, TABLE };

enum class StatementType {
  CREATE_STREAM,
  CREATE_VIEW,
  CREATE_MATERIALIZED_VIEW,
  CREATE_TABLE,
  INSERT,
  SELECT,
  SUBSCRIBE,
  DROP,
};

enum class SelectTier { TIER1_READ, TIER2_SCAN, TIER3_EPHEMERAL };

struct ColumnDef {
  std::string name;
  int index;  // position in the vector_number
  // All columns are DOUBLE (RTBot is numeric-only)
};

struct StreamSchema {
  std::string name;
  std::vector<ColumnDef> columns;

  std::optional<int> column_index(const std::string& col_name) const {
    for (const auto& col : columns) {
      if (col.name == col_name) {
        return col.index;
      }
    }
    return std::nullopt;
  }
};

struct ViewMeta {
  std::string name;
  EntityType entity_type;
  ViewType view_type;
  std::map<std::string, int> field_map;
  std::vector<std::string> source_streams;
  std::string program_json;
  std::string output_stream;
  std::string per_key_prefix;
  std::vector<double> known_keys;
  int key_index;
};

struct TableSchema {
  std::string name;
  std::vector<ColumnDef> columns;
  std::string changelog_stream;
};

struct CatalogSnapshot {
  std::map<std::string, StreamSchema> streams;
  std::map<std::string, ViewMeta> views;
  std::map<std::string, TableSchema> tables;
};

struct CompilationError {
  std::string message;
  int line;    // -1 if not applicable
  int column;  // -1 if not applicable
};

struct CompilationResult {
  StatementType statement_type;
  std::string program_json;
  std::map<std::string, int> field_map;
  std::vector<std::string> source_streams;
  ViewType view_type;
  int key_index;
  SelectTier select_tier;
  std::vector<double> insert_payload;
  StreamSchema stream_schema;
  std::string entity_name;
  std::string drop_entity_name;
  EntityType drop_entity_type;
  std::vector<CompilationError> errors;

  bool has_errors() const { return !errors.empty(); }
};

}  // namespace rtbot_sql
