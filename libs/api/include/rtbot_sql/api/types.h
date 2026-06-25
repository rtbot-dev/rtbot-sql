#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql {

// Source / SourceType / SourceTypeClause / SourceWindowClause live in
// parser::ast so the AST can carry them on CreateStreamStmt. Re-export here
// so existing api consumers keep using the bare names.
using parser::ast::Source;
using parser::ast::SourceType;
using parser::ast::SourceTypeClause;
using parser::ast::SourceWindowClause;

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
  DELETE,
};

enum class SelectTier { TIER1_READ, TIER2_SCAN, TIER3_EPHEMERAL };

enum class ColumnType { DOUBLE, TEXT };

struct ColumnDef {
  std::string name;
  int index;  // position in the vector_number
  ColumnType type = ColumnType::DOUBLE;
};

struct StreamSchema {
  std::string name;
  std::vector<ColumnDef> columns;
  std::optional<Source> source;  // FROM "..." metadata from CREATE STREAM

  std::optional<int> column_index(const std::string& col_name) const {
    for (const auto& col : columns) {
      if (col.name == col_name) {
        return col.index;
      }
    }
    return std::nullopt;
  }
};

// Maps a projected output field to the (source_stream, source_column)
// it was renamed from in the SELECT. Populated only for direct
// `column AS alias` projections — expressions (aggregates, arithmetic)
// have no single source column and are absent. Used by output decoders
// to look up the source TEXT column's StringDictionary so the alias
// receives the decoded string rather than the raw dictionary ID.
struct FieldOrigin {
  std::string source_stream;
  std::string source_column;
};

struct ViewMeta {
  std::string name;
  EntityType entity_type;
  ViewType view_type;
  std::map<std::string, int> field_map;
  std::map<std::string, FieldOrigin> field_origins;
  std::vector<std::string> source_streams;
  std::string program_json;
  std::string output_stream;
  std::string per_key_prefix;
  std::vector<double> known_keys;
  int key_index;

  // `TO '<template>'` output target metadata (materialized views only).
  // See CompilationResult for field semantics.
  std::optional<std::string> output_target;
  std::vector<std::string> output_payload_columns;
};

struct TableSchema {
  std::string name;
  std::vector<ColumnDef> columns;
  std::string changelog_stream;
  std::vector<int> key_columns;  // indices of primary key columns
};

struct CatalogSnapshot {
  std::map<std::string, StreamSchema> streams;
  std::map<std::string, ViewMeta> views;
  std::map<std::string, TableSchema> tables;
  std::map<std::string, std::map<double, std::string>> dictionaries;
};

struct CompilationError {
  std::string message;
  int line = -1;    // 1-based; -1 if not applicable
  int column = -1;  // 1-based; -1 if not applicable
  // End position is exclusive — points one character past the last
  // character of the offending token. Editors render this as a
  // selection from (line, column) to (end_line, end_column).
  int end_line = -1;
  int end_column = -1;
};

struct CompilationResult {
  StatementType statement_type;
  std::string program_json;
  std::map<std::string, int> field_map;
  // See FieldOrigin doc on ViewMeta. Mirror for the compile output so
  // host runtimes that don't round-trip via the catalog still get the
  // alias→source map needed for output decoding.
  std::map<std::string, FieldOrigin> field_origins;
  std::vector<std::string> source_streams;
  ViewType view_type;
  int key_index;
  SelectTier select_tier;
  int select_limit = -1;  // SQL LIMIT value (-1 = no limit)
  std::vector<double> insert_payload;
  StreamSchema stream_schema;
  TableSchema table_schema;        // for CREATE_TABLE
  std::vector<double> delete_payload;  // for DELETE: [key, NaN]
  std::string entity_name;
  std::string drop_entity_name;
  EntityType drop_entity_type;
  std::map<std::string, std::map<double, std::string>> dictionary_updates;

  // CREATE MATERIALIZED VIEW `TO '<template>'` output target.
  // `output_target` is the raw tag-path template (placeholders intact);
  // present only when the statement declared a TO clause. `{col}`
  // placeholders reference projected TEXT columns and resolve per row to
  // path segments. `output_payload_columns` are the view's NUMERIC
  // projected columns, in output order — the host writes one tag per
  // payload column under the resolved path (TEXT columns are path
  // material, never tag values).
  std::optional<std::string> output_target;
  std::vector<std::string> output_payload_columns;

  std::vector<CompilationError> errors;

  bool has_errors() const { return !errors.empty(); }
};

}  // namespace rtbot_sql
