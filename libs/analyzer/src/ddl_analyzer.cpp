#include "rtbot_sql/analyzer/ddl_analyzer.h"

#include <algorithm>
#include <set>
#include <string>

namespace rtbot_sql::analyzer {

namespace {

// Lower-case helper.
std::string lower(const std::string& s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return out;
}

// Set of PostgreSQL internal type names accepted by the compiler. The
// compiler maps `text`/`varchar`/`bpchar` to TEXT and everything else
// matched here to DOUBLE; anything outside this set should be rejected as
// an unknown column type rather than silently becoming DOUBLE.
//
// libpg_query lowercases user-typed types and translates common surface
// syntax to internal names: INT → int4, BIGINT → int8, REAL → float4,
// DOUBLE PRECISION → float8, BOOLEAN → bool, etc.
bool is_known_column_type(const std::string& name) {
  std::string n = lower(name);
  // Numeric
  if (n == "int2" || n == "int4" || n == "int8") return true;
  if (n == "smallint" || n == "integer" || n == "int" || n == "bigint")
    return true;
  if (n == "float4" || n == "float8" || n == "real") return true;
  if (n == "double" || n == "double precision") return true;
  if (n == "numeric" || n == "decimal") return true;
  // Text
  if (n == "text" || n == "varchar" || n == "bpchar" || n == "char") return true;
  // Boolean
  if (n == "bool" || n == "boolean") return true;
  return false;
}

// Mirrors the compiler's TEXT mapping (libs/api/src/compiler.cpp,
// handle_create_stream): text/varchar/bpchar become ColumnType::TEXT.
bool is_text_column_type(const std::string& name) {
  std::string n = lower(name);
  return n == "text" || n == "varchar" || n == "bpchar";
}

}  // namespace

void analyze_create_stream(const parser::ast::CreateStreamStmt& stmt,
                           const CatalogSnapshot& /*catalog*/,
                           DiagnosticBag& bag) {
  // --- CREATE STREAM source metadata (FROM "..." [TYPE …] [WINDOW …]) ---
  if (stmt.source.has_value()) {
    const auto& src = *stmt.source;

    // Unknown TYPE value — points at the bad identifier.
    if (src.type.has_value() &&
        src.type->value == parser::ast::SourceType::UNKNOWN) {
      parser::ast::SourceLocation loc;
      loc.line = src.type->line;
      loc.column = src.type->column;
      loc.end_line = src.type->end_line;
      loc.end_column = src.type->end_column;
      bag.error(
          "CREATE STREAM: unknown TYPE '" + src.type->raw +
              "', expected 'scalar' or 'csv_burst'",
          loc);
    } else {
      // Only run csv_burst↔WINDOW correlation when TYPE is well-defined.
      // CSV_BURST requires WINDOW.
      if (src.effective_type() == parser::ast::SourceType::CSV_BURST &&
          !src.window.has_value()) {
        parser::ast::SourceLocation loc;
        loc.line = src.line;
        loc.column = src.column;
        loc.end_line = src.end_line;
        loc.end_column = src.end_column;
        bag.error("CREATE STREAM: TYPE csv_burst requires WINDOW", loc);
      }
      // WINDOW only valid with csv_burst.
      if (src.window.has_value() &&
          src.effective_type() != parser::ast::SourceType::CSV_BURST) {
        parser::ast::SourceLocation loc;
        loc.line = src.window->line;
        loc.column = src.window->column;
        loc.end_line = src.window->end_line;
        loc.end_column = src.window->end_column;
        bag.error(
            "CREATE STREAM: WINDOW is only valid with TYPE csv_burst", loc);
      }
      // WINDOW value must be strictly positive.
      if (src.window.has_value() && src.window->value <= 0) {
        parser::ast::SourceLocation loc;
        loc.line = src.window->line;
        loc.column = src.window->column;
        loc.end_line = src.window->end_line;
        loc.end_column = src.window->end_column;
        bag.error("CREATE STREAM: WINDOW must be a positive integer", loc);
      }
    }

    // `{col}` placeholders must reference declared TEXT columns — they
    // resolve to path segments of the matched tag path, which are strings.
    for (const auto& ph : src.placeholders) {
      const parser::ast::ColumnDefAST* col = nullptr;
      for (const auto& c : stmt.columns) {
        if (c.name == ph.name) {
          col = &c;
          break;
        }
      }
      parser::ast::SourceLocation loc;
      loc.line = ph.line;
      loc.column = ph.column;
      loc.end_line = ph.end_line;
      loc.end_column = ph.end_column;
      if (col == nullptr) {
        bag.error("CREATE STREAM: FROM source references unknown column '" +
                      ph.name + "'",
                  loc);
      } else if (!is_text_column_type(col->type_name)) {
        bag.error("CREATE STREAM: FROM placeholder '{" + ph.name +
                      "}' requires a TEXT column",
                  loc);
      }
    }
  }

  // Composite primary keys are not yet supported. Mirrors the throw at
  // libs/api/src/compiler.cpp:467-468.
  int pk_count = 0;
  parser::ast::SourceLocation second_pk_loc;
  for (const auto& col : stmt.columns) {
    if (col.primary_key) {
      ++pk_count;
      if (pk_count == 2) second_pk_loc = col.loc;
    }
  }
  if (pk_count > 1) {
    bag.error("Composite table keys are not yet supported", second_pk_loc);
  }

  // Item 3: duplicate column names within the same CREATE statement.
  // Postgres lowercases unquoted identifiers, so compare case-insensitively
  // against what already came through the converter.
  std::set<std::string> seen;
  for (const auto& col : stmt.columns) {
    std::string key = lower(col.name);
    if (seen.count(key)) {
      bag.error("duplicate column name '" + col.name + "' in '" + stmt.name +
                    "'",
                col.loc);
    } else {
      seen.insert(key);
    }
  }

  // Item 5: unknown column types. The compiler silently maps anything it
  // doesn't recognize to DOUBLE; reject explicitly so typos like
  // `(value BANANA)` surface to the user.
  for (const auto& col : stmt.columns) {
    if (!is_known_column_type(col.type_name)) {
      bag.error("unknown column type '" + col.type_name + "' for column '" +
                    col.name + "'",
                col.loc);
    }
  }
}

}  // namespace rtbot_sql::analyzer
