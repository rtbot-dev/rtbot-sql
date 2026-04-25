#pragma once

#include <stdexcept>
#include <string>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::parser {

// Exception thrown by convert_parse_tree when the libpg_query AST contains
// a node the converter doesn't yet support (e.g. BETWEEN, an unhandled
// statement kind). Carries the source location of the offending node so
// downstream error formatters can render it as a positional underline.
//
// `what()` returns the human-readable message; `loc()` returns the span
// (or {-1,-1,-1,-1} when no location is available, e.g. for an empty
// parse tree).
class ConverterError : public std::runtime_error {
 public:
  ConverterError(const std::string& msg, ast::SourceLocation loc)
      : std::runtime_error(msg), loc_(loc) {}
  const ast::SourceLocation& loc() const { return loc_; }

 private:
  ast::SourceLocation loc_;
};

// Convert a pg_query JSON parse tree string into our AST Statement.
// Throws ConverterError on unsupported or malformed input.
//
// This overload does not populate AST node SourceLocations
// (loc fields default to {-1, -1, -1, -1}). Used by tests that don't need
// locations and by legacy callers.
ast::Statement convert_parse_tree(const std::string& json_str);

// Same as above, but populates each AST node's `loc` field by reading
// libpg_query's `location` byte offsets from the JSON tree and converting
// them to (line, column) pairs against `source_sql`.
ast::Statement convert_parse_tree(const std::string& source_sql,
                                  const std::string& json_str);

}  // namespace rtbot_sql::parser
