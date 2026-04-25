#pragma once

#include <string>
#include <vector>

#include "pg_query.h"

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::parser {

// Single libpg_query syntax error with optional source location.
//
// Provides .empty() so existing code that read ParseResult::errors as a
// vector<string> (and called `errors[i].empty()`) continues to compile.
struct ParseError {
  std::string message;
  ast::SourceLocation loc;

  bool empty() const { return message.empty(); }
};

struct ParseResult {
  PgQueryProtobufParseResult result;
  std::vector<ParseError> errors;

  bool ok() const { return errors.empty(); }
  const PgQueryProtobuf& protobuf() const { return result.parse_tree; }
};

ParseResult parse(const std::string& sql);
void free_result(ParseResult& r);

}  // namespace rtbot_sql::parser
