#pragma once

#include <string>
#include <vector>

#include "pg_query.h"

namespace rtbot_sql::parser {

struct ParseResult {
  PgQueryProtobufParseResult result;
  std::vector<std::string> errors;

  bool ok() const { return errors.empty(); }
  const PgQueryProtobuf& protobuf() const { return result.parse_tree; }
};

ParseResult parse(const std::string& sql);
void free_result(ParseResult& r);

}  // namespace rtbot_sql::parser
