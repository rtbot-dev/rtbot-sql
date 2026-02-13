#pragma once

#include <string>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::parser {

// Convert a pg_query JSON parse tree string into our AST Statement.
// Throws std::runtime_error on unsupported or malformed input.
ast::Statement convert_parse_tree(const std::string& json_str);

}  // namespace rtbot_sql::parser
