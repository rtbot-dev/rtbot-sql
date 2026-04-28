#pragma once

#include <string>
#include <vector>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::parser {

// Pre-computed line-offset table over an SQL source string. Holds the byte
// offset of the start of each line, allowing O(log N) byte-offset → (line,
// column) conversion via SourceText::compute_location.
struct SourceText {
  std::string sql;
  // line_offsets[0] = 0; line_offsets[i] = byte position immediately after
  // the i-th newline (i.e. the start of line i+1).
  std::vector<int> line_offsets;
};

// Build a SourceText by scanning sql once. The SourceText keeps its own copy
// of the SQL to remain valid independently of the caller's string lifetime.
SourceText make_source_text(std::string sql);

// Convert a libpg_query byte offset into (line, column), both 1-based.
// Returns {-1, -1} for negative offsets or offsets past end-of-string.
ast::SourceLocation compute_location(const SourceText& src, int byte_offset);

}  // namespace rtbot_sql::parser
