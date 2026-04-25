#include "rtbot_sql/parser/source_text.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace rtbot_sql::parser {

namespace {

// Convert a byte offset into 1-based (line, column).
struct LineCol {
  int line;
  int column;
};
LineCol byte_offset_to_line_col(const std::vector<int>& line_offsets,
                                int byte_offset) {
  auto it = std::upper_bound(line_offsets.begin(), line_offsets.end(),
                             byte_offset);
  int line_idx = static_cast<int>(it - line_offsets.begin()) - 1;
  if (line_idx < 0) line_idx = 0;
  return {line_idx + 1, byte_offset - line_offsets[line_idx] + 1};
}

// Find the byte offset of the character immediately after the token that
// starts at `start`. Identifies tokens by the first character:
//   * letter or '_'   → identifier (alphanumeric + underscore)
//   * digit           → number (digits, dot, optional exponent)
//   * '\'' or '"'     → quoted string / identifier (until matching quote)
//   * comparison op   → 1-2 chars (handles `<=`, `>=`, `==`, `<>`, `!=`)
//   * other           → single character
//
// Conservative: when the token kind is ambiguous, returns start + 1.
int find_token_end(const std::string& sql, int start) {
  if (start < 0 || start >= static_cast<int>(sql.size())) return start;
  unsigned char c = static_cast<unsigned char>(sql[start]);
  int end = start + 1;

  if (std::isalpha(c) || c == '_') {
    while (end < static_cast<int>(sql.size())) {
      unsigned char ch = static_cast<unsigned char>(sql[end]);
      if (!std::isalnum(ch) && ch != '_') break;
      ++end;
    }
  } else if (std::isdigit(c)) {
    while (end < static_cast<int>(sql.size())) {
      unsigned char ch = static_cast<unsigned char>(sql[end]);
      if (!std::isdigit(ch) && ch != '.') break;
      ++end;
    }
    // Scientific notation (e.g. 1.5e-10).
    if (end < static_cast<int>(sql.size()) &&
        (sql[end] == 'e' || sql[end] == 'E')) {
      ++end;
      if (end < static_cast<int>(sql.size()) &&
          (sql[end] == '+' || sql[end] == '-')) {
        ++end;
      }
      while (end < static_cast<int>(sql.size()) &&
             std::isdigit(static_cast<unsigned char>(sql[end]))) {
        ++end;
      }
    }
  } else if (c == '\'') {
    while (end < static_cast<int>(sql.size()) && sql[end] != '\'') ++end;
    if (end < static_cast<int>(sql.size())) ++end;  // include closing quote
  } else if (c == '"') {
    while (end < static_cast<int>(sql.size()) && sql[end] != '"') ++end;
    if (end < static_cast<int>(sql.size())) ++end;
  } else if (c == '<' || c == '>' || c == '!' || c == '=') {
    if (end < static_cast<int>(sql.size()) && sql[end] == '=') ++end;
    else if (c == '<' && end < static_cast<int>(sql.size()) && sql[end] == '>')
      ++end;  // <>
  }
  return end;
}

}  // namespace

SourceText make_source_text(std::string sql) {
  SourceText src;
  src.sql = std::move(sql);
  src.line_offsets.push_back(0);
  for (int i = 0; i < static_cast<int>(src.sql.size()); ++i) {
    if (src.sql[i] == '\n') {
      src.line_offsets.push_back(i + 1);
    }
  }
  return src;
}

ast::SourceLocation compute_location(const SourceText& src, int byte_offset) {
  if (byte_offset < 0 ||
      byte_offset > static_cast<int>(src.sql.size())) {
    return {};
  }
  auto start = byte_offset_to_line_col(src.line_offsets, byte_offset);
  int end_offset = find_token_end(src.sql, byte_offset);
  auto end = byte_offset_to_line_col(src.line_offsets, end_offset);
  ast::SourceLocation loc;
  loc.line = start.line;
  loc.column = start.column;
  loc.end_line = end.line;
  loc.end_column = end.column;
  return loc;
}

}  // namespace rtbot_sql::parser
