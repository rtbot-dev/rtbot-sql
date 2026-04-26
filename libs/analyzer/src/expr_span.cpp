#include "rtbot_sql/analyzer/expr_span.h"

#include <cctype>
#include <variant>
#include <vector>

namespace rtbot_sql::analyzer {

namespace {

using parser::ast::Expr;
using parser::ast::SourceLocation;

bool before(int la, int ca, int lb, int cb) {
  return la < lb || (la == lb && ca < cb);
}

void merge(SourceLocation& acc, const SourceLocation& add) {
  if (add.line < 1 || add.column < 1) return;
  if (acc.line < 1 || acc.column < 1) {
    acc.line = add.line;
    acc.column = add.column;
  } else if (before(add.line, add.column, acc.line, acc.column)) {
    acc.line = add.line;
    acc.column = add.column;
  }
  if (add.end_line >= 1 && add.end_column >= 1) {
    if (acc.end_line < 1 || acc.end_column < 1 ||
        before(acc.end_line, acc.end_column, add.end_line, add.end_column)) {
      acc.end_line = add.end_line;
      acc.end_column = add.end_column;
    }
  }
}

std::vector<int> build_line_offsets(std::string_view sql) {
  std::vector<int> offsets{0};
  for (int i = 0; i < static_cast<int>(sql.size()); ++i) {
    if (sql[i] == '\n') offsets.push_back(i + 1);
  }
  return offsets;
}

// Convert (line, column) — both 1-based — to a 0-based byte offset.
// Returns -1 if (line, column) is out of range for `sql`.
int offset_of(std::string_view sql, const std::vector<int>& line_offsets,
              int line, int column) {
  if (line < 1 || column < 1) return -1;
  if (line > static_cast<int>(line_offsets.size())) return -1;
  int line_start = line_offsets[line - 1];
  int line_end = line < static_cast<int>(line_offsets.size())
                     ? line_offsets[line] - 1
                     : static_cast<int>(sql.size());
  int off = line_start + (column - 1);
  if (off > line_end) return -1;
  return off;
}

// Convert a 0-based byte offset to 1-based (line, column).
void line_col_of(const std::vector<int>& line_offsets, int byte_offset,
                 int& line, int& column) {
  int lo = 0;
  int hi = static_cast<int>(line_offsets.size()) - 1;
  while (lo < hi) {
    int mid = (lo + hi + 1) / 2;
    if (line_offsets[mid] <= byte_offset)
      lo = mid;
    else
      hi = mid - 1;
  }
  line = lo + 1;
  column = byte_offset - line_offsets[lo] + 1;
}

// Given the byte offset just past a function name, find the byte offset
// one past the matching `)` of its argument list. Counts nested parens
// and skips over single- and double-quoted regions. Returns -1 if no
// matching `)` is found.
int find_call_close(std::string_view sql, int after_name) {
  int n = static_cast<int>(sql.size());
  int i = after_name;
  while (i < n && std::isspace(static_cast<unsigned char>(sql[i]))) ++i;
  if (i >= n || sql[i] != '(') return -1;
  int depth = 1;
  ++i;
  while (i < n && depth > 0) {
    char c = sql[i];
    if (c == '\'') {
      ++i;
      while (i < n && sql[i] != '\'') ++i;
      if (i < n) ++i;
    } else if (c == '"') {
      ++i;
      while (i < n && sql[i] != '"') ++i;
      if (i < n) ++i;
    } else if (c == '(') {
      ++depth;
      ++i;
    } else if (c == ')') {
      --depth;
      ++i;
      if (depth == 0) return i;
    } else {
      ++i;
    }
  }
  return -1;
}

void walk(const Expr& e, SourceLocation& acc, std::string_view sql,
          const std::vector<int>& line_offsets);

void extend_past_func_call_close(const parser::ast::FuncCall& fc,
                                 SourceLocation& acc, std::string_view sql,
                                 const std::vector<int>& line_offsets) {
  if (fc.loc.end_line < 1 || fc.loc.end_column < 1 || sql.empty()) return;
  int after_name =
      offset_of(sql, line_offsets, fc.loc.end_line, fc.loc.end_column);
  if (after_name < 0) return;
  int close = find_call_close(sql, after_name);
  if (close <= 0) return;
  int el, ec;
  line_col_of(line_offsets, close, el, ec);
  if (acc.end_line < 1 || acc.end_column < 1 ||
      before(acc.end_line, acc.end_column, el, ec)) {
    acc.end_line = el;
    acc.end_column = ec;
  }
}

void walk(const Expr& e, SourceLocation& acc, std::string_view sql,
          const std::vector<int>& line_offsets) {
  std::visit(
      [&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, parser::ast::ColumnRef> ||
                      std::is_same_v<T, parser::ast::Constant> ||
                      std::is_same_v<T, parser::ast::StringConstant> ||
                      std::is_same_v<T, parser::ast::ArrayLiteral>) {
          merge(acc, v.loc);
        } else {
          if (!v) return;
          merge(acc, v->loc);
          using P = std::decay_t<decltype(*v)>;
          if constexpr (std::is_same_v<P, parser::ast::BinaryExpr> ||
                        std::is_same_v<P, parser::ast::ComparisonExpr> ||
                        std::is_same_v<P, parser::ast::LogicalExpr>) {
            walk(v->left, acc, sql, line_offsets);
            walk(v->right, acc, sql, line_offsets);
          } else if constexpr (std::is_same_v<P, parser::ast::FuncCall>) {
            for (const auto& a : v->args) walk(a, acc, sql, line_offsets);
            extend_past_func_call_close(*v, acc, sql, line_offsets);
          } else if constexpr (std::is_same_v<P, parser::ast::NotExpr>) {
            walk(v->operand, acc, sql, line_offsets);
          } else if constexpr (std::is_same_v<P, parser::ast::BetweenExpr>) {
            walk(v->expr, acc, sql, line_offsets);
            walk(v->low, acc, sql, line_offsets);
            walk(v->high, acc, sql, line_offsets);
          } else if constexpr (std::is_same_v<P, parser::ast::CaseExpr>) {
            for (const auto& w : v->when_clauses) {
              walk(w.condition, acc, sql, line_offsets);
              walk(w.result, acc, sql, line_offsets);
            }
            if (v->else_result) walk(*v->else_result, acc, sql, line_offsets);
          }
        }
      },
      e);
}

}  // namespace

SourceLocation expr_span(const Expr& e, std::string_view sql) {
  std::vector<int> line_offsets = build_line_offsets(sql);
  SourceLocation acc;
  walk(e, acc, sql, line_offsets);
  return acc;
}

}  // namespace rtbot_sql::analyzer
