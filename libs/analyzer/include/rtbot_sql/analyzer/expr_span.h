#pragma once

#include <string_view>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Returns a SourceLocation that spans the entire expression: `(line,column)`
// at the first token, `(end_line,end_column)` one past the last token.
//
// Walks the AST tree and unions every per-node `.loc`, then for each
// FuncCall encountered also extends across the call's closing `)` by
// scanning the source SQL. Use at constant-arg error sites (POWER
// exponent, INSERT VALUES, BETWEEN bounds, etc.) so the diagnostic
// underlines the whole offending sub-expression end-to-end rather than
// just its first token.
//
// Best-effort: the only trailing punctuation extended is the `)` of a
// FuncCall. Wrapping/incidental parens (e.g. `INSERT VALUES (cpu+1)`,
// hand-written `((x))`) and CASE...END are not pulled into the span.
parser::ast::SourceLocation expr_span(const parser::ast::Expr& e,
                                      std::string_view sql);

}  // namespace rtbot_sql::analyzer
