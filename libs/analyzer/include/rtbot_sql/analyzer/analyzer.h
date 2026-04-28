#pragma once

#include <string_view>
#include <vector>

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Top-level semantic check on a single AST statement.
//
// `sql` is the source SQL text that produced `stmt`; the analyzer uses it
// only to compute whole-expression spans on certain diagnostics (see
// expr_span.h). Pass an empty string_view if not available — single-token
// spans will be used instead.
//
// Returns the list of diagnostics found. An empty vector means the statement
// is semantically valid (or no checks have been migrated yet for this kind).
//
// The analyzer never throws on user-level mistakes — every problem is
// reported as a Diagnostic with source location attached. Internal-invariant
// failures (malformed AST input, etc.) may still throw.
std::vector<Diagnostic> analyze_statement(const parser::ast::Statement& stmt,
                                          const CatalogSnapshot& catalog,
                                          std::string_view sql = {});

}  // namespace rtbot_sql::analyzer
