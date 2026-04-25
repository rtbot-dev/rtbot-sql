#pragma once

#include <vector>

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Top-level semantic check on a single AST statement.
//
// Returns the list of diagnostics found. An empty vector means the statement
// is semantically valid (or no checks have been migrated yet for this kind).
//
// The analyzer never throws on user-level mistakes — every problem is
// reported as a Diagnostic with source location attached. Internal-invariant
// failures (malformed AST input, etc.) may still throw.
std::vector<Diagnostic> analyze_statement(const parser::ast::Statement& stmt,
                                          const CatalogSnapshot& catalog);

}  // namespace rtbot_sql::analyzer
