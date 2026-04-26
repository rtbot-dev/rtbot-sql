#pragma once

#include <string_view>

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Run semantic validation on a SELECT statement: walk every expression
// (SELECT items, WHERE, HAVING, GROUP BY, ORDER BY, JOIN ON conditions)
// and produce diagnostics with source locations.
//
// `top_level` is true when the SELECT is the user's outermost statement
// (a SELECT executed directly), false when it's the inner SELECT of a
// CREATE [MATERIALIZED] VIEW. The stream-LIMIT requirement only applies
// to top-level SELECTs — view definitions over streams are stateful and
// don't need bounding.
void analyze_select(const parser::ast::SelectStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag,
                    bool top_level = true, std::string_view sql = {});

}  // namespace rtbot_sql::analyzer
