#pragma once

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Validate INSERT INTO <stream-or-table> VALUES (...).
//
// Mirrors handle_insert in libs/api/src/compiler.cpp:489-548:
// unknown stream/table, value-count mismatch, type mismatches between
// column type and provided constant, non-constant values.
void analyze_insert(const parser::ast::InsertStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag);

// Validate DELETE FROM <table> WHERE <key> = <const>.
//
// Mirrors handle_delete in libs/api/src/compiler.cpp:550-589.
void analyze_delete(const parser::ast::DeleteStmt& stmt,
                    const CatalogSnapshot& catalog, DiagnosticBag& bag);

// Validate DROP <kind> <name>.
//
// Mirrors the dependency check in handle_drop
// (libs/api/src/compiler.cpp:919-941).
void analyze_drop(const parser::ast::DropStmt& stmt,
                  const CatalogSnapshot& catalog, DiagnosticBag& bag);

}  // namespace rtbot_sql::analyzer
