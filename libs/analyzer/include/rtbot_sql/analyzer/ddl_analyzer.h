#pragma once

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Validate CREATE STREAM / CREATE TABLE.
//
// Mirrors the structural checks in libs/api/src/compiler.cpp:462-468:
// CREATE TABLE composite primary keys are not yet supported.
void analyze_create_stream(const parser::ast::CreateStreamStmt& stmt,
                           const CatalogSnapshot& catalog, DiagnosticBag& bag);

}  // namespace rtbot_sql::analyzer
