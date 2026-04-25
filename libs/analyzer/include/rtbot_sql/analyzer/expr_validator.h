#pragma once

#include <set>
#include <string>

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Recursively walk an Expr and run all expression-level validators
// (function arity, constant-arg requirements, CASE shape, etc.).
// Diagnostics with source locations are pushed into `bag`.
//
// Visits every sub-expression: function arguments, binary operands,
// comparison operands, logical operands, NOT operands, BETWEEN bounds,
// CASE WHEN branches.
//
// If `scope` is non-null, every ColumnRef encountered is resolved against
// it; failures (unknown / ambiguous columns) become diagnostics with the
// column reference's source location.
//
// `aliases` (when non-null) is a set of SELECT-list alias names. Column
// references whose name is in this set are skipped during resolution —
// they're forwarded to the compiler's alias-expansion stage, which
// substitutes the alias's defining expression before final compilation.
void validate_expression(const parser::ast::Expr& expr, DiagnosticBag& bag,
                         const Scope* scope = nullptr,
                         const std::set<std::string>* aliases = nullptr);

// Validate a predicate expression in WHERE/HAVING context.
//
// `context` is "WHERE" or "HAVING" — used in error messages so wording
// matches the original throws in where_compiler.cpp and group_by_compiler.cpp.
//
// Mirrors the structural checks in libs/compiler/src/where_compiler.cpp
// (compile_predicate / compile_comparison): predicates must be
// Comparison / Logical / NOT(Comparison) / BETWEEN; BETWEEN bounds must
// be literal constants; the BETWEEN'd expression cannot be constant;
// comparisons cannot have two literal-constant operands.
//
// `scope` and `aliases` are forwarded to validate_expression so column
// references and alias references inside the predicate are handled
// consistently with their occurrences in SELECT items.
//
// Also walks every sub-expression with validate_expression so function
// arity diagnostics from inside the predicate are surfaced too.
void validate_predicate(const parser::ast::Expr& expr, DiagnosticBag& bag,
                        const std::string& context = "WHERE",
                        const Scope* scope = nullptr,
                        const std::set<std::string>* aliases = nullptr);

}  // namespace rtbot_sql::analyzer
