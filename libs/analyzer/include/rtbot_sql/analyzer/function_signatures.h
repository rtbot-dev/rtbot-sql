#pragma once

#include <string>
#include <string_view>

#include "rtbot_sql/analyzer/diagnostic.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// True for SQL function names recognized by rtbot-sql (any case).
// Used by validate_function_call to decide between "unknown function" and
// per-function arity / argument-shape diagnostics.
bool is_known_function(const std::string& name);

// Validate a single function call: arity, constant-arg requirements,
// array-literal arguments, etc. Pushes one or more diagnostics into `bag`
// on failure; on success, leaves `bag` untouched.
//
// Mirrors the throw-on-failure rules in
// libs/compiler/src/function_compiler.cpp and the function-specific rules
// in libs/compiler/src/expression_compiler.cpp (POWER, TIMESHIFT, TS,
// RESAMPLE_CONSTANT, unary math).
void validate_function_call(const parser::ast::FuncCall& fc,
                            DiagnosticBag& bag,
                            std::string_view sql = {});

}  // namespace rtbot_sql::analyzer
