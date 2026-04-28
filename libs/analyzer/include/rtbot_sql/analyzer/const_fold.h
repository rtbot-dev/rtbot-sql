#pragma once

#include <optional>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// Attempt to constant-fold an expression at analysis time.
//
// Returns the folded numeric value if `expr` is a literal Constant,
// arithmetic on foldable operands (+ - * /), or a unary math function
// (ABS, FLOOR, CEIL/CEILING, ROUND, LN/LOG, LOG10, EXP, SIN, COS, TAN,
// SIGN, SQRT) applied to a foldable arg. Returns std::nullopt otherwise.
//
// Mirrors the constant-folding behaviour in
// libs/compiler/src/expression_compiler.cpp so the analyzer can decide
// whether arguments to functions like POWER/TIMESHIFT/RESAMPLE_CONSTANT
// will eventually evaluate to constants without false-positive rejections
// of expressions like POWER(x, 2*3).
std::optional<double> try_fold(const parser::ast::Expr& expr);

}  // namespace rtbot_sql::analyzer
