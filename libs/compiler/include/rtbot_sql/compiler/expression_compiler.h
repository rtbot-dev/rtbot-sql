#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

struct ConstantMarker {
  double value;
};

using ExprResult = std::variant<Endpoint, ConstantMarker>;

class ExprCache;  // forward declaration

// Compile an expression AST node into operator graph nodes.
// Returns either an Endpoint (stream output) or a ConstantMarker (deferred).
// When cache is non-null, sub-expression de-duplication is enabled.
// Throws std::runtime_error on compilation errors.
ExprResult compile_expression(const parser::ast::Expr& expr,
                              const Endpoint& input_endpoint,
                              const analyzer::Scope& scope,
                              GraphBuilder& builder,
                              ExprCache* cache = nullptr,
                              const std::map<std::string, Endpoint>* source_endpoints = nullptr);

// Result of compiling an expression tree into bytecode for FusedExpression.
struct BytecodeResult {
  std::vector<double> bytecode;  // opcodes for this expression (including END)
  bool success;                  // false if expression is not fusable
};

// Compile an expression AST into RPN bytecode for FusedExpression.
// The column_to_input map is shared across all expressions in a projection
// and maps (stream_name, column_index) pairs to FusedExpression input port indices.
// The constants vector is shared across all expressions.
// Returns success=false if the expression contains non-fusable nodes
// (aggregates, windowed functions, CASE, TIMESHIFT, RESAMPLE, etc.),
// allowing the caller to fall back to the standard operator-chain path.
BytecodeResult compile_expression_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    const std::map<std::string, Endpoint>* source_endpoints = nullptr);

}  // namespace rtbot_sql::compiler
