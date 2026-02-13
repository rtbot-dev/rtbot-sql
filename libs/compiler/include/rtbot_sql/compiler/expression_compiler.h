#pragma once

#include <stdexcept>
#include <string>
#include <variant>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

struct ConstantMarker {
  double value;
};

using ExprResult = std::variant<Endpoint, ConstantMarker>;

// Compile an expression AST node into operator graph nodes.
// Returns either an Endpoint (stream output) or a ConstantMarker (deferred).
// Throws std::runtime_error on compilation errors.
ExprResult compile_expression(const parser::ast::Expr& expr,
                              const Endpoint& input_endpoint,
                              const analyzer::Scope& scope,
                              GraphBuilder& builder);

}  // namespace rtbot_sql::compiler
