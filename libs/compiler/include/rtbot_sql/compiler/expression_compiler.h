#pragma once

#include <map>
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

}  // namespace rtbot_sql::compiler
