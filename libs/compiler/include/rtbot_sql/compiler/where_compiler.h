#pragma once

#include <map>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

// Compile a predicate expression into a boolean-producing operator chain.
// Returns the endpoint producing BooleanData.
// Throws std::runtime_error on compilation errors.
Endpoint compile_predicate(const parser::ast::Expr& expr,
                           const Endpoint& input_endpoint,
                           const analyzer::Scope& scope,
                           GraphBuilder& builder,
                           const std::map<std::string, Endpoint>* source_endpoints = nullptr);

// Compile a full WHERE clause: predicate + Demultiplexer gate.
// Returns the endpoint producing filtered VectorNumberData tuples.
Endpoint compile_where(const parser::ast::Expr& where_clause,
                       const Endpoint& input_endpoint,
                       const analyzer::Scope& scope,
                       GraphBuilder& builder,
                       const std::map<std::string, Endpoint>* source_endpoints = nullptr);

}  // namespace rtbot_sql::compiler
