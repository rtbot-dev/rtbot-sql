#pragma once

#include <map>
#include <string>
#include <vector>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

using FieldMap = std::map<std::string, int>;

struct SelectResult {
  Endpoint endpoint;
  FieldMap field_map;
};

// Compile a SELECT projection into operator graph nodes.
// - All ColumnRefs → VectorProject optimization
// - Mixed expressions/functions → VectorCompose
// - Empty select_list → SELECT * passthrough (identity)
// Returns the output endpoint and field map (name → position).
SelectResult compile_select_projection(
    const std::vector<parser::ast::SelectItem>& select_list,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    const std::map<std::string, Endpoint>* source_endpoints = nullptr);

}  // namespace rtbot_sql::compiler
