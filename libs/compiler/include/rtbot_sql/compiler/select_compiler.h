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
  bool is_segment_only = false;  // true when GROUP BY has only segment expressions (no persistent keys)
};

// Compile a SELECT projection into operator graph nodes.
// - All ColumnRefs → VectorProject optimization
// - Mixed expressions/functions → VectorCompose
// - Empty select_list → SELECT * passthrough (identity)
// Returns the output endpoint and field map (name → position).
//
// `where_clause`: optional WHERE predicate. When supplied together with a
// single-source fusable SELECT, the predicate is absorbed into the emitted
// FusedExpressionVector's bytecode via a GATE opcode — eliminating the
// standalone VectorExtract + CompareGT + Demultiplexer chain. Callers must
// set `where_absorbed` (out) to know whether to skip their own compile_where.
SelectResult compile_select_projection(
    const std::vector<parser::ast::SelectItem>& select_list,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    const std::map<std::string, Endpoint>* source_endpoints = nullptr,
    const parser::ast::Expr* where_clause = nullptr,
    bool* where_absorbed = nullptr);

}  // namespace rtbot_sql::compiler
