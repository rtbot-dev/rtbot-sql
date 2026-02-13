#pragma once

#include <optional>
#include <vector>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/compiler/select_compiler.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

// Compile a GROUP BY query into a KeyedPipeline + Prototype structure.
// The field map has the key column at index 0, followed by non-key items.
// Single GROUP BY column only (composite keys deferred to Phase 4).
SelectResult compile_group_by(
    const std::vector<parser::ast::SelectItem>& select_list,
    const std::vector<parser::ast::Expr>& group_by,
    const std::optional<parser::ast::Expr>& having,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder);

}  // namespace rtbot_sql::compiler
