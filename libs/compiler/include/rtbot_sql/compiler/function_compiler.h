#pragma once

#include <string>
#include <vector>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

// Compile an aggregate or windowed SQL function into operator graph nodes.
// Handles: SUM, COUNT, AVG, MOVING_AVERAGE, MOVING_SUM, MOVING_COUNT, MOVING_STD,
//          FIR, IIR, RESAMPLE, PEAK_DETECT (DSP stubs).
// Returns the output endpoint of the compiled function chain.
// Throws std::runtime_error on unknown function, wrong arg count, etc.
Endpoint compile_function(const std::string& name,
                          const std::vector<parser::ast::Expr>& args,
                          const Endpoint& input_endpoint,
                          const analyzer::Scope& scope,
                          GraphBuilder& builder);

// Returns true if the function name is handled by compile_function
// (i.e., it's an aggregate, windowed, or DSP function).
bool is_aggregate_or_windowed(const std::string& name);

}  // namespace rtbot_sql::compiler
