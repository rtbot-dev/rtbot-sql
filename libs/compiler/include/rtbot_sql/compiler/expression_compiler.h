#pragma once

#include <array>
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
// When enable_windowed is true, recognized windowed functions (MOVING_AVG,
// MOVING_SUM, STDDEV, ...) emit tier-1 opcodes inline with their window size
// carried in the bytecode (state offsets and aux_args are auto-derived by
// FusedExpression's packer). When false, those functions are treated as
// unfusable and the caller falls back to the standalone-operator path.
// Returns success=false if the expression contains non-fusable nodes
// (aggregates, CASE, TIMESHIFT, RESAMPLE, etc.).
BytecodeResult compile_expression_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    const std::map<std::string, Endpoint>* source_endpoints = nullptr,
    bool enable_windowed = false);

// Context for compiling aggregate expressions to bytecode.
// Shared across all expressions in a GROUP BY inner prototype to track
// persistent state allocation and enable shared COUNT optimization.
struct AggBytecodeContext {
  std::vector<double> state_init;       // accumulated state init values
  int shared_count_state_idx = -1;      // state index of shared COUNT slot (-1 = not yet allocated)
  bool count_emitted = false;           // whether COUNT opcode has been emitted (vs STATE_LOAD)
};

// Compile an expression (possibly containing aggregates) into bytecode.
// Used inside GROUP BY inner prototypes where SUM/COUNT/AVG/MAX/MIN are valid.
// The agg_ctx tracks shared state (e.g., shared COUNT for multiple AVGs).
// Returns success=false if expression contains non-fusable nodes (windowed
// functions, CASE, TIMESHIFT, etc.), allowing fallback to operator-chain path.
BytecodeResult compile_aggregate_expression_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    AggBytecodeContext& agg_ctx);

struct SegmentBytecodeResult {
  std::vector<double> bytecode;
  std::vector<double> constants;
  bool success;
};

SegmentBytecodeResult compile_segment_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    const std::string& stream_name);

}  // namespace rtbot_sql::compiler
