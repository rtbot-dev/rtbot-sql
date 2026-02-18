#include "rtbot_sql/compiler/where_compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Map comparison op to RTBot operator type
std::string comparison_to_rtbot(const std::string& op) {
  if (op == ">") return "CompareGT";
  if (op == "<") return "CompareLT";
  if (op == ">=") return "CompareGTE";
  if (op == "<=") return "CompareLTE";
  if (op == "=") return "CompareEQ";
  if (op == "!=") return "CompareNEQ";
  throw std::runtime_error("unknown comparison operator: " + op);
}

// Invert a comparison operator (for NOT optimization)
std::string invert_comparison(const std::string& op) {
  if (op == ">") return "<=";
  if (op == "<") return ">=";
  if (op == ">=") return "<";
  if (op == "<=") return ">";
  if (op == "=") return "!=";
  if (op == "!=") return "=";
  throw std::runtime_error("cannot invert comparison: " + op);
}

// Compile a single comparison: expr OP constant → CompareScalar
Endpoint compile_comparison(const parser::ast::ComparisonExpr& cmp,
                            const Endpoint& input_endpoint,
                            const analyzer::Scope& scope,
                            GraphBuilder& builder) {
  auto left = compile_expression(cmp.left, input_endpoint, scope, builder);
  auto right = compile_expression(cmp.right, input_endpoint, scope, builder);

  auto* left_const = std::get_if<ConstantMarker>(&left);
  auto* right_const = std::get_if<ConstantMarker>(&right);

  if (left_const && right_const) {
    throw std::runtime_error(
        "comparison of two constants is not supported in WHERE");
  }

  // stream OP constant
  if (right_const) {
    auto& stream_ep = std::get<Endpoint>(left);
    auto rtbot_type = comparison_to_rtbot(cmp.op);
    auto id = builder.next_id("cmp");
    builder.add_operator(id, rtbot_type, {{"value", right_const->value}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }

  // constant OP stream → flip comparison
  if (left_const) {
    auto& stream_ep = std::get<Endpoint>(right);
    // Flip: constant > stream ↔ stream < constant
    auto flipped = invert_comparison(cmp.op);
    // Actually, we need to flip direction, not invert:
    // constant > stream → stream < constant
    std::string flipped_op;
    if (cmp.op == ">") flipped_op = "<";
    else if (cmp.op == "<") flipped_op = ">";
    else if (cmp.op == ">=") flipped_op = "<=";
    else if (cmp.op == "<=") flipped_op = ">=";
    else flipped_op = cmp.op;  // = and != are symmetric

    auto rtbot_type = comparison_to_rtbot(flipped_op);
    auto id = builder.next_id("cmp");
    builder.add_operator(id, rtbot_type, {{"value", left_const->value}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }

  // Both streams → CompareSyncXX (synchronize by timestamp)
  {
    auto& left_ep = std::get<Endpoint>(left);
    auto& right_ep = std::get<Endpoint>(right);
    std::string rtbot_type;
    if (cmp.op == ">") rtbot_type = "CompareSyncGT";
    else if (cmp.op == "<") rtbot_type = "CompareSyncLT";
    else if (cmp.op == ">=") rtbot_type = "CompareSyncGTE";
    else if (cmp.op == "<=") rtbot_type = "CompareSyncLTE";
    else if (cmp.op == "=") rtbot_type = "CompareSyncEQ";
    else if (cmp.op == "!=") rtbot_type = "CompareSyncNEQ";
    else throw std::runtime_error("unknown comparison operator: " + cmp.op);
    auto id = builder.next_id("cmp_sync");
    builder.add_operator(id, rtbot_type);
    builder.connect(left_ep, {id, "i1"});
    builder.connect(right_ep, {id, "i2"});
    return {id, "o1"};
  }
}

}  // namespace

Endpoint compile_predicate(const parser::ast::Expr& expr,
                           const Endpoint& input_endpoint,
                           const analyzer::Scope& scope,
                           GraphBuilder& builder) {
  using namespace parser::ast;

  // ComparisonExpr → CompareScalar
  if (auto* cmp_ptr = std::get_if<std::unique_ptr<ComparisonExpr>>(&expr)) {
    return compile_comparison(**cmp_ptr, input_endpoint, scope, builder);
  }

  // LogicalExpr → LogicalAnd / LogicalOr
  if (auto* log_ptr = std::get_if<std::unique_ptr<LogicalExpr>>(&expr)) {
    const auto& log = **log_ptr;
    auto left_ep = compile_predicate(log.left, input_endpoint, scope, builder);
    auto right_ep =
        compile_predicate(log.right, input_endpoint, scope, builder);

    std::string upper_op = log.op;
    std::transform(upper_op.begin(), upper_op.end(), upper_op.begin(),
                   ::toupper);

    std::string rtbot_type;
    if (upper_op == "AND") rtbot_type = "LogicalAnd";
    else if (upper_op == "OR") rtbot_type = "LogicalOr";
    else throw std::runtime_error("unknown logical operator: " + log.op);

    auto id = builder.next_id(rtbot_type == "LogicalAnd" ? "and" : "or");
    builder.add_operator(id, rtbot_type, {{"numPorts", 2}});
    builder.connect(left_ep, {id, "i1"});
    builder.connect(right_ep, {id, "i2"});
    return {id, "o1"};
  }

  // NotExpr → invert the inner comparison
  if (auto* not_ptr = std::get_if<std::unique_ptr<NotExpr>>(&expr)) {
    const auto& not_expr = **not_ptr;

    // Optimization: if inner is a ComparisonExpr, invert the operator
    if (auto* inner_cmp =
            std::get_if<std::unique_ptr<ComparisonExpr>>(&not_expr.operand)) {
      const auto& cmp = **inner_cmp;
      // Build an inverted comparison
      auto inverted_op = invert_comparison(cmp.op);
      auto left =
          compile_expression(cmp.left, input_endpoint, scope, builder);
      auto right =
          compile_expression(cmp.right, input_endpoint, scope, builder);

      auto* right_const = std::get_if<ConstantMarker>(&right);
      if (!right_const) {
        throw std::runtime_error(
            "NOT optimization requires constant on right side");
      }

      auto rtbot_type = comparison_to_rtbot(inverted_op);
      auto id = builder.next_id("cmp");
      builder.add_operator(id, rtbot_type, {{"value", right_const->value}});
      builder.connect(std::get<Endpoint>(left), {id, "i1"});
      return {id, "o1"};
    }

    throw std::runtime_error("NOT is only supported on comparison expressions");
  }

  // BetweenExpr → desugar to GTE(low) AND LTE(high)
  if (auto* btw_ptr = std::get_if<std::unique_ptr<BetweenExpr>>(&expr)) {
    const auto& btw = **btw_ptr;

    auto expr_result =
        compile_expression(btw.expr, input_endpoint, scope, builder);
    if (std::holds_alternative<ConstantMarker>(expr_result)) {
      throw std::runtime_error("BETWEEN on constant expression not supported");
    }
    auto& stream_ep = std::get<Endpoint>(expr_result);

    auto low_result =
        compile_expression(btw.low, input_endpoint, scope, builder);
    auto high_result =
        compile_expression(btw.high, input_endpoint, scope, builder);
    auto* low_const = std::get_if<ConstantMarker>(&low_result);
    auto* high_const = std::get_if<ConstantMarker>(&high_result);
    if (!low_const || !high_const) {
      throw std::runtime_error("BETWEEN bounds must be constants");
    }

    // CompareGTE(low)
    auto gte_id = builder.next_id("cmp");
    builder.add_operator(gte_id, "CompareGTE", {{"value", low_const->value}});
    builder.connect(stream_ep, {gte_id, "i1"});

    // CompareLTE(high)
    auto lte_id = builder.next_id("cmp");
    builder.add_operator(lte_id, "CompareLTE", {{"value", high_const->value}});
    builder.connect(stream_ep, {lte_id, "i1"});

    // LogicalAnd
    auto and_id = builder.next_id("and");
    builder.add_operator(and_id, "LogicalAnd", {{"numPorts", 2}});
    builder.connect({gte_id, "o1"}, {and_id, "i1"});
    builder.connect({lte_id, "o1"}, {and_id, "i2"});
    return {and_id, "o1"};
  }

  throw std::runtime_error("unsupported predicate expression type");
}

Endpoint compile_where(const parser::ast::Expr& where_clause,
                       const Endpoint& input_endpoint,
                       const analyzer::Scope& scope,
                       GraphBuilder& builder) {
  auto bool_ep = compile_predicate(where_clause, input_endpoint, scope, builder);

  auto demux_id = builder.next_id("demux");
  builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                       {{"portType", "vector_number"}});
  builder.connect(bool_ep, {demux_id, "c1"});
  builder.connect(input_endpoint, {demux_id, "i1"});
  return {demux_id, "o1"};
}

}  // namespace rtbot_sql::compiler
