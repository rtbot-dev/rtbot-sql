#include "rtbot_sql/compiler/expression_compiler.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/function_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Map SQL function name to RTBot operator type
std::string math_func_to_rtbot(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  if (upper == "ABS") return "Abs";
  if (upper == "FLOOR") return "Floor";
  if (upper == "CEIL" || upper == "CEILING") return "Ceil";
  if (upper == "ROUND") return "Round";
  if (upper == "LN" || upper == "LOG") return "Log";
  if (upper == "LOG10") return "Log10";
  if (upper == "EXP") return "Exp";
  if (upper == "SIN") return "Sin";
  if (upper == "COS") return "Cos";
  if (upper == "TAN") return "Tan";
  if (upper == "SIGN") return "Sign";
  return "";
}

// Constant-fold a unary math function
double fold_math(const std::string& rtbot_type, double v) {
  if (rtbot_type == "Abs") return std::abs(v);
  if (rtbot_type == "Floor") return std::floor(v);
  if (rtbot_type == "Ceil") return std::ceil(v);
  if (rtbot_type == "Round") return std::round(v);
  if (rtbot_type == "Log") return std::log(v);
  if (rtbot_type == "Log10") return std::log10(v);
  if (rtbot_type == "Exp") return std::exp(v);
  if (rtbot_type == "Sin") return std::sin(v);
  if (rtbot_type == "Cos") return std::cos(v);
  if (rtbot_type == "Tan") return std::tan(v);
  if (rtbot_type == "Sign") return (v > 0) ? 1.0 : (v < 0) ? -1.0 : 0.0;
  return v;
}

double fold_binary(const std::string& op, double l, double r) {
  if (op == "+") return l + r;
  if (op == "-") return l - r;
  if (op == "*") return l * r;
  if (op == "/") return l / r;
  throw std::runtime_error("unknown binary operator: " + op);
}

// Map binary op to sync RTBot operator type
std::string sync_op_type(const std::string& op) {
  if (op == "+") return "Addition";
  if (op == "-") return "Subtraction";
  if (op == "*") return "Multiplication";
  if (op == "/") return "Division";
  throw std::runtime_error("unknown binary operator: " + op);
}

// Compile: stream OP constant → scalar operator chain
Endpoint compile_scalar_op(const std::string& op, const Endpoint& stream_ep,
                           double constant, GraphBuilder& builder) {
  if (op == "+") {
    auto id = builder.next_id("add");
    builder.add_operator(id, "Add", {{"value", constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  if (op == "-") {
    auto id = builder.next_id("add");
    builder.add_operator(id, "Add", {{"value", -constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  if (op == "*") {
    auto id = builder.next_id("scale");
    builder.add_operator(id, "Scale", {{"value", constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  if (op == "/") {
    auto id = builder.next_id("scale");
    builder.add_operator(id, "Scale", {{"value", 1.0 / constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  throw std::runtime_error("unknown binary operator: " + op);
}

// Compile: constant OP stream → reversed scalar operator chain
Endpoint compile_scalar_op_reversed(const std::string& op,
                                    const Endpoint& stream_ep, double constant,
                                    const Endpoint& input_endpoint,
                                    GraphBuilder& builder) {
  if (op == "+") {
    // Commutative
    auto id = builder.next_id("add");
    builder.add_operator(id, "Add", {{"value", constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  if (op == "-") {
    // constant - stream → Scale(-1) → Add(constant)
    auto scale_id = builder.next_id("scale");
    builder.add_operator(scale_id, "Scale", {{"value", -1.0}});
    builder.connect(stream_ep, {scale_id, "i1"});

    auto add_id = builder.next_id("add");
    builder.add_operator(add_id, "Add", {{"value", constant}});
    builder.connect({scale_id, "o1"}, {add_id, "i1"});
    return {add_id, "o1"};
  }
  if (op == "*") {
    // Commutative
    auto id = builder.next_id("scale");
    builder.add_operator(id, "Scale", {{"value", constant}});
    builder.connect(stream_ep, {id, "i1"});
    return {id, "o1"};
  }
  if (op == "/") {
    // constant / stream → ConstantNumber + Division sync
    auto const_id = builder.next_id("const");
    builder.add_operator(const_id, "ConstantNumber", {{"value", constant}});
    builder.connect(input_endpoint, {const_id, "i1"});

    auto div_id = builder.next_id("div");
    builder.add_operator(div_id, "Division", {{"numPorts", 2}});
    builder.connect({const_id, "o1"}, {div_id, "i1"});
    builder.connect(stream_ep, {div_id, "i2"});
    return {div_id, "o1"};
  }
  throw std::runtime_error("unknown binary operator: " + op);
}

// Compile: stream OP stream → sync arithmetic
Endpoint compile_sync_op(const std::string& op, const Endpoint& left_ep,
                         const Endpoint& right_ep, GraphBuilder& builder) {
  auto type = sync_op_type(op);
  auto id = builder.next_id(type == "Addition"       ? "add_sync"
                            : type == "Subtraction"   ? "sub_sync"
                            : type == "Multiplication" ? "mul_sync"
                                                      : "div_sync");
  builder.add_operator(id, type, {{"numPorts", 2}});
  builder.connect(left_ep, {id, "i1"});
  builder.connect(right_ep, {id, "i2"});
  return {id, "o1"};
}

}  // namespace

ExprResult compile_expression(const parser::ast::Expr& expr,
                              const Endpoint& input_endpoint,
                              const analyzer::Scope& scope,
                              GraphBuilder& builder) {
  using namespace parser::ast;

  // ColumnRef → VectorExtract
  if (auto* col = std::get_if<ColumnRef>(&expr)) {
    auto result = scope.resolve(*col);
    if (auto* err = std::get_if<std::string>(&result)) {
      throw std::runtime_error(*err);
    }
    auto& binding = std::get<analyzer::ColumnBinding>(result);
    auto id = builder.next_id("ext");
    builder.add_operator(id, "VectorExtract",
                         {{"index", static_cast<double>(binding.index)}});
    builder.connect(input_endpoint, {id, "i1"});
    return Endpoint{id, "o1"};
  }

  // Constant → ConstantMarker (deferred)
  if (auto* c = std::get_if<Constant>(&expr)) {
    return ConstantMarker{c->value};
  }

  // BinaryExpr → scalar or sync arithmetic
  if (auto* bin_ptr = std::get_if<std::unique_ptr<BinaryExpr>>(&expr)) {
    const auto& bin = **bin_ptr;
    auto left = compile_expression(bin.left, input_endpoint, scope, builder);
    auto right = compile_expression(bin.right, input_endpoint, scope, builder);

    auto* left_const = std::get_if<ConstantMarker>(&left);
    auto* right_const = std::get_if<ConstantMarker>(&right);

    // Both constants → fold
    if (left_const && right_const) {
      return ConstantMarker{fold_binary(bin.op, left_const->value,
                                        right_const->value)};
    }

    // Stream OP constant
    if (right_const) {
      return compile_scalar_op(bin.op, std::get<Endpoint>(left),
                               right_const->value, builder);
    }

    // Constant OP stream
    if (left_const) {
      return compile_scalar_op_reversed(bin.op, std::get<Endpoint>(right),
                                        left_const->value, input_endpoint,
                                        builder);
    }

    // Both streams
    return compile_sync_op(bin.op, std::get<Endpoint>(left),
                           std::get<Endpoint>(right), builder);
  }

  // FuncCall → math function operators
  if (auto* func_ptr = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    const auto& func = **func_ptr;

    // POWER(expr, n) — special case with exponent parameter
    std::string upper_name = func.name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                   ::toupper);
    if (upper_name == "POWER") {
      if (func.args.size() != 2) {
        throw std::runtime_error("POWER requires exactly 2 arguments");
      }
      auto base = compile_expression(func.args[0], input_endpoint, scope,
                                     builder);
      auto exp_result = compile_expression(func.args[1], input_endpoint, scope,
                                           builder);
      auto* exp_const = std::get_if<ConstantMarker>(&exp_result);
      if (!exp_const) {
        throw std::runtime_error("POWER exponent must be a constant");
      }

      // If base is also constant, fold
      if (auto* base_const = std::get_if<ConstantMarker>(&base)) {
        return ConstantMarker{std::pow(base_const->value, exp_const->value)};
      }

      auto id = builder.next_id("power");
      builder.add_operator(id, "Power", {{"value", exp_const->value}});
      builder.connect(std::get<Endpoint>(base), {id, "i1"});
      return Endpoint{id, "o1"};
    }

    // Unary math functions (ABS, FLOOR, CEIL, etc.)
    auto rtbot_type = math_func_to_rtbot(func.name);
    if (rtbot_type.empty()) {
      // Delegate to aggregate/windowed function compiler
      if (is_aggregate_or_windowed(func.name)) {
        return compile_function(func.name, func.args, input_endpoint, scope,
                                builder);
      }
      throw std::runtime_error("unknown function: " + func.name);
    }
    if (func.args.size() != 1) {
      throw std::runtime_error(func.name + " requires exactly 1 argument");
    }

    auto arg = compile_expression(func.args[0], input_endpoint, scope, builder);

    // Constant argument → fold
    if (auto* arg_const = std::get_if<ConstantMarker>(&arg)) {
      return ConstantMarker{fold_math(rtbot_type, arg_const->value)};
    }

    auto id = builder.next_id(rtbot_type);
    builder.add_operator(id, rtbot_type);
    builder.connect(std::get<Endpoint>(arg), {id, "i1"});
    return Endpoint{id, "o1"};
  }

  throw std::runtime_error("unsupported expression type");
}

}  // namespace rtbot_sql::compiler
