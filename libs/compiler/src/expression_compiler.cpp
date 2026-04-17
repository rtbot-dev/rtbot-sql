#include "rtbot_sql/compiler/expression_compiler.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expr_cache.h"
#include "rtbot_sql/compiler/function_compiler.h"
#include "rtbot_sql/compiler/where_compiler.h"

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

// Ensure an ExprResult is an Endpoint, materializing constants with a clock.
static Endpoint ensure_endpoint_local(ExprResult result,
                                      const Endpoint& input_endpoint,
                                      GraphBuilder& builder) {
  if (auto* ep = std::get_if<Endpoint>(&result)) {
    return *ep;
  }
  auto& cm = std::get<ConstantMarker>(result);
  // Derive a scalar clock from the VectorNumber input stream
  auto clock_id = builder.next_id("clock");
  builder.add_operator(clock_id, "VectorExtract", {{"index", 0.0}});
  builder.connect(input_endpoint, {clock_id, "i1"});
  auto const_id = builder.next_id("const");
  builder.add_operator(const_id, "ConstantNumber", {{"value", cm.value}});
  builder.connect({clock_id, "o1"}, {const_id, "i1"});
  return {const_id, "o1"};
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
    builder.connect(stream_ep, {const_id, "i1"});

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
                               GraphBuilder& builder,
                               ExprCache* cache,
                                const std::map<std::string, Endpoint>* source_endpoints) {
  using namespace parser::ast;

  // Cache lookup
  if (cache) {
    const Endpoint* cached = cache->lookup(expr);
    if (cached) return *cached;
  }

  // Helper to store result in cache if it's an Endpoint
  auto maybe_cache = [&](const parser::ast::Expr& e, ExprResult& r) {
    if (cache) {
      if (auto* ep = std::get_if<Endpoint>(&r)) {
        cache->store(e, *ep);
      }
    }
  };

  // ColumnRef → VectorExtract
  if (auto* col = std::get_if<ColumnRef>(&expr)) {
    auto result = scope.resolve(*col);
    if (auto* err = std::get_if<std::string>(&result)) {
      throw std::runtime_error(*err);
    }
    auto& binding = std::get<analyzer::ColumnBinding>(result);
    Endpoint source_ep = input_endpoint;
    if (source_endpoints) {
      auto it = source_endpoints->find(binding.stream_name);
      if (it != source_endpoints->end()) {
        source_ep = it->second;
      }
    }
    auto id = builder.next_id("ext");
    builder.add_operator(id, "VectorExtract",
                         {{"index", static_cast<double>(binding.index)}});
    builder.connect(source_ep, {id, "i1"});
    ExprResult r = Endpoint{id, "o1"};
    maybe_cache(expr, r);
    return r;
  }

  // Constant → ConstantMarker (deferred)
  if (auto* c = std::get_if<Constant>(&expr)) {
    return ConstantMarker{c->value};
  }

  // BinaryExpr → scalar or sync arithmetic
  if (auto* bin_ptr = std::get_if<std::unique_ptr<BinaryExpr>>(&expr)) {
    const auto& bin = **bin_ptr;
    auto left = compile_expression(bin.left, input_endpoint, scope, builder, cache,
                                   source_endpoints);
    auto right = compile_expression(bin.right, input_endpoint, scope, builder,
                                    cache, source_endpoints);

    auto* left_const = std::get_if<ConstantMarker>(&left);
    auto* right_const = std::get_if<ConstantMarker>(&right);

    // Both constants → fold
    if (left_const && right_const) {
      return ConstantMarker{fold_binary(bin.op, left_const->value,
                                        right_const->value)};
    }

    // Stream OP constant
    if (right_const) {
      ExprResult r = compile_scalar_op(bin.op, std::get<Endpoint>(left),
                                       right_const->value, builder);
      maybe_cache(expr, r);
      return r;
    }

    // Constant OP stream
    if (left_const) {
      ExprResult r = compile_scalar_op_reversed(bin.op, std::get<Endpoint>(right),
                                                left_const->value, builder);
      maybe_cache(expr, r);
      return r;
    }

    // Both streams
    ExprResult r = compile_sync_op(bin.op, std::get<Endpoint>(left),
                                   std::get<Endpoint>(right), builder);
    maybe_cache(expr, r);
    return r;
  }

  // FuncCall → math function operators
  if (auto* func_ptr = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    const auto& func = **func_ptr;

    // TS() — extract message timestamp as a scalar value
    std::string upper_name = func.name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                   ::toupper);
    if (upper_name == "TS") {
      if (!func.args.empty()) {
        throw std::runtime_error("TS() takes no arguments");
      }
      auto id = builder.next_id("ts");
      builder.add_operator(id, "TimestampExtract");
      builder.connect(input_endpoint, {id, "i1"});
      ExprResult r = Endpoint{id, "o1"};
      maybe_cache(expr, r);
      return r;
    }

    // POWER(expr, n) — special case with exponent parameter
    if (upper_name == "POWER") {
      if (func.args.size() != 2) {
        throw std::runtime_error("POWER requires exactly 2 arguments");
      }
      auto base = compile_expression(func.args[0], input_endpoint, scope,
                                     builder, cache, source_endpoints);
      auto exp_result = compile_expression(func.args[1], input_endpoint, scope,
                                            builder, cache, source_endpoints);
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
      ExprResult r = Endpoint{id, "o1"};
      maybe_cache(expr, r);
      return r;
    }

    // TIMESHIFT(expr, shift) — shift message timestamps by a constant
    if (upper_name == "TIMESHIFT") {
      if (func.args.size() != 2) {
        throw std::runtime_error(
            "TIMESHIFT requires exactly 2 arguments");
      }
      auto signal = compile_expression(func.args[0], input_endpoint, scope,
                                       builder, cache, source_endpoints);
      auto shift_result =
          compile_expression(func.args[1], input_endpoint, scope, builder,
                             cache, source_endpoints);
      auto* shift_const = std::get_if<ConstantMarker>(&shift_result);
      if (!shift_const) {
        throw std::runtime_error(
            "TIMESHIFT shift must be a constant");
      }

      auto signal_ep =
          ensure_endpoint_local(std::move(signal), input_endpoint, builder);
      auto id = builder.next_id("timeshift");
      builder.add_operator(
          id, "TimeShift",
          {{"shift", shift_const->value}});
      builder.connect(signal_ep, {id, "i1"});
      ExprResult r = Endpoint{id, "o1"};
      maybe_cache(expr, r);
      return r;
    }

    // RESAMPLE_CONSTANT(expr, interval [, snap_first]) — resampler with fixed grid (t0=0)
    if (upper_name == "RESAMPLE_CONSTANT") {
      if (func.args.size() < 2 || func.args.size() > 3) {
        throw std::runtime_error(
            "RESAMPLE_CONSTANT requires 2 or 3 arguments");
      }
      auto signal = compile_expression(func.args[0], input_endpoint, scope,
                                       builder, cache, source_endpoints);
      auto interval_result =
          compile_expression(func.args[1], input_endpoint, scope, builder,
                             cache, source_endpoints);
      auto* interval_const = std::get_if<ConstantMarker>(&interval_result);
      if (!interval_const) {
        throw std::runtime_error(
            "RESAMPLE_CONSTANT interval must be a constant");
      }

      // Optional 3rd arg: snap_first flag (default 0)
      double snap_first_val = 0.0;
      if (func.args.size() == 3) {
        auto snap_result =
            compile_expression(func.args[2], input_endpoint, scope, builder,
                               cache, source_endpoints);
        auto* snap_const = std::get_if<ConstantMarker>(&snap_result);
        if (!snap_const) {
          throw std::runtime_error(
              "RESAMPLE_CONSTANT snap_first must be a constant");
        }
        snap_first_val = snap_const->value;
      }

      auto signal_ep =
          ensure_endpoint_local(std::move(signal), input_endpoint, builder);
      auto id = builder.next_id("resample");
      std::map<std::string, double> params = {
          {"interval", interval_const->value}, {"t0", 0.0}};
      if (snap_first_val != 0.0) {
        params["snapFirst"] = 1.0;
      }
      builder.add_operator(id, "ResamplerConstant", params);
      builder.connect(signal_ep, {id, "i1"});
      ExprResult r = Endpoint{id, "o1"};
      maybe_cache(expr, r);
      return r;
    }

    // Unary math functions (ABS, FLOOR, CEIL, etc.)
    auto rtbot_type = math_func_to_rtbot(func.name);
    if (rtbot_type.empty()) {
      // Delegate to aggregate/windowed function compiler
      if (is_aggregate_or_windowed(func.name)) {
        ExprResult r = compile_function(func.name, func.args, input_endpoint,
                                        scope, builder, cache, source_endpoints);
        maybe_cache(expr, r);
        return r;
      }
      throw std::runtime_error("unknown function: " + func.name);
    }
    if (func.args.size() != 1) {
      throw std::runtime_error(func.name + " requires exactly 1 argument");
    }

    auto arg = compile_expression(func.args[0], input_endpoint, scope, builder,
                                  cache, source_endpoints);

    // Constant argument → fold
    if (auto* arg_const = std::get_if<ConstantMarker>(&arg)) {
      return ConstantMarker{fold_math(rtbot_type, arg_const->value)};
    }

    auto id = builder.next_id(rtbot_type);
    builder.add_operator(id, rtbot_type);
    builder.connect(std::get<Endpoint>(arg), {id, "i1"});
    ExprResult r = Endpoint{id, "o1"};
    maybe_cache(expr, r);
    return r;
  }

  // CaseExpr: CASE WHEN cond1 THEN expr1 ... ELSE expr_default END
  // Compiled as Multiplexer(N ports) with mutually-exclusive boolean controls.
  if (auto* case_ptr = std::get_if<std::unique_ptr<parser::ast::CaseExpr>>(&expr)) {
    const auto& ce = **case_ptr;
    if (ce.when_clauses.empty()) {
      throw std::runtime_error("CASE expression has no WHEN clauses");
    }

    // Compile each WHEN condition → boolean endpoint.
    std::vector<Endpoint> cond_eps;
    for (const auto& clause : ce.when_clauses) {
      cond_eps.push_back(compile_predicate(clause.condition, input_endpoint,
                                           scope, builder, source_endpoints));
    }

    // Compile each THEN result → number endpoint.
    std::vector<Endpoint> result_eps;
    for (const auto& clause : ce.when_clauses) {
      auto r = compile_expression(clause.result, input_endpoint, scope, builder,
                                  cache, source_endpoints);
      result_eps.push_back(ensure_endpoint_local(std::move(r), input_endpoint, builder));
    }

    // Build mutually-exclusive conditions using LogicalNand(1) for NOT and LogicalAnd(2):
    //   exclusive[0] = cond0
    //   exclusive[i] = NOT(cond0) AND ... AND NOT(cond_{i-1}) AND cond_i
    //   exclusive[else] = NOT(cond0) AND ... AND NOT(cond_{N-1})
    std::vector<Endpoint> exclusive_eps;
    Endpoint not_all_prev;  // NOT(cond0) AND ... AND NOT(cond_{i-1})

    for (size_t i = 0; i < cond_eps.size(); i++) {
      if (i == 0) {
        exclusive_eps.push_back(cond_eps[0]);
        // NOT(cond0) via LogicalNand(1)
        auto nand_id = builder.next_id("not");
        builder.add_operator(nand_id, "LogicalNand", {{"numPorts", 1.0}});
        builder.connect(cond_eps[0], {nand_id, "i1"});
        not_all_prev = {nand_id, "o1"};
      } else {
        // exclusive[i] = not_all_prev AND cond_i
        auto and_id = builder.next_id("and");
        builder.add_operator(and_id, "LogicalAnd", {{"numPorts", 2.0}});
        builder.connect(not_all_prev, {and_id, "i1"});
        builder.connect(cond_eps[i], {and_id, "i2"});
        exclusive_eps.push_back({and_id, "o1"});

        // Update not_all_prev = not_all_prev AND NOT(cond_i)
        auto nand_id = builder.next_id("not");
        builder.add_operator(nand_id, "LogicalNand", {{"numPorts", 1.0}});
        builder.connect(cond_eps[i], {nand_id, "i1"});

        auto new_and_id = builder.next_id("and");
        builder.add_operator(new_and_id, "LogicalAnd", {{"numPorts", 2.0}});
        builder.connect(not_all_prev, {new_and_id, "i1"});
        builder.connect({nand_id, "o1"}, {new_and_id, "i2"});
        not_all_prev = {new_and_id, "o1"};
      }
    }

    // ELSE branch (if present)
    if (ce.else_result.has_value()) {
      exclusive_eps.push_back(not_all_prev);  // else fires when no WHEN matched
      auto r = compile_expression(*ce.else_result, input_endpoint, scope,
                                  builder, cache, source_endpoints);
      result_eps.push_back(ensure_endpoint_local(std::move(r), input_endpoint, builder));
    }

    auto mux_id = builder.next_id("mux");
    builder.add_operator(mux_id, "Multiplexer",
                         {{"numPorts", static_cast<double>(result_eps.size())}});
    for (size_t i = 0; i < result_eps.size(); i++) {
      builder.connect(exclusive_eps[i], {mux_id, "c" + std::to_string(i + 1)});
      builder.connect(result_eps[i], {mux_id, "i" + std::to_string(i + 1)});
    }

    ExprResult r = Endpoint{mux_id, "o1"};
    maybe_cache(expr, r);
    return r;
  }

  throw std::runtime_error("unsupported expression type");
}

namespace {

// Map SQL function name to FusedExpression bytecode opcode.
// Returns -1 if the function is not a fusable unary math function.
double math_func_to_opcode(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  if (upper == "ABS") return 7;    // fused_op::ABS
  if (upper == "SQRT") return 8;   // fused_op::SQRT
  if (upper == "LN" || upper == "LOG") return 9;  // fused_op::LOG
  if (upper == "LOG10") return 10;  // fused_op::LOG10
  if (upper == "EXP") return 11;    // fused_op::EXP
  if (upper == "SIN") return 12;    // fused_op::SIN
  if (upper == "COS") return 13;    // fused_op::COS
  if (upper == "TAN") return 14;    // fused_op::TAN
  if (upper == "SIGN") return 15;   // fused_op::SIGN
  if (upper == "FLOOR") return 16;  // fused_op::FLOOR
  if (upper == "CEIL" || upper == "CEILING") return 17;  // fused_op::CEIL
  if (upper == "ROUND") return 18;  // fused_op::ROUND
  return -1;
}

// Map binary operator string to FusedExpression bytecode opcode.
double binary_op_to_opcode(const std::string& op) {
  if (op == "+") return 2;  // fused_op::ADD
  if (op == "-") return 3;  // fused_op::SUB
  if (op == "*") return 4;  // fused_op::MUL
  if (op == "/") return 5;  // fused_op::DIV
  return -1;
}

// Recursive bytecode compilation for a single expression.
// Appends opcodes to `bytecode`. Returns false if expression is not fusable.
//
// enable_windowed: when true, recognized windowed functions emit tier-1
// opcodes with their window size carried inline. State offsets and aux_args
// are auto-derived by the FusedExpression packer — the compiler never sees
// them. When false, those functions are treated as unfusable (caller falls
// back to the standalone-operator emission path).
bool compile_expr_bytecode_recursive(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    std::vector<double>& bytecode,
    const std::map<std::string, Endpoint>* source_endpoints,
    bool enable_windowed = false) {
  using namespace parser::ast;

  // ColumnRef → INPUT opcode
  if (auto* col = std::get_if<ColumnRef>(&expr)) {
    auto result = scope.resolve(*col);
    if (auto* err = std::get_if<std::string>(&result)) {
      return false;  // unresolved column
    }
    auto& binding = std::get<analyzer::ColumnBinding>(result);
    auto key = std::make_pair(binding.stream_name, binding.index);
    auto it = column_to_input.find(key);
    int input_index;
    if (it != column_to_input.end()) {
      input_index = it->second;
    } else {
      input_index = static_cast<int>(column_to_input.size());
      column_to_input[key] = input_index;
    }
    bytecode.push_back(0);  // fused_op::INPUT
    bytecode.push_back(static_cast<double>(input_index));
    return true;
  }

  // Constant → CONST opcode
  if (auto* c = std::get_if<Constant>(&expr)) {
    int const_index = static_cast<int>(constants.size());
    constants.push_back(c->value);
    bytecode.push_back(1);  // fused_op::CONST
    bytecode.push_back(static_cast<double>(const_index));
    return true;
  }

  // BinaryExpr → recurse left, recurse right, emit binary opcode
  if (auto* bin_ptr = std::get_if<std::unique_ptr<BinaryExpr>>(&expr)) {
    const auto& bin = **bin_ptr;
    double opcode = binary_op_to_opcode(bin.op);
    if (opcode < 0) return false;  // unknown binary operator
    if (!compile_expr_bytecode_recursive(bin.left, scope, column_to_input,
                                         constants, bytecode, source_endpoints,
                                         enable_windowed))
      return false;
    if (!compile_expr_bytecode_recursive(bin.right, scope, column_to_input,
                                         constants, bytecode, source_endpoints,
                                         enable_windowed))
      return false;
    bytecode.push_back(opcode);
    return true;
  }

  // FuncCall → math function opcodes
  if (auto* func_ptr = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    std::string upper_name = func.name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                   ::toupper);

    // POWER(expr, exponent) → recurse base, recurse exponent, emit POW
    if (upper_name == "POWER") {
      if (func.args.size() != 2) return false;
      if (!compile_expr_bytecode_recursive(func.args[0], scope,
                                            column_to_input, constants,
                                            bytecode, source_endpoints,
                                            enable_windowed))
        return false;
      if (!compile_expr_bytecode_recursive(func.args[1], scope,
                                            column_to_input, constants,
                                            bytecode, source_endpoints,
                                            enable_windowed))
        return false;
      bytecode.push_back(6);  // fused_op::POW
      return true;
    }

    // SQRT(expr) → recurse arg, emit SQRT
    if (upper_name == "SQRT") {
      if (func.args.size() != 1) return false;
      if (!compile_expr_bytecode_recursive(func.args[0], scope,
                                            column_to_input, constants,
                                            bytecode, source_endpoints,
                                            enable_windowed))
        return false;
      bytecode.push_back(8);  // fused_op::SQRT
      return true;
    }

    // Unary math functions
    double opcode = math_func_to_opcode(func.name);
    if (opcode >= 0) {
      if (func.args.size() != 1) return false;
      if (!compile_expr_bytecode_recursive(func.args[0], scope,
                                            column_to_input, constants,
                                            bytecode, source_endpoints,
                                            enable_windowed))
        return false;
      bytecode.push_back(opcode);
      return true;
    }

    // Windowed functions (tier-1 opcodes). Emit `OPCODE, W` inline — the
    // FusedExpression packer auto-allocates state slots and builds the
    // internal aux_args side table.
    if (enable_windowed) {
      auto emit_ring_window = [&](int opcode_id) -> bool {
        if (func.args.size() != 2) return false;
        auto* win_const = std::get_if<Constant>(&func.args[1]);
        if (!win_const) return false;
        int W = static_cast<int>(win_const->value);
        if (W <= 0 || W > 65535) return false;
        if (!compile_expr_bytecode_recursive(func.args[0], scope,
                                              column_to_input, constants,
                                              bytecode, source_endpoints,
                                              enable_windowed))
          return false;
        bytecode.push_back(static_cast<double>(opcode_id));
        bytecode.push_back(static_cast<double>(W));
        return true;
      };

      if (upper_name == "MOVING_AVG" || upper_name == "MOVING_AVERAGE") {
        return emit_ring_window(35 /* MA_UPDATE */);
      }
      if (upper_name == "MOVING_SUM") {
        return emit_ring_window(36 /* MSUM_UPDATE */);
      }
      if (upper_name == "STDDEV" || upper_name == "MOVING_STD") {
        return emit_ring_window(37 /* STD_UPDATE */);
      }
    }

    // Not a fusable function (aggregate, windowed when disabled, DSP, TS,
    // TIMESHIFT, etc.)
    return false;
  }

  // Anything else (CaseExpr, StringConstant, ArrayLiteral, ComparisonExpr,
  // LogicalExpr, NotExpr, BetweenExpr) is not fusable.
  return false;
}

// Recursive bytecode compilation for expressions that may contain aggregate
// functions (SUM, COUNT, AVG, MAX, MIN). Non-aggregate sub-expressions
// (ColumnRef, Constant, BinaryExpr, math functions) delegate to the pure
// bytecode compiler. Returns false if expression is not fusable.
bool compile_agg_expr_bytecode_recursive(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    std::vector<double>& bytecode,
    AggBytecodeContext& agg_ctx) {
  using namespace parser::ast;

  // FuncCall — check for aggregate functions first, then delegate to pure path
  if (auto* func_ptr = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    std::string upper_name = func.name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                   ::toupper);

    // SUM(expr) → compile arg recursively, CUMSUM state_idx
    if (upper_name == "SUM") {
      if (func.args.size() != 1) return false;
      if (!compile_agg_expr_bytecode_recursive(func.args[0], scope,
              column_to_input, constants, bytecode, agg_ctx))
        return false;
      int si = static_cast<int>(agg_ctx.state_init.size());
      agg_ctx.state_init.push_back(0.0);  // sum
      agg_ctx.state_init.push_back(0.0);  // kahan compensation
      bytecode.push_back(21);  // fused_op::CUMSUM
      bytecode.push_back(static_cast<double>(si));
      return true;
    }

    // COUNT(*) → COUNT opcode (first use) or STATE_LOAD (subsequent uses)
    if (upper_name == "COUNT") {
      if (!func.args.empty()) return false;  // only COUNT(*) supported
      if (agg_ctx.shared_count_state_idx < 0) {
        // First COUNT — allocate state slot
        agg_ctx.shared_count_state_idx = static_cast<int>(agg_ctx.state_init.size());
        agg_ctx.state_init.push_back(0.0);
        agg_ctx.count_emitted = false;
      }
      if (!agg_ctx.count_emitted) {
        bytecode.push_back(22);  // fused_op::COUNT
        bytecode.push_back(static_cast<double>(agg_ctx.shared_count_state_idx));
        agg_ctx.count_emitted = true;
      } else {
        bytecode.push_back(25);  // fused_op::STATE_LOAD
        bytecode.push_back(static_cast<double>(agg_ctx.shared_count_state_idx));
      }
      return true;
    }

    // AVG(expr) → compile arg, CUMSUM, COUNT/STATE_LOAD, DIV
    if (upper_name == "AVG") {
      if (func.args.size() != 1) return false;
      // CUMSUM(arg)
      if (!compile_agg_expr_bytecode_recursive(func.args[0], scope,
              column_to_input, constants, bytecode, agg_ctx))
        return false;
      int sum_si = static_cast<int>(agg_ctx.state_init.size());
      agg_ctx.state_init.push_back(0.0);  // sum
      agg_ctx.state_init.push_back(0.0);  // kahan compensation
      bytecode.push_back(21);  // fused_op::CUMSUM
      bytecode.push_back(static_cast<double>(sum_si));
      // COUNT (shared across all AVGs and COUNT(*) in the query)
      if (agg_ctx.shared_count_state_idx < 0) {
        agg_ctx.shared_count_state_idx = static_cast<int>(agg_ctx.state_init.size());
        agg_ctx.state_init.push_back(0.0);
        agg_ctx.count_emitted = false;
      }
      if (!agg_ctx.count_emitted) {
        bytecode.push_back(22);  // fused_op::COUNT
        bytecode.push_back(static_cast<double>(agg_ctx.shared_count_state_idx));
        agg_ctx.count_emitted = true;
      } else {
        bytecode.push_back(25);  // fused_op::STATE_LOAD
        bytecode.push_back(static_cast<double>(agg_ctx.shared_count_state_idx));
      }
      // DIV
      bytecode.push_back(5);  // fused_op::DIV
      return true;
    }

    // MAX(expr) → compile arg, MAX_AGG state_idx
    if (upper_name == "MAX") {
      if (func.args.size() != 1) return false;
      if (!compile_agg_expr_bytecode_recursive(func.args[0], scope,
              column_to_input, constants, bytecode, agg_ctx))
        return false;
      int si = static_cast<int>(agg_ctx.state_init.size());
      // Use -DBL_MAX instead of -infinity: JSON (RFC 8259) does not support
      // Infinity/-Infinity, so nlohmann/json serializes them as null, causing
      // deserialization failures.  -DBL_MAX is functionally equivalent for
      // tracking a running maximum (any real input will exceed it).
      agg_ctx.state_init.push_back(-std::numeric_limits<double>::max());
      bytecode.push_back(23);  // fused_op::MAX_AGG
      bytecode.push_back(static_cast<double>(si));
      return true;
    }

    // MIN(expr) → compile arg, MIN_AGG state_idx
    if (upper_name == "MIN") {
      if (func.args.size() != 1) return false;
      if (!compile_agg_expr_bytecode_recursive(func.args[0], scope,
              column_to_input, constants, bytecode, agg_ctx))
        return false;
      int si = static_cast<int>(agg_ctx.state_init.size());
      // Use DBL_MAX instead of +infinity: JSON does not support Infinity.
      // DBL_MAX is functionally equivalent for tracking a running minimum.
      agg_ctx.state_init.push_back(std::numeric_limits<double>::max());
      bytecode.push_back(24);  // fused_op::MIN_AGG
      bytecode.push_back(static_cast<double>(si));
      return true;
    }

    // Non-aggregate functions (POWER, ABS, SQRT, etc.) — delegate to pure path.
    // Windowed/DSP functions (MOVING_AVERAGE, etc.) will return false from the
    // pure path since math_func_to_opcode returns -1 for them.
  }

  // For all non-FuncCall types and non-aggregate FuncCalls, delegate to
  // the pure (non-aggregate) bytecode compiler which handles ColumnRef,
  // Constant, BinaryExpr, and unary math functions.
  return compile_expr_bytecode_recursive(expr, scope, column_to_input,
                                          constants, bytecode, nullptr);
}

// Map comparison operator string to FusedExpression bytecode opcode.
double comparison_op_to_opcode(const std::string& op) {
  if (op == ">") return 26;   // fused_op::GT
  if (op == ">=") return 27;  // fused_op::GTE
  if (op == "<") return 28;   // fused_op::LT
  if (op == "<=") return 29;  // fused_op::LTE
  if (op == "=") return 30;   // fused_op::EQ
  if (op == "!=") return 31;  // fused_op::NEQ
  return -1;
}

// Recursive bytecode compilation for segment expressions.
// Similar to compile_expr_bytecode_recursive but:
// - INPUT arguments use binding.index directly (vector column index) instead of
//   a column_to_input map
// - Handles ComparisonExpr, LogicalExpr, NotExpr, BetweenExpr
// Returns false if expression is not fusable.
bool compile_segment_expr_recursive(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    const std::string& stream_name,
    std::vector<double>& constants,
    std::vector<double>& bytecode) {
  using namespace parser::ast;

  // ColumnRef → INPUT opcode with direct column index
  if (auto* col = std::get_if<ColumnRef>(&expr)) {
    auto result = scope.resolve(*col);
    if (auto* err = std::get_if<std::string>(&result)) {
      return false;  // unresolved column
    }
    auto& binding = std::get<analyzer::ColumnBinding>(result);
    bytecode.push_back(0);  // fused_op::INPUT
    bytecode.push_back(static_cast<double>(binding.index));
    return true;
  }

  // Constant → CONST opcode
  if (auto* c = std::get_if<Constant>(&expr)) {
    int const_index = static_cast<int>(constants.size());
    constants.push_back(c->value);
    bytecode.push_back(1);  // fused_op::CONST
    bytecode.push_back(static_cast<double>(const_index));
    return true;
  }

  // BinaryExpr → recurse left, recurse right, emit binary opcode
  if (auto* bin_ptr = std::get_if<std::unique_ptr<BinaryExpr>>(&expr)) {
    const auto& bin = **bin_ptr;
    double opcode = binary_op_to_opcode(bin.op);
    if (opcode < 0) return false;
    if (!compile_segment_expr_recursive(bin.left, scope, stream_name,
                                         constants, bytecode))
      return false;
    if (!compile_segment_expr_recursive(bin.right, scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(opcode);
    return true;
  }

  // FuncCall → math function opcodes (no aggregates, TS, TIMESHIFT, etc.)
  if (auto* func_ptr = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    std::string upper_name = func.name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                   ::toupper);

    // POWER(expr, exponent) → recurse base, recurse exponent, emit POW
    if (upper_name == "POWER") {
      if (func.args.size() != 2) return false;
      if (!compile_segment_expr_recursive(func.args[0], scope, stream_name,
                                           constants, bytecode))
        return false;
      if (!compile_segment_expr_recursive(func.args[1], scope, stream_name,
                                           constants, bytecode))
        return false;
      bytecode.push_back(6);  // fused_op::POW
      return true;
    }

    // SQRT(expr) → recurse arg, emit SQRT
    if (upper_name == "SQRT") {
      if (func.args.size() != 1) return false;
      if (!compile_segment_expr_recursive(func.args[0], scope, stream_name,
                                           constants, bytecode))
        return false;
      bytecode.push_back(8);  // fused_op::SQRT
      return true;
    }

    // Unary math functions
    double opcode = math_func_to_opcode(func.name);
    if (opcode < 0) {
      // Not a fusable function (aggregate, windowed, DSP, TS, TIMESHIFT, etc.)
      return false;
    }
    if (func.args.size() != 1) return false;
    if (!compile_segment_expr_recursive(func.args[0], scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(opcode);
    return true;
  }

  // ComparisonExpr → recurse left, right, emit comparison opcode
  if (auto* cmp_ptr = std::get_if<std::unique_ptr<ComparisonExpr>>(&expr)) {
    const auto& cmp = **cmp_ptr;
    double opcode = comparison_op_to_opcode(cmp.op);
    if (opcode < 0) return false;
    if (!compile_segment_expr_recursive(cmp.left, scope, stream_name,
                                         constants, bytecode))
      return false;
    if (!compile_segment_expr_recursive(cmp.right, scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(opcode);
    return true;
  }

  // LogicalExpr → recurse left, right, emit AND=32 or OR=33
  if (auto* log_ptr = std::get_if<std::unique_ptr<LogicalExpr>>(&expr)) {
    const auto& log = **log_ptr;
    if (!compile_segment_expr_recursive(log.left, scope, stream_name,
                                         constants, bytecode))
      return false;
    if (!compile_segment_expr_recursive(log.right, scope, stream_name,
                                         constants, bytecode))
      return false;
    std::string upper_op = log.op;
    std::transform(upper_op.begin(), upper_op.end(), upper_op.begin(),
                   ::toupper);
    if (upper_op == "AND") {
      bytecode.push_back(32);  // fused_op::AND
    } else if (upper_op == "OR") {
      bytecode.push_back(33);  // fused_op::OR
    } else {
      return false;
    }
    return true;
  }

  // NotExpr → recurse operand, emit NOT=34
  if (auto* not_ptr = std::get_if<std::unique_ptr<NotExpr>>(&expr)) {
    const auto& not_e = **not_ptr;
    if (!compile_segment_expr_recursive(not_e.operand, scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(34);  // fused_op::NOT
    return true;
  }

  // BetweenExpr → desugar to expr >= low AND expr <= high
  // Compile: expr, low, GTE, expr, high, LTE, AND
  if (auto* bet_ptr = std::get_if<std::unique_ptr<BetweenExpr>>(&expr)) {
    const auto& bet = **bet_ptr;
    // expr >= low
    if (!compile_segment_expr_recursive(bet.expr, scope, stream_name,
                                         constants, bytecode))
      return false;
    if (!compile_segment_expr_recursive(bet.low, scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(27);  // fused_op::GTE
    // expr <= high
    if (!compile_segment_expr_recursive(bet.expr, scope, stream_name,
                                         constants, bytecode))
      return false;
    if (!compile_segment_expr_recursive(bet.high, scope, stream_name,
                                         constants, bytecode))
      return false;
    bytecode.push_back(29);  // fused_op::LTE
    // AND
    bytecode.push_back(32);  // fused_op::AND
    return true;
  }

  // Anything else (CaseExpr, StringConstant, ArrayLiteral) is not fusable.
  return false;
}

}  // anonymous namespace

BytecodeResult compile_expression_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    const std::map<std::string, Endpoint>* source_endpoints,
    bool enable_windowed) {
  BytecodeResult result;
  result.success = compile_expr_bytecode_recursive(
      expr, scope, column_to_input, constants, result.bytecode,
      source_endpoints, enable_windowed);
  if (result.success) {
    result.bytecode.push_back(20);  // fused_op::END
  }
  return result;
}

BytecodeResult compile_aggregate_expression_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    std::map<std::pair<std::string, int>, int>& column_to_input,
    std::vector<double>& constants,
    AggBytecodeContext& agg_ctx) {
  BytecodeResult result;
  result.success = compile_agg_expr_bytecode_recursive(
      expr, scope, column_to_input, constants, result.bytecode, agg_ctx);
  if (result.success) {
    result.bytecode.push_back(20);  // fused_op::END
  }
  return result;
}

SegmentBytecodeResult compile_segment_to_bytecode(
    const parser::ast::Expr& expr,
    const analyzer::Scope& scope,
    const std::string& stream_name) {
  SegmentBytecodeResult result;
  result.success = compile_segment_expr_recursive(
      expr, scope, stream_name, result.constants, result.bytecode);
  if (result.success) {
    result.bytecode.push_back(20);  // fused_op::END
  }
  return result;
}

}  // namespace rtbot_sql::compiler
