#include "rtbot_sql/compiler/function_compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Ensure an ExprResult is an Endpoint, materializing constants if needed.
Endpoint ensure_endpoint(ExprResult result, const Endpoint& input_endpoint,
                         GraphBuilder& builder) {
  if (auto* ep = std::get_if<Endpoint>(&result)) {
    return *ep;
  }
  auto& cm = std::get<ConstantMarker>(result);
  auto id = builder.next_id("const");
  builder.add_operator(id, "ConstantNumber", {{"value", cm.value}});
  builder.connect(input_endpoint, {id, "i1"});
  return {id, "o1"};
}

// Extract a constant integer argument (for window sizes).
int require_constant_int(const std::string& func_name,
                         const parser::ast::Expr& expr,
                         const std::string& param_name) {
  if (auto* c = std::get_if<parser::ast::Constant>(&expr)) {
    int val = static_cast<int>(c->value);
    if (val != c->value || val <= 0) {
      throw std::runtime_error(func_name + ": " + param_name +
                               " must be a positive integer");
    }
    return val;
  }
  throw std::runtime_error(func_name + ": " + param_name +
                           " must be a constant integer");
}

}  // namespace

bool is_aggregate_or_windowed(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  return upper == "SUM" || upper == "COUNT" || upper == "AVG" ||
         upper == "MOVING_AVERAGE" || upper == "MOVING_SUM" ||
         upper == "MOVING_COUNT" || upper == "MOVING_STD" ||
         upper == "FIR" || upper == "IIR" ||
         upper == "RESAMPLE" || upper == "PEAK_DETECT";
}

Endpoint compile_function(const std::string& name,
                          const std::vector<parser::ast::Expr>& args,
                          const Endpoint& input_endpoint,
                          const analyzer::Scope& scope,
                          GraphBuilder& builder) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

  // --- Cumulative aggregates ---

  if (upper == "SUM") {
    if (args.size() != 1) {
      throw std::runtime_error("SUM requires exactly 1 argument");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto sum_id = builder.next_id("cumsum");
    builder.add_operator(sum_id, "CumulativeSum");
    builder.connect(expr_ep, {sum_id, "i1"});
    return {sum_id, "o1"};
  }

  if (upper == "COUNT") {
    if (!args.empty()) {
      throw std::runtime_error(
          "COUNT(*) takes no arguments (use COUNT(*), not COUNT(expr))");
    }
    auto cnt_id = builder.next_id("count");
    builder.add_operator(cnt_id, "Count");
    builder.connect(input_endpoint, {cnt_id, "i1"});
    return {cnt_id, "o1"};
  }

  if (upper == "AVG") {
    if (args.size() != 1) {
      throw std::runtime_error("AVG requires exactly 1 argument");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto sum_id = builder.next_id("cumsum");
    builder.add_operator(sum_id, "CumulativeSum");
    builder.connect(expr_ep, {sum_id, "i1"});

    auto cnt_id = builder.next_id("count");
    builder.add_operator(cnt_id, "Count");
    builder.connect(input_endpoint, {cnt_id, "i1"});

    auto div_id = builder.next_id("div");
    builder.add_operator(div_id, "Division", {{"numPorts", 2}});
    builder.connect({sum_id, "o1"}, {div_id, "i1"});
    builder.connect({cnt_id, "o1"}, {div_id, "i2"});
    return {div_id, "o1"};
  }

  // --- Windowed functions ---

  if (upper == "MOVING_AVERAGE") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "MOVING_AVERAGE requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("MOVING_AVERAGE", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto ma_id = builder.next_id("mavg");
    builder.add_operator(ma_id, "MovingAverage",
                         {{"window", static_cast<double>(window)}});
    builder.connect(expr_ep, {ma_id, "i1"});
    return {ma_id, "o1"};
  }

  if (upper == "MOVING_SUM") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "MOVING_SUM requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("MOVING_SUM", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto ms_id = builder.next_id("msum");
    builder.add_operator(ms_id, "MovingSum",
                         {{"window", static_cast<double>(window)}});
    builder.connect(expr_ep, {ms_id, "i1"});
    return {ms_id, "o1"};
  }

  if (upper == "MOVING_COUNT") {
    if (args.size() != 1) {
      throw std::runtime_error(
          "MOVING_COUNT requires 1 argument: (window_size)");
    }
    int window = require_constant_int("MOVING_COUNT", args[0], "window_size");
    // ConstantNumber(1) → MovingSum(N)
    auto const_id = builder.next_id("const");
    builder.add_operator(const_id, "ConstantNumber", {{"value", 1.0}});
    builder.connect(input_endpoint, {const_id, "i1"});

    auto ms_id = builder.next_id("msum");
    builder.add_operator(ms_id, "MovingSum",
                         {{"window", static_cast<double>(window)}});
    builder.connect({const_id, "o1"}, {ms_id, "i1"});
    return {ms_id, "o1"};
  }

  if (upper == "MOVING_STD") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "MOVING_STD requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("MOVING_STD", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto sd_id = builder.next_id("stddev");
    builder.add_operator(sd_id, "StandardDeviation",
                         {{"window", static_cast<double>(window)}});
    builder.connect(expr_ep, {sd_id, "i1"});
    return {sd_id, "o1"};
  }

  // --- DSP functions (stubs — generate operators, detailed testing in Phase 2) ---

  if (upper == "FIR") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "FIR requires 2 arguments: (expr, ARRAY[coefficients])");
    }
    auto* arr = std::get_if<parser::ast::ArrayLiteral>(&args[1]);
    if (!arr) {
      throw std::runtime_error("FIR: second argument must be an array literal");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto fir_id = builder.next_id("fir");
    std::map<std::string, double> params;
    params["numCoeffs"] = static_cast<double>(arr->values.size());
    for (size_t i = 0; i < arr->values.size(); ++i) {
      params["coeff_" + std::to_string(i)] = arr->values[i];
    }
    builder.add_operator(fir_id, "FiniteImpulseResponse", params);
    builder.connect(expr_ep, {fir_id, "i1"});
    return {fir_id, "o1"};
  }

  if (upper == "IIR") {
    if (args.size() != 3) {
      throw std::runtime_error(
          "IIR requires 3 arguments: (expr, ARRAY[a_coeffs], ARRAY[b_coeffs])");
    }
    auto* a_arr = std::get_if<parser::ast::ArrayLiteral>(&args[1]);
    auto* b_arr = std::get_if<parser::ast::ArrayLiteral>(&args[2]);
    if (!a_arr || !b_arr) {
      throw std::runtime_error(
          "IIR: second and third arguments must be array literals");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto iir_id = builder.next_id("iir");
    std::map<std::string, double> params;
    params["numA"] = static_cast<double>(a_arr->values.size());
    params["numB"] = static_cast<double>(b_arr->values.size());
    for (size_t i = 0; i < a_arr->values.size(); ++i) {
      params["a_" + std::to_string(i)] = a_arr->values[i];
    }
    for (size_t i = 0; i < b_arr->values.size(); ++i) {
      params["b_" + std::to_string(i)] = b_arr->values[i];
    }
    builder.add_operator(iir_id, "InfiniteImpulseResponse", params);
    builder.connect(expr_ep, {iir_id, "i1"});
    return {iir_id, "o1"};
  }

  if (upper == "RESAMPLE") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "RESAMPLE requires 2 arguments: (expr, interval)");
    }
    int interval = require_constant_int("RESAMPLE", args[1], "interval");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto rs_id = builder.next_id("resample");
    builder.add_operator(rs_id, "ResamplerConstant",
                         {{"interval", static_cast<double>(interval)}});
    builder.connect(expr_ep, {rs_id, "i1"});
    return {rs_id, "o1"};
  }

  if (upper == "PEAK_DETECT") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "PEAK_DETECT requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("PEAK_DETECT", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder),
        input_endpoint, builder);
    auto pd_id = builder.next_id("peakdet");
    builder.add_operator(pd_id, "PeakDetector",
                         {{"window", static_cast<double>(window)}});
    builder.connect(expr_ep, {pd_id, "i1"});
    return {pd_id, "o1"};
  }

  throw std::runtime_error("unknown function: " + name);
}

}  // namespace rtbot_sql::compiler
