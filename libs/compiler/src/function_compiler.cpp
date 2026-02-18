#include "rtbot_sql/compiler/function_compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expr_cache.h"
#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Convert a VectorNumber endpoint into a Number endpoint by extracting
// index 0. Used as a "clock" signal for operators that need NumberData
// but only have access to the raw VectorNumber input stream.
Endpoint scalar_clock(const Endpoint& vec_input, GraphBuilder& builder) {
  auto id = builder.next_id("clock");
  builder.add_operator(id, "VectorExtract", {{"index", 0.0}});
  builder.connect(vec_input, {id, "i1"});
  return {id, "o1"};
}

// Ensure an ExprResult is an Endpoint, materializing constants if needed.
// The input_endpoint is expected to be a VectorNumber stream; a scalar
// clock is derived from it so that ConstantNumber receives NumberData.
Endpoint ensure_endpoint(ExprResult result, const Endpoint& input_endpoint,
                         GraphBuilder& builder) {
  if (auto* ep = std::get_if<Endpoint>(&result)) {
    return *ep;
  }
  auto& cm = std::get<ConstantMarker>(result);
  auto clock_ep = scalar_clock(input_endpoint, builder);
  auto id = builder.next_id("const");
  builder.add_operator(id, "ConstantNumber", {{"value", cm.value}});
  builder.connect(clock_ep, {id, "i1"});
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
         upper == "STDDEV" || upper == "MOVING_MIN" || upper == "MOVING_MAX" ||
         upper == "FIR" || upper == "IIR" ||
         upper == "RESAMPLE" || upper == "PEAK_DETECT";
}

Endpoint compile_function(const std::string& name,
                          const std::vector<parser::ast::Expr>& args,
                          const Endpoint& input_endpoint,
                          const analyzer::Scope& scope,
                          GraphBuilder& builder,
                          ExprCache* cache) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

  // --- Cumulative aggregates ---

  if (upper == "SUM") {
    if (args.size() != 1) {
      throw std::runtime_error("SUM requires exactly 1 argument");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder, cache),
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
    auto clock_ep = scalar_clock(input_endpoint, builder);
    auto cnt_id = builder.next_id("count");
    builder.add_operator(cnt_id, "CountNumber");
    builder.connect(clock_ep, {cnt_id, "i1"});
    return {cnt_id, "o1"};
  }

  if (upper == "AVG") {
    if (args.size() != 1) {
      throw std::runtime_error("AVG requires exactly 1 argument");
    }
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto sum_id = builder.next_id("cumsum");
    builder.add_operator(sum_id, "CumulativeSum");
    builder.connect(expr_ep, {sum_id, "i1"});

    auto cnt_id = builder.next_id("count");
    builder.add_operator(cnt_id, "CountNumber");
    builder.connect(expr_ep, {cnt_id, "i1"});

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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto ma_id = builder.next_id("mavg");
    builder.add_operator(ma_id, "MovingAverage",
                         {{"window_size", static_cast<double>(window)}});
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto ms_id = builder.next_id("msum");
    builder.add_operator(ms_id, "MovingSum",
                         {{"window_size", static_cast<double>(window)}});
    builder.connect(expr_ep, {ms_id, "i1"});
    return {ms_id, "o1"};
  }

  if (upper == "MOVING_COUNT") {
    if (args.size() != 1) {
      throw std::runtime_error(
          "MOVING_COUNT requires 1 argument: (window_size)");
    }
    int window = require_constant_int("MOVING_COUNT", args[0], "window_size");
    // scalar_clock → ConstantNumber(1) → MovingSum(N)
    auto clock_ep = scalar_clock(input_endpoint, builder);
    auto const_id = builder.next_id("const");
    builder.add_operator(const_id, "ConstantNumber", {{"value", 1.0}});
    builder.connect(clock_ep, {const_id, "i1"});

    auto ms_id = builder.next_id("msum");
    builder.add_operator(ms_id, "MovingSum",
                         {{"window_size", static_cast<double>(window)}});
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto sd_id = builder.next_id("stddev");
    builder.add_operator(sd_id, "StandardDeviation",
                         {{"window_size", static_cast<double>(window)}});
    builder.connect(expr_ep, {sd_id, "i1"});
    return {sd_id, "o1"};
  }

  // STDDEV is an alias for MOVING_STD
  if (upper == "STDDEV") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "STDDEV requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("STDDEV", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto sd_id = builder.next_id("stddev");
    builder.add_operator(sd_id, "StandardDeviation",
                         {{"window_size", static_cast<double>(window)}});
    builder.connect(expr_ep, {sd_id, "i1"});
    return {sd_id, "o1"};
  }

  // --- Windowed min/max ---

  if (upper == "MOVING_MIN") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "MOVING_MIN requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("MOVING_MIN", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto id = builder.next_id("wmin");
    builder.add_operator(id, "WindowMinMax",
                         {{"window_size", static_cast<double>(window)}},
                         {{"mode", "min"}});
    builder.connect(expr_ep, {id, "i1"});
    return {id, "o1"};
  }

  if (upper == "MOVING_MAX") {
    if (args.size() != 2) {
      throw std::runtime_error(
          "MOVING_MAX requires 2 arguments: (expr, window_size)");
    }
    int window = require_constant_int("MOVING_MAX", args[1], "window_size");
    auto expr_ep = ensure_endpoint(
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto id = builder.next_id("wmax");
    builder.add_operator(id, "WindowMinMax",
                         {{"window_size", static_cast<double>(window)}},
                         {{"mode", "max"}});
    builder.connect(expr_ep, {id, "i1"});
    return {id, "o1"};
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto fir_id = builder.next_id("fir");
    std::vector<double> coeffs(arr->values.begin(), arr->values.end());
    builder.add_operator(fir_id, "FiniteImpulseResponse", {}, {},
                         {{"coeff", coeffs}});
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto iir_id = builder.next_id("iir");
    std::vector<double> a_coeffs(a_arr->values.begin(), a_arr->values.end());
    std::vector<double> b_coeffs(b_arr->values.begin(), b_arr->values.end());
    builder.add_operator(iir_id, "InfiniteImpulseResponse", {}, {},
                         {{"a_coeffs", a_coeffs}, {"b_coeffs", b_coeffs}});
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
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
        compile_expression(args[0], input_endpoint, scope, builder, cache),
        input_endpoint, builder);
    auto pd_id = builder.next_id("peakdet");
    builder.add_operator(pd_id, "PeakDetector",
                         {{"window_size", static_cast<double>(window)}});
    builder.connect(expr_ep, {pd_id, "i1"});
    return {pd_id, "o1"};
  }

  throw std::runtime_error("unknown function: " + name);
}

}  // namespace rtbot_sql::compiler
