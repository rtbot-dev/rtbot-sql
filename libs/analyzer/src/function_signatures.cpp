#include "rtbot_sql/analyzer/function_signatures.h"

#include <algorithm>
#include <string>

#include "rtbot_sql/analyzer/const_fold.h"
#include "rtbot_sql/analyzer/expr_span.h"

namespace rtbot_sql::analyzer {

namespace {

std::string upper(const std::string& s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(), ::toupper);
  return out;
}

// True for unary math functions. Combines the standalone-operator list
// (math_func_to_rtbot in libs/compiler/src/expression_compiler.cpp) with
// the fused-bytecode list (math_func_to_opcode in the same file). SQRT is
// fused-only, so it appears here even though no standalone operator exists.
bool is_unary_math(const std::string& u) {
  return u == "ABS" || u == "SQRT" || u == "FLOOR" ||
         u == "CEIL" || u == "CEILING" || u == "ROUND" ||
         u == "LN" || u == "LOG" || u == "LOG10" || u == "EXP" ||
         u == "SIN" || u == "COS" || u == "TAN" || u == "SIGN";
}

// True for aggregate / cumulative / windowed functions handled in
// function_compiler.cpp.
bool is_aggregate_or_windowed(const std::string& u) {
  return u == "SUM" || u == "COUNT" || u == "AVG" || u == "MIN" ||
         u == "MAX" || u == "MOVING_AVERAGE" || u == "MOVING_SUM" ||
         u == "MOVING_COUNT" || u == "MOVING_STD" || u == "STDDEV" ||
         u == "MOVING_MIN" || u == "MOVING_MAX" || u == "DIFF" ||
         u == "FIR" || u == "IIR" || u == "RESAMPLE" ||
         u == "PEAK_DETECT";
}

// True for built-in scalar functions handled in expression_compiler.cpp
// (TS, POWER, TIMESHIFT, RESAMPLE_CONSTANT).
bool is_scalar_builtin(const std::string& u) {
  return u == "TS" || u == "POWER" || u == "TIMESHIFT" ||
         u == "RESAMPLE_CONSTANT";
}

// Validate that `arg` constant-folds to a positive integer value. Folded
// expressions like `2*3` and `FLOOR(7.7)` are accepted because the result
// is an integer; `2.5` and `cpu+1` are rejected.
void check_const_int_positive(const parser::ast::FuncCall& fc, std::size_t idx,
                              const std::string& param_name,
                              DiagnosticBag& bag, std::string_view sql) {
  if (idx >= fc.args.size()) return;
  const auto& arg = fc.args[idx];
  parser::ast::SourceLocation loc = expr_span(arg, sql);
  auto folded = try_fold(arg);
  if (!folded) {
    bag.error(fc.name + ": " + param_name + " must be a constant integer", loc);
    return;
  }
  int v = static_cast<int>(*folded);
  if (v != *folded || v <= 0) {
    bag.error(fc.name + ": " + param_name + " must be a positive integer", loc);
  }
}

// Validate that `arg` is an ArrayLiteral. Mirrors the FIR/IIR array checks
// in function_compiler.cpp.
void check_array_literal(const parser::ast::FuncCall& fc, std::size_t idx,
                         const std::string& msg, DiagnosticBag& bag) {
  if (idx >= fc.args.size()) return;
  const auto& arg = fc.args[idx];
  if (!std::get_if<parser::ast::ArrayLiteral>(&arg)) {
    bag.error(msg, parser::ast::loc_of(arg));
  }
}

}  // namespace

bool is_known_function(const std::string& name) {
  std::string u = upper(name);
  return is_unary_math(u) || is_aggregate_or_windowed(u) || is_scalar_builtin(u);
}

void validate_function_call(const parser::ast::FuncCall& fc,
                            DiagnosticBag& bag, std::string_view sql) {
  std::string u = upper(fc.name);

  // --- Cumulative aggregates (function_compiler.cpp) ---

  if (u == "SUM") {
    if (fc.args.size() != 1) {
      bag.error("SUM requires exactly 1 argument", fc.loc);
    }
    return;
  }
  if (u == "COUNT") {
    if (!fc.args.empty()) {
      bag.error(
          "COUNT(*) takes no arguments (use COUNT(*), not COUNT(expr))",
          fc.loc);
    }
    return;
  }
  if (u == "AVG") {
    if (fc.args.size() != 1) {
      bag.error("AVG requires exactly 1 argument", fc.loc);
    }
    return;
  }
  if (u == "MIN") {
    if (fc.args.size() != 1) {
      bag.error("MIN requires exactly 1 argument", fc.loc);
    }
    return;
  }
  if (u == "MAX") {
    if (fc.args.size() != 1) {
      bag.error("MAX requires exactly 1 argument", fc.loc);
    }
    return;
  }

  // --- Windowed functions (function_compiler.cpp) ---

  if (u == "MOVING_AVERAGE") {
    if (fc.args.size() != 2) {
      bag.error("MOVING_AVERAGE requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }
  if (u == "MOVING_SUM") {
    if (fc.args.size() != 2) {
      bag.error("MOVING_SUM requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }
  if (u == "MOVING_COUNT") {
    if (fc.args.size() != 1) {
      bag.error("MOVING_COUNT requires 1 argument: (window_size)", fc.loc);
      return;
    }
    check_const_int_positive(fc, 0, "window_size", bag, sql);
    return;
  }
  if (u == "MOVING_STD") {
    if (fc.args.size() != 2) {
      bag.error("MOVING_STD requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }
  if (u == "STDDEV") {
    if (fc.args.size() != 2) {
      bag.error("STDDEV requires 2 arguments: (expr, window_size)", fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }
  if (u == "MOVING_MIN") {
    if (fc.args.size() != 2) {
      bag.error("MOVING_MIN requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }
  if (u == "MOVING_MAX") {
    if (fc.args.size() != 2) {
      bag.error("MOVING_MAX requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }

  // --- DSP functions ---

  if (u == "DIFF") {
    if (fc.args.size() != 1) {
      bag.error("DIFF requires exactly 1 argument", fc.loc);
    }
    return;
  }
  if (u == "FIR") {
    if (fc.args.size() != 2) {
      bag.error("FIR requires 2 arguments: (expr, ARRAY[coefficients])",
                fc.loc);
      return;
    }
    check_array_literal(fc, 1, "FIR: second argument must be an array literal",
                        bag);
    return;
  }
  if (u == "IIR") {
    if (fc.args.size() != 3) {
      bag.error(
          "IIR requires 3 arguments: (expr, ARRAY[a_coeffs], ARRAY[b_coeffs])",
          fc.loc);
      return;
    }
    // The compiler-side throw is a single message covering both args; replicate
    // it on each non-array arg so the user sees an error at the offending site.
    bool a_ok = std::get_if<parser::ast::ArrayLiteral>(&fc.args[1]) != nullptr;
    bool b_ok = std::get_if<parser::ast::ArrayLiteral>(&fc.args[2]) != nullptr;
    if (!a_ok || !b_ok) {
      bag.error("IIR: second and third arguments must be array literals",
                a_ok ? parser::ast::loc_of(fc.args[2])
                     : parser::ast::loc_of(fc.args[1]));
    }
    return;
  }
  if (u == "RESAMPLE") {
    if (fc.args.size() != 2) {
      bag.error("RESAMPLE requires 2 arguments: (expr, interval)", fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "interval", bag, sql);
    return;
  }
  if (u == "PEAK_DETECT") {
    if (fc.args.size() != 2) {
      bag.error("PEAK_DETECT requires 2 arguments: (expr, window_size)",
                fc.loc);
      return;
    }
    check_const_int_positive(fc, 1, "window_size", bag, sql);
    return;
  }

  // --- Scalar built-ins (expression_compiler.cpp) ---

  if (u == "TS") {
    if (!fc.args.empty()) {
      bag.error("TS() takes no arguments", fc.loc);
    }
    return;
  }
  if (u == "POWER") {
    if (fc.args.size() != 2) {
      bag.error("POWER requires exactly 2 arguments", fc.loc);
      return;
    }
    // Mirrors libs/compiler/src/expression_compiler.cpp:292: the exponent
    // must constant-fold (POWER(x, 2*3) is fine; POWER(x, col) is not).
    if (!try_fold(fc.args[1])) {
      bag.error("POWER exponent must be a constant",
                expr_span(fc.args[1], sql));
    }
    return;
  }
  if (u == "TIMESHIFT") {
    if (fc.args.size() != 2) {
      bag.error("TIMESHIFT requires exactly 2 arguments", fc.loc);
      return;
    }
    // Mirrors libs/compiler/src/expression_compiler.cpp:317.
    if (!try_fold(fc.args[1])) {
      bag.error("TIMESHIFT shift must be a constant",
                expr_span(fc.args[1], sql));
    }
    return;
  }
  if (u == "RESAMPLE_CONSTANT") {
    if (fc.args.size() < 2 || fc.args.size() > 3) {
      bag.error("RESAMPLE_CONSTANT requires 2 or 3 arguments", fc.loc);
      return;
    }
    // Mirrors libs/compiler/src/expression_compiler.cpp:342, 354.
    if (!try_fold(fc.args[1])) {
      bag.error("RESAMPLE_CONSTANT interval must be a constant",
                expr_span(fc.args[1], sql));
    }
    if (fc.args.size() == 3 && !try_fold(fc.args[2])) {
      bag.error("RESAMPLE_CONSTANT snap_first must be a constant",
                expr_span(fc.args[2], sql));
    }
    return;
  }

  // --- Unary math ---

  if (is_unary_math(u)) {
    if (fc.args.size() != 1) {
      bag.error(fc.name + " requires exactly 1 argument", fc.loc);
    }
    return;
  }

  // --- Unknown ---

  bag.error("unknown function: " + fc.name, fc.loc);
}

}  // namespace rtbot_sql::analyzer
