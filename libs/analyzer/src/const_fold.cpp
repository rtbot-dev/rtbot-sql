#include "rtbot_sql/analyzer/const_fold.h"

#include <algorithm>
#include <cmath>
#include <string>
#include <variant>

namespace rtbot_sql::analyzer {

namespace {

std::string upper(const std::string& s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return out;
}

// Apply a unary math function. Returns nullopt if the name isn't a
// recognized math function. Mirrors fold_math in expression_compiler.cpp
// (and the SQRT case from the fused-bytecode path).
std::optional<double> apply_math(const std::string& name, double v) {
  std::string u = upper(name);
  if (u == "ABS") return std::abs(v);
  if (u == "FLOOR") return std::floor(v);
  if (u == "CEIL" || u == "CEILING") return std::ceil(v);
  if (u == "ROUND") return std::round(v);
  if (u == "LN" || u == "LOG") return std::log(v);
  if (u == "LOG10") return std::log10(v);
  if (u == "EXP") return std::exp(v);
  if (u == "SIN") return std::sin(v);
  if (u == "COS") return std::cos(v);
  if (u == "TAN") return std::tan(v);
  if (u == "SIGN") return (v > 0) ? 1.0 : (v < 0) ? -1.0 : 0.0;
  if (u == "SQRT") return std::sqrt(v);
  return std::nullopt;
}

}  // namespace

std::optional<double> try_fold(const parser::ast::Expr& expr) {
  using namespace parser::ast;
  return std::visit(
      [](const auto& v) -> std::optional<double> {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, Constant>) {
          return v.value;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<BinaryExpr>>) {
          if (!v) return std::nullopt;
          auto l = try_fold(v->left);
          auto r = try_fold(v->right);
          if (!l || !r) return std::nullopt;
          if (v->op == "+") return *l + *r;
          if (v->op == "-") return *l - *r;
          if (v->op == "*") return *l * *r;
          if (v->op == "/") return *r == 0.0 ? std::nullopt
                                              : std::optional<double>(*l / *r);
          return std::nullopt;
        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          if (!v || v->args.size() != 1) return std::nullopt;
          auto arg = try_fold(v->args[0]);
          if (!arg) return std::nullopt;
          return apply_math(v->name, *arg);
        } else {
          (void)v;
          return std::nullopt;
        }
      },
      expr);
}

}  // namespace rtbot_sql::analyzer
