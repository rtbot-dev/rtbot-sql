#include "rtbot_sql/planner/evaluator.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>

namespace rtbot_sql::planner {

namespace {

int resolve_column_index(const parser::ast::ColumnRef& ref,
                         const StreamSchema& schema) {
  auto idx = schema.column_index(ref.column_name);
  if (!idx.has_value()) {
    throw std::runtime_error("unknown column: " + ref.column_name);
  }
  return *idx;
}

std::function<double(double)> resolve_math_func(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

  if (upper == "ABS") return [](double v) { return std::abs(v); };
  if (upper == "FLOOR") return [](double v) { return std::floor(v); };
  if (upper == "CEIL") return [](double v) { return std::ceil(v); };
  if (upper == "SQRT") return [](double v) { return std::sqrt(v); };
  if (upper == "LOG") return [](double v) { return std::log(v); };
  if (upper == "LOG2") return [](double v) { return std::log2(v); };
  if (upper == "LOG10") return [](double v) { return std::log10(v); };
  if (upper == "EXP") return [](double v) { return std::exp(v); };
  if (upper == "ROUND") return [](double v) { return std::round(v); };
  if (upper == "SIGN")
    return [](double v) { return (v > 0) ? 1.0 : (v < 0) ? -1.0 : 0.0; };

  return nullptr;
}

char binary_op_char(const std::string& op) {
  if (op == "+") return '+';
  if (op == "-") return '-';
  if (op == "*") return '*';
  if (op == "/") return '/';
  throw std::runtime_error("unsupported binary operator: " + op);
}

}  // namespace

std::unique_ptr<CompiledExpr> compile_for_eval(
    const parser::ast::Expr& expr, const StreamSchema& schema) {
  // ColumnRef
  if (auto* col = std::get_if<parser::ast::ColumnRef>(&expr)) {
    return std::make_unique<ColumnAccess>(resolve_column_index(*col, schema));
  }

  // Constant
  if (auto* c = std::get_if<parser::ast::Constant>(&expr)) {
    return std::make_unique<ConstantExpr>(c->value);
  }

  // BinaryExpr (+, -, *, /)
  if (auto* bin = std::get_if<std::unique_ptr<parser::ast::BinaryExpr>>(&expr)) {
    auto left = compile_for_eval((*bin)->left, schema);
    auto right = compile_for_eval((*bin)->right, schema);
    return std::make_unique<BinaryOpExpr>(binary_op_char((*bin)->op),
                                          std::move(left), std::move(right));
  }

  // ComparisonExpr
  if (auto* cmp =
          std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&expr)) {
    auto left = compile_for_eval((*cmp)->left, schema);
    auto right = compile_for_eval((*cmp)->right, schema);
    return std::make_unique<ComparisonEvalExpr>((*cmp)->op, std::move(left),
                                                std::move(right));
  }

  // LogicalExpr (AND, OR)
  if (auto* log =
          std::get_if<std::unique_ptr<parser::ast::LogicalExpr>>(&expr)) {
    auto left = compile_for_eval((*log)->left, schema);
    auto right = compile_for_eval((*log)->right, schema);
    std::string upper = (*log)->op;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    if (upper == "AND") {
      return std::make_unique<LogicalAndExpr>(std::move(left),
                                              std::move(right));
    }
    return std::make_unique<LogicalOrExpr>(std::move(left), std::move(right));
  }

  // NotExpr
  if (auto* not_e =
          std::get_if<std::unique_ptr<parser::ast::NotExpr>>(&expr)) {
    return std::make_unique<NotEvalExpr>(
        compile_for_eval((*not_e)->operand, schema));
  }

  // FuncCall (math functions only — aggregates are not valid in Tier 2)
  if (auto* func_ptr =
          std::get_if<std::unique_ptr<parser::ast::FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    auto math_fn = resolve_math_func(func.name);
    if (!math_fn) {
      throw std::runtime_error(
          "function not supported in scan-time evaluation: " + func.name);
    }
    if (func.args.size() != 1) {
      throw std::runtime_error(func.name + " requires exactly 1 argument");
    }
    return std::make_unique<UnaryFuncExpr>(std::move(math_fn),
                                           compile_for_eval(func.args[0], schema));
  }

  throw std::runtime_error("unsupported expression type in evaluator");
}

bool evaluate_where(const CompiledExpr& predicate,
                    const std::vector<double>& row) {
  return predicate.evaluate(row) != 0.0;
}

std::vector<double> evaluate_select(
    const std::vector<std::unique_ptr<CompiledExpr>>& select_exprs,
    const std::vector<double>& row) {
  std::vector<double> result;
  result.reserve(select_exprs.size());
  for (const auto& expr : select_exprs) {
    result.push_back(expr->evaluate(row));
  }
  return result;
}

}  // namespace rtbot_sql::planner
