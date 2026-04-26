#include "rtbot_sql/analyzer/expr_validator.h"

#include <variant>

#include "rtbot_sql/analyzer/expr_span.h"
#include "rtbot_sql/analyzer/function_signatures.h"

namespace rtbot_sql::analyzer {

namespace {

void visit(const parser::ast::Expr& expr, DiagnosticBag& bag,
           const Scope* scope, const std::set<std::string>* aliases,
           std::string_view sql);

void visit_func_call(const parser::ast::FuncCall& fc, DiagnosticBag& bag,
                     const Scope* scope,
                     const std::set<std::string>* aliases,
                     std::string_view sql) {
  validate_function_call(fc, bag, sql);
  for (const auto& arg : fc.args) {
    visit(arg, bag, scope, aliases, sql);
  }
}

void visit_case_expr(const parser::ast::CaseExpr& ce, DiagnosticBag& bag,
                     const Scope* scope,
                     const std::set<std::string>* aliases,
                     std::string_view sql) {
  // Mirrors the throw at libs/compiler/src/expression_compiler.cpp:426.
  if (ce.when_clauses.empty()) {
    bag.error("CASE expression has no WHEN clauses", ce.loc);
  }
  for (const auto& clause : ce.when_clauses) {
    visit(clause.condition, bag, scope, aliases, sql);
    visit(clause.result, bag, scope, aliases, sql);
  }
  if (ce.else_result) visit(*ce.else_result, bag, scope, aliases, sql);
}

void visit(const parser::ast::Expr& expr, DiagnosticBag& bag,
           const Scope* scope, const std::set<std::string>* aliases,
           std::string_view sql) {
  std::visit(
      [&bag, scope, aliases, sql](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, parser::ast::ColumnRef>) {
          // Resolve when a scope is available. Mirrors the throw at
          // libs/compiler/src/expression_compiler.cpp:203. Skip when the
          // ref names a SELECT-list alias — the compiler's alias-expander
          // substitutes it before column resolution actually runs.
          if (!scope) return;
          if (aliases && v.table_alias.empty() &&
              aliases->count(v.column_name)) return;
          auto result = scope->resolve(v);
          if (auto* err = std::get_if<std::string>(&result)) {
            bag.error(*err, v.loc);
          }
        } else if constexpr (std::is_same_v<T, parser::ast::Constant> ||
                             std::is_same_v<T, parser::ast::StringConstant> ||
                             std::is_same_v<T, parser::ast::ArrayLiteral>) {
          (void)v;
        } else if constexpr (std::is_same_v<
                                 T,
                                 std::unique_ptr<parser::ast::BinaryExpr>>) {
          if (v) {
            visit(v->left, bag, scope, aliases, sql);
            visit(v->right, bag, scope, aliases, sql);
          }
        } else if constexpr (std::is_same_v<
                                 T,
                                 std::unique_ptr<parser::ast::ComparisonExpr>>) {
          if (v) {
            visit(v->left, bag, scope, aliases, sql);
            visit(v->right, bag, scope, aliases, sql);
          }
        } else if constexpr (std::is_same_v<
                                 T, std::unique_ptr<parser::ast::FuncCall>>) {
          if (v) visit_func_call(*v, bag, scope, aliases, sql);
        } else if constexpr (std::is_same_v<
                                 T,
                                 std::unique_ptr<parser::ast::LogicalExpr>>) {
          if (v) {
            visit(v->left, bag, scope, aliases, sql);
            visit(v->right, bag, scope, aliases, sql);
          }
        } else if constexpr (std::is_same_v<
                                 T, std::unique_ptr<parser::ast::NotExpr>>) {
          if (v) visit(v->operand, bag, scope, aliases, sql);
        } else if constexpr (std::is_same_v<
                                 T,
                                 std::unique_ptr<parser::ast::BetweenExpr>>) {
          if (v) {
            visit(v->expr, bag, scope, aliases, sql);
            visit(v->low, bag, scope, aliases, sql);
            visit(v->high, bag, scope, aliases, sql);
          }
        } else if constexpr (std::is_same_v<
                                 T, std::unique_ptr<parser::ast::CaseExpr>>) {
          if (v) visit_case_expr(*v, bag, scope, aliases, sql);
        }
      },
      expr);
}

}  // namespace

void validate_expression(const parser::ast::Expr& expr, DiagnosticBag& bag,
                         const Scope* scope,
                         const std::set<std::string>* aliases,
                         std::string_view sql) {
  visit(expr, bag, scope, aliases, sql);
}

void validate_predicate(const parser::ast::Expr& expr, DiagnosticBag& bag,
                        const std::string& context, const Scope* scope,
                        const std::set<std::string>* aliases,
                        std::string_view sql) {
  using namespace parser::ast;

  // Always run the recursive expression validator so function-arity errors
  // and CASE-shape errors inside the predicate are surfaced regardless of
  // the predicate-shape outcome.
  validate_expression(expr, bag, scope, aliases, sql);

  // Comparison: both sides cannot be literal constants.
  // Mirrors the throw at libs/compiler/src/where_compiler.cpp:50 and
  // libs/compiler/src/group_by_compiler.cpp:222.
  if (auto* cmp_ptr = std::get_if<std::unique_ptr<ComparisonExpr>>(&expr)) {
    const auto& cmp = **cmp_ptr;
    bool left_const = std::get_if<Constant>(&cmp.left) != nullptr ||
                      std::get_if<StringConstant>(&cmp.left) != nullptr;
    bool right_const = std::get_if<Constant>(&cmp.right) != nullptr ||
                       std::get_if<StringConstant>(&cmp.right) != nullptr;
    if (left_const && right_const) {
      bag.error("comparison of two constants is not supported in " + context,
                cmp.loc);
    }
    return;
  }

  // Logical (AND/OR): recurse into both sides as predicates.
  if (auto* log_ptr = std::get_if<std::unique_ptr<LogicalExpr>>(&expr)) {
    const auto& log = **log_ptr;
    validate_predicate(log.left, bag, context, scope, aliases, sql);
    validate_predicate(log.right, bag, context, scope, aliases, sql);
    return;
  }

  // NOT: must wrap a ComparisonExpr whose right side is a literal Constant
  // (the compiler implements NOT(a OP b) by inverting OP, which only works
  // when b is a constant the new comparison can compare against).
  // Mirrors the throws at libs/compiler/src/where_compiler.cpp:175 and 162.
  if (auto* not_ptr = std::get_if<std::unique_ptr<NotExpr>>(&expr)) {
    const auto& not_expr = **not_ptr;
    auto* inner_cmp =
        std::get_if<std::unique_ptr<ComparisonExpr>>(&not_expr.operand);
    if (!inner_cmp) {
      bag.error("NOT is only supported on comparison expressions", not_expr.loc);
      return;
    }
    if (!std::get_if<Constant>(&(*inner_cmp)->right)) {
      bag.error("NOT optimization requires constant on right side",
                (*inner_cmp)->loc);
    }
    return;
  }

  // BETWEEN: bounds must be literal constants and the BETWEEN'd expression
  // cannot itself be a literal constant.
  // Mirrors the throws at libs/compiler/src/where_compiler.cpp:186, 199.
  if (auto* btw_ptr = std::get_if<std::unique_ptr<BetweenExpr>>(&expr)) {
    const auto& btw = **btw_ptr;
    bool expr_const = std::get_if<Constant>(&btw.expr) != nullptr ||
                      std::get_if<StringConstant>(&btw.expr) != nullptr;
    if (expr_const) {
      bag.error("BETWEEN on constant expression not supported", loc_of(btw.expr));
    }
    bool low_const = std::get_if<Constant>(&btw.low) != nullptr;
    bool high_const = std::get_if<Constant>(&btw.high) != nullptr;
    if (!low_const || !high_const) {
      // Span just the bounds — the diagnostic is about them, not the
      // BETWEEN'd expression.
      parser::ast::SourceLocation bounds_loc;
      auto low_span = expr_span(btw.low, sql);
      auto high_span = expr_span(btw.high, sql);
      bounds_loc = low_span;
      if (high_span.line >= 1 && high_span.column >= 1) {
        if (bounds_loc.line < 1) {
          bounds_loc = high_span;
        } else {
          if (high_span.end_line > bounds_loc.end_line ||
              (high_span.end_line == bounds_loc.end_line &&
               high_span.end_column > bounds_loc.end_column)) {
            bounds_loc.end_line = high_span.end_line;
            bounds_loc.end_column = high_span.end_column;
          }
        }
      }
      if (bounds_loc.line < 1) bounds_loc = btw.loc;
      bag.error("BETWEEN bounds must be constants", bounds_loc);
    }
    return;
  }

  // Anything else (BinaryExpr, FuncCall, ColumnRef, Constant, etc.) is
  // not a valid top-level predicate. Mirrors the throw at
  // libs/compiler/src/where_compiler.cpp:220.
  bag.error("unsupported predicate expression type", loc_of(expr));
}

}  // namespace rtbot_sql::analyzer
