#pragma once

#include <cmath>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::planner {

// Compiled expression for efficient per-row evaluation (Tier 2).
class CompiledExpr {
 public:
  virtual ~CompiledExpr() = default;
  virtual double evaluate(const std::vector<double>& row) const = 0;
};

class ColumnAccess : public CompiledExpr {
  int index_;

 public:
  explicit ColumnAccess(int index) : index_(index) {}
  double evaluate(const std::vector<double>& row) const override {
    return row[index_];
  }
};

class ConstantExpr : public CompiledExpr {
  double value_;

 public:
  explicit ConstantExpr(double value) : value_(value) {}
  double evaluate(const std::vector<double>& /*row*/) const override {
    return value_;
  }
};

class BinaryOpExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> left_, right_;
  char op_;

 public:
  BinaryOpExpr(char op, std::unique_ptr<CompiledExpr> left,
               std::unique_ptr<CompiledExpr> right)
      : left_(std::move(left)), right_(std::move(right)), op_(op) {}

  double evaluate(const std::vector<double>& row) const override {
    double l = left_->evaluate(row);
    double r = right_->evaluate(row);
    switch (op_) {
      case '+': return l + r;
      case '-': return l - r;
      case '*': return l * r;
      case '/': return l / r;
      default: return 0.0;
    }
  }
};

class ComparisonEvalExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> left_, right_;
  std::string op_;

 public:
  ComparisonEvalExpr(std::string op, std::unique_ptr<CompiledExpr> left,
                     std::unique_ptr<CompiledExpr> right)
      : left_(std::move(left)), right_(std::move(right)), op_(std::move(op)) {}

  double evaluate(const std::vector<double>& row) const override {
    double l = left_->evaluate(row);
    double r = right_->evaluate(row);
    if (op_ == ">") return l > r ? 1.0 : 0.0;
    if (op_ == "<") return l < r ? 1.0 : 0.0;
    if (op_ == ">=") return l >= r ? 1.0 : 0.0;
    if (op_ == "<=") return l <= r ? 1.0 : 0.0;
    if (op_ == "=") return l == r ? 1.0 : 0.0;
    if (op_ == "!=") return l != r ? 1.0 : 0.0;
    return 0.0;
  }
};

class LogicalAndExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> left_, right_;

 public:
  LogicalAndExpr(std::unique_ptr<CompiledExpr> left,
                 std::unique_ptr<CompiledExpr> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  double evaluate(const std::vector<double>& row) const override {
    return (left_->evaluate(row) != 0.0 && right_->evaluate(row) != 0.0)
               ? 1.0
               : 0.0;
  }
};

class LogicalOrExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> left_, right_;

 public:
  LogicalOrExpr(std::unique_ptr<CompiledExpr> left,
                std::unique_ptr<CompiledExpr> right)
      : left_(std::move(left)), right_(std::move(right)) {}

  double evaluate(const std::vector<double>& row) const override {
    return (left_->evaluate(row) != 0.0 || right_->evaluate(row) != 0.0)
               ? 1.0
               : 0.0;
  }
};

class NotEvalExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> operand_;

 public:
  explicit NotEvalExpr(std::unique_ptr<CompiledExpr> operand)
      : operand_(std::move(operand)) {}

  double evaluate(const std::vector<double>& row) const override {
    return operand_->evaluate(row) == 0.0 ? 1.0 : 0.0;
  }
};

class UnaryFuncExpr : public CompiledExpr {
  std::unique_ptr<CompiledExpr> operand_;
  std::function<double(double)> func_;

 public:
  UnaryFuncExpr(std::function<double(double)> func,
                std::unique_ptr<CompiledExpr> operand)
      : operand_(std::move(operand)), func_(std::move(func)) {}

  double evaluate(const std::vector<double>& row) const override {
    return func_(operand_->evaluate(row));
  }
};

// Compile an AST expression into a CompiledExpr tree for fast row evaluation.
std::unique_ptr<CompiledExpr> compile_for_eval(
    const parser::ast::Expr& expr, const StreamSchema& schema);

// Evaluate a WHERE predicate on a row (true if non-zero).
bool evaluate_where(const CompiledExpr& predicate,
                    const std::vector<double>& row);

// Evaluate SELECT expressions on a row, producing an output row.
std::vector<double> evaluate_select(
    const std::vector<std::unique_ptr<CompiledExpr>>& select_exprs,
    const std::vector<double>& row);

}  // namespace rtbot_sql::planner
