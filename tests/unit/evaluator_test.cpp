#include "rtbot_sql/planner/evaluator.h"

#include <gtest/gtest.h>

namespace rtbot_sql::planner {
namespace {

using namespace parser::ast;

Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr num(double v) { return Constant{v}; }

Expr cmp(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<ComparisonExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr bin(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr logical(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<LogicalExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr func_expr(const std::string& name, std::vector<Expr> args) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args = std::move(args);
  return f;
}

class EvaluatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema = {"trades",
              {{"instrument_id", 0}, {"price", 1},
               {"quantity", 2}, {"account_id", 3}}};
    // Row: [1.0, 150.0, 200.0, 42.0]
    row = {1.0, 150.0, 200.0, 42.0};
  }

  StreamSchema schema;
  std::vector<double> row;
};

// price (index 1) → 150.0
TEST_F(EvaluatorTest, ColumnAccess) {
  auto expr = compile_for_eval(col("price"), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 150.0);
}

// price * quantity → 30000.0
TEST_F(EvaluatorTest, BinaryMultiply) {
  auto expr = compile_for_eval(bin("*", col("price"), col("quantity")), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 30000.0);
}

// price > 100 → true (1.0)
TEST_F(EvaluatorTest, ComparisonTrue) {
  auto expr = compile_for_eval(cmp(">", col("price"), num(100)), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 1.0);
}

// price > 200 → false (0.0)
TEST_F(EvaluatorTest, ComparisonFalse) {
  auto expr = compile_for_eval(cmp(">", col("price"), num(200)), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 0.0);
}

// price > 100 AND quantity < 500 → true
TEST_F(EvaluatorTest, LogicalAnd) {
  auto expr = compile_for_eval(
      logical("AND", cmp(">", col("price"), num(100)),
              cmp("<", col("quantity"), num(500))),
      schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 1.0);
}

// ABS(price - 160) → 10.0
TEST_F(EvaluatorTest, AbsFunction) {
  std::vector<Expr> abs_args;
  abs_args.push_back(bin("-", col("price"), num(160)));
  auto expr = compile_for_eval(func_expr("ABS", std::move(abs_args)), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 10.0);
}

// price * quantity > 25000 → true
TEST_F(EvaluatorTest, CompoundExpression) {
  auto expr = compile_for_eval(
      cmp(">", bin("*", col("price"), col("quantity")), num(25000)), schema);
  EXPECT_DOUBLE_EQ(expr->evaluate(row), 1.0);
}

// evaluate_where helper
TEST_F(EvaluatorTest, EvaluateWhereTrue) {
  auto pred = compile_for_eval(cmp(">", col("price"), num(100)), schema);
  EXPECT_TRUE(evaluate_where(*pred, row));
}

TEST_F(EvaluatorTest, EvaluateWhereFalse) {
  auto pred = compile_for_eval(cmp(">", col("price"), num(200)), schema);
  EXPECT_FALSE(evaluate_where(*pred, row));
}

// evaluate_select helper
TEST_F(EvaluatorTest, EvaluateSelect) {
  std::vector<std::unique_ptr<CompiledExpr>> exprs;
  exprs.push_back(compile_for_eval(col("price"), schema));
  exprs.push_back(
      compile_for_eval(bin("*", col("price"), col("quantity")), schema));

  auto result = evaluate_select(exprs, row);
  ASSERT_EQ(result.size(), 2u);
  EXPECT_DOUBLE_EQ(result[0], 150.0);
  EXPECT_DOUBLE_EQ(result[1], 30000.0);
}

}  // namespace
}  // namespace rtbot_sql::planner
