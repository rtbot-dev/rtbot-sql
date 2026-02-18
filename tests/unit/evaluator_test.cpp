#include "rtbot_sql/planner/evaluator.h"
#include "rtbot_sql/planner/planner.h"

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

// ---------------------------------------------------------------------------
// evaluate_cross_key_agg tests
// ---------------------------------------------------------------------------

// Rows: each row is [instrument_id, price, quantity]
// instrument_id values: 1, 2, 3
// price values: 100, 200, 150
// quantity values: 10, 20, 30

class CrossKeyAggTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rows = {
        {1.0, 100.0, 10.0},
        {2.0, 200.0, 20.0},
        {3.0, 150.0, 30.0},
    };
  }
  std::vector<std::vector<double>> rows;
};

TEST_F(CrossKeyAggTest, Count) {
  CrossKeyAgg agg;
  agg.func = "COUNT";
  agg.col_index = -1;
  agg.alias = "cnt";

  auto result = evaluate_cross_key_agg({agg}, rows);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 3.0);
}

TEST_F(CrossKeyAggTest, Sum) {
  CrossKeyAgg agg;
  agg.func = "SUM";
  agg.col_index = 1;  // price
  agg.alias = "total_price";

  auto result = evaluate_cross_key_agg({agg}, rows);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 450.0);  // 100 + 200 + 150
}

TEST_F(CrossKeyAggTest, Avg) {
  CrossKeyAgg agg;
  agg.func = "AVG";
  agg.col_index = 2;  // quantity
  agg.alias = "avg_qty";

  auto result = evaluate_cross_key_agg({agg}, rows);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 20.0);  // (10 + 20 + 30) / 3
}

TEST_F(CrossKeyAggTest, Min) {
  CrossKeyAgg agg;
  agg.func = "MIN";
  agg.col_index = 1;  // price
  agg.alias = "min_price";

  auto result = evaluate_cross_key_agg({agg}, rows);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 100.0);
}

TEST_F(CrossKeyAggTest, Max) {
  CrossKeyAgg agg;
  agg.func = "MAX";
  agg.col_index = 1;  // price
  agg.alias = "max_price";

  auto result = evaluate_cross_key_agg({agg}, rows);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 200.0);
}

TEST_F(CrossKeyAggTest, MultipleAggs) {
  CrossKeyAgg sum_agg;
  sum_agg.func = "SUM";
  sum_agg.col_index = 2;  // quantity
  sum_agg.alias = "total_qty";

  CrossKeyAgg cnt_agg;
  cnt_agg.func = "COUNT";
  cnt_agg.col_index = -1;
  cnt_agg.alias = "cnt";

  CrossKeyAgg avg_agg;
  avg_agg.func = "AVG";
  avg_agg.col_index = 1;  // price
  avg_agg.alias = "avg_price";

  auto result = evaluate_cross_key_agg({sum_agg, cnt_agg, avg_agg}, rows);
  ASSERT_EQ(result.size(), 3u);
  EXPECT_DOUBLE_EQ(result[0], 60.0);   // SUM(quantity)
  EXPECT_DOUBLE_EQ(result[1], 3.0);    // COUNT(*)
  EXPECT_DOUBLE_EQ(result[2], 150.0);  // AVG(price)
}

TEST_F(CrossKeyAggTest, EmptyRows) {
  CrossKeyAgg sum_agg;
  sum_agg.func = "SUM";
  sum_agg.col_index = 1;
  sum_agg.alias = "s";

  CrossKeyAgg cnt_agg;
  cnt_agg.func = "COUNT";
  cnt_agg.col_index = -1;
  cnt_agg.alias = "c";

  std::vector<std::vector<double>> empty_rows;
  auto result = evaluate_cross_key_agg({sum_agg, cnt_agg}, empty_rows);
  ASSERT_EQ(result.size(), 2u);
  EXPECT_DOUBLE_EQ(result[0], 0.0);  // SUM of empty = 0
  EXPECT_DOUBLE_EQ(result[1], 0.0);  // COUNT of empty = 0
}

}  // namespace
}  // namespace rtbot_sql::planner
