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

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return {std::move(expr), alias};
}

Expr func_expr(const std::string& name, std::vector<Expr> args) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args = std::move(args);
  return f;
}

class PlanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema trades{"trades",
                        {{"instrument_id", 0}, {"price", 1},
                         {"quantity", 2}, {"account_id", 3}}};
    catalog.register_stream("trades", trades);

    ViewMeta instrument_stats;
    instrument_stats.name = "instrument_stats";
    instrument_stats.entity_type = EntityType::MATERIALIZED_VIEW;
    instrument_stats.view_type = ViewType::KEYED;
    instrument_stats.field_map = {{"instrument_id", 0}, {"total_volume", 1}};
    instrument_stats.key_index = 0;
    catalog.register_view("instrument_stats", instrument_stats);

    ViewMeta bollinger;
    bollinger.name = "bollinger_view";
    bollinger.entity_type = EntityType::VIEW;
    bollinger.view_type = ViewType::SCALAR;
    catalog.register_view("bollinger_view", bollinger);
  }

  catalog::Catalog catalog;
};

// Tier 1: SELECT * FROM trades LIMIT 10
TEST_F(PlanTest, Tier1Plan) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;

  auto plan = plan_select(stmt, catalog);
  EXPECT_EQ(plan.tier, SelectTier::TIER1_READ);
  EXPECT_EQ(plan.read_stream, "trades");
  EXPECT_EQ(plan.limit, 10);
  EXPECT_FALSE(plan.key_filter.has_value());
  EXPECT_FALSE(plan.needs_compilation);
}

// Tier 1 with key filter: WHERE instrument_id = 1
TEST_F(PlanTest, Tier1KeyFilterPlan) {
  SelectStmt stmt;
  stmt.from_table = "instrument_stats";
  stmt.where_clause = cmp("=", col("instrument_id"), num(1));

  auto plan = plan_select(stmt, catalog);
  EXPECT_EQ(plan.tier, SelectTier::TIER1_READ);
  EXPECT_EQ(plan.read_stream, "instrument_stats");
  ASSERT_TRUE(plan.key_filter.has_value());
  EXPECT_DOUBLE_EQ(*plan.key_filter, 1.0);
}

// Tier 2: SELECT * FROM trades WHERE price > 100 LIMIT 10
TEST_F(PlanTest, Tier2Plan) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  stmt.where_clause = cmp(">", col("price"), num(100));

  auto plan = plan_select(stmt, catalog);
  EXPECT_EQ(plan.tier, SelectTier::TIER2_SCAN);
  EXPECT_EQ(plan.scan_stream, "trades");
  EXPECT_NE(plan.where_predicate, nullptr);
  EXPECT_EQ(plan.limit, 10);

  // Verify the predicate works
  std::vector<double> row_pass = {1.0, 150.0, 200.0, 42.0};
  std::vector<double> row_fail = {1.0, 50.0, 200.0, 42.0};
  EXPECT_TRUE(evaluate_where(*plan.where_predicate, row_pass));
  EXPECT_FALSE(evaluate_where(*plan.where_predicate, row_fail));
}

// Tier 2 with SELECT expressions
TEST_F(PlanTest, Tier2WithSelectExprs) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  stmt.select_list.push_back(
      item(bin("*", col("price"), col("quantity")), "value"));

  auto plan = plan_select(stmt, catalog);
  EXPECT_EQ(plan.tier, SelectTier::TIER2_SCAN);
  ASSERT_EQ(plan.select_exprs.size(), 1u);
  EXPECT_EQ(plan.field_map.at("value"), 0);

  std::vector<double> row = {1.0, 150.0, 200.0, 42.0};
  auto result = evaluate_select(plan.select_exprs, row);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_DOUBLE_EQ(result[0], 30000.0);
}

// Tier 3: SELECT * FROM bollinger_view
TEST_F(PlanTest, Tier3Plan) {
  SelectStmt stmt;
  stmt.from_table = "bollinger_view";

  auto plan = plan_select(stmt, catalog);
  EXPECT_EQ(plan.tier, SelectTier::TIER3_EPHEMERAL);
  EXPECT_TRUE(plan.needs_compilation);
}

}  // namespace
}  // namespace rtbot_sql::planner
