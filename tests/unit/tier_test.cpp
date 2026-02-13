#include "rtbot_sql/planner/classifier.h"

#include <gtest/gtest.h>

namespace rtbot_sql::planner {
namespace {

using namespace parser::ast;

Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr num(double v) { return Constant{v}; }

Expr func_expr(const std::string& name, std::vector<Expr> args) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args = std::move(args);
  return f;
}

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

class TierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // STREAM: trades
    StreamSchema trades{"trades",
                        {{"instrument_id", 0}, {"price", 1},
                         {"quantity", 2}, {"account_id", 3}}};
    catalog.register_stream("trades", trades);

    // MATERIALIZED_VIEW (scalar): large_trades
    ViewMeta large_trades;
    large_trades.name = "large_trades";
    large_trades.entity_type = EntityType::MATERIALIZED_VIEW;
    large_trades.view_type = ViewType::SCALAR;
    large_trades.field_map = {{"price", 0}, {"quantity", 1}};
    catalog.register_view("large_trades", large_trades);

    // MATERIALIZED_VIEW (keyed): instrument_stats
    ViewMeta instrument_stats;
    instrument_stats.name = "instrument_stats";
    instrument_stats.entity_type = EntityType::MATERIALIZED_VIEW;
    instrument_stats.view_type = ViewType::KEYED;
    instrument_stats.field_map = {{"instrument_id", 0}, {"total_volume", 1}};
    instrument_stats.key_index = 0;
    catalog.register_view("instrument_stats", instrument_stats);

    // VIEW: bollinger_view
    ViewMeta bollinger;
    bollinger.name = "bollinger_view";
    bollinger.entity_type = EntityType::VIEW;
    bollinger.view_type = ViewType::SCALAR;
    catalog.register_view("bollinger_view", bollinger);

    // TABLE: orders
    TableSchema orders;
    orders.name = "orders";
    orders.columns = {{"id", 0}, {"price", 1}};
    orders.changelog_stream = "orders_changelog";
    catalog.register_table("orders", orders);
  }

  catalog::Catalog catalog;
};

// Test 1: SELECT * FROM trades LIMIT 10 → Tier 1
TEST_F(TierTest, StreamWithLimitIsTier1) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER1_READ);
}

// Test 2: SELECT * FROM large_trades → Tier 1 (scalar mat. view)
TEST_F(TierTest, ScalarMatViewIsTier1) {
  SelectStmt stmt;
  stmt.from_table = "large_trades";
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER1_READ);
}

// Test 3: SELECT * FROM instrument_stats WHERE instrument_id = 1 → Tier 1
TEST_F(TierTest, KeyedViewKeyLookupIsTier1) {
  SelectStmt stmt;
  stmt.from_table = "instrument_stats";
  stmt.where_clause = cmp("=", col("instrument_id"), num(1));
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER1_READ);
}

// Test 4: SELECT * FROM trades WHERE price > 100 LIMIT 10 → Tier 2
TEST_F(TierTest, StreamWithFilterIsTier2) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  stmt.where_clause = cmp(">", col("price"), num(100));
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER2_SCAN);
}

// Test 5: SELECT price * quantity AS value FROM trades LIMIT 10 → Tier 2
TEST_F(TierTest, StreamWithExpressionIsTier2) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  stmt.select_list.push_back(
      item(bin("*", col("price"), col("quantity")), "value"));
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER2_SCAN);
}

// Test 6: SELECT SUM(total_volume) FROM instrument_stats → Tier 2
TEST_F(TierTest, KeyedViewWithAggregateIsTier2) {
  SelectStmt stmt;
  stmt.from_table = "instrument_stats";
  std::vector<Expr> sum_args;
  sum_args.push_back(col("total_volume"));
  stmt.select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "total"));
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER2_SCAN);
}

// Test 7: SELECT instrument_id, SUM(quantity) FROM trades
//         GROUP BY instrument_id → Tier 3
TEST_F(TierTest, StreamWithGroupByIsTier3) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  stmt.limit = 10;
  stmt.select_list.push_back(item(col("instrument_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  stmt.select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "total"));
  stmt.group_by.push_back(col("instrument_id"));
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER3_EPHEMERAL);
}

// Test 8: SELECT * FROM bollinger_view → Tier 3 (VIEW)
TEST_F(TierTest, ViewIsTier3) {
  SelectStmt stmt;
  stmt.from_table = "bollinger_view";
  EXPECT_EQ(classify_select(stmt, catalog), SelectTier::TIER3_EPHEMERAL);
}

// Test 9: SELECT * FROM trades (no LIMIT) → Error
TEST_F(TierTest, UnboundedStreamThrows) {
  SelectStmt stmt;
  stmt.from_table = "trades";
  EXPECT_THROW(classify_select(stmt, catalog), std::runtime_error);
}

}  // namespace
}  // namespace rtbot_sql::planner
