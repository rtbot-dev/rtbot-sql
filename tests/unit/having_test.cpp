#include "rtbot_sql/compiler/group_by_compiler.h"

#include <gtest/gtest.h>

namespace rtbot_sql::compiler {
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

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return {std::move(expr), alias};
}

class HavingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "trades",
        {{"instrument_id", 0}, {"price", 1}, {"quantity", 2},
         {"account_id", 3}},
    };
    scope.register_stream("trades", schema);
  }

  analyzer::Scope scope;
  GraphBuilder builder;
  Endpoint input{"input_0", "o1"};
};

// Test 4: SELECT instrument_id, COUNT(*) AS cnt
//         FROM trades GROUP BY instrument_id HAVING COUNT(*) > 5
TEST_F(HavingTest, HavingWithSharedCount) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  // HAVING COUNT(*) > 5
  Expr having = cmp(">", func_expr("COUNT", {}), num(5));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::move(having), input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // Count operator should appear exactly ONCE (shared)
  int count_ops = 0;
  for (const auto& op : proto.operators) {
    if (op.type == "CountNumber") count_ops++;
  }
  EXPECT_EQ(count_ops, 1);

  // CompareGT(5) and Demultiplexer present
  bool has_cmp = false, has_demux = false;
  for (const auto& op : proto.operators) {
    if (op.type == "CompareGT") {
      has_cmp = true;
      EXPECT_EQ(op.params.at("value"), 5.0);
    }
    if (op.type == "Demultiplexer") has_demux = true;
  }
  EXPECT_TRUE(has_cmp);
  EXPECT_TRUE(has_demux);

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("cnt"), 1);
}

// Test 5: SELECT instrument_id, SUM(quantity) AS total
//         FROM trades GROUP BY instrument_id HAVING SUM(quantity) > 1000
TEST_F(HavingTest, HavingWithSharedSum) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  std::vector<Expr> sum_args1;
  sum_args1.push_back(col("quantity"));
  select_list.push_back(
      item(func_expr("SUM", std::move(sum_args1)), "total"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  // HAVING SUM(quantity) > 1000
  std::vector<Expr> sum_args2;
  sum_args2.push_back(col("quantity"));
  Expr having = cmp(">", func_expr("SUM", std::move(sum_args2)), num(1000));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::move(having), input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // CumulativeSum should appear exactly ONCE (shared)
  int cumsum_count = 0;
  for (const auto& op : proto.operators) {
    if (op.type == "CumulativeSum") cumsum_count++;
  }
  EXPECT_EQ(cumsum_count, 1);

  // CompareGT(1000) and Demultiplexer
  bool has_cmp = false, has_demux = false;
  for (const auto& op : proto.operators) {
    if (op.type == "CompareGT") {
      has_cmp = true;
      EXPECT_EQ(op.params.at("value"), 1000.0);
    }
    if (op.type == "Demultiplexer") has_demux = true;
  }
  EXPECT_TRUE(has_cmp);
  EXPECT_TRUE(has_demux);
}

// Test 6: SELECT instrument_id, SUM(quantity) AS total
//         FROM trades GROUP BY instrument_id
//         HAVING MOVING_COUNT(5) > 3
// → outer pre-filter: VectorExtract(key) → MovingKeyCount(5) → CompareGT(3) → Demux
//   then KeyedPipeline (no HAVING inside prototype)
TEST_F(HavingTest, VelocityPatternBuildsOuterPrefilter) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  // HAVING MOVING_COUNT(5) > 3
  std::vector<Expr> mc_args;
  mc_args.push_back(num(5));
  Expr having = cmp(">", func_expr("MOVING_COUNT", std::move(mc_args)), num(3));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::move(having), input, scope,
                       builder);

  // No Demultiplexer inside the prototype (velocity HAVING goes to outer graph)
  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];
  for (const auto& op : proto.operators) {
    EXPECT_NE(op.type, "Demultiplexer") << "Demux should not be inside prototype";
  }

  // Outer graph must contain: VectorExtract, MovingKeyCount, CompareGT, Demultiplexer
  bool has_extract = false, has_mkc = false, has_cmp = false, has_demux = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "VectorExtract") has_extract = true;
    if (op.type == "MovingKeyCount") {
      has_mkc = true;
      EXPECT_EQ(op.params.at("window_size"), 5.0);
    }
    if (op.type == "CompareGT") {
      has_cmp = true;
      EXPECT_EQ(op.params.at("value"), 3.0);
    }
    if (op.type == "Demultiplexer") has_demux = true;
  }
  EXPECT_TRUE(has_extract);
  EXPECT_TRUE(has_mkc);
  EXPECT_TRUE(has_cmp);
  EXPECT_TRUE(has_demux);

  // KeyedPipeline receives filtered stream (from Demux, not raw input)
  bool kp_fed_by_demux = false;
  for (const auto& c : builder.connections()) {
    std::string demux_id;
    for (const auto& op : builder.operators()) {
      if (op.type == "Demultiplexer") demux_id = op.id;
    }
    if (c.from_id == demux_id && c.from_port == "o1") {
      kp_fed_by_demux = true;
    }
  }
  EXPECT_TRUE(kp_fed_by_demux);

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("total"), 1);
}

// Test: HAVING with two stream expressions (Bollinger-band pattern)
// e.g. HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
//
// Both sides of the comparison compile to stream endpoints.  The compiler
// must emit a CompareSyncGT operator (not throw) and connect both endpoints
// to it so that the comparison is evaluated timestamp-by-timestamp inside the
// KeyedPipeline prototype.
TEST_F(HavingTest, HavingTwoStreamExpressionsBollingerStyle) {
  // SELECT instrument_id, price,
  //        MOVING_AVERAGE(price, 20) AS mid,
  //        MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper
  // FROM trades GROUP BY instrument_id
  // HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)

  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("price")));

  std::vector<Expr> ma_args;
  ma_args.push_back(col("price"));
  ma_args.push_back(num(20));
  select_list.push_back(
      item(func_expr("MOVING_AVERAGE", std::move(ma_args)), "mid"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  // Build: HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
  // i.e. price > (ma + (2 * std))
  std::vector<Expr> ma_args2;
  ma_args2.push_back(col("price"));
  ma_args2.push_back(num(20));
  std::vector<Expr> std_args;
  std_args.push_back(col("price"));
  std_args.push_back(num(20));

  // 2 * STDDEV(price, 20)
  auto two_std_bin = std::make_unique<parser::ast::BinaryExpr>();
  two_std_bin->op = "*";
  two_std_bin->left = num(2);
  two_std_bin->right = func_expr("STDDEV", std::move(std_args));

  // MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
  auto upper_bin = std::make_unique<parser::ast::BinaryExpr>();
  upper_bin->op = "+";
  upper_bin->left = func_expr("MOVING_AVERAGE", std::move(ma_args2));
  upper_bin->right = std::move(two_std_bin);

  Expr having = cmp(">", col("price"), std::move(upper_bin));

  GraphBuilder fresh_builder;
  compile_group_by(select_list, group_by, std::move(having), input, scope,
                   fresh_builder);

  // CompareSyncGT lives inside the KeyedPipeline prototype, not the outer graph.
  ASSERT_EQ(fresh_builder.prototypes().size(), 1u);
  bool has_cmp_sync = false;
  for (const auto& op : fresh_builder.prototypes()[0].operators) {
    if (op.type == "CompareSyncGT") has_cmp_sync = true;
  }
  EXPECT_TRUE(has_cmp_sync)
      << "Expected CompareSyncGT operator for stream-vs-stream HAVING";
}

// Test: HAVING col - MA(col, n) > constant  (deviation from baseline)
// This pattern already worked before the fix (LHS folds into one endpoint),
// but verify it still does.
TEST_F(HavingTest, HavingDeviationFromBaseline) {
  // SELECT instrument_id, price
  // FROM trades GROUP BY instrument_id
  // HAVING price - MOVING_AVERAGE(price, 10) > 5.0

  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("price")));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  std::vector<Expr> ma_args;
  ma_args.push_back(col("price"));
  ma_args.push_back(num(10));

  auto diff_bin = std::make_unique<parser::ast::BinaryExpr>();
  diff_bin->op = "-";
  diff_bin->left = col("price");
  diff_bin->right = func_expr("MOVING_AVERAGE", std::move(ma_args));

  Expr having = cmp(">", std::move(diff_bin), num(5.0));

  GraphBuilder fresh_builder;
  EXPECT_NO_THROW({
    compile_group_by(select_list, group_by, std::move(having), input, scope,
                     fresh_builder);
  });

  // CompareGT lives inside the KeyedPipeline prototype, not the outer graph.
  ASSERT_EQ(fresh_builder.prototypes().size(), 1u);
  bool has_compare = false;
  for (const auto& op : fresh_builder.prototypes()[0].operators) {
    if (op.type == "CompareGT") has_compare = true;
  }
  EXPECT_TRUE(has_compare) << "Expected CompareGT for deviation > constant";
}

}  // namespace
}  // namespace rtbot_sql::compiler
