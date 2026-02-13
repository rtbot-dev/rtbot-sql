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

}  // namespace
}  // namespace rtbot_sql::compiler
