#include "rtbot_sql/compiler/select_compiler.h"

#include <gtest/gtest.h>

#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {
namespace {

using namespace parser::ast;

// AST helpers
Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr num(double v) { return Constant{v}; }

Expr binary(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
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

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return {std::move(expr), alias};
}

void expect_conn(const GraphBuilder& b, const std::string& from_id,
                 const std::string& from_port, const std::string& to_id,
                 const std::string& to_port) {
  for (const auto& c : b.connections()) {
    if (c.from_id == from_id && c.from_port == from_port &&
        c.to_id == to_id && c.to_port == to_port) {
      return;
    }
  }
  FAIL() << "Expected connection: " << from_id << ":" << from_port << " -> "
         << to_id << ":" << to_port;
}

class SelectTest : public ::testing::Test {
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

// SELECT * → identity passthrough
TEST_F(SelectTest, SelectStarPassthrough) {
  std::vector<SelectItem> select_list;  // empty = SELECT *
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  // Identity: no operators added
  EXPECT_TRUE(builder.operators().empty());
  EXPECT_TRUE(builder.connections().empty());
  // Endpoint is the same as input
  EXPECT_EQ(ep.operator_id, "input_0");
  EXPECT_EQ(ep.port, "o1");
  // Field map empty (caller fills from schema)
  EXPECT_TRUE(field_map.empty());
}

// SELECT instrument_id, price → VectorProject([0, 1])
TEST_F(SelectTest, PureColumnRefsUseVectorProject) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("price")));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 1u);
  auto& proj = builder.operators()[0];
  EXPECT_EQ(proj.type, "VectorProject");
  EXPECT_EQ(proj.params.at("numIndices"), 2.0);
  EXPECT_EQ(proj.params.at("index_0"), 0.0);  // instrument_id
  EXPECT_EQ(proj.params.at("index_1"), 1.0);  // price
  EXPECT_EQ(ep.operator_id, proj.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 1u);
  expect_conn(builder, "input_0", "o1", proj.id, "i1");

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("price"), 1);
}

// SELECT instrument_id, price * quantity AS trade_value
// → VectorExtract(0) + expression chain + VectorCompose(2)
TEST_F(SelectTest, MixedColumnsAndExpressionsUseVectorCompose) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(
      item(binary("*", col("price"), col("quantity")), "trade_value"));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  // Should have: VectorExtract(0), VectorExtract(1), VectorExtract(2),
  //              Multiplication, VectorCompose
  // instrument_id → VectorExtract(0)
  // price → VectorExtract(1), quantity → VectorExtract(2) → Multiplication
  // Both → VectorCompose

  // Find the VectorCompose
  const OperatorDef* compose = nullptr;
  for (const auto& op : builder.operators()) {
    if (op.type == "VectorCompose") {
      compose = &op;
      break;
    }
  }
  ASSERT_NE(compose, nullptr);
  EXPECT_EQ(compose->params.at("numPorts"), 2.0);
  EXPECT_EQ(ep.operator_id, compose->id);
  EXPECT_EQ(ep.port, "o1");

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("trade_value"), 1);
}

// SELECT SUM(quantity) AS total_qty, COUNT(*) AS cnt
// → CumulativeSum + Count + VectorCompose(2)
TEST_F(SelectTest, AggregateFunctionsWithVectorCompose) {
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  std::vector<Expr> count_args;  // empty

  std::vector<SelectItem> select_list;
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total_qty"));
  select_list.push_back(item(func_expr("COUNT", std::move(count_args)), "cnt"));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  // Should have: VectorExtract, CumulativeSum, Count, VectorCompose
  bool has_cumsum = false, has_count = false, has_compose = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "Count") has_count = true;
    if (op.type == "VectorCompose") has_compose = true;
  }
  EXPECT_TRUE(has_cumsum);
  EXPECT_TRUE(has_count);
  EXPECT_TRUE(has_compose);

  EXPECT_EQ(field_map.at("total_qty"), 0);
  EXPECT_EQ(field_map.at("cnt"), 1);
}

// SELECT with alias overrides column name
TEST_F(SelectTest, AliasOverridesColumnName) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("price"), "p"));
  select_list.push_back(item(col("quantity"), "q"));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  EXPECT_EQ(field_map.at("p"), 0);
  EXPECT_EQ(field_map.at("q"), 1);
  EXPECT_EQ(field_map.count("price"), 0u);
  EXPECT_EQ(field_map.count("quantity"), 0u);
}

// SELECT with auto-generated alias for function
TEST_F(SelectTest, AutoGeneratedAliasForFunction) {
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));

  std::vector<SelectItem> select_list;
  select_list.push_back(item(func_expr("SUM", std::move(sum_args))));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  // Auto-generated: "sum_quantity"
  EXPECT_EQ(field_map.count("sum_quantity"), 1u);
  EXPECT_EQ(field_map.at("sum_quantity"), 0);
}

// Complex: SELECT price, MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band
TEST_F(SelectTest, ComplexExpressionMixingFunctionsAndArithmetic) {
  // Build: MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20)
  std::vector<Expr> ma_args;
  ma_args.push_back(col("price"));
  ma_args.push_back(num(20));
  Expr ma_expr = func_expr("MOVING_AVERAGE", std::move(ma_args));

  std::vector<Expr> sd_args;
  sd_args.push_back(col("price"));
  sd_args.push_back(num(20));
  Expr sd_expr = func_expr("MOVING_STD", std::move(sd_args));

  // 2 * MOVING_STD(...)
  Expr two_sd = binary("*", num(2), std::move(sd_expr));
  // MOVING_AVERAGE(...) + 2 * MOVING_STD(...)
  Expr upper = binary("+", std::move(ma_expr), std::move(two_sd));

  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("price")));
  select_list.push_back(item(std::move(upper), "upper_band"));
  auto [ep, field_map] =
      compile_select_projection(select_list, input, scope, builder);

  // Verify the graph has the expected operator types
  bool has_ma = false, has_sd = false, has_compose = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "MovingAverage") has_ma = true;
    if (op.type == "StandardDeviation") has_sd = true;
    if (op.type == "VectorCompose") has_compose = true;
  }
  EXPECT_TRUE(has_ma);
  EXPECT_TRUE(has_sd);
  EXPECT_TRUE(has_compose);

  EXPECT_EQ(field_map.at("price"), 0);
  EXPECT_EQ(field_map.at("upper_band"), 1);
}

}  // namespace
}  // namespace rtbot_sql::compiler
