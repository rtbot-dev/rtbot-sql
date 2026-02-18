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

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return {std::move(expr), alias};
}

class GroupByTest : public ::testing::Test {
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

// Test 1: SELECT instrument_id, SUM(quantity) FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, BasicGroupByWithSum) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "sum_quantity"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  // Outer graph: KeyedPipeline
  ASSERT_EQ(builder.operators().size(), 1u);
  auto& keyed = builder.operators()[0];
  EXPECT_EQ(keyed.type, "KeyedPipeline");
  EXPECT_EQ(keyed.params.at("key_index"), 0.0);
  EXPECT_EQ(keyed.string_params.at("prototype"), builder.prototypes()[0].id);

  // One prototype
  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];
  EXPECT_EQ(proto.entry_id, "proto_in");
  EXPECT_EQ(proto.output_id, "proto_out");

  // Prototype contains: Input, VectorExtract, CumulativeSum, Output
  // VectorCompose is always present (even for single non-key output,
  // because proto_out expects vector_number port type)
  bool has_input = false, has_ext = false, has_cumsum = false,
       has_output = false;
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "Input") has_input = true;
    if (op.type == "VectorExtract") {
      has_ext = true;
      EXPECT_EQ(op.params.at("index"), 2.0);  // quantity
    }
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "Output") has_output = true;
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 1.0);
    }
  }
  EXPECT_TRUE(has_input);
  EXPECT_TRUE(has_ext);
  EXPECT_TRUE(has_cumsum);
  EXPECT_TRUE(has_output);
  EXPECT_TRUE(has_compose);  // always compose, even for single output

  // Field map
  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("sum_quantity"), 1);
}

// Test 2: SELECT instrument_id, SUM(quantity), COUNT(*), AVG(price)
//         FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, MultiAggregateGroupBy) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));

  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "total_qty"));

  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  std::vector<Expr> avg_args;
  avg_args.push_back(col("price"));
  select_list.push_back(
      item(func_expr("AVG", std::move(avg_args)), "avg_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // VectorCompose with 3 ports (SUM, COUNT, AVG)
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 3.0);
    }
  }
  EXPECT_TRUE(has_compose);

  // Field map: 4 entries
  EXPECT_EQ(field_map.size(), 4u);
  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("total_qty"), 1);
  EXPECT_EQ(field_map.at("cnt"), 2);
  EXPECT_EQ(field_map.at("avg_price"), 3);
}

// Test 3: SELECT instrument_id, price, MOVING_AVERAGE(price, 20)
//         FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, NonAggregatedColumnPassthrough) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("price")));

  std::vector<Expr> ma_args;
  ma_args.push_back(col("price"));
  ma_args.push_back(num(20));
  select_list.push_back(
      item(func_expr("MOVING_AVERAGE", std::move(ma_args)), "ma_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // Should have MovingAverage
  bool has_ma = false;
  for (const auto& op : proto.operators) {
    if (op.type == "MovingAverage") has_ma = true;
  }
  EXPECT_TRUE(has_ma);

  // VectorCompose with 2 ports (price passthrough + MA)
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);
    }
  }
  EXPECT_TRUE(has_compose);

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("price"), 1);
  EXPECT_EQ(field_map.at("ma_price"), 2);
}

// ---------------------------------------------------------------------------
// Test 4: Composite GROUP BY (2 keys) produces Linear hash + KeyedPipeline
// SELECT instrument_id, exchange_id, SUM(quantity) AS total
// FROM trades GROUP BY instrument_id, exchange_id
// ---------------------------------------------------------------------------

TEST_F(GroupByTest, CompositeGroupByTwoKeys) {
  // Use schema with 4 columns: instrument_id(0), price(1), quantity(2), account_id(3)
  // Re-purpose account_id as exchange_id for this test.
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("account_id")));  // treated as exchange_id key
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));
  group_by.push_back(col("account_id"));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder, 4 /*num_input_cols*/);

  // Outer graph must have: VectorExtract×4, Linear (hash), VectorCompose
  // (augment), KeyedPipeline.
  bool has_linear = false;
  bool has_keyed = false;
  bool has_augment_compose = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Linear") has_linear = true;
    if (op.type == "KeyedPipeline") has_keyed = true;
    if (op.type == "VectorCompose") has_augment_compose = true;
  }
  EXPECT_TRUE(has_linear) << "Expected Linear (hash) operator in outer graph";
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";
  EXPECT_TRUE(has_augment_compose) << "Expected VectorCompose in outer graph";

  // One prototype wrapping the per-key aggregation.
  ASSERT_EQ(builder.prototypes().size(), 1u);

  // Field map: both key columns + aggregate (KeyedPipeline prepends hash key
  // at index 0, so prototype outputs start at 1).
  EXPECT_EQ(field_map.at("instrument_id"), 1);
  EXPECT_EQ(field_map.at("account_id"), 2);
  EXPECT_EQ(field_map.at("total"), 3);
}

// ---------------------------------------------------------------------------
// Test 5: MOVING_MIN(price, 10) and MOVING_MAX(price, 5) inside GROUP BY
// ---------------------------------------------------------------------------

TEST_F(GroupByTest, MovingMinMaxInsideGroupBy) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));

  std::vector<Expr> min_args;
  min_args.push_back(col("price"));
  min_args.push_back(num(10));
  select_list.push_back(
      item(func_expr("MOVING_MIN", std::move(min_args)), "min_price"));

  std::vector<Expr> max_args;
  max_args.push_back(col("price"));
  max_args.push_back(num(5));
  select_list.push_back(
      item(func_expr("MOVING_MAX", std::move(max_args)), "max_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  bool has_wmin = false;
  bool has_wmax = false;
  for (const auto& op : proto.operators) {
    if (op.type == "WindowMinMax") {
      if (op.string_params.count("mode") &&
          op.string_params.at("mode") == "min") {
        has_wmin = true;
        EXPECT_EQ(op.params.at("window_size"), 10.0);
      }
      if (op.string_params.count("mode") &&
          op.string_params.at("mode") == "max") {
        has_wmax = true;
        EXPECT_EQ(op.params.at("window_size"), 5.0);
      }
    }
  }
  EXPECT_TRUE(has_wmin) << "Expected WindowMinMax(min) in prototype";
  EXPECT_TRUE(has_wmax) << "Expected WindowMinMax(max) in prototype";

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("min_price"), 1);
  EXPECT_EQ(field_map.at("max_price"), 2);
}

}  // namespace
}  // namespace rtbot_sql::compiler
