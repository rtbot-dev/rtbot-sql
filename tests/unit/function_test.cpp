#include "rtbot_sql/compiler/function_compiler.h"

#include <gtest/gtest.h>

#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {
namespace {

// For tests that exercise semantic errors caught by the analyzer rather
// than by direct calls to compile_function. Built once per test inline.
rtbot_sql::CatalogSnapshot trades_catalog() {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {
      {"instrument_id", 0, rtbot_sql::ColumnType::DOUBLE},
      {"price", 1, rtbot_sql::ColumnType::DOUBLE},
      {"quantity", 2, rtbot_sql::ColumnType::DOUBLE},
      {"account_id", 3, rtbot_sql::ColumnType::DOUBLE},
  };
  c.streams["trades"] = schema;
  return c;
}

using namespace parser::ast;

// AST helpers
Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr num(double v) { return Constant{v}; }

Expr func(const std::string& name, std::vector<Expr> args) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args = std::move(args);
  return f;
}

Expr binary(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
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

class FunctionTest : public ::testing::Test {
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

// SUM(quantity) → VectorExtract + CumulativeSum
TEST_F(FunctionTest, SumProducesCumulativeSum) {
  std::vector<Expr> args;
  args.push_back(col("quantity"));
  auto ep = compile_function("SUM", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& sum = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 2.0);
  EXPECT_EQ(sum.type, "CumulativeSum");
  EXPECT_EQ(ep.operator_id, sum.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", sum.id, "i1");
}

// COUNT(*) → VectorExtract(0) [clock] + CountNumber
TEST_F(FunctionTest, CountProducesCount) {
  std::vector<Expr> args;  // empty — COUNT(*)
  auto ep = compile_function("COUNT", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& clock = builder.operators()[0];
  auto& cnt = builder.operators()[1];
  EXPECT_EQ(clock.type, "VectorExtract");
  EXPECT_EQ(clock.params.at("index"), 0.0);
  EXPECT_EQ(cnt.type, "CountNumber");
  EXPECT_EQ(ep.operator_id, cnt.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", clock.id, "i1");
  expect_conn(builder, clock.id, "o1", cnt.id, "i1");
}

// AVG(price) → VectorExtract + CumulativeSum + Count + Division
TEST_F(FunctionTest, AvgProducesDiamondGraph) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  auto ep = compile_function("AVG", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 4u);
  auto& ext = builder.operators()[0];
  auto& sum = builder.operators()[1];
  auto& cnt = builder.operators()[2];
  auto& div = builder.operators()[3];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(sum.type, "CumulativeSum");
  EXPECT_EQ(cnt.type, "CountNumber");
  EXPECT_EQ(div.type, "Division");
  EXPECT_EQ(div.params.at("numPorts"), 2.0);
  EXPECT_EQ(ep.operator_id, div.id);

  ASSERT_EQ(builder.connections().size(), 5u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", sum.id, "i1");
  expect_conn(builder, ext.id, "o1", cnt.id, "i1");
  expect_conn(builder, sum.id, "o1", div.id, "i1");
  expect_conn(builder, cnt.id, "o1", div.id, "i2");
}

// MOVING_AVERAGE(price, 20) → VectorExtract + MovingAverage(window=20)
TEST_F(FunctionTest, MovingAverageProducesMovingAverage) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(20));
  auto ep = compile_function("MOVING_AVERAGE", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& ma = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(ma.type, "MovingAverage");
  EXPECT_EQ(ma.params.at("window_size"), 20.0);
  EXPECT_EQ(ep.operator_id, ma.id);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", ma.id, "i1");
}

// MOVING_COUNT(50) → VectorExtract(0) [clock] + ConstantNumber(1) + MovingSum(50)
TEST_F(FunctionTest, MovingCountProducesConstantAndMovingSum) {
  std::vector<Expr> args;
  args.push_back(num(50));
  auto ep = compile_function("MOVING_COUNT", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& clock = builder.operators()[0];
  auto& cnst = builder.operators()[1];
  auto& msum = builder.operators()[2];
  EXPECT_EQ(clock.type, "VectorExtract");
  EXPECT_EQ(clock.params.at("index"), 0.0);
  EXPECT_EQ(cnst.type, "ConstantNumber");
  EXPECT_EQ(cnst.params.at("value"), 1.0);
  EXPECT_EQ(msum.type, "MovingSum");
  EXPECT_EQ(msum.params.at("window_size"), 50.0);
  EXPECT_EQ(ep.operator_id, msum.id);

  ASSERT_EQ(builder.connections().size(), 3u);
  expect_conn(builder, "input_0", "o1", clock.id, "i1");
  expect_conn(builder, clock.id, "o1", cnst.id, "i1");
  expect_conn(builder, cnst.id, "o1", msum.id, "i1");
}

// MOVING_STD(price, 20) → VectorExtract + StandardDeviation(window=20)
TEST_F(FunctionTest, StddevProducesStandardDeviation) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(20));
  auto ep = compile_function("MOVING_STD", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& sd = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(sd.type, "StandardDeviation");
  EXPECT_EQ(sd.params.at("window_size"), 20.0);
  EXPECT_EQ(ep.operator_id, sd.id);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", sd.id, "i1");
}

// Unknown function → error (driven through the analyzer via compile_sql,
// since the user-facing path catches this before compile_function runs).
TEST(FunctionAnalyzerTest, UnknownFunctionRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT UNKNOWN_FUNC(price) FROM trades",
      catalog);
  EXPECT_TRUE(r.has_errors());
}

// Wrong arg count for SUM → error
TEST(FunctionAnalyzerTest, SumWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT SUM() FROM trades", catalog);
  EXPECT_TRUE(r.has_errors());
}

// Non-constant window size → error
TEST(FunctionAnalyzerTest, NonConstantWindowRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(price, quantity) FROM trades",
      catalog);
  EXPECT_TRUE(r.has_errors());
}

// Negative window size → error
TEST(FunctionAnalyzerTest, NegativeWindowRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(price, -5) FROM trades",
      catalog);
  EXPECT_TRUE(r.has_errors());
}

// Aggregate functions dispatch correctly from compile_expression
TEST_F(FunctionTest, ExpressionCompilerDispatchesSUM) {
  auto f = std::make_unique<FuncCall>();
  f->name = "SUM";
  f->args.push_back(col("quantity"));
  Expr expr = std::move(f);

  auto result = compile_expression(expr, input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  EXPECT_EQ(builder.operators()[0].type, "VectorExtract");
  EXPECT_EQ(builder.operators()[1].type, "CumulativeSum");
}

// MOVING_SUM(quantity, 10)
TEST_F(FunctionTest, MovingSumProducesMovingSum) {
  std::vector<Expr> args;
  args.push_back(col("quantity"));
  args.push_back(num(10));
  auto ep = compile_function("MOVING_SUM", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& ms = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ms.type, "MovingSum");
  EXPECT_EQ(ms.params.at("window_size"), 10.0);
  EXPECT_EQ(ep.operator_id, ms.id);
}

// MOVING_MIN(price, 10) → VectorExtract + WindowMinMax(min)
TEST_F(FunctionTest, MovingMinProducesWindowMinMax) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(10));
  auto ep = compile_function("MOVING_MIN", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& mn = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(mn.type, "WindowMinMax");
  EXPECT_EQ(mn.params.at("window_size"), 10.0);
  EXPECT_EQ(mn.string_params.at("mode"), "min");
  EXPECT_EQ(ep.operator_id, mn.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", mn.id, "i1");
}

// MOVING_MAX(price, 5) → VectorExtract + WindowMinMax(max)
TEST_F(FunctionTest, MovingMaxProducesWindowMinMax) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(5));
  auto ep = compile_function("MOVING_MAX", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& mx = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(mx.type, "WindowMinMax");
  EXPECT_EQ(mx.params.at("window_size"), 5.0);
  EXPECT_EQ(mx.string_params.at("mode"), "max");
  EXPECT_EQ(ep.operator_id, mx.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", mx.id, "i1");
}

// MOVING_MIN/MOVING_MAX wrong argument count → error
TEST(FunctionAnalyzerTest, MovingMinWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT MOVING_MIN() FROM trades",
      catalog);
  EXPECT_TRUE(r.has_errors());
}

TEST(FunctionAnalyzerTest, MovingMaxWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT MOVING_MAX() FROM trades",
      catalog);
  EXPECT_TRUE(r.has_errors());
}

// DIFF(quantity) → VectorExtract + Difference
TEST_F(FunctionTest, DiffProducesDifference) {
  std::vector<Expr> args;
  args.push_back(col("quantity"));
  auto ep = compile_function("DIFF", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& diff = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 2.0);
  EXPECT_EQ(diff.type, "Difference");
  EXPECT_EQ(ep.operator_id, diff.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", diff.id, "i1");
}

// DIFF wrong arg count → error
TEST(FunctionAnalyzerTest, DiffWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT DIFF() FROM trades", catalog);
  EXPECT_TRUE(r.has_errors());
}

// MIN(price) → VectorExtract + MinTracker
TEST_F(FunctionTest, MinProducesMinTracker) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  auto ep = compile_function("MIN", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& mn = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(mn.type, "MinTracker");
  EXPECT_EQ(ep.operator_id, mn.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", mn.id, "i1");
}

// MIN wrong arg count → error
TEST(FunctionAnalyzerTest, MinWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT MIN() FROM trades", catalog);
  EXPECT_TRUE(r.has_errors());
}

// MAX(price) → VectorExtract + MaxTracker
TEST_F(FunctionTest, MaxProducesMaxTracker) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  auto ep = compile_function("MAX", args, input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& mx = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(mx.type, "MaxTracker");
  EXPECT_EQ(ep.operator_id, mx.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", mx.id, "i1");
}

// MAX wrong arg count → error
TEST(FunctionAnalyzerTest, MaxWrongArgCountRejected) {
  auto catalog = trades_catalog();
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT MAX() FROM trades", catalog);
  EXPECT_TRUE(r.has_errors());
}

}  // namespace
}  // namespace rtbot_sql::compiler
