#include "rtbot_sql/compiler/function_compiler.h"

#include <gtest/gtest.h>

#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {
namespace {

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

// Unknown function → error
TEST_F(FunctionTest, UnknownFunctionThrows) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  EXPECT_THROW(compile_function("UNKNOWN_FUNC", args, input, scope, builder),
               std::runtime_error);
}

// Wrong arg count for SUM → error
TEST_F(FunctionTest, SumWrongArgCountThrows) {
  std::vector<Expr> args;  // empty
  EXPECT_THROW(compile_function("SUM", args, input, scope, builder),
               std::runtime_error);
}

// Non-constant window size → error
TEST_F(FunctionTest, NonConstantWindowThrows) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(col("quantity"));  // not a constant
  EXPECT_THROW(
      compile_function("MOVING_AVERAGE", args, input, scope, builder),
      std::runtime_error);
}

// Negative window size → error
TEST_F(FunctionTest, NegativeWindowThrows) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(-5));
  EXPECT_THROW(
      compile_function("MOVING_AVERAGE", args, input, scope, builder),
      std::runtime_error);
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

}  // namespace
}  // namespace rtbot_sql::compiler
