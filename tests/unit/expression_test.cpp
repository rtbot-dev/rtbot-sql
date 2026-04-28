#include "rtbot_sql/compiler/expression_compiler.h"

#include <gtest/gtest.h>

#include <limits>

#include "rtbot_sql/api/compiler.h"

namespace rtbot_sql::compiler {
namespace {

using namespace parser::ast;

// Helpers to build AST nodes
Expr col(const std::string& name) { return ColumnRef{"", name}; }

Expr col(const std::string& alias, const std::string& name) {
  return ColumnRef{alias, name};
}

Expr num(double v) { return Constant{v}; }

Expr binary(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr func(const std::string& name, Expr arg) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args.push_back(std::move(arg));
  return f;
}

Expr func2(const std::string& name, Expr arg1, Expr arg2) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args.push_back(std::move(arg1));
  f->args.push_back(std::move(arg2));
  return f;
}

Expr cmp(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<ComparisonExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

// Build: CASE WHEN cond THEN then_expr ELSE else_expr END
Expr case_when(Expr cond, Expr then_expr, Expr else_expr) {
  auto e = std::make_unique<CaseExpr>();
  CaseWhenClause clause;
  clause.condition = std::move(cond);
  clause.result = std::move(then_expr);
  e->when_clauses.push_back(std::move(clause));
  e->else_result = std::move(else_expr);
  return e;
}

// Helper to assert a specific connection exists
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

class ExpressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "trades",
        {{"instrument_id", 0}, {"price", 1}, {"quantity", 2},
         {"account_id", 3}},
    };
    scope.register_stream("trades", schema, "t");
  }

  analyzer::Scope scope;
  GraphBuilder builder;
  Endpoint input{"input_0", "o1"};
};

// price → VectorExtract(index=1)
TEST_F(ExpressionTest, ColumnRefProducesVectorExtract) {
  auto result = compile_expression(col("price"), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  ASSERT_EQ(builder.operators().size(), 1u);
  auto& ext = builder.operators()[0];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(ep.operator_id, ext.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 1u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
}

// 42.0 → ConstantMarker
TEST_F(ExpressionTest, ConstantProducesMarker) {
  auto result = compile_expression(num(42.0), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<ConstantMarker>(result));
  EXPECT_EQ(std::get<ConstantMarker>(result).value, 42.0);
  EXPECT_TRUE(builder.operators().empty());
  EXPECT_TRUE(builder.connections().empty());
}

// price * 0.9 → VectorExtract(1) → Scale(0.9)
TEST_F(ExpressionTest, ColumnTimesConstantProducesScale) {
  auto result = compile_expression(binary("*", col("price"), num(0.9)), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& scale = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(scale.type, "Scale");
  EXPECT_EQ(scale.params.at("value"), 0.9);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", scale.id, "i1");
}

// price + 10 → VectorExtract(1) → Add(10)
TEST_F(ExpressionTest, ColumnPlusConstantProducesAdd) {
  auto result = compile_expression(binary("+", col("price"), num(10.0)), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& add = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(add.type, "Add");
  EXPECT_EQ(add.params.at("value"), 10.0);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", add.id, "i1");
}

// price - 100 → VectorExtract(1) → Add(-100)
TEST_F(ExpressionTest, ColumnMinusConstantProducesAddNegative) {
  auto result = compile_expression(binary("-", col("price"), num(100.0)), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& add = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(add.type, "Add");
  EXPECT_EQ(add.params.at("value"), -100.0);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", add.id, "i1");
}

// price / 2 → VectorExtract(1) → Scale(0.5)
TEST_F(ExpressionTest, ColumnDivConstantProducesScale) {
  auto result = compile_expression(binary("/", col("price"), num(2.0)), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& scale = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(scale.type, "Scale");
  EXPECT_DOUBLE_EQ(scale.params.at("value"), 0.5);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", scale.id, "i1");
}

// 100 - price → VectorExtract(1) → Scale(-1) → Add(100)
TEST_F(ExpressionTest, ConstantMinusColumnProducesScaleAndAdd) {
  auto result = compile_expression(binary("-", num(100.0), col("price")), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& scale = builder.operators()[1];
  auto& add = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(scale.type, "Scale");
  EXPECT_EQ(scale.params.at("value"), -1.0);
  EXPECT_EQ(add.type, "Add");
  EXPECT_EQ(add.params.at("value"), 100.0);

  ASSERT_EQ(builder.connections().size(), 3u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", scale.id, "i1");
  expect_conn(builder, scale.id, "o1", add.id, "i1");
}

// price * quantity → VectorExtract(1) + VectorExtract(2) → Multiplication
TEST_F(ExpressionTest, TwoColumnsProducesSyncMultiplication) {
  auto result = compile_expression(binary("*", col("price"), col("quantity")),
                                   input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext_price = builder.operators()[0];
  auto& ext_qty = builder.operators()[1];
  auto& mult = builder.operators()[2];
  EXPECT_EQ(ext_price.type, "VectorExtract");
  EXPECT_EQ(ext_price.params.at("index"), 1.0);
  EXPECT_EQ(ext_qty.type, "VectorExtract");
  EXPECT_EQ(ext_qty.params.at("index"), 2.0);
  EXPECT_EQ(mult.type, "Multiplication");
  EXPECT_EQ(mult.params.at("numPorts"), 2.0);

  ASSERT_EQ(builder.connections().size(), 4u);
  expect_conn(builder, "input_0", "o1", ext_price.id, "i1");
  expect_conn(builder, "input_0", "o1", ext_qty.id, "i1");
  expect_conn(builder, ext_price.id, "o1", mult.id, "i1");
  expect_conn(builder, ext_qty.id, "o1", mult.id, "i2");
}

// ABS(price - 100) → VectorExtract(1) → Add(-100) → Abs
TEST_F(ExpressionTest, AbsOfExpressionProducesAbsOp) {
  auto result = compile_expression(
      func("ABS", binary("-", col("price"), num(100.0))), input, scope,
      builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& add = builder.operators()[1];
  auto& abs = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(add.type, "Add");
  EXPECT_EQ(add.params.at("value"), -100.0);
  EXPECT_EQ(abs.type, "Abs");

  ASSERT_EQ(builder.connections().size(), 3u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", add.id, "i1");
  expect_conn(builder, add.id, "o1", abs.id, "i1");
}

// POWER(price, 2) → VectorExtract(1) → Power(2)
TEST_F(ExpressionTest, PowerProducesPowerOp) {
  auto result = compile_expression(func2("POWER", col("price"), num(2.0)),
                                   input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& power = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(power.type, "Power");
  EXPECT_EQ(power.params.at("value"), 2.0);

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", power.id, "i1");
}

// Constant folding: 2 + 3 → ConstantMarker{5}
TEST_F(ExpressionTest, ConstantFolding) {
  auto result =
      compile_expression(binary("+", num(2.0), num(3.0)), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<ConstantMarker>(result));
  EXPECT_EQ(std::get<ConstantMarker>(result).value, 5.0);
  EXPECT_TRUE(builder.operators().empty());
  EXPECT_TRUE(builder.connections().empty());
}

// ABS(-5) constant folds to ConstantMarker{5}
TEST_F(ExpressionTest, MathFunctionConstantFolding) {
  auto result =
      compile_expression(func("ABS", num(-5.0)), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<ConstantMarker>(result));
  EXPECT_EQ(std::get<ConstantMarker>(result).value, 5.0);
  EXPECT_TRUE(builder.operators().empty());
  EXPECT_TRUE(builder.connections().empty());
}

// Unknown column rejected by the analyzer (driven through compile_sql).
TEST(ExpressionAnalyzerTest, UnknownColumnRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {{"price", 0, rtbot_sql::ColumnType::DOUBLE}};
  c.streams["trades"] = schema;
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT nonexistent FROM trades", c);
  EXPECT_TRUE(r.has_errors());
}

// Qualified column reference: t.price
TEST_F(ExpressionTest, QualifiedColumnRef) {
  auto result =
      compile_expression(col("t", "price"), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 1u);
  auto& ext = builder.operators()[0];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);

  ASSERT_EQ(builder.connections().size(), 1u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
}

// 100 / price → ConstantNumber(100) + VectorExtract → Division(sync)
TEST_F(ExpressionTest, ConstantDivColumnProducesDivisionSync) {
  auto result = compile_expression(binary("/", num(100.0), col("price")), input,
                                   scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& cnum = builder.operators()[1];
  auto& div = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(cnum.type, "ConstantNumber");
  EXPECT_EQ(cnum.params.at("value"), 100.0);
  EXPECT_EQ(div.type, "Division");

  ASSERT_EQ(builder.connections().size(), 4u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", cnum.id, "i1");
  expect_conn(builder, cnum.id, "o1", div.id, "i1");
  expect_conn(builder, ext.id, "o1", div.id, "i2");
}

// CASE WHEN price > 100 THEN price ELSE 0 END
// → CompareGT(100) as condition, VectorExtract for price, ConstantNumber(0)
//   + LogicalNand(1) for NOT → Multiplexer(2)
TEST_F(ExpressionTest, CaseWhenSingleBranchProducesMultiplexer) {
  auto expr = case_when(
      cmp(">", col("price"), num(100.0)),  // WHEN price > 100
      col("price"),                         // THEN price
      num(0.0)                             // ELSE 0
  );
  auto result = compile_expression(std::move(expr), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  // The Multiplexer must be the last operator
  bool has_mux = false;
  bool has_nand = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Multiplexer") {
      has_mux = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);
      EXPECT_EQ(ep.operator_id, op.id);
    }
    if (op.type == "LogicalNand") {
      has_nand = true;
      EXPECT_EQ(op.params.at("numPorts"), 1.0);
    }
  }
  EXPECT_TRUE(has_mux);
  EXPECT_TRUE(has_nand);
}

// CASE WHEN price > 100 THEN price END (no ELSE — Multiplexer with 1 port)
TEST_F(ExpressionTest, CaseWhenNoElseProducesMultiplexerOnePort) {
  auto e = std::make_unique<CaseExpr>();
  CaseWhenClause clause;
  clause.condition = cmp(">", col("price"), num(100.0));
  clause.result = col("price");
  e->when_clauses.push_back(std::move(clause));
  // No else_result
  Expr expr = std::move(e);

  auto result = compile_expression(std::move(expr), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  bool has_mux = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Multiplexer") {
      has_mux = true;
      EXPECT_EQ(op.params.at("numPorts"), 1.0);
    }
  }
  EXPECT_TRUE(has_mux);
}

// TS() → TimestampExtract
TEST_F(ExpressionTest, TSFunctionProducesTimestampExtract) {
  auto f = std::make_unique<FuncCall>();
  f->name = "TS";
  Expr expr = std::move(f);

  auto result = compile_expression(std::move(expr), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  ASSERT_EQ(builder.operators().size(), 1u);
  auto& op = builder.operators()[0];
  EXPECT_EQ(op.type, "TimestampExtract");
  EXPECT_EQ(ep.operator_id, op.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 1u);
  expect_conn(builder, "input_0", "o1", op.id, "i1");
}

// ts() lowercase → TimestampExtract
TEST_F(ExpressionTest, TSFunctionCaseInsensitive) {
  auto f = std::make_unique<FuncCall>();
  f->name = "ts";
  Expr expr = std::move(f);

  auto result = compile_expression(std::move(expr), input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 1u);
  EXPECT_EQ(builder.operators()[0].type, "TimestampExtract");
}

// TS(123) should fail — no arguments allowed.
// Driven through compile_sql since the analyzer catches this on the
// user-facing path.
TEST(ExpressionAnalyzerTest, TSFunctionWithArgsRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {{"price", 0, rtbot_sql::ColumnType::DOUBLE}};
  c.streams["trades"] = schema;
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT TS(123) FROM trades", c);
  EXPECT_TRUE(r.has_errors());
}

// RESAMPLE_CONSTANT(price, 1000000) → VectorExtract(1) → ResamplerConstant(interval=1000000, t0=0)
TEST_F(ExpressionTest, ResampleConstantProducesResamplerConstant) {
  auto result = compile_expression(
      func2("RESAMPLE_CONSTANT", col("price"), num(1000000.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& rs = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(rs.type, "ResamplerConstant");
  EXPECT_EQ(rs.params.at("interval"), 1000000.0);
  EXPECT_EQ(rs.params.at("t0"), 0.0);
  EXPECT_EQ(ep.operator_id, rs.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", rs.id, "i1");
}

// resample_constant() lowercase → ResamplerConstant (case insensitive)
TEST_F(ExpressionTest, ResampleConstantCaseInsensitive) {
  auto result = compile_expression(
      func2("resample_constant", col("price"), num(5000.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  EXPECT_EQ(builder.operators()[1].type, "ResamplerConstant");
  EXPECT_EQ(builder.operators()[1].params.at("interval"), 5000.0);
  EXPECT_EQ(builder.operators()[1].params.at("t0"), 0.0);
}

// RESAMPLE_CONSTANT with wrong arg count rejected by the analyzer.
TEST(ExpressionAnalyzerTest, ResampleConstantWrongArgCountRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {{"price", 0, rtbot_sql::ColumnType::DOUBLE}};
  c.streams["trades"] = schema;
  // Zero args
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT RESAMPLE_CONSTANT() FROM trades",
                  c)
                  .has_errors());
  // One arg
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT RESAMPLE_CONSTANT(price) FROM trades",
                  c)
                  .has_errors());
  // Four args
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT RESAMPLE_CONSTANT(price, 1000, 1, 0) FROM trades",
                  c)
                  .has_errors());
}

// RESAMPLE_CONSTANT(price, quantity) — non-constant interval rejected by
// the analyzer.
TEST(ExpressionAnalyzerTest, ResampleConstantNonConstantIntervalRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {
      {"price", 0, rtbot_sql::ColumnType::DOUBLE},
      {"quantity", 1, rtbot_sql::ColumnType::DOUBLE},
  };
  c.streams["trades"] = schema;
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT RESAMPLE_CONSTANT(price, quantity) FROM trades",
      c);
  EXPECT_TRUE(r.has_errors());
}

// RESAMPLE_CONSTANT(price * 2, 1000) — expression as first arg works
TEST_F(ExpressionTest, ResampleConstantWithExpressionArg) {
  auto result = compile_expression(
      func2("RESAMPLE_CONSTANT", binary("*", col("price"), num(2.0)),
            num(1000.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  // Should have: VectorExtract → Scale(2) → ResamplerConstant(interval=1000, t0=0)
  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& scale = builder.operators()[1];
  auto& rs = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(scale.type, "Scale");
  EXPECT_EQ(scale.params.at("value"), 2.0);
  EXPECT_EQ(rs.type, "ResamplerConstant");
  EXPECT_EQ(rs.params.at("interval"), 1000.0);
  EXPECT_EQ(rs.params.at("t0"), 0.0);

  ASSERT_EQ(builder.connections().size(), 3u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", scale.id, "i1");
  expect_conn(builder, scale.id, "o1", rs.id, "i1");
}

// TIMESHIFT(price, -1000) → VectorExtract(1) → TimeShift(shift=-1000)
TEST_F(ExpressionTest, TimeShiftProducesTimeShift) {
  auto result = compile_expression(
      func2("TIMESHIFT", col("price"), num(-1000.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& ts = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(ts.type, "TimeShift");
  EXPECT_EQ(ts.params.at("shift"), -1000.0);
  EXPECT_EQ(ep.operator_id, ts.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", ts.id, "i1");
}

// timeshift() lowercase → TimeShift (case insensitive)
TEST_F(ExpressionTest, TimeShiftCaseInsensitive) {
  auto result = compile_expression(
      func2("timeshift", col("price"), num(-500.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));

  ASSERT_EQ(builder.operators().size(), 2u);
  EXPECT_EQ(builder.operators()[1].type, "TimeShift");
  EXPECT_EQ(builder.operators()[1].params.at("shift"), -500.0);
}

// TIMESHIFT with wrong arg count rejected by the analyzer.
TEST(ExpressionAnalyzerTest, TimeShiftWrongArgCountRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {{"price", 0, rtbot_sql::ColumnType::DOUBLE}};
  c.streams["trades"] = schema;
  // Zero args
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT TIMESHIFT() FROM trades",
                  c)
                  .has_errors());
  // One arg
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT TIMESHIFT(price) FROM trades",
                  c)
                  .has_errors());
  // Three args
  EXPECT_TRUE(rtbot_sql::api::compile_sql(
                  "CREATE MATERIALIZED VIEW v AS "
                  "SELECT TIMESHIFT(price, -1000, 0) FROM trades",
                  c)
                  .has_errors());
}

// TIMESHIFT(price, quantity) — non-constant shift rejected by the analyzer.
TEST(ExpressionAnalyzerTest, TimeShiftNonConstantShiftRejected) {
  rtbot_sql::CatalogSnapshot c;
  rtbot_sql::StreamSchema schema;
  schema.name = "trades";
  schema.columns = {
      {"price", 0, rtbot_sql::ColumnType::DOUBLE},
      {"quantity", 1, rtbot_sql::ColumnType::DOUBLE},
  };
  c.streams["trades"] = schema;
  auto r = rtbot_sql::api::compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT TIMESHIFT(price, quantity) FROM trades",
      c);
  EXPECT_TRUE(r.has_errors());
}

// TIMESHIFT(RESAMPLE_CONSTANT(price, 1000), -1000) — composition
TEST_F(ExpressionTest, TimeShiftWrappingResampleConstant) {
  auto inner = func2("RESAMPLE_CONSTANT", col("price"), num(1000.0));
  auto result = compile_expression(
      func2("TIMESHIFT", std::move(inner), num(-1000.0)),
      input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result));
  auto& ep = std::get<Endpoint>(result);

  // Should have: VectorExtract → ResamplerConstant → TimeShift
  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& rs = builder.operators()[1];
  auto& ts = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(rs.type, "ResamplerConstant");
  EXPECT_EQ(rs.params.at("interval"), 1000.0);
  EXPECT_EQ(rs.params.at("t0"), 0.0);
  EXPECT_EQ(ts.type, "TimeShift");
  EXPECT_EQ(ts.params.at("shift"), -1000.0);
  EXPECT_EQ(ep.operator_id, ts.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 3u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", rs.id, "i1");
  expect_conn(builder, rs.id, "o1", ts.id, "i1");
}

// --- Bytecode compilation tests ---

class BytecodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "trades",
        {{"instrument_id", 0}, {"price", 1}, {"quantity", 2},
         {"account_id", 3}},
    };
    scope.register_stream("trades", schema, "t");
  }

  analyzer::Scope scope;
  std::map<std::pair<std::string, int>, int> column_to_input;
  std::vector<double> constants;
};

// ColumnRef → INPUT opcode
TEST_F(BytecodeTest, ColumnRefProducesInputOpcode) {
  auto result = compile_expression_to_bytecode(
      col("price"), scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  // Expected: INPUT 0 END (price maps to input port 0, first seen)
  ASSERT_EQ(result.bytecode.size(), 3u);
  EXPECT_EQ(result.bytecode[0], 0);  // INPUT
  EXPECT_EQ(result.bytecode[1], 0);  // input index 0 (first column seen)
  EXPECT_EQ(result.bytecode[2], 20); // END
  EXPECT_EQ(column_to_input.size(), 1u);
  auto key = std::make_pair(std::string("trades"), 1);
  EXPECT_EQ(column_to_input[key], 0);
}

// Constant → CONST opcode
TEST_F(BytecodeTest, ConstantProducesConstOpcode) {
  auto result = compile_expression_to_bytecode(
      num(42.0), scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  ASSERT_EQ(result.bytecode.size(), 3u);
  EXPECT_EQ(result.bytecode[0], 1);  // CONST
  EXPECT_EQ(result.bytecode[1], 0);  // constant index 0
  EXPECT_EQ(result.bytecode[2], 20); // END
  ASSERT_EQ(constants.size(), 1u);
  EXPECT_EQ(constants[0], 42.0);
}

// price * quantity → INPUT 0 INPUT 1 MUL END
TEST_F(BytecodeTest, BinaryExprProducesOpcodes) {
  auto result = compile_expression_to_bytecode(
      binary("*", col("price"), col("quantity")),
      scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  // INPUT(price=0), INPUT(quantity=1), MUL, END
  std::vector<double> expected = {0, 0, 0, 1, 4, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  EXPECT_EQ(column_to_input.size(), 2u);
}

// Column deduplication: same column referenced twice shares input index
TEST_F(BytecodeTest, ColumnDeduplication) {
  // price * price → INPUT 0 INPUT 0 MUL END
  auto result = compile_expression_to_bytecode(
      binary("*", col("price"), col("price")),
      scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 0, 0, 4, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  EXPECT_EQ(column_to_input.size(), 1u);  // only one unique column
}

// POWER(price, 2) → INPUT 0 CONST 0 POW END
TEST_F(BytecodeTest, PowerFunction) {
  auto result = compile_expression_to_bytecode(
      func2("POWER", col("price"), num(2.0)),
      scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 1, 0, 6, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(constants.size(), 1u);
  EXPECT_EQ(constants[0], 2.0);
}

// ABS(price) → INPUT 0 ABS END
TEST_F(BytecodeTest, UnaryMathFunction) {
  auto result = compile_expression_to_bytecode(
      func("ABS", col("price")),
      scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 7, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
}

// SQRT(price) → INPUT 0 SQRT END
TEST_F(BytecodeTest, SqrtFunction) {
  auto result = compile_expression_to_bytecode(
      func("SQRT", col("price")),
      scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 8, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
}

// Aggregate function → not fusable
TEST_F(BytecodeTest, AggregateFunctionNotFusable) {
  auto result = compile_expression_to_bytecode(
      func("SUM", col("price")),
      scope, column_to_input, constants);
  EXPECT_FALSE(result.success);
}

// CASE expression → not fusable
TEST_F(BytecodeTest, CaseExprNotFusable) {
  auto expr = case_when(
      cmp(">", col("price"), num(100.0)),
      col("price"),
      num(0.0));
  auto result = compile_expression_to_bytecode(
      std::move(expr), scope, column_to_input, constants);
  EXPECT_FALSE(result.success);
}

// TIMESHIFT → not fusable
TEST_F(BytecodeTest, TimeShiftNotFusable) {
  auto result = compile_expression_to_bytecode(
      func2("TIMESHIFT", col("price"), num(-1000.0)),
      scope, column_to_input, constants);
  EXPECT_FALSE(result.success);
}

// Complex expression: POWER(price - quantity * quantity, 0.5)
// → INPUT(price) INPUT(qty) INPUT(qty) MUL SUB CONST(0.5) POW END
TEST_F(BytecodeTest, ComplexExpressionBytecode) {
  auto expr = func2("POWER",
      binary("-", col("price"),
             binary("*", col("quantity"), col("quantity"))),
      num(0.5));
  auto result = compile_expression_to_bytecode(
      std::move(expr), scope, column_to_input, constants);
  ASSERT_TRUE(result.success);
  // INPUT 0(price), INPUT 1(qty), INPUT 1(qty), MUL, SUB, CONST 0(0.5), POW, END
  std::vector<double> expected = {0, 0, 0, 1, 0, 1, 4, 3, 1, 0, 6, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  EXPECT_EQ(column_to_input.size(), 2u);
  ASSERT_EQ(constants.size(), 1u);
  EXPECT_EQ(constants[0], 0.5);
}

// Shared state across multiple expressions (column_to_input and constants persist)
TEST_F(BytecodeTest, SharedStateAcrossExpressions) {
  // First expression: price + 1.0
  auto r1 = compile_expression_to_bytecode(
      binary("+", col("price"), num(1.0)),
      scope, column_to_input, constants);
  ASSERT_TRUE(r1.success);
  EXPECT_EQ(column_to_input.size(), 1u);
  EXPECT_EQ(constants.size(), 1u);

  // Second expression: quantity * 2.0 (quantity is new column, 2.0 is new constant)
  auto r2 = compile_expression_to_bytecode(
      binary("*", col("quantity"), num(2.0)),
      scope, column_to_input, constants);
  ASSERT_TRUE(r2.success);
  EXPECT_EQ(column_to_input.size(), 2u);  // price + quantity
  EXPECT_EQ(constants.size(), 2u);         // 1.0 + 2.0

  // Second expression should use input index 1 for quantity and const index 1 for 2.0
  std::vector<double> expected_r2 = {0, 1, 1, 1, 4, 20};
  ASSERT_EQ(r2.bytecode.size(), expected_r2.size());
  for (size_t i = 0; i < expected_r2.size(); i++) {
    EXPECT_EQ(r2.bytecode[i], expected_r2[i]) << "at index " << i;
  }
}

// --- Aggregate bytecode compilation tests ---

// Helper: create a zero-arg FuncCall (for COUNT(*))
Expr func0(const std::string& name) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  return f;
}

class AggBytecodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "data",
        {{"id", 0}, {"amplitude", 1}, {"frequency", 2}},
    };
    scope.register_stream("data", schema, "d");
  }

  analyzer::Scope scope;
  std::map<std::pair<std::string, int>, int> column_to_input;
  std::vector<double> constants;
  AggBytecodeContext agg_ctx;
};

// SUM(amplitude) → INPUT 0, CUMSUM 0, END
// state_init: [0.0, 0.0] (sum + kahan)
TEST_F(AggBytecodeTest, SumProducesCumsumBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("SUM", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 21, 0, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  // State: [sum=0, kahan=0]
  ASSERT_EQ(agg_ctx.state_init.size(), 2u);
  EXPECT_EQ(agg_ctx.state_init[0], 0.0);
  EXPECT_EQ(agg_ctx.state_init[1], 0.0);
  // One input port (amplitude)
  EXPECT_EQ(column_to_input.size(), 1u);
}

// COUNT(*) → COUNT 0, END
// state_init: [0.0]
TEST_F(AggBytecodeTest, CountProducesCountBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func0("COUNT"), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {22, 0, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(agg_ctx.state_init.size(), 1u);
  EXPECT_EQ(agg_ctx.state_init[0], 0.0);
  EXPECT_EQ(agg_ctx.shared_count_state_idx, 0);
  EXPECT_TRUE(agg_ctx.count_emitted);
  // No input ports needed for COUNT(*)
  EXPECT_EQ(column_to_input.size(), 0u);
}

// AVG(amplitude) → INPUT 0, CUMSUM 0, COUNT 2, DIV, END
// state_init: [0, 0, 0] (sum, kahan, count)
TEST_F(AggBytecodeTest, AvgProducesCumsumCountDivBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("AVG", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  // INPUT 0, CUMSUM 0, COUNT 2, DIV, END
  std::vector<double> expected = {0, 0, 21, 0, 22, 2, 5, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  // State: [sum=0, kahan=0, count=0]
  ASSERT_EQ(agg_ctx.state_init.size(), 3u);
  EXPECT_EQ(agg_ctx.state_init[0], 0.0);
  EXPECT_EQ(agg_ctx.state_init[1], 0.0);
  EXPECT_EQ(agg_ctx.state_init[2], 0.0);
  EXPECT_EQ(agg_ctx.shared_count_state_idx, 2);
  EXPECT_TRUE(agg_ctx.count_emitted);
}

// MAX(amplitude) → INPUT 0, MAX_AGG 0, END
// state_init: [-DBL_MAX] (not -inf: JSON cannot represent infinity)
TEST_F(AggBytecodeTest, MaxProducesMaxAggBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("MAX", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 23, 0, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(agg_ctx.state_init.size(), 1u);
  EXPECT_EQ(agg_ctx.state_init[0], -std::numeric_limits<double>::max());
}

// MIN(amplitude) → INPUT 0, MIN_AGG 0, END
// state_init: [+DBL_MAX] (not +inf: JSON cannot represent infinity)
TEST_F(AggBytecodeTest, MinProducesMinAggBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("MIN", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 24, 0, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(agg_ctx.state_init.size(), 1u);
  EXPECT_EQ(agg_ctx.state_init[0], std::numeric_limits<double>::max());
}

// MAX(ABS(amplitude)) → INPUT 0, ABS, MAX_AGG 0, END
// Aggregate wrapping a pure math function
TEST_F(AggBytecodeTest, MaxOfAbsProducesNestedBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("MAX", func("ABS", col("amplitude"))),
      scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  std::vector<double> expected = {0, 0, 7, 23, 0, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(agg_ctx.state_init.size(), 1u);
  EXPECT_EQ(agg_ctx.state_init[0], -std::numeric_limits<double>::max());
}

// AVG(POWER(amplitude, 2)) → INPUT 0, CONST 0, POW, CUMSUM 0, COUNT 2, DIV, END
TEST_F(AggBytecodeTest, AvgOfPowerProducesBytecode) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("AVG", func2("POWER", col("amplitude"), num(2.0))),
      scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  // INPUT 0, CONST 0(=2.0), POW, CUMSUM 0, COUNT 2, DIV, END
  std::vector<double> expected = {0, 0, 1, 0, 6, 21, 0, 22, 2, 5, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  ASSERT_EQ(constants.size(), 1u);
  EXPECT_EQ(constants[0], 2.0);
  // State: [sum=0, kahan=0, count=0]
  ASSERT_EQ(agg_ctx.state_init.size(), 3u);
}

// Shared COUNT: Two AVGs share the same COUNT state slot.
// First AVG emits COUNT opcode, second AVG emits STATE_LOAD.
TEST_F(AggBytecodeTest, SharedCountAcrossTwoAvgs) {
  // First: AVG(amplitude) → INPUT 0, CUMSUM 0, COUNT 2, DIV, END
  auto r1 = compile_aggregate_expression_to_bytecode(
      func("AVG", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r1.success);
  std::vector<double> expected1 = {0, 0, 21, 0, 22, 2, 5, 20};
  ASSERT_EQ(r1.bytecode.size(), expected1.size());
  for (size_t i = 0; i < expected1.size(); i++) {
    EXPECT_EQ(r1.bytecode[i], expected1[i]) << "r1 at index " << i;
  }

  // Second: AVG(frequency) → INPUT 1, CUMSUM 3, STATE_LOAD 2, DIV, END
  // (frequency is input 1, new CUMSUM at state[3..4], reuses COUNT at state[2])
  auto r2 = compile_aggregate_expression_to_bytecode(
      func("AVG", col("frequency")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r2.success);
  std::vector<double> expected2 = {0, 1, 21, 3, 25, 2, 5, 20};
  ASSERT_EQ(r2.bytecode.size(), expected2.size());
  for (size_t i = 0; i < expected2.size(); i++) {
    EXPECT_EQ(r2.bytecode[i], expected2[i]) << "r2 at index " << i;
  }

  // State layout: [sum_amp(0), kahan_amp(1), count(2), sum_freq(3), kahan_freq(4)]
  ASSERT_EQ(agg_ctx.state_init.size(), 5u);
  EXPECT_EQ(agg_ctx.state_init[0], 0.0);  // sum_amp
  EXPECT_EQ(agg_ctx.state_init[1], 0.0);  // kahan_amp
  EXPECT_EQ(agg_ctx.state_init[2], 0.0);  // count (shared)
  EXPECT_EQ(agg_ctx.state_init[3], 0.0);  // sum_freq
  EXPECT_EQ(agg_ctx.state_init[4], 0.0);  // kahan_freq
  EXPECT_EQ(column_to_input.size(), 2u);   // amplitude + frequency
}

// Shared COUNT: AVG then COUNT(*) reuses the same slot with STATE_LOAD
TEST_F(AggBytecodeTest, SharedCountAvgThenExplicitCount) {
  // First: AVG(amplitude) → allocates COUNT at some state index
  auto r1 = compile_aggregate_expression_to_bytecode(
      func("AVG", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r1.success);
  int count_idx = agg_ctx.shared_count_state_idx;
  EXPECT_GE(count_idx, 0);
  EXPECT_TRUE(agg_ctx.count_emitted);

  // Second: COUNT(*) → should emit STATE_LOAD (not COUNT) since count already emitted
  auto r2 = compile_aggregate_expression_to_bytecode(
      func0("COUNT"), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r2.success);
  // STATE_LOAD count_idx, END
  std::vector<double> expected2 = {25, static_cast<double>(count_idx), 20};
  ASSERT_EQ(r2.bytecode.size(), expected2.size());
  for (size_t i = 0; i < expected2.size(); i++) {
    EXPECT_EQ(r2.bytecode[i], expected2[i]) << "r2 at index " << i;
  }
}

// Full vibration_moments-like pattern: AVG(x), AVG(POWER(x,2)), MAX(ABS(x)), COUNT(*)
TEST_F(AggBytecodeTest, FullVibrationMomentsPattern) {
  // Expression 1: AVG(amplitude) → INPUT 0, CUMSUM 0, COUNT 2, DIV, END
  auto r1 = compile_aggregate_expression_to_bytecode(
      func("AVG", col("amplitude")), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r1.success);

  // Expression 2: AVG(POWER(amplitude, 2)) → INPUT 0, CONST 0, POW, CUMSUM 3, STATE_LOAD 2, DIV, END
  auto r2 = compile_aggregate_expression_to_bytecode(
      func("AVG", func2("POWER", col("amplitude"), num(2.0))),
      scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r2.success);
  // Verify STATE_LOAD (not COUNT) for second AVG
  bool found_state_load = false;
  for (size_t i = 0; i + 1 < r2.bytecode.size(); i++) {
    if (r2.bytecode[i] == 25) {  // STATE_LOAD
      found_state_load = true;
      EXPECT_EQ(r2.bytecode[i + 1], static_cast<double>(agg_ctx.shared_count_state_idx));
    }
  }
  EXPECT_TRUE(found_state_load) << "Expected STATE_LOAD in second AVG";

  // Expression 3: MAX(ABS(amplitude)) → INPUT 0, ABS, MAX_AGG idx, END
  auto r3 = compile_aggregate_expression_to_bytecode(
      func("MAX", func("ABS", col("amplitude"))),
      scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r3.success);

  // Expression 4: COUNT(*) → STATE_LOAD count_idx, END
  auto r4 = compile_aggregate_expression_to_bytecode(
      func0("COUNT"), scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(r4.success);
  EXPECT_EQ(r4.bytecode[0], 25);  // STATE_LOAD (not COUNT)

  // Only 1 input column (amplitude) despite being referenced multiple times
  EXPECT_EQ(column_to_input.size(), 1u);
  // Only COUNT slot allocated once (shared)
  EXPECT_EQ(agg_ctx.shared_count_state_idx, 2);
}

// Non-fusable: windowed function inside aggregate
TEST_F(AggBytecodeTest, WindowedFunctionNotFusable) {
  // MOVING_AVERAGE(amplitude, 10) — not a fusable aggregate
  auto result = compile_aggregate_expression_to_bytecode(
      func2("MOVING_AVERAGE", col("amplitude"), num(10.0)),
      scope, column_to_input, constants, agg_ctx);
  EXPECT_FALSE(result.success);
}

// Non-fusable: COUNT(amplitude) — COUNT with args not supported
TEST_F(AggBytecodeTest, CountWithArgsNotFusable) {
  auto result = compile_aggregate_expression_to_bytecode(
      func("COUNT", col("amplitude")),
      scope, column_to_input, constants, agg_ctx);
  EXPECT_FALSE(result.success);
}

// Pure expressions still work through the aggregate compiler
TEST_F(AggBytecodeTest, PureExpressionPassthrough) {
  // amplitude * 2 + frequency → should compile through the delegate to pure path
  auto result = compile_aggregate_expression_to_bytecode(
      binary("+", binary("*", col("amplitude"), num(2.0)), col("frequency")),
      scope, column_to_input, constants, agg_ctx);
  ASSERT_TRUE(result.success);
  // INPUT 0(amp), CONST 0(2.0), MUL, INPUT 1(freq), ADD, END
  std::vector<double> expected = {0, 0, 1, 0, 4, 0, 1, 2, 20};
  ASSERT_EQ(result.bytecode.size(), expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    EXPECT_EQ(result.bytecode[i], expected[i]) << "at index " << i;
  }
  EXPECT_TRUE(agg_ctx.state_init.empty());  // no state needed for pure expressions
}

// =========================================================================
// Segment bytecode compilation
// =========================================================================

// Helper to build ComparisonExpr
Expr comparison(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<ComparisonExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

// Helper to build LogicalExpr
Expr logical(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<LogicalExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

// Helper to build NotExpr
Expr not_expr(Expr operand) {
  auto e = std::make_unique<NotExpr>();
  e->operand = std::move(operand);
  return e;
}

// Helper to build BetweenExpr
Expr between(Expr expr, Expr low, Expr high) {
  auto e = std::make_unique<BetweenExpr>();
  e->expr = std::move(expr);
  e->low = std::move(low);
  e->high = std::move(high);
  return e;
}

TEST(SegmentBytecode, BooleanComparisonGT) {
  // ABS(amplitude) > 0  where amplitude is column 2
  // Expected bytecode: INPUT 2, ABS, CONST 0, GT, END
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"device_id", 0}, {"channel_id", 1}, {"amplitude", 2}}};
  scope.register_stream("s", schema);

  auto expr = comparison(">", func("ABS", col("amplitude")), num(0.0));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode, std::vector<double>({0, 2, 7, 1, 0, 26, 20}));
  EXPECT_EQ(result.constants, std::vector<double>({0.0}));
}

TEST(SegmentBytecode, BooleanComparisonLTE) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = comparison("<=", col("amplitude"), num(100.0));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode, std::vector<double>({0, 0, 1, 0, 29, 20}));
  EXPECT_EQ(result.constants, std::vector<double>({100.0}));
}

TEST(SegmentBytecode, LogicalAND) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = logical("AND",
      comparison(">", func("ABS", col("amplitude")), num(0.0)),
      comparison("<", col("amplitude"), num(100.0)));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode,
            std::vector<double>({0, 0, 7, 1, 0, 26, 0, 0, 1, 1, 28, 32, 20}));
}

TEST(SegmentBytecode, LogicalOR) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = logical("OR",
      comparison(">", col("amplitude"), num(100.0)),
      comparison("<", col("amplitude"), num(-100.0)));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode,
            std::vector<double>({0, 0, 1, 0, 26, 0, 0, 1, 1, 28, 33, 20}));
}

TEST(SegmentBytecode, NotComparison) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = not_expr(comparison(">", col("amplitude"), num(5.0)));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode, std::vector<double>({0, 0, 1, 0, 26, 34, 20}));
}

TEST(SegmentBytecode, BetweenExpr) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = between(col("amplitude"), num(10.0), num(100.0));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode,
            std::vector<double>({0, 0, 1, 0, 27, 0, 0, 1, 1, 29, 32, 20}));
  EXPECT_EQ(result.constants, std::vector<double>({10.0, 100.0}));
}

TEST(SegmentBytecode, NumericExpression) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = func("FLOOR", binary("/", col("amplitude"), num(10.0)));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.bytecode, std::vector<double>({0, 0, 1, 0, 5, 16, 20}));
}

TEST(SegmentBytecode, NonFusableReturnsFalse) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"amplitude", 0}}};
  scope.register_stream("s", schema);

  auto expr = comparison(">", func("TS", col("amplitude")), num(0.0));

  auto result = compile_segment_to_bytecode(expr, scope, "s");
  EXPECT_FALSE(result.success);
}

TEST(SegmentBytecode, AllComparisonOps) {
  analyzer::Scope scope;
  StreamSchema schema{"s", {{"x", 0}}};
  scope.register_stream("s", schema);

  auto test_op = [&](const std::string& op, double expected_opcode) {
    auto expr = comparison(op, col("x"), num(1.0));
    auto result = compile_segment_to_bytecode(expr, scope, "s");
    ASSERT_TRUE(result.success) << "Failed for op: " << op;
    ASSERT_GE(result.bytecode.size(), 4u);
    EXPECT_EQ(result.bytecode[result.bytecode.size() - 2], expected_opcode)
        << "Wrong opcode for op: " << op;
  };

  test_op(">", 26);   // GT
  test_op(">=", 27);  // GTE
  test_op("<", 28);   // LT
  test_op("<=", 29);  // LTE
  test_op("=", 30);   // EQ
  test_op("!=", 31);  // NEQ
}

}  // namespace
}  // namespace rtbot_sql::compiler
