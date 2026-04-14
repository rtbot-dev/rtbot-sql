#include "rtbot_sql/compiler/expression_compiler.h"

#include <gtest/gtest.h>

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

// Unknown column throws
TEST_F(ExpressionTest, UnknownColumnThrows) {
  EXPECT_THROW(compile_expression(col("nonexistent"), input, scope, builder),
               std::runtime_error);
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

// TS(123) should fail — no arguments allowed
TEST_F(ExpressionTest, TSFunctionWithArgsThrows) {
  auto f = std::make_unique<FuncCall>();
  f->name = "TS";
  f->args.push_back(num(123.0));
  Expr expr = std::move(f);

  EXPECT_THROW(
      compile_expression(std::move(expr), input, scope, builder),
      std::runtime_error);
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

// RESAMPLE_CONSTANT with wrong arg count throws
TEST_F(ExpressionTest, ResampleConstantWrongArgCountThrows) {
  // Zero args
  {
    auto f = std::make_unique<FuncCall>();
    f->name = "RESAMPLE_CONSTANT";
    Expr expr = std::move(f);
    EXPECT_THROW(
        compile_expression(std::move(expr), input, scope, builder),
        std::runtime_error);
  }
  // One arg
  {
    GraphBuilder b2;
    EXPECT_THROW(
        compile_expression(func("RESAMPLE_CONSTANT", col("price")),
                           input, scope, b2),
        std::runtime_error);
  }
  // Four args
  {
    GraphBuilder b3;
    auto f = std::make_unique<FuncCall>();
    f->name = "RESAMPLE_CONSTANT";
    f->args.push_back(col("price"));
    f->args.push_back(num(1000.0));
    f->args.push_back(num(1.0));
    f->args.push_back(num(0.0));
    Expr expr = std::move(f);
    EXPECT_THROW(
        compile_expression(std::move(expr), input, scope, b3),
        std::runtime_error);
  }
}

// RESAMPLE_CONSTANT(price, quantity) — non-constant interval should throw
TEST_F(ExpressionTest, ResampleConstantNonConstantIntervalThrows) {
  EXPECT_THROW(
      compile_expression(
          func2("RESAMPLE_CONSTANT", col("price"), col("quantity")),
          input, scope, builder),
      std::runtime_error);
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

// TIMESHIFT with wrong arg count throws
TEST_F(ExpressionTest, TimeShiftWrongArgCountThrows) {
  // Zero args
  {
    auto f = std::make_unique<FuncCall>();
    f->name = "TIMESHIFT";
    Expr expr = std::move(f);
    EXPECT_THROW(
        compile_expression(std::move(expr), input, scope, builder),
        std::runtime_error);
  }
  // One arg
  {
    GraphBuilder b2;
    EXPECT_THROW(
        compile_expression(func("TIMESHIFT", col("price")),
                           input, scope, b2),
        std::runtime_error);
  }
  // Three args
  {
    GraphBuilder b3;
    auto f = std::make_unique<FuncCall>();
    f->name = "TIMESHIFT";
    f->args.push_back(col("price"));
    f->args.push_back(num(-1000.0));
    f->args.push_back(num(0.0));
    Expr expr = std::move(f);
    EXPECT_THROW(
        compile_expression(std::move(expr), input, scope, b3),
        std::runtime_error);
  }
}

// TIMESHIFT(price, quantity) — non-constant shift should throw
TEST_F(ExpressionTest, TimeShiftNonConstantShiftThrows) {
  EXPECT_THROW(
      compile_expression(
          func2("TIMESHIFT", col("price"), col("quantity")),
          input, scope, builder),
      std::runtime_error);
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

}  // namespace
}  // namespace rtbot_sql::compiler
