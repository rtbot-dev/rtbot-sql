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

}  // namespace
}  // namespace rtbot_sql::compiler
