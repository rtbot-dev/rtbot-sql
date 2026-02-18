#include "rtbot_sql/compiler/where_compiler.h"

#include <gtest/gtest.h>

namespace rtbot_sql::compiler {
namespace {

using namespace parser::ast;

// Helpers to build AST nodes
Expr col(const std::string& name) { return ColumnRef{"", name}; }

Expr num(double v) { return Constant{v}; }

Expr binary(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr cmp(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<ComparisonExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr logical(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<LogicalExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

Expr not_expr(Expr operand) {
  auto e = std::make_unique<NotExpr>();
  e->operand = std::move(operand);
  return e;
}

Expr between(Expr expr, Expr low, Expr high) {
  auto e = std::make_unique<BetweenExpr>();
  e->expr = std::move(expr);
  e->low = std::move(low);
  e->high = std::move(high);
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

class WhereTest : public ::testing::Test {
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

// WHERE price > 30 → VectorExtract → CompareGT(30) → Demultiplexer
TEST_F(WhereTest, SimpleComparisonGT) {
  auto result =
      compile_where(cmp(">", col("price"), num(30.0)), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& cmpop = builder.operators()[1];
  auto& demux = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(ext.params.at("index"), 1.0);
  EXPECT_EQ(cmpop.type, "CompareGT");
  EXPECT_EQ(cmpop.params.at("value"), 30.0);
  EXPECT_EQ(demux.type, "Demultiplexer");
  EXPECT_EQ(result.operator_id, demux.id);

  ASSERT_EQ(builder.connections().size(), 4u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", cmpop.id, "i1");
  expect_conn(builder, cmpop.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE price * quantity > 100000
TEST_F(WhereTest, ExpressionComparison) {
  auto where_expr =
      cmp(">", binary("*", col("price"), col("quantity")), num(100000.0));
  auto result = compile_where(std::move(where_expr), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 5u);
  auto& ext_p = builder.operators()[0];
  auto& ext_q = builder.operators()[1];
  auto& mult = builder.operators()[2];
  auto& cmpop = builder.operators()[3];
  auto& demux = builder.operators()[4];
  EXPECT_EQ(ext_p.type, "VectorExtract");
  EXPECT_EQ(ext_q.type, "VectorExtract");
  EXPECT_EQ(mult.type, "Multiplication");
  EXPECT_EQ(cmpop.type, "CompareGT");
  EXPECT_EQ(cmpop.params.at("value"), 100000.0);
  EXPECT_EQ(demux.type, "Demultiplexer");

  ASSERT_EQ(builder.connections().size(), 7u);
  expect_conn(builder, "input_0", "o1", ext_p.id, "i1");
  expect_conn(builder, "input_0", "o1", ext_q.id, "i1");
  expect_conn(builder, ext_p.id, "o1", mult.id, "i1");
  expect_conn(builder, ext_q.id, "o1", mult.id, "i2");
  expect_conn(builder, mult.id, "o1", cmpop.id, "i1");
  expect_conn(builder, cmpop.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE price > 30 AND quantity < 500
TEST_F(WhereTest, LogicalAnd) {
  auto where_expr = logical("AND", cmp(">", col("price"), num(30.0)),
                            cmp("<", col("quantity"), num(500.0)));
  auto result = compile_where(std::move(where_expr), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 6u);
  auto& ext_p = builder.operators()[0];
  auto& cmp_gt = builder.operators()[1];
  auto& ext_q = builder.operators()[2];
  auto& cmp_lt = builder.operators()[3];
  auto& land = builder.operators()[4];
  auto& demux = builder.operators()[5];
  EXPECT_EQ(ext_p.type, "VectorExtract");
  EXPECT_EQ(cmp_gt.type, "CompareGT");
  EXPECT_EQ(ext_q.type, "VectorExtract");
  EXPECT_EQ(cmp_lt.type, "CompareLT");
  EXPECT_EQ(land.type, "LogicalAnd");
  EXPECT_EQ(demux.type, "Demultiplexer");

  ASSERT_EQ(builder.connections().size(), 8u);
  expect_conn(builder, "input_0", "o1", ext_p.id, "i1");
  expect_conn(builder, ext_p.id, "o1", cmp_gt.id, "i1");
  expect_conn(builder, "input_0", "o1", ext_q.id, "i1");
  expect_conn(builder, ext_q.id, "o1", cmp_lt.id, "i1");
  expect_conn(builder, cmp_gt.id, "o1", land.id, "i1");
  expect_conn(builder, cmp_lt.id, "o1", land.id, "i2");
  expect_conn(builder, land.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE price > 30 OR price < 10
TEST_F(WhereTest, LogicalOr) {
  auto where_expr = logical("OR", cmp(">", col("price"), num(30.0)),
                            cmp("<", col("price"), num(10.0)));
  auto result = compile_where(std::move(where_expr), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 6u);
  auto& ext1 = builder.operators()[0];
  auto& cmp_gt = builder.operators()[1];
  auto& ext2 = builder.operators()[2];
  auto& cmp_lt = builder.operators()[3];
  auto& lor = builder.operators()[4];
  auto& demux = builder.operators()[5];
  EXPECT_EQ(lor.type, "LogicalOr");
  EXPECT_EQ(demux.type, "Demultiplexer");

  ASSERT_EQ(builder.connections().size(), 8u);
  expect_conn(builder, "input_0", "o1", ext1.id, "i1");
  expect_conn(builder, ext1.id, "o1", cmp_gt.id, "i1");
  expect_conn(builder, "input_0", "o1", ext2.id, "i1");
  expect_conn(builder, ext2.id, "o1", cmp_lt.id, "i1");
  expect_conn(builder, cmp_gt.id, "o1", lor.id, "i1");
  expect_conn(builder, cmp_lt.id, "o1", lor.id, "i2");
  expect_conn(builder, lor.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE NOT price > 30 → CompareLTE(30) + Demultiplexer (NOT optimized away)
TEST_F(WhereTest, NotOptimization) {
  auto where_expr = not_expr(cmp(">", col("price"), num(30.0)));
  auto result = compile_where(std::move(where_expr), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 3u);
  auto& ext = builder.operators()[0];
  auto& cmpop = builder.operators()[1];
  auto& demux = builder.operators()[2];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(cmpop.type, "CompareLTE");
  EXPECT_EQ(cmpop.params.at("value"), 30.0);
  EXPECT_EQ(demux.type, "Demultiplexer");

  ASSERT_EQ(builder.connections().size(), 4u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", cmpop.id, "i1");
  expect_conn(builder, cmpop.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE price BETWEEN 10 AND 50 →
//   VectorExtract → CompareGTE(10) + CompareLTE(50) → LogicalAnd → Demux
TEST_F(WhereTest, BetweenDesugaring) {
  auto where_expr = between(col("price"), num(10.0), num(50.0));
  auto result = compile_where(std::move(where_expr), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 5u);
  auto& ext = builder.operators()[0];
  auto& gte = builder.operators()[1];
  auto& lte = builder.operators()[2];
  auto& land = builder.operators()[3];
  auto& demux = builder.operators()[4];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(gte.type, "CompareGTE");
  EXPECT_EQ(gte.params.at("value"), 10.0);
  EXPECT_EQ(lte.type, "CompareLTE");
  EXPECT_EQ(lte.params.at("value"), 50.0);
  EXPECT_EQ(land.type, "LogicalAnd");
  EXPECT_EQ(demux.type, "Demultiplexer");

  ASSERT_EQ(builder.connections().size(), 7u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", gte.id, "i1");
  expect_conn(builder, ext.id, "o1", lte.id, "i1");
  expect_conn(builder, gte.id, "o1", land.id, "i1");
  expect_conn(builder, lte.id, "o1", land.id, "i2");
  expect_conn(builder, land.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// compile_predicate returns boolean endpoint (no Demux)
TEST_F(WhereTest, PredicateWithoutDemux) {
  auto pred = cmp("<", col("quantity"), num(100.0));
  auto ep = compile_predicate(std::move(pred), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 2u);
  auto& ext = builder.operators()[0];
  auto& cmpop = builder.operators()[1];
  EXPECT_EQ(ext.type, "VectorExtract");
  EXPECT_EQ(cmpop.type, "CompareLT");
  EXPECT_EQ(ep.operator_id, cmpop.id);
  EXPECT_EQ(ep.port, "o1");

  ASSERT_EQ(builder.connections().size(), 2u);
  expect_conn(builder, "input_0", "o1", ext.id, "i1");
  expect_conn(builder, ext.id, "o1", cmpop.id, "i1");
}

// WHERE price > quantity → two VectorExtract + CompareSyncGT + Demultiplexer
TEST_F(WhereTest, SameStreamComparisonGT) {
  auto result =
      compile_where(cmp(">", col("price"), col("quantity")), input, scope, builder);

  ASSERT_EQ(builder.operators().size(), 4u);
  auto& ext_p = builder.operators()[0];
  auto& ext_q = builder.operators()[1];
  auto& cmpop = builder.operators()[2];
  auto& demux = builder.operators()[3];
  EXPECT_EQ(ext_p.type, "VectorExtract");
  EXPECT_EQ(ext_p.params.at("index"), 1.0);
  EXPECT_EQ(ext_q.type, "VectorExtract");
  EXPECT_EQ(ext_q.params.at("index"), 2.0);
  EXPECT_EQ(cmpop.type, "CompareSyncGT");
  EXPECT_EQ(demux.type, "Demultiplexer");
  EXPECT_EQ(result.operator_id, demux.id);

  ASSERT_EQ(builder.connections().size(), 6u);
  expect_conn(builder, "input_0", "o1", ext_p.id, "i1");
  expect_conn(builder, "input_0", "o1", ext_q.id, "i1");
  expect_conn(builder, ext_p.id, "o1", cmpop.id, "i1");
  expect_conn(builder, ext_q.id, "o1", cmpop.id, "i2");
  expect_conn(builder, cmpop.id, "o1", demux.id, "c1");
  expect_conn(builder, "input_0", "o1", demux.id, "i1");
}

// WHERE price <= quantity (stream OP stream, ≤)
TEST_F(WhereTest, SameStreamComparisonLTE) {
  auto ep = compile_predicate(cmp("<=", col("price"), col("quantity")),
                              input, scope, builder);

  bool has_sync_lte = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "CompareSyncLTE") has_sync_lte = true;
  }
  EXPECT_TRUE(has_sync_lte);
}

}  // namespace
}  // namespace rtbot_sql::compiler
