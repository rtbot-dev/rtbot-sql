#include "rtbot_sql/compiler/expr_cache.h"

#include <gtest/gtest.h>

#include "rtbot_sql/compiler/expression_compiler.h"

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

class DedupTest : public ::testing::Test {
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

// Canonicalization produces consistent keys
TEST_F(DedupTest, CanonicalizeConsistentKeys) {
  auto key1 = canonicalize(func_expr("COUNT", {}));
  auto key2 = canonicalize(func_expr("COUNT", {}));
  EXPECT_EQ(key1, key2);

  // Case insensitive
  auto key3 = canonicalize(func_expr("count", {}));
  EXPECT_EQ(key1, key3);

  // Different functions → different keys
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  auto key4 = canonicalize(func_expr("SUM", std::move(sum_args)));
  EXPECT_NE(key1, key4);
}

// MOVING_AVERAGE(price, 20) compiled once, cache hit on second lookup
TEST_F(DedupTest, SharedMovingAverageCompilesOnce) {
  ExprCache cache;

  // First compilation
  std::vector<Expr> ma_args1;
  ma_args1.push_back(col("price"));
  ma_args1.push_back(num(20));
  Expr ma1 = func_expr("MOVING_AVERAGE", std::move(ma_args1));

  auto result1 = compile_expression(ma1, input, scope, builder);
  ASSERT_TRUE(std::holds_alternative<Endpoint>(result1));
  cache.store(ma1, std::get<Endpoint>(result1));

  size_t ops_after_first = builder.operators().size();

  // Second lookup — same canonical form
  std::vector<Expr> ma_args2;
  ma_args2.push_back(col("price"));
  ma_args2.push_back(num(20));
  Expr ma2 = func_expr("MOVING_AVERAGE", std::move(ma_args2));

  const Endpoint* cached = cache.lookup(ma2);
  ASSERT_NE(cached, nullptr);

  // No new operators added
  EXPECT_EQ(builder.operators().size(), ops_after_first);

  // Same endpoint
  EXPECT_EQ(cached->operator_id, std::get<Endpoint>(result1).operator_id);
  EXPECT_EQ(cached->port, std::get<Endpoint>(result1).port);
}

// ColumnRef canonicalization
TEST_F(DedupTest, CanonicalizeColumnRef) {
  auto key1 = canonicalize(col("price"));
  auto key2 = canonicalize(col("price"));
  EXPECT_EQ(key1, key2);

  auto key3 = canonicalize(col("quantity"));
  EXPECT_NE(key1, key3);
}

}  // namespace
}  // namespace rtbot_sql::compiler
