#include "rtbot_sql/compiler/alias_expander.h"

#include <gtest/gtest.h>

namespace rtbot_sql::compiler {
namespace {

using namespace parser::ast;

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

Expr func(const std::string& name, std::vector<Expr> args) {
  auto e = std::make_unique<FuncCall>();
  e->name = name;
  e->args = std::move(args);
  return e;
}

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return SelectItem{std::move(expr), std::move(alias)};
}

// ─── deep_clone tests ────────────────────────────────────────────────────────

TEST(DeepClone, CloneColumnRef) {
  Expr src = col("price");
  Expr cloned = deep_clone(src);
  const auto* c = std::get_if<ColumnRef>(&cloned);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(c->column_name, "price");
}

TEST(DeepClone, CloneConstant) {
  Expr src = num(42.0);
  Expr cloned = deep_clone(src);
  const auto* c = std::get_if<Constant>(&cloned);
  ASSERT_NE(c, nullptr);
  EXPECT_DOUBLE_EQ(c->value, 42.0);
}

TEST(DeepClone, CloneBinaryExpr) {
  Expr src = binary("*", col("price"), num(2.0));
  Expr cloned = deep_clone(src);
  const auto* b = std::get_if<std::unique_ptr<BinaryExpr>>(&cloned);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ((*b)->op, "*");
  const auto* lhs = std::get_if<ColumnRef>(&(*b)->left);
  ASSERT_NE(lhs, nullptr);
  EXPECT_EQ(lhs->column_name, "price");
  const auto* rhs = std::get_if<Constant>(&(*b)->right);
  ASSERT_NE(rhs, nullptr);
  EXPECT_DOUBLE_EQ(rhs->value, 2.0);
  // Pointers must be distinct (deep copy, not shallow)
  const auto* src_b = std::get_if<std::unique_ptr<BinaryExpr>>(&src);
  EXPECT_NE(b->get(), src_b->get());
}

TEST(DeepClone, CloneFuncCall) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(20.0));
  Expr src = func("MOVING_AVERAGE", std::move(args));
  Expr cloned = deep_clone(src);
  const auto* f = std::get_if<std::unique_ptr<FuncCall>>(&cloned);
  ASSERT_NE(f, nullptr);
  EXPECT_EQ((*f)->name, "MOVING_AVERAGE");
  ASSERT_EQ((*f)->args.size(), 2u);
  const auto* arg0 = std::get_if<ColumnRef>(&(*f)->args[0]);
  ASSERT_NE(arg0, nullptr);
  EXPECT_EQ(arg0->column_name, "price");
}

// ─── build_alias_map tests ───────────────────────────────────────────────────

TEST(BuildAliasMap, EmptySelectList) {
  std::vector<SelectItem> items;
  auto alias_map = build_alias_map(items);
  EXPECT_TRUE(alias_map.empty());
}

TEST(BuildAliasMap, SimpleAlias) {
  // SELECT 2 * price AS dp
  std::vector<SelectItem> items;
  items.push_back(item(binary("*", num(2.0), col("price")), "dp"));
  auto alias_map = build_alias_map(items);
  ASSERT_EQ(alias_map.size(), 1u);
  EXPECT_NE(alias_map.find("dp"), alias_map.end());
  const auto* b = std::get_if<std::unique_ptr<BinaryExpr>>(&alias_map["dp"]);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ((*b)->op, "*");
}

TEST(BuildAliasMap, AliasChain) {
  // SELECT 2*price AS dp, AVG(dp) AS avg_dp
  std::vector<SelectItem> items;
  items.push_back(item(binary("*", num(2.0), col("price")), "dp"));
  std::vector<Expr> avg_args;
  avg_args.push_back(col("dp"));
  items.push_back(item(func("AVG", std::move(avg_args)), "avg_dp"));
  auto alias_map = build_alias_map(items);
  ASSERT_EQ(alias_map.size(), 2u);
  // avg_dp should expand dp → 2*price inside AVG args
  const auto* f =
      std::get_if<std::unique_ptr<FuncCall>>(&alias_map["avg_dp"]);
  ASSERT_NE(f, nullptr);
  EXPECT_EQ((*f)->name, "AVG");
  ASSERT_EQ((*f)->args.size(), 1u);
  // The arg should have been expanded to BinaryExpr (2*price), not ColumnRef(dp)
  const auto* arg_bin =
      std::get_if<std::unique_ptr<BinaryExpr>>(&(*f)->args[0]);
  EXPECT_NE(arg_bin, nullptr)
      << "AVG argument should have been expanded from alias 'dp' to 2*price";
}

TEST(BuildAliasMap, NoAliasItems) {
  // Items without AS aliases are ignored
  std::vector<SelectItem> items;
  items.push_back(item(col("price")));          // no alias
  items.push_back(item(col("quantity")));       // no alias
  items.push_back(item(col("instrument_id"), "iid"));  // has alias
  auto alias_map = build_alias_map(items);
  ASSERT_EQ(alias_map.size(), 1u);
  EXPECT_NE(alias_map.find("iid"), alias_map.end());
}

// ─── expand_aliases tests ────────────────────────────────────────────────────

TEST(ExpandAliases, NoAliasInExpr) {
  AliasMap alias_map;
  alias_map["dp"] = binary("*", num(2.0), col("price"));
  // Expression references a column that is NOT an alias
  Expr expr = col("quantity");
  Expr result = expand_aliases(expr, alias_map);
  const auto* c = std::get_if<ColumnRef>(&result);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(c->column_name, "quantity");
}

TEST(ExpandAliases, DirectSubstitution) {
  AliasMap alias_map;
  alias_map["dp"] = binary("*", num(2.0), col("price"));
  // ColumnRef "dp" should be replaced with 2*price
  Expr expr = col("dp");
  Expr result = expand_aliases(expr, alias_map);
  const auto* b = std::get_if<std::unique_ptr<BinaryExpr>>(&result);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ((*b)->op, "*");
}

TEST(ExpandAliases, NestedSubstitution) {
  AliasMap alias_map;
  alias_map["dp"] = binary("*", num(2.0), col("price"));
  // ComparisonExpr: dp > 100 → (2*price) > 100
  Expr expr = cmp(">", col("dp"), num(100.0));
  Expr result = expand_aliases(expr, alias_map);
  const auto* c = std::get_if<std::unique_ptr<ComparisonExpr>>(&result);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ((*c)->op, ">");
  // Left should now be BinaryExpr (2*price)
  const auto* lhs =
      std::get_if<std::unique_ptr<BinaryExpr>>(&(*c)->left);
  EXPECT_NE(lhs, nullptr) << "left side should be expanded to 2*price";
  // Right should still be constant 100
  const auto* rhs = std::get_if<Constant>(&(*c)->right);
  ASSERT_NE(rhs, nullptr);
  EXPECT_DOUBLE_EQ(rhs->value, 100.0);
}

TEST(ExpandAliases, ChainedSubstitution) {
  // dp = 2*price, avg_dp = AVG(dp) → AVG(2*price)
  AliasMap alias_map;
  alias_map["dp"] = binary("*", num(2.0), col("price"));
  // AVG(dp) — dp is an alias
  std::vector<Expr> avg_args;
  avg_args.push_back(col("dp"));
  Expr expr = func("AVG", std::move(avg_args));
  Expr result = expand_aliases(expr, alias_map);
  const auto* f = std::get_if<std::unique_ptr<FuncCall>>(&result);
  ASSERT_NE(f, nullptr);
  EXPECT_EQ((*f)->name, "AVG");
  ASSERT_EQ((*f)->args.size(), 1u);
  const auto* arg_bin =
      std::get_if<std::unique_ptr<BinaryExpr>>(&(*f)->args[0]);
  EXPECT_NE(arg_bin, nullptr) << "AVG arg should be expanded from dp to 2*price";
}

// ─── expr_has_aggregate tests ────────────────────────────────────────────────

TEST(ExprHasAggregate, ReturnsTrueForSum) {
  std::vector<Expr> args;
  args.push_back(col("price"));
  Expr expr = func("SUM", std::move(args));
  EXPECT_TRUE(expr_has_aggregate(expr));
}

TEST(ExprHasAggregate, ReturnsTrueNested) {
  // BinaryExpr containing AVG(price) → should return true
  std::vector<Expr> avg_args;
  avg_args.push_back(col("price"));
  Expr expr = binary("+", func("AVG", std::move(avg_args)), num(1.0));
  EXPECT_TRUE(expr_has_aggregate(expr));
}

TEST(ExprHasAggregate, ReturnsFalseForColumn) {
  Expr expr = col("price");
  EXPECT_FALSE(expr_has_aggregate(expr));
}

TEST(ExprHasAggregate, ReturnsFalseForMath) {
  Expr expr = binary("*", col("price"), num(2.0));
  EXPECT_FALSE(expr_has_aggregate(expr));
}

TEST(ExprHasAggregate, AllAggregateNames) {
  for (const auto& name : {"sum", "count", "avg", "min", "max",
                            "SUM", "COUNT", "AVG", "MIN", "MAX"}) {
    std::vector<Expr> args;
    args.push_back(col("x"));
    Expr expr = func(name, std::move(args));
    EXPECT_TRUE(expr_has_aggregate(expr)) << "should detect aggregate: " << name;
  }
}

TEST(ExprHasAggregate, NonAggregateFunc) {
  // MOVING_AVERAGE is not an aggregate in the SQL sense (window function)
  std::vector<Expr> args;
  args.push_back(col("price"));
  args.push_back(num(20.0));
  Expr expr = func("MOVING_AVERAGE", std::move(args));
  EXPECT_FALSE(expr_has_aggregate(expr));
}

}  // namespace
}  // namespace rtbot_sql::compiler
