#include "rtbot_sql/analyzer/scope.h"

#include <gtest/gtest.h>

namespace rtbot_sql::analyzer {
namespace {

StreamSchema make_trades_schema() {
  return StreamSchema{
      "trades",
      {{"instrument_id", 0}, {"price", 1}, {"quantity", 2}, {"account_id", 3}},
  };
}

StreamSchema make_orders_schema() {
  return StreamSchema{
      "orders",
      {{"order_id", 0}, {"price", 1}, {"side", 2}},
  };
}

TEST(Scope, ResolvesUnqualifiedColumn) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema());

  parser::ast::ColumnRef ref{"", "price"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result));
  auto& binding = std::get<ColumnBinding>(result);
  EXPECT_EQ(binding.stream_name, "trades");
  EXPECT_EQ(binding.index, 1);
}

TEST(Scope, ResolvesQualifiedByStreamName) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema());

  parser::ast::ColumnRef ref{"trades", "quantity"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result));
  auto& binding = std::get<ColumnBinding>(result);
  EXPECT_EQ(binding.stream_name, "trades");
  EXPECT_EQ(binding.index, 2);
}

TEST(Scope, ResolvesQualifiedByAlias) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema(), "t");

  parser::ast::ColumnRef ref{"t", "price"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result));
  auto& binding = std::get<ColumnBinding>(result);
  EXPECT_EQ(binding.stream_name, "trades");
  EXPECT_EQ(binding.index, 1);
}

TEST(Scope, StreamNameStillWorksWithAlias) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema(), "t");

  parser::ast::ColumnRef ref{"trades", "price"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result));
}

TEST(Scope, UnknownColumnReturnsError) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema());

  parser::ast::ColumnRef ref{"", "nonexistent"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<std::string>(result));
  EXPECT_NE(std::get<std::string>(result).find("unknown"), std::string::npos);
}

TEST(Scope, AmbiguousColumnReturnsError) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema());
  scope.register_stream("orders", make_orders_schema());

  // "price" exists in both streams
  parser::ast::ColumnRef ref{"", "price"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<std::string>(result));
  EXPECT_NE(std::get<std::string>(result).find("ambiguous"), std::string::npos);
}

TEST(Scope, AmbiguousResolvedByQualifier) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema(), "t");
  scope.register_stream("orders", make_orders_schema(), "o");

  parser::ast::ColumnRef ref_t{"t", "price"};
  auto result_t = scope.resolve(ref_t);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result_t));
  EXPECT_EQ(std::get<ColumnBinding>(result_t).stream_name, "trades");
  EXPECT_EQ(std::get<ColumnBinding>(result_t).index, 1);

  parser::ast::ColumnRef ref_o{"o", "price"};
  auto result_o = scope.resolve(ref_o);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result_o));
  EXPECT_EQ(std::get<ColumnBinding>(result_o).stream_name, "orders");
  EXPECT_EQ(std::get<ColumnBinding>(result_o).index, 1);
}

TEST(Scope, NonOverlappingColumnsNotAmbiguous) {
  Scope scope;
  scope.register_stream("trades", make_trades_schema());
  scope.register_stream("orders", make_orders_schema());

  // "quantity" only in trades
  parser::ast::ColumnRef ref{"", "quantity"};
  auto result = scope.resolve(ref);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result));
  EXPECT_EQ(std::get<ColumnBinding>(result).index, 2);

  // "order_id" only in orders
  parser::ast::ColumnRef ref2{"", "order_id"};
  auto result2 = scope.resolve(ref2);
  ASSERT_TRUE(std::holds_alternative<ColumnBinding>(result2));
  EXPECT_EQ(std::get<ColumnBinding>(result2).index, 0);
}

}  // namespace
}  // namespace rtbot_sql::analyzer
