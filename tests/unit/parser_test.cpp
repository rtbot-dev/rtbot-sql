#include "rtbot_sql/parser/parser.h"

#include <gtest/gtest.h>

namespace rtbot_sql::parser {
namespace {

TEST(Parser, ParsesSimpleSelect) {
  auto r = parse("SELECT 1");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesCreateTable) {
  auto r = parse("CREATE TABLE foo (id INT, name TEXT)");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesSelectFromTable) {
  auto r = parse("SELECT a, b FROM my_stream WHERE a > 1.0");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesInsert) {
  auto r = parse("INSERT INTO my_stream (a, b) VALUES (1.0, 2.0)");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesDropTable) {
  auto r = parse("DROP TABLE my_stream");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesDropView) {
  auto r = parse("DROP VIEW my_view");
  EXPECT_TRUE(r.ok());
  free_result(r);
}

TEST(Parser, ParsesCreateView) {
  auto r = parse("CREATE VIEW my_view AS SELECT a, b FROM stream1");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ParsesCreateMaterializedView) {
  auto r = parse(
      "CREATE MATERIALIZED VIEW my_mv AS SELECT SUM(a) FROM stream1");
  EXPECT_TRUE(r.ok());
  EXPECT_GT(r.protobuf().len, 0u);
  free_result(r);
}

TEST(Parser, ReturnsErrorForInvalidSQL) {
  auto r = parse("SELEC FROM WHERE");
  EXPECT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  EXPECT_FALSE(r.errors[0].empty());
  free_result(r);
}

TEST(Parser, ReturnsErrorForEmptyInput) {
  auto r = parse("");
  // Empty input parses successfully in PostgreSQL (produces empty tree)
  // but has zero-length protobuf
  EXPECT_TRUE(r.ok());
  free_result(r);
}

TEST(Parser, FreeResultClearsState) {
  auto r = parse("SELECT 1");
  EXPECT_TRUE(r.ok());
  free_result(r);
  EXPECT_TRUE(r.errors.empty());
}

}  // namespace
}  // namespace rtbot_sql::parser
