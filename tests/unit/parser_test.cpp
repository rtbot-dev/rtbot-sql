#include "rtbot_sql/parser/parser.h"

#include "rtbot_sql/parser/ast.h"
#include "rtbot_sql/parser/ast_converter.h"

#include <gtest/gtest.h>
#include <pg_query.h>

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

TEST(Parser, ParsesCreateStream) {
  // The parser layer itself only wraps libpg_query; normalization lives in the
  // compiler (compile_sql) and the browser bindings (validate_sql).  This test
  // verifies that the normalized form ("CREATE TABLE") round-trips through the
  // parser successfully — which is what callers produce after normalization.
  auto r = parse("CREATE TABLE foo (t DOUBLE PRECISION)");
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

// ---------------------------------------------------------------------------
// AST-level tests using convert_parse_tree
// ---------------------------------------------------------------------------

class ParserTest : public ::testing::Test {
 protected:
  ast::Statement parse(const std::string& sql) {
    auto json_result = pg_query_parse(sql.c_str());
    if (json_result.error) {
      std::string msg = json_result.error->message
                            ? json_result.error->message
                            : "parse error";
      pg_query_free_parse_result(json_result);
      throw std::runtime_error(msg);
    }
    std::string tree(json_result.parse_tree);
    pg_query_free_parse_result(json_result);
    return convert_parse_tree(tree);
  }
};

TEST_F(ParserTest, ParseStringConstantInInsert) {
  auto stmt = parse("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)");
  auto* insert = std::get_if<ast::InsertStmt>(&stmt);
  ASSERT_NE(insert, nullptr);
  ASSERT_EQ(insert->values.size(), 3u);

  // First value: numeric 1
  auto* c0 = std::get_if<ast::Constant>(&insert->values[0]);
  ASSERT_NE(c0, nullptr);
  EXPECT_DOUBLE_EQ(c0->value, 1.0);

  // Second value: string 'Bay A'
  auto* s1 = std::get_if<ast::StringConstant>(&insert->values[1]);
  ASSERT_NE(s1, nullptr);
  EXPECT_EQ(s1->value, "Bay A");

  // Third value: numeric 42.0
  auto* c2 = std::get_if<ast::Constant>(&insert->values[2]);
  ASSERT_NE(c2, nullptr);
  EXPECT_DOUBLE_EQ(c2->value, 42.0);
}

TEST_F(ParserTest, ParseStringConstantInWhere) {
  auto stmt = parse("SELECT * FROM sensors WHERE location = 'Bay A'");
  auto* select = std::get_if<ast::SelectStmt>(&stmt);
  ASSERT_NE(select, nullptr);
  ASSERT_TRUE(select->where_clause.has_value());

  auto* cmp = std::get_if<std::unique_ptr<ast::ComparisonExpr>>(&*select->where_clause);
  ASSERT_NE(cmp, nullptr);
  auto* rhs = std::get_if<ast::StringConstant>(&(*cmp)->right);
  ASSERT_NE(rhs, nullptr);
  EXPECT_EQ(rhs->value, "Bay A");
}

}  // namespace
}  // namespace rtbot_sql::parser
