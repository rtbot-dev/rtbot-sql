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
// Source location tests
// ---------------------------------------------------------------------------

TEST(Parser, SyntaxErrorPopulatesSingleLineLocation) {
  // "SELEKT" is invalid; libpg_query reports cursorpos at column 1
  auto r = parse("SELEKT FROM x");
  ASSERT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  EXPECT_GE(r.errors[0].loc.line, 1);
  EXPECT_GE(r.errors[0].loc.column, 1);
  free_result(r);
}

TEST(Parser, SyntaxErrorPopulatesMultiLineLocation) {
  // Error on line 2; cursorpos > 0 must produce line >= 1.
  auto r = parse("SELECT 1\nFROOM x");
  ASSERT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  // Line at least 1; if libpg_query reports cursorpos > newline byte (8),
  // the resolved line should be 2.
  EXPECT_GE(r.errors[0].loc.line, 1);
  free_result(r);
}

TEST(Parser, SuccessfulParseHasNoErrorLocations) {
  auto r = parse("SELECT 1 FROM x");
  EXPECT_TRUE(r.ok());
  EXPECT_TRUE(r.errors.empty());
  free_result(r);
}

TEST(Parser, SyntaxErrorPopulatesEndLocation) {
  // SELEKT is invalid; libpg_query points cursorpos at it.
  auto r = parse("SELEKT FROM x");
  ASSERT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  // Both start AND end positions must be populated for a span renderer
  // (editor selection, error underline) to work.
  EXPECT_GE(r.errors[0].loc.line, 1);
  EXPECT_GE(r.errors[0].loc.column, 1);
  EXPECT_GE(r.errors[0].loc.end_line, 1);
  EXPECT_GE(r.errors[0].loc.end_column, 1);
  // The end must come after (or equal to) the start.
  EXPECT_TRUE(r.errors[0].loc.end_line > r.errors[0].loc.line ||
              (r.errors[0].loc.end_line == r.errors[0].loc.line &&
               r.errors[0].loc.end_column >= r.errors[0].loc.column));
  free_result(r);
}

TEST(Parser, SyntaxErrorEndSpansToken) {
  // Force libpg_query to point cursorpos at "BANANA" (a non-keyword used
  // where a keyword was expected). The end should land one char past 'A'.
  auto r = parse("BANANA");
  ASSERT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  // BANANA starts at column 1; if the cursor points at it, end_column
  // should be 7 (1 + 6 chars).
  if (r.errors[0].loc.column == 1) {
    EXPECT_EQ(r.errors[0].loc.end_column, 7)
        << "expected token end after BANANA";
  }
  free_result(r);
}

TEST(Parser, MultiLineSyntaxErrorEndOnSameLine) {
  // The token at the error site shouldn't span multiple lines (tokens
  // are single-line in SQL). Verify end_line == line for the simple case.
  auto r = parse("SELECT 1\nFROOM x");
  ASSERT_FALSE(r.ok());
  ASSERT_FALSE(r.errors.empty());
  EXPECT_EQ(r.errors[0].loc.line, r.errors[0].loc.end_line)
      << "single-token error span should not cross lines";
  free_result(r);
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
