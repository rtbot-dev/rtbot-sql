// End-to-end tests for the semantic analyzer (libs/analyzer).
//
// These tests drive `compile_sql` so they cover the full pipeline:
//   parse → AST conversion (with locations) → analyze_statement → compile.
//
// They verify that semantic errors:
//   * are surfaced via `result.errors` (not as throws),
//   * carry source `(line, column)` populated by libpg_query's location
//     fields and the analyzer's diagnostic bag,
//   * accumulate in a single compile pass when multiple problems exist.

#include "rtbot_sql/api/compiler.h"

#include <pg_query.h>

#include <gtest/gtest.h>

#include <regex>

#include "rtbot_sql/analyzer/analyzer.h"
#include "rtbot_sql/parser/ast_converter.h"
#include "rtbot_sql/parser/parser.h"

namespace rtbot_sql::api {
namespace {

// Helper: build a small catalog with one stream.
CatalogSnapshot stream_catalog() {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "memory_usage";
  schema.columns = {
      {"memory", 0, ColumnType::DOUBLE},
      {"cpu", 1, ColumnType::DOUBLE},
  };
  catalog.streams["memory_usage"] = schema;
  return catalog;
}

// ---------------------------------------------------------------------------
// Function arity (commit 4)
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, MovingSumWithWrongArityProducesLocatedError) {
  auto catalog = stream_catalog();
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS\n"
      "SELECT MOVING_SUM(memory) AS s FROM memory_usage",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("MOVING_SUM requires 2 arguments"),
            std::string::npos);
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

TEST(AnalyzerTest, UnknownFunctionProducesLocatedError) {
  auto catalog = stream_catalog();
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT FOOBAR(memory) FROM memory_usage",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("unknown function"), std::string::npos);
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

TEST(AnalyzerTest, MovingSumNonPositiveWindowReports) {
  auto catalog = stream_catalog();
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_SUM(memory, 0) AS s FROM memory_usage",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("must be a positive integer"),
            std::string::npos);
}

// ---------------------------------------------------------------------------
// Expression shape (commit 5)
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, AggregateInWhereClauseProducesLocatedError) {
  auto catalog = stream_catalog();
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory FROM memory_usage WHERE SUM(memory) > 100",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find(
                "aggregate function not allowed in WHERE"),
            std::string::npos);
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

// ---------------------------------------------------------------------------
// Aggregate-in-WHERE through SELECT-list alias expansion
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, AggregateAliasInWhereRejected) {
  auto catalog = stream_catalog();
  // SELECT SUM(memory) AS s ... WHERE s > 0 — alias `s` resolves to
  // SUM(memory), so WHERE effectively contains an aggregate.
  auto r = compile_sql(
      "SELECT SUM(memory) AS s FROM memory_usage WHERE s > 0 LIMIT 10",
      catalog);
  ASSERT_TRUE(r.has_errors());
  bool found = false;
  for (const auto& e : r.errors) {
    if (e.message.find("aggregate function not allowed in WHERE") !=
        std::string::npos) {
      found = true;
      EXPECT_GE(e.line, 1);
      EXPECT_GE(e.column, 1);
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST(AnalyzerTest, AggregateAliasChainInWhereRejected) {
  auto catalog = stream_catalog();
  // Alias chain: t = s + 1, s = SUM(memory). WHERE references t.
  auto r = compile_sql(
      "SELECT SUM(memory) AS s, s + 1 AS t FROM memory_usage WHERE t > 0 LIMIT 10",
      catalog);
  ASSERT_TRUE(r.has_errors());
}

TEST(AnalyzerTest, NonAggregateAliasInWhereOk) {
  auto catalog = stream_catalog();
  // Alias resolves to a plain column expression — no aggregate, should pass.
  auto r = compile_sql(
      "SELECT memory * 2 AS doubled FROM memory_usage WHERE doubled > 0 LIMIT 10",
      catalog);
  EXPECT_FALSE(r.has_errors());
}

// ---------------------------------------------------------------------------
// Source resolution (commit 8)
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, UnknownSourceProducesLocatedError) {
  CatalogSnapshot empty;
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT * FROM nonexistent_stream", empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("unknown source"), std::string::npos);
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

// ---------------------------------------------------------------------------
// INSERT validation (commit 7)
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, InsertUnknownTableProducesLocatedError) {
  CatalogSnapshot empty;
  auto r = compile_sql("INSERT INTO mystery VALUES (1)", empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("INSERT: unknown stream or table"),
            std::string::npos);
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

TEST(AnalyzerTest, InsertValueCountMismatchProducesLocatedError) {
  auto catalog = stream_catalog();
  auto r = compile_sql("INSERT INTO memory_usage VALUES (1.0)", catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("value count mismatch"),
            std::string::npos);
}

// ---------------------------------------------------------------------------
// Syntax errors (commit 2) — confirms the parser path also carries locations
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, SyntaxErrorCarriesLocation) {
  CatalogSnapshot empty;
  auto r = compile_sql("SELEKT FROM x", empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_GE(r.errors[0].line, 1);
  EXPECT_GE(r.errors[0].column, 1);
}

// AST converter throws — these are language-feature gaps surfaced at AST
// conversion time. Until this work, they came through as "(-1, -1)"
// errors via make_error(e.what()). They now carry source spans.

TEST(AnalyzerTest, BetweenErrorCarriesLocation) {
  CatalogSnapshot empty;
  // BETWEEN is rejected at AST conversion (not yet supported).
  auto r = compile_sql(
      "SELECT 1 FROM x WHERE x BETWEEN 1 AND 10 LIMIT 1", empty);
  ASSERT_TRUE(r.has_errors());
  // Find the BETWEEN error.
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find("BETWEEN") != std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr);
  EXPECT_GE(match->line, 1);
  EXPECT_GE(match->column, 1);
  EXPECT_GE(match->end_line, 1);
  EXPECT_GE(match->end_column, 1);
}

TEST(AnalyzerTest, ArrayElementMustBeNumericConstantCarriesLocation) {
  // FIR's second arg requires ARRAY of numerics. Passing a string element
  // triggers the converter's "ARRAY elements must be numeric constants".
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "t";
  schema.columns = {{"x", 0, ColumnType::DOUBLE}};
  c.streams["t"] = schema;
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS SELECT FIR(x, ARRAY['a', 'b']) FROM t",
      c);
  ASSERT_TRUE(r.has_errors());
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find("ARRAY elements must be numeric constants") !=
        std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr);
  EXPECT_GE(match->line, 1);
  EXPECT_GE(match->column, 1);
  EXPECT_GE(match->end_line, 1);
  EXPECT_GE(match->end_column, 1);
}

TEST(AnalyzerTest, UnsupportedStatementTypeCarriesLocation) {
  CatalogSnapshot empty;
  // UPDATE isn't supported by the converter — should surface with location.
  auto r = compile_sql("UPDATE t SET x = 1", empty);
  ASSERT_TRUE(r.has_errors());
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find("unsupported statement type") != std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr);
  EXPECT_GE(match->line, 1);
  EXPECT_GE(match->column, 1);
}

TEST(AnalyzerTest, SyntaxErrorCarriesEndLocation) {
  // Verify the syntax-error path through compile_sql populates end_line
  // and end_column too — an editor consuming the diagnostic needs the
  // full span to highlight the offending token.
  CatalogSnapshot empty;
  auto r = compile_sql("SELEKT FROM x", empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_GE(r.errors[0].end_line, 1);
  EXPECT_GE(r.errors[0].end_column, 1);
  // End must come after start (token has at least 1 char).
  EXPECT_TRUE(r.errors[0].end_line > r.errors[0].line ||
              (r.errors[0].end_line == r.errors[0].line &&
               r.errors[0].end_column > r.errors[0].column));
}

// ---------------------------------------------------------------------------
// Multi-error: collect-all behavior — multiple semantic problems in a single
// compile pass should each surface as a separate diagnostic with its own
// location.
// ---------------------------------------------------------------------------

TEST(AnalyzerTest, MultipleErrorsAccumulateInSingleCompile) {
  auto catalog = stream_catalog();
  // Two problems: bad MOVING_SUM arity, AND an unknown function.
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_SUM(memory) AS a, FOOBAR(memory) AS b FROM memory_usage",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_GE(r.errors.size(), 2u)
      << "expected analyzer to surface both errors at once; got "
      << r.errors.size();
}

// ---------------------------------------------------------------------------
// Location-accuracy tests
//
// Verify that the (line, column) on each error actually points at the
// offending token. Catches regressions where a converter forgets to
// populate `loc` on a node or where libpg_query's location semantics
// change. These compute expected offsets from the SQL string so they
// don't hard-code magic numbers.
// ---------------------------------------------------------------------------

// Mirror compile_sql's normalize_sql: rewrite CREATE STREAM → CREATE TABLE
// and DROP STREAM → DROP TABLE so byte offsets in the test SQL match what
// the parser actually saw. Without this, span extraction from the original
// SQL is off-by-N for any input that contains STREAM keywords.
inline std::string normalize_for_span(const std::string& sql) {
  static const std::regex kCreateStream(R"(\bCREATE\s+STREAM\b)",
                                        std::regex::icase);
  static const std::regex kDropStream(R"(\bDROP\s+STREAM\b)",
                                      std::regex::icase);
  std::string out = std::regex_replace(sql, kCreateStream, "CREATE TABLE");
  out = std::regex_replace(out, kDropStream, "DROP TABLE");
  return out;
}

// Convert a 0-based byte offset in `sql` into a 1-based (line, column).
// Mirrors parser::compute_location's semantics so test expectations match
// what the analyzer produces.
struct ExpectedLoc { int line; int column; };
ExpectedLoc loc_at(const std::string& sql, std::size_t byte_offset) {
  int line = 1;
  int col = 1;
  for (std::size_t i = 0; i < byte_offset && i < sql.size(); ++i) {
    if (sql[i] == '\n') {
      ++line;
      col = 1;
    } else {
      ++col;
    }
  }
  return {line, col};
}

TEST(AnalyzerLocationTest, FunctionArityErrorPointsAtFunctionName) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT MOVING_SUM(memory) FROM memory_usage";
  auto pos = sql.find("MOVING_SUM");
  ASSERT_NE(pos, std::string::npos);
  auto expected = loc_at(sql, pos);

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].line, expected.line);
  EXPECT_EQ(r.errors[0].column, expected.column);
}

TEST(AnalyzerLocationTest, MultiLineSqlReportsCorrectLine) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS\n"
      "SELECT memory,\n"
      "       MOVING_SUM(memory)\n"
      "FROM memory_usage";
  auto pos = sql.find("MOVING_SUM");
  ASSERT_NE(pos, std::string::npos);
  auto expected = loc_at(sql, pos);
  EXPECT_EQ(expected.line, 3);  // sanity check on the helper

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].line, expected.line);
  EXPECT_EQ(r.errors[0].column, expected.column);
}

TEST(AnalyzerLocationTest, UnknownSourcePointsAtTableName) {
  CatalogSnapshot empty;
  const std::string sql = "SELECT * FROM nonexistent_stream LIMIT 10";
  auto pos = sql.find("nonexistent_stream");
  ASSERT_NE(pos, std::string::npos);
  auto expected = loc_at(sql, pos);

  auto r = compile_sql(sql, empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].line, expected.line);
  EXPECT_EQ(r.errors[0].column, expected.column);
}

TEST(AnalyzerLocationTest, UnknownFunctionPointsAtFunctionCall) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT FOOBAR(memory) FROM memory_usage";
  auto pos = sql.find("FOOBAR");
  ASSERT_NE(pos, std::string::npos);
  auto expected = loc_at(sql, pos);

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].line, expected.line);
  EXPECT_EQ(r.errors[0].column, expected.column);
}

// Helper for tests that scan the errors for a specific message and verify
// its (line, column) match a token's offset.
//
// IMPORTANT: the analyzer's locations are relative to the SQL after
// normalize_sql (CREATE STREAM → CREATE TABLE, DROP STREAM → DROP TABLE),
// not the user's original. We normalize here so the search-for-token uses
// the same byte space the analyzer reports against.
void expect_loc_at_token(const std::string& sql,
                         const CatalogSnapshot& catalog,
                         const std::string& needle,
                         const std::string& token,
                         std::size_t token_skip = 0) {
  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  // Find the matching error.
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find(needle) != std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr) << "no error matched: " << needle;

  std::string normalized = normalize_for_span(sql);
  std::size_t pos = 0;
  for (std::size_t i = 0; i <= token_skip; ++i) {
    pos = normalized.find(token, pos);
    ASSERT_NE(pos, std::string::npos) << "token not found: " << token;
    if (i < token_skip) ++pos;
  }
  auto expected = loc_at(normalized, pos);
  EXPECT_EQ(match->line, expected.line) << "needle: " << needle;
  EXPECT_EQ(match->column, expected.column) << "needle: " << needle;
}

TEST(AnalyzerLocationTest, InsertTypeMismatchPointsAtValue) {
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {
      {"id", 0, ColumnType::DOUBLE},
      {"location", 1, ColumnType::TEXT},
  };
  c.streams["sensors"] = schema;
  // String in DOUBLE column: error should point at the offending '42'.
  // (Column 0 is DOUBLE; we pass 'foo' to it.)
  const std::string sql = "INSERT INTO sensors VALUES ('foo', 'Bay A')";
  expect_loc_at_token(sql, c, "is DOUBLE but got a string", "'foo'");
}

TEST(AnalyzerLocationTest, DeleteWhereNotEqualityPointsAtComparisonOperator) {
  CatalogSnapshot c;
  TableSchema t;
  t.name = "lookup";
  t.columns = {{"id", 0, ColumnType::DOUBLE}};
  t.key_columns = {0};
  c.tables["lookup"] = t;
  // libpg_query's A_Expr.location points at the operator token, not the
  // LHS. The diagnostic inherits that — pointing at the wrong operator is
  // the most actionable signal here.
  const std::string sql = "DELETE FROM lookup WHERE id > 1";
  expect_loc_at_token(sql, c, "DELETE WHERE must be key_column = constant",
                      ">");
}

TEST(AnalyzerLocationTest, GroupByMultiFromPointsAtFirstGroupItem) {
  CatalogSnapshot c;
  StreamSchema a;
  a.name = "a";
  a.columns = {{"x", 0, ColumnType::DOUBLE}};
  StreamSchema b;
  b.name = "b";
  b.columns = {{"y", 0, ColumnType::DOUBLE}};
  c.streams["a"] = a;
  c.streams["b"] = b;
  // The error should point at the GROUP BY column reference.
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT x FROM a, b GROUP BY x";
  // The GROUP BY's "x" is the second occurrence in the SQL.
  expect_loc_at_token(sql, c,
                      "GROUP BY with multiple FROM sources", "x",
                      /*token_skip=*/1);
}

TEST(AnalyzerLocationTest, UnknownColumnPointsAtColumnRef) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT bogus_col FROM memory_usage";
  expect_loc_at_token(sql, catalog, "unknown column: bogus_col", "bogus_col");
}

TEST(AnalyzerLocationTest, AggregateInWherePointsAtWhereExpression) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory FROM memory_usage WHERE SUM(memory) > 100";
  // The error points at the WHERE expression (libpg_query's A_Expr
  // location is at the comparison operator `>`).
  expect_loc_at_token(sql, catalog,
                      "aggregate function not allowed in WHERE", ">");
}

TEST(AnalyzerLocationTest, WindowSizeMustBePositivePointsAtValue) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(memory, 0) FROM memory_usage";
  // The window-size diagnostic should point at the offending '0'.
  expect_loc_at_token(sql, catalog, "must be a positive integer", "0");
}

TEST(AnalyzerLocationTest, DuplicateAliasPointsAtSecondItem) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory AS y, cpu AS y FROM memory_usage";
  // Should point at the SECOND `cpu AS y` SelectItem.
  expect_loc_at_token(sql, catalog, "duplicate alias 'y' in SELECT list",
                      "cpu");
}

TEST(AnalyzerLocationTest, UnknownColumnTypePointsAtColumnName) {
  CatalogSnapshot empty;
  // libpg_query's ColumnDef.location points at the column-name token —
  // verified empirically against the parser's actual output. The earlier
  // commit comment claiming it pointed at `(` was wrong; the test was
  // passing only by coincidence (CREATE STREAM normalizes to CREATE
  // TABLE, shifting offsets by 1, which made `(` in the original happen
  // to align with `val` in the normalized).
  const std::string sql = "CREATE STREAM s (val BANANA)";
  expect_loc_at_token(sql, empty, "unknown column type", "val");
}

TEST(AnalyzerLocationTest, ZeroLimitPointsAtLimitValue) {
  auto catalog = stream_catalog();
  const std::string sql = "SELECT memory FROM memory_usage LIMIT 0";
  // Should point at the `0` of the LIMIT clause.
  expect_loc_at_token(sql, catalog, "LIMIT must be a positive integer", "0");
}

// Convert a 1-based (line, column) to a byte offset in `sql`. Inverse of
// the analyzer's location-computation pipeline.
std::size_t line_col_to_offset(const std::string& sql, int line, int column) {
  int cur_line = 1, cur_col = 1;
  for (std::size_t i = 0; i < sql.size(); ++i) {
    if (cur_line == line && cur_col == column) return i;
    if (sql[i] == '\n') {
      ++cur_line;
      cur_col = 1;
    } else {
      ++cur_col;
    }
  }
  return sql.size();
}

// Extract the substring of `sql` covered by the [start..end) range carried
// by a CompilationError. End is exclusive — same convention as editor
// selections.
std::string extract_span(const std::string& sql, const CompilationError& e) {
  std::size_t start = line_col_to_offset(sql, e.line, e.column);
  std::size_t end = line_col_to_offset(sql, e.end_line, e.end_column);
  if (end <= start || end > sql.size()) return "";
  return sql.substr(start, end - start);
}

// Find the error whose message contains `needle`, then assert its
// (start, end) span exactly extracts `expected_token` from the SQL.
// Verifies BOTH start and end positions in one shot. Normalizes the SQL
// the same way compile_sql does so byte offsets line up.
void expect_span_matches(const std::string& sql,
                         const CatalogSnapshot& catalog,
                         const std::string& needle,
                         const std::string& expected_token) {
  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors())
      << "expected error containing \"" << needle << "\" but compile_sql succeeded";
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find(needle) != std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr) << "no error matched: " << needle;
  EXPECT_GE(match->line, 1) << "needle: " << needle;
  EXPECT_GE(match->column, 1) << "needle: " << needle;
  EXPECT_GE(match->end_line, 1) << "needle: " << needle;
  EXPECT_GE(match->end_column, 1) << "needle: " << needle;
  std::string normalized = normalize_for_span(sql);
  std::string span = extract_span(normalized, *match);
  EXPECT_EQ(span, expected_token)
      << "needle: " << needle << "; span: '" << span
      << "'; expected: '" << expected_token << "'";
}

// Verify that end_line/end_column span the offending token, not just
// start. Helps editors render selections from start to end.
TEST(AnalyzerEndLocationTest, FunctionArityErrorSpansFunctionName) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT MOVING_SUM(memory) FROM memory_usage";
  auto pos = sql.find("MOVING_SUM");
  ASSERT_NE(pos, std::string::npos);
  auto start = loc_at(sql, pos);
  auto end = loc_at(sql, pos + std::string("MOVING_SUM").size());

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].line, start.line);
  EXPECT_EQ(r.errors[0].column, start.column);
  EXPECT_EQ(r.errors[0].end_line, end.line);
  EXPECT_EQ(r.errors[0].end_column, end.column);
}

TEST(AnalyzerEndLocationTest, UnknownColumnSpansColumnName) {
  auto catalog = stream_catalog();
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS SELECT bogus_col FROM memory_usage";
  auto pos = sql.find("bogus_col");
  ASSERT_NE(pos, std::string::npos);
  auto end = loc_at(sql, pos + std::string("bogus_col").size());

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  // Find the matching error.
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find("unknown column") != std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr);
  EXPECT_EQ(match->end_line, end.line);
  EXPECT_EQ(match->end_column, end.column);
}

TEST(AnalyzerEndLocationTest, UnknownSourceSpansTableName) {
  CatalogSnapshot empty;
  const std::string sql = "SELECT * FROM nonexistent_stream LIMIT 10";
  auto pos = sql.find("nonexistent_stream");
  auto end = loc_at(sql, pos + std::string("nonexistent_stream").size());

  auto r = compile_sql(sql, empty);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].end_line, end.line);
  EXPECT_EQ(r.errors[0].end_column, end.column);
}

TEST(AnalyzerEndLocationTest, ZeroLimitSpansLimitValue) {
  auto catalog = stream_catalog();
  const std::string sql = "SELECT memory FROM memory_usage LIMIT 0";
  auto pos = sql.find("LIMIT 0") + std::string("LIMIT ").size();
  auto end = loc_at(sql, pos + 1);  // single char "0"

  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  // Find the matching error.
  const CompilationError* match = nullptr;
  for (const auto& e : r.errors) {
    if (e.message.find("LIMIT must be a positive integer") !=
        std::string::npos) {
      match = &e;
      break;
    }
  }
  ASSERT_NE(match, nullptr);
  EXPECT_EQ(match->end_column, end.column);
}

// --- Comprehensive span verification using extract_span ---
//
// Each test pins that the [start, end) range carried by the diagnostic
// extracts EXACTLY the offending token from the SQL. This verifies BOTH
// start and end positions in one assertion.

TEST(AnalyzerSpanTest, FunctionArityErrorSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "SELECT MOVING_SUM(memory) FROM memory_usage LIMIT 1",
      c, "MOVING_SUM requires 2 arguments", "MOVING_SUM");
}

TEST(AnalyzerSpanTest, UnknownFunctionSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "SELECT FOOBAR(memory) FROM memory_usage LIMIT 1",
      c, "unknown function", "FOOBAR");
}

TEST(AnalyzerSpanTest, UnknownColumnSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "SELECT bogus_col FROM memory_usage LIMIT 1",
      c, "unknown column", "bogus_col");
}

TEST(AnalyzerSpanTest, UnknownSourceSpan) {
  CatalogSnapshot empty;
  expect_span_matches(
      "SELECT * FROM nonexistent_stream LIMIT 1",
      empty, "unknown source", "nonexistent_stream");
}

TEST(AnalyzerSpanTest, ZeroLimitSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "SELECT memory FROM memory_usage LIMIT 0",
      c, "LIMIT must be a positive integer", "0");
}

TEST(AnalyzerSpanTest, WindowSizeMustBePositiveSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(memory, 0) FROM memory_usage",
      c, "must be a positive integer", "0");
}

TEST(AnalyzerSpanTest, WindowSizeMustBeConstantSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(memory, cpu) FROM memory_usage",
      c, "must be a constant integer", "cpu");
}

TEST(AnalyzerSpanTest, PowerNonConstantExponentSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS SELECT POWER(memory, cpu) FROM memory_usage",
      c, "POWER exponent must be a constant", "cpu");
}

TEST(AnalyzerSpanTest, JoinUnknownTableSpan) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = trades;
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS SELECT price FROM trades "
      "INNER JOIN missing_table ON trades.instrument_id = missing_table.id",
      c, "JOIN: unknown table: missing_table", "missing_table");
}

TEST(AnalyzerSpanTest, JoinOnQualifiedTargetColumnSpan) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {{"instrument_id", 0, ColumnType::DOUBLE}};
  c.streams["trades"] = trades;
  TableSchema instruments;
  instruments.name = "instruments";
  instruments.columns = {{"id", 0, ColumnType::DOUBLE}};
  instruments.key_columns = {0};
  c.tables["instruments"] = instruments;
  // Qualified ref `instruments.nope` — the analyzer reports at the ColumnRef's
  // location, which libpg_query points at the qualifier (`instruments`).
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS SELECT instrument_id FROM trades "
      "INNER JOIN instruments ON trades.instrument_id = instruments.nope",
      c, "JOIN: column 'instruments.nope' not found", "instruments");
}

TEST(AnalyzerSpanTest, DuplicateAliasSpan) {
  auto c = stream_catalog();
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory AS y, cpu AS y FROM memory_usage",
      c, "duplicate alias 'y'", "cpu");
}

TEST(AnalyzerSpanTest, UnknownColumnTypeSpan) {
  CatalogSnapshot empty;
  // libpg_query's ColumnDef.location points at the column-name token, not
  // at the surrounding parens — span identifies the column name.
  expect_span_matches("CREATE STREAM s (val BANANA)", empty,
                      "unknown column type", "val");
}

TEST(AnalyzerSpanTest, DeleteWhereNotEqualitySpan) {
  CatalogSnapshot c;
  TableSchema t;
  t.name = "lookup";
  t.columns = {{"id", 0, ColumnType::DOUBLE}};
  t.key_columns = {0};
  c.tables["lookup"] = t;
  // A_Expr.location points at the operator.
  expect_span_matches("DELETE FROM lookup WHERE id > 1", c,
                      "DELETE WHERE must be key_column = constant", ">");
}

TEST(AnalyzerSpanTest, NotOnNonComparisonSpan) {
  auto c = stream_catalog();
  // BoolExpr.location for NOT — the diagnostic points at the NOT itself.
  // The find_token_end helper treats `NOT` as an identifier-shaped token.
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory FROM memory_usage WHERE NOT memory",
      c, "NOT is only supported on comparison expressions", "NOT");
}

TEST(AnalyzerSpanTest, WhereTwoConstantsSpan) {
  auto c = stream_catalog();
  // ComparisonExpr at the operator.
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory FROM memory_usage WHERE 1 > 0",
      c, "comparison of two constants is not supported in WHERE", ">");
}

TEST(AnalyzerSpanTest, OrderByWithoutLimitSpan) {
  auto c = stream_catalog();
  // ORDER BY items use the inner expression's location (column name).
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT memory FROM memory_usage ORDER BY memory DESC",
      c, "ORDER BY requires LIMIT", "memory");
}

TEST(AnalyzerSpanTest, GroupByMultiFromSpan) {
  CatalogSnapshot c;
  StreamSchema a;
  a.name = "a";
  a.columns = {{"x", 0, ColumnType::DOUBLE}};
  StreamSchema b;
  b.name = "b";
  b.columns = {{"y", 0, ColumnType::DOUBLE}};
  c.streams["a"] = a;
  c.streams["b"] = b;
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS SELECT x FROM a, b GROUP BY x",
      c, "GROUP BY with multiple FROM sources", "x");
}

TEST(AnalyzerSpanTest, InsertUnknownColumnSpan) {
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "t";
  schema.columns = {{"a", 0, ColumnType::DOUBLE}};
  c.streams["t"] = schema;
  // INSERT errors point at stmt.loc which is the relation name.
  expect_span_matches("INSERT INTO t (nope) VALUES (1)", c,
                      "INSERT: column 'nope' not found in 't'", "t");
}

TEST(AnalyzerSpanTest, DropDependentSpan) {
  CatalogSnapshot c;
  StreamSchema s;
  s.name = "src";
  s.columns = {{"x", 0, ColumnType::DOUBLE}};
  c.streams["src"] = s;
  ViewMeta v;
  v.name = "dependent";
  v.entity_type = EntityType::MATERIALIZED_VIEW;
  v.source_streams = {"src"};
  c.views["dependent"] = v;
  // DROP STREAM is normalized to DROP TABLE before parsing, so the span
  // covers DROP — verify the start is at the keyword.
  expect_span_matches("DROP STREAM src", c,
                      "Cannot drop 'src': referenced by", "DROP");
}

TEST(AnalyzerSpanTest, FunctionArityNumberLiteralEndsAfterDigit) {
  // Sanity check on the number-token classifier.
  auto c = stream_catalog();
  // 0 is a single-char token — start_col + 1 = end_col.
  expect_span_matches(
      "SELECT memory FROM memory_usage LIMIT 0",
      c, "LIMIT must be a positive integer", "0");
}

TEST(AnalyzerSpanTest, MultiCharNumberLiteralFullySpanned) {
  auto c = stream_catalog();
  // Verify a multi-digit literal is fully spanned (12345 → 5 chars).
  expect_span_matches(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_AVERAGE(memory, 12345.6) FROM memory_usage",
      c, "must be a positive integer", "12345.6");
}

TEST(AnalyzerEndLocationTest, EndExclusiveOfLastChar) {
  // Verify the convention: end_column points one *past* the last character
  // of the token. So for "abc" at column 1, end_column = 4 (a=1, b=2, c=3,
  // end=4).
  auto catalog = stream_catalog();
  // Use a single-line SQL with a function name "MOVING_SUM" (10 chars).
  const std::string sql = "SELECT MOVING_SUM(memory) FROM memory_usage LIMIT 1";
  // "MOVING_SUM" starts at column 8 (1-based), so end_column = 8 + 10 = 18.
  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_EQ(r.errors[0].column, 8);
  EXPECT_EQ(r.errors[0].end_column, 18);
}

TEST(AnalyzerLocationTest, JoinUnknownTablePointsAtTableName) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = trades;
  const std::string sql =
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT price FROM trades INNER JOIN missing_table "
      "ON trades.instrument_id = missing_table.id";
  expect_loc_at_token(sql, c, "JOIN: unknown table: missing_table",
                      "missing_table");
}

// ---------------------------------------------------------------------------
// More multi-error coverage — the analyzer's main UX claim is that all
// errors surface in a single compile pass with their own locations.
// ---------------------------------------------------------------------------

TEST(AnalyzerMultiErrorTest, ThreeIndependentSelectErrorsAllSurface) {
  auto catalog = stream_catalog();
  // SELECT items: bad MOVING_SUM arity AND unknown function FOOBAR;
  // WHERE: aggregate not allowed.
  // Three distinct semantic problems in a single SELECT.
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v AS "
      "SELECT MOVING_SUM(memory) AS a, FOOBAR(memory) AS b "
      "FROM memory_usage WHERE SUM(memory) > 1",
      catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_GE(r.errors.size(), 3u);
  // Every diagnostic should have a populated location.
  for (const auto& e : r.errors) {
    EXPECT_GE(e.line, 1);
    EXPECT_GE(e.column, 1);
  }
}

TEST(AnalyzerMultiErrorTest, BadInsertValuesAllSurface) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "mixed";
  schema.columns = {
      {"a", 0, ColumnType::DOUBLE},
      {"b", 1, ColumnType::TEXT},
      {"c", 2, ColumnType::DOUBLE},
  };
  catalog.streams["mixed"] = schema;

  // Two type mismatches: position 0 (string in DOUBLE) and position 1
  // (numeric in TEXT). Both should be reported.
  auto r =
      compile_sql("INSERT INTO mixed VALUES ('oops', 99, 1.0)", catalog);
  ASSERT_TRUE(r.has_errors());
  EXPECT_GE(r.errors.size(), 2u);
}

// ---------------------------------------------------------------------------
// Direct-analyzer unit tests
//
// Bypass compile_sql so we exercise the analyzer in isolation from the
// compiler's defensive throws. This proves the analyzer is doing the work
// (not the compiler) and lets us inspect Diagnostic vectors directly
// without going through the CompilationResult conversion.
// ---------------------------------------------------------------------------

namespace {

// Parse SQL into AST with locations. Helper for direct-analyzer tests.
parser::ast::Statement parse_with_locs(const std::string& sql) {
  auto json_result = pg_query_parse(sql.c_str());
  if (json_result.error) {
    std::string msg = json_result.error->message;
    pg_query_free_parse_result(json_result);
    throw std::runtime_error("test SQL failed to parse: " + msg);
  }
  std::string tree(json_result.parse_tree);
  pg_query_free_parse_result(json_result);
  return parser::convert_parse_tree(sql, tree);
}

}  // namespace

TEST(AnalyzerDirectTest, AnalyzeStatementReturnsEmptyForValidSelect) {
  auto catalog = stream_catalog();
  auto stmt = parse_with_locs(
      "SELECT memory * 2 AS doubled FROM memory_usage LIMIT 10");
  auto diags = analyzer::analyze_statement(stmt, catalog);
  EXPECT_TRUE(diags.empty());
}

TEST(AnalyzerDirectTest, AnalyzeStatementSurfacesArityWithLocation) {
  auto catalog = stream_catalog();
  const std::string sql =
      "SELECT MOVING_AVERAGE(memory) FROM memory_usage LIMIT 10";
  auto stmt = parse_with_locs(sql);
  auto diags = analyzer::analyze_statement(stmt, catalog);
  ASSERT_FALSE(diags.empty());
  EXPECT_NE(diags[0].message.find("MOVING_AVERAGE requires 2 arguments"),
            std::string::npos);
  auto pos = sql.find("MOVING_AVERAGE");
  ASSERT_NE(pos, std::string::npos);
  auto expected = loc_at(sql, pos);
  EXPECT_EQ(diags[0].loc.line, expected.line);
  EXPECT_EQ(diags[0].loc.column, expected.column);
}

TEST(AnalyzerDirectTest, AnalyzeStatementSurfacesUnknownSource) {
  CatalogSnapshot empty;
  auto stmt =
      parse_with_locs("SELECT * FROM mystery_stream LIMIT 1");
  auto diags = analyzer::analyze_statement(stmt, empty);
  ASSERT_FALSE(diags.empty());
  EXPECT_NE(diags[0].message.find("unknown source"), std::string::npos);
  EXPECT_GE(diags[0].loc.line, 1);
  EXPECT_GE(diags[0].loc.column, 1);
}

TEST(AnalyzerDirectTest, AnalyzeStatementSurfacesAllErrorsFromOneSelect) {
  auto catalog = stream_catalog();
  // Three independent problems for the analyzer to find.
  auto stmt = parse_with_locs(
      "SELECT MOVING_SUM(memory) AS a, NOPE(memory) AS b "
      "FROM memory_usage WHERE COUNT(*) > 0 LIMIT 10");
  auto diags = analyzer::analyze_statement(stmt, catalog);
  EXPECT_GE(diags.size(), 3u);
}

// ---------------------------------------------------------------------------
// Coverage gates for the deletion phase.
//
// Every test below pins a semantic rule that currently has duplicated
// implementations in the analyzer AND in compiler/planner. After deletion
// of the compiler-side duplicate, only the analyzer remains — so these
// tests become the regression net.
//
// Each test must:
//  * drive compile_sql (NOT internal compile_function/compile_expression),
//  * assert has_errors() is true,
//  * pin a substring of the message so that wording regressions show up.
// ---------------------------------------------------------------------------

namespace {

// Wrap a SELECT statement inside CREATE MATERIALIZED VIEW so the analyzer's
// SELECT pass runs without requiring an explicit LIMIT.
std::string mv(const std::string& select_stmt) {
  return "CREATE MATERIALIZED VIEW v AS " + select_stmt;
}

// Helper: assert compile_sql produces an error containing `needle`, with
// (line, column) populated. Every analyzer diagnostic must have a source
// location — coverage tests enforce that contract.
void expect_error_containing(const std::string& sql,
                             const CatalogSnapshot& catalog,
                             const std::string& needle) {
  auto r = compile_sql(sql, catalog);
  ASSERT_TRUE(r.has_errors())
      << "expected error containing \"" << needle << "\" but compile_sql succeeded";
  bool found = false;
  for (const auto& e : r.errors) {
    if (e.message.find(needle) != std::string::npos) {
      found = true;
      EXPECT_GE(e.line, 1) << "needle: " << needle;
      EXPECT_GE(e.column, 1) << "needle: " << needle;
      break;
    }
  }
  EXPECT_TRUE(found) << "no error matched substring: \"" << needle
                     << "\"; got " << r.errors.size() << " errors";
}

}  // namespace

// --- Aggregate function arity (function_compiler.cpp:84,98,110,133,147) ---

TEST(AnalyzerCoverageTest, SumWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT SUM(memory, cpu) FROM memory_usage"), c,
                          "SUM requires exactly 1 argument");
}

TEST(AnalyzerCoverageTest, CountWithArgsRejected) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT COUNT(memory) FROM memory_usage"), c,
                          "COUNT(*) takes no arguments");
}

TEST(AnalyzerCoverageTest, AvgWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT AVG(memory, cpu) FROM memory_usage"), c,
                          "AVG requires exactly 1 argument");
}

TEST(AnalyzerCoverageTest, MinWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MIN(memory, cpu) FROM memory_usage"), c,
                          "MIN requires exactly 1 argument");
}

TEST(AnalyzerCoverageTest, MaxWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MAX(memory, cpu) FROM memory_usage"), c,
                          "MAX requires exactly 1 argument");
}

// --- Windowed function arity (function_compiler.cpp 163-272) ---

TEST(AnalyzerCoverageTest, MovingAverageWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MOVING_AVERAGE(memory) FROM memory_usage"),
                          c, "MOVING_AVERAGE requires 2 arguments");
}

TEST(AnalyzerCoverageTest, MovingCountWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MOVING_COUNT(memory, 5) FROM memory_usage"),
                          c, "MOVING_COUNT requires 1 argument");
}

TEST(AnalyzerCoverageTest, MovingStdWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MOVING_STD(memory) FROM memory_usage"), c,
                          "MOVING_STD requires 2 arguments");
}

TEST(AnalyzerCoverageTest, StddevWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT STDDEV(memory) FROM memory_usage"), c,
                          "STDDEV requires 2 arguments");
}

TEST(AnalyzerCoverageTest, MovingMinWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MOVING_MIN(memory) FROM memory_usage"), c,
                          "MOVING_MIN requires 2 arguments");
}

TEST(AnalyzerCoverageTest, MovingMaxWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT MOVING_MAX(memory) FROM memory_usage"), c,
                          "MOVING_MAX requires 2 arguments");
}

// --- Window-size constant validation (function_compiler.cpp:47, 52) ---
// "must be a constant integer" — covered by a non-literal window arg.
// "must be a positive integer" — covered by 0 or negative literal.

TEST(AnalyzerCoverageTest, WindowSizeMustBePositive) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT MOVING_AVERAGE(memory, 0) FROM memory_usage"), c,
      "must be a positive integer");
}

TEST(AnalyzerCoverageTest, WindowSizeMustBeConstantInteger) {
  auto c = stream_catalog();
  // Non-literal window size: a column reference, not a constant.
  expect_error_containing(
      mv("SELECT MOVING_AVERAGE(memory, cpu) FROM memory_usage"), c,
      "must be a constant integer");
}

// --- DSP function arity (function_compiler.cpp 291-368) ---

TEST(AnalyzerCoverageTest, DiffWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT DIFF(memory, cpu) FROM memory_usage"), c,
                          "DIFF requires exactly 1 argument");
}

TEST(AnalyzerCoverageTest, FirWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT FIR(memory) FROM memory_usage"), c,
                          "FIR requires 2 arguments");
}

TEST(AnalyzerCoverageTest, FirSecondArgMustBeArrayLiteral) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT FIR(memory, cpu) FROM memory_usage"), c,
                          "FIR: second argument must be an array literal");
}

TEST(AnalyzerCoverageTest, IirWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT IIR(memory) FROM memory_usage"), c,
                          "IIR requires 3 arguments");
}

TEST(AnalyzerCoverageTest, IirArgsMustBeArrayLiterals) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT IIR(memory, cpu, memory) FROM memory_usage"), c,
      "IIR: second and third arguments must be array literals");
}

TEST(AnalyzerCoverageTest, ResampleWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT RESAMPLE(memory) FROM memory_usage"), c,
                          "RESAMPLE requires 2 arguments");
}

TEST(AnalyzerCoverageTest, PeakDetectWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT PEAK_DETECT(memory) FROM memory_usage"), c,
                          "PEAK_DETECT requires 2 arguments");
}

// --- Special expression-context functions (expression_compiler.cpp) ---

TEST(AnalyzerCoverageTest, TsWithArgsRejected) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT TS(memory) FROM memory_usage"), c,
                          "TS() takes no arguments");
}

TEST(AnalyzerCoverageTest, PowerWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT POWER(memory) FROM memory_usage"), c,
                          "POWER requires exactly 2 arguments");
}

TEST(AnalyzerCoverageTest, TimeshiftWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT TIMESHIFT(memory) FROM memory_usage"), c,
                          "TIMESHIFT requires exactly 2 arguments");
}

TEST(AnalyzerCoverageTest, ResampleConstantWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT RESAMPLE_CONSTANT(memory) FROM memory_usage"), c,
      "RESAMPLE_CONSTANT requires 2 or 3 arguments");
}

TEST(AnalyzerCoverageTest, UnaryMathWrongArity) {
  auto c = stream_catalog();
  expect_error_containing(mv("SELECT ABS(memory, cpu) FROM memory_usage"), c,
                          "ABS requires exactly 1 argument");
}

// --- CASE shape (expression_compiler.cpp:426) ---
// Note: Postgres parser rejects CASE with no WHEN at the syntax level, so
// expression_compiler.cpp:426 is unreachable from the user-facing path
// today. Left in place as a defensive throw; no analyzer test added.

// --- WHERE/HAVING shape (where_compiler.cpp:50, 175, 186, 199;
//     group_by_compiler.cpp:222) ---

TEST(AnalyzerCoverageTest, WhereTwoConstants) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT memory FROM memory_usage WHERE 1 > 0"), c,
      "comparison of two constants is not supported in WHERE");
}

TEST(AnalyzerCoverageTest, NotOnNonComparison) {
  auto c = stream_catalog();
  // NOT(memory) — operand is a column ref, not a comparison.
  expect_error_containing(
      mv("SELECT memory FROM memory_usage WHERE NOT memory"), c,
      "NOT is only supported on comparison expressions");
}

// Probing whether "unsupported predicate expression type" is reachable
// from real SQL — `WHERE <bare-arithmetic>` is not a valid predicate
// (analyzer should reject it).
TEST(AnalyzerCoverageTest, UnsupportedPredicateRejected) {
  auto c = stream_catalog();
  // `WHERE memory + 1` is a BinaryExpr at the top of WHERE, not a
  // Comparison/Logical/NOT/BETWEEN — should be rejected.
  expect_error_containing(
      mv("SELECT memory FROM memory_usage WHERE memory + 1"), c,
      "unsupported predicate expression type");
}

TEST(AnalyzerCoverageTest, HavingTwoConstants) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT memory FROM memory_usage GROUP BY memory HAVING 1 > 0"), c,
      "comparison of two constants is not supported in HAVING");
}

// BETWEEN: the AST converter at libs/parser/src/ast_converter.cpp:177
// rejects BETWEEN unconditionally today. The where_compiler BETWEEN
// checks (and the analyzer's predicate-shape BETWEEN check) are
// unreachable until that converter gains support. No coverage test
// can be written against compile_sql until then; the analyzer rules
// are kept in place for future-proofing.
//
// Other unreachable rule:
//   - "CASE expression has no WHEN clauses" (parser syntactically requires
//     at least one WHEN)

// --- Column resolution (analyzer-only since expression_compiler.cpp:203
//     was deleted) ---

TEST(AnalyzerCoverageTest, UnknownColumnInSelect) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT bogus_column FROM memory_usage"), c, "unknown column");
}

TEST(AnalyzerCoverageTest, UnknownColumnInWhere) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT memory FROM memory_usage WHERE bogus > 0"), c,
      "unknown column");
}

// --- Stream LIMIT requirement (analyzer-only since classifier.cpp:190
//     was deleted) ---

TEST(AnalyzerCoverageTest, StreamSelectWithoutLimitRejected) {
  auto c = stream_catalog();
  expect_error_containing("SELECT memory FROM memory_usage", c,
                          "requires LIMIT");
}

TEST(AnalyzerCoverageTest, StreamSelectWithLimitOk) {
  auto c = stream_catalog();
  auto r = compile_sql("SELECT memory FROM memory_usage LIMIT 10", c);
  EXPECT_FALSE(r.has_errors());
}

// --- NOT optimization needs constant on right (analyzer-only since
//     where_compiler.cpp:162 was deleted) ---

TEST(AnalyzerCoverageTest, NotComparisonWithNonConstantRightRejected) {
  auto c = stream_catalog();
  // NOT(memory > cpu) — right side of comparison is a column, not a
  // constant. Must be rejected.
  expect_error_containing(
      mv("SELECT memory FROM memory_usage WHERE NOT (memory > cpu)"), c,
      "NOT optimization requires constant on right side");
}

// --- Coverage for rules whose compiler-side throws were deleted but had
//     no existing test exercising the rule via compile_sql ---

TEST(AnalyzerCoverageTest, OrderByWithoutLimitRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT memory FROM memory_usage ORDER BY memory DESC"), c,
      "ORDER BY requires LIMIT");
}

TEST(AnalyzerCoverageTest, OrderByColumnNotInSelectListRejected) {
  auto c = stream_catalog();
  // ORDER BY cpu but SELECT only includes memory.
  expect_error_containing(
      mv("SELECT memory FROM memory_usage ORDER BY cpu DESC LIMIT 5"), c,
      "ORDER BY column not found in SELECT list");
}

TEST(AnalyzerCoverageTest, GroupByWithMultipleFromRejected) {
  CatalogSnapshot c;
  StreamSchema a;
  a.name = "a";
  a.columns = {{"x", 0, ColumnType::DOUBLE}};
  StreamSchema b;
  b.name = "b";
  b.columns = {{"y", 0, ColumnType::DOUBLE}};
  c.streams["a"] = a;
  c.streams["b"] = b;
  expect_error_containing(
      mv("SELECT x FROM a, b GROUP BY x"), c,
      "GROUP BY with multiple FROM sources is not yet supported");
}

TEST(AnalyzerCoverageTest, CompositeTableKeysRejected) {
  CatalogSnapshot empty;
  expect_error_containing(
      "CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)", empty,
      "Composite table keys");
}

// --- Item 9: LIMIT must be positive ---

TEST(AnalyzerCoverageTest, ZeroLimitRejected) {
  auto c = stream_catalog();
  expect_error_containing("SELECT memory FROM memory_usage LIMIT 0", c,
                          "LIMIT must be a positive integer");
}

TEST(AnalyzerCoverageTest, PositiveLimitAccepted) {
  auto c = stream_catalog();
  auto r = compile_sql("SELECT memory FROM memory_usage LIMIT 1", c);
  EXPECT_FALSE(r.has_errors());
}

// --- Item 4: Duplicate aliases in SELECT ---

TEST(AnalyzerCoverageTest, DuplicateSelectAliasesRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT memory AS y, cpu AS y FROM memory_usage"), c,
      "duplicate alias 'y' in SELECT list");
}

// --- Item 3: Duplicate column names in CREATE ---

TEST(AnalyzerCoverageTest, DuplicateColumnNameInCreateStreamRejected) {
  CatalogSnapshot empty;
  expect_error_containing(
      "CREATE STREAM s (x DOUBLE PRECISION, x DOUBLE PRECISION)", empty,
      "duplicate column name 'x' in 's'");
}

TEST(AnalyzerCoverageTest, DuplicateColumnNameInCreateTableRejected) {
  CatalogSnapshot empty;
  expect_error_containing(
      "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, id DOUBLE PRECISION)",
      empty, "duplicate column name 'id' in 't'");
}

// --- Item 5: Unknown column types in CREATE ---

TEST(AnalyzerCoverageTest, UnknownColumnTypeRejected) {
  CatalogSnapshot empty;
  expect_error_containing(
      "CREATE STREAM s (val BANANA)", empty,
      "unknown column type");
}

TEST(AnalyzerCoverageTest, KnownColumnTypesAccepted) {
  CatalogSnapshot empty;
  // All these should compile cleanly.
  EXPECT_FALSE(compile_sql(
                   "CREATE STREAM s1 (a INT, b BIGINT, c REAL, d FLOAT, "
                   "e TEXT, f VARCHAR, g BOOLEAN)",
                   empty)
                   .has_errors());
}

// --- INSERT: column-list validation (Item 2) ---

TEST(AnalyzerCoverageTest, InsertColumnListUnknownColumnRejected) {
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "t";
  schema.columns = {
      {"a", 0, ColumnType::DOUBLE},
      {"b", 1, ColumnType::DOUBLE},
  };
  c.streams["t"] = schema;
  expect_error_containing(
      "INSERT INTO t (a, nope) VALUES (1, 2)", c,
      "INSERT: column 'nope' not found in 't'");
}

TEST(AnalyzerCoverageTest, InsertNamedColumnsValueCountMismatch) {
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "t";
  schema.columns = {
      {"a", 0, ColumnType::DOUBLE},
      {"b", 1, ColumnType::DOUBLE},
      {"c", 2, ColumnType::DOUBLE},
  };
  c.streams["t"] = schema;
  // 2 named columns, 3 values: mismatch against the named count.
  expect_error_containing(
      "INSERT INTO t (a, b) VALUES (1, 2, 3)", c,
      "value count mismatch (3 values for 2 named columns)");
}

// --- INSERT: non-constant values ---

TEST(AnalyzerCoverageTest, InsertNonConstantValueRejected) {
  CatalogSnapshot c;
  StreamSchema schema;
  schema.name = "t";
  schema.columns = {{"x", 0, ColumnType::DOUBLE}};
  c.streams["t"] = schema;
  expect_error_containing("INSERT INTO t VALUES (FOO())", c,
                          "INSERT values must be constants");
}

// --- DELETE: each shape rule covered ---

namespace {
CatalogSnapshot table_catalog() {
  CatalogSnapshot c;
  TableSchema t;
  t.name = "lookup";
  t.columns = {
      {"id", 0, ColumnType::DOUBLE},
      {"name", 1, ColumnType::TEXT},
  };
  t.key_columns = {0};
  c.tables["lookup"] = t;
  return c;
}
}  // namespace

TEST(AnalyzerCoverageTest, DeleteUnknownTableRejected) {
  CatalogSnapshot empty;
  expect_error_containing("DELETE FROM mystery WHERE id = 1", empty,
                          "DELETE: unknown table");
}

TEST(AnalyzerCoverageTest, DeleteTableWithoutPrimaryKeyRejected) {
  CatalogSnapshot c;
  TableSchema t;
  t.name = "no_pk";
  t.columns = {{"x", 0, ColumnType::DOUBLE}};
  // key_columns intentionally empty
  c.tables["no_pk"] = t;
  expect_error_containing("DELETE FROM no_pk WHERE x = 1", c,
                          "DELETE: table has no primary key");
}

TEST(AnalyzerCoverageTest, DeleteWithoutWhereRejected) {
  auto c = table_catalog();
  expect_error_containing("DELETE FROM lookup", c,
                          "DELETE requires WHERE key_column = value");
}

TEST(AnalyzerCoverageTest, DeleteWhereNotEqualityRejected) {
  auto c = table_catalog();
  expect_error_containing("DELETE FROM lookup WHERE id > 1", c,
                          "DELETE WHERE must be key_column = constant");
}

TEST(AnalyzerCoverageTest, DeleteWhereNonConstantRejected) {
  auto c = table_catalog();
  // Note: needs a valid expression that's not a literal Constant. A column
  // reference would be valid SQL but rejected as non-constant key value.
  expect_error_containing("DELETE FROM lookup WHERE id = id", c,
                          "DELETE WHERE value must be a constant");
}

// --- Constant-fold-aware checks (POWER/TIMESHIFT/RESAMPLE_CONSTANT) ---

TEST(AnalyzerCoverageTest, PowerExponentNonConstantRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT POWER(memory, cpu) FROM memory_usage"), c,
      "POWER exponent must be a constant");
}

TEST(AnalyzerCoverageTest, PowerExponentFoldedConstantAccepted) {
  auto c = stream_catalog();
  // 2*3 folds to 6 — must be accepted.
  auto r = compile_sql(
      mv("SELECT POWER(memory, 2*3) FROM memory_usage"), c);
  EXPECT_FALSE(r.has_errors());
}

TEST(AnalyzerCoverageTest, TimeshiftNonConstantShiftRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT TIMESHIFT(memory, cpu) FROM memory_usage"), c,
      "TIMESHIFT shift must be a constant");
}

TEST(AnalyzerCoverageTest, ResampleConstantNonConstantIntervalRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT RESAMPLE_CONSTANT(memory, cpu) FROM memory_usage"), c,
      "RESAMPLE_CONSTANT interval must be a constant");
}

TEST(AnalyzerCoverageTest, ResampleConstantNonConstantSnapFirstRejected) {
  auto c = stream_catalog();
  expect_error_containing(
      mv("SELECT RESAMPLE_CONSTANT(memory, 1000, cpu) FROM memory_usage"), c,
      "RESAMPLE_CONSTANT snap_first must be a constant");
}

// --- JOIN target validation (MEDIUM) ---

TEST(AnalyzerCoverageTest, JoinUnknownTableRejected) {
  CatalogSnapshot c;
  StreamSchema s;
  s.name = "trades";
  s.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = s;
  expect_error_containing(
      mv("SELECT price FROM trades INNER JOIN missing ON trades.instrument_id = missing.id"),
      c, "JOIN: unknown table: missing");
}

TEST(AnalyzerCoverageTest, JoinUnsupportedTargetTypeRejected) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = trades;
  // Target is a STREAM, not a TABLE — joins only support TABLEs today.
  StreamSchema other;
  other.name = "other_stream";
  other.columns = {{"id", 0, ColumnType::DOUBLE}};
  c.streams["other_stream"] = other;
  expect_error_containing(
      mv("SELECT price FROM trades INNER JOIN other_stream ON trades.instrument_id = other_stream.id"),
      c, "JOIN: unsupported join target type for: other_stream");
}

TEST(AnalyzerCoverageTest, JoinOnConditionMustResolveLeftColumn) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = trades;
  TableSchema instruments;
  instruments.name = "instruments";
  instruments.columns = {
      {"id", 0, ColumnType::DOUBLE},
      {"name", 1, ColumnType::TEXT},
  };
  instruments.key_columns = {0};
  c.tables["instruments"] = instruments;
  // ON condition references columns that aren't in the LEFT stream — neither
  // side is a `trades.*` column.
  expect_error_containing(
      mv("SELECT price FROM trades INNER JOIN instruments "
         "ON instruments.id = instruments.name"),
      c, "JOIN: could not resolve join column from ON condition");
}

TEST(AnalyzerCoverageTest, JoinOnQualifiedTargetColumnMustExist) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {
      {"instrument_id", 0, ColumnType::DOUBLE},
      {"price", 1, ColumnType::DOUBLE},
  };
  c.streams["trades"] = trades;
  TableSchema instruments;
  instruments.name = "instruments";
  instruments.columns = {
      {"id", 0, ColumnType::DOUBLE},
      {"name", 1, ColumnType::TEXT},
  };
  instruments.key_columns = {0};
  c.tables["instruments"] = instruments;
  // Right side has a typo: instruments.nope (postgres lowercases identifiers).
  expect_error_containing(
      mv("SELECT price FROM trades INNER JOIN instruments "
         "ON trades.instrument_id = instruments.nope"),
      c, "JOIN: column 'instruments.nope' not found in 'instruments'");
}

TEST(AnalyzerCoverageTest, JoinOnQualifiedLeftColumnMustExist) {
  CatalogSnapshot c;
  StreamSchema trades;
  trades.name = "trades";
  trades.columns = {{"instrument_id", 0, ColumnType::DOUBLE}};
  c.streams["trades"] = trades;
  TableSchema instruments;
  instruments.name = "instruments";
  instruments.columns = {{"id", 0, ColumnType::DOUBLE}};
  instruments.key_columns = {0};
  c.tables["instruments"] = instruments;
  // Left side has a typo: trades.nope.
  expect_error_containing(
      mv("SELECT trades.instrument_id FROM trades INNER JOIN instruments "
         "ON trades.nope = instruments.id"),
      c, "JOIN: column 'trades.nope' not found in 'trades'");
}

// --- SELECT FROM VIEW LIMIT (MEDIUM) ---

TEST(AnalyzerCoverageTest, SelectFromViewWithoutLimitRejected) {
  CatalogSnapshot c;
  StreamSchema src;
  src.name = "src";
  src.columns = {{"x", 0, ColumnType::DOUBLE}};
  c.streams["src"] = src;
  ViewMeta v;
  v.name = "myview";
  v.entity_type = EntityType::VIEW;
  v.field_map = {{"x", 0}};
  v.source_streams = {"src"};
  c.views["myview"] = v;
  expect_error_containing("SELECT * FROM myview", c,
                          "SELECT FROM VIEW 'myview' requires LIMIT");
}

// --- DROP: dependency check ---

TEST(AnalyzerCoverageTest, DropEntityWithDependentsRejected) {
  CatalogSnapshot c;
  StreamSchema s;
  s.name = "src";
  s.columns = {{"x", 0, ColumnType::DOUBLE}};
  c.streams["src"] = s;
  ViewMeta v;
  v.name = "dependent";
  v.entity_type = EntityType::MATERIALIZED_VIEW;
  v.source_streams = {"src"};
  c.views["dependent"] = v;
  expect_error_containing("DROP STREAM src", c,
                          "Cannot drop 'src': referenced by");
}

}  // namespace
}  // namespace rtbot_sql::api
