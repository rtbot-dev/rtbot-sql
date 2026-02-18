#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/compiler/graph_builder.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

namespace rtbot_sql::api {
namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Trades stream: instrument_id(0), price(1), quantity(2)
CatalogSnapshot trades_catalog() {
  CatalogSnapshot cat;
  cat.streams["trades"] = StreamSchema{
      "trades",
      {{"instrument_id", 0}, {"price", 1}, {"quantity", 2}}};
  return cat;
}

// Check that a string contains a substring (case-sensitive).
bool contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

// Return the first error message, or "" if no errors.
std::string first_error(const CompilationResult& r) {
  if (r.errors.empty()) return "";
  return r.errors[0].message;
}

// ---------------------------------------------------------------------------
// Test 1: CREATE VIEW compilation
// ---------------------------------------------------------------------------

TEST(ViewTest, CreateViewSetsCorrectStatementType) {
  auto catalog = trades_catalog();

  auto result = compile_sql(
      "CREATE VIEW bollinger AS "
      "SELECT instrument_id, MOVING_AVERAGE(price, 20) AS ma "
      "FROM trades GROUP BY instrument_id",
      catalog);

  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_EQ(result.statement_type, StatementType::CREATE_VIEW);
  EXPECT_EQ(result.entity_name, "bollinger");
  EXPECT_FALSE(result.program_json.empty());
  // field_map carries the compiled view's output columns
  EXPECT_EQ(result.field_map.count("instrument_id"), 1u);
  EXPECT_EQ(result.field_map.count("ma"), 1u);
  // source_streams points at the underlying stream, not a deployed view stream
  ASSERT_EQ(result.source_streams.size(), 1u);
  EXPECT_EQ(result.source_streams[0], "trades");
}

TEST(ViewTest, CreateViewDoesNotProduceMaterializedViewType) {
  auto catalog = trades_catalog();

  auto view_result = compile_sql(
      "CREATE VIEW v AS SELECT instrument_id, price FROM trades LIMIT 1",
      catalog);
  auto matview_result = compile_sql(
      "CREATE MATERIALIZED VIEW mv AS SELECT instrument_id, price FROM trades",
      catalog);

  EXPECT_EQ(view_result.statement_type, StatementType::CREATE_VIEW);
  EXPECT_EQ(matview_result.statement_type,
            StatementType::CREATE_MATERIALIZED_VIEW);
}

// ---------------------------------------------------------------------------
// Test 2: SELECT FROM VIEW without LIMIT → error
// ---------------------------------------------------------------------------

TEST(ViewTest, SelectFromViewWithoutLimitIsError) {
  auto catalog = trades_catalog();

  // Register a VIEW in the catalog (simulating what the runtime does after
  // processing the CREATE VIEW result).
  ViewMeta bollinger{};
  bollinger.name = "bollinger";
  bollinger.entity_type = EntityType::VIEW;
  bollinger.view_type = ViewType::KEYED;
  bollinger.key_index = 0;
  bollinger.field_map = {{"instrument_id", 0}, {"ma", 1}};
  bollinger.source_streams = {"trades"};

  // Build a minimal valid program_json for bollinger.
  // We compile a real bollinger query to get a valid stored graph.
  auto create_result = compile_sql(
      "CREATE VIEW bollinger AS "
      "SELECT instrument_id, MOVING_AVERAGE(price, 20) AS ma "
      "FROM trades GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(create_result.has_errors());
  bollinger.program_json = create_result.program_json;
  bollinger.field_map = create_result.field_map;
  catalog.views["bollinger"] = bollinger;

  // SELECT without LIMIT → error
  auto result = compile_sql("SELECT * FROM bollinger", catalog);
  EXPECT_TRUE(result.has_errors());
  EXPECT_TRUE(contains(first_error(result), "requires LIMIT"))
      << "Error was: " << first_error(result);
}

// ---------------------------------------------------------------------------
// Test 3: SELECT * FROM VIEW LIMIT N — Tier 3 with stored graph augmented
// ---------------------------------------------------------------------------

TEST(ViewTest, SelectStarFromViewWithLimitIsTier3) {
  auto catalog = trades_catalog();

  // Compile and register the VIEW.
  auto create_result = compile_sql(
      "CREATE VIEW bollinger AS "
      "SELECT instrument_id, MOVING_AVERAGE(price, 20) AS ma "
      "FROM trades GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(create_result.has_errors());

  ViewMeta bollinger{};
  bollinger.name = "bollinger";
  bollinger.entity_type = EntityType::VIEW;
  bollinger.view_type = ViewType::KEYED;
  bollinger.key_index = 0;
  bollinger.field_map = create_result.field_map;
  bollinger.source_streams = create_result.source_streams;
  bollinger.program_json = create_result.program_json;
  catalog.views["bollinger"] = bollinger;

  auto result = compile_sql("SELECT * FROM bollinger LIMIT 10", catalog);
  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_EQ(result.statement_type, StatementType::SELECT);
  EXPECT_EQ(result.select_tier, SelectTier::TIER3_EPHEMERAL);
  EXPECT_EQ(result.select_limit, 10);
  // Source streams must be the VIEW's underlying streams (not "bollinger").
  ASSERT_EQ(result.source_streams.size(), 1u);
  EXPECT_EQ(result.source_streams[0], "trades");
  // The augmented graph is valid JSON and non-empty.
  EXPECT_FALSE(result.program_json.empty());
  EXPECT_EQ(result.field_map, bollinger.field_map);
}

// ---------------------------------------------------------------------------
// Test 4: SELECT FROM VIEW with WHERE clause augments graph with Demux
// ---------------------------------------------------------------------------

TEST(ViewTest, SelectFromViewWithWhereAugmentsGraph) {
  auto catalog = trades_catalog();

  // Use a simple scalar VIEW (SELECT * with expressions) so the output has
  // a known field_map we can filter on.
  auto create_result = compile_sql(
      "CREATE VIEW price_view AS "
      "SELECT instrument_id, price FROM trades LIMIT 1",
      catalog);
  ASSERT_FALSE(create_result.has_errors());

  ViewMeta pv{};
  pv.name = "price_view";
  pv.entity_type = EntityType::VIEW;
  pv.view_type = ViewType::SCALAR;
  pv.key_index = -1;
  pv.field_map = create_result.field_map;
  pv.source_streams = create_result.source_streams;
  pv.program_json = create_result.program_json;
  catalog.views["price_view"] = pv;

  // SELECT FROM VIEW with additional WHERE filter.
  auto result = compile_sql(
      "SELECT * FROM price_view WHERE price > 100 LIMIT 5", catalog);
  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_EQ(result.select_tier, SelectTier::TIER3_EPHEMERAL);

  // The augmented program must have a Demultiplexer (from compile_where).
  EXPECT_TRUE(contains(result.program_json, "Demultiplexer"))
      << "Expected Demultiplexer in augmented graph";
  EXPECT_TRUE(contains(result.program_json, "CompareGT"))
      << "Expected CompareGT in augmented graph";

  EXPECT_EQ(result.select_limit, 5);
  ASSERT_EQ(result.source_streams.size(), 1u);
  EXPECT_EQ(result.source_streams[0], "trades");
}

// ---------------------------------------------------------------------------
// Test 5: DROP VIEW with dependents → error
// ---------------------------------------------------------------------------

TEST(ViewTest, DropViewWithDependentsFails) {
  auto catalog = trades_catalog();

  // Register v1 backed by "trades".
  ViewMeta v1{};
  v1.name = "v1";
  v1.entity_type = EntityType::VIEW;
  v1.view_type = ViewType::SCALAR;
  v1.key_index = -1;
  v1.field_map = {{"instrument_id", 0}, {"price", 1}};
  v1.source_streams = {"trades"};
  v1.program_json = "{}";  // not needed for dependency check
  catalog.views["v1"] = v1;

  // Register v2 that reads FROM v1.
  ViewMeta v2{};
  v2.name = "v2";
  v2.entity_type = EntityType::VIEW;
  v2.view_type = ViewType::SCALAR;
  v2.key_index = -1;
  v2.field_map = {{"instrument_id", 0}, {"price", 1}};
  v2.source_streams = {"v1"};  // depends on v1
  v2.program_json = "{}";
  catalog.views["v2"] = v2;

  // Dropping v1 should fail because v2 depends on it.
  auto result = compile_sql("DROP VIEW v1", catalog);
  EXPECT_TRUE(result.has_errors());
  EXPECT_TRUE(contains(first_error(result), "v2"))
      << "Error should mention v2: " << first_error(result);
  EXPECT_TRUE(contains(first_error(result), "Cannot drop"))
      << "Error was: " << first_error(result);
}

// ---------------------------------------------------------------------------
// Test 6: DROP VIEW without dependents succeeds
// ---------------------------------------------------------------------------

TEST(ViewTest, DropViewWithoutDependentsSucceeds) {
  auto catalog = trades_catalog();

  ViewMeta v1{};
  v1.name = "v1";
  v1.entity_type = EntityType::VIEW;
  v1.view_type = ViewType::SCALAR;
  v1.key_index = -1;
  v1.field_map = {{"instrument_id", 0}};
  v1.source_streams = {"trades"};
  v1.program_json = "{}";
  catalog.views["v1"] = v1;

  auto result = compile_sql("DROP VIEW v1", catalog);
  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_EQ(result.statement_type, StatementType::DROP);
  EXPECT_EQ(result.drop_entity_name, "v1");
  EXPECT_EQ(result.drop_entity_type, EntityType::VIEW);
}

// ---------------------------------------------------------------------------
// Test 7: DROP stream that a VIEW depends on also fails
// ---------------------------------------------------------------------------

TEST(ViewTest, DropStreamReferencedByViewFails) {
  auto catalog = trades_catalog();

  ViewMeta v{};
  v.name = "my_view";
  v.entity_type = EntityType::VIEW;
  v.view_type = ViewType::SCALAR;
  v.key_index = -1;
  v.field_map = {{"price", 0}};
  v.source_streams = {"trades"};
  v.program_json = "{}";
  catalog.views["my_view"] = v;

  // Dropping the underlying stream (via DROP TABLE syntax) should fail.
  auto result = compile_sql("DROP TABLE trades", catalog);
  EXPECT_TRUE(result.has_errors());
  EXPECT_TRUE(contains(first_error(result), "my_view"))
      << "Error was: " << first_error(result);
}

// ---------------------------------------------------------------------------
// Test 8: from_json_for_augmentation round-trip sanity
// ---------------------------------------------------------------------------

TEST(ViewTest, FromJsonForAugmentationRoundTrip) {
  // Compile a simple query and then use from_json_for_augmentation to reload.
  auto catalog = trades_catalog();
  auto result = compile_sql(
      "CREATE VIEW pv AS SELECT instrument_id, price FROM trades LIMIT 1",
      catalog);
  ASSERT_FALSE(result.has_errors());

  // from_json_for_augmentation should not throw and should return a valid
  // pre-output endpoint.
  auto [builder, pre_output_ep] =
      ::rtbot_sql::compiler::GraphBuilder::from_json_for_augmentation(
          result.program_json);

  EXPECT_FALSE(pre_output_ep.operator_id.empty());
  EXPECT_FALSE(pre_output_ep.port.empty());

  // The builder should have an Input and an Output operator.
  bool has_input = false, has_output = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Input") has_input = true;
    if (op.type == "Output") has_output = true;
  }
  EXPECT_TRUE(has_input);
  EXPECT_TRUE(has_output);

  // After re-wiring Output, to_json() must not throw.
  std::string output_id;
  for (const auto& op : builder.operators()) {
    if (op.type == "Output") { output_id = op.id; break; }
  }
  builder.connect(pre_output_ep, {output_id, "i1"});
  EXPECT_NO_THROW(builder.to_json());
}

// ---------------------------------------------------------------------------
// Test 9: ORDER BY + LIMIT compiles to TopK
// ---------------------------------------------------------------------------

TEST(ViewTest, OrderByLimitCompilesTopK) {
  auto catalog = trades_catalog();

  // SELECT instrument_id, quantity FROM trades ORDER BY quantity DESC LIMIT 3
  auto result = compile_sql(
      "SELECT instrument_id, quantity FROM trades "
      "ORDER BY quantity DESC LIMIT 3",
      catalog);

  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_EQ(result.statement_type, StatementType::SELECT);
  EXPECT_EQ(result.select_tier, SelectTier::TIER3_EPHEMERAL);

  // The compiled graph must contain a TopK operator.
  EXPECT_TRUE(contains(result.program_json, "TopK"))
      << "Expected TopK in program: " << result.program_json;

  // Verify k=3 and descending=true are serialized correctly.
  // The JSON is pretty-printed so field values have a space after the colon.
  EXPECT_TRUE(contains(result.program_json, "\"k\": 3"))
      << "program_json: " << result.program_json;
  EXPECT_TRUE(contains(result.program_json, "\"descending\": \"true\""))
      << "program_json: " << result.program_json;
}

// ---------------------------------------------------------------------------
// Test 10: ORDER BY without LIMIT → error
// ---------------------------------------------------------------------------

TEST(ViewTest, OrderByWithoutLimitIsError) {
  auto catalog = trades_catalog();

  auto result = compile_sql(
      "SELECT instrument_id, quantity FROM trades ORDER BY quantity DESC",
      catalog);

  EXPECT_TRUE(result.has_errors());
  EXPECT_TRUE(contains(first_error(result), "LIMIT"))
      << "Error was: " << first_error(result);
}

// ---------------------------------------------------------------------------
// Test 11: ORDER BY ASC LIMIT compiles TopK with descending=false
// ---------------------------------------------------------------------------

TEST(ViewTest, OrderByAscLimitCompilesTopKAscending) {
  auto catalog = trades_catalog();

  auto result = compile_sql(
      "SELECT instrument_id, quantity FROM trades "
      "ORDER BY quantity ASC LIMIT 5",
      catalog);

  EXPECT_FALSE(result.has_errors()) << first_error(result);
  EXPECT_TRUE(contains(result.program_json, "TopK"))
      << "program_json: " << result.program_json;
  EXPECT_TRUE(contains(result.program_json, "\"k\": 5"))
      << "program_json: " << result.program_json;
  EXPECT_TRUE(contains(result.program_json, "\"descending\": \"false\""))
      << "program_json: " << result.program_json;
}

}  // namespace
}  // namespace rtbot_sql::api
