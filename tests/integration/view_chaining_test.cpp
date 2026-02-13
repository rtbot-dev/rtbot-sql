#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <set>

namespace rtbot_sql::api {
namespace {

using json = nlohmann::json;

class ViewChainingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema trades{"trades",
                        {{"instrument_id", 0},
                         {"price", 1},
                         {"quantity", 2},
                         {"account_id", 3}}};
    catalog.streams["trades"] = trades;
  }

  // Register a compilation result as a view in the catalog.
  void register_view(const std::string& name, const CompilationResult& r,
                     EntityType et = EntityType::MATERIALIZED_VIEW) {
    ViewMeta meta{};
    meta.name = name;
    meta.entity_type = et;
    meta.view_type = r.view_type;
    meta.field_map = r.field_map;
    meta.source_streams = r.source_streams;
    meta.program_json = r.program_json;
    meta.key_index = r.key_index;
    catalog.views[name] = meta;
  }

  // Register a table in the catalog.
  void register_table(const std::string& name,
                      std::vector<ColumnDef> columns,
                      const std::string& changelog_stream = "") {
    TableSchema schema;
    schema.name = name;
    schema.columns = std::move(columns);
    schema.changelog_stream = changelog_stream;
    catalog.tables[name] = schema;
  }

  CatalogSnapshot catalog;
};

// ---------------------------------------------------------------------------
// Test 1: Stream -> v1 (GROUP BY) -> v2 (GROUP BY on v1)
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, StreamToGroupByToGroupBy) {
  // v1: per-instrument aggregates
  auto r1 = compile_sql(
      "CREATE MATERIALIZED VIEW instrument_stats AS "
      "SELECT instrument_id, SUM(quantity) AS total_vol, COUNT(*) AS "
      "trade_count "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  EXPECT_EQ(r1.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r1.entity_name, "instrument_stats");
  EXPECT_EQ(r1.view_type, ViewType::KEYED);
  EXPECT_EQ(r1.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r1.field_map.at("total_vol"), 1);
  EXPECT_EQ(r1.field_map.at("trade_count"), 2);
  ASSERT_EQ(r1.source_streams.size(), 1u);
  EXPECT_EQ(r1.source_streams[0], "trades");

  // v1 program: KeyedPipeline with CumulativeSum, CountNumber, VectorCompose
  {
    auto program = json::parse(r1.program_json);
    bool has_keyed = false;
    for (const auto& op : program["operators"]) {
      if (op["type"] == "KeyedPipeline") {
        has_keyed = true;
        ASSERT_TRUE(op.contains("prototype"));
        bool has_cum_sum = false, has_count = false, has_compose = false;
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "CumulativeSum") has_cum_sum = true;
          if (proto_op["type"] == "CountNumber") has_count = true;
          if (proto_op["type"] == "VectorCompose") has_compose = true;
        }
        EXPECT_TRUE(has_cum_sum) << "Expected CumulativeSum in v1 prototype";
        EXPECT_TRUE(has_count) << "Expected CountNumber in v1 prototype";
        EXPECT_TRUE(has_compose) << "Expected VectorCompose in v1 prototype";
      }
    }
    EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in v1 program";
  }

  // Register v1 so v2 can reference it
  register_view("instrument_stats", r1);

  // v2: moving average on v1's output
  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW vol_trends AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_vol, 5) AS avg_vol "
      "FROM instrument_stats GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  EXPECT_EQ(r2.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r2.entity_name, "vol_trends");
  EXPECT_EQ(r2.view_type, ViewType::KEYED);
  EXPECT_EQ(r2.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r2.field_map.at("avg_vol"), 1);
  ASSERT_EQ(r2.source_streams.size(), 1u);
  EXPECT_EQ(r2.source_streams[0], "instrument_stats");

  // v2 program: KeyedPipeline with VectorExtract(index=1) + MovingAverage(window_size=5)
  {
    auto program = json::parse(r2.program_json);
    bool has_keyed = false;
    for (const auto& op : program["operators"]) {
      if (op["type"] == "KeyedPipeline") {
        has_keyed = true;
        ASSERT_TRUE(op.contains("prototype"));
        bool has_extract = false, has_ma = false;
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "VectorExtract") {
            has_extract = true;
            EXPECT_EQ(proto_op["index"], 1)
                << "VectorExtract should extract total_vol at index 1";
          }
          if (proto_op["type"] == "MovingAverage") {
            has_ma = true;
            EXPECT_EQ(proto_op["window_size"], 5);
          }
        }
        EXPECT_TRUE(has_extract) << "Expected VectorExtract in v2 prototype";
        EXPECT_TRUE(has_ma) << "Expected MovingAverage in v2 prototype";
      }
    }
    EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in v2 program";
  }
}

// ---------------------------------------------------------------------------
// Test 2: Stream -> v1 (GROUP BY) -> v2 (simple projection, no GROUP BY)
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, GroupByToSimpleProjection) {
  // v1: Bollinger mid-band
  auto r1 = compile_sql(
      "CREATE MATERIALIZED VIEW bollinger AS "
      "SELECT instrument_id, price, MOVING_AVERAGE(price, 20) AS mid "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  EXPECT_EQ(r1.view_type, ViewType::KEYED);
  register_view("bollinger", r1);

  // v2: re-project from v1
  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW band_summary AS "
      "SELECT instrument_id, mid FROM bollinger",
      catalog);

  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  EXPECT_EQ(r2.entity_name, "band_summary");
  EXPECT_EQ(r2.view_type, ViewType::SCALAR);
  ASSERT_EQ(r2.source_streams.size(), 1u);
  EXPECT_EQ(r2.source_streams[0], "bollinger");

  // v2 should resolve fields from v1's field_map
  EXPECT_EQ(r2.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r2.field_map.at("mid"), 1);
}

// ---------------------------------------------------------------------------
// Test 3: Three levels deep — stream -> v1 -> v2 -> v3
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, ThreeLevelsDeep) {
  auto r1 = compile_sql(
      "CREATE MATERIALIZED VIEW v1 AS "
      "SELECT instrument_id, SUM(quantity) AS vol "
      "FROM trades GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  ASSERT_EQ(r1.source_streams.size(), 1u);
  EXPECT_EQ(r1.source_streams[0], "trades");
  register_view("v1", r1);

  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW v2 AS "
      "SELECT instrument_id, MOVING_AVERAGE(vol, 10) AS smooth_vol "
      "FROM v1 GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  ASSERT_EQ(r2.source_streams.size(), 1u);
  EXPECT_EQ(r2.source_streams[0], "v1");
  register_view("v2", r2);

  auto r3 = compile_sql(
      "CREATE MATERIALIZED VIEW v3 AS "
      "SELECT instrument_id, MOVING_AVERAGE(smooth_vol, 5) AS trend "
      "FROM v2 GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r3.has_errors()) << r3.errors[0].message;
  ASSERT_EQ(r3.source_streams.size(), 1u);
  EXPECT_EQ(r3.source_streams[0], "v2");

  // Field maps chain correctly
  EXPECT_EQ(r3.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r3.field_map.at("trend"), 1);

  // v3 prototype: VectorExtract(index=1) for smooth_vol + MovingAverage(window_size=5)
  auto program = json::parse(r3.program_json);
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      ASSERT_TRUE(op.contains("prototype"));
      bool has_extract = false, has_ma = false;
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "VectorExtract") {
          has_extract = true;
          EXPECT_EQ(proto_op["index"], 1)
              << "VectorExtract should extract smooth_vol at index 1";
        }
        if (proto_op["type"] == "MovingAverage") {
          has_ma = true;
          EXPECT_EQ(proto_op["window_size"], 5);
        }
      }
      EXPECT_TRUE(has_extract);
      EXPECT_TRUE(has_ma);
    }
  }
}

// ---------------------------------------------------------------------------
// Test 4: JSON structure deep validation on chained program
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, JsonStructureDeepValidation) {
  // Build v1 (same as Test 1)
  auto r1 = compile_sql(
      "CREATE MATERIALIZED VIEW instrument_stats AS "
      "SELECT instrument_id, SUM(quantity) AS total_vol, COUNT(*) AS "
      "trade_count "
      "FROM trades GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  register_view("instrument_stats", r1);

  // Build v2
  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW vol_trends AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_vol, 5) AS avg_vol "
      "FROM instrument_stats GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;

  auto program = json::parse(r2.program_json);

  // Top-level program structure
  EXPECT_EQ(program["title"], "<auto-generated>");
  EXPECT_EQ(program["entryOperator"], "input_0");

  // Output mapping
  ASSERT_TRUE(program.contains("output"));
  ASSERT_TRUE(program["output"].contains("output_0"));
  auto output_refs = program["output"]["output_0"];
  EXPECT_FALSE(output_refs.empty());

  // Find Input, Output, and KeyedPipeline at top level
  bool has_input = false, has_output = false, has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "Input") {
      has_input = true;
      ASSERT_TRUE(op.contains("portTypes"));
      bool has_vector_number = false;
      for (const auto& pt : op["portTypes"]) {
        if (pt == "vector_number") has_vector_number = true;
      }
      EXPECT_TRUE(has_vector_number);
    }
    if (op["type"] == "Output") {
      has_output = true;
      ASSERT_TRUE(op.contains("portTypes"));
      bool has_vector_number = false;
      for (const auto& pt : op["portTypes"]) {
        if (pt == "vector_number") has_vector_number = true;
      }
      EXPECT_TRUE(has_vector_number);
    }
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;

      // Prototype is a nested program object
      ASSERT_TRUE(op.contains("prototype"));
      const auto& proto = op["prototype"];

      // Prototype has entry/output/operators/connections
      ASSERT_TRUE(proto.contains("entry"));
      ASSERT_TRUE(proto["entry"].contains("operator"));
      ASSERT_TRUE(proto.contains("output"));
      ASSERT_TRUE(proto["output"].contains("operator"));
      ASSERT_TRUE(proto.contains("operators"));
      ASSERT_TRUE(proto.contains("connections"));

      // Collect all operator IDs in prototype
      std::set<std::string> proto_op_ids;
      bool proto_has_input = false, proto_has_output = false;
      bool proto_has_extract = false, proto_has_ma = false;
      for (const auto& proto_op : proto["operators"]) {
        ASSERT_TRUE(proto_op.contains("id"));
        proto_op_ids.insert(proto_op["id"].get<std::string>());

        if (proto_op["type"] == "Input") proto_has_input = true;
        if (proto_op["type"] == "Output") proto_has_output = true;
        if (proto_op["type"] == "VectorExtract") proto_has_extract = true;
        if (proto_op["type"] == "MovingAverage") proto_has_ma = true;
      }
      EXPECT_TRUE(proto_has_input) << "Prototype needs Input";
      EXPECT_TRUE(proto_has_output) << "Prototype needs Output";
      EXPECT_TRUE(proto_has_extract) << "Prototype needs VectorExtract";
      EXPECT_TRUE(proto_has_ma) << "Prototype needs MovingAverage";

      // All connection from/to IDs reference valid operator IDs
      for (const auto& conn : proto["connections"]) {
        std::string from_id = conn["from"].get<std::string>();
        std::string to_id = conn["to"].get<std::string>();
        EXPECT_TRUE(proto_op_ids.count(from_id))
            << "Connection 'from' ID '" << from_id
            << "' not found in prototype operators";
        EXPECT_TRUE(proto_op_ids.count(to_id))
            << "Connection 'to' ID '" << to_id
            << "' not found in prototype operators";
      }
    }
  }
  EXPECT_TRUE(has_input) << "Top-level program needs Input";
  EXPECT_TRUE(has_output) << "Top-level program needs Output";
  EXPECT_TRUE(has_keyed) << "Top-level program needs KeyedPipeline";
}

// ---------------------------------------------------------------------------
// Test 5: Table as FROM source — simple projection
// TODO: Review table-as-source semantics. The compiler currently treats tables
// like streams (schema resolution only), but runtime needs a KeyedVariable
// operator to read from table state. These tests verify compilation succeeds
// but the generated programs are not yet executable against actual tables.
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, TableAsFromSource) {
  register_table("orders",
                  {{"id", 0}, {"price", 1}, {"qty", 2}, {"status", 3}},
                  "orders_changelog");

  // Simple read from table — should compile as Tier 1
  auto r = compile_sql("SELECT id, price FROM orders LIMIT 10", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);
  EXPECT_EQ(r.select_tier, SelectTier::TIER1_READ);
  ASSERT_EQ(r.source_streams.size(), 1u);
  EXPECT_EQ(r.source_streams[0], "orders");
  EXPECT_EQ(r.field_map.at("id"), 0);
  EXPECT_EQ(r.field_map.at("price"), 1);
}

// ---------------------------------------------------------------------------
// Test 6: Table -> materialized view (GROUP BY on table source)
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, TableToMaterializedView) {
  register_table("orders",
                  {{"instrument_id", 0}, {"price", 1}, {"qty", 2}},
                  "orders_changelog");

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW order_stats AS "
      "SELECT instrument_id, SUM(qty) AS total_qty, COUNT(*) AS cnt "
      "FROM orders GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r.entity_name, "order_stats");
  EXPECT_EQ(r.view_type, ViewType::KEYED);
  ASSERT_EQ(r.source_streams.size(), 1u);
  EXPECT_EQ(r.source_streams[0], "orders");
  EXPECT_EQ(r.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r.field_map.at("total_qty"), 1);
  EXPECT_EQ(r.field_map.at("cnt"), 2);

  // Program should have KeyedPipeline just like stream-sourced views
  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      ASSERT_TRUE(op.contains("prototype"));
      bool has_cum_sum = false, has_count = false;
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "CumulativeSum") has_cum_sum = true;
        if (proto_op["type"] == "CountNumber") has_count = true;
      }
      EXPECT_TRUE(has_cum_sum);
      EXPECT_TRUE(has_count);
    }
  }
  EXPECT_TRUE(has_keyed);
}

// ---------------------------------------------------------------------------
// Test 7: Non-materialized VIEW — CREATE VIEW + chain from it
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, NonMaterializedViewChaining) {
  // v1: non-materialized view
  auto r1 = compile_sql(
      "CREATE VIEW live_stats AS "
      "SELECT instrument_id, SUM(quantity) AS vol, COUNT(*) AS cnt "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  EXPECT_EQ(r1.statement_type, StatementType::CREATE_VIEW);
  EXPECT_EQ(r1.entity_name, "live_stats");
  EXPECT_EQ(r1.view_type, ViewType::KEYED);
  EXPECT_EQ(r1.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r1.field_map.at("vol"), 1);
  EXPECT_EQ(r1.field_map.at("cnt"), 2);
  ASSERT_EQ(r1.source_streams.size(), 1u);
  EXPECT_EQ(r1.source_streams[0], "trades");

  // Register as VIEW (not MATERIALIZED_VIEW)
  register_view("live_stats", r1, EntityType::VIEW);

  // v2: materialized view on top of non-materialized view
  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW smoothed_stats AS "
      "SELECT instrument_id, MOVING_AVERAGE(vol, 10) AS smooth_vol "
      "FROM live_stats GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  EXPECT_EQ(r2.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r2.entity_name, "smoothed_stats");
  EXPECT_EQ(r2.view_type, ViewType::KEYED);
  ASSERT_EQ(r2.source_streams.size(), 1u);
  EXPECT_EQ(r2.source_streams[0], "live_stats");
  EXPECT_EQ(r2.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r2.field_map.at("smooth_vol"), 1);
}

// ---------------------------------------------------------------------------
// Test 8: Table -> mat view -> mat view (full chain from table source)
// ---------------------------------------------------------------------------

TEST_F(ViewChainingTest, TableToMatViewToMatView) {
  register_table("orders",
                  {{"instrument_id", 0}, {"price", 1}, {"qty", 2}},
                  "orders_changelog");

  // v1: aggregate from table
  auto r1 = compile_sql(
      "CREATE MATERIALIZED VIEW order_vol AS "
      "SELECT instrument_id, SUM(qty) AS total_qty "
      "FROM orders GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r1.has_errors()) << r1.errors[0].message;
  EXPECT_EQ(r1.source_streams[0], "orders");
  register_view("order_vol", r1);

  // v2: chain from v1
  auto r2 = compile_sql(
      "CREATE MATERIALIZED VIEW order_trends AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_qty, 10) AS avg_qty "
      "FROM order_vol GROUP BY instrument_id",
      catalog);
  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  EXPECT_EQ(r2.source_streams[0], "order_vol");
  EXPECT_EQ(r2.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r2.field_map.at("avg_qty"), 1);

  // v2 resolves total_qty at index 1 from v1's field_map
  auto program = json::parse(r2.program_json);
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "VectorExtract") {
          EXPECT_EQ(proto_op["index"], 1)
              << "Should extract total_qty at index 1 from order_vol";
        }
        if (proto_op["type"] == "MovingAverage") {
          EXPECT_EQ(proto_op["window_size"], 10);
        }
      }
    }
  }
}

}  // namespace
}  // namespace rtbot_sql::api
