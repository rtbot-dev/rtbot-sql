#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>

namespace rtbot_sql::api {
namespace {

using json = nlohmann::json;

class CompilerIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Register trades stream
    StreamSchema trades{"trades",
                        {{"instrument_id", 0}, {"price", 1},
                         {"quantity", 2}, {"account_id", 3}}};
    catalog.streams["trades"] = trades;
  }

  CatalogSnapshot catalog;
};

// --- Test 1: CREATE STREAM ---

TEST_F(CompilerIntegrationTest, CreateStream) {
  auto r = compile_sql(
      "CREATE TABLE orders (id DOUBLE PRECISION, price DOUBLE PRECISION, "
      "quantity DOUBLE PRECISION)",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_STREAM);
  EXPECT_EQ(r.entity_name, "orders");
  ASSERT_EQ(r.stream_schema.columns.size(), 3u);
  EXPECT_EQ(r.stream_schema.columns[0].name, "id");
  EXPECT_EQ(r.stream_schema.columns[0].index, 0);
  EXPECT_EQ(r.stream_schema.columns[1].name, "price");
  EXPECT_EQ(r.stream_schema.columns[2].name, "quantity");
}

// --- Test 2: INSERT ---

TEST_F(CompilerIntegrationTest, Insert) {
  auto r = compile_sql("INSERT INTO trades VALUES (1, 150.0, 200, 42)",
                        catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::INSERT);
  EXPECT_EQ(r.entity_name, "trades");
  ASSERT_EQ(r.insert_payload.size(), 4u);
  EXPECT_DOUBLE_EQ(r.insert_payload[0], 1.0);
  EXPECT_DOUBLE_EQ(r.insert_payload[1], 150.0);
  EXPECT_DOUBLE_EQ(r.insert_payload[2], 200.0);
  EXPECT_DOUBLE_EQ(r.insert_payload[3], 42.0);
}

// --- Test 3: Simple SELECT (Tier 1) ---

TEST_F(CompilerIntegrationTest, SimpleSelectTier1) {
  auto r = compile_sql(
      "SELECT instrument_id, price FROM trades LIMIT 10", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);
  EXPECT_EQ(r.select_tier, SelectTier::TIER1_READ);
}

// --- Test 4: SELECT with WHERE (Tier 2) ---

TEST_F(CompilerIntegrationTest, SelectWithWhereTier2) {
  auto r = compile_sql(
      "SELECT instrument_id, price, quantity FROM trades "
      "WHERE price > 100 LIMIT 100",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);
  EXPECT_EQ(r.select_tier, SelectTier::TIER2_SCAN);
}

// --- Test 5: GROUP BY ---

TEST_F(CompilerIntegrationTest, GroupBy) {
  auto r = compile_sql(
      "SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);
  EXPECT_EQ(r.select_tier, SelectTier::TIER3_EPHEMERAL);

  // Verify field map
  EXPECT_EQ(r.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r.field_map.at("total_qty"), 1);
  EXPECT_EQ(r.field_map.at("cnt"), 2);

  // Verify program_json contains KeyedPipeline
  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      EXPECT_EQ(op["key_index"], 0);
    }
  }
  EXPECT_TRUE(has_keyed);
}

// --- Test 6: GROUP BY with HAVING ---

TEST_F(CompilerIntegrationTest, GroupByWithHaving) {
  auto r = compile_sql(
      "SELECT instrument_id, SUM(quantity) AS total, COUNT(*) AS cnt "
      "FROM trades GROUP BY instrument_id HAVING COUNT(*) > 2",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;

  // Parse program to check inlined prototype contents in KeyedPipeline
  auto program = json::parse(r.program_json);

  bool has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;

      // Check inlined prototype operators for CompareGT and Demultiplexer
      ASSERT_TRUE(op.contains("prototype"));
      bool has_cmp = false, has_demux = false;
      int count_ops = 0;
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "CompareGT") has_cmp = true;
        if (proto_op["type"] == "Demultiplexer") has_demux = true;
        if (proto_op["type"] == "CountNumber") count_ops++;
      }
      EXPECT_TRUE(has_cmp);
      EXPECT_TRUE(has_demux);
      EXPECT_EQ(count_ops, 1);  // CountNumber shared between SELECT and HAVING
    }
  }
  EXPECT_TRUE(has_keyed);
}

// --- Test 7: Bollinger Bands (full worked example) ---

TEST_F(CompilerIntegrationTest, BollingerBands) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW bollinger AS "
      "SELECT instrument_id, price, "
      "MOVING_AVERAGE(price, 20) AS mid_band, "
      "MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band, "
      "MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r.entity_name, "bollinger");
  EXPECT_EQ(r.view_type, ViewType::KEYED);
  EXPECT_EQ(r.key_index, 0);
  ASSERT_EQ(r.source_streams.size(), 1u);
  EXPECT_EQ(r.source_streams[0], "trades");

  // Field map
  EXPECT_EQ(r.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r.field_map.at("price"), 1);
  EXPECT_EQ(r.field_map.at("mid_band"), 2);
  EXPECT_EQ(r.field_map.at("upper_band"), 3);
  EXPECT_EQ(r.field_map.at("lower_band"), 4);

  // De-duplication: count operator types in inlined prototype
  auto program = json::parse(r.program_json);
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      ASSERT_TRUE(op.contains("prototype"));
      int ma_count = 0, sd_count = 0, extract_count = 0;
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "MovingAverage") ma_count++;
        if (proto_op["type"] == "StandardDeviation") sd_count++;
        if (proto_op["type"] == "VectorExtract") extract_count++;
      }
      // MOVING_AVERAGE(price, 20) appears 3 times but should compile once
      EXPECT_EQ(ma_count, 1) << "MovingAverage should be deduplicated";
      // STDDEV(price, 20) appears 2 times but should compile once
      EXPECT_EQ(sd_count, 1) << "StandardDeviation should be deduplicated";
      // VectorExtract for price should appear once (deduplicated)
      EXPECT_EQ(extract_count, 1) << "VectorExtract should be deduplicated";
    }
  }
}

// --- Test 8: DROP ---

TEST_F(CompilerIntegrationTest, DropMatView) {
  auto r = compile_sql("DROP MATERIALIZED VIEW bollinger", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::DROP);
  EXPECT_EQ(r.drop_entity_name, "bollinger");
  EXPECT_EQ(r.drop_entity_type, EntityType::MATERIALIZED_VIEW);
}

// --- Test 9: Error cases ---

TEST_F(CompilerIntegrationTest, ErrorUnknownSource) {
  auto r = compile_sql("SELECT * FROM nonexistent LIMIT 10", catalog);
  EXPECT_TRUE(r.has_errors());
}

TEST_F(CompilerIntegrationTest, ErrorUnknownColumn) {
  auto r = compile_sql(
      "SELECT foo FROM trades LIMIT 10", catalog);
  EXPECT_TRUE(r.has_errors());
}

TEST_F(CompilerIntegrationTest, ErrorUnboundedStream) {
  auto r = compile_sql("SELECT * FROM trades", catalog);
  EXPECT_TRUE(r.has_errors());
}

TEST_F(CompilerIntegrationTest, ErrorParseFailure) {
  auto r = compile_sql("SELEC FROM WHERE", catalog);
  EXPECT_TRUE(r.has_errors());
}

}  // namespace
}  // namespace rtbot_sql::api
