#include "rtbot_sql/api/compiler.h"

#include <cmath>

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

// --- Test 8: CREATE TABLE with PRIMARY KEY ---

TEST_F(CompilerIntegrationTest, CreateTable) {
  auto r = compile_sql(
      "CREATE TABLE watchlist (account_id DOUBLE PRIMARY KEY, risk_score DOUBLE)",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_TABLE);
  EXPECT_EQ(r.entity_name, "watchlist");
  ASSERT_EQ(r.table_schema.columns.size(), 2u);
  EXPECT_EQ(r.table_schema.columns[0].name, "account_id");
  EXPECT_EQ(r.table_schema.columns[0].index, 0);
  EXPECT_EQ(r.table_schema.columns[1].name, "risk_score");
  ASSERT_EQ(r.table_schema.key_columns.size(), 1u);
  EXPECT_EQ(r.table_schema.key_columns[0], 0);
  EXPECT_EQ(r.table_schema.changelog_stream, "rtbot:sql:table:watchlist:changelog");
}

// --- Test 9: DELETE ---

TEST_F(CompilerIntegrationTest, DeleteFromTable) {
  // Register watchlist as a table
  TableSchema watchlist;
  watchlist.name = "watchlist";
  watchlist.columns = {{"account_id", 0}};
  watchlist.key_columns = {0};
  watchlist.changelog_stream = "rtbot:sql:table:watchlist:changelog";
  catalog.tables["watchlist"] = watchlist;

  auto r = compile_sql("DELETE FROM watchlist WHERE account_id = 42", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::DELETE);
  EXPECT_EQ(r.entity_name, "watchlist");
  ASSERT_EQ(r.delete_payload.size(), 2u);
  EXPECT_DOUBLE_EQ(r.delete_payload[0], 42.0);
  EXPECT_TRUE(std::isnan(r.delete_payload[1]));
}

// --- Test 10: Stream–TABLE JOIN compilation ---

TEST_F(CompilerIntegrationTest, StreamTableJoin) {
  // Register watchlist as a table
  TableSchema watchlist;
  watchlist.name = "watchlist";
  watchlist.columns = {{"account_id", 0}, {"risk_score", 1}};
  watchlist.key_columns = {0};
  watchlist.changelog_stream = "rtbot:sql:table:watchlist:changelog";
  catalog.tables["watchlist"] = watchlist;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW watched_trades AS "
      "SELECT t.instrument_id, t.price, t.account_id "
      "FROM trades t JOIN watchlist w ON t.account_id = w.account_id",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r.entity_name, "watched_trades");

  // source_streams should include both the stream and the table entity name
  ASSERT_EQ(r.source_streams.size(), 2u);
  EXPECT_EQ(r.source_streams[0], "trades");
  EXPECT_EQ(r.source_streams[1], "watchlist");

  // Verify graph contains KeyedVariable and Demultiplexer
  auto program = json::parse(r.program_json);
  bool has_kv = false, has_dmux = false, has_two_port_input = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedVariable") {
      has_kv = true;
      EXPECT_EQ(op["mode"], "exists");
    }
    if (op["type"] == "Demultiplexer") {
      has_dmux = true;
      EXPECT_EQ(op["portType"], "vector_number");
    }
    if (op["type"] == "Input") {
      const auto& pt = op["portTypes"];
      if (pt.is_array() && pt.size() == 2) has_two_port_input = true;
    }
  }
  EXPECT_TRUE(has_kv) << "graph should have a KeyedVariable";
  EXPECT_TRUE(has_dmux) << "graph should have a Demultiplexer";
  EXPECT_TRUE(has_two_port_input) << "Input should have 2 portTypes";
}

// --- Test 11: DROP with dependency checking ---

TEST_F(CompilerIntegrationTest, DropWithDependencies) {
  // Register a view that sources from trades
  ViewMeta v;
  v.name = "price_view";
  v.entity_type = EntityType::VIEW;
  v.view_type = ViewType::SCALAR;
  v.source_streams = {"trades"};
  catalog.views["price_view"] = v;

  // Attempting to drop trades should fail due to the dependent view
  auto r = compile_sql("DROP TABLE trades", catalog);
  EXPECT_TRUE(r.has_errors());
  EXPECT_NE(r.errors[0].message.find("price_view"), std::string::npos);

  // Dropping the view first should succeed
  auto r2 = compile_sql("DROP VIEW price_view", catalog);
  ASSERT_FALSE(r2.has_errors()) << r2.errors[0].message;
  EXPECT_EQ(r2.statement_type, StatementType::DROP);
  EXPECT_EQ(r2.drop_entity_name, "price_view");
}

// --- Test 12: DROP ---

TEST_F(CompilerIntegrationTest, DropMatView) {
  auto r = compile_sql("DROP MATERIALIZED VIEW bollinger", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::DROP);
  EXPECT_EQ(r.drop_entity_name, "bollinger");
  EXPECT_EQ(r.drop_entity_type, EntityType::MATERIALIZED_VIEW);
}

// --- Test 13: ORDER BY + LIMIT → TopK ---

TEST_F(CompilerIntegrationTest, OrderByLimit) {
  auto r = compile_sql(
      "SELECT instrument_id, quantity FROM trades ORDER BY quantity DESC LIMIT 3",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);

  // Verify TopK operator in program
  auto program = json::parse(r.program_json);
  bool has_topk = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "TopK") {
      has_topk = true;
      EXPECT_EQ(op["k"], 3);
      EXPECT_EQ(op["score_index"], r.field_map.at("quantity"));
      EXPECT_EQ(op.value("descending", ""), "true");
    }
  }
  EXPECT_TRUE(has_topk) << "program should have a TopK operator";
}

TEST_F(CompilerIntegrationTest, OrderByAscLimit) {
  auto r = compile_sql(
      "SELECT instrument_id, price FROM trades ORDER BY price ASC LIMIT 5",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;

  auto program = json::parse(r.program_json);
  for (const auto& op : program["operators"]) {
    if (op["type"] == "TopK") {
      EXPECT_EQ(op["k"], 5);
      EXPECT_EQ(op.value("descending", ""), "false");
    }
  }
}

TEST_F(CompilerIntegrationTest, OrderByWithoutLimitIsError) {
  auto r = compile_sql(
      "SELECT instrument_id FROM trades ORDER BY instrument_id DESC", catalog);
  EXPECT_TRUE(r.has_errors());
}

// --- Test 14: MOVING_MIN / MOVING_MAX → WindowMinMax ---

TEST_F(CompilerIntegrationTest, MovingMinMax) {
  auto r = compile_sql(
      "SELECT MOVING_MIN(price, 5) AS min_p, MOVING_MAX(price, 5) AS max_p "
      "FROM trades GROUP BY instrument_id",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);

  // Verify WindowMinMax operators in KeyedPipeline prototype
  auto program = json::parse(r.program_json);
  int wmin_count = 0, wmax_count = 0;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      ASSERT_TRUE(op.contains("prototype"));
      for (const auto& proto_op : op["prototype"]["operators"]) {
        if (proto_op["type"] == "WindowMinMax") {
          std::string mode = proto_op.value("mode", "");
          if (mode == "min") wmin_count++;
          if (mode == "max") wmax_count++;
          EXPECT_EQ(proto_op["window_size"], 5);
        }
      }
    }
  }
  EXPECT_EQ(wmin_count, 1) << "should have one WindowMinMax(min)";
  EXPECT_EQ(wmax_count, 1) << "should have one WindowMinMax(max)";
}

// --- Test 15: Composite GROUP BY → Linear hash + KeyedPipeline ---

TEST_F(CompilerIntegrationTest, CompositeGroupBy) {
  // trades has: instrument_id(0), price(1), quantity(2), account_id(3)
  // Add exchange_id to stream for this test via a fresh catalog
  CatalogSnapshot cat2;
  StreamSchema trades2{"trades2",
                       {{"instrument_id", 0}, {"exchange_id", 1},
                        {"quantity", 2}}};
  cat2.streams["trades2"] = trades2;

  auto r = compile_sql(
      "SELECT instrument_id, exchange_id, SUM(quantity) AS total "
      "FROM trades2 GROUP BY instrument_id, exchange_id",
      cat2);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::SELECT);

  // Verify computed key KeyedPipeline in graph (no Linear, no VectorCompose)
  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  bool has_fused_vector = false;
  bool has_fused_scalar = false;
  int extract_count = 0;
  for (const auto& op : program["operators"]) {
    EXPECT_NE(op["type"], "Linear")
        << "Linear should NOT be in outer graph (computed key mode)";
    EXPECT_NE(op["type"], "VectorCompose")
        << "VectorCompose should NOT be in outer graph (computed key mode)";
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      // Computed key mode: has keyColumnIndices, no key_index or keyCoefficients
      EXPECT_FALSE(op.contains("key_index"))
          << "Computed key mode should not have key_index";
      EXPECT_TRUE(op.contains("keyColumnIndices"))
          << "KeyedPipeline should have keyColumnIndices";
      EXPECT_FALSE(op.contains("keyCoefficients"))
          << "keyCoefficients should not be in JSON (computed internally by operator)";

      if (op.contains("prototype") && op["prototype"].contains("operators")) {
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "FusedExpressionVector") has_fused_vector = true;
          if (proto_op["type"] == "FusedExpression") has_fused_scalar = true;
          if (proto_op["type"] == "VectorExtract") extract_count++;
        }
      }
    }
  }
  EXPECT_TRUE(has_keyed) << "should have KeyedPipeline for routing";
  EXPECT_TRUE(has_fused_vector)
      << "Composite GROUP BY should use FusedExpressionVector in prototype";
  EXPECT_FALSE(has_fused_scalar)
      << "Composite GROUP BY should not use scalar FusedExpression in prototype";
  EXPECT_EQ(extract_count, 0)
      << "Composite GROUP BY fused path should not emit VectorExtract operators";

  // Field map should include both key columns and the aggregate
  EXPECT_NE(r.field_map.find("instrument_id"), r.field_map.end());
  EXPECT_NE(r.field_map.find("exchange_id"), r.field_map.end());
  EXPECT_NE(r.field_map.find("total"), r.field_map.end());
}

TEST_F(CompilerIntegrationTest, MultiFromCrossStreamSelect) {
  StreamSchema eth_trades{"eth_trades", {{"price", 0}, {"quantity", 1}}};
  catalog.streams["eth_trades"] = eth_trades;

  auto r = compile_sql(
      "SELECT t.price AS trade_price, e.price AS eth_price, "
      "t.price - e.price AS spread "
      "FROM trades t, eth_trades e LIMIT 10",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.select_tier, SelectTier::TIER3_EPHEMERAL);
  ASSERT_EQ(r.source_streams.size(), 2u);
  EXPECT_EQ(r.source_streams[0], "trades");
  EXPECT_EQ(r.source_streams[1], "eth_trades");

  EXPECT_EQ(r.field_map.at("trade_price"), 0);
  EXPECT_EQ(r.field_map.at("eth_price"), 1);
  EXPECT_EQ(r.field_map.at("spread"), 2);

  auto program = json::parse(r.program_json);
  bool has_two_port_input = false;
  bool has_subtraction = false;
  bool has_join = false;
  bool has_fused = false;
  int addition_count = 0;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "Input") {
      const auto& pt = op["portTypes"];
      if (pt.is_array() && pt.size() == 2) {
        has_two_port_input = true;
      }
    }
    if (op["type"] == "Subtraction") {
      has_subtraction = true;
    }
    if (op["type"] == "Join") {
      has_join = true;
    }
    if (op["type"] == "FusedExpression") {
      has_fused = true;
    }
    if (op["type"] == "Addition") {
      addition_count++;
    }
  }
  EXPECT_TRUE(has_two_port_input);
  EXPECT_FALSE(has_subtraction)
      << "Subtraction should be fused into FusedExpression bytecode";
  EXPECT_TRUE(has_fused)
      << "Cross-stream projection must compile to FusedExpression";
  EXPECT_FALSE(has_join)
      << "No extra explicit Join operators should be synthesized";
  EXPECT_EQ(addition_count, 0)
      << "No arithmetic Addition operator should be synthesized for sync";
}

// --- Test 16: Tier 2 cross-key aggregation via apply_tier2_filter ---
//
// Setup: instrument_stats is a keyed materialized view with schema
//   instrument_id(0), total_volume(1)
// Input rows simulate one per-key latest row:
//   key 1 → total_volume 100
//   key 2 → total_volume 200
//   key 3 → total_volume 150
//
// apply_tier2_filter must return a single output row with the aggregate result.

namespace {

CatalogSnapshot make_keyed_view_catalog() {
  CatalogSnapshot cat;
  ViewMeta stats;
  stats.name = "instrument_stats";
  stats.entity_type = EntityType::MATERIALIZED_VIEW;
  stats.view_type = ViewType::KEYED;
  stats.key_index = 0;
  stats.field_map = {{"instrument_id", 0}, {"total_volume", 1}};
  cat.views["instrument_stats"] = stats;
  return cat;
}

const std::vector<std::vector<double>> kKeyedRows = {
    {1.0, 100.0},
    {2.0, 200.0},
    {3.0, 150.0},
};

}  // namespace

TEST(Tier2CrossKeyAgg, Sum) {
  auto cat = make_keyed_view_catalog();
  auto result = apply_tier2_filter(
      "SELECT SUM(total_volume) AS total FROM instrument_stats",
      cat, kKeyedRows, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 450.0);  // 100 + 200 + 150
  EXPECT_EQ(result.field_map.at("total"), 0);
}

TEST(Tier2CrossKeyAgg, Count) {
  auto cat = make_keyed_view_catalog();
  auto result = apply_tier2_filter(
      "SELECT COUNT(*) AS cnt FROM instrument_stats",
      cat, kKeyedRows, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 3.0);
  EXPECT_EQ(result.field_map.at("cnt"), 0);
}

TEST(Tier2CrossKeyAgg, Avg) {
  auto cat = make_keyed_view_catalog();
  auto result = apply_tier2_filter(
      "SELECT AVG(total_volume) AS avg_vol FROM instrument_stats",
      cat, kKeyedRows, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 150.0);  // (100 + 200 + 150) / 3
  EXPECT_EQ(result.field_map.at("avg_vol"), 0);
}

TEST(Tier2CrossKeyAgg, MinMax) {
  auto cat = make_keyed_view_catalog();
  auto result = apply_tier2_filter(
      "SELECT MIN(total_volume) AS lo, MAX(total_volume) AS hi "
      "FROM instrument_stats",
      cat, kKeyedRows, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 100.0);  // MIN
  EXPECT_DOUBLE_EQ(result.rows[0][1], 200.0);  // MAX
  EXPECT_EQ(result.field_map.at("lo"), 0);
  EXPECT_EQ(result.field_map.at("hi"), 1);
}

TEST(Tier2CrossKeyAgg, MultipleAggsSingleCall) {
  auto cat = make_keyed_view_catalog();
  auto result = apply_tier2_filter(
      "SELECT SUM(total_volume) AS total, COUNT(*) AS cnt, "
      "AVG(total_volume) AS avg_vol FROM instrument_stats",
      cat, kKeyedRows, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  ASSERT_EQ(result.rows[0].size(), 3u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 450.0);  // SUM
  EXPECT_DOUBLE_EQ(result.rows[0][1], 3.0);    // COUNT
  EXPECT_DOUBLE_EQ(result.rows[0][2], 150.0);  // AVG
  EXPECT_EQ(result.field_map.at("total"), 0);
  EXPECT_EQ(result.field_map.at("cnt"), 1);
  EXPECT_EQ(result.field_map.at("avg_vol"), 2);
}

TEST(Tier2CrossKeyAgg, EmptyInputRows) {
  auto cat = make_keyed_view_catalog();
  std::vector<std::vector<double>> empty;
  auto result = apply_tier2_filter(
      "SELECT SUM(total_volume) AS total, COUNT(*) AS cnt "
      "FROM instrument_stats",
      cat, empty, -1);

  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_DOUBLE_EQ(result.rows[0][0], 0.0);  // SUM of nothing
  EXPECT_DOUBLE_EQ(result.rows[0][1], 0.0);  // COUNT of nothing
}

// --- Test 17: Alias expansion ---

TEST_F(CompilerIntegrationTest, AliasInWhere) {
  auto r = compile_sql(
      "SELECT 2*price AS dp FROM trades WHERE dp > 100 LIMIT 10", catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.select_tier, SelectTier::TIER2_SCAN);
  // field_map should reflect the alias
  EXPECT_NE(r.field_map.find("dp"), r.field_map.end());
}

TEST_F(CompilerIntegrationTest, AliasInHaving) {
  auto r = compile_sql(
      "SELECT instrument_id, AVG(price) AS avg_p FROM trades "
      "GROUP BY instrument_id HAVING avg_p > 100",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.select_tier, SelectTier::TIER3_EPHEMERAL);

  auto program = json::parse(r.program_json);
  bool has_keyed = false, has_cmp = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      if (op.contains("prototype")) {
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "CompareGT") has_cmp = true;
        }
      }
    }
  }
  EXPECT_TRUE(has_keyed) << "should have KeyedPipeline for GROUP BY";
  EXPECT_TRUE(has_cmp) << "HAVING avg_p > 100 should compile to CompareGT";
}

TEST_F(CompilerIntegrationTest, AliasChainInSelectAndHaving) {
  auto r = compile_sql(
      "SELECT instrument_id, 2*price AS dp, AVG(dp) AS avg_dp "
      "FROM trades GROUP BY instrument_id HAVING avg_dp > 100",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.select_tier, SelectTier::TIER3_EPHEMERAL);

  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") has_keyed = true;
  }
  EXPECT_TRUE(has_keyed) << "should have KeyedPipeline";
}

TEST_F(CompilerIntegrationTest, AggregateAliasInWhereIsError) {
  auto r = compile_sql(
      "SELECT AVG(price) AS avg_p FROM trades WHERE avg_p > 100 LIMIT 10",
      catalog);

  EXPECT_TRUE(r.has_errors());
  ASSERT_FALSE(r.errors.empty());
  EXPECT_NE(r.errors[0].message.find("HAVING"), std::string::npos)
      << "error message should mention HAVING; got: " << r.errors[0].message;
}

// --- Test 18: Error cases ---

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

// --- HAVING stream-vs-stream comparisons (Bollinger-band pattern) ---

// Bollinger upper-band alert: HAVING price > MA + 2*STDDEV
// Both sides of > compile to stream endpoints inside the prototype.
// Must compile successfully and emit no errors.
TEST_F(CompilerIntegrationTest, HavingBollingerUpperBand) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW bollinger_alerts AS "
      "  SELECT instrument_id, price,"
      "         MOVING_AVERAGE(price, 20) AS mid_band,"
      "         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band"
      "  FROM trades"
      "  GROUP BY instrument_id"
      "  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.entity_name, "bollinger_alerts");
}

// Bollinger two-sided alert (OR): price outside either band
TEST_F(CompilerIntegrationTest, HavingBollingerTwoSidedAlert) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW price_alerts AS "
      "  SELECT instrument_id, price"
      "  FROM trades"
      "  GROUP BY instrument_id"
      "  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)"
      "      OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.entity_name, "price_alerts");
}

// Fuel-leak alert: HAVING col < aggregate - constant
// col is a stream endpoint; (aggregate - constant) is also a stream endpoint
// (aggregate endpoint piped through an Add(-constant) scalar operator).
TEST_F(CompilerIntegrationTest, HavingColLessThanAggregateMinusConstant) {
  StreamSchema vs{"vehicle_telemetry",
                  {{"vehicle_id", 0}, {"fuel_level", 1}, {"speed_kmh", 2}}};
  catalog.streams["vehicle_telemetry"] = vs;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW fuel_alerts AS "
      "  SELECT vehicle_id, fuel_level,"
      "         MOVING_AVERAGE(fuel_level, 20) AS baseline_fuel"
      "  FROM vehicle_telemetry"
      "  GROUP BY vehicle_id"
      "  HAVING fuel_level < MOVING_AVERAGE(fuel_level, 20) - 10.0",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.entity_name, "fuel_alerts");
}

// Deviation-from-baseline: HAVING col - MA(col) > constant
// LHS arithmetic folds to a single endpoint; comparison is scalar CompareGT.
// This already worked before the fix; keep as regression test.
TEST_F(CompilerIntegrationTest, HavingDeviationFromMeanGreaterThanConstant) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW thermal_alerts AS "
      "  SELECT instrument_id, price,"
      "         MOVING_AVERAGE(price, 10) AS baseline"
      "  FROM trades"
      "  GROUP BY instrument_id"
      "  HAVING price - MOVING_AVERAGE(price, 10) > 5.0",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.entity_name, "thermal_alerts");
}

// --- Test: Alias expansion in GROUP BY ---

// GROUP BY should resolve SELECT aliases to their underlying expressions.
// Here "dev" is an alias for "instrument_id" (a plain ColumnRef), so the
// GROUP BY compiler should see the resolved ColumnRef and work correctly.
TEST_F(CompilerIntegrationTest, AliasInGroupBy) {
  auto r = compile_sql(
      "SELECT instrument_id AS dev, SUM(quantity) AS total "
      "FROM trades GROUP BY dev",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.select_tier, SelectTier::TIER3_EPHEMERAL);
  EXPECT_EQ(r.view_type, ViewType::KEYED);

  // The GROUP BY key column appears in field_map under its original column
  // name (instrument_id), and the aggregate uses its SELECT alias (total).
  // This matches the behavior of non-aliased GROUP BY queries.
  EXPECT_EQ(r.field_map.at("instrument_id"), 0);
  EXPECT_EQ(r.field_map.at("total"), 1);

  // Verify KeyedPipeline exists in the program
  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      EXPECT_EQ(op["key_index"], 0);
    }
  }
  EXPECT_TRUE(has_keyed) << "should have KeyedPipeline for GROUP BY";
}

// --- Segment-only GROUP BY produces SCALAR view type ---

TEST_F(CompilerIntegrationTest, SegmentOnlyGroupByProducesScalarView) {
  // Register a vibration stream for segment GROUP BY test
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW burst_stats AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.view_type, ViewType::SCALAR)
      << "Segment-only GROUP BY should produce SCALAR view";
  EXPECT_EQ(r.key_index, -1);

  // Verify Pipeline (not KeyedPipeline) in the program
  auto program = json::parse(r.program_json);
  bool has_pipeline = false;
  bool has_keyed_pipeline = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "Pipeline") has_pipeline = true;
    if (op["type"] == "KeyedPipeline") has_keyed_pipeline = true;
  }
  EXPECT_TRUE(has_pipeline) << "should have Pipeline for segment GROUP BY";
  EXPECT_FALSE(has_keyed_pipeline)
      << "segment-only should NOT have KeyedPipeline";
}

// --- Mixed GROUP BY produces KEYED view type ---

TEST_F(CompilerIntegrationTest, MixedGroupByProducesKeyedView) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW burst_stats AS "
      "SELECT device_id, SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY device_id, ABS(amplitude) > 0",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.view_type, ViewType::KEYED)
      << "Mixed GROUP BY should produce KEYED view";
  EXPECT_EQ(r.key_index, 0);

  // Verify KeyedPipeline with either nested Pipeline (legacy path) or a
  // BurstAggregate inside the prototype (collapsed path when the aggregate
  // bytecode is windowless).
  auto program = json::parse(r.program_json);
  bool has_keyed_pipeline = false;
  bool has_nested_pipeline = false;
  bool has_nested_burst = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed_pipeline = true;
      if (op.contains("prototype") && op["prototype"].contains("operators")) {
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "Pipeline") has_nested_pipeline = true;
          if (proto_op["type"] == "BurstAggregate") has_nested_burst = true;
        }
      }
    }
  }
  EXPECT_TRUE(has_keyed_pipeline) << "should have KeyedPipeline";
  EXPECT_TRUE(has_nested_pipeline || has_nested_burst)
      << "KeyedPipeline prototype should contain Pipeline or BurstAggregate";
}

// --- Segment-only HAVING → post-Pipeline filter ---

TEST_F(CompilerIntegrationTest, SegmentHavingPostFilter) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW filtered_bursts AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0 "
      "HAVING SUM(amplitude) > 50",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.view_type, ViewType::SCALAR)
      << "Segment-only GROUP BY with HAVING should produce SCALAR view";
  EXPECT_EQ(r.key_index, -1);

  // Verify Pipeline + Demultiplexer in the program (outer graph)
  auto program = json::parse(r.program_json);
  bool has_pipeline = false;
  bool has_demux = false;
  bool has_keyed_pipeline = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "Pipeline") has_pipeline = true;
    if (op["type"] == "Demultiplexer") has_demux = true;
    if (op["type"] == "KeyedPipeline") has_keyed_pipeline = true;
  }
  EXPECT_TRUE(has_pipeline) << "should have Pipeline for segment GROUP BY";
  EXPECT_TRUE(has_demux) << "should have Demultiplexer for HAVING post-filter";
  EXPECT_FALSE(has_keyed_pipeline)
      << "segment-only should NOT have KeyedPipeline";

  // field_map
  EXPECT_EQ(r.field_map.at("total"), 0);
  EXPECT_EQ(r.field_map.at("cnt"), 1);
}

// --- Composite persistent keys + segment expression ---

TEST_F(CompilerIntegrationTest, CompositeKeysWithSegmentExpression) {
  StreamSchema vibration{
      "vibration",
      {{"device_id", 0}, {"channel_id", 1}, {"amplitude", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW channel_stats AS "
      "SELECT device_id, channel_id, SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY device_id, channel_id, ABS(amplitude) > 0",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.view_type, ViewType::KEYED)
      << "Composite keys + segment should produce KEYED view";

  // Verify KeyedPipeline with either nested Pipeline (legacy path) or a
  // BurstAggregate inside the prototype (collapsed path).
  auto program = json::parse(r.program_json);
  bool has_keyed_pipeline = false;
  bool has_nested_pipeline = false;
  bool has_nested_burst = false;
  for (const auto& op : program["operators"]) {
    EXPECT_NE(op["type"], "Linear")
        << "Linear should NOT be in outer graph (computed key mode)";
    if (op["type"] == "KeyedPipeline") {
      has_keyed_pipeline = true;
      EXPECT_FALSE(op.contains("key_index"));
      EXPECT_TRUE(op.contains("keyColumnIndices"));
      EXPECT_FALSE(op.contains("keyCoefficients"));
      if (op.contains("prototype") && op["prototype"].contains("operators")) {
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "Pipeline") has_nested_pipeline = true;
          if (proto_op["type"] == "BurstAggregate") has_nested_burst = true;
        }
      }
    }
  }
  EXPECT_TRUE(has_keyed_pipeline) << "should have KeyedPipeline";
  EXPECT_TRUE(has_nested_pipeline || has_nested_burst)
      << "KeyedPipeline prototype should contain Pipeline or BurstAggregate";

  // Field map: computed key mode outputs directly (no VectorProject), 0-based
  EXPECT_EQ(r.field_map.at("device_id"), 0);
  EXPECT_EQ(r.field_map.at("channel_id"), 1);
  EXPECT_EQ(r.field_map.at("total"), 2);
  EXPECT_EQ(r.field_map.at("cnt"), 3);
}

// --- Preset-style: RMS trend pattern ---

TEST_F(CompilerIntegrationTest, PresetRmsTrendPattern) {
  StreamSchema vibration{
      "vibration",
      {{"device_id", 0}, {"channel_id", 1}, {"amplitude", 2}}};
  catalog.streams["vibration"] = vibration;

  // Pattern from rotating machinery presets:
  // RMS trend by device+channel, reset on zero-crossing
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW rms_trend AS "
      "SELECT device_id, channel_id, "
      "       MOVING_AVERAGE(amplitude, 50) AS rms_ma, "
      "       COUNT(*) AS sample_count "
      "FROM vibration "
      "GROUP BY device_id, channel_id, ABS(amplitude) > 0",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_MATERIALIZED_VIEW);
  EXPECT_EQ(r.entity_name, "rms_trend");
  EXPECT_EQ(r.view_type, ViewType::KEYED);

  // Verify structure: KeyedPipeline with nested Pipeline
  auto program = json::parse(r.program_json);
  bool has_keyed = false;
  bool has_inner_pipeline = false;
  bool has_inner_ma = false;
  for (const auto& op : program["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      has_keyed = true;
      if (op.contains("prototype") && op["prototype"].contains("operators")) {
        for (const auto& proto_op : op["prototype"]["operators"]) {
          if (proto_op["type"] == "Pipeline") {
            has_inner_pipeline = true;
            // Pipeline now uses native format (operators at top level, not
            // nested under "prototype")
            if (proto_op.contains("operators")) {
              for (const auto& inner_op : proto_op["operators"]) {
                if (inner_op["type"] == "MovingAverage") has_inner_ma = true;
              }
            }
          }
        }
      }
    }
  }
  EXPECT_TRUE(has_keyed) << "should have KeyedPipeline";
  EXPECT_TRUE(has_inner_pipeline) << "should have nested Pipeline";
  EXPECT_TRUE(has_inner_ma) << "inner prototype should have MovingAverage";

  // Field map: VectorProject strips hash key, so field indices are 0-based
  EXPECT_EQ(r.field_map.at("device_id"), 0);
  EXPECT_EQ(r.field_map.at("channel_id"), 1);
  EXPECT_EQ(r.field_map.at("rms_ma"), 2);
  EXPECT_EQ(r.field_map.at("sample_count"), 3);
}

// --- CreateStreamSyntax: verify that the raw STREAM keyword is accepted ---

TEST_F(CompilerIntegrationTest, CreateStreamSyntax) {
  // compile_sql must normalize "CREATE STREAM" → "CREATE TABLE" internally
  // before handing the SQL to libpg_query.  Pass the raw keyword directly
  // (no external normalization) and assert the result is a CREATE_STREAM.
  auto r = compile_sql(
      "CREATE STREAM orders (id DOUBLE PRECISION, price DOUBLE PRECISION, "
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
  EXPECT_FALSE(r.stream_schema.source.has_value());
}

// --- CreateStreamWithSource: optional FROM "..." metadata ---

TEST_F(CompilerIntegrationTest, CreateStreamWithSource) {
  auto r = compile_sql(
      R"(CREATE STREAM system_cpu(cpu DOUBLE, x TEXT) FROM "abcdeg")",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  EXPECT_EQ(r.statement_type, StatementType::CREATE_STREAM);
  EXPECT_EQ(r.entity_name, "system_cpu");
  ASSERT_EQ(r.stream_schema.columns.size(), 2u);
  ASSERT_TRUE(r.stream_schema.source.has_value());
  EXPECT_EQ(r.stream_schema.source.value(), "abcdeg");
}

TEST_F(CompilerIntegrationTest, CreateStreamWithSourceCaseInsensitive) {
  auto r = compile_sql(
      R"(CREATE STREAM system_cpu(cpu DOUBLE, x TEXT) from "my-source")",
      catalog);

  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  ASSERT_TRUE(r.stream_schema.source.has_value());
  EXPECT_EQ(r.stream_schema.source.value(), "my-source");
}

// --- CreateStreamMultiple: three streams compiled as a sequential program ---
// The current API compiles one statement at a time; this test models the
// real session workflow where each statement is compiled in order and the
// catalog is updated between steps so later statements see earlier definitions.

TEST_F(CompilerIntegrationTest, CreateStreamMultiple) {
  CatalogSnapshot session;

  auto r1 = compile_sql_expanded(
      R"(CREATE STREAM system_cpu(cpu DOUBLE, x TEXT) FROM "Aleluya";)",
      session, 1000);
  ASSERT_EQ(r1.results.size(), 1u);
  ASSERT_FALSE(r1.results[0].has_errors()) << r1.results[0].errors[0].message;
  EXPECT_EQ(r1.results[0].statement_type, StatementType::CREATE_STREAM);
  EXPECT_EQ(r1.results[0].entity_name, "system_cpu");
  ASSERT_EQ(r1.results[0].stream_schema.columns.size(), 2u);
  EXPECT_EQ(r1.results[0].stream_schema.columns[0].name, "cpu");
  EXPECT_EQ(r1.results[0].stream_schema.columns[0].type, ColumnType::DOUBLE);
  EXPECT_EQ(r1.results[0].stream_schema.columns[1].name, "x");
  EXPECT_EQ(r1.results[0].stream_schema.columns[1].type, ColumnType::TEXT);
  ASSERT_TRUE(r1.results[0].stream_schema.source.has_value());
  EXPECT_EQ(r1.results[0].stream_schema.source.value(), "Aleluya");
  session.streams["system_cpu"] = r1.results[0].stream_schema;

  auto r2 = compile_sql_expanded(
      R"(CREATE STREAM system_memory(memory DOUBLE) FROM "Maria Morena";)",
      session, 1000);
  ASSERT_EQ(r2.results.size(), 1u);
  ASSERT_FALSE(r2.results[0].has_errors()) << r2.results[0].errors[0].message;
  EXPECT_EQ(r2.results[0].statement_type, StatementType::CREATE_STREAM);
  EXPECT_EQ(r2.results[0].entity_name, "system_memory");
  ASSERT_EQ(r2.results[0].stream_schema.columns.size(), 1u);
  EXPECT_EQ(r2.results[0].stream_schema.columns[0].name, "memory");
  EXPECT_EQ(r2.results[0].stream_schema.columns[0].type, ColumnType::DOUBLE);
  ASSERT_TRUE(r2.results[0].stream_schema.source.has_value());
  EXPECT_EQ(r2.results[0].stream_schema.source.value(), "Maria Morena");
  session.streams["system_memory"] = r2.results[0].stream_schema;

  auto r3 = compile_sql_expanded(
      "CREATE STREAM system_decay(cpu DOUBLE, x TEXT);",
      session, 1000);
  ASSERT_EQ(r3.results.size(), 1u);
  ASSERT_FALSE(r3.results[0].has_errors()) << r3.results[0].errors[0].message;
  EXPECT_EQ(r3.results[0].statement_type, StatementType::CREATE_STREAM);
  EXPECT_EQ(r3.results[0].entity_name, "system_decay");
  ASSERT_EQ(r3.results[0].stream_schema.columns.size(), 2u);
  EXPECT_FALSE(r3.results[0].stream_schema.source.has_value());
  session.streams["system_decay"] = r3.results[0].stream_schema;

  EXPECT_EQ(session.streams.size(), 3u);
  EXPECT_TRUE(session.streams.count("system_cpu"));
  EXPECT_TRUE(session.streams.count("system_memory"));
  EXPECT_TRUE(session.streams.count("system_decay"));
}

}  // namespace
}  // namespace rtbot_sql::api
