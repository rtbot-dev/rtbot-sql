#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <cmath>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace rtbot_sql::api {
namespace {

using json = nlohmann::json;

class CorrelationDebugTest : public ::testing::Test {
 protected:
  std::unique_ptr<rtbot::Message<rtbot::VectorNumberData>> make_msg(
      rtbot::timestamp_t t, std::vector<double> values) {
    return rtbot::create_message<rtbot::VectorNumberData>(
        t, rtbot::VectorNumberData{std::move(values)});
  }

  std::vector<double> extract_output(const rtbot::ProgramMsgBatch& batch) {
    for (const auto& [op_id, op_batch] : batch) {
      for (const auto& [port, msgs] : op_batch) {
        if (!msgs.empty()) {
          auto* vec_msg = dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
              msgs[0].get());
          if (vec_msg) {
            return vec_msg->data.values;
          }
        }
      }
    }
    return {};
  }

  CompilationResult compile(const std::string& sql, CatalogSnapshot& catalog) {
    auto r = compile_sql(sql, catalog);
    EXPECT_FALSE(r.has_errors())
        << "Compilation failed: " << (r.errors.empty() ? "" : r.errors[0].message);
    return r;
  }

  void register_view(const std::string& name, const CompilationResult& r,
                      CatalogSnapshot& catalog) {
    ViewMeta meta{};
    meta.name = name;
    meta.entity_type = EntityType::MATERIALIZED_VIEW;
    meta.view_type = r.view_type;
    meta.field_map = r.field_map;
    meta.source_streams = r.source_streams;
    meta.program_json = r.program_json;
    meta.key_index = r.key_index;
    catalog.views[name] = meta;
  }
};

TEST_F(CorrelationDebugTest, BtcEthCorrelationDrift) {
  CatalogSnapshot catalog;

  // Define input streams (close, volume — minimal schema)
  StreamSchema btc{"btcusdt_klines", {{"close", 0}, {"volume", 1}}};
  StreamSchema eth{"ethusdt_klines", {{"close", 0}, {"volume", 1}}};
  catalog.streams["btcusdt_klines"] = btc;
  catalog.streams["ethusdt_klines"] = eth;

  // Layer 1: cross-join
  auto r1 = compile(
      "CREATE MATERIALIZED VIEW cross_stats AS "
      "SELECT b.close AS btc_price, e.close AS eth_price "
      "FROM btcusdt_klines b, ethusdt_klines e",
      catalog);
  register_view("cross_stats", r1, catalog);

  // Layer 2: correlation with WHERE guard
  auto r2 = compile(
      "CREATE MATERIALIZED VIEW correlation AS "
      "SELECT btc_price, eth_price, "
      "(MOVING_AVERAGE(btc_price * eth_price, 5) "
      " - MOVING_AVERAGE(btc_price, 5) * MOVING_AVERAGE(eth_price, 5)) "
      "/ (MOVING_STD(btc_price, 5) * MOVING_STD(eth_price, 5)) AS rho "
      "FROM cross_stats "
      "WHERE MOVING_STD(btc_price, 5) > 1 AND MOVING_STD(eth_price, 5) > 0.1",
      catalog);

  // Dump program JSON for debugging
  std::cout << "\n=== Program 1 (cross_stats) JSON ===" << std::endl;
  std::cout << r1.program_json << std::endl;
  std::cout << "\n=== Program 2 (correlation) JSON ===" << std::endl;
  std::cout << r2.program_json << std::endl;

  // Create programs
  rtbot::Program prog1(r1.program_json);
  rtbot::Program prog2(r2.program_json);

  auto rho_idx = r2.field_map.at("rho");
  auto btc_idx = r2.field_map.at("btc_price");
  auto eth_idx = r2.field_map.at("eth_price");

  // ── Scenario A: realistic feeding like the live mode ──
  // In live mode, BTC and ETH arrive independently. The cross-join latches
  // the latest value from each port and emits on every update after both
  // ports have been initialized.
  //
  // Simulate 1-second Binance klines: prices repeat a LOT, with occasional
  // 0.01 ticks. Feed BTC first, then ETH (like the JS loop does).

  double btc_base = 100123.45;
  double eth_base = 2567.89;

  int output_count = 0;
  int bad_count = 0;

  std::cout << "\n=== Correlation Debug Output ===" << std::endl;
  std::cout << "msg#  | BTC price     | ETH price    | rho" << std::endl;
  std::cout << "------|---------------|--------------|----------------" << std::endl;

  for (int i = 0; i < 500; ++i) {
    auto t = static_cast<rtbot::timestamp_t>(i + 1);

    // Most ticks: no change. Occasional tiny movement.
    double btc_delta = 0.0;
    double eth_delta = 0.0;
    if (i % 10 == 0) btc_delta = 0.01;
    if (i % 15 == 0) btc_delta = -0.01;
    if (i % 12 == 0) eth_delta = 0.01;
    if (i % 17 == 0) eth_delta = -0.01;
    if (i % 50 == 0) { btc_delta = 5.0; eth_delta = 0.5; }
    if (i % 70 == 0) { btc_delta = -3.0; eth_delta = -0.3; }

    btc_base += btc_delta;
    eth_base += eth_delta;

    // Feed BTC first (port i1), then ETH (port i2) — just like JS live loop
    auto batch_btc = prog1.receive(make_msg(t, {btc_base, 1.0}), "i1");

    // Collect any cross-join output from BTC feed
    auto cross_out_btc = extract_output(batch_btc);
    if (!cross_out_btc.empty()) {
      auto batch2 = prog2.receive(make_msg(t, cross_out_btc));
      auto out = extract_output(batch2);
      if (!out.empty()) {
        double rho = out[rho_idx];
        output_count++;
        bool bad = std::abs(rho) > 1.0;
        if (bad) bad_count++;
        std::cout << std::setw(5) << i << "b"
                  << " | " << std::fixed << std::setprecision(2) << std::setw(13) << out[btc_idx]
                  << " | " << std::setw(12) << out[eth_idx]
                  << " | " << std::setprecision(10) << std::setw(16) << rho
                  << (bad ? " *** BAD ***" : "") << std::endl;
      }
    }

    // Feed ETH (port i2)
    auto batch_eth = prog1.receive(make_msg(t, {eth_base, 1.0}), "i2");

    // Collect any cross-join output from ETH feed
    auto cross_out_eth = extract_output(batch_eth);
    if (!cross_out_eth.empty()) {
      auto batch2 = prog2.receive(make_msg(t, cross_out_eth));
      auto out = extract_output(batch2);
      if (!out.empty()) {
        double rho = out[rho_idx];
        output_count++;
        bool bad = std::abs(rho) > 1.0;
        if (bad) bad_count++;
        std::cout << std::setw(5) << i << "e"
                  << " | " << std::fixed << std::setprecision(2) << std::setw(13) << out[btc_idx]
                  << " | " << std::setw(12) << out[eth_idx]
                  << " | " << std::setprecision(10) << std::setw(16) << rho
                  << (bad ? " *** BAD ***" : "") << std::endl;
      }
    }
  }

  std::cout << "\n=== Summary ===" << std::endl;
  std::cout << "Total outputs: " << output_count << std::endl;
  std::cout << "Bad outputs (|rho| > 1): " << bad_count << std::endl;

  EXPECT_EQ(bad_count, 0) << "Found " << bad_count << " rho values outside [-1, 1]";
}

}  // namespace
}  // namespace rtbot_sql::api
