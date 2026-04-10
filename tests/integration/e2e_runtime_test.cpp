#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace rtbot_sql::api {
namespace {

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class E2eRuntimeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema trades{"trades",
                        {{"instrument_id", 0},
                         {"price", 1},
                         {"quantity", 2},
                         {"account_id", 3}}};
    catalog.streams["trades"] = trades;
  }

  // Compile SQL and assert no errors.
  CompilationResult compile(const std::string& sql) {
    auto r = compile_sql(sql, catalog);
    EXPECT_FALSE(r.has_errors())
        << "Compilation failed: " << (r.errors.empty() ? "" : r.errors[0].message);
    return r;
  }

  // Build a VectorNumberData message.
  std::unique_ptr<rtbot::Message<rtbot::VectorNumberData>> make_msg(
      rtbot::timestamp_t t, std::vector<double> values) {
    return rtbot::create_message<rtbot::VectorNumberData>(
        t, rtbot::VectorNumberData{std::move(values)});
  }

  // Send a message to a program and return the output batch.
  rtbot::ProgramMsgBatch send(rtbot::Program& program,
                               rtbot::timestamp_t t,
                               std::vector<double> values) {
    return program.receive(make_msg(t, std::move(values)));
  }

  // Extract the first VectorNumberData output values from a batch.
  // Returns empty if no output was produced.
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

  // Count total output messages in a batch.
  size_t count_outputs(const rtbot::ProgramMsgBatch& batch) {
    size_t count = 0;
    for (const auto& [op_id, op_batch] : batch) {
      for (const auto& [port, msgs] : op_batch) {
        count += msgs.size();
      }
    }
    return count;
  }

  // Register a compilation result as a view in the catalog.
  void register_view(const std::string& name, const CompilationResult& r) {
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

  CatalogSnapshot catalog;
};

// ---------------------------------------------------------------------------
// T1: WHERE filtering
// Materialized as a view so the compiler produces a full operator graph.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, WhereFiltering) {
  auto r = compile(
      "CREATE MATERIALIZED VIEW filtered AS "
      "SELECT instrument_id, price, quantity FROM trades "
      "WHERE price > 100");

  rtbot::Program program(r.program_json);

  // columns: [instrument_id=0, price=1, quantity=2, account_id=3]
  struct Row {
    double instrument_id, price, quantity, account_id;
    bool expect_output;
  };
  std::vector<Row> rows = {
      {1, 50, 10, 0, false},   // price <= 100
      {2, 150, 20, 0, true},   // price > 100
      {3, 80, 30, 0, false},   // price <= 100
      {4, 200, 40, 0, true},   // price > 100
      {5, 100, 50, 0, false},  // price == 100, not > 100
  };

  int output_count = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    auto batch = send(program, static_cast<rtbot::timestamp_t>(i + 1),
                      {rows[i].instrument_id, rows[i].price, rows[i].quantity,
                       rows[i].account_id});

    if (rows[i].expect_output) {
      ASSERT_GT(count_outputs(batch), 0u)
          << "Expected output for row " << i << " (price=" << rows[i].price << ")";
      auto out = extract_output(batch);
      ASSERT_FALSE(out.empty()) << "Output was empty for row " << i;
      // Projected columns: instrument_id, price, quantity
      EXPECT_DOUBLE_EQ(out[0], rows[i].instrument_id);
      EXPECT_DOUBLE_EQ(out[1], rows[i].price);
      EXPECT_DOUBLE_EQ(out[2], rows[i].quantity);
      ++output_count;
    } else {
      EXPECT_EQ(count_outputs(batch), 0u)
          << "Unexpected output for row " << i << " (price=" << rows[i].price << ")";
    }
  }
  EXPECT_EQ(output_count, 2);
}

// ---------------------------------------------------------------------------
// T2: GROUP BY with SUM and COUNT
// SELECT instrument_id, SUM(quantity) AS total, COUNT(*) AS cnt
// FROM trades GROUP BY instrument_id
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, GroupBySumCount) {
  auto r = compile(
      "SELECT instrument_id, SUM(quantity) AS total, COUNT(*) AS cnt "
      "FROM trades GROUP BY instrument_id");

  rtbot::Program program(r.program_json);

  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  // Feed rows for instrument_id=1: quantities 10, 20, 30
  {
    auto batch = send(program, 1, {1, 100, 10, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_DOUBLE_EQ(out[0], 1);            // instrument_id
    EXPECT_DOUBLE_EQ(out[total_idx], 10);   // cumulative sum
    EXPECT_DOUBLE_EQ(out[cnt_idx], 1);      // count
  }
  {
    auto batch = send(program, 2, {1, 200, 20, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_DOUBLE_EQ(out[total_idx], 30);   // 10 + 20
    EXPECT_DOUBLE_EQ(out[cnt_idx], 2);
  }
  {
    auto batch = send(program, 3, {1, 300, 30, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_DOUBLE_EQ(out[total_idx], 60);   // 10 + 20 + 30
    EXPECT_DOUBLE_EQ(out[cnt_idx], 3);
  }

  // Feed rows for instrument_id=2: quantities 5, 15
  {
    auto batch = send(program, 4, {2, 100, 5, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_DOUBLE_EQ(out[0], 2);            // instrument_id
    EXPECT_DOUBLE_EQ(out[total_idx], 5);    // first for key 2
    EXPECT_DOUBLE_EQ(out[cnt_idx], 1);
  }
  {
    auto batch = send(program, 5, {2, 200, 15, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_DOUBLE_EQ(out[total_idx], 20);   // 5 + 15
    EXPECT_DOUBLE_EQ(out[cnt_idx], 2);
  }
}

// ---------------------------------------------------------------------------
// T3: AVG (previously broken — produced no output)
// SELECT instrument_id, AVG(price) AS avg_price
// FROM trades GROUP BY instrument_id
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, AvgProducesOutput) {
  auto r = compile(
      "SELECT instrument_id, AVG(price) AS avg_price "
      "FROM trades GROUP BY instrument_id");

  rtbot::Program program(r.program_json);

  int avg_idx = r.field_map.at("avg_price");

  // Feed prices [10, 20, 30] for instrument_id=1
  // Expected cumulative averages: [10, 15, 20]
  {
    auto batch = send(program, 1, {1, 10, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty()) << "AVG must produce output on first message";
    EXPECT_DOUBLE_EQ(out[0], 1);              // instrument_id
    EXPECT_DOUBLE_EQ(out[avg_idx], 10.0);     // avg(10) = 10
  }
  {
    auto batch = send(program, 2, {1, 20, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty()) << "AVG must produce output on second message";
    EXPECT_DOUBLE_EQ(out[avg_idx], 15.0);     // avg(10, 20) = 15
  }
  {
    auto batch = send(program, 3, {1, 30, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty()) << "AVG must produce output on third message";
    EXPECT_DOUBLE_EQ(out[avg_idx], 20.0);     // avg(10, 20, 30) = 20
  }
}

// ---------------------------------------------------------------------------
// T4: MOVING_AVERAGE
// CREATE MATERIALIZED VIEW ma AS
// SELECT instrument_id, MOVING_AVERAGE(price, 3) AS ma3
// FROM trades GROUP BY instrument_id
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MovingAverage) {
  auto r = compile(
      "CREATE MATERIALIZED VIEW ma AS "
      "SELECT instrument_id, MOVING_AVERAGE(price, 3) AS ma3 "
      "FROM trades GROUP BY instrument_id");

  rtbot::Program program(r.program_json);

  int ma_idx = r.field_map.at("ma3");

  // Feed prices [10, 20, 30, 40, 50] for instrument_id=1
  // MA(3) produces output starting from the 3rd message (window fills)

  // Messages 1 and 2: window not full yet — may or may not produce output
  send(program, 1, {1, 10, 100, 0});
  send(program, 2, {1, 20, 100, 0});

  // Message 3: window is full — MA(10,20,30) = 20
  {
    auto batch = send(program, 3, {1, 30, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty()) << "MA should produce output when window fills";
    EXPECT_DOUBLE_EQ(out[0], 1);            // instrument_id
    EXPECT_NEAR(out[ma_idx], 20.0, 1e-9);  // (10+20+30)/3
  }
  // Message 4: MA(20,30,40) = 30
  {
    auto batch = send(program, 4, {1, 40, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_NEAR(out[ma_idx], 30.0, 1e-9);  // (20+30+40)/3
  }
  // Message 5: MA(30,40,50) = 40
  {
    auto batch = send(program, 5, {1, 50, 100, 0});
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    EXPECT_NEAR(out[ma_idx], 40.0, 1e-9);  // (30+40+50)/3
  }
}

// ---------------------------------------------------------------------------
// T5: Simple passthrough (projection only)
// Materialized as a view so the compiler produces a full operator graph.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, SimplePassthrough) {
  auto r = compile(
      "CREATE MATERIALIZED VIEW passthrough AS "
      "SELECT instrument_id, price FROM trades");

  rtbot::Program program(r.program_json);

  // Feed 3 rows and verify each passes through with correct projection
  struct Row {
    double instrument_id, price, quantity, account_id;
  };
  std::vector<Row> rows = {
      {1, 100.5, 10, 42},
      {2, 200.0, 20, 43},
      {3, 50.75, 30, 44},
  };

  for (size_t i = 0; i < rows.size(); ++i) {
    auto batch = send(program, static_cast<rtbot::timestamp_t>(i + 1),
                      {rows[i].instrument_id, rows[i].price, rows[i].quantity,
                       rows[i].account_id});

    ASSERT_GT(count_outputs(batch), 0u) << "Row " << i << " should produce output";
    auto out = extract_output(batch);
    ASSERT_FALSE(out.empty());
    // Projected columns: instrument_id, price
    EXPECT_DOUBLE_EQ(out[0], rows[i].instrument_id);
    EXPECT_DOUBLE_EQ(out[1], rows[i].price);
  }
}

// ---------------------------------------------------------------------------
// T6: PEAK_DETECT — sparse output on local maxima
// PeakDetector with window=3 emits the center value when it is strictly
// greater than both neighbours.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, PeakDetect) {
  auto r = compile(
      "CREATE MATERIALIZED VIEW peaks AS "
      "SELECT instrument_id, PEAK_DETECT(price, 3) AS peak "
      "FROM trades GROUP BY instrument_id");

  rtbot::Program program(r.program_json);

  int peak_idx = r.field_map.at("peak");

  // Feed prices for instrument_id=1:
  //   t=1: 10   (buffer not full)
  //   t=2: 30   (buffer fills [10,30,?] — not yet)
  //   t=3: 20   (buffer [10,30,20] — center=30 > 10,20 -> PEAK)
  //   t=4: 15   (buffer [30,20,15] — center=20, not > 30 -> no peak)
  //   t=5: 25   (buffer [20,15,25] — center=15, not peak)
  //   t=6: 10   (buffer [15,25,10] — center=25 > 15,10 -> PEAK)

  auto b1 = send(program, 1, {1, 10, 100, 0});
  EXPECT_EQ(count_outputs(b1), 0u);

  auto b2 = send(program, 2, {1, 30, 100, 0});
  EXPECT_EQ(count_outputs(b2), 0u);

  // t=3: peak at t=2 (price=30)
  auto b3 = send(program, 3, {1, 20, 100, 0});
  ASSERT_GT(count_outputs(b3), 0u) << "Expected peak output at t=3";
  auto out3 = extract_output(b3);
  ASSERT_FALSE(out3.empty());
  EXPECT_DOUBLE_EQ(out3[0], 1);              // instrument_id
  EXPECT_DOUBLE_EQ(out3[peak_idx], 30.0);    // peak value

  // t=4: no peak
  auto b4 = send(program, 4, {1, 15, 100, 0});
  EXPECT_EQ(count_outputs(b4), 0u);

  // t=5: no peak
  auto b5 = send(program, 5, {1, 25, 100, 0});
  EXPECT_EQ(count_outputs(b5), 0u);

  // t=6: peak at t=5 (price=25)
  auto b6 = send(program, 6, {1, 10, 100, 0});
  ASSERT_GT(count_outputs(b6), 0u) << "Expected peak output at t=6";
  auto out6 = extract_output(b6);
  ASSERT_FALSE(out6.empty());
  EXPECT_DOUBLE_EQ(out6[peak_idx], 25.0);
}

// ---------------------------------------------------------------------------
// T7: PEAK_DETECT per-key isolation
// Two instrument_ids should track peaks independently.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, PeakDetectPerKey) {
  auto r = compile(
      "CREATE MATERIALIZED VIEW peaks_keyed AS "
      "SELECT instrument_id, PEAK_DETECT(price, 3) AS peak "
      "FROM trades GROUP BY instrument_id");

  rtbot::Program program(r.program_json);

  int peak_idx = r.field_map.at("peak");

  // Key 1: prices [5, 20, 10] -> peak at center (20)
  send(program, 1, {1, 5, 100, 0});
  send(program, 2, {1, 20, 100, 0});
  auto b_k1 = send(program, 3, {1, 10, 100, 0});
  ASSERT_GT(count_outputs(b_k1), 0u) << "Key 1 should have a peak";
  EXPECT_DOUBLE_EQ(extract_output(b_k1)[peak_idx], 20.0);

  // Key 2: prices [100, 50, 80] -> no peak (center=50 is minimum)
  send(program, 4, {2, 100, 100, 0});
  send(program, 5, {2, 50, 100, 0});
  auto b_k2 = send(program, 6, {2, 80, 100, 0});
  EXPECT_EQ(count_outputs(b_k2), 0u) << "Key 2 should have no peak";

  // Key 2: continue [50, 80, 30] -> peak at center (80)
  auto b_k2b = send(program, 7, {2, 30, 100, 0});
  ASSERT_GT(count_outputs(b_k2b), 0u) << "Key 2 should now have a peak";
  EXPECT_DOUBLE_EQ(extract_output(b_k2b)[peak_idx], 80.0);
}

// ---------------------------------------------------------------------------
// T8: Nested view — two-level chain at runtime
// View A: per-instrument cumulative sum
// View B: moving average over A's output
// Feed raw trades to A, pipe A's output to B, verify B's results.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, NestedViewTwoLevel) {
  // View A: cumulative sum of quantity per instrument
  auto ra = compile(
      "CREATE MATERIALIZED VIEW vol AS "
      "SELECT instrument_id, SUM(quantity) AS total_vol "
      "FROM trades GROUP BY instrument_id");
  register_view("vol", ra);

  // View B: moving average of total_vol from A, window=3
  auto rb = compile(
      "CREATE MATERIALIZED VIEW vol_ma AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_vol, 3) AS avg_vol "
      "FROM vol GROUP BY instrument_id");

  rtbot::Program prog_a(ra.program_json);
  rtbot::Program prog_b(rb.program_json);

  int avg_vol_idx = rb.field_map.at("avg_vol");

  // Feed trades for instrument_id=1 with quantities [10, 20, 30, 40, 50]
  // A produces cumulative sums: [10, 30, 60, 100, 150]
  // B computes MA(3) on those:
  //   msg 1: 10  (window not full)
  //   msg 2: 30  (window not full)
  //   msg 3: 60  -> MA(10,30,60) = 33.333...
  //   msg 4: 100 -> MA(30,60,100) = 63.333...
  //   msg 5: 150 -> MA(60,100,150) = 103.333...

  std::vector<double> quantities = {10, 20, 30, 40, 50};
  std::vector<std::vector<double>> a_outputs;

  for (size_t i = 0; i < quantities.size(); ++i) {
    auto batch_a = send(prog_a, static_cast<rtbot::timestamp_t>(i + 1),
                        {1, 100, quantities[i], 0});
    auto out_a = extract_output(batch_a);
    ASSERT_FALSE(out_a.empty()) << "View A should produce output for msg " << i;
    a_outputs.push_back(out_a);
  }

  // Verify A's cumulative sums
  EXPECT_DOUBLE_EQ(a_outputs[0][ra.field_map.at("total_vol")], 10);
  EXPECT_DOUBLE_EQ(a_outputs[1][ra.field_map.at("total_vol")], 30);
  EXPECT_DOUBLE_EQ(a_outputs[2][ra.field_map.at("total_vol")], 60);
  EXPECT_DOUBLE_EQ(a_outputs[3][ra.field_map.at("total_vol")], 100);
  EXPECT_DOUBLE_EQ(a_outputs[4][ra.field_map.at("total_vol")], 150);

  // Feed A's outputs into B
  for (size_t i = 0; i < a_outputs.size(); ++i) {
    auto batch_b = prog_b.receive(
        make_msg(static_cast<rtbot::timestamp_t>(i + 1), a_outputs[i]));

    if (i < 2) {
      // Window not yet full — no output expected
      continue;
    }
    auto out_b = extract_output(batch_b);
    ASSERT_FALSE(out_b.empty())
        << "View B should produce output for msg " << i;
    EXPECT_DOUBLE_EQ(out_b[0], 1);  // instrument_id preserved

    if (i == 2) EXPECT_NEAR(out_b[avg_vol_idx], 100.0 / 3.0, 1e-9);
    if (i == 3) EXPECT_NEAR(out_b[avg_vol_idx], 190.0 / 3.0, 1e-9);
    if (i == 4) EXPECT_NEAR(out_b[avg_vol_idx], 310.0 / 3.0, 1e-9);
  }
}

// ---------------------------------------------------------------------------
// T9: Nested view — three-level chain at runtime
// A: SUM(quantity) per instrument
// B: MOVING_AVERAGE(total_vol, 3) from A
// C: MOVING_AVERAGE(avg_vol, 2) from B
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, NestedViewThreeLevel) {
  auto ra = compile(
      "CREATE MATERIALIZED VIEW chain_a AS "
      "SELECT instrument_id, SUM(quantity) AS total_vol "
      "FROM trades GROUP BY instrument_id");
  register_view("chain_a", ra);

  auto rb = compile(
      "CREATE MATERIALIZED VIEW chain_b AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_vol, 3) AS avg_vol "
      "FROM chain_a GROUP BY instrument_id");
  register_view("chain_b", rb);

  auto rc = compile(
      "CREATE MATERIALIZED VIEW chain_c AS "
      "SELECT instrument_id, MOVING_AVERAGE(avg_vol, 2) AS trend "
      "FROM chain_b GROUP BY instrument_id");

  rtbot::Program prog_a(ra.program_json);
  rtbot::Program prog_b(rb.program_json);
  rtbot::Program prog_c(rc.program_json);

  int trend_idx = rc.field_map.at("trend");

  // Feed 6 trades for instrument_id=1 with quantities [10, 20, 30, 40, 50, 60]
  // A cumulative sums: [10, 30, 60, 100, 150, 210]
  // B MA(3) on A:       [-, -, 33.33, 63.33, 103.33, 153.33]
  // C MA(2) on B:       [-, -, -, -, 48.33, 83.33, 128.33]
  //   Actually C gets: msg3=33.33 (not full), msg4=63.33 -> MA(33.33,63.33)=48.33,
  //                    msg5=103.33 -> MA(63.33,103.33)=83.33,
  //                    msg6=153.33 -> MA(103.33,153.33)=128.33

  std::vector<double> quantities = {10, 20, 30, 40, 50, 60};
  std::vector<std::vector<double>> b_outputs;

  for (size_t i = 0; i < quantities.size(); ++i) {
    auto t = static_cast<rtbot::timestamp_t>(i + 1);

    // A: raw trades -> cumulative sums
    auto batch_a = send(prog_a, t, {1, 100, quantities[i], 0});
    auto out_a = extract_output(batch_a);
    ASSERT_FALSE(out_a.empty());

    // B: A's output -> moving average
    auto batch_b = prog_b.receive(make_msg(t, out_a));
    auto out_b = extract_output(batch_b);

    if (!out_b.empty()) {
      b_outputs.push_back(out_b);

      // C: B's output -> trend
      auto batch_c = prog_c.receive(make_msg(t, out_b));
      auto out_c = extract_output(batch_c);

      if (b_outputs.size() >= 2) {
        // C should produce output once it has 2 inputs from B
        ASSERT_FALSE(out_c.empty())
            << "View C should produce output after B output #" << b_outputs.size();
        EXPECT_DOUBLE_EQ(out_c[0], 1);  // instrument_id

        // Verify trend values
        if (b_outputs.size() == 2) {
          // MA(33.33, 63.33) = 48.33
          EXPECT_NEAR(out_c[trend_idx], (100.0 / 3.0 + 190.0 / 3.0) / 2.0, 1e-9);
        }
        if (b_outputs.size() == 3) {
          // MA(63.33, 103.33) = 83.33
          EXPECT_NEAR(out_c[trend_idx], (190.0 / 3.0 + 310.0 / 3.0) / 2.0, 1e-9);
        }
        if (b_outputs.size() == 4) {
          // MA(103.33, 153.33) = 128.33
          EXPECT_NEAR(out_c[trend_idx], (310.0 / 3.0 + 460.0 / 3.0) / 2.0, 1e-9);
        }
      }
    }
  }

  // B should have produced 4 outputs (msgs 3-6)
  EXPECT_EQ(b_outputs.size(), 4u);
}

// ---------------------------------------------------------------------------
// T10: Nested view — aggregation then WHERE filtering
// View A: SUM per instrument
// View B: filter A's output where total > threshold
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, NestedViewAggThenFilter) {
  auto ra = compile(
      "CREATE MATERIALIZED VIEW agg AS "
      "SELECT instrument_id, SUM(quantity) AS total "
      "FROM trades GROUP BY instrument_id");
  register_view("agg", ra);

  auto rb = compile(
      "CREATE MATERIALIZED VIEW agg_filtered AS "
      "SELECT instrument_id, total FROM agg WHERE total > 50");

  rtbot::Program prog_a(ra.program_json);
  rtbot::Program prog_b(rb.program_json);

  // Feed quantities [10, 20, 30] for key=1 -> cumulative sums [10, 30, 60]
  // B filters on total > 50, so only the 3rd message (total=60) should pass
  std::vector<double> quantities = {10, 20, 30};

  for (size_t i = 0; i < quantities.size(); ++i) {
    auto t = static_cast<rtbot::timestamp_t>(i + 1);
    auto batch_a = send(prog_a, t, {1, 100, quantities[i], 0});
    auto out_a = extract_output(batch_a);
    ASSERT_FALSE(out_a.empty());

    auto batch_b = prog_b.receive(make_msg(t, out_a));
    if (i < 2) {
      EXPECT_EQ(count_outputs(batch_b), 0u)
          << "total=" << out_a[ra.field_map.at("total")]
          << " should be filtered out";
    } else {
      ASSERT_GT(count_outputs(batch_b), 0u) << "total=60 should pass filter";
      auto out_b = extract_output(batch_b);
      EXPECT_DOUBLE_EQ(out_b[0], 1);     // instrument_id
      EXPECT_DOUBLE_EQ(out_b[1], 60.0);  // total
    }
  }
}

// ---------------------------------------------------------------------------
// T11: Nested view — multi-key chaining
// Verify that keyed views correctly isolate state across keys when chained.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, NestedViewMultiKey) {
  auto ra = compile(
      "CREATE MATERIALIZED VIEW mk_vol AS "
      "SELECT instrument_id, SUM(quantity) AS total_vol, COUNT(*) AS cnt "
      "FROM trades GROUP BY instrument_id");
  register_view("mk_vol", ra);

  auto rb = compile(
      "CREATE MATERIALIZED VIEW mk_avg AS "
      "SELECT instrument_id, MOVING_AVERAGE(total_vol, 2) AS avg_vol "
      "FROM mk_vol GROUP BY instrument_id");

  rtbot::Program prog_a(ra.program_json);
  rtbot::Program prog_b(rb.program_json);

  int avg_vol_idx = rb.field_map.at("avg_vol");
  int total_vol_idx = ra.field_map.at("total_vol");

  // Interleave key=1 and key=2 trades
  // Key 1: qty [10, 20] -> cum_sum [10, 30] -> MA(2): [-, (10+30)/2=20]
  // Key 2: qty [5, 15]  -> cum_sum [5, 20]  -> MA(2): [-, (5+20)/2=12.5]

  // key=1, msg 1
  auto ba1 = send(prog_a, 1, {1, 100, 10, 0});
  auto oa1 = extract_output(ba1);
  ASSERT_FALSE(oa1.empty());
  EXPECT_DOUBLE_EQ(oa1[total_vol_idx], 10);
  prog_b.receive(make_msg(1, oa1));

  // key=2, msg 1
  auto ba2 = send(prog_a, 2, {2, 200, 5, 0});
  auto oa2 = extract_output(ba2);
  ASSERT_FALSE(oa2.empty());
  EXPECT_DOUBLE_EQ(oa2[total_vol_idx], 5);
  prog_b.receive(make_msg(2, oa2));

  // key=1, msg 2 — B should output MA for key 1
  auto ba3 = send(prog_a, 3, {1, 100, 20, 0});
  auto oa3 = extract_output(ba3);
  ASSERT_FALSE(oa3.empty());
  EXPECT_DOUBLE_EQ(oa3[total_vol_idx], 30);
  auto bb3 = prog_b.receive(make_msg(3, oa3));
  auto ob3 = extract_output(bb3);
  ASSERT_FALSE(ob3.empty()) << "B should output for key=1 after 2 messages";
  EXPECT_DOUBLE_EQ(ob3[0], 1);  // instrument_id=1
  EXPECT_NEAR(ob3[avg_vol_idx], 20.0, 1e-9);  // (10+30)/2

  // key=2, msg 2 — B should output MA for key 2
  auto ba4 = send(prog_a, 4, {2, 200, 15, 0});
  auto oa4 = extract_output(ba4);
  ASSERT_FALSE(oa4.empty());
  EXPECT_DOUBLE_EQ(oa4[total_vol_idx], 20);
  auto bb4 = prog_b.receive(make_msg(4, oa4));
  auto ob4 = extract_output(bb4);
  ASSERT_FALSE(ob4.empty()) << "B should output for key=2 after 2 messages";
  EXPECT_DOUBLE_EQ(ob4[0], 2);  // instrument_id=2
  EXPECT_NEAR(ob4[avg_vol_idx], 12.5, 1e-9);  // (5+20)/2
}

// ---------------------------------------------------------------------------
// T12: Multi-stream cross-join — two input streams at runtime
// Verifies that a program compiled from "FROM stream_a a, stream_b b"
// correctly receives data on separate input ports (i1 and i2) and
// produces cross-joined output.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MultiStreamCrossJoinRuntime) {
  // Register a second stream alongside the default "trades" stream.
  StreamSchema btc_klines{"btcusdt_klines", {{"close", 0}, {"volume", 1}}};
  StreamSchema eth_klines{"ethusdt_klines", {{"close", 0}, {"volume", 1}}};
  catalog.streams["btcusdt_klines"] = btc_klines;
  catalog.streams["ethusdt_klines"] = eth_klines;

  // Simple cross-join: select close prices from both streams
  auto r = compile(
      "CREATE MATERIALIZED VIEW cross_prices AS "
      "SELECT b.close AS btc_price, e.close AS eth_price, "
      "b.close - e.close AS spread "
      "FROM btcusdt_klines b, ethusdt_klines e");

  ASSERT_EQ(r.source_streams.size(), 2u);
  EXPECT_EQ(r.source_streams[0], "btcusdt_klines");
  EXPECT_EQ(r.source_streams[1], "ethusdt_klines");

  int btc_idx = r.field_map.at("btc_price");
  int eth_idx = r.field_map.at("eth_price");
  int spread_idx = r.field_map.at("spread");

  // Inspect program JSON to verify multi-port Input
  auto program_json = json::parse(r.program_json);
  bool has_two_port_input = false;
  for (const auto& op : program_json["operators"]) {
    if (op["type"] == "Input") {
      const auto& pt = op["portTypes"];
      if (pt.is_array() && pt.size() == 2) {
        has_two_port_input = true;
      }
    }
  }
  ASSERT_TRUE(has_two_port_input) << "Input operator must have 2 portTypes";

  rtbot::Program program(r.program_json);

  // Send BTC data to port i1 (stream 0: btcusdt_klines)
  // BTC: close=71000, volume=1.5
  auto batch1 = program.receive(
      make_msg(1000, {71000.0, 1.5}), "i1");

  // After sending to only one port, we may or may not get output yet.
  // The cross-join needs data on both ports.

  // Send ETH data to port i2 (stream 1: ethusdt_klines)
  // ETH: close=2500, volume=10.0
  auto batch2 = program.receive(
      make_msg(1000, {2500.0, 10.0}), "i2");

  // After feeding both ports, the cross-join should produce output
  // containing: btc_price=71000, eth_price=2500, spread=68500
  size_t total_outputs = count_outputs(batch1) + count_outputs(batch2);
  ASSERT_GT(total_outputs, 0u)
      << "Cross-join must produce output after feeding both streams";

  // Find the batch that has output
  auto out = extract_output(batch2);
  if (out.empty()) {
    out = extract_output(batch1);
  }
  ASSERT_FALSE(out.empty()) << "Should have output values from the cross-join";

  EXPECT_DOUBLE_EQ(out[btc_idx], 71000.0);
  EXPECT_DOUBLE_EQ(out[eth_idx], 2500.0);
  EXPECT_DOUBLE_EQ(out[spread_idx], 68500.0);
}

// ---------------------------------------------------------------------------
// T13: Multi-stream cross-join — multiple messages on time grid
// Verifies that repeatedly feeding aligned timestamps produces
// correct cross-join output for each pair.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MultiStreamCrossJoinTimeSeries) {
  StreamSchema btc{"btcusdt_klines", {{"close", 0}}};
  StreamSchema eth{"ethusdt_klines", {{"close", 0}}};
  catalog.streams["btcusdt_klines"] = btc;
  catalog.streams["ethusdt_klines"] = eth;

  auto r = compile(
      "CREATE MATERIALIZED VIEW cross_prices AS "
      "SELECT b.close AS btc_price, e.close AS eth_price "
      "FROM btcusdt_klines b, ethusdt_klines e");

  rtbot::Program program(r.program_json);

  int btc_idx = r.field_map.at("btc_price");
  int eth_idx = r.field_map.at("eth_price");

  // Feed 5 aligned data points
  struct Pair {
    double btc;
    double eth;
  };
  std::vector<Pair> data = {
      {70000, 2400},
      {70100, 2410},
      {70200, 2420},
      {70300, 2430},
      {70400, 2440},
  };

  size_t output_count = 0;
  for (size_t i = 0; i < data.size(); ++i) {
    auto t = static_cast<rtbot::timestamp_t>(i + 1);

    // Send BTC (port i1)
    auto batch_btc = program.receive(make_msg(t, {data[i].btc}), "i1");

    // Send ETH (port i2) at the same timestamp
    auto batch_eth = program.receive(make_msg(t, {data[i].eth}), "i2");

    // Check for output from either batch
    auto out = extract_output(batch_eth);
    if (out.empty()) out = extract_output(batch_btc);

    if (!out.empty()) {
      EXPECT_DOUBLE_EQ(out[btc_idx], data[i].btc)
          << "BTC price mismatch at step " << i;
      EXPECT_DOUBLE_EQ(out[eth_idx], data[i].eth)
          << "ETH price mismatch at step " << i;
      ++output_count;
    }
  }

  EXPECT_GT(output_count, 0u)
      << "Multi-stream cross-join must produce at least one output row";
}

// ===========================================================================
// Segment GROUP BY end-to-end tests
//
// These tests verify the FULL pipeline: SQL → compiler → operator graph JSON
// → rtbot runtime → actual data processing with segment-scoped computation.
//
// Pipeline with control port behavior:
//   - Control port receives the segment key (numeric value from the segment
//     expression, e.g. 1.0 for true, 0.0 for false).
//   - While control value stays the same: accumulate in internal operators.
//   - When control value changes: emit buffered accumulated result, reset
//     internals, start new segment with the new key.
//   - First message always starts a segment (no emission).
//   - Pipeline only emits at KEY TRANSITIONS.
// ===========================================================================

// ---------------------------------------------------------------------------
// T14: Segment-only GROUP BY — burst data pattern
//
// SQL: SELECT SUM(amplitude) AS total, COUNT(*) AS cnt
//      FROM vibration
//      GROUP BY ABS(amplitude) > 0
//
// Data: active → silence → active
// Expected emissions only at segment boundary transitions.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, SegmentGroupByBurstData) {
  // Register vibration stream
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile(
      "CREATE MATERIALIZED VIEW burst_stats AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0");

  EXPECT_EQ(r.view_type, ViewType::SCALAR)
      << "Segment-only GROUP BY should produce SCALAR view";
  EXPECT_EQ(r.key_index, -1);

  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  rtbot::Program program(r.program_json);

  // Collect all outputs as (timestamp, values) pairs for flexible validation
  struct OutputRow {
    rtbot::timestamp_t time;
    std::vector<double> values;
  };
  std::vector<OutputRow> all_outputs;

  // Helper to collect outputs from a batch
  auto collect = [&](const rtbot::ProgramMsgBatch& batch) {
    for (const auto& [op_id, op_batch] : batch) {
      for (const auto& [port, msgs] : op_batch) {
        for (const auto& msg : msgs) {
          auto* vec_msg =
              dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(msg.get());
          if (vec_msg) {
            all_outputs.push_back({vec_msg->time, vec_msg->data.values});
          }
        }
      }
    }
  };

  // Feed burst data: active → silence → active
  //
  // vibration columns: [device_id=0, amplitude=1, frequency=2]
  //
  // t=1: amplitude=5.0  → ABS(5)>0  = 1.0 (active) → first msg, key=1.0
  // t=2: amplitude=3.0  → ABS(3)>0  = 1.0           → same key, accumulate
  // t=3: amplitude=2.0  → ABS(2)>0  = 1.0           → same key, accumulate
  // t=4: amplitude=0.0  → ABS(0)>0  = 0.0 (silence) → KEY CHANGE! emit, reset
  // t=5: amplitude=0.0  → ABS(0)>0  = 0.0           → same key, accumulate
  // t=6: amplitude=4.0  → ABS(4)>0  = 1.0 (active)  → KEY CHANGE! emit, reset
  // t=7: amplitude=6.0  → ABS(6)>0  = 1.0           → same key, accumulate
  // t=8: amplitude=0.0  → ABS(0)>0  = 0.0 (silence) → KEY CHANGE! emit, reset

  struct InputRow {
    rtbot::timestamp_t t;
    double device_id, amplitude, frequency;
  };
  std::vector<InputRow> inputs = {
      {1, 1, 5.0, 100},  // active segment starts
      {2, 1, 3.0, 100},  // accumulating
      {3, 1, 2.0, 100},  // accumulating
      {4, 1, 0.0, 100},  // silence → boundary!
      {5, 1, 0.0, 100},  // silence continues
      {6, 1, 4.0, 100},  // active again → boundary!
      {7, 1, 6.0, 100},  // accumulating
      {8, 1, 0.0, 100},  // silence → boundary!
  };

  for (const auto& row : inputs) {
    auto batch =
        send(program, row.t, {row.device_id, row.amplitude, row.frequency});
    collect(batch);
  }

  // Pipeline emits at key transitions only.
  // Expected 3 emissions:
  //   1. At t=4: SUM(5+3+2)=10, COUNT=3  (active segment t=1,2,3)
  //   2. At t=6: SUM(0+0)=0, COUNT=2     (silence segment t=4,5)
  //   3. At t=8: SUM(4+6)=10, COUNT=2    (active segment t=6,7)
  ASSERT_EQ(all_outputs.size(), 3u)
      << "Pipeline should emit exactly 3 times at segment boundaries";

  // Emission 1: active segment (t=1,2,3) emitted at boundary t=4
  {
    SCOPED_TRACE("Emission 1: active segment t=1..3");
    const auto& out = all_outputs[0];
    EXPECT_EQ(out.time, 4u);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 10.0);  // SUM(5+3+2)
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 3.0);
  }

  // Emission 2: silence segment (t=4,5) emitted at boundary t=6
  {
    SCOPED_TRACE("Emission 2: silence segment t=4..5");
    const auto& out = all_outputs[1];
    EXPECT_EQ(out.time, 6u);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 0.0);  // SUM(0+0)
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 2.0);
  }

  // Emission 3: active segment (t=6,7) emitted at boundary t=8
  {
    SCOPED_TRACE("Emission 3: active segment t=6..7");
    const auto& out = all_outputs[2];
    EXPECT_EQ(out.time, 8u);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 10.0);  // SUM(4+6)
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 2.0);
  }
}

// ---------------------------------------------------------------------------
// T15: Segment-only GROUP BY — single transition
//
// Minimal test: just two messages with one key change.
// Verifies that the boundary emission contains the first segment's data.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, SegmentGroupBySingleTransition) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile(
      "CREATE MATERIALIZED VIEW single_transition AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0");

  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  rtbot::Program program(r.program_json);

  // t=1: amplitude=7 → active (key=1.0), first msg, no output
  auto b1 = send(program, 1, {1, 7.0, 100});
  EXPECT_EQ(count_outputs(b1), 0u) << "First message should not emit";

  // t=2: amplitude=0 → silence (key=0.0), KEY CHANGE → emit active segment
  auto b2 = send(program, 2, {1, 0.0, 100});
  ASSERT_GT(count_outputs(b2), 0u)
      << "Key transition should produce output";
  auto out2 = extract_output(b2);
  ASSERT_FALSE(out2.empty());
  EXPECT_DOUBLE_EQ(out2[total_idx], 7.0);  // SUM(7)
  EXPECT_DOUBLE_EQ(out2[cnt_idx], 1.0);    // COUNT=1
}

// ---------------------------------------------------------------------------
// T16: Mixed GROUP BY — persistent key + segment expression
//
// SQL: SELECT device_id, SUM(amplitude) AS total, COUNT(*) AS cnt
//      FROM vibration
//      GROUP BY device_id, ABS(amplitude) > 0
//
// Verifies per-device isolation + correct segment accumulation.
// Two devices with interleaved data and different burst patterns.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MixedGroupByBurstData) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile(
      "CREATE MATERIALIZED VIEW device_burst_stats AS "
      "SELECT device_id, SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY device_id, ABS(amplitude) > 0");

  EXPECT_EQ(r.view_type, ViewType::KEYED)
      << "Mixed GROUP BY should produce KEYED view";

  // Field map: KeyedPipeline prepends hash key at index 0
  // device_id at index 1, then aggregates
  int device_id_idx = r.field_map.at("device_id");
  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  rtbot::Program program(r.program_json);

  // Collect all outputs
  struct OutputRow {
    rtbot::timestamp_t time;
    std::vector<double> values;
  };
  std::vector<OutputRow> all_outputs;

  auto collect = [&](const rtbot::ProgramMsgBatch& batch) {
    for (const auto& [op_id, op_batch] : batch) {
      for (const auto& [port, msgs] : op_batch) {
        for (const auto& msg : msgs) {
          auto* vec_msg =
              dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(msg.get());
          if (vec_msg) {
            all_outputs.push_back({vec_msg->time, vec_msg->data.values});
          }
        }
      }
    }
  };

  // Feed interleaved data for two devices:
  //
  // Device 1: active(5, 3) → silence(0) → active(4)
  //   t=1: [1, 5.0, 100] active (key changes)
  //   t=3: [1, 3.0, 100] active
  //   t=5: [1, 0.0, 100] silence → emit dev1 active segment: SUM=8, CNT=2
  //   t=7: [1, 4.0, 100] active → emit dev1 silence segment: SUM=0, CNT=1
  //
  // Device 2: active(10) → silence(0, 0) → active(6)
  //   t=2: [2, 10.0, 200] active
  //   t=4: [2, 0.0, 200]  silence → emit dev2 active segment: SUM=10, CNT=1
  //   t=6: [2, 0.0, 200]  silence
  //   t=8: [2, 6.0, 200]  active → emit dev2 silence segment: SUM=0, CNT=2

  collect(send(program, 1, {1, 5.0, 100}));   // dev1: active start
  collect(send(program, 2, {2, 10.0, 200}));  // dev2: active start
  collect(send(program, 3, {1, 3.0, 100}));   // dev1: accumulate
  collect(send(program, 4, {2, 0.0, 200}));   // dev2: silence → emit
  collect(send(program, 5, {1, 0.0, 100}));   // dev1: silence → emit
  collect(send(program, 6, {2, 0.0, 200}));   // dev2: silence continue
  collect(send(program, 7, {1, 4.0, 100}));   // dev1: active → emit
  collect(send(program, 8, {2, 6.0, 200}));   // dev2: active → emit

  // Expected 4 emissions total (2 per device)
  ASSERT_EQ(all_outputs.size(), 4u)
      << "Should have 4 segment boundary emissions (2 per device)";

  // Sort by time for stable verification
  std::sort(all_outputs.begin(), all_outputs.end(),
            [](const auto& a, const auto& b) { return a.time < b.time; });

  // t=4: device 2 active segment (SUM=10, CNT=1)
  {
    SCOPED_TRACE("t=4: device 2 active segment");
    const auto& out = all_outputs[0];
    EXPECT_EQ(out.time, 4u);
    EXPECT_DOUBLE_EQ(out.values[device_id_idx], 2);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 10.0);
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 1.0);
  }

  // t=5: device 1 active segment (SUM=8, CNT=2)
  {
    SCOPED_TRACE("t=5: device 1 active segment");
    const auto& out = all_outputs[1];
    EXPECT_EQ(out.time, 5u);
    EXPECT_DOUBLE_EQ(out.values[device_id_idx], 1);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 8.0);
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 2.0);
  }

  // t=7: device 1 silence segment (SUM=0, CNT=1)
  {
    SCOPED_TRACE("t=7: device 1 silence segment");
    const auto& out = all_outputs[2];
    EXPECT_EQ(out.time, 7u);
    EXPECT_DOUBLE_EQ(out.values[device_id_idx], 1);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 0.0);
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 1.0);
  }

  // t=8: device 2 silence segment (SUM=0, CNT=2)
  {
    SCOPED_TRACE("t=8: device 2 silence segment");
    const auto& out = all_outputs[3];
    EXPECT_EQ(out.time, 8u);
    EXPECT_DOUBLE_EQ(out.values[device_id_idx], 2);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 0.0);
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 2.0);
  }
}

// ---------------------------------------------------------------------------
// T17: Segment GROUP BY with HAVING
//
// SQL: SELECT SUM(amplitude) AS total, COUNT(*) AS cnt
//      FROM vibration
//      GROUP BY ABS(amplitude) > 0
//      HAVING SUM(amplitude) > 5
//
// Only segment boundary emissions where total > 5 pass through.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, SegmentGroupByWithHaving) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile(
      "CREATE MATERIALIZED VIEW filtered_bursts AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0 "
      "HAVING SUM(amplitude) > 5");

  EXPECT_EQ(r.view_type, ViewType::SCALAR);
  EXPECT_EQ(r.key_index, -1);

  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  rtbot::Program program(r.program_json);

  // Collect all outputs
  struct OutputRow {
    rtbot::timestamp_t time;
    std::vector<double> values;
  };
  std::vector<OutputRow> all_outputs;

  auto collect = [&](const rtbot::ProgramMsgBatch& batch) {
    for (const auto& [op_id, op_batch] : batch) {
      for (const auto& [port, msgs] : op_batch) {
        for (const auto& msg : msgs) {
          auto* vec_msg =
              dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(msg.get());
          if (vec_msg) {
            all_outputs.push_back({vec_msg->time, vec_msg->data.values});
          }
        }
      }
    }
  };

  // Burst pattern: two active segments with different totals
  //
  // Active segment 1 (small): amplitude=[2, 1] → SUM=3 (< 5, filtered out)
  //   t=1: [1, 2.0, 100] active
  //   t=2: [1, 1.0, 100] active
  //   t=3: [1, 0.0, 100] silence → emit SUM=3, CNT=2 → HAVING filters it
  //
  // Silence segment: amplitude=[0] → SUM=0 (< 5, filtered out)
  //   t=4: [1, 0.0, 100] silence
  //
  // Active segment 2 (large): amplitude=[4, 6] → SUM=10 (> 5, passes HAVING)
  //   t=5: [1, 4.0, 100] active → emit silence SUM=0, CNT=2 → filtered
  //   t=6: [1, 6.0, 100] active
  //   t=7: [1, 0.0, 100] silence → emit SUM=10, CNT=2 → PASSES HAVING

  collect(send(program, 1, {1, 2.0, 100}));
  collect(send(program, 2, {1, 1.0, 100}));
  collect(send(program, 3, {1, 0.0, 100}));
  collect(send(program, 4, {1, 0.0, 100}));
  collect(send(program, 5, {1, 4.0, 100}));
  collect(send(program, 6, {1, 6.0, 100}));
  collect(send(program, 7, {1, 0.0, 100}));

  // Only 1 emission should pass HAVING (SUM=10 > 5)
  // The other emissions (SUM=3, SUM=0) are filtered out
  ASSERT_EQ(all_outputs.size(), 1u)
      << "Only segments with SUM > 5 should pass HAVING";

  {
    SCOPED_TRACE("HAVING-filtered emission: active segment 2");
    const auto& out = all_outputs[0];
    EXPECT_EQ(out.time, 7u);
    EXPECT_DOUBLE_EQ(out.values[total_idx], 10.0);  // SUM(4+6)
    EXPECT_DOUBLE_EQ(out.values[cnt_idx], 2.0);
  }
}

// ---------------------------------------------------------------------------
// T18: Segment GROUP BY — long sustained segment
//
// Verifies accumulation over many messages within a single segment,
// then correct emission at the boundary.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, SegmentGroupByLongSegment) {
  StreamSchema vibration{
      "vibration", {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}}};
  catalog.streams["vibration"] = vibration;

  auto r = compile(
      "CREATE MATERIALIZED VIEW long_burst AS "
      "SELECT SUM(amplitude) AS total, COUNT(*) AS cnt "
      "FROM vibration "
      "GROUP BY ABS(amplitude) > 0");

  int total_idx = r.field_map.at("total");
  int cnt_idx = r.field_map.at("cnt");

  rtbot::Program program(r.program_json);

  // Feed 100 active messages (amplitude=1.0), then one silence to trigger emit
  double expected_sum = 0.0;
  int num_active = 100;
  for (int i = 1; i <= num_active; ++i) {
    auto batch = send(program, static_cast<rtbot::timestamp_t>(i),
                      {1, 1.0, 100});
    // No output expected during active accumulation (no key change)
    EXPECT_EQ(count_outputs(batch), 0u)
        << "No output expected during accumulation at t=" << i;
    expected_sum += 1.0;
  }

  // Trigger boundary: amplitude=0 → key change from 1.0 to 0.0
  auto boundary_batch = send(program, num_active + 1, {1, 0.0, 100});
  ASSERT_GT(count_outputs(boundary_batch), 0u)
      << "Boundary transition should emit accumulated result";

  auto out = extract_output(boundary_batch);
  ASSERT_FALSE(out.empty());
  EXPECT_DOUBLE_EQ(out[total_idx], expected_sum);     // SUM of 100 × 1.0
  EXPECT_DOUBLE_EQ(out[cnt_idx], (double)num_active);  // COUNT = 100
}

// ===========================================================================
// Multi-stream timestamp synchronization tests
//
// These tests verify that the compiled RTBot program enforces strict timestamp
// synchronization for multi-source views. When two streams feed into a
// cross-join, output is produced ONLY when both streams send data at the SAME
// timestamp. Messages at non-matching timestamps are buffered by the operator
// and never combined — no output is produced.
//
// This is a critical architectural guarantee: the program controls time
// alignment, not the runtime. If the user wants to combine streams with
// different timestamps, they must explicitly declare how (windowing,
// resampling, etc.) in their SQL.
// ===========================================================================

// ---------------------------------------------------------------------------
// T-sync-1: Non-matching timestamps produce no output
//
// Stream A sends at t=300, t=500, t=700.
// Stream B sends at t=400, t=600, t=800.
// No timestamp ever matches → no output from the cross-join.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MultiStreamNonMatchingTimestampsProduceNoOutput) {
  StreamSchema stream_a{"stream_a", {{"value", 0}}};
  StreamSchema stream_b{"stream_b", {{"value", 0}}};
  catalog.streams["stream_a"] = stream_a;
  catalog.streams["stream_b"] = stream_b;

  auto r = compile(
      "CREATE MATERIALIZED VIEW combined AS "
      "SELECT a.value AS val_a, b.value AS val_b, "
      "a.value + b.value AS total "
      "FROM stream_a a, stream_b b");

  ASSERT_EQ(r.source_streams.size(), 2u);
  rtbot::Program program(r.program_json);

  size_t total_outputs = 0;

  // Interleave messages with non-matching timestamps
  total_outputs += count_outputs(program.receive(make_msg(300, {10.0}), "i1"));
  total_outputs += count_outputs(program.receive(make_msg(400, {20.0}), "i2"));
  total_outputs += count_outputs(program.receive(make_msg(500, {30.0}), "i1"));
  total_outputs += count_outputs(program.receive(make_msg(600, {40.0}), "i2"));
  total_outputs += count_outputs(program.receive(make_msg(700, {50.0}), "i1"));
  total_outputs += count_outputs(program.receive(make_msg(800, {60.0}), "i2"));

  EXPECT_EQ(total_outputs, 0u)
      << "Non-matching timestamps must never produce output. "
         "The program synchronizes on timestamps — if stream A sends at t=300 "
         "and stream B sends at t=400, the Addition operator waits for a "
         "matching timestamp on the other port that never arrives.";
}

// ---------------------------------------------------------------------------
// T-sync-2: Matching timestamps produce correct output
//
// Both streams send at the same timestamps (t=1000, t=2000, t=3000).
// Each pair should produce output with the correct combined values.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MultiStreamMatchingTimestampsProduceOutput) {
  StreamSchema stream_a{"stream_a", {{"value", 0}}};
  StreamSchema stream_b{"stream_b", {{"value", 0}}};
  catalog.streams["stream_a"] = stream_a;
  catalog.streams["stream_b"] = stream_b;

  auto r = compile(
      "CREATE MATERIALIZED VIEW combined AS "
      "SELECT a.value AS val_a, b.value AS val_b, "
      "a.value + b.value AS total "
      "FROM stream_a a, stream_b b");

  int a_idx = r.field_map.at("val_a");
  int b_idx = r.field_map.at("val_b");
  int total_idx = r.field_map.at("total");

  rtbot::Program program(r.program_json);

  struct TestCase {
    rtbot::timestamp_t t;
    double a_val;
    double b_val;
  };
  std::vector<TestCase> cases = {
      {1000, 10.0, 20.0},
      {2000, 30.0, 40.0},
      {3000, 50.0, 60.0},
  };

  for (const auto& tc : cases) {
    auto batch_a = program.receive(make_msg(tc.t, {tc.a_val}), "i1");
    auto batch_b = program.receive(make_msg(tc.t, {tc.b_val}), "i2");

    size_t outputs = count_outputs(batch_a) + count_outputs(batch_b);
    ASSERT_GT(outputs, 0u)
        << "Matching timestamp t=" << tc.t << " must produce output";

    auto out = extract_output(batch_b);
    if (out.empty()) out = extract_output(batch_a);
    ASSERT_FALSE(out.empty());

    EXPECT_DOUBLE_EQ(out[a_idx], tc.a_val)
        << "val_a mismatch at t=" << tc.t;
    EXPECT_DOUBLE_EQ(out[b_idx], tc.b_val)
        << "val_b mismatch at t=" << tc.t;
    EXPECT_DOUBLE_EQ(out[total_idx], tc.a_val + tc.b_val)
        << "total mismatch at t=" << tc.t;
  }
}

// ---------------------------------------------------------------------------
// T-sync-3: Mixed matching and non-matching timestamps
//
// Only the timestamps where both streams have data should produce output.
// Timestamps where only one stream fires should produce nothing.
// ---------------------------------------------------------------------------

TEST_F(E2eRuntimeTest, MultiStreamMixedTimestamps) {
  StreamSchema stream_a{"stream_a", {{"value", 0}}};
  StreamSchema stream_b{"stream_b", {{"value", 0}}};
  catalog.streams["stream_a"] = stream_a;
  catalog.streams["stream_b"] = stream_b;

  auto r = compile(
      "CREATE MATERIALIZED VIEW combined AS "
      "SELECT a.value AS val_a, b.value AS val_b "
      "FROM stream_a a, stream_b b");

  int a_idx = r.field_map.at("val_a");
  int b_idx = r.field_map.at("val_b");

  rtbot::Program program(r.program_json);

  // t=100: only A fires → no output
  auto b1 = program.receive(make_msg(100, {1.0}), "i1");
  EXPECT_EQ(count_outputs(b1), 0u) << "t=100: only A sent, no output expected";

  // t=200: only B fires → no output
  auto b2 = program.receive(make_msg(200, {2.0}), "i2");
  EXPECT_EQ(count_outputs(b2), 0u) << "t=200: only B sent, no output expected";

  // t=300: both fire → output expected
  auto b3a = program.receive(make_msg(300, {3.0}), "i1");
  auto b3b = program.receive(make_msg(300, {30.0}), "i2");
  size_t out_300 = count_outputs(b3a) + count_outputs(b3b);
  ASSERT_GT(out_300, 0u) << "t=300: both streams sent, output expected";
  auto out3 = extract_output(b3b);
  if (out3.empty()) out3 = extract_output(b3a);
  EXPECT_DOUBLE_EQ(out3[a_idx], 3.0);
  EXPECT_DOUBLE_EQ(out3[b_idx], 30.0);

  // t=400: only A fires → no output
  auto b4 = program.receive(make_msg(400, {4.0}), "i1");
  EXPECT_EQ(count_outputs(b4), 0u) << "t=400: only A sent, no output expected";

  // t=500: both fire → output expected
  auto b5a = program.receive(make_msg(500, {5.0}), "i1");
  auto b5b = program.receive(make_msg(500, {50.0}), "i2");
  size_t out_500 = count_outputs(b5a) + count_outputs(b5b);
  ASSERT_GT(out_500, 0u) << "t=500: both streams sent, output expected";
  auto out5 = extract_output(b5b);
  if (out5.empty()) out5 = extract_output(b5a);
  EXPECT_DOUBLE_EQ(out5[a_idx], 5.0);
  EXPECT_DOUBLE_EQ(out5[b_idx], 50.0);
}

// ---------------------------------------------------------------------------
// T-text-1: TEXT column round-trip via CREATE STREAM + INSERT
//
// Verifies that:
//   1. CREATE STREAM with TEXT column produces correct ColumnType in schema
//   2. INSERT with string literal resolves to dictionary ID
//   3. dictionary_updates contains the new encoding
// ---------------------------------------------------------------------------

TEST(E2ERuntimeTextTest, TextColumnRoundTrip) {
  CatalogSnapshot snap;
  auto create_result = compile_sql(
      "CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, "
      "value DOUBLE PRECISION)",
      snap);
  ASSERT_FALSE(create_result.has_errors());

  // Verify schema has correct column types
  ASSERT_EQ(create_result.stream_schema.columns.size(), 3u);
  EXPECT_EQ(create_result.stream_schema.columns[0].type, ColumnType::DOUBLE);
  EXPECT_EQ(create_result.stream_schema.columns[1].type, ColumnType::TEXT);
  EXPECT_EQ(create_result.stream_schema.columns[2].type, ColumnType::DOUBLE);

  // Register in catalog
  snap.streams["sensors"] = create_result.stream_schema;

  auto insert_result =
      compile_sql("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)", snap);
  ASSERT_FALSE(insert_result.has_errors());
  ASSERT_EQ(insert_result.insert_payload.size(), 3u);
  EXPECT_DOUBLE_EQ(insert_result.insert_payload[0], 1.0);
  EXPECT_DOUBLE_EQ(insert_result.insert_payload[1], 1.0);  // dictionary ID
  EXPECT_DOUBLE_EQ(insert_result.insert_payload[2], 42.0);

  // dictionary_updates should contain the new encoding
  ASSERT_EQ(insert_result.dictionary_updates.count("sensors.location"), 1u);
  EXPECT_EQ(insert_result.dictionary_updates.at("sensors.location").at(1.0),
            "Bay A");
}

// ---------------------------------------------------------------------------
// T-text-2: JSON serialization round-trip for catalog with TEXT + dictionaries
//
// Simulates the JSON serialization that happens in JNI/browser bindings:
//   1. Serialize a CatalogSnapshot with TEXT columns and dictionaries to JSON
//   2. Deserialize it back
//   3. Verify type info and dictionaries survive the round-trip
//   4. Use the deserialized catalog for INSERT
// ---------------------------------------------------------------------------

TEST(E2ERuntimeTextTest, JsonCatalogRoundTrip) {
  // Build a catalog with TEXT columns and pre-populated dictionaries
  CatalogSnapshot original;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {
      {"device_id", 0, ColumnType::DOUBLE},
      {"location", 1, ColumnType::TEXT},
      {"value", 2, ColumnType::DOUBLE},
  };
  original.streams["sensors"] = schema;
  original.dictionaries["sensors.location"] = {{1.0, "Bay A"}, {2.0, "Bay B"}};

  // Serialize to JSON (mimicking what result_to_json/catalog serialization does)
  json catalog_json;
  json streams_json = json::object();
  for (const auto& [name, s] : original.streams) {
    json sj;
    sj["name"] = s.name;
    json cols = json::array();
    for (const auto& c : s.columns) {
      json cj;
      cj["name"] = c.name;
      cj["index"] = c.index;
      cj["type"] = (c.type == ColumnType::TEXT) ? "TEXT" : "DOUBLE";
      cols.push_back(cj);
    }
    sj["columns"] = cols;
    streams_json[name] = sj;
  }
  catalog_json["streams"] = streams_json;

  // Serialize dictionaries
  json dict_json = json::object();
  for (const auto& [key, entries] : original.dictionaries) {
    json entry_json = json::object();
    for (const auto& [id, str] : entries) {
      entry_json[std::to_string(static_cast<int>(id))] = str;
    }
    dict_json[key] = entry_json;
  }
  catalog_json["dictionaries"] = dict_json;

  // Deserialize (mimicking catalog_from_json)
  CatalogSnapshot restored;
  auto j = catalog_json;
  if (j.contains("streams")) {
    for (const auto& [name, val] : j["streams"].items()) {
      StreamSchema s;
      s.name = val.at("name").get<std::string>();
      for (const auto& col : val.at("columns")) {
        ColumnType col_type = ColumnType::DOUBLE;
        if (col.contains("type") && col["type"].is_string()) {
          if (col["type"] == "TEXT") col_type = ColumnType::TEXT;
        }
        s.columns.push_back(
            {col.at("name").get<std::string>(), col.at("index").get<int>(),
             col_type});
      }
      restored.streams[name] = s;
    }
  }
  if (j.contains("dictionaries") && j["dictionaries"].is_object()) {
    for (auto& [key, entries] : j["dictionaries"].items()) {
      std::map<double, std::string> dict_entries;
      for (auto& [id_str, str_val] : entries.items()) {
        dict_entries[std::stod(id_str)] = str_val.get<std::string>();
      }
      restored.dictionaries[key] = dict_entries;
    }
  }

  // Verify the round-trip preserved types
  ASSERT_EQ(restored.streams.count("sensors"), 1u);
  const auto& restored_schema = restored.streams.at("sensors");
  ASSERT_EQ(restored_schema.columns.size(), 3u);
  EXPECT_EQ(restored_schema.columns[0].type, ColumnType::DOUBLE);
  EXPECT_EQ(restored_schema.columns[1].type, ColumnType::TEXT);
  EXPECT_EQ(restored_schema.columns[2].type, ColumnType::DOUBLE);

  // Verify dictionaries survived
  ASSERT_EQ(restored.dictionaries.count("sensors.location"), 1u);
  EXPECT_EQ(restored.dictionaries.at("sensors.location").at(1.0), "Bay A");
  EXPECT_EQ(restored.dictionaries.at("sensors.location").at(2.0), "Bay B");

  // Use restored catalog for INSERT — should reuse existing dictionary IDs
  auto insert_result = compile_sql(
      "INSERT INTO sensors VALUES (1, 'Bay A', 42.0)", restored);
  ASSERT_FALSE(insert_result.has_errors());
  ASSERT_EQ(insert_result.insert_payload.size(), 3u);
  EXPECT_DOUBLE_EQ(insert_result.insert_payload[1], 1.0);  // reused ID

  // New string should get next available ID
  auto insert_result2 = compile_sql(
      "INSERT INTO sensors VALUES (2, 'Bay C', 99.0)", restored);
  ASSERT_FALSE(insert_result2.has_errors());
  EXPECT_DOUBLE_EQ(insert_result2.insert_payload[1], 3.0);  // next ID after 2.0
}

}  // namespace
}  // namespace rtbot_sql::api
