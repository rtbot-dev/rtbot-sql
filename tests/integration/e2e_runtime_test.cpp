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

}  // namespace
}  // namespace rtbot_sql::api
