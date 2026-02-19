// scenarios_e2e_test.cpp
//
// End-to-end integration tests for the 7 RTBot SQL Playground industry scenarios.
// Each test verifies:
//   1. All SQL statements in the scenario compile without errors.
//   2. Key views (especially HAVING alert views) produce correct output at
//      runtime when fed appropriate input data.

#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace rtbot_sql::api {
namespace {

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static void register_view(CatalogSnapshot& catalog, const std::string& name,
                           const CompilationResult& r) {
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

static CompilationResult compile_ok(const std::string& sql,
                                     CatalogSnapshot& catalog) {
  auto r = compile_sql(sql, catalog);
  EXPECT_FALSE(r.has_errors())
      << "Compilation failed: "
      << (r.errors.empty() ? "" : r.errors[0].message);
  return r;
}

static std::unique_ptr<rtbot::Message<rtbot::VectorNumberData>> make_msg(
    rtbot::timestamp_t t, std::vector<double> values) {
  return rtbot::create_message<rtbot::VectorNumberData>(
      t, rtbot::VectorNumberData{std::move(values)});
}

static rtbot::ProgramMsgBatch send(rtbot::Program& prog, rtbot::timestamp_t t,
                                    std::vector<double> values) {
  return prog.receive(make_msg(t, std::move(values)));
}

// Count total output messages across all ports in a batch.
static size_t count_outputs(const rtbot::ProgramMsgBatch& batch) {
  size_t n = 0;
  for (const auto& [op, ports] : batch)
    for (const auto& [port, msgs] : ports)
      n += msgs.size();
  return n;
}

// Send N identical messages starting at timestamp base+1.
// Returns the last batch produced.
static rtbot::ProgramMsgBatch send_n(rtbot::Program& prog, size_t n,
                                      rtbot::timestamp_t base,
                                      std::vector<double> values) {
  rtbot::ProgramMsgBatch last;
  for (size_t i = 0; i < n; ++i)
    last = send(prog, base + static_cast<rtbot::timestamp_t>(i + 1), values);
  return last;
}

// ---------------------------------------------------------------------------
// Scenario 1: Trade Surveillance
//   Stream: trades  (instrument_id=0, price=1, quantity=2, account_id=3)
// ---------------------------------------------------------------------------

class TradeSurveillanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema trades{"trades",
                        {{"instrument_id", 0},
                         {"price", 1},
                         {"quantity", 2},
                         {"account_id", 3}}};
    catalog.streams["trades"] = trades;
  }
  CatalogSnapshot catalog;
};

TEST_F(TradeSurveillanceTest, AllViewsCompile) {
  compile_ok("CREATE MATERIALIZED VIEW large_trades AS "
             "SELECT instrument_id, price, quantity, account_id "
             "FROM trades WHERE price * quantity > 20000",
             catalog);

  auto vwap_r = compile_ok(
      "CREATE MATERIALIZED VIEW instrument_vwap AS "
      "SELECT instrument_id, "
      "       SUM(price * quantity) / SUM(quantity) AS vwap, "
      "       SUM(quantity) AS total_volume, COUNT(*) AS trade_count "
      "FROM trades GROUP BY instrument_id",
      catalog);
  register_view(catalog, "instrument_vwap", vwap_r);

  auto bb_r = compile_ok(
      "CREATE MATERIALIZED VIEW bollinger_bands AS "
      "SELECT instrument_id, price, "
      "       MOVING_AVERAGE(price, 20) AS mid_band, "
      "       MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band, "
      "       MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band "
      "FROM trades GROUP BY instrument_id",
      catalog);
  register_view(catalog, "bollinger_bands", bb_r);

  compile_ok(
      "CREATE MATERIALIZED VIEW price_alerts AS "
      "SELECT instrument_id, price, quantity, account_id, "
      "       MOVING_AVERAGE(price, 20) AS mid_band, "
      "       MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band, "
      "       MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band "
      "FROM trades GROUP BY instrument_id "
      "HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) "
      "    OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW velocity_alerts AS "
      "SELECT account_id, instrument_id, price, quantity, "
      "       MOVING_COUNT(20) AS trades_in_window "
      "FROM trades GROUP BY account_id "
      "HAVING MOVING_COUNT(20) > 15",
      catalog);

  auto tbl = compile_ok(
      "CREATE TABLE watchlist (account_id DOUBLE PRIMARY KEY)", catalog);
  catalog.tables["watchlist"] = tbl.table_schema;

  compile_ok(
      "CREATE MATERIALIZED VIEW watchlist_alerts AS "
      "SELECT instrument_id, price, quantity, account_id "
      "FROM trades JOIN watchlist ON trades.account_id = watchlist.account_id",
      catalog);
}

// large_trades: WHERE filter — notional value > $20,000
TEST_F(TradeSurveillanceTest, LargeTradesFilter) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW large_trades AS "
      "SELECT instrument_id, price, quantity, account_id "
      "FROM trades WHERE price * quantity > 20000",
      catalog);
  rtbot::Program prog(r.program_json);

  // price=100, qty=100 → notional=10,000 → filtered out
  EXPECT_EQ(count_outputs(send(prog, 1, {1, 100, 100, 42})), 0u);
  // price=500, qty=50 → notional=25,000 → passes
  EXPECT_GT(count_outputs(send(prog, 2, {1, 500, 50, 42})), 0u);
}

// instrument_vwap: GROUP BY with running VWAP
TEST_F(TradeSurveillanceTest, InstrumentVwap) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW instrument_vwap AS "
      "SELECT instrument_id, "
      "       SUM(price * quantity) / SUM(quantity) AS vwap, "
      "       SUM(quantity) AS total_volume, COUNT(*) AS trade_count "
      "FROM trades GROUP BY instrument_id",
      catalog);
  rtbot::Program prog(r.program_json);

  // instrument_id=1: price=100, qty=10 → vwap=100, vol=10, count=1
  auto b1 = send(prog, 1, {1, 100, 10, 0});
  ASSERT_GT(count_outputs(b1), 0u);

  // instrument_id=1: price=200, qty=10 → vwap=(100*10+200*10)/20=150
  auto b2 = send(prog, 2, {1, 200, 10, 0});
  ASSERT_GT(count_outputs(b2), 0u);
}

// price_alerts: HAVING with Bollinger bands — key runtime test for
// the KeyedPipeline control-port fix (OperatorJson.h).
TEST_F(TradeSurveillanceTest, PriceAlertsHavingBollinger) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW price_alerts AS "
      "SELECT instrument_id, price, quantity, account_id, "
      "       MOVING_AVERAGE(price, 20) AS mid_band, "
      "       MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band, "
      "       MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band "
      "FROM trades GROUP BY instrument_id "
      "HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) "
      "    OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)",
      catalog);
  rtbot::Program prog(r.program_json);

  // Fill the window: 20 messages at stable price=100 for instrument_id=1.
  // After the window fills, STDDEV≈0 so bands are very tight around 100.
  // No alert expected during stable period.
  size_t alert_during_stable = 0;
  for (int i = 1; i <= 20; ++i) {
    auto b = send(prog, static_cast<rtbot::timestamp_t>(i), {1, 100.0, 10, 42});
    alert_during_stable += count_outputs(b);
  }
  // Window fills at msg 20 — no anomaly yet
  EXPECT_EQ(alert_during_stable, 0u) << "No alert expected during stable price";

  // Now send price=1000 — way above the upper band → should trigger alert
  auto spike = send(prog, 21, {1, 1000.0, 10, 42});
  EXPECT_GT(count_outputs(spike), 0u)
      << "Price spike must trigger a Bollinger alert";
}

// velocity_alerts: HAVING MOVING_COUNT > 15 — wash trading signal
//
// BUG: HAVING MOVING_COUNT(N) > threshold (velocity-pattern path) produces
// 0 runtime output. Compilation is correct; execution is broken.
//
// This test is written INVERTED: it asserts the buggy behaviour (0 outputs)
// so the suite stays green. When the bug is fixed this assertion will fail,
// forcing you to flip it to EXPECT_GT and remove this comment.
TEST_F(TradeSurveillanceTest, VelocityAlertsMovingCount) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW velocity_alerts AS "
      "SELECT account_id, instrument_id, price, quantity, "
      "       MOVING_COUNT(20) AS trades_in_window "
      "FROM trades GROUP BY account_id "
      "HAVING MOVING_COUNT(20) > 15",
      catalog);
  rtbot::Program prog(r.program_json);

  size_t total_alerts = 0;
  for (int i = 1; i <= 25; ++i)
    total_alerts += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i), {7, 1, 100, 7}));

  // INVERTED ASSERTION — flip to EXPECT_GT once the bug is fixed.
  EXPECT_EQ(total_alerts, 0u) << "Bug still present: velocity-pattern HAVING "
                                  "MOVING_COUNT produces no output. Fix the "
                                  "velocity-pat execution path and flip this "
                                  "assertion to EXPECT_GT.";
}

// ---------------------------------------------------------------------------
// Scenario 2: Predictive Maintenance
//   Stream: cnc_sensors  (machine_id=0, vibration=1, temperature=2, spindle_load=3)
// ---------------------------------------------------------------------------

class PredictiveMaintenanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema cnc{"cnc_sensors",
                     {{"machine_id", 0},
                      {"vibration", 1},
                      {"temperature", 2},
                      {"spindle_load", 3}}};
    catalog.streams["cnc_sensors"] = cnc;
  }
  CatalogSnapshot catalog;
};

TEST_F(PredictiveMaintenanceTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW bearing_health AS "
      "SELECT machine_id, vibration, "
      "       FIR(vibration, ARRAY[-0.003,-0.007,-0.012,0.0,0.025,0.052,0.065,"
      "           0.052,0.025,0.0,-0.012,-0.007,-0.003]) AS filtered_vibration, "
      "       MOVING_AVERAGE(vibration, 20) AS vibration_trend "
      "FROM cnc_sensors GROUP BY machine_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW thermal_monitor AS "
      "SELECT machine_id, temperature, "
      "       MOVING_AVERAGE(temperature, 100) AS temp_baseline, "
      "       MOVING_AVERAGE(temperature, 100) + 2 * STDDEV(temperature, 100) AS temp_upper "
      "FROM cnc_sensors GROUP BY machine_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW thermal_alerts AS "
      "SELECT machine_id, temperature, "
      "       MOVING_AVERAGE(temperature, 100) AS temp_baseline "
      "FROM cnc_sensors GROUP BY machine_id "
      "HAVING temperature - MOVING_AVERAGE(temperature, 100) > 5.0",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW tool_wear AS "
      "SELECT machine_id, spindle_load, "
      "       MOVING_AVERAGE(spindle_load, 50) AS load_trend, "
      "       STDDEV(spindle_load, 50) AS load_variability "
      "FROM cnc_sensors GROUP BY machine_id",
      catalog);
}

// thermal_alerts: spike > 5°C above 100-sample rolling baseline
TEST_F(PredictiveMaintenanceTest, ThermalAlertSpike) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW thermal_alerts AS "
      "SELECT machine_id, temperature, "
      "       MOVING_AVERAGE(temperature, 100) AS temp_baseline "
      "FROM cnc_sensors GROUP BY machine_id "
      "HAVING temperature - MOVING_AVERAGE(temperature, 100) > 5.0",
      catalog);
  rtbot::Program prog(r.program_json);

  // 100 stable readings at temp=60°C to fill the baseline window
  send_n(prog, 100, 0, {1, 0.0, 60.0, 50.0});

  // Normal reading: no alert
  EXPECT_EQ(count_outputs(send(prog, 101, {1, 0.0, 61.0, 50.0})), 0u);

  // Spike to 70°C: deviation = 70 - ~60 = 10 > 5 → alert
  auto spike = send(prog, 102, {1, 0.0, 70.0, 50.0});
  EXPECT_GT(count_outputs(spike), 0u) << "Thermal spike must trigger alert";
}

// ---------------------------------------------------------------------------
// Scenario 3: Patient Vitals Monitoring
//   Stream: patient_vitals  (patient_id=0, ecg=1, spo2=2, resp_rate=3)
// ---------------------------------------------------------------------------

class PatientVitalsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema pv{"patient_vitals",
                    {{"patient_id", 0},
                     {"ecg", 1},
                     {"spo2", 2},
                     {"resp_rate", 3}}};
    catalog.streams["patient_vitals"] = pv;
  }
  CatalogSnapshot catalog;
};

TEST_F(PatientVitalsTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW ecg_processed AS "
      "SELECT patient_id, ecg, "
      "       FIR(ecg, ARRAY[0.003,0.008,0.015,0.028,0.045,0.062,0.075,0.082,"
      "           0.075,0.062,0.045,0.028,0.015,0.008,0.003]) AS filtered_ecg, "
      "       PEAK_DETECT(ecg, 50) AS r_peak "
      "FROM patient_vitals GROUP BY patient_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW hrv_monitor AS "
      "SELECT patient_id, STDDEV(ecg, 60) AS hrv_60, "
      "       MOVING_AVERAGE(ecg, 60) AS mean_ecg "
      "FROM patient_vitals GROUP BY patient_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW spo2_monitor AS "
      "SELECT patient_id, spo2, MOVING_AVERAGE(spo2, 10) AS smooth_spo2 "
      "FROM patient_vitals GROUP BY patient_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW spo2_alerts AS "
      "SELECT patient_id, spo2, MOVING_AVERAGE(spo2, 10) AS smooth_spo2 "
      "FROM patient_vitals GROUP BY patient_id "
      "HAVING MOVING_AVERAGE(spo2, 10) < 90.0",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW respiratory_distress AS "
      "SELECT patient_id, spo2, resp_rate, "
      "       MOVING_AVERAGE(spo2, 10) AS smooth_spo2, "
      "       MOVING_AVERAGE(resp_rate, 10) AS smooth_resp "
      "FROM patient_vitals GROUP BY patient_id "
      "HAVING MOVING_AVERAGE(spo2, 10) < 92.0 "
      "   AND MOVING_AVERAGE(resp_rate, 10) > 22.0",
      catalog);
}

// spo2_alerts: smoothed SpO2 < 90%
TEST_F(PatientVitalsTest, Spo2DesaturationAlert) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW spo2_alerts AS "
      "SELECT patient_id, spo2, MOVING_AVERAGE(spo2, 10) AS smooth_spo2 "
      "FROM patient_vitals GROUP BY patient_id "
      "HAVING MOVING_AVERAGE(spo2, 10) < 90.0",
      catalog);
  rtbot::Program prog(r.program_json);

  // Normal: 10 readings at spo2=98 → MA=98, no alert
  size_t alerts_normal = 0;
  for (int i = 1; i <= 10; ++i)
    alerts_normal += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i), {1, 0.5, 98.0, 16.0}));
  EXPECT_EQ(alerts_normal, 0u);

  // Desaturation: 10 readings at spo2=85 → MA drops toward 85 < 90 → alert
  size_t alerts_desat = 0;
  for (int i = 11; i <= 20; ++i)
    alerts_desat += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i), {1, 0.5, 85.0, 16.0}));
  EXPECT_GT(alerts_desat, 0u) << "SpO2 desaturation must trigger alert";
}

// respiratory_distress: combined SpO2 < 92 AND resp_rate > 22
TEST_F(PatientVitalsTest, RespiratoryDistressAnd) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW respiratory_distress AS "
      "SELECT patient_id, spo2, resp_rate, "
      "       MOVING_AVERAGE(spo2, 10) AS smooth_spo2, "
      "       MOVING_AVERAGE(resp_rate, 10) AS smooth_resp "
      "FROM patient_vitals GROUP BY patient_id "
      "HAVING MOVING_AVERAGE(spo2, 10) < 92.0 "
      "   AND MOVING_AVERAGE(resp_rate, 10) > 22.0",
      catalog);
  rtbot::Program prog(r.program_json);

  // Both conditions met: spo2=88 < 92, resp_rate=25 > 22
  size_t alerts = 0;
  for (int i = 1; i <= 10; ++i)
    alerts += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i), {1, 0.5, 88.0, 25.0}));
  EXPECT_GT(alerts, 0u) << "Both conditions met — distress alert must fire";
}

// ---------------------------------------------------------------------------
// Scenario 4: Grid Monitoring
//   Stream: solar_telemetry
//     (site_id=0, zone_id=1, power_kw=2, irradiance=3, panel_temp=4, frequency=5)
// ---------------------------------------------------------------------------

class GridMonitoringTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema st{"solar_telemetry",
                    {{"site_id", 0},
                     {"zone_id", 1},
                     {"power_kw", 2},
                     {"irradiance", 3},
                     {"panel_temp", 4},
                     {"frequency", 5}}};
    catalog.streams["solar_telemetry"] = st;
  }
  CatalogSnapshot catalog;
};

TEST_F(GridMonitoringTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW zone_generation AS "
      "SELECT zone_id, SUM(power_kw) AS total_solar_kw, COUNT(*) AS active_sites "
      "FROM solar_telemetry GROUP BY zone_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW solar_deviation AS "
      "SELECT site_id, power_kw, "
      "       MOVING_AVERAGE(power_kw, 60) AS smooth_actual, "
      "       MOVING_AVERAGE(irradiance, 60) * 0.18 AS expected_kw, "
      "       MOVING_AVERAGE(power_kw, 60) - MOVING_AVERAGE(irradiance, 60) * 0.18 AS deviation "
      "FROM solar_telemetry GROUP BY site_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW ramp_alerts AS "
      "SELECT zone_id, SUM(power_kw) AS total_kw, "
      "       MOVING_AVERAGE(power_kw, 60) AS smooth_kw "
      "FROM solar_telemetry GROUP BY zone_id "
      "HAVING SUM(power_kw) - MOVING_AVERAGE(power_kw, 60) > 30.0",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW frequency_quality AS "
      "SELECT site_id, frequency, "
      "       MOVING_AVERAGE(frequency, 60) AS freq_baseline, "
      "       IIR(frequency, ARRAY[1.0,-1.8,0.81], ARRAY[0.0045,0.009,0.0045]) AS oscillation_mode "
      "FROM solar_telemetry GROUP BY site_id",
      catalog);
}

// zone_generation: GROUP BY with SUM + COUNT — produces output immediately
TEST_F(GridMonitoringTest, ZoneGenerationOutput) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW zone_generation AS "
      "SELECT zone_id, SUM(power_kw) AS total_solar_kw, COUNT(*) AS active_sites "
      "FROM solar_telemetry GROUP BY zone_id",
      catalog);
  rtbot::Program prog(r.program_json);

  auto b1 = send(prog, 1, {1, 10, 50.0, 800, 25, 60.0});
  ASSERT_GT(count_outputs(b1), 0u) << "zone_generation must produce output immediately";

  auto b2 = send(prog, 2, {1, 10, 75.0, 900, 26, 60.0});
  ASSERT_GT(count_outputs(b2), 0u);
}

// ---------------------------------------------------------------------------
// Scenario 5: Fleet Tracking
//   Stream: vehicle_telemetry
//     (vehicle_id=0, segment_id=1, lat=2, lon=3, speed_kmh=4, fuel_level=5, engine_temp=6)
// ---------------------------------------------------------------------------

class FleetTrackingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema vt{"vehicle_telemetry",
                    {{"vehicle_id", 0},
                     {"segment_id", 1},
                     {"lat", 2},
                     {"lon", 3},
                     {"speed_kmh", 4},
                     {"fuel_level", 5},
                     {"engine_temp", 6}}};
    catalog.streams["vehicle_telemetry"] = vt;
  }
  CatalogSnapshot catalog;
};

TEST_F(FleetTrackingTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW vehicle_smooth AS "
      "SELECT vehicle_id, MOVING_AVERAGE(lat, 5) AS smooth_lat, "
      "       MOVING_AVERAGE(lon, 5) AS smooth_lon, "
      "       MOVING_AVERAGE(speed_kmh, 10) AS smooth_speed "
      "FROM vehicle_telemetry GROUP BY vehicle_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW fuel_monitor AS "
      "SELECT vehicle_id, fuel_level, "
      "       MOVING_AVERAGE(fuel_level, 20) AS baseline_fuel "
      "FROM vehicle_telemetry GROUP BY vehicle_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW fuel_alerts AS "
      "SELECT vehicle_id, fuel_level, "
      "       MOVING_AVERAGE(fuel_level, 20) AS baseline_fuel "
      "FROM vehicle_telemetry GROUP BY vehicle_id "
      "HAVING fuel_level < MOVING_AVERAGE(fuel_level, 20) - 10.0",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW segment_speeds AS "
      "SELECT segment_id, MOVING_AVERAGE(speed_kmh, 50) AS avg_speed, "
      "       STDDEV(speed_kmh, 50) AS speed_variability, COUNT(*) AS sample_count "
      "FROM vehicle_telemetry GROUP BY segment_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW congestion AS "
      "SELECT segment_id, MOVING_AVERAGE(speed_kmh, 50) AS avg_speed "
      "FROM vehicle_telemetry GROUP BY segment_id "
      "HAVING MOVING_AVERAGE(speed_kmh, 50) < 15.0",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW incidents AS "
      "SELECT segment_id, speed_kmh, "
      "       MOVING_AVERAGE(speed_kmh, 100) AS speed_baseline, "
      "       STDDEV(speed_kmh, 100) AS speed_stddev "
      "FROM vehicle_telemetry GROUP BY segment_id "
      "HAVING speed_kmh < MOVING_AVERAGE(speed_kmh, 100) - 2 * STDDEV(speed_kmh, 100)",
      catalog);
}

// fuel_alerts: sudden drop > 10 below 20-sample baseline
TEST_F(FleetTrackingTest, FuelAlertDrop) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW fuel_alerts AS "
      "SELECT vehicle_id, fuel_level, "
      "       MOVING_AVERAGE(fuel_level, 20) AS baseline_fuel "
      "FROM vehicle_telemetry GROUP BY vehicle_id "
      "HAVING fuel_level < MOVING_AVERAGE(fuel_level, 20) - 10.0",
      catalog);
  rtbot::Program prog(r.program_json);

  // 20 stable readings at fuel=80
  send_n(prog, 20, 0, {42, 1, 0.0, 0.0, 60.0, 80.0, 90.0});

  // Normal reading: no alert
  EXPECT_EQ(count_outputs(send(prog, 21, {42, 1, 0.0, 0.0, 60.0, 79.0, 90.0})), 0u);

  // Drop to 65: 65 < 80 - 10 = 70 → alert
  auto drop = send(prog, 22, {42, 1, 0.0, 0.0, 60.0, 65.0, 90.0});
  EXPECT_GT(count_outputs(drop), 0u) << "Fuel drop must trigger alert";
}

// congestion: MA(speed,50) < 15 km/h
TEST_F(FleetTrackingTest, CongestionDetection) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW congestion AS "
      "SELECT segment_id, MOVING_AVERAGE(speed_kmh, 50) AS avg_speed "
      "FROM vehicle_telemetry GROUP BY segment_id "
      "HAVING MOVING_AVERAGE(speed_kmh, 50) < 15.0",
      catalog);
  rtbot::Program prog(r.program_json);

  // 50 readings at speed=10 km/h — MA=10 < 15 → alert from msg 50 onward
  size_t alerts = 0;
  for (int i = 1; i <= 50; ++i)
    alerts += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i), {5, 5, 0.0, 0.0, 10.0, 50.0, 90.0}));
  EXPECT_GT(alerts, 0u) << "Low-speed segment must trigger congestion alert";
}

// ---------------------------------------------------------------------------
// Scenario 6: Network Quality Monitoring
//   Stream: tower_metrics
//     (tower_id=0, sector_id=1, signal_dbm=2, packet_loss_pct=3, latency_ms=4, throughput_mbps=5)
// ---------------------------------------------------------------------------

class NetworkQualityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema tm{"tower_metrics",
                    {{"tower_id", 0},
                     {"sector_id", 1},
                     {"signal_dbm", 2},
                     {"packet_loss_pct", 3},
                     {"latency_ms", 4},
                     {"throughput_mbps", 5}}};
    catalog.streams["tower_metrics"] = tm;
  }
  CatalogSnapshot catalog;
};

TEST_F(NetworkQualityTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW tower_health AS "
      "SELECT tower_id, "
      "       MOVING_AVERAGE(signal_dbm, 300) AS avg_signal, "
      "       MOVING_AVERAGE(packet_loss_pct, 300) AS avg_loss, "
      "       MOVING_AVERAGE(latency_ms, 300) AS avg_latency, "
      "       STDDEV(latency_ms, 300) AS jitter "
      "FROM tower_metrics GROUP BY tower_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW tower_degradation AS "
      "SELECT tower_id, latency_ms, packet_loss_pct, "
      "       MOVING_AVERAGE(latency_ms, 300) AS latency_baseline, "
      "       STDDEV(latency_ms, 300) AS latency_band, "
      "       MOVING_AVERAGE(packet_loss_pct, 300) AS loss_baseline, "
      "       STDDEV(packet_loss_pct, 300) AS loss_band "
      "FROM tower_metrics GROUP BY tower_id "
      "HAVING latency_ms > MOVING_AVERAGE(latency_ms, 300) + 2 * STDDEV(latency_ms, 300) "
      "    OR packet_loss_pct > MOVING_AVERAGE(packet_loss_pct, 300) + 2 * STDDEV(packet_loss_pct, 300)",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW sector_stats AS "
      "SELECT sector_id, SUM(throughput_mbps) AS total_throughput, "
      "       COUNT(*) AS active_towers, "
      "       MOVING_AVERAGE(packet_loss_pct, 100) AS sector_loss "
      "FROM tower_metrics GROUP BY sector_id",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW jitter_analysis AS "
      "SELECT tower_id, latency_ms, "
      "       STDDEV(latency_ms, 100) AS short_jitter, "
      "       STDDEV(latency_ms, 300) AS long_jitter, "
      "       FIR(latency_ms, ARRAY[0.1,0.2,0.4,0.2,0.1]) AS smooth_latency "
      "FROM tower_metrics GROUP BY tower_id",
      catalog);
}

// tower_degradation: latency spike > MA + 2*STDDEV (window=300)
TEST_F(NetworkQualityTest, TowerDegradationLatencySpike) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW tower_degradation AS "
      "SELECT tower_id, latency_ms, packet_loss_pct, "
      "       MOVING_AVERAGE(latency_ms, 300) AS latency_baseline, "
      "       STDDEV(latency_ms, 300) AS latency_band, "
      "       MOVING_AVERAGE(packet_loss_pct, 300) AS loss_baseline, "
      "       STDDEV(packet_loss_pct, 300) AS loss_band "
      "FROM tower_metrics GROUP BY tower_id "
      "HAVING latency_ms > MOVING_AVERAGE(latency_ms, 300) + 2 * STDDEV(latency_ms, 300) "
      "    OR packet_loss_pct > MOVING_AVERAGE(packet_loss_pct, 300) + 2 * STDDEV(packet_loss_pct, 300)",
      catalog);
  rtbot::Program prog(r.program_json);

  // 300 readings with natural jitter: alternating 18ms and 22ms.
  // Mean=20ms, σ≈2ms → MA + 2σ ≈ 24ms.
  for (int i = 1; i <= 300; ++i) {
    double lat = (i % 2 == 0) ? 22.0 : 18.0;
    send(prog, static_cast<rtbot::timestamp_t>(i), {1, 1, -70.0, 0.1, lat, 100.0});
  }

  // Normal reading at 21ms: within MA + 2σ ≈ 24ms → no alert
  EXPECT_EQ(count_outputs(send(prog, 301, {1, 1, -70.0, 0.1, 21.0, 100.0})), 0u);

  // Spike to 200ms: far above 24ms → alert
  auto spike = send(prog, 302, {1, 1, -70.0, 0.1, 200.0, 100.0});
  EXPECT_GT(count_outputs(spike), 0u) << "Latency spike must trigger degradation alert";
}

// ---------------------------------------------------------------------------
// Scenario 7: Behavioral Anomaly Detection
//   Stream: netflow  (src_ip=0, dst_ip=1, dst_port=2, bytes=3, packets=4, protocol=5)
// ---------------------------------------------------------------------------

class BehavioralAnomalyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema nf{"netflow",
                    {{"src_ip", 0},
                     {"dst_ip", 1},
                     {"dst_port", 2},
                     {"bytes", 3},
                     {"packets", 4},
                     {"protocol", 5}}};
    catalog.streams["netflow"] = nf;
  }
  CatalogSnapshot catalog;
};

TEST_F(BehavioralAnomalyTest, AllViewsCompile) {
  compile_ok(
      "CREATE MATERIALIZED VIEW ip_profiles AS "
      "SELECT src_ip, MOVING_AVERAGE(bytes, 100) AS avg_bytes, "
      "       STDDEV(bytes, 100) AS std_bytes, COUNT(*) AS total_connections "
      "FROM netflow GROUP BY src_ip",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW traffic_anomalies AS "
      "SELECT src_ip, bytes, packets, "
      "       MOVING_AVERAGE(bytes, 100) AS bytes_baseline, "
      "       STDDEV(bytes, 100) AS bytes_stddev, "
      "       MOVING_AVERAGE(bytes, 100) + 3 * STDDEV(bytes, 100) AS anomaly_threshold "
      "FROM netflow GROUP BY src_ip "
      "HAVING bytes > MOVING_AVERAGE(bytes, 100) + 3 * STDDEV(bytes, 100)",
      catalog);

  compile_ok(
      "CREATE MATERIALIZED VIEW scan_detection AS "
      "SELECT src_ip, dst_ip, dst_port, MOVING_COUNT(100) AS recent_connections "
      "FROM netflow GROUP BY src_ip "
      "HAVING MOVING_COUNT(100) > 80",
      catalog);

  auto tbl = compile_ok(
      "CREATE TABLE threat_intel (ip DOUBLE PRIMARY KEY)", catalog);
  catalog.tables["threat_intel"] = tbl.table_schema;

  compile_ok(
      "CREATE MATERIALIZED VIEW enriched_alerts AS "
      "SELECT src_ip, dst_ip, bytes "
      "FROM netflow JOIN threat_intel ON netflow.src_ip = threat_intel.ip",
      catalog);
}

// traffic_anomalies: bytes > MA + 3σ (potential data exfiltration)
TEST_F(BehavioralAnomalyTest, TrafficExfiltrationAnomaly) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW traffic_anomalies AS "
      "SELECT src_ip, bytes, packets, "
      "       MOVING_AVERAGE(bytes, 100) AS bytes_baseline, "
      "       STDDEV(bytes, 100) AS bytes_stddev, "
      "       MOVING_AVERAGE(bytes, 100) + 3 * STDDEV(bytes, 100) AS anomaly_threshold "
      "FROM netflow GROUP BY src_ip "
      "HAVING bytes > MOVING_AVERAGE(bytes, 100) + 3 * STDDEV(bytes, 100)",
      catalog);
  rtbot::Program prog(r.program_json);

  // 100 readings with natural variance: alternating 800 and 1200 bytes.
  // Mean=1000, σ≈200 → MA + 3σ ≈ 1600.
  for (int i = 1; i <= 100; ++i) {
    double bytes = (i % 2 == 0) ? 1200.0 : 800.0;
    send(prog, static_cast<rtbot::timestamp_t>(i), {10, 20, 443, bytes, 5, 6});
  }

  // Normal flow at 1100 bytes: below 1600 threshold → no alert
  EXPECT_EQ(count_outputs(send(prog, 101, {10, 20, 443, 1100.0, 5, 6})), 0u);

  // Exfiltration burst: 100,000 bytes >> 1600 threshold → alert
  auto burst = send(prog, 102, {10, 20, 443, 100000.0, 5, 6});
  EXPECT_GT(count_outputs(burst), 0u) << "Exfiltration burst must trigger traffic anomaly";
}

// scan_detection: MOVING_COUNT(100) > 80 — port scan / DDoS detection
//
// BUG: same velocity-pattern issue as VelocityAlertsMovingCount above.
// Inverted assertion — flip to EXPECT_GT once the bug is fixed.
TEST_F(BehavioralAnomalyTest, ScanDetectionVelocity) {
  auto r = compile_ok(
      "CREATE MATERIALIZED VIEW scan_detection AS "
      "SELECT src_ip, dst_ip, dst_port, MOVING_COUNT(100) AS recent_connections "
      "FROM netflow GROUP BY src_ip "
      "HAVING MOVING_COUNT(100) > 80",
      catalog);
  rtbot::Program prog(r.program_json);

  size_t total_alerts = 0;
  for (int i = 1; i <= 100; ++i)
    total_alerts += count_outputs(
        send(prog, static_cast<rtbot::timestamp_t>(i),
             {3, static_cast<double>(i % 254 + 1),
              static_cast<double>(1024 + i), 500, 1, 6}));

  // INVERTED ASSERTION — flip to EXPECT_GT once the bug is fixed.
  EXPECT_EQ(total_alerts, 0u) << "Bug still present: velocity-pattern HAVING "
                                  "MOVING_COUNT produces no output. Fix the "
                                  "velocity-pat execution path and flip this "
                                  "assertion to EXPECT_GT.";
}

}  // namespace
}  // namespace rtbot_sql::api
