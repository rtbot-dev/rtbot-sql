#include "rtbot_sql/api/preprocessor.h"
#include <gtest/gtest.h>

using namespace rtbot_sql::api;

class PreprocessorTest : public ::testing::Test {};

TEST_F(PreprocessorTest, RegularSqlPassesThrough) {
  auto r = preprocess_sql("CREATE STREAM sensor (value DOUBLE)", 1000000);
  ASSERT_EQ(r.statements.size(), 1);
  EXPECT_EQ(r.statements[0], "CREATE STREAM sensor (value DOUBLE)");
  EXPECT_EQ(r.new_ts_units_per_second, -1);
}

TEST_F(PreprocessorTest, AlignedStreamTwoColumns) {
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)",
      1000000);
  // 2 streams + 2 _bin views + 2 _rs views + 1 combining materialized view = 7
  ASSERT_EQ(r.statements.size(), 7);
  // Per-column streams
  EXPECT_EQ(r.statements[0], "CREATE STREAM vibration (value DOUBLE)");
  EXPECT_EQ(r.statements[1], "CREATE STREAM bearing_temp (value DOUBLE)");
  // Binning views
  EXPECT_EQ(r.statements[2],
      "CREATE VIEW vibration_bin AS SELECT AVG(value) AS vibration "
      "FROM vibration GROUP BY FLOOR(TS() / 1000000)");
  EXPECT_EQ(r.statements[3],
      "CREATE VIEW bearing_temp_bin AS SELECT AVG(value) AS bearing_temp "
      "FROM bearing_temp GROUP BY FLOOR(TS() / 1000000)");
  // Resampler views (with TIMESHIFT to compensate one-bin latency)
  EXPECT_EQ(r.statements[4],
      "CREATE VIEW vibration_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(vibration, 1000000), -1000000) "
      "AS vibration FROM vibration_bin");
  EXPECT_EQ(r.statements[5],
      "CREATE VIEW bearing_temp_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(bearing_temp, 1000000), -1000000) "
      "AS bearing_temp FROM bearing_temp_bin");
  // Combining materialized view references _rs views
  EXPECT_EQ(r.statements[6],
      "CREATE MATERIALIZED VIEW input AS SELECT vibration, bearing_temp "
      "FROM vibration_rs, bearing_temp_rs");
}

TEST_F(PreprocessorTest, AlignedStreamSingleColumn) {
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM data (pressure DOUBLE) BIN(500ms)", 1000000);
  // 1 stream + 1 _bin + 1 _rs + 1 combining = 4
  ASSERT_EQ(r.statements.size(), 4);
  EXPECT_EQ(r.statements[0], "CREATE STREAM pressure (value DOUBLE)");
  EXPECT_EQ(r.statements[1],
      "CREATE VIEW pressure_bin AS SELECT AVG(value) AS pressure "
      "FROM pressure GROUP BY FLOOR(TS() / 500000)");
  EXPECT_EQ(r.statements[2],
      "CREATE VIEW pressure_rs AS SELECT TIMESHIFT(RESAMPLE_CONSTANT(pressure, 500000), -500000) "
      "AS pressure FROM pressure_bin");
  EXPECT_EQ(r.statements[3],
      "CREATE MATERIALIZED VIEW data AS SELECT pressure FROM pressure_rs");
}

TEST_F(PreprocessorTest, DurationSeconds) {
  auto r = preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(2s)", 1000000);
  // _bin view at index 1 should have the bin interval
  EXPECT_NE(r.statements[1].find("2000000"), std::string::npos);
  // _rs view at index 2 should also have the interval
  EXPECT_NE(r.statements[2].find("2000000"), std::string::npos);
}

TEST_F(PreprocessorTest, DurationMilliseconds) {
  auto r = preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(250ms)", 1000);
  EXPECT_NE(r.statements[1].find("250"), std::string::npos);
  EXPECT_NE(r.statements[2].find("250"), std::string::npos);
}

TEST_F(PreprocessorTest, DurationMinutes) {
  auto r = preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(1m)", 1000000);
  EXPECT_NE(r.statements[1].find("60000000"), std::string::npos);
  EXPECT_NE(r.statements[2].find("60000000"), std::string::npos);
}

TEST_F(PreprocessorTest, DurationHours) {
  auto r = preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(1h)", 1000000);
  EXPECT_NE(r.statements[1].find("3600000000"), std::string::npos);
  EXPECT_NE(r.statements[2].find("3600000000"), std::string::npos);
}

TEST_F(PreprocessorTest, DurationFiveMinutes) {
  auto r = preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(5m)", 1000000);
  EXPECT_NE(r.statements[1].find("300000000"), std::string::npos);
  EXPECT_NE(r.statements[2].find("300000000"), std::string::npos);
}

TEST_F(PreprocessorTest, InvalidBinNoUnits) {
  EXPECT_THROW(
      preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(1000)", 1000000),
      std::runtime_error);
}

TEST_F(PreprocessorTest, SetTimescaleMs) {
  auto r = preprocess_sql("SET TIMESCALE ms", 1000000);
  EXPECT_TRUE(r.statements.empty());
  EXPECT_EQ(r.new_ts_units_per_second, 1000);
}

TEST_F(PreprocessorTest, SetTimescaleUs) {
  auto r = preprocess_sql("SET TIMESCALE us", 1000);
  EXPECT_TRUE(r.statements.empty());
  EXPECT_EQ(r.new_ts_units_per_second, 1000000);
}

TEST_F(PreprocessorTest, SetTimescaleS) {
  auto r = preprocess_sql("SET TIMESCALE s", 1000000);
  EXPECT_TRUE(r.statements.empty());
  EXPECT_EQ(r.new_ts_units_per_second, 1);
}

TEST_F(PreprocessorTest, CaseInsensitive) {
  auto r = preprocess_sql(
      "create aligned stream S (a double) bin(1s)", 1000000);
  // 1 stream + 1 _bin + 1 _rs + 1 combining = 4
  ASSERT_EQ(r.statements.size(), 4);
}

TEST_F(PreprocessorTest, ThreeColumns) {
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM multi (x DOUBLE, y DOUBLE, z DOUBLE) BIN(100ms)",
      1000000);
  // 3 streams + 3 _bin + 3 _rs + 1 combining = 10
  ASSERT_EQ(r.statements.size(), 10);
  EXPECT_EQ(r.statements[0], "CREATE STREAM x (value DOUBLE)");
  EXPECT_EQ(r.statements[1], "CREATE STREAM y (value DOUBLE)");
  EXPECT_EQ(r.statements[2], "CREATE STREAM z (value DOUBLE)");
  // _bin views
  EXPECT_NE(r.statements[3].find("x_bin"), std::string::npos);
  EXPECT_NE(r.statements[4].find("y_bin"), std::string::npos);
  EXPECT_NE(r.statements[5].find("z_bin"), std::string::npos);
  // _rs views
  EXPECT_NE(r.statements[6].find("x_rs"), std::string::npos);
  EXPECT_NE(r.statements[6].find("TIMESHIFT(RESAMPLE_CONSTANT"), std::string::npos);
  EXPECT_NE(r.statements[7].find("y_rs"), std::string::npos);
  EXPECT_NE(r.statements[8].find("z_rs"), std::string::npos);
  // Combining materialized view references _rs views
  EXPECT_EQ(r.statements[9],
      "CREATE MATERIALIZED VIEW multi AS SELECT x, y, z FROM x_rs, y_rs, z_rs");
}

TEST_F(PreprocessorTest, FiveColumns) {
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM many (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE) BIN(1s)",
      1000000);
  // 5 streams + 5 _bin + 5 _rs + 1 combining = 16
  ASSERT_EQ(r.statements.size(), 16);
  EXPECT_EQ(r.statements[0], "CREATE STREAM a (value DOUBLE)");
  EXPECT_EQ(r.statements[4], "CREATE STREAM e (value DOUBLE)");
  EXPECT_EQ(r.statements[15],
            "CREATE MATERIALIZED VIEW many AS SELECT a, b, c, d, e "
            "FROM a_rs, b_rs, c_rs, d_rs, e_rs");
}

TEST_F(PreprocessorTest, KeywordColumnNameThrows) {
  EXPECT_THROW(
      preprocess_sql("CREATE ALIGNED STREAM s (select DOUBLE) BIN(1s)",
                     1000000),
      std::runtime_error);
}

TEST_F(PreprocessorTest, EmptyColumnsThrows) {
  EXPECT_THROW(
      preprocess_sql("CREATE ALIGNED STREAM s () BIN(1s)", 1000000),
      std::runtime_error);
}

TEST_F(PreprocessorTest, BinIntervalMatchesBetweenBinAndRs) {
  // Verify the same interval appears in both _bin GROUP BY and _rs RESAMPLE_CONSTANT
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM s (val DOUBLE) BIN(2s)", 1000000);
  ASSERT_EQ(r.statements.size(), 4);
  // _bin: GROUP BY FLOOR(TS() / 2000000)
  EXPECT_NE(r.statements[1].find("FLOOR(TS() / 2000000)"), std::string::npos);
  // _rs: TIMESHIFT(RESAMPLE_CONSTANT(val, 2000000), -2000000)
  EXPECT_NE(r.statements[2].find("TIMESHIFT(RESAMPLE_CONSTANT(val, 2000000), -2000000)"), std::string::npos);
}

TEST_F(PreprocessorTest, TrailingSemicolonAndWhitespace) {
  auto r = preprocess_sql(
      "  CREATE ALIGNED STREAM s (a DOUBLE) BIN(1s) ;  \n", 1000000);
  ASSERT_EQ(r.statements.size(), 4);
}

TEST_F(PreprocessorTest, DurationTooSmallForTimescaleThrows) {
  // ts_units_per_second=1 (seconds), BIN(1ms) → (1*1)/1000 = 0 → error
  EXPECT_THROW(
      preprocess_sql("CREATE ALIGNED STREAM s (a DOUBLE) BIN(1ms)", 1),
      std::runtime_error);
}

TEST_F(PreprocessorTest, DurationMillisecondsWithSmallTimescale) {
  // ts_units_per_second=1000 (ms), BIN(1ms) → (1000*1)/1000 = 1 → OK
  auto r = preprocess_sql(
      "CREATE ALIGNED STREAM s (a DOUBLE) BIN(1ms)", 1000);
  ASSERT_EQ(r.statements.size(), 4);
  EXPECT_NE(r.statements[1].find("FLOOR(TS() / 1)"), std::string::npos);
}
