#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

namespace rtbot_sql::api {
namespace {

// ---------------------------------------------------------------------------
// SELECT STREAM — column types
// ---------------------------------------------------------------------------

TEST(SchemaTest, CreateStreamWithTextColumnPreservesType) {
  CatalogSnapshot empty;
  auto result = compile_sql(
      "SELECT STREAM sensors "
      "(device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)",
      empty);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.stream_schema.columns.size(), 3u);
  EXPECT_EQ(result.stream_schema.columns[0].name, "device_id");
  EXPECT_EQ(result.stream_schema.columns[0].type, ColumnType::DOUBLE);
  EXPECT_EQ(result.stream_schema.columns[1].name, "location");
  EXPECT_EQ(result.stream_schema.columns[1].type, ColumnType::TEXT);
  EXPECT_EQ(result.stream_schema.columns[2].name, "value");
  EXPECT_EQ(result.stream_schema.columns[2].type, ColumnType::DOUBLE);
}

// ---------------------------------------------------------------------------
// CREATE TABLE — column types
// ---------------------------------------------------------------------------

TEST(SchemaTest, CreateTableWithTextColumnPreservesType) {
  CatalogSnapshot empty;
  auto result = compile_sql(
      "CREATE TABLE assets "
      "(asset_id DOUBLE PRECISION PRIMARY KEY, name TEXT, rating DOUBLE PRECISION)",
      empty);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.table_schema.columns.size(), 3u);
  EXPECT_EQ(result.table_schema.columns[0].name, "asset_id");
  EXPECT_EQ(result.table_schema.columns[0].type, ColumnType::DOUBLE);
  EXPECT_EQ(result.table_schema.columns[1].name, "name");
  EXPECT_EQ(result.table_schema.columns[1].type, ColumnType::TEXT);
  EXPECT_EQ(result.table_schema.columns[2].name, "rating");
  EXPECT_EQ(result.table_schema.columns[2].type, ColumnType::DOUBLE);
}

// ---------------------------------------------------------------------------
// Default to DOUBLE when no TEXT columns present
// ---------------------------------------------------------------------------

TEST(SchemaTest, CreateStreamWithoutTextDefaultsToDouble) {
  CatalogSnapshot empty;
  auto result = compile_sql(
      "SELECT STREAM ticks (value DOUBLE PRECISION)", empty);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.stream_schema.columns.size(), 1u);
  EXPECT_EQ(result.stream_schema.columns[0].name, "value");
  EXPECT_EQ(result.stream_schema.columns[0].type, ColumnType::DOUBLE);
}

}  // namespace
}  // namespace rtbot_sql::api
