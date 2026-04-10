#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

namespace rtbot_sql::api {
namespace {

// ---------------------------------------------------------------------------
// INSERT with string constants on TEXT columns
// ---------------------------------------------------------------------------

TEST(InsertTest, InsertWithStringConstantResolvesDictionaryId) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {
      {"device_id", 0, ColumnType::DOUBLE},
      {"location", 1, ColumnType::TEXT},
      {"value", 2, ColumnType::DOUBLE},
  };
  catalog.streams["sensors"] = schema;

  auto result = compile_sql(
      "INSERT INTO sensors VALUES (1, 'Bay A', 42.0)", catalog);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.insert_payload.size(), 3u);
  EXPECT_DOUBLE_EQ(result.insert_payload[0], 1.0);
  // 'Bay A' should be encoded as dictionary ID 1.0
  EXPECT_DOUBLE_EQ(result.insert_payload[1], 1.0);
  EXPECT_DOUBLE_EQ(result.insert_payload[2], 42.0);
}

// ---------------------------------------------------------------------------
// Type mismatch: string → DOUBLE column
// ---------------------------------------------------------------------------

TEST(InsertTest, InsertStringIntoDoubleColumnFails) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "ticks";
  schema.columns = {{"value", 0, ColumnType::DOUBLE}};
  catalog.streams["ticks"] = schema;

  auto result = compile_sql("INSERT INTO ticks VALUES ('hello')", catalog);
  EXPECT_TRUE(result.has_errors());
}

// ---------------------------------------------------------------------------
// Type mismatch: numeric → TEXT column
// ---------------------------------------------------------------------------

TEST(InsertTest, InsertNumericIntoTextColumnFails) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {{"location", 0, ColumnType::TEXT}};
  catalog.streams["sensors"] = schema;

  auto result = compile_sql("INSERT INTO sensors VALUES (42)", catalog);
  EXPECT_TRUE(result.has_errors());
}

// ---------------------------------------------------------------------------
// Repeated string INSERT reuses existing dictionary ID
// ---------------------------------------------------------------------------

TEST(InsertTest, RepeatedStringInsertReusesId) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {{"location", 0, ColumnType::TEXT}};
  catalog.streams["sensors"] = schema;
  // Pre-populate dictionary
  catalog.dictionaries["sensors.location"] = {{1.0, "Bay A"}};

  auto result =
      compile_sql("INSERT INTO sensors VALUES ('Bay A')", catalog);
  ASSERT_FALSE(result.has_errors());
  EXPECT_DOUBLE_EQ(result.insert_payload[0], 1.0);  // reuses existing ID
}

// ---------------------------------------------------------------------------
// dictionary_updates captures new string encodings
// ---------------------------------------------------------------------------

TEST(InsertTest, DictionaryUpdatesContainNewEncodings) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {
      {"device_id", 0, ColumnType::DOUBLE},
      {"location", 1, ColumnType::TEXT},
  };
  catalog.streams["sensors"] = schema;

  auto result = compile_sql(
      "INSERT INTO sensors VALUES (1, 'Bay A')", catalog);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.dictionary_updates.count("sensors.location"), 1u);
  const auto& dict = result.dictionary_updates.at("sensors.location");
  ASSERT_EQ(dict.count(1.0), 1u);
  EXPECT_EQ(dict.at(1.0), "Bay A");
}

// ---------------------------------------------------------------------------
// INSERT into unknown stream/table fails
// ---------------------------------------------------------------------------

TEST(InsertTest, InsertIntoUnknownStreamFails) {
  CatalogSnapshot catalog;
  auto result =
      compile_sql("INSERT INTO nonexistent VALUES (1)", catalog);
  EXPECT_TRUE(result.has_errors());
}

// ---------------------------------------------------------------------------
// All-numeric INSERT into numeric-only stream still works
// ---------------------------------------------------------------------------

TEST(InsertTest, AllNumericInsertStillWorks) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "ticks";
  schema.columns = {
      {"price", 0, ColumnType::DOUBLE},
      {"volume", 1, ColumnType::DOUBLE},
  };
  catalog.streams["ticks"] = schema;

  auto result =
      compile_sql("INSERT INTO ticks VALUES (100.5, 200)", catalog);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.insert_payload.size(), 2u);
  EXPECT_DOUBLE_EQ(result.insert_payload[0], 100.5);
  EXPECT_DOUBLE_EQ(result.insert_payload[1], 200.0);
  // No dictionary updates for all-numeric stream
  EXPECT_TRUE(result.dictionary_updates.empty());
}

// ---------------------------------------------------------------------------
// INSERT into table (not just stream) works
// ---------------------------------------------------------------------------

TEST(InsertTest, InsertIntoTableWithTextColumn) {
  CatalogSnapshot catalog;
  TableSchema tschema;
  tschema.name = "assets";
  tschema.columns = {
      {"asset_id", 0, ColumnType::DOUBLE},
      {"name", 1, ColumnType::TEXT},
  };
  tschema.key_columns = {0};
  tschema.changelog_stream = "rtbot:sql:table:assets:changelog";
  catalog.tables["assets"] = tschema;

  auto result = compile_sql(
      "INSERT INTO assets VALUES (1, 'Widget')", catalog);
  ASSERT_FALSE(result.has_errors());
  ASSERT_EQ(result.insert_payload.size(), 2u);
  EXPECT_DOUBLE_EQ(result.insert_payload[0], 1.0);
  EXPECT_DOUBLE_EQ(result.insert_payload[1], 1.0);  // dictionary ID for 'Widget'
  ASSERT_EQ(result.dictionary_updates.count("assets.name"), 1u);
}

// ---------------------------------------------------------------------------
// Value count mismatch produces error
// ---------------------------------------------------------------------------

TEST(InsertTest, ValueCountMismatchFails) {
  CatalogSnapshot catalog;
  StreamSchema schema;
  schema.name = "sensors";
  schema.columns = {
      {"device_id", 0, ColumnType::DOUBLE},
      {"location", 1, ColumnType::TEXT},
  };
  catalog.streams["sensors"] = schema;

  auto result =
      compile_sql("INSERT INTO sensors VALUES (1, 'Bay A', 99)", catalog);
  EXPECT_TRUE(result.has_errors());
}

}  // namespace
}  // namespace rtbot_sql::api
