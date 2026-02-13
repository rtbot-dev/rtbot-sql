#include "rtbot_sql/api/types.h"

#include <gtest/gtest.h>

namespace rtbot_sql {
namespace {

TEST(StreamSchema, ColumnIndexFindsExistingColumn) {
  StreamSchema s{"test_stream", {{"a", 0}, {"b", 1}, {"c", 2}}};
  auto idx = s.column_index("b");
  ASSERT_TRUE(idx.has_value());
  EXPECT_EQ(*idx, 1);
}

TEST(StreamSchema, ColumnIndexReturnsNulloptForMissing) {
  StreamSchema s{"test_stream", {{"a", 0}, {"b", 1}}};
  auto idx = s.column_index("z");
  EXPECT_FALSE(idx.has_value());
}

TEST(StreamSchema, ColumnIndexWorksOnEmptyColumns) {
  StreamSchema s{"empty", {}};
  EXPECT_FALSE(s.column_index("any").has_value());
}

TEST(CatalogSnapshot, DefaultConstructionIsEmpty) {
  CatalogSnapshot snap;
  EXPECT_TRUE(snap.streams.empty());
  EXPECT_TRUE(snap.views.empty());
  EXPECT_TRUE(snap.tables.empty());
}

TEST(CatalogSnapshot, CanInsertAndRetrieve) {
  CatalogSnapshot snap;
  snap.streams["s1"] = StreamSchema{"s1", {{"x", 0}}};
  ASSERT_EQ(snap.streams.count("s1"), 1u);
  EXPECT_EQ(snap.streams["s1"].columns.size(), 1u);
}

TEST(CompilationResult, HasErrorsWhenErrorsPresent) {
  CompilationResult r{};
  EXPECT_FALSE(r.has_errors());
  r.errors.push_back({"some error", -1, -1});
  EXPECT_TRUE(r.has_errors());
}

}  // namespace
}  // namespace rtbot_sql
