#include "rtbot_sql/catalog/string_dictionary.h"
#include <gtest/gtest.h>

namespace rtbot_sql::catalog {
namespace {

class StringDictionaryTest : public ::testing::Test {
 protected:
  StringDictionary dict;
};

TEST_F(StringDictionaryTest, EncodeAssignsSequentialIds) {
  double id1 = dict.encode("Bay A");
  double id2 = dict.encode("Bay B");
  double id3 = dict.encode("Bay C");
  EXPECT_DOUBLE_EQ(id1, 1.0);
  EXPECT_DOUBLE_EQ(id2, 2.0);
  EXPECT_DOUBLE_EQ(id3, 3.0);
}

TEST_F(StringDictionaryTest, EncodeReturnsSameIdForSameString) {
  double id1 = dict.encode("Bay A");
  double id2 = dict.encode("Bay A");
  EXPECT_DOUBLE_EQ(id1, id2);
}

TEST_F(StringDictionaryTest, DecodeReturnsOriginalString) {
  double id = dict.encode("Bay A");
  auto result = dict.decode(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "Bay A");
}

TEST_F(StringDictionaryTest, DecodeUnknownIdReturnsNullopt) {
  EXPECT_FALSE(dict.decode(999.0).has_value());
}

TEST_F(StringDictionaryTest, LookupReturnsIdWithoutCreating) {
  EXPECT_FALSE(dict.lookup("Bay A").has_value());
  dict.encode("Bay A");
  auto id = dict.lookup("Bay A");
  ASSERT_TRUE(id.has_value());
  EXPECT_DOUBLE_EQ(*id, 1.0);
}

TEST_F(StringDictionaryTest, SizeReflectsUniqueEntries) {
  EXPECT_EQ(dict.size(), 0u);
  dict.encode("a");
  dict.encode("b");
  dict.encode("a");  // duplicate
  EXPECT_EQ(dict.size(), 2u);
}

TEST_F(StringDictionaryTest, EmptyDictionaryHasSizeZero) {
  EXPECT_EQ(dict.size(), 0u);
  EXPECT_TRUE(dict.empty());
}

TEST_F(StringDictionaryTest, AllEntriesReturnsFullMapping) {
  dict.encode("x");
  dict.encode("y");
  auto entries = dict.all_entries();
  ASSERT_EQ(entries.size(), 2u);
  EXPECT_EQ(entries[1.0], "x");
  EXPECT_EQ(entries[2.0], "y");
}

TEST_F(StringDictionaryTest, EncodeEmptyStringWorks) {
  double id = dict.encode("");
  EXPECT_DOUBLE_EQ(id, 1.0);
  auto result = dict.decode(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

TEST_F(StringDictionaryTest, EncodeManyStringsDoesNotCollide) {
  for (int i = 0; i < 10000; ++i) {
    double id = dict.encode("str_" + std::to_string(i));
    EXPECT_DOUBLE_EQ(id, static_cast<double>(i + 1));
  }
  EXPECT_EQ(dict.size(), 10000u);
  // Verify all decode correctly
  for (int i = 0; i < 10000; ++i) {
    auto result = dict.decode(static_cast<double>(i + 1));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "str_" + std::to_string(i));
  }
}

}  // namespace
}  // namespace rtbot_sql::catalog
