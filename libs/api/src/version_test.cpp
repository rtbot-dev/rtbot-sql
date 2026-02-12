#include "rtbot_sql/version.h"

#include <gtest/gtest.h>

TEST(Version, ReturnsExpectedString) {
  EXPECT_EQ(rtbot_sql::version(), "0.1.0");
}
