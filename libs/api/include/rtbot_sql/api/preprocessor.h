#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace rtbot_sql::api {

struct PreprocessResult {
  std::vector<std::string> statements;
  int64_t new_ts_units_per_second = -1;  // -1 = no change
};

PreprocessResult preprocess_sql(const std::string& sql,
                                int64_t ts_units_per_second);

}  // namespace rtbot_sql::api
