#pragma once

#include <map>
#include <string>
#include <vector>

#include "rtbot_sql/api/types.h"

namespace rtbot_sql::api {

// Compile a SQL string into a CompilationResult.
// The catalog snapshot provides schema information for name resolution.
CompilationResult compile_sql(const std::string& sql,
                              const CatalogSnapshot& catalog);

// Result of applying Tier 2 filter/projection to raw rows.
struct Tier2FilterResult {
  std::vector<std::vector<double>> rows;
  std::map<std::string, int> field_map;
};

// Apply Tier 2 WHERE filtering and SELECT projection to decoded rows.
// Re-parses the SQL to build the evaluation plan, then applies it.
// Returns filtered/projected rows and the output field_map.
// For non-Tier-2 queries, returns input rows unchanged.
Tier2FilterResult apply_tier2_filter(
    const std::string& sql, const CatalogSnapshot& catalog,
    const std::vector<std::vector<double>>& input_rows, int limit = -1);

}  // namespace rtbot_sql::api
