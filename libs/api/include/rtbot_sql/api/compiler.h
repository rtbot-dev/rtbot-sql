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

// Result of preprocessing + compiling a SQL statement.
// Handles sugar syntax (CREATE ALIGNED STREAM ... BIN()) by expanding
// into multiple statements, compiling each in sequence, and updating
// the catalog between steps so later statements see earlier definitions.
struct ExpandedCompilationResult {
  std::vector<CompilationResult> results;
  int64_t new_ts_units_per_second = -1;  // -1 = no change
};

// Preprocess and compile a SQL statement in one step.
// Sugar syntax is expanded and each resulting statement is compiled
// against an incrementally updated catalog.  SET TIMESCALE returns
// an empty results vector with new_ts_units_per_second set.
ExpandedCompilationResult compile_sql_expanded(
    const std::string& sql, const CatalogSnapshot& catalog,
    int64_t ts_units_per_second);

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
