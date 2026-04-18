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

// Result of consolidating all views in a session catalog into a single
// rtbot Program (view subgraphs merged, Output operators dropped at every
// view boundary, one Input per base stream, one Collector sink per
// materialized view).
struct SessionCompilationResult {
  std::string program_json;
  // view_name → operator id producing that view's output inside the
  // consolidated graph. Present for every view in the session, even
  // non-materialized ones.
  std::map<std::string, std::string> view_terminals;
  // view_name → output port id ("o1", "o2"...) on the terminal operator.
  std::map<std::string, std::string> view_terminal_ports;
  // Subset of view_terminals keys that are exposed as Program outputs
  // (the materialized views). Each appears in the Program JSON's "output"
  // map so Program attaches a Collector sink.
  std::vector<std::string> materialized_views;
  // base_stream_name → Input operator id in the consolidated graph.
  std::map<std::string, std::string> base_stream_inputs;
  // base_stream_name → port id ("i1","i2"...) on its Input operator.
  std::map<std::string, std::string> base_stream_ports;
  std::vector<CompilationError> errors;

  bool has_errors() const { return !errors.empty(); }
};

// Compile all views in the catalog into one consolidated rtbot Program.
// Views are merged in topological order of their FROM dependencies; each
// view's body is inlined and its Input/Output operators are dropped.
// Exactly one Input operator is retained per base stream (SQL table) as
// the session's entry point. Materialized views are exposed as named
// outputs via the Program JSON's "output" map.
//
// Constraints (error if violated):
//   - No dependency cycles among views.
//   - Single-source views only (no cross-select / join views yet).
//   - Single base stream per session (one Input entry operator).
SessionCompilationResult compile_session_program(
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
