#include "rtbot_sql/api/preprocessor.h"

#include <algorithm>
#include <regex>
#include <set>
#include <stdexcept>
#include <string>

namespace rtbot_sql::api {

namespace {

int64_t parse_duration(const std::string& lit, int64_t ts_units_per_second) {
  std::regex re(R"(^(\d+)(ms|s|m|h)$)");
  std::smatch match;
  if (!std::regex_match(lit, match, re)) {
    throw std::runtime_error(
        "BIN() requires a duration literal (e.g., 1s, 100ms, 5m, 1h), got: " +
        lit);
  }
  int64_t value = std::stoll(match[1].str());
  std::string unit = match[2].str();
  int64_t result;
  if (unit == "ms")
    result = (ts_units_per_second * value) / 1000;
  else if (unit == "s")
    result = ts_units_per_second * value;
  else if (unit == "m")
    result = ts_units_per_second * value * 60;
  else if (unit == "h")
    result = ts_units_per_second * value * 3600;
  else
    throw std::runtime_error("unknown duration unit: " + unit);
  if (result <= 0) {
    throw std::runtime_error(
        "BIN() duration " + lit +
        " is too small for the current timescale (ts_units_per_second=" +
        std::to_string(ts_units_per_second) +
        ") — would produce 0 time units");
  }
  return result;
}

std::string trim(const std::string& s) {
  auto start = s.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) return "";
  auto end = s.find_last_not_of(" \t\n\r;");
  return s.substr(start, end - start + 1);
}

bool is_reserved_sql_keyword(const std::string& ident) {
  static const std::set<std::string> kReserved = {
      "select", "from", "where", "group", "by", "having", "order",
      "limit", "join", "on", "as", "create", "stream", "table",
      "view", "materialized", "insert", "into", "drop", "set", "timescale"};
  std::string lowered = ident;
  std::transform(lowered.begin(), lowered.end(), lowered.begin(), ::tolower);
  return kReserved.count(lowered) > 0;
}

}  // namespace

PreprocessResult preprocess_sql(const std::string& sql,
                                int64_t ts_units_per_second) {
  std::string trimmed = trim(sql);

  // SET TIMESCALE
  {
    std::regex re(R"(^SET\s+TIMESCALE\s+(ms|us|s)$)", std::regex::icase);
    std::smatch match;
    if (std::regex_match(trimmed, match, re)) {
      std::string unit = match[1].str();
      std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
      int64_t new_scale;
      if (unit == "ms")
        new_scale = 1000;
      else if (unit == "us")
        new_scale = 1000000;
      else if (unit == "s")
        new_scale = 1;
      else
        throw std::runtime_error("unknown timescale unit: " + unit);
      return {{}, new_scale};
    }
  }

  // CREATE ALIGNED STREAM
  {
    std::regex re(
        R"(^CREATE\s+ALIGNED\s+STREAM\s+(\w+)\s*\(([^)]*)\)\s*BIN\s*\((\w+)\)$)",
        std::regex::icase);
    std::smatch match;
    if (std::regex_match(trimmed, match, re)) {
      std::string stream_name = match[1].str();
      std::string columns_str = match[2].str();
      std::string duration_lit = match[3].str();

      int64_t bin_units = parse_duration(duration_lit, ts_units_per_second);

      struct ColDef {
        std::string name;
        std::string type;
      };
      std::vector<ColDef> columns;
      {
        std::regex col_re(R"(\s*(\w+)\s+(\w+)\s*)");
        std::sregex_iterator it(columns_str.begin(), columns_str.end(),
                                col_re);
        std::sregex_iterator end;
        for (; it != end; ++it) {
          std::string col_type = (*it)[2].str();
          std::transform(col_type.begin(), col_type.end(), col_type.begin(),
                         ::toupper);
          columns.push_back({(*it)[1].str(), col_type});
        }
      }

      if (columns.empty()) {
        throw std::runtime_error(
            "CREATE ALIGNED STREAM requires at least one column");
      }

      if (is_reserved_sql_keyword(stream_name)) {
        throw std::runtime_error(
            "CREATE ALIGNED STREAM name cannot be an SQL keyword: " +
            stream_name);
      }

      for (const auto& col : columns) {
        if (is_reserved_sql_keyword(col.name)) {
          throw std::runtime_error(
              "CREATE ALIGNED STREAM column name cannot be an SQL keyword: " +
              col.name);
        }
      }

      std::string bin_str = std::to_string(bin_units);
      std::vector<std::string> stmts;

      // SELECT STREAM per column
      for (const auto& col : columns) {
        stmts.push_back("SELECT STREAM " + col.name + " (value " + col.type +
                         ")");
      }

      // CREATE VIEW per column with binning (_bin suffix)
      for (const auto& col : columns) {
        stmts.push_back("CREATE VIEW " + col.name +
                         "_bin AS SELECT AVG(value) AS " + col.name + " FROM " +
                         col.name + " GROUP BY FLOOR(TS() / " + bin_str + ")");
      }

      // CREATE VIEW per column with snap resampler (_rs suffix)
      for (const auto& col : columns) {
        stmts.push_back("CREATE VIEW " + col.name +
                         "_rs AS SELECT RESAMPLE_CONSTANT(" +
                         col.name + ", " + bin_str + ", 1" +
                         ") AS " + col.name + " FROM " + col.name + "_bin");
      }

      // CREATE VIEW joining all resampled views
      std::string select_cols, from_sources;
      for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
          select_cols += ", ";
          from_sources += ", ";
        }
        select_cols += columns[i].name;
        from_sources += columns[i].name + "_rs";
      }
      stmts.push_back("CREATE MATERIALIZED VIEW " + stream_name + " AS SELECT " +
                       select_cols + " FROM " + from_sources);

      return {stmts, -1};
    }
  }

  // Regular SQL passthrough
  return {{trimmed}, -1};
}

}  // namespace rtbot_sql::api
