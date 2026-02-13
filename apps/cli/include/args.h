#pragma once

#include <cxxopts.hpp>
#include <optional>
#include <string>

namespace rtbot_sql::cli {

enum class Mode { COMPILE, REPL };

enum class OutputFormat { JSON, COMPACT };

struct CLIArguments {
  Mode mode = Mode::COMPILE;
  std::string sql;              // SQL string (from --sql or positional)
  std::string input_file;       // SQL file path (from --file)
  std::string catalog_file;     // Catalog JSON path (from --catalog)
  std::string output_file;      // Output file path (from --output)
  OutputFormat format = OutputFormat::JSON;
  bool verbose = false;

  static CLIArguments parse(int argc, char* argv[]);
  static void print_usage();

 private:
  static cxxopts::Options create_options();
};

class ArgumentException : public std::runtime_error {
 public:
  explicit ArgumentException(const std::string& msg) : std::runtime_error(msg) {}
};

}  // namespace rtbot_sql::cli
