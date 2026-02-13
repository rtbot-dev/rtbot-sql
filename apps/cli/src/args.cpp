#include "args.h"

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>

namespace rtbot_sql::cli {

cxxopts::Options CLIArguments::create_options() {
  cxxopts::Options options("rtbot-sql", "RTBot SQL Compiler");

  options.add_options()
      ("h,help", "Print help")
      ("sql", "SQL statement to compile", cxxopts::value<std::string>())
      ("f,file", "Read SQL from file", cxxopts::value<std::string>())
      ("c,catalog", "Catalog JSON file with stream/view definitions",
       cxxopts::value<std::string>())
      ("o,output", "Write output to file instead of stdout",
       cxxopts::value<std::string>())
      ("format", "Output format: json (default) or compact",
       cxxopts::value<std::string>()->default_value("json"))
      ("v,verbose", "Show extra compilation details",
       cxxopts::value<bool>()->default_value("false"))
      ("repl", "Start interactive REPL mode");

  options.parse_positional({"sql"});
  options.positional_help("[SQL]");

  return options;
}

CLIArguments CLIArguments::parse(int argc, char* argv[]) {
  auto options = create_options();

  try {
    auto result = options.parse(argc, argv);

    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      exit(0);
    }

    CLIArguments args;

    // Mode
    if (result.count("repl")) {
      args.mode = Mode::REPL;
    } else {
      args.mode = Mode::COMPILE;
    }

    // SQL input sources (positional, --sql, --file, or stdin)
    if (result.count("sql")) {
      args.sql = result["sql"].as<std::string>();
    }
    if (result.count("file")) {
      args.input_file = result["file"].as<std::string>();
      if (!std::ifstream(args.input_file).good()) {
        throw ArgumentException("SQL file does not exist: " + args.input_file);
      }
    }
    if (result.count("catalog")) {
      args.catalog_file = result["catalog"].as<std::string>();
      if (!std::ifstream(args.catalog_file).good()) {
        throw ArgumentException("Catalog file does not exist: " +
                                args.catalog_file);
      }
    }
    if (result.count("output")) {
      args.output_file = result["output"].as<std::string>();
    }

    // Format
    std::string fmt = result["format"].as<std::string>();
    if (fmt == "compact") {
      args.format = OutputFormat::COMPACT;
    } else {
      args.format = OutputFormat::JSON;
    }

    args.verbose = result["verbose"].as<bool>();

    // In COMPILE mode, we need SQL from somewhere
    if (args.mode == Mode::COMPILE && args.sql.empty() &&
        args.input_file.empty()) {
      // Try reading from stdin if it's not a terminal
      if (!isatty(fileno(stdin))) {
        std::ostringstream ss;
        ss << std::cin.rdbuf();
        args.sql = ss.str();
      }
      if (args.sql.empty()) {
        throw ArgumentException(
            "No SQL provided. Use positional arg, --file, or pipe via stdin.");
      }
    }

    // If file was specified, read it
    if (!args.input_file.empty() && args.sql.empty()) {
      std::ifstream ifs(args.input_file);
      if (!ifs.is_open()) {
        throw ArgumentException("Cannot open SQL file: " + args.input_file);
      }
      std::ostringstream ss;
      ss << ifs.rdbuf();
      args.sql = ss.str();
    }

    return args;
  } catch (const cxxopts::exceptions::parsing& e) {
    throw ArgumentException(std::string("Error parsing arguments: ") +
                            e.what());
  }
}

void CLIArguments::print_usage() {
  std::cout << create_options().help() << std::endl;
}

}  // namespace rtbot_sql::cli
