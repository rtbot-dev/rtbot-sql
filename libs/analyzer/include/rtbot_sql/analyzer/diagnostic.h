#pragma once

#include <string>
#include <utility>
#include <vector>

#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

// A single semantic diagnostic surfaced by the analyzer.
struct Diagnostic {
  std::string message;
  parser::ast::SourceLocation loc;
};

// Accumulator for analyzer diagnostics. Use `error()` to record problems
// and `to_compilation_errors()` to convert into the API's CompilationError
// format for inclusion in CompilationResult.
class DiagnosticBag {
 public:
  void error(std::string msg, parser::ast::SourceLocation loc = {}) {
    diags_.push_back({std::move(msg), loc});
  }

  bool has_errors() const { return !diags_.empty(); }
  const std::vector<Diagnostic>& diagnostics() const { return diags_; }

  std::vector<CompilationError> to_compilation_errors() const {
    std::vector<CompilationError> out;
    out.reserve(diags_.size());
    for (const auto& d : diags_) {
      out.push_back({d.message, d.loc.line, d.loc.column, d.loc.end_line,
                      d.loc.end_column});
    }
    return out;
  }

 private:
  std::vector<Diagnostic> diags_;
};

}  // namespace rtbot_sql::analyzer
