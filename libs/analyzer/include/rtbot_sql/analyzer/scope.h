#pragma once

#include <map>
#include <string>
#include <variant>
#include <vector>

#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {

struct ColumnBinding {
  std::string stream_name;
  int index;
};

class Scope {
 public:
  Scope();

  void push();
  void pop();

  // Register columns from a stream (with optional alias)
  void register_stream(const std::string& stream_name,
                       const StreamSchema& schema,
                       const std::string& alias = "");

  // Resolve a column reference → ColumnBinding or error string
  std::variant<ColumnBinding, std::string> resolve(
      const parser::ast::ColumnRef& ref) const;

 private:
  struct Entry {
    ColumnBinding binding;
    bool ambiguous = false;
  };

  std::vector<std::map<std::string, Entry>> levels_;
};

}  // namespace rtbot_sql::analyzer
