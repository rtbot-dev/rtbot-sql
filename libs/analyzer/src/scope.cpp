#include "rtbot_sql/analyzer/scope.h"

namespace rtbot_sql::analyzer {

Scope::Scope() { levels_.emplace_back(); }

void Scope::push() { levels_.emplace_back(); }

void Scope::pop() {
  if (levels_.size() > 1) {
    levels_.pop_back();
  }
}

void Scope::register_stream(const std::string& stream_name,
                            const StreamSchema& schema,
                            const std::string& alias) {
  auto& level = levels_.back();

  for (const auto& col : schema.columns) {
    Entry entry{ColumnBinding{stream_name, col.index}, false};

    // Qualified: stream_name.column
    level[stream_name + "." + col.name] = entry;

    // Qualified: alias.column (if alias provided)
    if (!alias.empty()) {
      level[alias + "." + col.name] = entry;
    }

    // Unqualified: column
    auto it = level.find(col.name);
    if (it != level.end()) {
      // Already exists from another stream → mark ambiguous
      if (it->second.binding.stream_name != stream_name) {
        it->second.ambiguous = true;
      }
    } else {
      level[col.name] = entry;
    }
  }
}

std::variant<ColumnBinding, std::string> Scope::resolve(
    const parser::ast::ColumnRef& ref) const {
  std::string key;
  if (!ref.table_alias.empty()) {
    key = ref.table_alias + "." + ref.column_name;
  } else {
    key = ref.column_name;
  }

  // Search from innermost scope outward
  for (auto it = levels_.rbegin(); it != levels_.rend(); ++it) {
    auto found = it->find(key);
    if (found != it->end()) {
      if (found->second.ambiguous) {
        return "ambiguous column reference: " + ref.column_name;
      }
      return found->second.binding;
    }
  }

  return "unknown column: " + key;
}

}  // namespace rtbot_sql::analyzer
