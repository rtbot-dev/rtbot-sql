#include "rtbot_sql/catalog/string_dictionary.h"

namespace rtbot_sql::catalog {

double StringDictionary::encode(const std::string& value) {
  auto it = str_to_id_.find(value);
  if (it != str_to_id_.end()) {
    return it->second;
  }
  double id = next_id_++;
  str_to_id_[value] = id;
  id_to_str_[id] = value;
  return id;
}

std::optional<double> StringDictionary::lookup(const std::string& value) const {
  auto it = str_to_id_.find(value);
  if (it != str_to_id_.end()) return it->second;
  return std::nullopt;
}

std::optional<std::string> StringDictionary::decode(double id) const {
  auto it = id_to_str_.find(id);
  if (it != id_to_str_.end()) return it->second;
  return std::nullopt;
}

std::map<double, std::string> StringDictionary::all_entries() const {
  return id_to_str_;
}

void StringDictionary::restore(const std::map<double, std::string>& entries) {
  str_to_id_.clear();
  id_to_str_.clear();
  next_id_ = 1.0;
  for (const auto& [id, str] : entries) {
    str_to_id_[str] = id;
    id_to_str_[id] = str;
    if (id >= next_id_) {
      next_id_ = id + 1.0;
    }
  }
}

size_t StringDictionary::size() const { return str_to_id_.size(); }

bool StringDictionary::empty() const { return str_to_id_.empty(); }

}  // namespace rtbot_sql::catalog
