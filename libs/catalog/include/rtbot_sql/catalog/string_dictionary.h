#pragma once

#include <map>
#include <optional>
#include <string>

namespace rtbot_sql::catalog {

class StringDictionary {
 public:
  // Encode a string to its dictionary ID, creating a new entry if needed.
  double encode(const std::string& value);

  // Look up a string's ID without creating an entry. Returns nullopt if not found.
  std::optional<double> lookup(const std::string& value) const;

  // Decode a dictionary ID back to its string. Returns nullopt if not found.
  std::optional<std::string> decode(double id) const;

  // Return all entries as id -> string map (for serialization).
  std::map<double, std::string> all_entries() const;

  // Restore from serialized entries (for deserialization).
  void restore(const std::map<double, std::string>& entries);

  size_t size() const;
  bool empty() const;

 private:
  std::map<std::string, double> str_to_id_;
  std::map<double, std::string> id_to_str_;
  double next_id_ = 1.0;
};

}  // namespace rtbot_sql::catalog
