#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "rtbot_sql/api/types.h"

namespace rtbot_sql::catalog {

class Catalog {
 public:
  void register_stream(const std::string& name, const StreamSchema& schema);
  void register_view(const std::string& name, const ViewMeta& meta);
  void register_table(const std::string& name, const TableSchema& schema);

  std::optional<StreamSchema> lookup_stream(const std::string& name) const;
  std::optional<ViewMeta> lookup_view(const std::string& name) const;
  std::optional<TableSchema> lookup_table(const std::string& name) const;

  std::optional<EntityType> resolve_entity(const std::string& name) const;

  CatalogSnapshot snapshot() const;

  void add_key(const std::string& view_name, double key);
  std::vector<double> get_known_keys(const std::string& view_name) const;

  void drop_stream(const std::string& name);
  void drop_view(const std::string& name);
  void drop_table(const std::string& name);

  std::vector<std::string> list_streams() const;
  std::vector<std::string> list_views() const;
  std::vector<std::string> list_tables() const;

 private:
  std::map<std::string, StreamSchema> streams_;
  std::map<std::string, ViewMeta> views_;
  std::map<std::string, TableSchema> tables_;
};

}  // namespace rtbot_sql::catalog
