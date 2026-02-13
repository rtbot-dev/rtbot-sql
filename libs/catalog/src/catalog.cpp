#include "rtbot_sql/catalog/catalog.h"

namespace rtbot_sql::catalog {

void Catalog::register_stream(const std::string& name,
                              const StreamSchema& schema) {
  streams_[name] = schema;
}

void Catalog::register_view(const std::string& name, const ViewMeta& meta) {
  views_[name] = meta;
}

void Catalog::register_table(const std::string& name,
                             const TableSchema& schema) {
  tables_[name] = schema;
}

std::optional<StreamSchema> Catalog::lookup_stream(
    const std::string& name) const {
  auto it = streams_.find(name);
  if (it != streams_.end()) return it->second;
  return std::nullopt;
}

std::optional<ViewMeta> Catalog::lookup_view(const std::string& name) const {
  auto it = views_.find(name);
  if (it != views_.end()) return it->second;
  return std::nullopt;
}

std::optional<TableSchema> Catalog::lookup_table(
    const std::string& name) const {
  auto it = tables_.find(name);
  if (it != tables_.end()) return it->second;
  return std::nullopt;
}

std::optional<EntityType> Catalog::resolve_entity(
    const std::string& name) const {
  if (streams_.count(name)) return EntityType::STREAM;
  if (views_.count(name)) {
    return views_.at(name).entity_type;
  }
  if (tables_.count(name)) return EntityType::TABLE;
  return std::nullopt;
}

CatalogSnapshot Catalog::snapshot() const {
  return {streams_, views_, tables_};
}

void Catalog::add_key(const std::string& view_name, double key) {
  auto it = views_.find(view_name);
  if (it != views_.end()) {
    it->second.known_keys.push_back(key);
  }
}

std::vector<double> Catalog::get_known_keys(
    const std::string& view_name) const {
  auto it = views_.find(view_name);
  if (it != views_.end()) return it->second.known_keys;
  return {};
}

void Catalog::drop_stream(const std::string& name) { streams_.erase(name); }

void Catalog::drop_view(const std::string& name) { views_.erase(name); }

void Catalog::drop_table(const std::string& name) { tables_.erase(name); }

std::vector<std::string> Catalog::list_streams() const {
  std::vector<std::string> result;
  result.reserve(streams_.size());
  for (const auto& [name, _] : streams_) {
    result.push_back(name);
  }
  return result;
}

std::vector<std::string> Catalog::list_views() const {
  std::vector<std::string> result;
  result.reserve(views_.size());
  for (const auto& [name, _] : views_) {
    result.push_back(name);
  }
  return result;
}

std::vector<std::string> Catalog::list_tables() const {
  std::vector<std::string> result;
  result.reserve(tables_.size());
  for (const auto& [name, _] : tables_) {
    result.push_back(name);
  }
  return result;
}

}  // namespace rtbot_sql::catalog
