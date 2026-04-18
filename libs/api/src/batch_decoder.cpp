#include "rtbot_sql/api/batch_decoder.h"

namespace rtbot_sql::api {

void SessionOutputRegistry::register_outputs(
    const rtbot::Program* program, std::vector<std::string> op_ids) {
  std::unordered_map<std::string, int> id_to_idx;
  id_to_idx.reserve(op_ids.size());
  for (size_t i = 0; i < op_ids.size(); ++i) {
    id_to_idx[op_ids[i]] = static_cast<int>(i);
  }
  std::lock_guard<std::mutex> lock(mu_);
  map_[program] = Entry{std::move(op_ids), std::move(id_to_idx)};
}

void SessionOutputRegistry::clear(const rtbot::Program* program) {
  std::lock_guard<std::mutex> lock(mu_);
  map_.erase(program);
}

const SessionOutputRegistry::Entry* SessionOutputRegistry::lookup(
    const rtbot::Program* program) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(program);
  return it == map_.end() ? nullptr : &it->second;
}

SessionOutputRegistry& SessionOutputRegistry::instance() {
  static SessionOutputRegistry s;
  return s;
}

}  // namespace rtbot_sql::api
