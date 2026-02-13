#include "rtbot_sql/compiler/graph_builder.h"

#include <iomanip>
#include <set>

namespace rtbot_sql::compiler {

std::string GraphBuilder::next_id(const std::string& prefix) {
  return prefix + "_" + std::to_string(id_counter_++);
}

void GraphBuilder::add_operator(const std::string& id,
                                const std::string& type,
                                const std::map<std::string, double>& params) {
  operators_.push_back({id, type, params});
}

void GraphBuilder::connect(const Endpoint& from, const Endpoint& to) {
  connections_.push_back({from.operator_id, from.port, to.operator_id, to.port});
}

const OperatorDef* GraphBuilder::find_operator(const std::string& id) const {
  for (const auto& op : operators_) {
    if (op.id == id) return &op;
  }
  return nullptr;
}

std::string GraphBuilder::to_json() const {
  static const std::set<std::string> int_params = {"index", "numPorts"};

  std::ostringstream os;
  os << "{\n  \"operators\": [\n";

  for (size_t i = 0; i < operators_.size(); ++i) {
    const auto& op = operators_[i];
    os << "    {\"id\": \"" << op.id << "\", \"type\": \"" << op.type << "\"";
    for (const auto& [key, val] : op.params) {
      os << ", \"" << key << "\": ";
      if (int_params.count(key)) {
        os << static_cast<int>(val);
      } else {
        // Use enough precision for round-trip fidelity
        os << std::setprecision(15) << val;
      }
    }
    os << "}";
    if (i + 1 < operators_.size()) os << ",";
    os << "\n";
  }

  os << "  ],\n  \"connections\": [\n";

  for (size_t i = 0; i < connections_.size(); ++i) {
    const auto& c = connections_[i];
    os << "    {\"from\": \"" << c.from_id << "\", \"fromPort\": \""
       << c.from_port << "\", \"to\": \"" << c.to_id << "\", \"toPort\": \""
       << c.to_port << "\"}";
    if (i + 1 < connections_.size()) os << ",";
    os << "\n";
  }

  os << "  ]\n}";
  return os.str();
}

}  // namespace rtbot_sql::compiler
