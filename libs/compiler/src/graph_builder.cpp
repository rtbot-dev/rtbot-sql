#include "rtbot_sql/compiler/graph_builder.h"

#include <iomanip>
#include <set>

namespace rtbot_sql::compiler {

std::string GraphBuilder::next_id(const std::string& prefix) {
  return prefix + "_" + std::to_string(id_counter_++);
}

void GraphBuilder::add_operator(
    const std::string& id, const std::string& type,
    const std::map<std::string, double>& params,
    const std::map<std::string, std::string>& string_params) {
  operators_.push_back({id, type, params, string_params});
}

void GraphBuilder::connect(const Endpoint& from, const Endpoint& to) {
  connections_.push_back(
      {from.operator_id, from.port, to.operator_id, to.port});
}

void GraphBuilder::add_prototype(const PrototypeDef& proto) {
  prototypes_.push_back(proto);
}

const OperatorDef* GraphBuilder::find_operator(const std::string& id) const {
  for (const auto& op : operators_) {
    if (op.id == id) return &op;
  }
  return nullptr;
}

const PrototypeDef* GraphBuilder::find_prototype(
    const std::string& id) const {
  for (const auto& p : prototypes_) {
    if (p.id == id) return &p;
  }
  return nullptr;
}

namespace {

void emit_operator(std::ostringstream& os, const OperatorDef& op,
                   const std::set<std::string>& int_params) {
  os << "{\"id\": \"" << op.id << "\", \"type\": \"" << op.type << "\"";
  for (const auto& [key, val] : op.params) {
    os << ", \"" << key << "\": ";
    if (int_params.count(key) || key.substr(0, 6) == "index_") {
      os << static_cast<int>(val);
    } else {
      os << std::setprecision(15) << val;
    }
  }
  for (const auto& [key, val] : op.string_params) {
    os << ", \"" << key << "\": \"" << val << "\"";
  }
  os << "}";
}

void emit_connection(std::ostringstream& os, const Connection& c) {
  os << "{\"from\": \"" << c.from_id << "\", \"fromPort\": \"" << c.from_port
     << "\", \"to\": \"" << c.to_id << "\", \"toPort\": \"" << c.to_port
     << "\"}";
}

}  // namespace

std::string GraphBuilder::to_json() const {
  static const std::set<std::string> int_params = {
      "index",    "numPorts",  "window",   "numIndices",
      "interval", "numCoeffs", "numA",     "numB",
      "key_index"};

  std::ostringstream os;
  os << "{\n  \"operators\": [\n";

  size_t total = prototypes_.size() + operators_.size();
  size_t idx = 0;

  // Emit prototypes as special operators with nested structure
  for (const auto& proto : prototypes_) {
    os << "    {\"id\": \"" << proto.id
       << "\", \"type\": \"Prototype\""
       << ", \"entry\": \"" << proto.entry_id << "\""
       << ", \"output\": \"" << proto.output_id << "\""
       << ", \"operators\": [\n";
    for (size_t j = 0; j < proto.operators.size(); ++j) {
      os << "      ";
      emit_operator(os, proto.operators[j], int_params);
      if (j + 1 < proto.operators.size()) os << ",";
      os << "\n";
    }
    os << "    ], \"connections\": [\n";
    for (size_t j = 0; j < proto.connections.size(); ++j) {
      os << "      ";
      emit_connection(os, proto.connections[j]);
      if (j + 1 < proto.connections.size()) os << ",";
      os << "\n";
    }
    os << "    ]}";
    ++idx;
    if (idx < total) os << ",";
    os << "\n";
  }

  // Emit regular operators
  for (size_t i = 0; i < operators_.size(); ++i) {
    os << "    ";
    emit_operator(os, operators_[i], int_params);
    ++idx;
    if (idx < total) os << ",";
    os << "\n";
  }

  os << "  ],\n  \"connections\": [\n";

  for (size_t i = 0; i < connections_.size(); ++i) {
    os << "    ";
    emit_connection(os, connections_[i]);
    if (i + 1 < connections_.size()) os << ",";
    os << "\n";
  }

  os << "  ]\n}";
  return os.str();
}

}  // namespace rtbot_sql::compiler
