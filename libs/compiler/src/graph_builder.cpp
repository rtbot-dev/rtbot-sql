#include "rtbot_sql/compiler/graph_builder.h"

#include <nlohmann/json.hpp>
#include <set>

using json = nlohmann::json;

namespace rtbot_sql::compiler {

std::string GraphBuilder::next_id(const std::string& prefix) {
  return prefix + "_" + std::to_string(id_counter_++);
}

void GraphBuilder::add_operator(
    const std::string& id, const std::string& type,
    const std::map<std::string, double>& params,
    const std::map<std::string, std::string>& string_params,
    const std::map<std::string, std::vector<double>>& double_array_params,
    const std::map<std::string, std::vector<int>>& int_array_params) {
  operators_.push_back(
      {id, type, params, string_params, double_array_params, int_array_params});
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

static const std::set<std::string> int_params = {
    "index", "numPorts", "window", "window_size", "interval", "key_index"};

json operator_to_json(const OperatorDef& op) {
  json j;
  j["id"] = op.id;
  j["type"] = op.type;

  if (op.type == "Input" || op.type == "Output") {
    j["portTypes"] = json::array({"vector_number"});
  }

  for (const auto& [key, val] : op.params) {
    if (int_params.count(key)) {
      j[key] = static_cast<int>(val);
    } else {
      j[key] = val;
    }
  }
  for (const auto& [key, val] : op.string_params) {
    j[key] = val;
  }
  for (const auto& [key, vals] : op.double_array_params) {
    j[key] = vals;
  }
  for (const auto& [key, vals] : op.int_array_params) {
    j[key] = vals;
  }

  return j;
}

json connection_to_json(const Connection& c) {
  return {{"from", c.from_id},
          {"fromPort", c.from_port},
          {"to", c.to_id},
          {"toPort", c.to_port}};
}

json prototype_to_json(const PrototypeDef& proto) {
  json j;
  j["entry"] = {{"operator", proto.entry_id}};
  j["output"] = {{"operator", proto.output_id}};

  j["operators"] = json::array();
  for (const auto& op : proto.operators) {
    j["operators"].push_back(operator_to_json(op));
  }

  j["connections"] = json::array();
  for (const auto& c : proto.connections) {
    j["connections"].push_back(connection_to_json(c));
  }

  return j;
}

}  // namespace

// --- Port type system for validation ---

// Data types that flow between operators.
enum class DataType { NUMBER, BOOLEAN, VECTOR_NUMBER, VECTOR_BOOLEAN, UNKNOWN };

static std::string dtype_name(DataType dt) {
  switch (dt) {
    case DataType::NUMBER: return "number";
    case DataType::BOOLEAN: return "boolean";
    case DataType::VECTOR_NUMBER: return "vector_number";
    case DataType::VECTOR_BOOLEAN: return "vector_boolean";
    default: return "unknown";
  }
}

// Describes the type signature of one port direction for an operator.
struct PortSig {
  DataType data_type = DataType::UNKNOWN;    // type for data ports (i/o)
  DataType control_type = DataType::UNKNOWN; // type for control ports (c)
};

// Return the output port type and input port type for a known operator.
// For operators whose port type depends on configuration, inspect the
// OperatorDef to figure it out.
static PortSig output_sig(const OperatorDef& op) {
  const auto& t = op.type;

  // Operators that output VectorNumber
  if (t == "Input" || t == "Output" || t == "VectorCompose" ||
      t == "VectorProject" || t == "KeyedPipeline")
    return {DataType::VECTOR_NUMBER};

  // Operators that output Number
  if (t == "VectorExtract" || t == "CumulativeSum" || t == "CountNumber" ||
      t == "MovingAverage" || t == "MovingSum" || t == "StandardDeviation" ||
      t == "PeakDetector" || t == "Division" || t == "Multiplication" ||
      t == "Addition" || t == "Subtraction" || t == "ConstantNumber" ||
      t == "Linear" || t == "Power" || t == "Add" || t == "Difference" ||
      t == "FiniteImpulseResponse" || t == "InfiniteImpulseResponse" ||
      t == "ResamplerConstant" || t == "ResamplerHermite" ||
      t == "Sin" || t == "Cos" || t == "Tan" || t == "Exp" ||
      t == "Log" || t == "Log10" || t == "Abs" || t == "Sign" ||
      t == "Floor" || t == "Ceil" || t == "Round" ||
      t == "Identity" || t == "TimeShift" || t == "Variable" ||
      t == "Replace")
    return {DataType::NUMBER};

  // Operators that output Boolean
  if (t == "CompareGT" || t == "CompareLT" || t == "CompareGTE" ||
      t == "CompareLTE" || t == "CompareEQ" || t == "CompareNEQ" ||
      t == "LogicalAnd" || t == "LogicalOr" || t == "LogicalXor" ||
      t == "LogicalNand" || t == "LogicalNor" || t == "LogicalImplication")
    return {DataType::BOOLEAN};

  // Demultiplexer / Multiplexer: output type depends on portType param
  if (t == "Demultiplexer" || t == "Multiplexer") {
    auto it = op.string_params.find("portType");
    if (it != op.string_params.end()) {
      if (it->second == "vector_number") return {DataType::VECTOR_NUMBER};
      if (it->second == "boolean") return {DataType::BOOLEAN};
      if (it->second == "vector_boolean") return {DataType::VECTOR_BOOLEAN};
    }
    return {DataType::NUMBER};
  }

  return {DataType::UNKNOWN};
}

static PortSig input_sig(const OperatorDef& op, const std::string& port) {
  const auto& t = op.type;

  // Control ports always expect Boolean
  if (!port.empty() && port[0] == 'c')
    return {DataType::BOOLEAN};

  // Operators that accept VectorNumber on data ports
  if (t == "Input" || t == "Output" || t == "VectorExtract" ||
      t == "VectorProject" || t == "KeyedPipeline")
    return {DataType::VECTOR_NUMBER};

  // VectorCompose accepts Number on each data port
  if (t == "VectorCompose")
    return {DataType::NUMBER};

  // Operators that accept Number on data ports
  if (t == "CumulativeSum" || t == "CountNumber" ||
      t == "MovingAverage" || t == "MovingSum" || t == "StandardDeviation" ||
      t == "PeakDetector" || t == "Division" || t == "Multiplication" ||
      t == "Addition" || t == "Subtraction" || t == "ConstantNumber" ||
      t == "Linear" || t == "Power" || t == "Add" || t == "Difference" ||
      t == "FiniteImpulseResponse" || t == "InfiniteImpulseResponse" ||
      t == "ResamplerConstant" || t == "ResamplerHermite" ||
      t == "Sin" || t == "Cos" || t == "Tan" || t == "Exp" ||
      t == "Log" || t == "Log10" || t == "Abs" || t == "Sign" ||
      t == "Floor" || t == "Ceil" || t == "Round" ||
      t == "Identity" || t == "TimeShift" || t == "Variable" ||
      t == "Replace")
    return {DataType::NUMBER};

  // Compare operators accept Number
  if (t == "CompareGT" || t == "CompareLT" || t == "CompareGTE" ||
      t == "CompareLTE" || t == "CompareEQ" || t == "CompareNEQ")
    return {DataType::NUMBER};

  // Logical operators accept Boolean
  if (t == "LogicalAnd" || t == "LogicalOr" || t == "LogicalXor" ||
      t == "LogicalNand" || t == "LogicalNor" || t == "LogicalImplication")
    return {DataType::BOOLEAN};

  // Demultiplexer / Multiplexer: data port type depends on portType param
  if (t == "Demultiplexer" || t == "Multiplexer") {
    auto it = op.string_params.find("portType");
    if (it != op.string_params.end()) {
      if (it->second == "vector_number") return {DataType::VECTOR_NUMBER};
      if (it->second == "boolean") return {DataType::BOOLEAN};
      if (it->second == "vector_boolean") return {DataType::VECTOR_BOOLEAN};
    }
    return {DataType::NUMBER};
  }

  return {DataType::UNKNOWN};
}

// Validate a set of operators + connections and append errors.
static void validate_graph(const std::vector<OperatorDef>& ops,
                           const std::vector<Connection>& conns,
                           const std::string& context_prefix,
                           std::vector<std::string>& errors) {
  // Build operator lookup
  std::map<std::string, const OperatorDef*> op_map;
  for (const auto& op : ops) op_map[op.id] = &op;

  // --- 1. Check connections reference valid operators ---
  for (const auto& c : conns) {
    if (!op_map.count(c.from_id))
      errors.push_back(context_prefix + "connection references unknown source operator: " + c.from_id);
    if (!op_map.count(c.to_id))
      errors.push_back(context_prefix + "connection references unknown target operator: " + c.to_id);
  }

  // --- 2. Check required parameters ---
  auto require_param = [&](const OperatorDef& op, const std::string& param) {
    if (!op.params.count(param))
      errors.push_back(context_prefix + op.type + " (" + op.id +
                        ") missing required parameter: " + param);
  };
  auto require_string_param = [&](const OperatorDef& op, const std::string& param) {
    if (!op.string_params.count(param))
      errors.push_back(context_prefix + op.type + " (" + op.id +
                        ") missing required parameter: " + param);
  };

  for (const auto& op : ops) {
    if (op.type == "VectorExtract") require_param(op, "index");
    else if (op.type == "VectorProject") {
      if (!op.int_array_params.count("indices"))
        errors.push_back(context_prefix + op.type + " (" + op.id +
                          ") missing required parameter: indices");
    }
    else if (op.type == "VectorCompose") require_param(op, "numPorts");
    else if (op.type == "MovingAverage" || op.type == "MovingSum" ||
             op.type == "StandardDeviation" || op.type == "PeakDetector")
      require_param(op, "window_size");
    else if (op.type == "CompareGT" || op.type == "CompareLT" ||
             op.type == "CompareGTE" || op.type == "CompareLTE" ||
             op.type == "CompareEQ" || op.type == "CompareNEQ")
      require_param(op, "value");
    else if (op.type == "Division" || op.type == "Multiplication" ||
             op.type == "Addition" || op.type == "Subtraction" ||
             op.type == "LogicalAnd" || op.type == "LogicalOr")
      require_param(op, "numPorts");
    else if (op.type == "Demultiplexer" || op.type == "Multiplexer")
      require_param(op, "numPorts");
    else if (op.type == "ResamplerConstant")
      require_param(op, "interval");
    else if (op.type == "ConstantNumber")
      require_param(op, "value");
    else if (op.type == "KeyedPipeline") {
      require_param(op, "key_index");
      require_string_param(op, "prototype");
    }
  }

  // --- 3. Check port type compatibility on connections ---
  for (const auto& c : conns) {
    auto from_it = op_map.find(c.from_id);
    auto to_it = op_map.find(c.to_id);
    if (from_it == op_map.end() || to_it == op_map.end()) continue;

    auto src = output_sig(*from_it->second);
    auto dst = input_sig(*to_it->second, c.to_port);

    if (src.data_type != DataType::UNKNOWN &&
        dst.data_type != DataType::UNKNOWN &&
        src.data_type != dst.data_type) {
      errors.push_back(
          context_prefix + "type mismatch: " +
          c.from_id + " (" + from_it->second->type + ") output is " +
          dtype_name(src.data_type) + " but " +
          c.to_id + " (" + to_it->second->type + ") port " + c.to_port +
          " expects " + dtype_name(dst.data_type));
    }
  }
}

std::vector<std::string> GraphBuilder::validate() const {
  std::vector<std::string> errors;

  // Validate outer graph
  validate_graph(operators_, connections_, "", errors);

  // Validate each prototype's internal graph
  for (const auto& proto : prototypes_) {
    validate_graph(proto.operators, proto.connections,
                   "prototype " + proto.id + ": ", errors);
  }

  // Check that entry and output operators exist
  bool has_input = false, has_output = false;
  for (const auto& op : operators_) {
    if (op.type == "Input") has_input = true;
    if (op.type == "Output") has_output = true;
  }
  if (!has_input) errors.push_back("graph is missing an Input operator");
  if (!has_output) errors.push_back("graph is missing an Output operator");

  return errors;
}

std::string GraphBuilder::to_json() const {
  // Build prototype lookup by ID
  std::map<std::string, const PrototypeDef*> proto_map;
  for (const auto& p : prototypes_) {
    proto_map[p.id] = &p;
  }

  // Find entry and output operator IDs
  std::string entry_id, output_id;
  for (const auto& op : operators_) {
    if (op.type == "Input") entry_id = op.id;
    if (op.type == "Output") output_id = op.id;
  }

  json program;
  program["title"] = "<auto-generated>";
  if (!entry_id.empty()) {
    program["entryOperator"] = entry_id;
  }
  if (!output_id.empty()) {
    program["output"] = {{output_id, json::array({"o1"})}};
  }

  program["operators"] = json::array();
  for (const auto& op : operators_) {
    if (op.type == "KeyedPipeline") {
      // Build operator JSON, inlining the referenced prototype
      json j = operator_to_json(op);
      j.erase("prototype");  // remove the string ref

      auto proto_it = op.string_params.find("prototype");
      if (proto_it != op.string_params.end()) {
        auto pm_it = proto_map.find(proto_it->second);
        if (pm_it != proto_map.end()) {
          j["prototype"] = prototype_to_json(*pm_it->second);
        }
      }
      program["operators"].push_back(j);
    } else {
      program["operators"].push_back(operator_to_json(op));
    }
  }

  program["connections"] = json::array();
  for (const auto& c : connections_) {
    program["connections"].push_back(connection_to_json(c));
  }

  return program.dump(2);
}

}  // namespace rtbot_sql::compiler
