#pragma once

#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace rtbot_sql::compiler {

struct Endpoint {
  std::string operator_id;
  std::string port;  // "o1", "i1", etc.
};

struct OperatorDef {
  std::string id;
  std::string type;
  std::map<std::string, double> params;
  std::map<std::string, std::string> string_params;
  std::map<std::string, std::vector<double>> double_array_params;
  std::map<std::string, std::vector<int>> int_array_params;
};

struct Connection {
  std::string from_id;
  std::string from_port;
  std::string to_id;
  std::string to_port;
};

struct PrototypeDef {
  std::string id;
  std::string entry_id;   // e.g. "proto_in"
  std::string output_id;  // e.g. "proto_out"
  std::vector<OperatorDef> operators;
  std::vector<Connection> connections;
};

class GraphBuilder {
 public:
  std::string next_id(const std::string& prefix);

  void add_operator(
      const std::string& id, const std::string& type,
      const std::map<std::string, double>& params = {},
      const std::map<std::string, std::string>& string_params = {},
      const std::map<std::string, std::vector<double>>& double_array_params = {},
      const std::map<std::string, std::vector<int>>& int_array_params = {});

  void connect(const Endpoint& from, const Endpoint& to);

  void add_prototype(const PrototypeDef& proto);

  std::string to_json() const;

  // Validate the graph structure and return a list of error strings.
  // Returns empty vector if the graph is valid.
  std::vector<std::string> validate() const;

  // Parse a stored program JSON back into a builder ready for augmentation.
  // All operators (including Output) are loaded, but the connection TO the
  // Output operator's input port is dropped.  Returns the endpoint that was
  // connected to Output so callers can insert additional processing before
  // re-wiring to Output themselves.
  static std::pair<GraphBuilder, Endpoint> from_json_for_augmentation(
      const std::string& json_str);

  // Accessors for testing
  const std::vector<OperatorDef>& operators() const { return operators_; }
  const std::vector<Connection>& connections() const { return connections_; }
  const OperatorDef* find_operator(const std::string& id) const;
  const std::vector<PrototypeDef>& prototypes() const { return prototypes_; }
  const PrototypeDef* find_prototype(const std::string& id) const;

 private:
  std::vector<OperatorDef> operators_;
  std::vector<Connection> connections_;
  std::vector<PrototypeDef> prototypes_;
  int id_counter_ = 0;
};

}  // namespace rtbot_sql::compiler
