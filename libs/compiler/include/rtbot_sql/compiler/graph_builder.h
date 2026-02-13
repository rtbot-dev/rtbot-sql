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
};

struct Connection {
  std::string from_id;
  std::string from_port;
  std::string to_id;
  std::string to_port;
};

class GraphBuilder {
 public:
  std::string next_id(const std::string& prefix);

  void add_operator(const std::string& id, const std::string& type,
                    const std::map<std::string, double>& params = {});

  void connect(const Endpoint& from, const Endpoint& to);

  std::string to_json() const;

  // Accessors for testing
  const std::vector<OperatorDef>& operators() const { return operators_; }
  const std::vector<Connection>& connections() const { return connections_; }
  const OperatorDef* find_operator(const std::string& id) const;

 private:
  std::vector<OperatorDef> operators_;
  std::vector<Connection> connections_;
  int id_counter_ = 0;
};

}  // namespace rtbot_sql::compiler
