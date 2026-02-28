#include "rtbot_sql/compiler/select_compiler.h"

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expression_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Generate a default alias for an expression without an explicit alias.
std::string default_alias(const parser::ast::Expr& expr) {
  // ColumnRef → column name
  if (auto* col = std::get_if<parser::ast::ColumnRef>(&expr)) {
    return col->column_name;
  }
  // FuncCall → func_name_arg_name (e.g., sum_quantity)
  if (auto* func_ptr =
          std::get_if<std::unique_ptr<parser::ast::FuncCall>>(&expr)) {
    const auto& func = **func_ptr;
    std::string name = func.name;
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    if (!func.args.empty()) {
      if (auto* col =
              std::get_if<parser::ast::ColumnRef>(&func.args[0])) {
        return name + "_" + col->column_name;
      }
    }
    return name;
  }
  return "expr";
}

// Check if all items in the select list are plain ColumnRefs.
bool all_column_refs(
    const std::vector<parser::ast::SelectItem>& select_list) {
  for (const auto& item : select_list) {
    if (!std::holds_alternative<parser::ast::ColumnRef>(item.expr)) {
      return false;
    }
  }
  return true;
}

// Ensure an ExprResult is an Endpoint, materializing constants if needed.
Endpoint ensure_endpoint(ExprResult result, const Endpoint& input_endpoint,
                         GraphBuilder& builder) {
  if (auto* ep = std::get_if<Endpoint>(&result)) {
    return *ep;
  }
  auto& cm = std::get<ConstantMarker>(result);
  auto id = builder.next_id("const");
  builder.add_operator(id, "ConstantNumber", {{"value", cm.value}});
  builder.connect(input_endpoint, {id, "i1"});
  return {id, "o1"};
}

}  // namespace

SelectResult compile_select_projection(
    const std::vector<parser::ast::SelectItem>& select_list,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    const std::map<std::string, Endpoint>* source_endpoints) {
  // SELECT * → identity passthrough
  if (select_list.empty()) {
    // Build field map from scope — not available directly, so return empty map.
    // The caller (query planner) will fill in the field map from the source schema.
    return {input_endpoint, {}};
  }

  // Optimization: all plain ColumnRefs from the same source → VectorProject
  bool can_use_vector_project = all_column_refs(select_list);
  Endpoint projection_input = input_endpoint;
  if (can_use_vector_project && source_endpoints) {
    std::string projection_stream;
    bool has_projection_stream = false;

    for (const auto& item : select_list) {
      const auto& col = std::get<parser::ast::ColumnRef>(item.expr);
      auto result = scope.resolve(col);
      if (auto* err = std::get_if<std::string>(&result)) {
        throw std::runtime_error(*err);
      }
      const auto& binding = std::get<analyzer::ColumnBinding>(result);
      if (!has_projection_stream) {
        projection_stream = binding.stream_name;
        has_projection_stream = true;
      } else if (binding.stream_name != projection_stream) {
        can_use_vector_project = false;
        break;
      }
    }

    if (can_use_vector_project && has_projection_stream) {
      auto it = source_endpoints->find(projection_stream);
      if (it != source_endpoints->end()) {
        projection_input = it->second;
      }
    }
  }

  if (can_use_vector_project) {
    std::vector<int> indices;
    FieldMap field_map;

    for (size_t i = 0; i < select_list.size(); ++i) {
      const auto& col =
          std::get<parser::ast::ColumnRef>(select_list[i].expr);
      auto result = scope.resolve(col);
      if (auto* err = std::get_if<std::string>(&result)) {
        throw std::runtime_error(*err);
      }
      auto& binding = std::get<analyzer::ColumnBinding>(result);
      indices.push_back(binding.index);

      std::string alias = select_list[i].alias.value_or(col.column_name);
      field_map[alias] = static_cast<int>(i);
    }

    auto proj_id = builder.next_id("proj");
    builder.add_operator(proj_id, "VectorProject", {}, {}, {},
                         {{"indices", indices}});
    builder.connect(projection_input, {proj_id, "i1"});
    return {{proj_id, "o1"}, field_map};
  }

  // Mixed: compile each item, compose with VectorCompose
  std::vector<Endpoint> endpoints;
  FieldMap field_map;

  std::optional<Endpoint> sync_zero_ep;
  if (source_endpoints && source_endpoints->size() > 1) {
    std::vector<Endpoint> source_clocks;
    source_clocks.reserve(source_endpoints->size());
    for (const auto& [_, source_ep] : *source_endpoints) {
      auto clock_id = builder.next_id("clock");
      builder.add_operator(clock_id, "VectorExtract", {{"index", 0.0}});
      builder.connect(source_ep, {clock_id, "i1"});
      source_clocks.push_back({clock_id, "o1"});
    }

    Endpoint any_clock = source_clocks.front();
    for (size_t i = 1; i < source_clocks.size(); ++i) {
      auto add_id = builder.next_id("clock_sync");
      builder.add_operator(add_id, "Addition", {{"numPorts", 2.0}});
      builder.connect(any_clock, {add_id, "i1"});
      builder.connect(source_clocks[i], {add_id, "i2"});
      any_clock = {add_id, "o1"};
    }

    auto zero_id = builder.next_id("const");
    builder.add_operator(zero_id, "ConstantNumber", {{"value", 0.0}});
    builder.connect(any_clock, {zero_id, "i1"});
    sync_zero_ep = Endpoint{zero_id, "o1"};
  }

  for (size_t i = 0; i < select_list.size(); ++i) {
    const auto& item = select_list[i];
    auto result =
        compile_expression(item.expr, input_endpoint, scope, builder, nullptr,
                           source_endpoints);
    auto ep = ensure_endpoint(std::move(result), input_endpoint, builder);

    if (sync_zero_ep.has_value()) {
      auto sync_id = builder.next_id("col_sync");
      builder.add_operator(sync_id, "Addition", {{"numPorts", 2.0}});
      builder.connect(ep, {sync_id, "i1"});
      builder.connect(*sync_zero_ep, {sync_id, "i2"});
      ep = {sync_id, "o1"};
    }

    endpoints.push_back(ep);

    std::string alias =
        item.alias.value_or(default_alias(item.expr));
    field_map[alias] = static_cast<int>(i);
  }

  auto compose_id = builder.next_id("compose");
  builder.add_operator(
      compose_id, "VectorCompose",
      {{"numPorts", static_cast<double>(endpoints.size())}});
  for (size_t i = 0; i < endpoints.size(); ++i) {
    builder.connect(endpoints[i],
                    {compose_id, "i" + std::to_string(i + 1)});
  }

  return {{compose_id, "o1"}, field_map};
}

}  // namespace rtbot_sql::compiler
