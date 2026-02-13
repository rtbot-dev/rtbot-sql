#include "rtbot_sql/compiler/select_compiler.h"

#include <algorithm>
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
    GraphBuilder& builder) {
  // SELECT * → identity passthrough
  if (select_list.empty()) {
    // Build field map from scope — not available directly, so return empty map.
    // The caller (query planner) will fill in the field map from the source schema.
    return {input_endpoint, {}};
  }

  // Optimization: all plain ColumnRefs → VectorProject
  if (all_column_refs(select_list)) {
    std::map<std::string, double> params;
    params["numIndices"] = static_cast<double>(select_list.size());
    FieldMap field_map;

    for (size_t i = 0; i < select_list.size(); ++i) {
      const auto& col =
          std::get<parser::ast::ColumnRef>(select_list[i].expr);
      auto result = scope.resolve(col);
      if (auto* err = std::get_if<std::string>(&result)) {
        throw std::runtime_error(*err);
      }
      auto& binding = std::get<analyzer::ColumnBinding>(result);
      params["index_" + std::to_string(i)] =
          static_cast<double>(binding.index);

      std::string alias = select_list[i].alias.value_or(col.column_name);
      field_map[alias] = static_cast<int>(i);
    }

    auto proj_id = builder.next_id("proj");
    builder.add_operator(proj_id, "VectorProject", params);
    builder.connect(input_endpoint, {proj_id, "i1"});
    return {{proj_id, "o1"}, field_map};
  }

  // Mixed: compile each item, compose with VectorCompose
  std::vector<Endpoint> endpoints;
  FieldMap field_map;

  for (size_t i = 0; i < select_list.size(); ++i) {
    const auto& item = select_list[i];
    auto result =
        compile_expression(item.expr, input_endpoint, scope, builder);
    auto ep = ensure_endpoint(std::move(result), input_endpoint, builder);
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
