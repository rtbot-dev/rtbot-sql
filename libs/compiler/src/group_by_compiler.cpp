#include "rtbot_sql/compiler/group_by_compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expr_cache.h"
#include "rtbot_sql/compiler/expression_compiler.h"
#include "rtbot_sql/compiler/where_compiler.h"

namespace rtbot_sql::compiler {

namespace {

// Check if a SelectItem is the GROUP BY key column.
bool is_group_by_key(const parser::ast::SelectItem& item,
                     const parser::ast::Expr& group_by_expr,
                     const analyzer::Scope& scope) {
  auto* item_col = std::get_if<parser::ast::ColumnRef>(&item.expr);
  auto* key_col = std::get_if<parser::ast::ColumnRef>(&group_by_expr);
  if (!item_col || !key_col) return false;

  auto item_res = scope.resolve(*item_col);
  auto key_res = scope.resolve(*key_col);
  auto* item_bind = std::get_if<analyzer::ColumnBinding>(&item_res);
  auto* key_bind = std::get_if<analyzer::ColumnBinding>(&key_res);
  if (!item_bind || !key_bind) return false;

  return item_bind->index == key_bind->index;
}

// Generate a default alias for an expression.
std::string default_alias(const parser::ast::Expr& expr) {
  if (auto* col = std::get_if<parser::ast::ColumnRef>(&expr)) {
    return col->column_name;
  }
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

// Convert a VectorNumber endpoint into a Number endpoint for clocking.
Endpoint scalar_clock(const Endpoint& vec_input, GraphBuilder& builder) {
  auto id = builder.next_id("clock");
  builder.add_operator(id, "VectorExtract", {{"index", 0.0}});
  builder.connect(vec_input, {id, "i1"});
  return {id, "o1"};
}

// Ensure an ExprResult is an Endpoint, materializing constants if needed.
Endpoint ensure_endpoint(ExprResult result, const Endpoint& input_endpoint,
                         GraphBuilder& builder) {
  if (auto* ep = std::get_if<Endpoint>(&result)) {
    return *ep;
  }
  auto& cm = std::get<ConstantMarker>(result);
  auto clock_ep = scalar_clock(input_endpoint, builder);
  auto id = builder.next_id("const");
  builder.add_operator(id, "ConstantNumber", {{"value", cm.value}});
  builder.connect(clock_ep, {id, "i1"});
  return {id, "o1"};
}

// Cache-aware expression compilation (delegates to compile_expression with cache).
ExprResult compile_expression_cached(const parser::ast::Expr& expr,
                                     const Endpoint& input_endpoint,
                                     const analyzer::Scope& scope,
                                     GraphBuilder& builder,
                                     ExprCache& cache) {
  return compile_expression(expr, input_endpoint, scope, builder, &cache);
}

// Map comparison op to RTBot operator type.
std::string comparison_to_rtbot(const std::string& op) {
  if (op == ">") return "CompareGT";
  if (op == "<") return "CompareLT";
  if (op == ">=") return "CompareGTE";
  if (op == "<=") return "CompareLTE";
  if (op == "=") return "CompareEQ";
  if (op == "!=") return "CompareNEQ";
  throw std::runtime_error("unknown comparison operator: " + op);
}

// Flip comparison direction (for constant on left side).
std::string flip_comparison(const std::string& op) {
  if (op == ">") return "<";
  if (op == "<") return ">";
  if (op == ">=") return "<=";
  if (op == "<=") return ">=";
  return op;  // = and != are symmetric
}

// Detect HAVING MOVING_COUNT(N) OP threshold pattern.
// Returns {window_size, threshold, rtbot_compare_type} if matched, nullopt otherwise.
struct VelocityPattern {
  int window_size;
  double threshold;
  std::string rtbot_type;  // "CompareGT", "CompareGTE", etc.
};

std::optional<VelocityPattern> detect_velocity_pattern(
    const parser::ast::Expr& having_expr) {
  using namespace parser::ast;

  auto* cmp_ptr = std::get_if<std::unique_ptr<ComparisonExpr>>(&having_expr);
  if (!cmp_ptr) return std::nullopt;
  const auto& cmp = **cmp_ptr;

  // Check: MOVING_COUNT(N) OP constant
  auto extract = [](const Expr& e) -> std::optional<int> {
    auto* fp = std::get_if<std::unique_ptr<FuncCall>>(&e);
    if (!fp) return std::nullopt;
    const auto& f = **fp;
    std::string upper = f.name;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    if (upper != "MOVING_COUNT" || f.args.size() != 1) return std::nullopt;
    auto* c = std::get_if<Constant>(&f.args[0]);
    if (!c || c->value <= 0 || c->value != static_cast<int>(c->value))
      return std::nullopt;
    return static_cast<int>(c->value);
  };

  auto mc_left = extract(cmp.left);
  auto* right_const = std::get_if<Constant>(&cmp.right);

  if (mc_left.has_value() && right_const) {
    // MOVING_COUNT(N) OP threshold
    static const std::map<std::string, std::string> op_map = {
        {">", "CompareGT"}, {"<", "CompareLT"},
        {">=", "CompareGTE"}, {"<=", "CompareLTE"},
        {"=", "CompareEQ"}, {"!=", "CompareNEQ"}};
    auto it = op_map.find(cmp.op);
    if (it == op_map.end()) return std::nullopt;
    return VelocityPattern{*mc_left, right_const->value, it->second};
  }

  auto mc_right = extract(cmp.right);
  auto* left_const = std::get_if<Constant>(&cmp.left);

  if (mc_right.has_value() && left_const) {
    // constant OP MOVING_COUNT(N) — flip direction
    static const std::map<std::string, std::string> flipped_map = {
        {">", "CompareLT"}, {"<", "CompareGT"},
        {">=", "CompareLTE"}, {"<=", "CompareGTE"},
        {"=", "CompareEQ"}, {"!=", "CompareNEQ"}};
    auto it = flipped_map.find(cmp.op);
    if (it == flipped_map.end()) return std::nullopt;
    return VelocityPattern{*mc_right, left_const->value, it->second};
  }

  return std::nullopt;
}

// Cache-aware HAVING predicate compilation.
Endpoint compile_having_predicate(const parser::ast::Expr& expr,
                                  const Endpoint& input_endpoint,
                                  const analyzer::Scope& scope,
                                  GraphBuilder& builder,
                                  ExprCache& cache) {
  using namespace parser::ast;

  // ComparisonExpr: e.g. COUNT(*) > 5
  if (auto* cmp_ptr = std::get_if<std::unique_ptr<ComparisonExpr>>(&expr)) {
    const auto& cmp = **cmp_ptr;

    auto left = compile_expression_cached(cmp.left, input_endpoint, scope,
                                          builder, cache);
    auto right = compile_expression_cached(cmp.right, input_endpoint, scope,
                                           builder, cache);

    auto* left_const = std::get_if<ConstantMarker>(&left);
    auto* right_const = std::get_if<ConstantMarker>(&right);

    if (left_const && right_const) {
      throw std::runtime_error(
          "comparison of two constants is not supported in HAVING");
    }

    // stream OP constant
    if (right_const) {
      auto& stream_ep = std::get<Endpoint>(left);
      auto id = builder.next_id("cmp");
      builder.add_operator(id, comparison_to_rtbot(cmp.op),
                           {{"value", right_const->value}});
      builder.connect(stream_ep, {id, "i1"});
      return {id, "o1"};
    }

    // constant OP stream → flip
    if (left_const) {
      auto& stream_ep = std::get<Endpoint>(right);
      auto id = builder.next_id("cmp");
      builder.add_operator(id, comparison_to_rtbot(flip_comparison(cmp.op)),
                           {{"value", left_const->value}});
      builder.connect(stream_ep, {id, "i1"});
      return {id, "o1"};
    }

    // Both sides are stream endpoints: synchronise by timestamp and compare.
    // This is the pattern used by Bollinger-band style HAVING clauses, e.g.:
    //   HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
    //   HAVING fuel_level < MOVING_AVERAGE(fuel_level, 20) - 10.0
    // Inside the KeyedPipeline prototype every operator emits once per input
    // message, so the two endpoints are always timestamp-aligned and
    // CompareSync* produces the correct boolean gate.
    {
      auto& left_ep = std::get<Endpoint>(left);
      auto& right_ep = std::get<Endpoint>(right);
      std::string rtbot_type;
      if (cmp.op == ">") rtbot_type = "CompareSyncGT";
      else if (cmp.op == "<") rtbot_type = "CompareSyncLT";
      else if (cmp.op == ">=") rtbot_type = "CompareSyncGTE";
      else if (cmp.op == "<=") rtbot_type = "CompareSyncLTE";
      else if (cmp.op == "=") rtbot_type = "CompareSyncEQ";
      else if (cmp.op == "!=") rtbot_type = "CompareSyncNEQ";
      else
        throw std::runtime_error("unknown comparison operator in HAVING: " +
                                 cmp.op);
      auto id = builder.next_id("cmp_sync");
      builder.add_operator(id, rtbot_type);
      builder.connect(left_ep, {id, "i1"});
      builder.connect(right_ep, {id, "i2"});
      return {id, "o1"};
    }
  }

  // LogicalExpr: AND/OR
  if (auto* log_ptr = std::get_if<std::unique_ptr<LogicalExpr>>(&expr)) {
    const auto& log = **log_ptr;
    auto left_ep = compile_having_predicate(log.left, input_endpoint, scope,
                                            builder, cache);
    auto right_ep = compile_having_predicate(log.right, input_endpoint, scope,
                                             builder, cache);

    std::string upper_op = log.op;
    std::transform(upper_op.begin(), upper_op.end(), upper_op.begin(),
                   ::toupper);

    std::string rtbot_type;
    if (upper_op == "AND")
      rtbot_type = "LogicalAnd";
    else if (upper_op == "OR")
      rtbot_type = "LogicalOr";
    else
      throw std::runtime_error("unknown logical operator: " + log.op);

    auto id = builder.next_id(rtbot_type == "LogicalAnd" ? "and" : "or");
    builder.add_operator(id, rtbot_type, {{"numPorts", 2}});
    builder.connect(left_ep, {id, "i1"});
    builder.connect(right_ep, {id, "i2"});
    return {id, "o1"};
  }

  throw std::runtime_error("unsupported HAVING predicate expression type");
}

}  // namespace

// --- GroupByClassification helper methods ---

bool GroupByClassification::has_persistent_keys() const {
  for (const auto& item : items) {
    if (item.kind == GroupByItemKind::PERSISTENT_KEY) return true;
  }
  return false;
}

bool GroupByClassification::has_segment_expressions() const {
  for (const auto& item : items) {
    if (item.kind == GroupByItemKind::SEGMENT_EXPRESSION) return true;
  }
  return false;
}

int GroupByClassification::persistent_key_count() const {
  int count = 0;
  for (const auto& item : items) {
    if (item.kind == GroupByItemKind::PERSISTENT_KEY) ++count;
  }
  return count;
}

int GroupByClassification::segment_expression_count() const {
  int count = 0;
  for (const auto& item : items) {
    if (item.kind == GroupByItemKind::SEGMENT_EXPRESSION) ++count;
  }
  return count;
}

// --- classify_group_by ---

GroupByClassification classify_group_by(
    const std::vector<parser::ast::Expr>& group_by,
    const analyzer::Scope& scope) {
  GroupByClassification result;
  for (const auto& expr : group_by) {
    auto* col_ref = std::get_if<parser::ast::ColumnRef>(&expr);
    if (col_ref) {
      // Attempt to resolve: if it resolves to a column, it's a persistent key
      auto resolved = scope.resolve(*col_ref);
      if (auto* binding = std::get_if<analyzer::ColumnBinding>(&resolved)) {
        result.items.push_back(
            {GroupByItemKind::PERSISTENT_KEY, binding->index, col_ref->column_name});
        continue;
      }
    }
    // Boolean-producing expressions → SEGMENT_EXPRESSION
    // Numeric expressions (FuncCall, BinaryExpr) → also SEGMENT_EXPRESSION
    // Everything else (boolean or numeric) is a segment expression
    result.items.push_back({GroupByItemKind::SEGMENT_EXPRESSION, -1, ""});
  }
  return result;
}

SelectResult compile_group_by(
    const std::vector<parser::ast::SelectItem>& select_list,
    const std::vector<parser::ast::Expr>& group_by,
    const std::optional<parser::ast::Expr>& having,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    int num_input_cols) {
  using namespace parser::ast;

  // --- Step 0: Classify GROUP BY items ---
  if (group_by.empty()) {
    throw std::runtime_error("GROUP BY requires at least one column");
  }

  auto classification = classify_group_by(group_by, scope);

  // Mixed GROUP BY (persistent keys + segment expressions)
  if (classification.has_persistent_keys() && classification.has_segment_expressions()) {
    if (classification.segment_expression_count() > 1) {
      throw std::runtime_error(
          "multiple segment expressions in GROUP BY not yet supported");
    }
    if (having.has_value()) {
      throw std::runtime_error(
          "HAVING with mixed GROUP BY not yet supported");
    }

    // --- Collect all persistent keys ---
    struct KeyInfo { int index; std::string name; };
    std::vector<KeyInfo> keys;
    for (const auto& gi : classification.items) {
      if (gi.kind == GroupByItemKind::PERSISTENT_KEY) {
        keys.push_back({gi.key_index, gi.key_name});
      }
    }

    const bool composite = keys.size() > 1;

    // --- Identify the segment expression ---
    const Expr* segment_expr = nullptr;
    for (size_t i = 0; i < group_by.size(); ++i) {
      if (classification.items[i].kind == GroupByItemKind::SEGMENT_EXPRESSION) {
        segment_expr = &group_by[i];
        break;
      }
    }

    // --- For composite keys: compute key coefficients for KeyedPipeline ---
    Endpoint keyed_input = input_endpoint;
    std::vector<int> key_column_indices;
    std::vector<double> key_coefficients;

    if (composite) {
      static const double PRIME = 1000003.0;
      // Precompute coefficients: PRIME^(N-1), PRIME^(N-2), ..., 1
      double coeff = 1.0;
      key_column_indices.reserve(keys.size());
      key_coefficients.resize(keys.size());
      for (int i = static_cast<int>(keys.size()) - 1; i >= 0; --i) {
        key_column_indices.push_back(keys[i].index);
        key_coefficients[i] = coeff;
        coeff *= PRIME;
      }
      // Fix order: key_column_indices was filled backwards
      std::reverse(key_column_indices.begin(), key_column_indices.end());
    }

    // --- Build inner prototype (aggregates for Pipeline) ---
    GraphBuilder inner_proto_builder;
    ExprCache inner_cache;
    inner_proto_builder.add_operator("proto_in", "Input");
    Endpoint inner_input_ep{"proto_in", "o1"};

    std::vector<Endpoint> inner_endpoints;
    std::vector<std::string> field_names;

    // --- Attempt fused aggregate path for inner prototype ---
    // Replaces the per-aggregate operator chains + VectorCompose with a single
    // FusedExpression bytecode interpreter.
    bool inner_fused = false;
    if (std::getenv("RTBOT_DISABLE_FUSION") == nullptr) {
      std::map<std::pair<std::string, int>, int> column_to_input;
      std::vector<double> constants;
      std::vector<double> all_bytecode;
      AggBytecodeContext agg_ctx;
      bool all_fusable = true;
      size_t num_outputs = 0;

      // For composite keys: emit INPUT col_index, END per key column
      // so the FusedExpression passes key values through to the output vector.
      if (composite) {
        for (const auto& ki : keys) {
          auto resolved = scope.resolve(parser::ast::ColumnRef{"", ki.name});
          auto* binding = std::get_if<analyzer::ColumnBinding>(&resolved);
          if (!binding) { all_fusable = false; break; }
          auto col_key = std::make_pair(binding->stream_name, binding->index);
          if (column_to_input.find(col_key) == column_to_input.end()) {
            column_to_input[col_key] = static_cast<int>(column_to_input.size());
          }
          int input_idx = column_to_input[col_key];
          all_bytecode.push_back(0.0);   // fused_op::INPUT
          all_bytecode.push_back(static_cast<double>(input_idx));
          all_bytecode.push_back(20.0);  // fused_op::END
          field_names.push_back(ki.name);
          ++num_outputs;
        }
      } else {
        field_names.push_back(keys[0].name);
      }

      // Compile aggregate expressions to bytecode
      if (all_fusable) {
        for (const auto& select_item : select_list) {
          bool is_key = false;
          for (const auto& gbe : group_by) {
            if (is_group_by_key(select_item, gbe, scope)) { is_key = true; break; }
          }
          if (is_key) continue;

          auto bc = compile_aggregate_expression_to_bytecode(
              select_item.expr, scope, column_to_input, constants, agg_ctx);
          if (!bc.success) { all_fusable = false; break; }
          all_bytecode.insert(all_bytecode.end(), bc.bytecode.begin(), bc.bytecode.end());
          field_names.push_back(
              select_item.alias.value_or(default_alias(select_item.expr)));
          ++num_outputs;
        }
      }

      if (all_fusable && num_outputs > 0) {
        inner_fused = true;

        // Emit VectorExtract per input column
        size_t num_inputs = column_to_input.size();
        std::vector<Endpoint> extract_endpoints(num_inputs);
        for (const auto& [col_key, input_idx] : column_to_input) {
          const auto& [stream_name, col_index] = col_key;
          auto ext_id = inner_proto_builder.next_id("ext");
          inner_proto_builder.add_operator(ext_id, "VectorExtract",
                                           {{"index", static_cast<double>(col_index)}});
          inner_proto_builder.connect(inner_input_ep, {ext_id, "i1"});
          extract_endpoints[input_idx] = {ext_id, "o1"};
        }

        // Emit FusedExpression
        auto fused_id = inner_proto_builder.next_id("fused");
        std::map<std::string, std::vector<double>> double_array_params = {
            {"bytecode", all_bytecode}, {"constants", constants}};
        if (!agg_ctx.state_init.empty()) {
          double_array_params["stateInit"] = agg_ctx.state_init;
        }
        inner_proto_builder.add_operator(
            fused_id, "FusedExpression",
            {{"numPorts", static_cast<double>(num_inputs)},
             {"numOutputs", static_cast<double>(num_outputs)}},
            {},  // no string params
            double_array_params);

        for (size_t i = 0; i < num_inputs; ++i) {
          inner_proto_builder.connect(extract_endpoints[i],
                                      {fused_id, "i" + std::to_string(i + 1)});
        }

        // FusedExpression outputs VectorNumber directly — connect to proto_out
        inner_proto_builder.add_operator("proto_out", "Output");
        inner_proto_builder.connect({fused_id, "o1"}, {"proto_out", "i1"});
      } else {
        // Fusion failed — reset field_names for the fallback path
        field_names.clear();
      }
    }

    // --- Fallback: original per-operator path ---
    if (!inner_fused) {
      if (composite) {
        for (const auto& ki : keys) {
          auto ve_id = inner_proto_builder.next_id("extract");
          inner_proto_builder.add_operator(
              ve_id, "VectorExtract",
              {{"index", static_cast<double>(ki.index)}});
          inner_proto_builder.connect(inner_input_ep, {ve_id, "i1"});
          inner_endpoints.push_back({ve_id, "o1"});
          field_names.push_back(ki.name);
        }
      } else {
        field_names.push_back(keys[0].name);
      }

      for (const auto& select_item : select_list) {
        bool is_key = false;
        for (const auto& gbe : group_by) {
          if (is_group_by_key(select_item, gbe, scope)) { is_key = true; break; }
        }
        if (is_key) continue;

        auto result = compile_expression_cached(
            select_item.expr, inner_input_ep, scope, inner_proto_builder, inner_cache);
        auto ep = ensure_endpoint(std::move(result), inner_input_ep, inner_proto_builder);
        inner_endpoints.push_back(ep);
        field_names.push_back(
            select_item.alias.value_or(default_alias(select_item.expr)));
      }

      // Compose inner prototype outputs
      auto inner_compose_id = inner_proto_builder.next_id("compose");
      inner_proto_builder.add_operator(
          inner_compose_id, "VectorCompose",
          {{"numPorts", static_cast<double>(inner_endpoints.size())}});
      for (size_t i = 0; i < inner_endpoints.size(); ++i) {
        inner_proto_builder.connect(inner_endpoints[i],
                                    {inner_compose_id, "i" + std::to_string(i + 1)});
      }

      inner_proto_builder.add_operator("proto_out", "Output");
      inner_proto_builder.connect({inner_compose_id, "o1"}, {"proto_out", "i1"});
    }

    auto inner_proto_id = builder.next_id("proto");
    PrototypeDef inner_proto_def;
    inner_proto_def.id = inner_proto_id;
    inner_proto_def.entry_id = "proto_in";
    inner_proto_def.output_id = "proto_out";
    inner_proto_def.operators = inner_proto_builder.operators();
    inner_proto_def.connections = inner_proto_builder.connections();
    builder.add_prototype(inner_proto_def);

    // --- Build outer prototype (segment expression + Pipeline) ---
    GraphBuilder outer_proto_builder;
    Endpoint outer_input_ep{"proto_in", "o1"};
    outer_proto_builder.add_operator("proto_in", "Input");

    // Try bytecode path for segment expression first
    auto seg_result = compile_segment_to_bytecode(*segment_expr, scope, "");

    auto pipeline_id = outer_proto_builder.next_id("pipeline");
    if (seg_result.success) {
      // Bytecode path: emit Pipeline with segmentBytecode/segmentConstants
      outer_proto_builder.add_operator(pipeline_id, "Pipeline",
                                       /*params=*/{},
                                       /*string_params=*/{{"prototype", inner_proto_id}},
                                       /*double_array_params=*/{
                                           {"segmentBytecode", seg_result.bytecode},
                                           {"segmentConstants", seg_result.constants}});
      outer_proto_builder.connect(outer_input_ep, {pipeline_id, "i1"});
    } else {
      // Fallback: operator chain (compile_predicate + BooleanToNumber)
      auto bool_ep =
          compile_predicate(*segment_expr, outer_input_ep, scope, outer_proto_builder);

      // Convert boolean predicate to numeric segment key (Pipeline c1 expects NumberData)
      auto b2n_id = outer_proto_builder.next_id("b2n");
      outer_proto_builder.add_operator(b2n_id, "BooleanToNumber");
      outer_proto_builder.connect(bool_ep, {b2n_id, "i1"});
      Endpoint num_ep{b2n_id, "o1"};

      outer_proto_builder.add_operator(pipeline_id, "Pipeline",
                                       /*params=*/{},
                                       /*string_params=*/{{"prototype", inner_proto_id}});
      outer_proto_builder.connect(outer_input_ep, {pipeline_id, "i1"});
      outer_proto_builder.connect(num_ep, {pipeline_id, "c1"});
    }

    outer_proto_builder.add_operator("proto_out", "Output");
    outer_proto_builder.connect({pipeline_id, "o1"}, {"proto_out", "i1"});

    auto outer_proto_id = builder.next_id("proto");
    PrototypeDef outer_proto_def;
    outer_proto_def.id = outer_proto_id;
    outer_proto_def.entry_id = "proto_in";
    outer_proto_def.output_id = "proto_out";
    outer_proto_def.operators = outer_proto_builder.operators();
    outer_proto_def.connections = outer_proto_builder.connections();
    builder.add_prototype(outer_proto_def);

    // --- Add KeyedPipeline to outer graph ---
    auto keyed_id = builder.next_id("keyed");
    if (composite) {
      // Computed key mode: KeyedPipeline computes hash internally,
      // output is prototype output directly (no key prepend, no VectorProject).
      builder.add_operator(keyed_id, "KeyedPipeline",
                           {},  // no scalar params
                           {{"prototype", outer_proto_id}},
                           {{"keyCoefficients", key_coefficients}},
                           {{"keyColumnIndices", key_column_indices}});
    } else {
      builder.add_operator(keyed_id, "KeyedPipeline",
                           {{"key_index", static_cast<double>(keys[0].index)}},
                           {{"prototype", outer_proto_id}});
    }
    builder.connect(keyed_input, {keyed_id, "i1"});

    // --- Build field map ---
    if (composite) {
      // Computed key mode: no VectorProject needed, output is directly
      // [key0, key1, ..., agg0, agg1, ...] from prototype
      FieldMap field_map;
      for (size_t i = 0; i < field_names.size(); ++i) {
        field_map[field_names[i]] = static_cast<int>(i);
      }
      return {{keyed_id, "o1"}, field_map, /*is_segment_only=*/false};
    } else {
      // Single key: key at index 0 (prepended by KeyedPipeline), aggregates at 1, 2, ...
      FieldMap field_map;
      for (size_t i = 0; i < field_names.size(); ++i) {
        field_map[field_names[i]] = static_cast<int>(i);
      }
      return {{keyed_id, "o1"}, field_map, /*is_segment_only=*/false};
    }
  }

  // --- Segment-only GROUP BY → Pipeline with control port ---
  if (classification.has_segment_expressions()) {
    if (classification.segment_expression_count() > 1) {
      throw std::runtime_error(
          "multiple segment expressions in GROUP BY not yet supported");
    }

    const auto& seg_expr = group_by[0];

    // Try bytecode path for segment expression first
    auto seg_result = compile_segment_to_bytecode(seg_expr, scope, "");
    bool use_bytecode = seg_result.success;

    // Fallback: compile segment expression as operator chain
    Endpoint num_ep;
    if (!use_bytecode) {
      // Boolean expressions → compile_predicate() + BooleanToNumber
      // Numeric expressions → compile_expression() directly (feeds Pipeline c1)
      bool is_boolean =
          std::get_if<std::unique_ptr<parser::ast::ComparisonExpr>>(&seg_expr) != nullptr
       || std::get_if<std::unique_ptr<parser::ast::LogicalExpr>>(&seg_expr) != nullptr
       || std::get_if<std::unique_ptr<parser::ast::NotExpr>>(&seg_expr) != nullptr
       || std::get_if<std::unique_ptr<parser::ast::BetweenExpr>>(&seg_expr) != nullptr;

      if (is_boolean) {
        auto bool_ep =
            compile_predicate(seg_expr, input_endpoint, scope, builder);
        auto b2n_id = builder.next_id("b2n");
        builder.add_operator(b2n_id, "BooleanToNumber");
        builder.connect(bool_ep, {b2n_id, "i1"});
        num_ep = {b2n_id, "o1"};
      } else {
        // Numeric segment expression (e.g., FLOOR(TS() / N)) — feeds Pipeline c1 directly
        auto result = compile_expression(seg_expr, input_endpoint, scope, builder);
        num_ep = ensure_endpoint(std::move(result), input_endpoint, builder);
      }
    }

    // Build prototype sub-graph for aggregates
    GraphBuilder proto_builder;
    ExprCache cache;
    proto_builder.add_operator("proto_in", "Input");
    Endpoint proto_input_ep{"proto_in", "o1"};

    std::vector<Endpoint> proto_endpoints;
    std::vector<std::string> field_names;

    for (const auto& select_item : select_list) {
      auto result = compile_expression_cached(
          select_item.expr, proto_input_ep, scope, proto_builder, cache);
      auto ep = ensure_endpoint(std::move(result), proto_input_ep, proto_builder);
      proto_endpoints.push_back(ep);
      field_names.push_back(
          select_item.alias.value_or(default_alias(select_item.expr)));
    }

    // Compose prototype outputs into a vector
    auto compose_id = proto_builder.next_id("compose");
    proto_builder.add_operator(
        compose_id, "VectorCompose",
        {{"numPorts", static_cast<double>(proto_endpoints.size())}});
    for (size_t i = 0; i < proto_endpoints.size(); ++i) {
      proto_builder.connect(proto_endpoints[i],
                            {compose_id, "i" + std::to_string(i + 1)});
    }
    Endpoint proto_output_ep = {compose_id, "o1"};

    // Finalize prototype
    proto_builder.add_operator("proto_out", "Output");
    proto_builder.connect(proto_output_ep, {"proto_out", "i1"});

    auto proto_id = builder.next_id("proto");
    PrototypeDef proto_def;
    proto_def.id = proto_id;
    proto_def.entry_id = "proto_in";
    proto_def.output_id = "proto_out";
    proto_def.operators = proto_builder.operators();
    proto_def.connections = proto_builder.connections();
    builder.add_prototype(proto_def);

    // Add Pipeline to outer graph
    auto pipeline_id = builder.next_id("pipeline");
    if (use_bytecode) {
      // Bytecode path: emit Pipeline with segmentBytecode/segmentConstants
      builder.add_operator(pipeline_id, "Pipeline",
                           /*params=*/{},
                           /*string_params=*/{{"prototype", proto_id}},
                           /*double_array_params=*/{
                               {"segmentBytecode", seg_result.bytecode},
                               {"segmentConstants", seg_result.constants}});
      builder.connect(input_endpoint, {pipeline_id, "i1"});
    } else {
      // Fallback: operator chain with control port
      builder.add_operator(pipeline_id, "Pipeline",
                           /*params=*/{},
                           /*string_params=*/{{"prototype", proto_id}});
      builder.connect(input_endpoint, {pipeline_id, "i1"});
      builder.connect(num_ep, {pipeline_id, "c1"});
    }

    // --- HAVING (if present): post-Pipeline filter ---
    Endpoint pipeline_output{pipeline_id, "o1"};
    if (having.has_value()) {
      // Pre-populate cache with VectorExtract endpoints from Pipeline output.
      // Each SELECT item at index i maps to VectorExtract(index=i) on the
      // Pipeline's vector output.  This way, when compile_having_predicate
      // encounters e.g. SUM(quantity), the cache returns the extracted scalar
      // instead of creating a new accumulator operator.
      ExprCache having_cache;
      for (size_t i = 0; i < select_list.size(); ++i) {
        auto ve_id = builder.next_id("having_extract");
        builder.add_operator(ve_id, "VectorExtract",
                             {{"index", static_cast<double>(i)}});
        builder.connect(pipeline_output, {ve_id, "i1"});
        having_cache.store(select_list[i].expr, {ve_id, "o1"});
      }

      auto having_bool_ep = compile_having_predicate(
          *having, pipeline_output, scope, builder, having_cache);

      auto demux_id = builder.next_id("demux");
      builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                           {{"portType", "vector_number"}});
      builder.connect(having_bool_ep, {demux_id, "c1"});
      builder.connect(pipeline_output, {demux_id, "i1"});

      pipeline_output = {demux_id, "o1"};
    }

    // Build field map: no key column, just aggregate aliases
    FieldMap field_map;
    for (size_t i = 0; i < field_names.size(); ++i) {
      field_map[field_names[i]] = static_cast<int>(i);
    }

    return {pipeline_output, field_map, /*is_segment_only=*/true};
  }

  // --- Step 1: Identify key column(s) (persistent keys only) ---

  // --- Composite GROUP BY (2+ keys) ---
  if (group_by.size() > 1) {
    if (num_input_cols <= 0) {
      throw std::runtime_error(
          "composite GROUP BY requires stream column count (internal error)");
    }

    // Resolve all key columns
    struct KeyInfo { int index; std::string name; };
    std::vector<KeyInfo> keys;
    for (const auto& key_expr : group_by) {
      auto* kc = std::get_if<ColumnRef>(&key_expr);
      if (!kc) throw std::runtime_error("GROUP BY expression must be a column reference");
      auto res = scope.resolve(*kc);
      if (auto* err = std::get_if<std::string>(&res)) throw std::runtime_error(*err);
      auto& b = std::get<analyzer::ColumnBinding>(res);
      keys.push_back({b.index, kc->column_name});
    }

    // In outer graph: extract each original column from input, compute hash,
    // and compose augmented vector [col0, col1, ..., colN-1, hash].
    static const double PRIME = 1000003.0;

    std::vector<std::string> extract_ids;
    extract_ids.reserve(num_input_cols);
    for (int c = 0; c < num_input_cols; ++c) {
      auto eid = builder.next_id("extract");
      builder.add_operator(eid, "VectorExtract", {{"index", static_cast<double>(c)}});
      builder.connect(input_endpoint, {eid, "i1"});
      extract_ids.push_back(eid);
    }

    // Compute hash = PRIME * key0 + key1 (+ ... for more keys via chaining)
    std::string hash_ep_id = extract_ids[keys[0].index];
    for (size_t ki = 1; ki < keys.size(); ++ki) {
      auto lin_id = builder.next_id("linear");
      builder.add_operator(lin_id, "Linear", {},
                           {}, {{"coefficients", {PRIME, 1.0}}});
      builder.connect({hash_ep_id, "o1"}, {lin_id, "i1"});
      builder.connect({extract_ids[keys[ki].index], "o1"}, {lin_id, "i2"});
      hash_ep_id = lin_id;
    }

    // Compose augmented vector: [original cols..., hash]
    int compose_n = num_input_cols + 1;
    auto compose_id = builder.next_id("augment");
    builder.add_operator(compose_id, "VectorCompose",
                         {{"numPorts", static_cast<double>(compose_n)}});
    for (int c = 0; c < num_input_cols; ++c) {
      builder.connect({extract_ids[c], "o1"}, {compose_id, "i" + std::to_string(c + 1)});
    }
    builder.connect({hash_ep_id, "o1"}, {compose_id, "i" + std::to_string(compose_n)});
    int hash_key_index = num_input_cols;  // last index in augmented vector

    // Prototype: receives augmented vector, original indices unchanged
    GraphBuilder proto_builder;
    ExprCache cache;
    proto_builder.add_operator("proto_in", "Input");
    Endpoint proto_input_ep{"proto_in", "o1"};

    // Include both key columns explicitly in the prototype output
    std::vector<Endpoint> proto_endpoints;
    std::vector<std::string> field_names;
    for (const auto& ki : keys) {
      auto ve_id = proto_builder.next_id("extract");
      proto_builder.add_operator(ve_id, "VectorExtract",
                                 {{"index", static_cast<double>(ki.index)}});
      proto_builder.connect(proto_input_ep, {ve_id, "i1"});
      proto_endpoints.push_back({ve_id, "o1"});
      field_names.push_back(ki.name);
    }

    // Compile non-key SELECT items
    for (const auto& item : select_list) {
      bool is_key = false;
      for (const auto& gbe : group_by) {
        if (is_group_by_key(item, gbe, scope)) { is_key = true; break; }
      }
      if (is_key) continue;

      auto result = compile_expression_cached(item.expr, proto_input_ep, scope,
                                              proto_builder, cache);
      auto ep = ensure_endpoint(std::move(result), proto_input_ep, proto_builder);
      proto_endpoints.push_back(ep);
      field_names.push_back(item.alias.value_or(default_alias(item.expr)));
    }

    // Compose prototype outputs
    auto pcompose_id = proto_builder.next_id("compose");
    proto_builder.add_operator(
        pcompose_id, "VectorCompose",
        {{"numPorts", static_cast<double>(proto_endpoints.size())}});
    for (size_t i = 0; i < proto_endpoints.size(); ++i) {
      proto_builder.connect(proto_endpoints[i],
                            {pcompose_id, "i" + std::to_string(i + 1)});
    }
    Endpoint proto_output_ep = {pcompose_id, "o1"};

    // HAVING (not supported for composite GROUP BY in this phase)
    if (having.has_value()) {
      throw std::runtime_error(
          "HAVING with composite GROUP BY not yet supported");
    }

    proto_builder.add_operator("proto_out", "Output");
    proto_builder.connect(proto_output_ep, {"proto_out", "i1"});

    auto proto_id = builder.next_id("proto");
    PrototypeDef proto_def;
    proto_def.id = proto_id;
    proto_def.entry_id = "proto_in";
    proto_def.output_id = "proto_out";
    proto_def.operators = proto_builder.operators();
    proto_def.connections = proto_builder.connections();
    builder.add_prototype(proto_def);

    auto keyed_id = builder.next_id("keyed");
    builder.add_operator(keyed_id, "KeyedPipeline",
                         {{"key_index", static_cast<double>(hash_key_index)}},
                         {{"prototype", proto_id}});
    builder.connect({compose_id, "o1"}, {keyed_id, "i1"});

    // KeyedPipeline prepends an internal hash key at index 0, shifting all
    // prototype outputs by 1.  Add a VectorProject to strip the hash key
    // so that downstream consumers (and the view's public field_map)
    // see a clean, 0-based vector: [key0, key1, agg0, ...].
    std::vector<int> proj_indices;
    proj_indices.reserve(field_names.size());
    for (size_t i = 0; i < field_names.size(); ++i) {
      proj_indices.push_back(static_cast<int>(i + 1));  // skip hash at 0
    }
    auto proj_id = builder.next_id("proj");
    builder.add_operator(proj_id, "VectorProject", {}, {}, {},
                         {{"indices", proj_indices}});
    builder.connect({keyed_id, "o1"}, {proj_id, "i1"});

    FieldMap field_map;
    for (size_t i = 0; i < field_names.size(); ++i) {
      field_map[field_names[i]] = static_cast<int>(i);
    }

    return {{proj_id, "o1"}, field_map};
  }

  // --- Single-key GROUP BY ---
  auto* key_col = std::get_if<ColumnRef>(&group_by[0]);
  if (!key_col) {
    throw std::runtime_error("GROUP BY expression must be a column reference");
  }

  auto key_result = scope.resolve(*key_col);
  if (auto* err = std::get_if<std::string>(&key_result)) {
    throw std::runtime_error(*err);
  }
  auto& key_binding = std::get<analyzer::ColumnBinding>(key_result);
  int key_index = key_binding.index;
  std::string key_name = key_col->column_name;

  // --- Step 2: Build prototype sub-graph ---
  GraphBuilder proto_builder;
  ExprCache cache;

  proto_builder.add_operator("proto_in", "Input");
  Endpoint proto_input_ep{"proto_in", "o1"};

  // Compile each non-key SELECT item inside the prototype
  std::vector<Endpoint> proto_endpoints;
  std::vector<std::string> field_names;
  field_names.push_back(key_name);  // key at index 0

  // --- Attempt fused aggregate path ---
  // Try to compile all non-key SELECT items into a single FusedExpression.
  // If successful, this replaces the operator chain + VectorCompose with a
  // single bytecode interpreter, eliminating ~40 virtual dispatches per message.
  if (std::getenv("RTBOT_DISABLE_FUSION") == nullptr) {
    std::map<std::pair<std::string, int>, int> column_to_input;
    std::vector<double> constants;
    std::vector<double> all_bytecode;
    AggBytecodeContext agg_ctx;
    bool all_fusable = true;
    size_t num_agg_outputs = 0;
    std::vector<std::string> fused_field_names;

    for (const auto& item : select_list) {
      if (is_group_by_key(item, group_by[0], scope)) continue;
      auto bc = compile_aggregate_expression_to_bytecode(
          item.expr, scope, column_to_input, constants, agg_ctx);
      if (!bc.success) { all_fusable = false; break; }
      all_bytecode.insert(all_bytecode.end(), bc.bytecode.begin(), bc.bytecode.end());
      fused_field_names.push_back(
          item.alias.value_or(default_alias(item.expr)));
      ++num_agg_outputs;
    }

    if (all_fusable && num_agg_outputs > 0) {
      // Emit VectorExtract per input column in the prototype
      size_t num_inputs = column_to_input.size();
      std::vector<Endpoint> extract_endpoints(num_inputs);
      for (const auto& [col_key, input_idx] : column_to_input) {
        const auto& [stream_name, col_index] = col_key;
        auto ext_id = proto_builder.next_id("ext");
        proto_builder.add_operator(ext_id, "VectorExtract",
                                   {{"index", static_cast<double>(col_index)}});
        proto_builder.connect(proto_input_ep, {ext_id, "i1"});
        extract_endpoints[input_idx] = {ext_id, "o1"};
      }

      // Emit FusedExpression
      auto fused_id = proto_builder.next_id("fused");
      std::map<std::string, std::vector<double>> double_array_params = {
          {"bytecode", all_bytecode}, {"constants", constants}};
      if (!agg_ctx.state_init.empty()) {
        double_array_params["stateInit"] = agg_ctx.state_init;
      }
      proto_builder.add_operator(
          fused_id, "FusedExpression",
          {{"numPorts", static_cast<double>(num_inputs)},
           {"numOutputs", static_cast<double>(num_agg_outputs)}},
          {},  // no string params
          double_array_params);

      for (size_t i = 0; i < num_inputs; ++i) {
        proto_builder.connect(extract_endpoints[i],
                              {fused_id, "i" + std::to_string(i + 1)});
      }

      // FusedExpression outputs VectorNumber directly — use as prototype output
      Endpoint proto_output_ep = {fused_id, "o1"};

      // Merge field names
      for (auto& fn : fused_field_names) {
        field_names.push_back(std::move(fn));
      }

      // --- HAVING (same logic as unfused path) ---
      std::optional<VelocityPattern> velocity_pat;
      if (having.has_value()) {
        velocity_pat = detect_velocity_pattern(*having);
        if (!velocity_pat.has_value()) {
          auto bool_ep = compile_having_predicate(*having, proto_input_ep, scope,
                                                  proto_builder, cache);
          auto demux_id = proto_builder.next_id("demux");
          proto_builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                                     {{"portType", "vector_number"}});
          proto_builder.connect(bool_ep, {demux_id, "c1"});
          proto_builder.connect(proto_output_ep, {demux_id, "i1"});
          proto_output_ep = {demux_id, "o1"};
        }
      }

      // Finalize prototype
      proto_builder.add_operator("proto_out", "Output");
      proto_builder.connect(proto_output_ep, {"proto_out", "i1"});

      auto proto_id = builder.next_id("proto");
      PrototypeDef proto_def;
      proto_def.id = proto_id;
      proto_def.entry_id = "proto_in";
      proto_def.output_id = "proto_out";
      proto_def.operators = proto_builder.operators();
      proto_def.connections = proto_builder.connections();
      builder.add_prototype(proto_def);

      // Add KeyedPipeline (with optional velocity pre-filter)
      Endpoint keyed_input = input_endpoint;
      if (velocity_pat.has_value()) {
        const auto& vp = *velocity_pat;
        auto extract_id = builder.next_id("extract");
        builder.add_operator(extract_id, "VectorExtract",
                             {{"index", static_cast<double>(key_index)}});
        builder.connect(input_endpoint, {extract_id, "i1"});
        auto mkc_id = builder.next_id("mkc");
        builder.add_operator(mkc_id, "MovingKeyCount",
                             {{"window_size", static_cast<double>(vp.window_size)}});
        builder.connect({extract_id, "o1"}, {mkc_id, "i1"});
        auto cmp_id = builder.next_id("cmp");
        builder.add_operator(cmp_id, vp.rtbot_type, {{"value", vp.threshold}});
        builder.connect({mkc_id, "o1"}, {cmp_id, "i1"});
        auto demux_id = builder.next_id("demux");
        builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                             {{"portType", "vector_number"}});
        builder.connect({cmp_id, "o1"}, {demux_id, "c1"});
        builder.connect(input_endpoint, {demux_id, "i1"});
        keyed_input = {demux_id, "o1"};
      }

      auto keyed_id = builder.next_id("keyed");
      builder.add_operator(keyed_id, "KeyedPipeline",
                           {{"key_index", static_cast<double>(key_index)}},
                           {{"prototype", proto_id}});
      builder.connect(keyed_input, {keyed_id, "i1"});

      FieldMap field_map;
      for (size_t i = 0; i < field_names.size(); ++i) {
        field_map[field_names[i]] = static_cast<int>(i);
      }
      return {{keyed_id, "o1"}, field_map};
    }
  }
  // --- End fused aggregate path (fallback below) ---

  for (const auto& item : select_list) {
    if (is_group_by_key(item, group_by[0], scope)) {
      continue;
    }

    auto result = compile_expression_cached(item.expr, proto_input_ep, scope,
                                            proto_builder, cache);
    auto ep =
        ensure_endpoint(std::move(result), proto_input_ep, proto_builder);
    proto_endpoints.push_back(ep);

    std::string alias = item.alias.value_or(default_alias(item.expr));
    field_names.push_back(alias);
  }

  // Compose prototype outputs into a vector (even for a single item,
  // because proto_out expects vector_number port type)
  auto compose_id = proto_builder.next_id("compose");
  proto_builder.add_operator(
      compose_id, "VectorCompose",
      {{"numPorts", static_cast<double>(proto_endpoints.size())}});
  for (size_t i = 0; i < proto_endpoints.size(); ++i) {
    proto_builder.connect(proto_endpoints[i],
                          {compose_id, "i" + std::to_string(i + 1)});
  }
  Endpoint proto_output_ep = {compose_id, "o1"};

  // --- Step 3: HAVING (if present) ---
  // Velocity patterns (MOVING_COUNT(N) OP threshold) are handled as an outer
  // pre-filter before KeyedPipeline, not inside the prototype.
  std::optional<VelocityPattern> velocity_pat;
  if (having.has_value()) {
    velocity_pat = detect_velocity_pattern(*having);
    if (!velocity_pat.has_value()) {
      // General HAVING: compile predicate inside prototype and gate output.
      auto bool_ep = compile_having_predicate(*having, proto_input_ep, scope,
                                              proto_builder, cache);
      auto demux_id = proto_builder.next_id("demux");
      proto_builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                                 {{"portType", "vector_number"}});
      proto_builder.connect(bool_ep, {demux_id, "c1"});
      proto_builder.connect(proto_output_ep, {demux_id, "i1"});
      proto_output_ep = {demux_id, "o1"};
    }
  }

  // --- Step 4: Add Output to prototype ---
  proto_builder.add_operator("proto_out", "Output");
  proto_builder.connect(proto_output_ep, {"proto_out", "i1"});

  // --- Step 5: Wrap as PrototypeDef ---
  auto proto_id = builder.next_id("proto");
  PrototypeDef proto_def;
  proto_def.id = proto_id;
  proto_def.entry_id = "proto_in";
  proto_def.output_id = "proto_out";
  proto_def.operators = proto_builder.operators();
  proto_def.connections = proto_builder.connections();

  builder.add_prototype(proto_def);

  // --- Step 6: Add KeyedPipeline to outer graph ---
  // For velocity patterns, insert a pre-filter chain in the outer graph:
  //   VectorExtract(key_index) → MovingKeyCount(N) → Compare(threshold)
  //     → Demux.c1 ; input_endpoint → Demux.i1 ; Demux.o1 → KeyedPipeline
  Endpoint keyed_input = input_endpoint;
  if (velocity_pat.has_value()) {
    const auto& vp = *velocity_pat;

    auto extract_id = builder.next_id("extract");
    builder.add_operator(extract_id, "VectorExtract",
                         {{"index", static_cast<double>(key_index)}});
    builder.connect(input_endpoint, {extract_id, "i1"});

    auto mkc_id = builder.next_id("mkc");
    builder.add_operator(mkc_id, "MovingKeyCount",
                         {{"window_size", static_cast<double>(vp.window_size)}});
    builder.connect({extract_id, "o1"}, {mkc_id, "i1"});

    auto cmp_id = builder.next_id("cmp");
    builder.add_operator(cmp_id, vp.rtbot_type, {{"value", vp.threshold}});
    builder.connect({mkc_id, "o1"}, {cmp_id, "i1"});

    auto demux_id = builder.next_id("demux");
    builder.add_operator(demux_id, "Demultiplexer", {{"numPorts", 1}},
                         {{"portType", "vector_number"}});
    builder.connect({cmp_id, "o1"}, {demux_id, "c1"});
    builder.connect(input_endpoint, {demux_id, "i1"});

    keyed_input = {demux_id, "o1"};
  }

  auto keyed_id = builder.next_id("keyed");
  builder.add_operator(keyed_id, "KeyedPipeline",
                       {{"key_index", static_cast<double>(key_index)}},
                       {{"prototype", proto_id}});
  builder.connect(keyed_input, {keyed_id, "i1"});

  // --- Step 7: Build field map ---
  FieldMap field_map;
  for (size_t i = 0; i < field_names.size(); ++i) {
    field_map[field_names[i]] = static_cast<int>(i);
  }

  return {{keyed_id, "o1"}, field_map};
}

}  // namespace rtbot_sql::compiler
