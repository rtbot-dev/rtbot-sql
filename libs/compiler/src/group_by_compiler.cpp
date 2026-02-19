#include "rtbot_sql/compiler/group_by_compiler.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include "rtbot_sql/compiler/expr_cache.h"
#include "rtbot_sql/compiler/expression_compiler.h"

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

SelectResult compile_group_by(
    const std::vector<parser::ast::SelectItem>& select_list,
    const std::vector<parser::ast::Expr>& group_by,
    const std::optional<parser::ast::Expr>& having,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    int num_input_cols) {
  using namespace parser::ast;

  // --- Step 1: Identify key column(s) ---
  if (group_by.empty()) {
    throw std::runtime_error("GROUP BY requires at least one column");
  }

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

    // Field map: key columns first (KeyedPipeline prepends its key, but since
    // we include keys explicitly in the prototype, keys appear at their natural
    // positions in the prototype output. KeyedPipeline prepends its hash key at
    // index 0, shifting all prototype outputs by 1.
    // field_map: hash_key=0, key0=1, key1=2, agg0=3, ...
    FieldMap field_map;
    for (size_t i = 0; i < field_names.size(); ++i) {
      field_map[field_names[i]] = static_cast<int>(i + 1);  // +1 for hash key prepended
    }

    return {{keyed_id, "o1"}, field_map};
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
