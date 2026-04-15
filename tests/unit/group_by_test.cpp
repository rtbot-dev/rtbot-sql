#include "rtbot_sql/compiler/group_by_compiler.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace rtbot_sql::compiler {
namespace {

using namespace parser::ast;

Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr num(double v) { return Constant{v}; }

Expr func_expr(const std::string& name, std::vector<Expr> args) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args = std::move(args);
  return f;
}

Expr binary_expr(const std::string& op, Expr left, Expr right) {
  auto e = std::make_unique<BinaryExpr>();
  e->op = op;
  e->left = std::move(left);
  e->right = std::move(right);
  return e;
}

SelectItem item(Expr expr, std::optional<std::string> alias = std::nullopt) {
  return {std::move(expr), alias};
}

class GroupByTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "trades",
        {{"instrument_id", 0}, {"price", 1}, {"quantity", 2},
         {"account_id", 3}},
    };
    scope.register_stream("trades", schema);
  }

  analyzer::Scope scope;
  GraphBuilder builder;
  Endpoint input{"input_0", "o1"};
};

// Test 1: SELECT instrument_id, SUM(quantity) FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, BasicGroupByWithSum) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "sum_quantity"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  EXPECT_FALSE(is_seg);

  // Outer graph: KeyedPipeline
  ASSERT_EQ(builder.operators().size(), 1u);
  auto& keyed = builder.operators()[0];
  EXPECT_EQ(keyed.type, "KeyedPipeline");
  EXPECT_EQ(keyed.params.at("key_index"), 0.0);
  EXPECT_EQ(keyed.string_params.at("prototype"), builder.prototypes()[0].id);

  // One prototype
  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];
  EXPECT_EQ(proto.entry_id, "proto_in");
  EXPECT_EQ(proto.output_id, "proto_out");

  // With fusion enabled the prototype contains:
  //   Input, FusedExpressionVector (with CUMSUM opcode), Output
  // Without fusion (RTBOT_DISABLE_FUSION=1) the prototype contains:
  //   Input, VectorExtract, CumulativeSum, VectorCompose, Output
  bool has_input = false, has_ext = false, has_output = false;
  bool has_fused_vector = false, has_fused_scalar = false;
  bool has_cumsum = false, has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "Input") has_input = true;
    if (op.type == "VectorExtract") {
      has_ext = true;
      EXPECT_EQ(op.params.at("index"), 2.0);  // quantity
    }
    if (op.type == "FusedExpressionVector") {
      has_fused_vector = true;
      EXPECT_EQ(op.params.at("numOutputs"), 1.0);
    }
    if (op.type == "FusedExpression") has_fused_scalar = true;
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "Output") has_output = true;
    if (op.type == "VectorCompose") has_compose = true;
  }
  EXPECT_TRUE(has_input);
  EXPECT_TRUE(has_output);
  // Either fused path or unfused path, but not both
  EXPECT_TRUE(has_fused_vector || (has_cumsum && has_compose));
  EXPECT_FALSE(has_fused_scalar)
      << "Single-key fused path should use FusedExpressionVector";
  if (has_fused_vector) {
    EXPECT_FALSE(has_ext)
        << "FusedExpressionVector path should not emit VectorExtract";
  } else {
    EXPECT_TRUE(has_ext)
        << "Fallback path should include VectorExtract";
  }

  // Field map
  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("sum_quantity"), 1);
}

// Test 2: SELECT instrument_id, SUM(quantity), COUNT(*), AVG(price)
//         FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, MultiAggregateGroupBy) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));

  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(
      item(func_expr("SUM", std::move(sum_args)), "total_qty"));

  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  std::vector<Expr> avg_args;
  avg_args.push_back(col("price"));
  select_list.push_back(
      item(func_expr("AVG", std::move(avg_args)), "avg_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map, is_seg2] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // With fusion: FusedExpressionVector with 3 outputs (SUM, COUNT, AVG)
  // Without fusion: VectorCompose with 3 ports
  bool has_compose = false, has_fused_vector = false, has_fused_scalar = false;
  for (const auto& op : proto.operators) {
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 3.0);
    }
    if (op.type == "FusedExpressionVector") {
      has_fused_vector = true;
      EXPECT_EQ(op.params.at("numOutputs"), 3.0);
    }
    if (op.type == "FusedExpression") has_fused_scalar = true;
  }
  EXPECT_TRUE(has_compose || has_fused_vector);
  EXPECT_FALSE(has_fused_scalar)
      << "Single-key fused path should use FusedExpressionVector";

  // Field map: 4 entries
  EXPECT_EQ(field_map.size(), 4u);
  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("total_qty"), 1);
  EXPECT_EQ(field_map.at("cnt"), 2);
  EXPECT_EQ(field_map.at("avg_price"), 3);
}

// Test 3: SELECT instrument_id, price, MOVING_AVERAGE(price, 20)
//         FROM trades GROUP BY instrument_id
TEST_F(GroupByTest, NonAggregatedColumnPassthrough) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("price")));

  std::vector<Expr> ma_args;
  ma_args.push_back(col("price"));
  ma_args.push_back(num(20));
  select_list.push_back(
      item(func_expr("MOVING_AVERAGE", std::move(ma_args)), "ma_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map, is_seg3] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  // Should have MovingAverage
  bool has_ma = false;
  for (const auto& op : proto.operators) {
    if (op.type == "MovingAverage") has_ma = true;
  }
  EXPECT_TRUE(has_ma);

  // VectorCompose with 2 ports (price passthrough + MA)
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);
    }
  }
  EXPECT_TRUE(has_compose);

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("price"), 1);
  EXPECT_EQ(field_map.at("ma_price"), 2);
}

// ---------------------------------------------------------------------------
// Test 4: Composite GROUP BY (2 keys) produces Linear hash + KeyedPipeline
// SELECT instrument_id, exchange_id, SUM(quantity) AS total
// FROM trades GROUP BY instrument_id, exchange_id
// ---------------------------------------------------------------------------

TEST_F(GroupByTest, CompositeGroupByTwoKeys) {
  // Use schema with 4 columns: instrument_id(0), price(1), quantity(2), account_id(3)
  // Re-purpose account_id as exchange_id for this test.
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));
  select_list.push_back(item(col("account_id")));  // treated as exchange_id key
  std::vector<Expr> sum_args;
  sum_args.push_back(col("quantity"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));
  group_by.push_back(col("account_id"));

  auto [ep, field_map, is_seg4] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder, 4 /*num_input_cols*/);

  // Outer graph must have: KeyedPipeline with computed key (keyColumnIndices).
  // No Linear, VectorCompose, or VectorProject needed.
  bool has_keyed = false;
  for (const auto& op : builder.operators()) {
    EXPECT_NE(op.type, "Linear")
        << "Linear should NOT be in outer graph (computed key mode)";
    EXPECT_NE(op.type, "VectorCompose")
        << "VectorCompose should NOT be in outer graph (computed key mode)";
    EXPECT_NE(op.type, "VectorProject")
        << "VectorProject should NOT be in outer graph (computed key mode)";
    if (op.type == "KeyedPipeline") {
      has_keyed = true;
      // Computed key mode: no key_index, has keyColumnIndices (no keyCoefficients — computed internally)
      EXPECT_EQ(op.params.count("key_index"), 0u)
          << "Computed key mode should not have key_index";
      ASSERT_TRUE(op.int_array_params.count("keyColumnIndices"))
          << "KeyedPipeline should have keyColumnIndices";
      EXPECT_EQ(op.double_array_params.count("keyCoefficients"), 0u)
          << "keyCoefficients should not be in compiler output (computed internally)";
      // instrument_id=0, account_id=3
      const auto& indices = op.int_array_params.at("keyColumnIndices");
      ASSERT_EQ(indices.size(), 2u);
      EXPECT_EQ(indices[0], 0);  // instrument_id
      EXPECT_EQ(indices[1], 3);  // account_id
    }
  }
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";

  // One prototype wrapping the per-key aggregation.
  ASSERT_EQ(builder.prototypes().size(), 1u);

  // Field map: computed key mode outputs directly (no VectorProject), 0-based.
  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("account_id"), 1);
  EXPECT_EQ(field_map.at("total"), 2);
}

// ---------------------------------------------------------------------------
// Test 5: MOVING_MIN(price, 10) and MOVING_MAX(price, 5) inside GROUP BY
// ---------------------------------------------------------------------------

TEST_F(GroupByTest, MovingMinMaxInsideGroupBy) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("instrument_id")));

  std::vector<Expr> min_args;
  min_args.push_back(col("price"));
  min_args.push_back(num(10));
  select_list.push_back(
      item(func_expr("MOVING_MIN", std::move(min_args)), "min_price"));

  std::vector<Expr> max_args;
  max_args.push_back(col("price"));
  max_args.push_back(num(5));
  select_list.push_back(
      item(func_expr("MOVING_MAX", std::move(max_args)), "max_price"));

  std::vector<Expr> group_by;
  group_by.push_back(col("instrument_id"));

  auto [ep, field_map, is_seg5] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder);

  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];

  bool has_wmin = false;
  bool has_wmax = false;
  for (const auto& op : proto.operators) {
    if (op.type == "WindowMinMax") {
      if (op.string_params.count("mode") &&
          op.string_params.at("mode") == "min") {
        has_wmin = true;
        EXPECT_EQ(op.params.at("window_size"), 10.0);
      }
      if (op.string_params.count("mode") &&
          op.string_params.at("mode") == "max") {
        has_wmax = true;
        EXPECT_EQ(op.params.at("window_size"), 5.0);
      }
    }
  }
  EXPECT_TRUE(has_wmin) << "Expected WindowMinMax(min) in prototype";
  EXPECT_TRUE(has_wmax) << "Expected WindowMinMax(max) in prototype";

  EXPECT_EQ(field_map.at("instrument_id"), 0);
  EXPECT_EQ(field_map.at("min_price"), 1);
  EXPECT_EQ(field_map.at("max_price"), 2);
}

// ---------------------------------------------------------------------------
// Test 6: Pipeline operator in graph builder — port types, validation, JSON
// ---------------------------------------------------------------------------

TEST(PipelineGraphBuilderTest, PipelineValidatesAndSerializes) {
  GraphBuilder builder;

  // Build a minimal graph:  Input -> Pipeline -> Output
  // Pipeline takes VECTOR_NUMBER data in i1, NUMBER control in c1,
  // and outputs VECTOR_NUMBER on o1.
  builder.add_operator("input_0", "Input");
  builder.add_operator("pipeline_0", "Pipeline",
                       /*params=*/{},
                       /*string_params=*/{{"prototype", "proto_0"}});
  builder.add_operator("output_0", "Output");

  // Add a prototype for the Pipeline to reference
  PrototypeDef proto;
  proto.id = "proto_0";
  proto.entry_id = "proto_in";
  proto.output_id = "proto_out";
  proto.operators.push_back({"proto_in", "Input", {}, {}, {}, {}});
  proto.operators.push_back(
      {"extract_0", "VectorExtract", {{"index", 0.0}}, {}, {}, {}});
  proto.operators.push_back(
      {"cumsum_0", "CumulativeSum", {}, {}, {}, {}});
  proto.operators.push_back(
      {"compose_0", "VectorCompose", {{"numPorts", 1.0}}, {}, {}, {}});
  proto.operators.push_back({"proto_out", "Output", {}, {}, {}, {}});
  proto.connections.push_back({"proto_in", "o1", "extract_0", "i1"});
  proto.connections.push_back({"extract_0", "o1", "cumsum_0", "i1"});
  proto.connections.push_back({"cumsum_0", "o1", "compose_0", "i1"});
  proto.connections.push_back({"compose_0", "o1", "proto_out", "i1"});
  builder.add_prototype(proto);

  // Connect: Input -> Pipeline (data), Pipeline -> Output
  builder.connect({"input_0", "o1"}, {"pipeline_0", "i1"});
  builder.connect({"pipeline_0", "o1"}, {"output_0", "i1"});

  // Validation should pass (no errors)
  auto errors = builder.validate();
  EXPECT_TRUE(errors.empty())
      << "Unexpected validation errors: " << errors[0];

  // Serialize to JSON
  std::string json_str = builder.to_json();

  // The JSON should contain native Pipeline format (not prototype)
  auto j = nlohmann::json::parse(json_str);
  bool found_pipeline = false;
  for (const auto& op : j["operators"]) {
    if (op["type"] == "Pipeline") {
      found_pipeline = true;
      // Pipeline should have native fields, NOT a "prototype" object
      EXPECT_FALSE(op.contains("prototype"))
          << "Pipeline should NOT have prototype field in native format";
      EXPECT_TRUE(op.contains("input_port_types"))
          << "Pipeline should have input_port_types";
      EXPECT_TRUE(op.contains("output_port_types"))
          << "Pipeline should have output_port_types";
      EXPECT_TRUE(op.contains("operators"))
          << "Pipeline should have operators array";
      EXPECT_TRUE(op.contains("connections"))
          << "Pipeline should have connections array";
      EXPECT_TRUE(op.contains("entryOperator"))
          << "Pipeline should have entryOperator";
      EXPECT_TRUE(op.contains("outputMappings"))
          << "Pipeline should have outputMappings";
    }
  }
  EXPECT_TRUE(found_pipeline) << "Pipeline operator not found in JSON";

  // Round-trip: deserialize and re-serialize
  auto [builder2, pre_output] =
      GraphBuilder::from_json_for_augmentation(json_str);
  // builder2 should have Pipeline with a prototype string_param
  bool found_pipeline_op = false;
  for (const auto& op : builder2.operators()) {
    if (op.type == "Pipeline") {
      found_pipeline_op = true;
      EXPECT_TRUE(op.string_params.count("prototype"))
          << "Pipeline should have prototype string_param after deserialization";
    }
  }
  EXPECT_TRUE(found_pipeline_op)
      << "Pipeline operator not found after deserialization";
  EXPECT_EQ(builder2.prototypes().size(), 1u)
      << "Prototype should be deserialized";
}

// Test that Pipeline without prototype param fails validation
TEST(PipelineGraphBuilderTest, PipelineMissingPrototypeFails) {
  GraphBuilder builder;
  builder.add_operator("input_0", "Input");
  builder.add_operator("pipeline_0", "Pipeline");  // no prototype!
  builder.add_operator("output_0", "Output");
  builder.connect({"input_0", "o1"}, {"pipeline_0", "i1"});
  builder.connect({"pipeline_0", "o1"}, {"output_0", "i1"});

  auto errors = builder.validate();
  bool has_prototype_error = false;
  for (const auto& e : errors) {
    if (e.find("prototype") != std::string::npos) has_prototype_error = true;
  }
  EXPECT_TRUE(has_prototype_error)
      << "Should report missing prototype for Pipeline";
}

// Test that Pipeline control port accepts NUMBER (segment key)
TEST(PipelineGraphBuilderTest, PipelineControlPortAcceptsNumber) {
  GraphBuilder builder;
  builder.add_operator("input_0", "Input");
  builder.add_operator("pipeline_0", "Pipeline",
                       /*params=*/{},
                       /*string_params=*/{{"prototype", "proto_0"}});
  builder.add_operator("extract_0", "VectorExtract",
                       /*params=*/{{"index", 0.0}});
  builder.add_operator("output_0", "Output");

  PrototypeDef proto;
  proto.id = "proto_0";
  proto.entry_id = "proto_in";
  proto.output_id = "proto_out";
  proto.operators.push_back({"proto_in", "Input", {}, {}, {}, {}});
  proto.operators.push_back({"proto_out", "Output", {}, {}, {}, {}});
  proto.connections.push_back({"proto_in", "o1", "proto_out", "i1"});
  builder.add_prototype(proto);

  // Input(VECTOR_NUMBER) -> VectorExtract(NUMBER) -> Pipeline c1
  // Input(VECTOR_NUMBER) -> Pipeline i1 (data)
  builder.connect({"input_0", "o1"}, {"pipeline_0", "i1"});
  builder.connect({"input_0", "o1"}, {"extract_0", "i1"});
  // VectorExtract outputs NUMBER — Pipeline c1 expects NUMBER (segment key)
  builder.connect({"extract_0", "o1"}, {"pipeline_0", "c1"});
  builder.connect({"pipeline_0", "o1"}, {"output_0", "i1"});

  auto errors = builder.validate();
  // Should NOT have a type mismatch — Pipeline c1 accepts NUMBER
  for (const auto& e : errors) {
    EXPECT_TRUE(e.find("type mismatch") == std::string::npos)
        << "Unexpected type mismatch: " << e;
  }
}

// ---------------------------------------------------------------------------
// Pipeline control port: CompareGT (BOOLEAN) -> Pipeline.c1 should be
// rejected because Pipeline c1 expects NUMBER (segment key), not BOOLEAN.
// The compiler inserts a BooleanToNumber conversion for this case.
// ---------------------------------------------------------------------------
TEST(PipelineGraphBuilderTest, PipelineControlPortRejectsBooleanFromCompare) {
  GraphBuilder builder;
  builder.add_operator("input_0", "Input");
  builder.add_operator("extract_0", "VectorExtract", {{"index", 0.0}});
  builder.add_operator("cmp_0", "CompareGT", {{"value", 0.0}});
  builder.add_operator("pipeline_0", "Pipeline",
                       /*params=*/{},
                       /*string_params=*/{{"prototype", "proto_0"}});
  builder.add_operator("output_0", "Output");

  PrototypeDef proto;
  proto.id = "proto_0";
  proto.entry_id = "proto_in";
  proto.output_id = "proto_out";
  proto.operators.push_back({"proto_in", "Input", {}, {}, {}, {}});
  proto.operators.push_back({"proto_out", "Output", {}, {}, {}, {}});
  proto.connections.push_back({"proto_in", "o1", "proto_out", "i1"});
  builder.add_prototype(proto);

  // Input -> VectorExtract -> CompareGT(BOOLEAN) -> Pipeline.c1
  builder.connect({"input_0", "o1"}, {"extract_0", "i1"});
  builder.connect({"extract_0", "o1"}, {"cmp_0", "i1"});
  builder.connect({"cmp_0", "o1"}, {"pipeline_0", "c1"});
  builder.connect({"input_0", "o1"}, {"pipeline_0", "i1"});
  builder.connect({"pipeline_0", "o1"}, {"output_0", "i1"});

  auto errors = builder.validate();
  // Should HAVE a type mismatch: BOOLEAN -> NUMBER c1
  bool has_mismatch = false;
  for (const auto& e : errors) {
    if (e.find("type mismatch") != std::string::npos) has_mismatch = true;
  }
  EXPECT_TRUE(has_mismatch)
      << "Expected type mismatch: BOOLEAN cannot connect to NUMBER c1";
}

// ---------------------------------------------------------------------------
// BooleanToNumber Graph Builder Type Validation
// ---------------------------------------------------------------------------

// BooleanToNumber: BOOLEAN input -> NUMBER output, validates correctly
TEST(BooleanToNumberGraphBuilderTest, BooleanToNumberTypeValidation) {
  GraphBuilder builder;
  builder.add_operator("input_0", "Input");
  builder.add_operator("extract_0", "VectorExtract", {{"index", 0.0}});
  builder.add_operator("cmp_0", "CompareGT", {{"value", 0.0}});
  builder.add_operator("b2n_0", "BooleanToNumber");
  builder.add_operator("pipeline_0", "Pipeline",
                       /*params=*/{},
                       /*string_params=*/{{"prototype", "proto_0"}});
  builder.add_operator("output_0", "Output");

  PrototypeDef proto;
  proto.id = "proto_0";
  proto.entry_id = "proto_in";
  proto.output_id = "proto_out";
  proto.operators.push_back({"proto_in", "Input", {}, {}, {}, {}});
  proto.operators.push_back({"proto_out", "Output", {}, {}, {}, {}});
  proto.connections.push_back({"proto_in", "o1", "proto_out", "i1"});
  builder.add_prototype(proto);

  // Input -> VectorExtract -> CompareGT(BOOLEAN) -> BooleanToNumber(NUMBER) -> Pipeline.c1
  builder.connect({"input_0", "o1"}, {"extract_0", "i1"});
  builder.connect({"extract_0", "o1"}, {"cmp_0", "i1"});
  builder.connect({"cmp_0", "o1"}, {"b2n_0", "i1"});
  builder.connect({"b2n_0", "o1"}, {"pipeline_0", "c1"});
  builder.connect({"input_0", "o1"}, {"pipeline_0", "i1"});
  builder.connect({"pipeline_0", "o1"}, {"output_0", "i1"});

  auto errors = builder.validate();
  for (const auto& e : errors) {
    EXPECT_TRUE(e.find("type mismatch") == std::string::npos)
        << "Unexpected type mismatch: " << e;
  }
}

// BooleanToNumber rejects NUMBER input (must receive BOOLEAN)
TEST(BooleanToNumberGraphBuilderTest, BooleanToNumberRejectsNumberInput) {
  GraphBuilder builder;
  builder.add_operator("input_0", "Input");
  builder.add_operator("extract_0", "VectorExtract", {{"index", 0.0}});
  builder.add_operator("b2n_0", "BooleanToNumber");
  builder.add_operator("output_0", "Output");

  // VectorExtract outputs NUMBER, BooleanToNumber expects BOOLEAN
  builder.connect({"input_0", "o1"}, {"extract_0", "i1"});
  builder.connect({"extract_0", "o1"}, {"b2n_0", "i1"});
  builder.connect({"input_0", "o1"}, {"output_0", "i1"});

  auto errors = builder.validate();
  bool has_mismatch = false;
  for (const auto& e : errors) {
    if (e.find("type mismatch") != std::string::npos) has_mismatch = true;
  }
  EXPECT_TRUE(has_mismatch)
      << "Expected type mismatch: NUMBER cannot connect to BooleanToNumber (expects BOOLEAN)";
}

// ---------------------------------------------------------------------------
// GROUP BY Classification Tests
// ---------------------------------------------------------------------------

class ClassificationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "vibration",
        {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}},
    };
    scope.register_stream("vibration", schema);
  }

  analyzer::Scope scope;
};

// Helper: create a ComparisonExpr
Expr cmp_expr(Expr left, const std::string& op, Expr right) {
  auto c = std::make_unique<ComparisonExpr>();
  c->op = op;
  c->left = std::move(left);
  c->right = std::move(right);
  return c;
}

// Test: bare ColumnRef → PERSISTENT_KEY
TEST_F(ClassificationTest, ClassifyPersistentKey) {
  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));

  auto cls = classify_group_by(group_by, scope);

  ASSERT_EQ(cls.items.size(), 1u);
  EXPECT_EQ(cls.items[0].kind, GroupByItemKind::PERSISTENT_KEY);
  EXPECT_EQ(cls.items[0].key_index, 0);
  EXPECT_EQ(cls.items[0].key_name, "device_id");
  EXPECT_TRUE(cls.has_persistent_keys());
  EXPECT_FALSE(cls.has_segment_expressions());
  EXPECT_EQ(cls.persistent_key_count(), 1);
  EXPECT_EQ(cls.segment_expression_count(), 0);
}

// Test: comparison expression → SEGMENT_EXPRESSION
TEST_F(ClassificationTest, ClassifySegmentExpression) {
  // GROUP BY ABS(amplitude) > 0
  std::vector<Expr> group_by;
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  auto cls = classify_group_by(group_by, scope);

  ASSERT_EQ(cls.items.size(), 1u);
  EXPECT_EQ(cls.items[0].kind, GroupByItemKind::SEGMENT_EXPRESSION);
  EXPECT_FALSE(cls.has_persistent_keys());
  EXPECT_TRUE(cls.has_segment_expressions());
  EXPECT_EQ(cls.persistent_key_count(), 0);
  EXPECT_EQ(cls.segment_expression_count(), 1);
}

// Test: mixed — 1 persistent + 1 segment
TEST_F(ClassificationTest, ClassifyMixed) {
  // GROUP BY device_id, ABS(amplitude) > 0
  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  auto cls = classify_group_by(group_by, scope);

  ASSERT_EQ(cls.items.size(), 2u);
  EXPECT_EQ(cls.items[0].kind, GroupByItemKind::PERSISTENT_KEY);
  EXPECT_EQ(cls.items[0].key_name, "device_id");
  EXPECT_EQ(cls.items[1].kind, GroupByItemKind::SEGMENT_EXPRESSION);
  EXPECT_TRUE(cls.has_persistent_keys());
  EXPECT_TRUE(cls.has_segment_expressions());
  EXPECT_EQ(cls.persistent_key_count(), 1);
  EXPECT_EQ(cls.segment_expression_count(), 1);
}

// Test: function call on column (not a comparison) → SEGMENT_EXPRESSION
TEST_F(ClassificationTest, ClassifyFunctionCallAsSegmentExpression) {
  // GROUP BY ABS(amplitude) — a FuncCall, not a ColumnRef → SEGMENT_EXPRESSION
  std::vector<Expr> group_by;
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(func_expr("ABS", std::move(abs_args)));

  auto cls = classify_group_by(group_by, scope);

  ASSERT_EQ(cls.items.size(), 1u);
  EXPECT_EQ(cls.items[0].kind, GroupByItemKind::SEGMENT_EXPRESSION);
}

// ---------------------------------------------------------------------------
// Segment-Only GROUP BY → Pipeline Compilation Test
// ---------------------------------------------------------------------------

class SegmentGroupByTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema schema{
        "vibration",
        {{"device_id", 0}, {"amplitude", 1}, {"frequency", 2}},
    };
    scope.register_stream("vibration", schema);
  }

  analyzer::Scope scope;
  GraphBuilder builder;
  Endpoint input{"input_0", "o1"};
};

// SELECT SUM(amplitude) AS total, COUNT(*) AS cnt
// FROM vibration GROUP BY ABS(amplitude) > 0
TEST_F(SegmentGroupByTest, SegmentOnlyGroupBy) {
  // Build SELECT list: SUM(amplitude) AS total, COUNT(*) AS cnt
  std::vector<SelectItem> select_list;
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));
  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  // GROUP BY ABS(amplitude) > 0
  std::vector<Expr> group_by;
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope, builder);

  // Should be segment-only
  EXPECT_TRUE(is_seg);

  // Outer graph should contain Pipeline operator
  bool has_pipeline = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Pipeline") {
      has_pipeline = true;
      EXPECT_TRUE(op.string_params.count("prototype"))
          << "Pipeline should have a prototype string_param";
    }
  }
  EXPECT_TRUE(has_pipeline) << "Expected Pipeline operator in outer graph";

  // Segment expression is compiled to bytecode — no Abs/CompareGT/BooleanToNumber
  // operators in the outer graph. Pipeline carries segmentBytecode instead.
  for (const auto& op : builder.operators()) {
    EXPECT_NE(op.type, "Abs")
        << "Abs should NOT be in outer graph (bytecode path)";
    EXPECT_NE(op.type, "CompareGT")
        << "CompareGT should NOT be in outer graph (bytecode path)";
    EXPECT_NE(op.type, "BooleanToNumber")
        << "BooleanToNumber should NOT be in outer graph (bytecode path)";
  }

  // Pipeline should have segmentBytecode and segmentConstants
  for (const auto& op : builder.operators()) {
    if (op.type == "Pipeline") {
      EXPECT_TRUE(op.double_array_params.count("segmentBytecode"))
          << "Pipeline should have segmentBytecode";
      EXPECT_TRUE(op.double_array_params.count("segmentConstants"))
          << "Pipeline should have segmentConstants";
      EXPECT_FALSE(op.double_array_params.at("segmentBytecode").empty())
          << "segmentBytecode should not be empty";
    }
  }

  // No c1 connections to Pipeline (bytecode replaces control port)
  for (const auto& conn : builder.connections()) {
    if (conn.to_port == "c1") {
      for (const auto& op : builder.operators()) {
        if (op.id == conn.to_id && op.type == "Pipeline") {
          ADD_FAILURE() << "Pipeline should NOT have a c1 connection (bytecode path)";
        }
      }
    }
  }

  // Prototype should contain aggregates: CumulativeSum, CountNumber
  ASSERT_EQ(builder.prototypes().size(), 1u);
  const auto& proto = builder.prototypes()[0];
  bool has_cumsum = false, has_count = false, has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "CountNumber") has_count = true;
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);
    }
  }
  EXPECT_TRUE(has_cumsum) << "Expected CumulativeSum in prototype";
  EXPECT_TRUE(has_count) << "Expected CountNumber in prototype";
  EXPECT_TRUE(has_compose) << "Expected VectorCompose in prototype";

  // Field map: no key column, just aggregates
  EXPECT_EQ(field_map.size(), 2u);
  EXPECT_EQ(field_map.at("total"), 0);
  EXPECT_EQ(field_map.at("cnt"), 1);
}

// ---------------------------------------------------------------------------
// Mixed GROUP BY: persistent key + segment expression
// ---------------------------------------------------------------------------

// SELECT device_id, SUM(amplitude) AS total, COUNT(*) AS cnt
// FROM vibration GROUP BY device_id, ABS(amplitude) > 0
TEST_F(SegmentGroupByTest, MixedGroupBy) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("device_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));
  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope, builder);

  // Not segment-only (has persistent key)
  EXPECT_FALSE(is_seg);

  // Outer graph: KeyedPipeline
  bool has_keyed = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "KeyedPipeline") {
      has_keyed = true;
      EXPECT_EQ(op.params.at("key_index"), 0.0);  // device_id at index 0
    }
  }
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";

  // Should have 2 prototypes: one for KeyedPipeline (outer), one for Pipeline (inner)
  EXPECT_EQ(builder.prototypes().size(), 2u);

  // Find the outer prototype (the one containing a Pipeline operator)
  const PrototypeDef* outer_proto = nullptr;
  const PrototypeDef* inner_proto = nullptr;
  for (const auto& proto : builder.prototypes()) {
    bool has_pipeline = false;
    for (const auto& op : proto.operators) {
      if (op.type == "Pipeline") has_pipeline = true;
    }
    if (has_pipeline) {
      outer_proto = &proto;
    } else {
      inner_proto = &proto;
    }
  }
  ASSERT_NE(outer_proto, nullptr) << "One prototype should contain a Pipeline";
  ASSERT_NE(inner_proto, nullptr) << "One prototype should be the inner aggregate";

  // Outer prototype: segment expression is compiled to bytecode.
  // No Abs/CompareGT/BooleanToNumber operators — Pipeline has segmentBytecode.
  bool proto_has_pipeline = false;
  for (const auto& op : outer_proto->operators) {
    EXPECT_NE(op.type, "Abs")
        << "Abs should NOT be in outer prototype (bytecode path)";
    EXPECT_NE(op.type, "CompareGT")
        << "CompareGT should NOT be in outer prototype (bytecode path)";
    EXPECT_NE(op.type, "BooleanToNumber")
        << "BooleanToNumber should NOT be in outer prototype (bytecode path)";
    if (op.type == "Pipeline") proto_has_pipeline = true;
  }
  EXPECT_TRUE(proto_has_pipeline) << "Outer prototype should have Pipeline";

  // Pipeline in outer prototype should have segmentBytecode/segmentConstants
  for (const auto& op : outer_proto->operators) {
    if (op.type == "Pipeline") {
      EXPECT_TRUE(op.double_array_params.count("segmentBytecode"))
          << "Pipeline should have segmentBytecode";
      EXPECT_TRUE(op.double_array_params.count("segmentConstants"))
          << "Pipeline should have segmentConstants";
      EXPECT_FALSE(op.double_array_params.at("segmentBytecode").empty())
          << "segmentBytecode should not be empty";
    }
  }

  // No c1 connections to Pipeline in outer prototype (bytecode replaces control port)
  for (const auto& conn : outer_proto->connections) {
    if (conn.to_port == "c1") {
      for (const auto& op : outer_proto->operators) {
        if (op.id == conn.to_id && op.type == "Pipeline") {
          ADD_FAILURE() << "Pipeline should NOT have a c1 connection (bytecode path)";
        }
      }
    }
  }

  // Inner prototype: with fusion → FusedExpressionVector; without → CumulativeSum + CountNumber + VectorCompose
  bool has_cumsum = false, has_count = false, has_compose = false;
  bool has_fused_vector = false, has_fused_scalar = false;
  int extract_count = 0;
  for (const auto& op : inner_proto->operators) {
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "CountNumber") has_count = true;
    if (op.type == "VectorExtract") extract_count++;
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);  // total + cnt
    }
    if (op.type == "FusedExpressionVector") {
      has_fused_vector = true;
      EXPECT_EQ(op.params.at("numOutputs"), 2.0);  // total + cnt
    }
    if (op.type == "FusedExpression") has_fused_scalar = true;
  }
  EXPECT_TRUE(has_fused_vector || (has_cumsum && has_count && has_compose))
      << "Inner prototype should have FusedExpressionVector or CumulativeSum+CountNumber+VectorCompose";
  EXPECT_FALSE(has_fused_scalar)
      << "Inner prototype should use FusedExpressionVector for vector input fusion";
  if (has_fused_vector) {
    EXPECT_EQ(extract_count, 0)
        << "FusedExpressionVector path should not emit VectorExtract operators";
  }

  // Field map: device_id=0, total=1, cnt=2
  EXPECT_EQ(field_map.at("device_id"), 0);
  EXPECT_EQ(field_map.at("total"), 1);
  EXPECT_EQ(field_map.at("cnt"), 2);
}

// ---------------------------------------------------------------------------
// Composite persistent keys + segment expression
// GROUP BY device_id, frequency, ABS(amplitude) > 0
// → hash-augmented KeyedPipeline with nested Pipeline
// ---------------------------------------------------------------------------
TEST_F(SegmentGroupByTest, CompositeKeysWithSegmentExpression) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("device_id")));
  select_list.push_back(item(col("frequency")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));
  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  // GROUP BY device_id, frequency, ABS(amplitude) > 0 — 2 persistent + 1 segment
  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));
  group_by.push_back(col("frequency"));
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope, builder,
                       3 /*num_input_cols: device_id(0), amplitude(1), frequency(2)*/);

  // Not segment-only (has persistent keys)
  EXPECT_FALSE(is_seg);

  // Outer graph: should have KeyedPipeline with computed key mode
  // (keyColumnIndices).  No Linear, VectorCompose, or VectorProject.
  bool has_keyed = false;
  for (const auto& op : builder.operators()) {
    EXPECT_NE(op.type, "Linear")
        << "Linear should NOT be in outer graph (computed key mode)";
    EXPECT_NE(op.type, "VectorCompose")
        << "VectorCompose should NOT be in outer graph (computed key mode)";
    EXPECT_NE(op.type, "VectorProject")
        << "VectorProject should NOT be in outer graph (computed key mode)";
    if (op.type == "KeyedPipeline") {
      has_keyed = true;
      // Computed key mode: no key_index, has keyColumnIndices (no keyCoefficients — computed internally)
      EXPECT_EQ(op.params.count("key_index"), 0u)
          << "Computed key mode should not have key_index";
      ASSERT_TRUE(op.int_array_params.count("keyColumnIndices"))
          << "KeyedPipeline should have keyColumnIndices";
      EXPECT_EQ(op.double_array_params.count("keyCoefficients"), 0u)
          << "keyCoefficients should not be in compiler output (computed internally)";
      // device_id=0, frequency=2
      const auto& indices = op.int_array_params.at("keyColumnIndices");
      ASSERT_EQ(indices.size(), 2u);
      EXPECT_EQ(indices[0], 0);  // device_id
      EXPECT_EQ(indices[1], 2);  // frequency
    }
  }
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";

  // Should have 2 prototypes: outer (segment + Pipeline) and inner (aggregates)
  EXPECT_EQ(builder.prototypes().size(), 2u);

  // Find the outer prototype (contains Pipeline) and inner prototype
  const PrototypeDef* outer_proto = nullptr;
  const PrototypeDef* inner_proto = nullptr;
  for (const auto& proto : builder.prototypes()) {
    bool has_pipeline = false;
    for (const auto& op : proto.operators) {
      if (op.type == "Pipeline") has_pipeline = true;
    }
    if (has_pipeline) {
      outer_proto = &proto;
    } else {
      inner_proto = &proto;
    }
  }
  ASSERT_NE(outer_proto, nullptr) << "One prototype should contain a Pipeline";
  ASSERT_NE(inner_proto, nullptr) << "One prototype should be the inner aggregate";

  // Outer prototype: segment expression compiled to bytecode.
  // No Abs/CompareGT/BooleanToNumber operators — Pipeline has segmentBytecode.
  bool proto_has_pipeline = false;
  for (const auto& op : outer_proto->operators) {
    EXPECT_NE(op.type, "Abs")
        << "Abs should NOT be in outer prototype (bytecode path)";
    EXPECT_NE(op.type, "CompareGT")
        << "CompareGT should NOT be in outer prototype (bytecode path)";
    EXPECT_NE(op.type, "BooleanToNumber")
        << "BooleanToNumber should NOT be in outer prototype (bytecode path)";
    if (op.type == "Pipeline") proto_has_pipeline = true;
  }
  EXPECT_TRUE(proto_has_pipeline) << "Outer prototype should have Pipeline";

  // Pipeline in outer prototype should have segmentBytecode/segmentConstants
  for (const auto& op : outer_proto->operators) {
    if (op.type == "Pipeline") {
      EXPECT_TRUE(op.double_array_params.count("segmentBytecode"))
          << "Pipeline should have segmentBytecode";
      EXPECT_TRUE(op.double_array_params.count("segmentConstants"))
          << "Pipeline should have segmentConstants";
      EXPECT_FALSE(op.double_array_params.at("segmentBytecode").empty())
          << "segmentBytecode should not be empty";
    }
  }

  // No c1 connections to Pipeline in outer prototype (bytecode path)
  for (const auto& conn : outer_proto->connections) {
    if (conn.to_port == "c1") {
      for (const auto& op : outer_proto->operators) {
        if (op.id == conn.to_id && op.type == "Pipeline") {
          ADD_FAILURE() << "Pipeline should NOT have a c1 connection (bytecode path)";
        }
      }
    }
  }

  // Inner prototype: with fusion → FusedExpressionVector; without → key extracts + CumulativeSum + CountNumber + VectorCompose
  bool has_cumsum = false, has_count = false, has_compose = false;
  bool has_fused_vector = false, has_fused_scalar = false;
  int extract_count = 0;
  for (const auto& op : inner_proto->operators) {
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "CountNumber") has_count = true;
    if (op.type == "VectorExtract") extract_count++;
    if (op.type == "VectorCompose") {
      has_compose = true;
      // 2 key columns + 2 aggregates = 4 ports
      EXPECT_EQ(op.params.at("numPorts"), 4.0);
    }
    if (op.type == "FusedExpressionVector") {
      has_fused_vector = true;
      // 2 key columns + 2 aggregates = 4 outputs
      EXPECT_EQ(op.params.at("numOutputs"), 4.0);
    }
    if (op.type == "FusedExpression") has_fused_scalar = true;
  }
  EXPECT_TRUE(has_fused_vector || (has_cumsum && has_count && has_compose))
      << "Inner prototype should have FusedExpressionVector or CumulativeSum+CountNumber+VectorCompose";
  EXPECT_FALSE(has_fused_scalar)
      << "Inner prototype should use FusedExpressionVector for vector input fusion";
  if (has_fused_vector) {
    EXPECT_EQ(extract_count, 0)
        << "FusedExpressionVector path should not emit VectorExtract operators";
  } else {
    EXPECT_GE(extract_count, 1)
        << "Fallback path should extract input columns";
  }

  // Field map: computed key mode outputs directly (no VectorProject), 0-based.
  // prototype outputs: [device_id, frequency, total, cnt]
  EXPECT_EQ(field_map.at("device_id"), 0);
  EXPECT_EQ(field_map.at("frequency"), 1);
  EXPECT_EQ(field_map.at("total"), 2);
  EXPECT_EQ(field_map.at("cnt"), 3);
}

// HAVING + mixed GROUP BY should throw
TEST_F(SegmentGroupByTest, MixedGroupByWithHavingThrows) {
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("device_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));

  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  // HAVING COUNT(*) > 5
  std::vector<Expr> count_args;
  auto having = cmp_expr(func_expr("COUNT", std::move(count_args)), ">", num(5));

  EXPECT_THROW(
      compile_group_by(select_list, group_by, std::move(having), input, scope, builder),
      std::runtime_error);
}

// Multiple segment expressions should throw
TEST_F(SegmentGroupByTest, MultipleSegmentExpressionsThrows) {
  std::vector<SelectItem> select_list;
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));

  // GROUP BY ABS(amplitude) > 0, frequency > 100  — 2 segment expressions
  std::vector<Expr> group_by;
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));
  group_by.push_back(cmp_expr(col("frequency"), ">", num(100)));

  EXPECT_THROW(
      compile_group_by(select_list, group_by, std::nullopt, input, scope, builder),
      std::runtime_error);
}

// ---------------------------------------------------------------------------
// Nested prototype JSON serialization round-trip
// ---------------------------------------------------------------------------

TEST_F(SegmentGroupByTest, MixedGroupByJsonRoundTrip) {
  // Compile a mixed GROUP BY query
  std::vector<SelectItem> select_list;
  select_list.push_back(item(col("device_id")));
  std::vector<Expr> sum_args;
  sum_args.push_back(col("amplitude"));
  select_list.push_back(item(func_expr("SUM", std::move(sum_args)), "total"));
  select_list.push_back(item(func_expr("COUNT", {}), "cnt"));

  std::vector<Expr> group_by;
  group_by.push_back(col("device_id"));
  std::vector<Expr> abs_args;
  abs_args.push_back(col("amplitude"));
  group_by.push_back(cmp_expr(func_expr("ABS", std::move(abs_args)), ">", num(0)));

  // Need to set up the outer graph with Input/Output since to_json() expects them
  builder.add_operator("input_0", "Input");
  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope, builder);
  builder.add_operator("output_0", "Output");
  builder.connect(ep, {"output_0", "i1"});

  // Serialize to JSON
  std::string json_str = builder.to_json();
  auto j = nlohmann::json::parse(json_str);

  // Find KeyedPipeline in JSON — its prototype should be inlined as an object
  bool found_keyed = false;
  for (const auto& op : j["operators"]) {
    if (op["type"] == "KeyedPipeline") {
      found_keyed = true;
      ASSERT_TRUE(op.contains("prototype"));
      ASSERT_TRUE(op["prototype"].is_object())
          << "KeyedPipeline prototype should be inlined as object";

      // The outer prototype should contain a Pipeline operator in native format
      // (not with a "prototype" field, but with entryOperator, outputMappings, etc.)
      bool found_inner_pipeline = false;
      for (const auto& inner_op : op["prototype"]["operators"]) {
        if (inner_op["type"] == "Pipeline") {
          found_inner_pipeline = true;
          EXPECT_FALSE(inner_op.contains("prototype"))
              << "Pipeline inside prototype should NOT have 'prototype' field in native format";
          EXPECT_TRUE(inner_op.contains("input_port_types"))
              << "Pipeline should have input_port_types";
          EXPECT_TRUE(inner_op.contains("output_port_types"))
              << "Pipeline should have output_port_types";
          EXPECT_TRUE(inner_op.contains("operators"))
              << "Pipeline should have operators array";
          EXPECT_TRUE(inner_op.contains("connections"))
              << "Pipeline should have connections array";
          EXPECT_TRUE(inner_op.contains("entryOperator"))
              << "Pipeline should have entryOperator";
          EXPECT_TRUE(inner_op.contains("outputMappings"))
              << "Pipeline should have outputMappings";
        }
      }
      EXPECT_TRUE(found_inner_pipeline)
          << "KeyedPipeline prototype should contain a Pipeline in native format";
    }
  }
  EXPECT_TRUE(found_keyed) << "KeyedPipeline not found in JSON";

  // Round-trip: deserialize and check structure
  auto [builder2, pre_output] =
      GraphBuilder::from_json_for_augmentation(json_str);

  // builder2 should have KeyedPipeline with a prototype reference
  bool found_keyed2 = false;
  for (const auto& op : builder2.operators()) {
    if (op.type == "KeyedPipeline") {
      found_keyed2 = true;
      EXPECT_TRUE(op.string_params.count("prototype"))
          << "KeyedPipeline should have prototype string_param after deser";
    }
  }
  EXPECT_TRUE(found_keyed2);

  // Should have 2 prototypes after round-trip (outer + inner)
  EXPECT_EQ(builder2.prototypes().size(), 2u)
      << "Should have 2 prototypes after round-trip (outer KeyedPipeline + inner Pipeline)";

  // The outer prototype should contain a Pipeline referencing the inner prototype
  bool outer_has_pipeline = false;
  for (const auto& proto : builder2.prototypes()) {
    for (const auto& op : proto.operators) {
      if (op.type == "Pipeline") {
        outer_has_pipeline = true;
        EXPECT_TRUE(op.string_params.count("prototype"))
            << "Pipeline inside prototype should reference inner prototype";
      }
    }
  }
  EXPECT_TRUE(outer_has_pipeline)
      << "Outer prototype should contain a Pipeline after round-trip";
}

// Test: SELECT AVG(value) FROM sensor GROUP BY FLOOR(TS() / 1000000)
// Numeric segment expression partitions data into time bins via Pipeline.
TEST_F(GroupByTest, NumericSegmentFloorTsDivision) {
  // Register a different stream for this test
  StreamSchema sensor_schema{"sensor", {{"value", 0}}};
  scope.register_stream("sensor", sensor_schema);

  std::vector<SelectItem> select_list;
  std::vector<Expr> avg_args;
  avg_args.push_back(col("value"));
  select_list.push_back(item(func_expr("AVG", std::move(avg_args)), "avg_value"));

  // GROUP BY FLOOR(TS() / 1000000)
  std::vector<Expr> group_by;
  std::vector<Expr> floor_args;
  floor_args.push_back(binary_expr("/", func_expr("TS", {}), num(1000000.0)));
  group_by.push_back(func_expr("FLOOR", std::move(floor_args)));

  auto [ep, field_map, is_seg] =
      compile_group_by(select_list, group_by, std::nullopt, input, scope,
                       builder, /*num_input_cols=*/1);

  EXPECT_TRUE(is_seg) << "Numeric segment expression should produce segment-only view";

  // Should have a Pipeline in the outer graph (not KeyedPipeline)
  bool found_pipeline = false;
  for (const auto& op : builder.operators()) {
    EXPECT_NE(op.type, "KeyedPipeline") << "Should NOT use KeyedPipeline for numeric segments";
    if (op.type == "Pipeline") {
      found_pipeline = true;
    }
  }
  EXPECT_TRUE(found_pipeline) << "Expected Pipeline in outer graph";

  // Should have exactly one prototype
  EXPECT_EQ(builder.prototypes().size(), 1u);

  // field_map: just avg_value, no key column
  EXPECT_EQ(field_map.size(), 1u);
  EXPECT_EQ(field_map.count("avg_value"), 1u);
  EXPECT_EQ(field_map.at("avg_value"), 0);
}

}  // namespace
}  // namespace rtbot_sql::compiler
