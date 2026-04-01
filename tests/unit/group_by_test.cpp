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

  // Prototype contains: Input, VectorExtract, CumulativeSum, Output
  // VectorCompose is always present (even for single non-key output,
  // because proto_out expects vector_number port type)
  bool has_input = false, has_ext = false, has_cumsum = false,
       has_output = false;
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "Input") has_input = true;
    if (op.type == "VectorExtract") {
      has_ext = true;
      EXPECT_EQ(op.params.at("index"), 2.0);  // quantity
    }
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "Output") has_output = true;
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 1.0);
    }
  }
  EXPECT_TRUE(has_input);
  EXPECT_TRUE(has_ext);
  EXPECT_TRUE(has_cumsum);
  EXPECT_TRUE(has_output);
  EXPECT_TRUE(has_compose);  // always compose, even for single output

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

  // VectorCompose with 3 ports (SUM, COUNT, AVG)
  bool has_compose = false;
  for (const auto& op : proto.operators) {
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 3.0);
    }
  }
  EXPECT_TRUE(has_compose);

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

  // Outer graph must have: VectorExtract×4, Linear (hash), VectorCompose
  // (augment), KeyedPipeline.
  bool has_linear = false;
  bool has_keyed = false;
  bool has_augment_compose = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Linear") has_linear = true;
    if (op.type == "KeyedPipeline") has_keyed = true;
    if (op.type == "VectorCompose") has_augment_compose = true;
  }
  EXPECT_TRUE(has_linear) << "Expected Linear (hash) operator in outer graph";
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";
  EXPECT_TRUE(has_augment_compose) << "Expected VectorCompose in outer graph";

  // One prototype wrapping the per-key aggregation.
  ASSERT_EQ(builder.prototypes().size(), 1u);

  // Field map: both key columns + aggregate (KeyedPipeline prepends hash key
  // at index 0, so prototype outputs start at 1).
  EXPECT_EQ(field_map.at("instrument_id"), 1);
  EXPECT_EQ(field_map.at("account_id"), 2);
  EXPECT_EQ(field_map.at("total"), 3);
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
TEST_F(ClassificationTest, ClassifyFunctionCallAsSegment) {
  // GROUP BY ABS(amplitude) — a FuncCall, not a ColumnRef
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

  // Outer graph should contain segment expression operators:
  // VectorExtract (amplitude), Abs, CompareGT
  bool has_abs = false, has_cmp_gt = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Abs") has_abs = true;
    if (op.type == "CompareGT") {
      has_cmp_gt = true;
      EXPECT_EQ(op.params.at("value"), 0.0);
    }
  }
  EXPECT_TRUE(has_abs) << "Expected Abs in outer graph for segment expression";
  EXPECT_TRUE(has_cmp_gt)
      << "Expected CompareGT in outer graph for segment expression";

  // Outer graph should contain BooleanToNumber (converts predicate for Pipeline c1)
  bool has_b2n = false;
  std::string b2n_id;
  for (const auto& op : builder.operators()) {
    if (op.type == "BooleanToNumber") {
      has_b2n = true;
      b2n_id = op.id;
    }
  }
  EXPECT_TRUE(has_b2n)
      << "Expected BooleanToNumber in outer graph between predicate and Pipeline c1";

  // Verify connection chain: CompareGT -> BooleanToNumber -> Pipeline.c1
  if (has_b2n) {
    bool cmp_to_b2n = false;
    bool b2n_to_pipeline_c1 = false;
    for (const auto& conn : builder.connections()) {
      if (conn.to_id == b2n_id && conn.to_port == "i1") {
        // The source should be the CompareGT operator
        for (const auto& op : builder.operators()) {
          if (op.id == conn.from_id && op.type == "CompareGT") {
            cmp_to_b2n = true;
          }
        }
      }
      if (conn.from_id == b2n_id && conn.from_port == "o1" &&
          conn.to_port == "c1") {
        // The target should be the Pipeline operator
        for (const auto& op : builder.operators()) {
          if (op.id == conn.to_id && op.type == "Pipeline") {
            b2n_to_pipeline_c1 = true;
          }
        }
      }
    }
    EXPECT_TRUE(cmp_to_b2n)
        << "CompareGT output should connect to BooleanToNumber input";
    EXPECT_TRUE(b2n_to_pipeline_c1)
        << "BooleanToNumber output should connect to Pipeline c1";
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

  // Outer prototype should contain segment expression operators (Abs, CompareGT)
  // and Pipeline operator
  bool proto_has_abs = false, proto_has_cmp = false, proto_has_pipeline = false;
  for (const auto& op : outer_proto->operators) {
    if (op.type == "Abs") proto_has_abs = true;
    if (op.type == "CompareGT") {
      proto_has_cmp = true;
      EXPECT_EQ(op.params.at("value"), 0.0);
    }
    if (op.type == "Pipeline") proto_has_pipeline = true;
  }
  EXPECT_TRUE(proto_has_abs) << "Outer prototype should have Abs for segment expr";
  EXPECT_TRUE(proto_has_cmp) << "Outer prototype should have CompareGT for segment expr";
  EXPECT_TRUE(proto_has_pipeline) << "Outer prototype should have Pipeline";

  // Outer prototype should contain BooleanToNumber between predicate and Pipeline c1
  bool proto_has_b2n = false;
  std::string b2n_id;
  for (const auto& op : outer_proto->operators) {
    if (op.type == "BooleanToNumber") {
      proto_has_b2n = true;
      b2n_id = op.id;
    }
  }
  EXPECT_TRUE(proto_has_b2n)
      << "Outer prototype should have BooleanToNumber between predicate and Pipeline c1";

  // Verify connection chain in outer prototype: CompareGT -> BooleanToNumber -> Pipeline.c1
  if (proto_has_b2n) {
    bool cmp_to_b2n = false;
    bool b2n_to_pipeline_c1 = false;
    for (const auto& conn : outer_proto->connections) {
      if (conn.to_id == b2n_id && conn.to_port == "i1") {
        for (const auto& op : outer_proto->operators) {
          if (op.id == conn.from_id && op.type == "CompareGT") {
            cmp_to_b2n = true;
          }
        }
      }
      if (conn.from_id == b2n_id && conn.from_port == "o1" &&
          conn.to_port == "c1") {
        for (const auto& op : outer_proto->operators) {
          if (op.id == conn.to_id && op.type == "Pipeline") {
            b2n_to_pipeline_c1 = true;
          }
        }
      }
    }
    EXPECT_TRUE(cmp_to_b2n)
        << "CompareGT should connect to BooleanToNumber in outer prototype";
    EXPECT_TRUE(b2n_to_pipeline_c1)
        << "BooleanToNumber should connect to Pipeline c1 in outer prototype";
  }

  // Inner prototype should have CumulativeSum, CountNumber, VectorCompose
  bool has_cumsum = false, has_count = false, has_compose = false;
  for (const auto& op : inner_proto->operators) {
    if (op.type == "CumulativeSum") has_cumsum = true;
    if (op.type == "CountNumber") has_count = true;
    if (op.type == "VectorCompose") {
      has_compose = true;
      EXPECT_EQ(op.params.at("numPorts"), 2.0);  // total + cnt
    }
  }
  EXPECT_TRUE(has_cumsum) << "Inner prototype should have CumulativeSum";
  EXPECT_TRUE(has_count) << "Inner prototype should have CountNumber";
  EXPECT_TRUE(has_compose) << "Inner prototype should have VectorCompose";

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

  // Outer graph: should have hash computation (Linear), augmented vector
  // (VectorCompose), and KeyedPipeline
  bool has_linear = false;
  bool has_keyed = false;
  bool has_augment_compose = false;
  for (const auto& op : builder.operators()) {
    if (op.type == "Linear") has_linear = true;
    if (op.type == "KeyedPipeline") {
      has_keyed = true;
      // key_index should be num_input_cols (= 3, the hash column position)
      EXPECT_EQ(op.params.at("key_index"), 3.0);
    }
    if (op.type == "VectorCompose") has_augment_compose = true;
  }
  EXPECT_TRUE(has_linear) << "Expected Linear (hash) operator in outer graph";
  EXPECT_TRUE(has_keyed) << "Expected KeyedPipeline in outer graph";
  EXPECT_TRUE(has_augment_compose) << "Expected VectorCompose (augment) in outer graph";

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

  // Outer prototype should contain segment expression operators and Pipeline
  bool proto_has_abs = false, proto_has_cmp = false, proto_has_pipeline = false;
  for (const auto& op : outer_proto->operators) {
    if (op.type == "Abs") proto_has_abs = true;
    if (op.type == "CompareGT") {
      proto_has_cmp = true;
      EXPECT_EQ(op.params.at("value"), 0.0);
    }
    if (op.type == "Pipeline") proto_has_pipeline = true;
  }
  EXPECT_TRUE(proto_has_abs) << "Outer prototype should have Abs for segment expr";
  EXPECT_TRUE(proto_has_cmp) << "Outer prototype should have CompareGT for segment expr";
  EXPECT_TRUE(proto_has_pipeline) << "Outer prototype should have Pipeline";

  // Outer prototype should contain BooleanToNumber between predicate and Pipeline c1
  bool proto_has_b2n = false;
  for (const auto& op : outer_proto->operators) {
    if (op.type == "BooleanToNumber") proto_has_b2n = true;
  }
  EXPECT_TRUE(proto_has_b2n)
      << "Outer prototype should have BooleanToNumber for segment predicate conversion";

  // Inner prototype should have key column extraction + aggregates
  bool has_cumsum = false, has_count = false, has_compose = false;
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
  }
  EXPECT_TRUE(has_cumsum) << "Inner prototype should have CumulativeSum";
  EXPECT_TRUE(has_count) << "Inner prototype should have CountNumber";
  EXPECT_TRUE(has_compose) << "Inner prototype should have VectorCompose";
  // Should extract device_id, frequency, and amplitude (for SUM)
  EXPECT_GE(extract_count, 3) << "Inner prototype should extract key columns + data";

  // Field map: hash prepended at index 0 by KeyedPipeline
  // prototype outputs: [device_id, frequency, total, cnt]
  // final output: [hash(0), device_id(1), frequency(2), total(3), cnt(4)]
  EXPECT_EQ(field_map.at("device_id"), 1);
  EXPECT_EQ(field_map.at("frequency"), 2);
  EXPECT_EQ(field_map.at("total"), 3);
  EXPECT_EQ(field_map.at("cnt"), 4);
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

}  // namespace
}  // namespace rtbot_sql::compiler
