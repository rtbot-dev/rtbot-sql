// Tests for compile_session_program: multi-view consolidation into one
// rtbot Program with inlined view subgraphs.

#include "rtbot_sql/api/compiler.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace rtbot_sql::api {
namespace {

using json = nlohmann::json;

class SessionConsolidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StreamSchema trades{
        "trades",
        {{"instrument_id", 0}, {"price", 1}, {"quantity", 2}}};
    catalog.streams["trades"] = trades;
  }

  void register_view(const std::string& name, const CompilationResult& r,
                     EntityType et) {
    ViewMeta meta{};
    meta.name = name;
    meta.entity_type = et;
    meta.view_type = r.view_type;
    meta.field_map = r.field_map;
    meta.source_streams = r.source_streams;
    meta.program_json = r.program_json;
    meta.key_index = r.key_index;
    catalog.views[name] = meta;
  }

  CatalogSnapshot catalog;
};

// ---------------------------------------------------------------------------
// Empty session: no views.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, NoViewsIsError) {
  auto s = compile_session_program(catalog);
  EXPECT_TRUE(s.has_errors())
      << "empty catalog should fail: no base streams";
}

// ---------------------------------------------------------------------------
// Single materialized view: one Input, no Output, one Collector attachment.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, SingleMaterializedView) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v1 AS SELECT price FROM trades",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  register_view("v1", r, EntityType::MATERIALIZED_VIEW);

  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);

  // view_terminals has one entry; v1 is in materialized_views.
  EXPECT_EQ(s.view_terminals.size(), 1u);
  ASSERT_EQ(s.materialized_views.size(), 1u);
  EXPECT_EQ(s.materialized_views[0], "v1");

  // One base-stream input.
  ASSERT_EQ(s.base_stream_inputs.size(), 1u);
  EXPECT_EQ(s.base_stream_inputs.at("trades"), "input__trades");
  EXPECT_EQ(s.base_stream_ports.at("trades"), "i1");

  // Program JSON has an "output" map keyed by the terminal op id.
  auto program = json::parse(s.program_json);
  EXPECT_TRUE(program.contains("output"));
  EXPECT_TRUE(program["output"].contains(s.view_terminals.at("v1")));

  // No operator of type Output should remain in the consolidated graph.
  for (const auto& op : program["operators"]) {
    EXPECT_NE(op["type"].get<std::string>(), "Output");
  }

  // Exactly one Input operator: the shared base-stream entry.
  int input_count = 0;
  for (const auto& op : program["operators"]) {
    if (op["type"].get<std::string>() == "Input") input_count++;
  }
  EXPECT_EQ(input_count, 1);

  // Deploy and send one row; verify the Collector receives the output.
  rtbot::Program program_obj(s.program_json);
  auto msg = rtbot::create_message<rtbot::VectorNumberData>(
      1, rtbot::VectorNumberData{{100.0, 42.0, 1.0}});
  auto batch = program_obj.receive(std::move(msg), "i1");

  // ProgramMsgBatch is keyed by upstream op id; should contain the v1
  // terminal.
  ASSERT_TRUE(batch.count(s.view_terminals.at("v1")))
      << "terminal for v1 not present in batch";
}

// ---------------------------------------------------------------------------
// View chain: A (plain VIEW) → B (MATERIALIZED). B's subgraph must wire
// directly from A's terminal — no intermediate Input or Output.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, ViewToMaterializedViewChain) {
  auto rA = compile_sql(
      "CREATE VIEW va AS SELECT price FROM trades", catalog);
  ASSERT_FALSE(rA.has_errors()) << rA.errors[0].message;
  register_view("va", rA, EntityType::VIEW);

  auto rB = compile_sql(
      "CREATE MATERIALIZED VIEW vb AS SELECT price FROM va", catalog);
  ASSERT_FALSE(rB.has_errors()) << rB.errors[0].message;
  register_view("vb", rB, EntityType::MATERIALIZED_VIEW);

  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);

  // Only vb is materialized; va terminal exists but is not exposed.
  ASSERT_EQ(s.materialized_views.size(), 1u);
  EXPECT_EQ(s.materialized_views[0], "vb");
  EXPECT_EQ(s.view_terminals.size(), 2u);

  auto program = json::parse(s.program_json);

  // No Output operators at all.
  for (const auto& op : program["operators"]) {
    EXPECT_NE(op["type"].get<std::string>(), "Output");
  }

  // Exactly one Input: the base-stream entry.
  int input_count = 0;
  for (const auto& op : program["operators"]) {
    if (op["type"].get<std::string>() == "Input") input_count++;
  }
  EXPECT_EQ(input_count, 1);

  // Output map contains only vb's terminal, not va's.
  ASSERT_TRUE(program.contains("output"));
  EXPECT_EQ(program["output"].size(), 1u);
  EXPECT_TRUE(program["output"].contains(s.view_terminals.at("vb")));
}

// ---------------------------------------------------------------------------
// Diamond fan-out: A → B, A → C. A appears once; B and C each inline their
// own body attached to A's terminal.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, DiamondFanout) {
  auto rA = compile_sql(
      "CREATE VIEW va AS SELECT price FROM trades", catalog);
  ASSERT_FALSE(rA.has_errors());
  register_view("va", rA, EntityType::VIEW);

  auto rB = compile_sql(
      "CREATE MATERIALIZED VIEW vb AS SELECT price FROM va", catalog);
  ASSERT_FALSE(rB.has_errors());
  register_view("vb", rB, EntityType::MATERIALIZED_VIEW);

  auto rC = compile_sql(
      "CREATE MATERIALIZED VIEW vc AS SELECT price FROM va", catalog);
  ASSERT_FALSE(rC.has_errors());
  register_view("vc", rC, EntityType::MATERIALIZED_VIEW);

  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);

  ASSERT_EQ(s.materialized_views.size(), 2u);
  auto program = json::parse(s.program_json);

  // Named outputs: vb and vc terminals both present.
  ASSERT_TRUE(program.contains("output"));
  EXPECT_EQ(program["output"].size(), 2u);
  EXPECT_TRUE(program["output"].contains(s.view_terminals.at("vb")));
  EXPECT_TRUE(program["output"].contains(s.view_terminals.at("vc")));
}

// ---------------------------------------------------------------------------
// End-to-end runtime parity: one row fed into the consolidated program
// produces the same materialized value as a single-view program.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, RuntimeParityWithSingleView) {
  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v1 AS SELECT price FROM trades",
      catalog);
  ASSERT_FALSE(r.has_errors());
  register_view("v1", r, EntityType::MATERIALIZED_VIEW);

  // Solo (pre-consolidation) program.
  rtbot::Program solo(r.program_json);
  auto solo_msg = rtbot::create_message<rtbot::VectorNumberData>(
      10, rtbot::VectorNumberData{{7.0, 3.14, 2.0}});
  auto solo_batch = solo.receive(std::move(solo_msg), "i1");

  // Consolidated program.
  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);
  rtbot::Program session(s.program_json);
  auto session_msg = rtbot::create_message<rtbot::VectorNumberData>(
      10, rtbot::VectorNumberData{{7.0, 3.14, 2.0}});
  auto session_batch = session.receive(std::move(session_msg), "i1");

  // Both should produce exactly one output message with the same value.
  auto first_output = [](const rtbot::ProgramMsgBatch& b)
      -> std::vector<double> {
    for (const auto& [op_id, op_batch] : b) {
      for (const auto& [port, msgs] : op_batch) {
        if (msgs.empty()) continue;
        auto* v = dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
            msgs[0].get());
        if (v && v->data.values) return *v->data.values;
      }
    }
    return {};
  };

  auto solo_vals = first_output(solo_batch);
  auto session_vals = first_output(session_batch);
  EXPECT_EQ(solo_vals, session_vals);
  ASSERT_EQ(session_vals.size(), 1u);
  EXPECT_DOUBLE_EQ(session_vals[0], 3.14);  // price was column 1
}

}  // namespace
}  // namespace rtbot_sql::api
