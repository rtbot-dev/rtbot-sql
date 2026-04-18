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
  EXPECT_EQ(s.base_stream_inputs.at("trades"), "input__session");
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

// ---------------------------------------------------------------------------
// Multi-base-stream: two distinct streams, each with its own view; session
// emits a single shared Input with two ports.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, MultiBaseStream) {
  StreamSchema other{"ticks", {{"price", 0}, {"qty", 1}}};
  catalog.streams["ticks"] = other;

  auto rA = compile_sql(
      "CREATE MATERIALIZED VIEW va AS SELECT price FROM trades", catalog);
  ASSERT_FALSE(rA.has_errors()) << rA.errors[0].message;
  register_view("va", rA, EntityType::MATERIALIZED_VIEW);

  auto rB = compile_sql(
      "CREATE MATERIALIZED VIEW vb AS SELECT price FROM ticks", catalog);
  ASSERT_FALSE(rB.has_errors()) << rB.errors[0].message;
  register_view("vb", rB, EntityType::MATERIALIZED_VIEW);

  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);

  // Two base streams, each with its own port on the shared session Input.
  ASSERT_EQ(s.base_stream_inputs.size(), 2u);
  EXPECT_EQ(s.base_stream_inputs.at("trades"), "input__session");
  EXPECT_EQ(s.base_stream_inputs.at("ticks"), "input__session");
  // Ports are assigned in topological-visit order.
  EXPECT_NE(s.base_stream_ports.at("trades"),
            s.base_stream_ports.at("ticks"));

  auto program = json::parse(s.program_json);
  // Exactly one Input operator with numInputPorts=2 (encoded as portTypes array).
  int input_count = 0;
  for (const auto& op : program["operators"]) {
    if (op["type"].get<std::string>() == "Input") {
      input_count++;
      ASSERT_TRUE(op.contains("portTypes"));
      EXPECT_EQ(op["portTypes"].size(), 2u);
    }
  }
  EXPECT_EQ(input_count, 1);

  // Feed each stream on its port and confirm the corresponding view emits.
  rtbot::Program program_obj(s.program_json);

  auto trades_port = s.base_stream_ports.at("trades");
  auto msg_t = rtbot::create_message<rtbot::VectorNumberData>(
      1, rtbot::VectorNumberData{{100.0, 42.0, 1.0}});
  auto batch_t = program_obj.receive(std::move(msg_t), trades_port);

  auto ticks_port = s.base_stream_ports.at("ticks");
  auto msg_k = rtbot::create_message<rtbot::VectorNumberData>(
      2, rtbot::VectorNumberData{{3.14, 1.0}});
  auto batch_k = program_obj.receive(std::move(msg_k), ticks_port);

  EXPECT_TRUE(batch_t.count(s.view_terminals.at("va")));
  EXPECT_TRUE(batch_k.count(s.view_terminals.at("vb")));
}

// ---------------------------------------------------------------------------
// Table-JOIN view: still rejected in session mode (table changelog wiring
// is a follow-up). The error message must be informative.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, TableJoinRejectedInSession) {
  TableSchema instruments;
  instruments.name = "instruments";
  instruments.columns = {{"id", 0}, {"name", 1, ColumnType::TEXT}};
  catalog.tables["instruments"] = instruments;

  auto r = compile_sql(
      "CREATE MATERIALIZED VIEW v1 AS "
      "SELECT price FROM trades "
      "INNER JOIN instruments ON trades.instrument_id = instruments.id",
      catalog);
  ASSERT_FALSE(r.has_errors()) << r.errors[0].message;
  register_view("v1", r, EntityType::MATERIALIZED_VIEW);

  auto s = compile_session_program(catalog);
  EXPECT_TRUE(s.has_errors());
  ASSERT_FALSE(s.errors.empty());
  EXPECT_NE(s.errors[0].message.find("table"), std::string::npos)
      << "error should mention table join: " << s.errors[0].message;
}

// ---------------------------------------------------------------------------
// Port alignment: in a multi-base-stream catalog, the view's local Input
// oN port must wire to the correct session source matching its
// source_streams[N-1] ordering. Tested via runtime parity: both streams
// feeding the session produces the same output the stand-alone views
// would have, on the same timestamps.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, MultiBaseStreamRuntimeParity) {
  StreamSchema other{"ticks", {{"price", 0}, {"qty", 1}}};
  catalog.streams["ticks"] = other;

  auto rA = compile_sql(
      "CREATE MATERIALIZED VIEW va AS SELECT price FROM trades", catalog);
  register_view("va", rA, EntityType::MATERIALIZED_VIEW);
  auto rB = compile_sql(
      "CREATE MATERIALIZED VIEW vb AS SELECT price FROM ticks", catalog);
  register_view("vb", rB, EntityType::MATERIALIZED_VIEW);

  // Solo pipelines for each view.
  rtbot::Program solo_a(rA.program_json);
  rtbot::Program solo_b(rB.program_json);
  auto solo_a_batch = solo_a.receive(
      rtbot::create_message<rtbot::VectorNumberData>(
          10, rtbot::VectorNumberData{{9.0, 2.5, 4.0}}),
      "i1");
  auto solo_b_batch = solo_b.receive(
      rtbot::create_message<rtbot::VectorNumberData>(
          20, rtbot::VectorNumberData{{7.7, 1.0}}),
      "i1");

  auto s = compile_session_program(catalog);
  ASSERT_FALSE(s.has_errors())
      << (s.errors.empty() ? "" : s.errors[0].message);
  rtbot::Program session(s.program_json);
  auto session_a = session.receive(
      rtbot::create_message<rtbot::VectorNumberData>(
          10, rtbot::VectorNumberData{{9.0, 2.5, 4.0}}),
      s.base_stream_ports.at("trades"));
  auto session_b = session.receive(
      rtbot::create_message<rtbot::VectorNumberData>(
          20, rtbot::VectorNumberData{{7.7, 1.0}}),
      s.base_stream_ports.at("ticks"));

  auto first = [](const rtbot::ProgramMsgBatch& b) -> std::vector<double> {
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

  EXPECT_EQ(first(solo_a_batch), first(session_a));
  EXPECT_EQ(first(solo_b_batch), first(session_b));
}

// ---------------------------------------------------------------------------
// Cycle detection still works after the multi-source refactor.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, CycleDetection) {
  ViewMeta a{};
  a.name = "a";
  a.entity_type = EntityType::VIEW;
  a.source_streams = {"b"};
  a.program_json = "{}";  // content irrelevant — topo sort trips first
  catalog.views["a"] = a;

  ViewMeta b{};
  b.name = "b";
  b.entity_type = EntityType::VIEW;
  b.source_streams = {"a"};
  b.program_json = "{}";
  catalog.views["b"] = b;

  auto s = compile_session_program(catalog);
  EXPECT_TRUE(s.has_errors());
  ASSERT_FALSE(s.errors.empty());
  EXPECT_NE(s.errors[0].message.find("cycle"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Unknown source: view references something that's not a stream, view, or
// table.
// ---------------------------------------------------------------------------

TEST_F(SessionConsolidationTest, UnknownSourceError) {
  // Register a legitimate view so `base_streams` is non-empty and the
  // compile reaches the per-view source-resolution step.
  auto rOk = compile_sql(
      "CREATE MATERIALIZED VIEW vok AS SELECT price FROM trades", catalog);
  ASSERT_FALSE(rOk.has_errors());
  register_view("vok", rOk, EntityType::MATERIALIZED_VIEW);

  // Insert a view that claims a bogus source in the catalog.
  ViewMeta v{};
  v.name = "vbad";
  v.entity_type = EntityType::MATERIALIZED_VIEW;
  v.source_streams = {"no_such_stream"};
  v.program_json = rOk.program_json;
  catalog.views["vbad"] = v;

  auto s = compile_session_program(catalog);
  EXPECT_TRUE(s.has_errors());
  ASSERT_FALSE(s.errors.empty());
  // Any error message mentioning the bogus source name is acceptable.
  EXPECT_NE(s.errors[0].message.find("no_such_stream"), std::string::npos)
      << "error should name the bogus source: " << s.errors[0].message;
}

}  // namespace
}  // namespace rtbot_sql::api
