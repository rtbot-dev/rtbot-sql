// Minimal test: Does receive_debug() work with IMS vibration_moments program?
//
// The C++ benchmark uses receive() (non-debug) and works fine.
// The WASM/JavaScript path uses processMessageBufferDebug which calls
// receive_debug() and crashes with C++ exceptions for IMS programs.
// This test isolates whether the bug is in C++ receive_debug() or the WASM bridge.

#include "rtbot_sql/api/compiler.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace {

using rtbot_sql::CatalogSnapshot;
using rtbot_sql::CompilationResult;
using rtbot_sql::EntityType;
using rtbot_sql::StatementType;
using rtbot_sql::StreamSchema;
using rtbot_sql::ViewMeta;
using rtbot_sql::api::compile_sql;

constexpr const char* kStreamSql = R"sql(
CREATE TABLE vibration_raw (
    device_id DOUBLE,
    channel_id DOUBLE,
    amplitude DOUBLE
)
)sql";

constexpr const char* kBaseViewSql = R"sql(
CREATE VIEW vibration_moments AS
  SELECT device_id,
         channel_id,
         AVG(amplitude)                           AS mean_value,
         AVG(POWER(amplitude, 2))                 AS ex2,
         AVG(POWER(amplitude, 3))                 AS ex3,
         AVG(POWER(amplitude, 4))                 AS ex4,
         MAX(ABS(amplitude))                      AS peak_value,
         AVG(ABS(amplitude))                      AS mean_abs,
         AVG(POWER(ABS(amplitude), 0.5))          AS mean_sqrt_abs,
         COUNT(*)                                  AS sample_count
  FROM vibration_raw
  GROUP BY device_id, channel_id, ABS(amplitude) > 0
)sql";

constexpr const char* kPreset01Sql = R"sql(
CREATE MATERIALIZED VIEW rms_trend AS
  SELECT device_id,
         channel_id,
         POWER(ex2, 0.5) AS rms_value,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1
)sql";

CompilationResult compile_or_die(const std::string& sql,
                                 const CatalogSnapshot& catalog,
                                 const std::string& label) {
  auto result = compile_sql(sql, catalog);
  if (!result.has_errors()) {
    return result;
  }
  std::cerr << "Compilation failed for " << label << ":\n";
  for (const auto& error : result.errors) {
    std::cerr << "  - " << error.message << "\n";
  }
  std::exit(2);
}

void register_stream(CatalogSnapshot& catalog, const CompilationResult& stream) {
  catalog.streams[stream.entity_name] = stream.stream_schema;
}

void register_view(CatalogSnapshot& catalog,
                   const CompilationResult& view,
                   EntityType entity_type) {
  ViewMeta meta{};
  meta.name = view.entity_name;
  meta.entity_type = entity_type;
  meta.view_type = view.view_type;
  meta.field_map = view.field_map;
  meta.source_streams = view.source_streams;
  meta.program_json = view.program_json;
  meta.key_index = view.key_index;
  catalog.views[meta.name] = meta;
}

std::unique_ptr<rtbot::Message<rtbot::VectorNumberData>> make_vector_message(
    rtbot::timestamp_t timestamp, std::vector<double> values) {
  return rtbot::create_message<rtbot::VectorNumberData>(
      timestamp, rtbot::VectorNumberData{std::move(values)});
}

std::size_t count_total_messages(const rtbot::ProgramMsgBatch& batch) {
  std::size_t count = 0;
  for (const auto& [op_id, op_batch] : batch) {
    (void)op_id;
    for (const auto& [port, messages] : op_batch) {
      (void)port;
      count += messages.size();
    }
  }
  return count;
}

void print_batch(const rtbot::ProgramMsgBatch& batch, const std::string& label) {
  std::cout << label << ":\n";
  if (batch.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& [op_id, op_batch] : batch) {
    for (const auto& [port, messages] : op_batch) {
      std::cout << "  " << op_id << ":" << port << " -> " << messages.size() << " messages\n";
    }
  }
}

}  // namespace

int main() {
  try {
    CatalogSnapshot catalog;

    // ── Step 1: Compile stream ──────────────────────────────────
    std::cout << "=== Compiling vibration_raw stream ===\n";
    const auto stream = compile_or_die(kStreamSql, catalog, "vibration_raw");
    register_stream(catalog, stream);

    // ── Step 2: Compile vibration_moments view ──────────────────
    std::cout << "=== Compiling vibration_moments view ===\n";
    const auto base_view = compile_or_die(kBaseViewSql, catalog, "vibration_moments");
    register_view(catalog, base_view, EntityType::VIEW);

    std::cout << "  program_json length: " << base_view.program_json.size() << "\n";
    std::cout << "  program_json:\n" << base_view.program_json << "\n";
    std::cout << "  field_map:\n";
    for (const auto& [name, idx] : base_view.field_map) {
      std::cout << "    " << name << " -> " << idx << "\n";
    }
    std::cout << "  source_streams:";
    for (const auto& s : base_view.source_streams) std::cout << " " << s;
    std::cout << "\n\n";

    // ── Step 3: Create Program from JSON ────────────────────────
    std::cout << "=== Creating Program from base_view.program_json ===\n";
    rtbot::Program base_program(base_view.program_json);
    std::cout << "  OK\n\n";

    // ── Step 4: Test receive() (non-debug, known to work) ───────
    std::cout << "=== Test receive() with 3 data samples + sentinel ===\n";
    rtbot::timestamp_t ts = 1;

    for (int i = 0; i < 3; ++i) {
      auto batch = base_program.receive(
          make_vector_message(ts++, {2.0, 1.0, 0.5 + 0.1 * i}), "i1");
      std::cout << "  receive() sample " << i << ": ";
      print_batch(batch, "output");
    }
    // Sentinel (amplitude = 0)
    {
      auto batch = base_program.receive(
          make_vector_message(ts++, {2.0, 1.0, 0.0}), "i1");
      std::cout << "  receive() sentinel: ";
      print_batch(batch, "output");
    }
    std::cout << "\n";

    // ── Step 5: Test receive_debug() (the path that crashes in WASM) ──
    std::cout << "=== Test receive_debug() with 3 data samples + sentinel ===\n";

    for (int i = 0; i < 3; ++i) {
      try {
        auto batch = base_program.receive_debug(
            make_vector_message(ts++, {2.0, 1.0, 0.5 + 0.1 * i}), "i1");
        std::cout << "  receive_debug() sample " << i << ": ";
        print_batch(batch, "output");
      } catch (const std::exception& e) {
        std::cerr << "  *** CRASH in receive_debug() sample " << i << ": " << e.what() << "\n";
      }
    }
    // Sentinel
    {
      try {
        auto batch = base_program.receive_debug(
            make_vector_message(ts++, {2.0, 1.0, 0.0}), "i1");
        std::cout << "  receive_debug() sentinel: ";
        print_batch(batch, "output");
      } catch (const std::exception& e) {
        std::cerr << "  *** CRASH in receive_debug() sentinel: " << e.what() << "\n";
      }
    }
    std::cout << "\n";

    // ── Step 6: Also test via ProgramManager (the WASM path) ────
    std::cout << "=== Test via ProgramManager (mimicking WASM path) ===\n";
    auto& mgr = rtbot::ProgramManager::instance();

    std::string create_result = mgr.create_program("test_debug", base_view.program_json);
    std::cout << "  create_program: " << (create_result.empty() ? "OK" : create_result) << "\n";

    // Add vector message to buffer
    bool started = mgr.begin_vector_message("test_debug", "i1", ts);
    std::cout << "  begin_vector_message: " << started << "\n";
    mgr.push_vector_message_value("test_debug", "i1", 2.0);
    mgr.push_vector_message_value("test_debug", "i1", 1.0);
    mgr.push_vector_message_value("test_debug", "i1", 0.75);
    bool ended = mgr.end_vector_message("test_debug", "i1");
    std::cout << "  end_vector_message: " << ended << "\n";

    try {
      auto batch = mgr.process_message_buffer_debug("test_debug");
      std::cout << "  process_message_buffer_debug: ";
      print_batch(batch, "output");
    } catch (const std::exception& e) {
      std::cerr << "  *** CRASH in process_message_buffer_debug: " << e.what() << "\n";
    }
    std::cout << "\n";

    // ── Step 7: Also test rms_trend (downstream program) ────────
    std::cout << "=== Compiling rms_trend (downstream) ===\n";
    const auto rms = compile_or_die(kPreset01Sql, catalog, "rms_trend");
    std::cout << "  program_json length: " << rms.program_json.size() << "\n";
    std::cout << "  field_map:\n";
    for (const auto& [name, idx] : rms.field_map) {
      std::cout << "    " << name << " -> " << idx << "\n";
    }
    std::cout << "  source_streams:";
    for (const auto& s : rms.source_streams) std::cout << " " << s;
    std::cout << "\n";
    std::cout << "  program_json:\n" << rms.program_json << "\n\n";

    std::cout << "=== Test receive_debug() for rms_trend ===\n";
    rtbot::Program rms_program(rms.program_json);
    // Feed a vector representing vibration_moments output:
    // device_id, channel_id, mean_value, ex2, ex3, ex4, peak_value, mean_abs, mean_sqrt_abs, sample_count
    try {
      auto batch = rms_program.receive_debug(
          make_vector_message(100, {2.0, 1.0, 0.5, 0.3, 0.2, 0.1, 0.8, 0.4, 0.6, 100.0}), "i1");
      std::cout << "  receive_debug() for rms_trend: ";
      print_batch(batch, "output");
    } catch (const std::exception& e) {
      std::cerr << "  *** CRASH in receive_debug() for rms_trend: " << e.what() << "\n";
    }

    std::cout << "\n=== ALL TESTS COMPLETED ===\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "FATAL: " << ex.what() << "\n";
    return 1;
  }
}
