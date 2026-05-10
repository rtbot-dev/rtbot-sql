#include <jni.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "rtbot/Message.h"
#include "rtbot/Program.h"
#include "rtbot/compiled/jit/JitCompiler.h"
#include "rtbot_sql/api/batch_decoder.h"
#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/preprocessor.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/parser.h"

using json = nlohmann::json;
using namespace rtbot_sql;

namespace {


struct NativeFeedStats {
  std::atomic<std::uint64_t> calls{0};
  std::atomic<std::uint64_t> input_values{0};
  std::atomic<std::uint64_t> total_ns{0};
  std::atomic<std::uint64_t> values_copy_ns{0};
  std::atomic<std::uint64_t> port_decode_ns{0};
  std::atomic<std::uint64_t> message_build_ns{0};
  std::atomic<std::uint64_t> receive_ns{0};
  std::atomic<std::uint64_t> json_serialize_ns{0};
  std::atomic<std::uint64_t> jstring_ns{0};
  std::atomic<std::uint64_t> output_messages{0};
  std::atomic<std::uint64_t> output_values{0};
};

NativeFeedStats g_native_feed_stats;
std::atomic<bool> g_native_feed_stats_enabled{false};

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void reset_native_feed_stats() {
  g_native_feed_stats.calls.store(0, std::memory_order_relaxed);
  g_native_feed_stats.input_values.store(0, std::memory_order_relaxed);
  g_native_feed_stats.total_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.values_copy_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.port_decode_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.message_build_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.receive_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.json_serialize_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.jstring_ns.store(0, std::memory_order_relaxed);
  g_native_feed_stats.output_messages.store(0, std::memory_order_relaxed);
  g_native_feed_stats.output_values.store(0, std::memory_order_relaxed);
}

json native_feed_stats_to_json() {
  return {
      {"native_feed_calls",
       g_native_feed_stats.calls.load(std::memory_order_relaxed)},
      {"native_input_values",
       g_native_feed_stats.input_values.load(std::memory_order_relaxed)},
      {"native_total_ns",
       g_native_feed_stats.total_ns.load(std::memory_order_relaxed)},
      {"native_values_copy_ns",
       g_native_feed_stats.values_copy_ns.load(std::memory_order_relaxed)},
      {"native_port_decode_ns",
       g_native_feed_stats.port_decode_ns.load(std::memory_order_relaxed)},
      {"native_message_build_ns",
       g_native_feed_stats.message_build_ns.load(std::memory_order_relaxed)},
      {"native_receive_ns",
       g_native_feed_stats.receive_ns.load(std::memory_order_relaxed)},
      {"native_json_serialize_ns",
       g_native_feed_stats.json_serialize_ns.load(std::memory_order_relaxed)},
      {"native_jstring_ns",
       g_native_feed_stats.jstring_ns.load(std::memory_order_relaxed)},
      {"native_output_messages",
       g_native_feed_stats.output_messages.load(std::memory_order_relaxed)},
      {"native_output_values",
       g_native_feed_stats.output_values.load(std::memory_order_relaxed)},
  };
}

struct BatchStats {
  std::uint64_t messages = 0;
  std::uint64_t values = 0;
};

BatchStats count_batch_stats(const rtbot::ProgramMsgBatch& batch) {
  BatchStats stats{};
  for (const auto& [operator_id, operator_batch] : batch) {
    (void)operator_id;
    for (const auto& [port, messages] : operator_batch) {
      (void)port;
      for (const auto& message : messages) {
        ++stats.messages;
        if (auto* vec = dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
                message.get())) {
          if (vec->data.values) {
            stats.values += static_cast<std::uint64_t>(vec->data.values->size());
          }
        } else if (dynamic_cast<rtbot::Message<rtbot::NumberData>*>(
                       message.get()) != nullptr) {
          ++stats.values;
        }
      }
    }
  }
  return stats;
}

bool is_batch_empty(const rtbot::ProgramMsgBatch& batch) {
  if (batch.empty()) return true;
  for (const auto& [operator_id, operator_batch] : batch) {
    (void)operator_id;
    for (const auto& [port, messages] : operator_batch) {
      (void)port;
      if (!messages.empty()) return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// JNI Helpers (pattern from rtbot/libs/wrappers/java/rtbot_jni.cpp)
// ---------------------------------------------------------------------------

void throw_runtime_exception(JNIEnv* env, const std::string& msg) {
  jclass cls = env->FindClass("java/lang/RuntimeException");
  if (cls) env->ThrowNew(cls, msg.c_str());
}

std::string jstring_to_std(JNIEnv* env, jstring s) {
  if (!s) return "";
  const char* chars = env->GetStringUTFChars(s, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(s, chars);
  return result;
}

jstring std_to_jstring(JNIEnv* env, const std::string& s) {
  return env->NewStringUTF(s.c_str());
}

// ---------------------------------------------------------------------------
// JSON deserialization for CatalogSnapshot (input)
// (from runtimes/browser/bindings.cpp)
// ---------------------------------------------------------------------------

ColumnDef column_def_from_json(const json& j) {
  ColumnType col_type = ColumnType::DOUBLE;
  if (j.contains("type") && j["type"].is_string()) {
    if (j["type"] == "TEXT") col_type = ColumnType::TEXT;
  }
  return {j.at("name").get<std::string>(), j.at("index").get<int>(), col_type};
}

StreamSchema stream_schema_from_json(const json& j) {
  StreamSchema s;
  s.name = j.at("name").get<std::string>();
  for (const auto& col : j.at("columns")) {
    s.columns.push_back(column_def_from_json(col));
  }
  return s;
}

ViewMeta view_meta_from_json(const json& j) {
  ViewMeta v;
  v.name = j.at("name").get<std::string>();

  auto et = j.at("entity_type").get<std::string>();
  if (et == "STREAM")
    v.entity_type = EntityType::STREAM;
  else if (et == "VIEW")
    v.entity_type = EntityType::VIEW;
  else if (et == "MATERIALIZED_VIEW")
    v.entity_type = EntityType::MATERIALIZED_VIEW;
  else
    v.entity_type = EntityType::TABLE;

  auto vt = j.at("view_type").get<std::string>();
  if (vt == "KEYED")
    v.view_type = ViewType::KEYED;
  else if (vt == "TOPK")
    v.view_type = ViewType::TOPK;
  else
    v.view_type = ViewType::SCALAR;

  v.field_map = j.at("field_map").get<std::map<std::string, int>>();
  v.source_streams =
      j.at("source_streams").get<std::vector<std::string>>();
  v.program_json = j.value("program_json", "");
  v.output_stream = j.value("output_stream", "");
  v.per_key_prefix = j.value("per_key_prefix", "");
  v.known_keys = j.value("known_keys", std::vector<double>{});
  v.key_index = j.value("key_index", -1);
  return v;
}

TableSchema table_schema_from_json(const json& j) {
  TableSchema t;
  t.name = j.at("name").get<std::string>();
  for (const auto& col : j.at("columns")) {
    t.columns.push_back(column_def_from_json(col));
  }
  t.changelog_stream = j.value("changelog_stream", "");
  t.key_columns = j.value("key_columns", std::vector<int>{});
  return t;
}

CatalogSnapshot catalog_from_json(const std::string& catalog_json) {
  CatalogSnapshot snap;
  if (catalog_json.empty()) return snap;

  auto j = json::parse(catalog_json);

  if (j.contains("streams")) {
    for (const auto& [name, val] : j["streams"].items()) {
      snap.streams[name] = stream_schema_from_json(val);
    }
  }
  if (j.contains("views")) {
    for (const auto& [name, val] : j["views"].items()) {
      snap.views[name] = view_meta_from_json(val);
    }
  }
  if (j.contains("tables")) {
    for (const auto& [name, val] : j["tables"].items()) {
      snap.tables[name] = table_schema_from_json(val);
    }
  }
  if (j.contains("dictionaries") && j["dictionaries"].is_object()) {
    for (auto& [key, entries] : j["dictionaries"].items()) {
      std::map<double, std::string> dict_entries;
      for (auto& [id_str, str_val] : entries.items()) {
        dict_entries[std::stod(id_str)] = str_val.get<std::string>();
      }
      snap.dictionaries[key] = dict_entries;
    }
  }
  return snap;
}

// ---------------------------------------------------------------------------
// JSON serialization for CompilationResult (output)
// (from runtimes/browser/bindings.cpp, extended with select_limit and
//  delete_payload)
// ---------------------------------------------------------------------------

std::string statement_type_str(StatementType t) {
  switch (t) {
    case StatementType::CREATE_STREAM:
      return "CREATE_STREAM";
    case StatementType::CREATE_VIEW:
      return "CREATE_VIEW";
    case StatementType::CREATE_MATERIALIZED_VIEW:
      return "CREATE_MATERIALIZED_VIEW";
    case StatementType::CREATE_TABLE:
      return "CREATE_TABLE";
    case StatementType::INSERT:
      return "INSERT";
    case StatementType::SELECT:
      return "SELECT";
    case StatementType::SUBSCRIBE:
      return "SUBSCRIBE";
    case StatementType::DROP:
      return "DROP";
    case StatementType::DELETE:
      return "DELETE";
  }
  return "UNKNOWN";
}

std::string view_type_str(ViewType t) {
  switch (t) {
    case ViewType::SCALAR:
      return "SCALAR";
    case ViewType::KEYED:
      return "KEYED";
    case ViewType::TOPK:
      return "TOPK";
  }
  return "UNKNOWN";
}

std::string entity_type_str(EntityType t) {
  switch (t) {
    case EntityType::STREAM:
      return "STREAM";
    case EntityType::VIEW:
      return "VIEW";
    case EntityType::MATERIALIZED_VIEW:
      return "MATERIALIZED_VIEW";
    case EntityType::TABLE:
      return "TABLE";
  }
  return "UNKNOWN";
}

std::string select_tier_str(SelectTier t) {
  switch (t) {
    case SelectTier::TIER1_READ:
      return "TIER1_READ";
    case SelectTier::TIER2_SCAN:
      return "TIER2_SCAN";
    case SelectTier::TIER3_EPHEMERAL:
      return "TIER3_EPHEMERAL";
  }
  return "UNKNOWN";
}

json result_to_json(const CompilationResult& r) {
  json j;

  // Errors
  json errs = json::array();
  for (const auto& e : r.errors) {
    errs.push_back({{"message", e.message},
                    {"line", e.line},
                    {"column", e.column},
                    {"end_line", e.end_line},
                    {"end_column", e.end_column}});
  }
  j["errors"] = errs;

  if (r.has_errors()) return j;

  j["statement_type"] = statement_type_str(r.statement_type);
  j["entity_name"] = r.entity_name;
  j["program_json"] = r.program_json;
  j["field_map"] = r.field_map;
  j["source_streams"] = r.source_streams;
  j["view_type"] = view_type_str(r.view_type);
  j["key_index"] = r.key_index;
  j["select_tier"] = select_tier_str(r.select_tier);
  j["select_limit"] = r.select_limit;
  j["insert_payload"] = r.insert_payload;
  j["delete_payload"] = r.delete_payload;

  // stream_schema
  json schema;
  schema["name"] = r.stream_schema.name;
  json cols = json::array();
  for (const auto& c : r.stream_schema.columns) {
    cols.push_back({{"name", c.name}, {"index", c.index},
                    {"type", (c.type == ColumnType::TEXT) ? "TEXT" : "DOUBLE"}});
  }
  schema["columns"] = cols;
  j["stream_schema"] = schema;

  // table_schema
  json tschema;
  tschema["name"] = r.table_schema.name;
  json tcols = json::array();
  for (const auto& c : r.table_schema.columns) {
    tcols.push_back({{"name", c.name}, {"index", c.index},
                     {"type", (c.type == ColumnType::TEXT) ? "TEXT" : "DOUBLE"}});
  }
  tschema["columns"] = tcols;
  tschema["changelog_stream"] = r.table_schema.changelog_stream;
  tschema["key_columns"] = r.table_schema.key_columns;
  j["table_schema"] = tschema;

  // drop info
  j["drop_entity_name"] = r.drop_entity_name;
  j["drop_entity_type"] = entity_type_str(r.drop_entity_type);

  // dictionary_updates
  if (!r.dictionary_updates.empty()) {
    json dict_json = json::object();
    for (const auto& [key, entries] : r.dictionary_updates) {
      json entry_json = json::object();
      for (const auto& [id, str] : entries) {
        entry_json[std::to_string(static_cast<int>(id))] = str;
      }
      dict_json[key] = entry_json;
    }
    j["dictionary_updates"] = dict_json;
  }

  return j;
}

// ---------------------------------------------------------------------------
// Pipeline output serialization — JSON. Iteration + sort come from the
// shared rtbot_sql::api::decode_program_batch; this function only
// renders the decoded messages as JSON for the legacy text-output JNI
// entrypoints.
// ---------------------------------------------------------------------------

std::string batch_to_json(const rtbot::ProgramMsgBatch& batch) {
  auto decoded = api::decode_program_batch(batch);
  json arr = json::array();
  for (const auto& m : decoded) {
    arr.push_back({{"timestamp", static_cast<std::uint64_t>(m.timestamp)},
                   {"values", m.values},
                   {"operator_id", m.operator_id},
                   {"port", m.port}});
  }
  return arr.dump();
}

}  // namespace

// ---------------------------------------------------------------------------
// JNI exports — Java class: dev.rtbot.sql.RtBotSqlCompiler
// ---------------------------------------------------------------------------

extern "C" {

// ---- SQL compilation ----

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_preprocessSqlJson(JNIEnv* env, jclass,
                                                       jstring sql,
                                                       jlong tsUnitsPerSecond) {
  try {
    auto result = api::preprocess_sql(jstring_to_std(env, sql),
                                      static_cast<int64_t>(tsUnitsPerSecond));
    json j;
    json stmts = json::array();
    for (const auto& s : result.statements) {
      stmts.push_back(s);
    }
    j["statements"] = stmts;
    j["new_ts_units_per_second"] = result.new_ts_units_per_second;
    return std_to_jstring(env, j.dump());
  } catch (const std::exception& e) {
    json err;
    err["error"] = e.what();
    return std_to_jstring(env, err.dump());
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_compileSqlJson(JNIEnv* env, jclass,
                                                     jstring sql,
                                                     jstring catalogJson,
                                                     jlong tsUnitsPerSecond) {
  try {
    auto catalog = catalog_from_json(jstring_to_std(env, catalogJson));
    auto expanded = api::compile_sql_expanded(
        jstring_to_std(env, sql), catalog,
        static_cast<int64_t>(tsUnitsPerSecond));
    json j;
    json results_arr = json::array();
    for (const auto& r : expanded.results) {
      results_arr.push_back(result_to_json(r));
    }
    j["results"] = results_arr;
    j["new_ts_units_per_second"] = expanded.new_ts_units_per_second;
    return std_to_jstring(env, j.dump());
  } catch (const std::exception& e) {
    json j;
    json results_arr = json::array();
    results_arr.push_back(
        {{"errors", {{{"message", e.what()}, {"line", -1}, {"column", -1}}}}});
    j["results"] = results_arr;
    j["new_ts_units_per_second"] = -1;
    return std_to_jstring(env, j.dump());
  }
}

// Compile all views in a catalog into one consolidated rtbot Program.
// Returns JSON { program_json, view_terminals, view_terminal_ports,
//                materialized_views, base_stream_inputs, base_stream_ports,
//                errors }.
// Errors are returned in the "errors" array; callers should check before
// deploying.
JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_compileSessionJson(JNIEnv* env, jclass,
                                                        jstring catalogJson) {
  try {
    auto catalog = catalog_from_json(jstring_to_std(env, catalogJson));
    auto result = api::compile_session_program(catalog);
    json j;
    j["program_json"] = result.program_json;
    j["view_terminals"] = result.view_terminals;
    j["view_terminal_ports"] = result.view_terminal_ports;
    j["materialized_views"] = result.materialized_views;
    j["base_stream_inputs"] = result.base_stream_inputs;
    j["base_stream_ports"] = result.base_stream_ports;
    json errs = json::array();
    for (const auto& e : result.errors) {
      errs.push_back(
          {{"message", e.message}, {"line", e.line}, {"column", e.column}});
    }
    j["errors"] = errs;
    return std_to_jstring(env, j.dump());
  } catch (const std::exception& e) {
    json j;
    j["errors"] = json::array();
    j["errors"].push_back(
        {{"message", e.what()}, {"line", -1}, {"column", -1}});
    return std_to_jstring(env, j.dump());
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_validateSql(JNIEnv* env, jclass,
                                                 jstring sql) {
  try {
    auto parse_result = parser::parse(jstring_to_std(env, sql));
    json j;
    j["valid"] = parse_result.ok();
    json errs = json::array();
    for (const auto& e : parse_result.errors) {
      errs.push_back({{"message", e.message},
                      {"line", e.loc.line},
                      {"column", e.loc.column},
                      {"end_line", e.loc.end_line},
                      {"end_column", e.loc.end_column}});
    }
    j["errors"] = errs;
    parser::free_result(parse_result);
    return std_to_jstring(env, j.dump());
  } catch (const std::exception& e) {
    json j;
    j["valid"] = false;
    j["errors"] = {{{"message", e.what()}, {"line", -1}, {"column", -1}}};
    return std_to_jstring(env, j.dump());
  }
}

// ---- Pipeline execution ----

JNIEXPORT jlong JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_createPipeline(JNIEnv* env, jclass,
                                                    jstring programJson) {
  try {
    std::string program_json_str = jstring_to_std(env, programJson);
    auto* program = new rtbot::Program(program_json_str);
    std::string head = program_json_str.substr(0, std::min<size_t>(
        program_json_str.size(), 120));
    for (auto& c : head) {
      if (c == '\n' || c == '\r') c = ' ';
    }
    std::cerr << "[jit-probe] createPipeline using_jit="
              << (program->using_jit() ? 1 : 0)
              << " bytes=" << program_json_str.size()
              << " head='" << head << "'\n";
    if (!program->using_jit()) {
      try {
        rtbot::jit::JitCompiler compiler;
        auto probe = compiler.compile(program_json_str);
        (void)probe;
        std::cerr << "[jit-probe] direct compile() succeeded — "
                     "Program::using_jit_ false for unknown reason\n";
      } catch (const std::exception& e) {
        std::cerr << "[jit-probe] direct compile() failed: "
                  << e.what() << "\n";
      } catch (...) {
        std::cerr << "[jit-probe] direct compile() failed: unknown\n";
      }
    }
    return reinterpret_cast<jlong>(program);
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("createPipeline: ") + e.what());
    return 0;
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_feedPipeline(JNIEnv* env, jclass,
                                                  jlong handle,
                                                  jlong timestamp,
                                                  jdoubleArray values,
                                                  jstring port) {
  try {
    const bool profile =
        g_native_feed_stats_enabled.load(std::memory_order_relaxed);
    const std::uint64_t t0 = profile ? now_ns() : 0;
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "feedPipeline: null pipeline handle");
      return nullptr;
    }

    // Convert jdoubleArray to std::vector<double>
    jsize len = env->GetArrayLength(values);
    jdouble* elems = env->GetDoubleArrayElements(values, nullptr);
    std::vector<double> values_vec(elems, elems + len);
    env->ReleaseDoubleArrayElements(values, elems, JNI_ABORT);
    const std::uint64_t t_after_values = profile ? now_ns() : 0;

    std::string port_string = jstring_to_std(env, port);
    const std::uint64_t t_after_port = profile ? now_ns() : 0;

    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values_vec});
    const std::uint64_t t_after_msg = profile ? now_ns() : 0;

    auto batch = program->receive(std::move(msg), port_string);
    const std::uint64_t t_after_receive = profile ? now_ns() : 0;
    if (is_batch_empty(batch)) {
      if (profile) {
        const std::uint64_t t_end = now_ns();
        g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
        g_native_feed_stats.input_values.fetch_add(
            static_cast<std::uint64_t>(len), std::memory_order_relaxed);
        g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                               std::memory_order_relaxed);
        g_native_feed_stats.values_copy_ns.fetch_add(
            t_after_values - t0, std::memory_order_relaxed);
        g_native_feed_stats.port_decode_ns.fetch_add(
            t_after_port - t_after_values, std::memory_order_relaxed);
        g_native_feed_stats.message_build_ns.fetch_add(
            t_after_msg - t_after_port, std::memory_order_relaxed);
        g_native_feed_stats.receive_ns.fetch_add(
            t_after_receive - t_after_msg, std::memory_order_relaxed);
      }
      return nullptr;
    }

    std::string json_out = batch_to_json(batch);
    const std::uint64_t t_after_json = profile ? now_ns() : 0;
    jstring out = std_to_jstring(env, json_out);
    if (profile) {
      const std::uint64_t t_end = now_ns();
      BatchStats batch_stats = count_batch_stats(batch);
      g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
      g_native_feed_stats.input_values.fetch_add(
          static_cast<std::uint64_t>(len), std::memory_order_relaxed);
      g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                             std::memory_order_relaxed);
      g_native_feed_stats.values_copy_ns.fetch_add(
          t_after_values - t0, std::memory_order_relaxed);
      g_native_feed_stats.port_decode_ns.fetch_add(
          t_after_port - t_after_values, std::memory_order_relaxed);
      g_native_feed_stats.message_build_ns.fetch_add(
          t_after_msg - t_after_port, std::memory_order_relaxed);
      g_native_feed_stats.receive_ns.fetch_add(
          t_after_receive - t_after_msg, std::memory_order_relaxed);
      g_native_feed_stats.json_serialize_ns.fetch_add(
          t_after_json - t_after_receive, std::memory_order_relaxed);
      g_native_feed_stats.jstring_ns.fetch_add(
          t_end - t_after_json, std::memory_order_relaxed);
      g_native_feed_stats.output_messages.fetch_add(
          batch_stats.messages, std::memory_order_relaxed);
      g_native_feed_stats.output_values.fetch_add(
          batch_stats.values, std::memory_order_relaxed);
    }

    return out;
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("feedPipeline: ") + e.what());
    return nullptr;
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_feedPipeline3(JNIEnv* env, jclass,
                                                   jlong handle,
                                                   jlong timestamp,
                                                   jdouble v0,
                                                   jdouble v1,
                                                   jdouble v2,
                                                   jstring port) {
  try {
    const bool profile =
        g_native_feed_stats_enabled.load(std::memory_order_relaxed);
    const std::uint64_t t0 = profile ? now_ns() : 0;
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "feedPipeline3: null pipeline handle");
      return nullptr;
    }

    std::vector<double> values_vec;
    values_vec.reserve(3);
    values_vec.push_back(static_cast<double>(v0));
    values_vec.push_back(static_cast<double>(v1));
    values_vec.push_back(static_cast<double>(v2));
    const std::uint64_t t_after_values = profile ? now_ns() : 0;

    std::string port_string = jstring_to_std(env, port);
    const std::uint64_t t_after_port = profile ? now_ns() : 0;

    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values_vec});
    const std::uint64_t t_after_msg = profile ? now_ns() : 0;

    auto batch = program->receive(std::move(msg), port_string);
    const std::uint64_t t_after_receive = profile ? now_ns() : 0;
    if (is_batch_empty(batch)) {
      if (profile) {
        const std::uint64_t t_end = now_ns();
        g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
        g_native_feed_stats.input_values.fetch_add(
            3, std::memory_order_relaxed);
        g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                               std::memory_order_relaxed);
        g_native_feed_stats.values_copy_ns.fetch_add(
            t_after_values - t0, std::memory_order_relaxed);
        g_native_feed_stats.port_decode_ns.fetch_add(
            t_after_port - t_after_values, std::memory_order_relaxed);
        g_native_feed_stats.message_build_ns.fetch_add(
            t_after_msg - t_after_port, std::memory_order_relaxed);
        g_native_feed_stats.receive_ns.fetch_add(
            t_after_receive - t_after_msg, std::memory_order_relaxed);
      }
      return nullptr;
    }

    std::string json_out = batch_to_json(batch);
    const std::uint64_t t_after_json = profile ? now_ns() : 0;
    jstring out = std_to_jstring(env, json_out);
    if (profile) {
      const std::uint64_t t_end = now_ns();
      BatchStats batch_stats = count_batch_stats(batch);
      g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
      g_native_feed_stats.input_values.fetch_add(
          3, std::memory_order_relaxed);
      g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                             std::memory_order_relaxed);
      g_native_feed_stats.values_copy_ns.fetch_add(
          t_after_values - t0, std::memory_order_relaxed);
      g_native_feed_stats.port_decode_ns.fetch_add(
          t_after_port - t_after_values, std::memory_order_relaxed);
      g_native_feed_stats.message_build_ns.fetch_add(
          t_after_msg - t_after_port, std::memory_order_relaxed);
      g_native_feed_stats.receive_ns.fetch_add(
          t_after_receive - t_after_msg, std::memory_order_relaxed);
      g_native_feed_stats.json_serialize_ns.fetch_add(
          t_after_json - t_after_receive, std::memory_order_relaxed);
      g_native_feed_stats.jstring_ns.fetch_add(
          t_end - t_after_json, std::memory_order_relaxed);
      g_native_feed_stats.output_messages.fetch_add(
          batch_stats.messages, std::memory_order_relaxed);
      g_native_feed_stats.output_values.fetch_add(
          batch_stats.values, std::memory_order_relaxed);
    }

    return out;
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("feedPipeline3: ") + e.what());
    return nullptr;
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_feedPipeline3I1(JNIEnv* env, jclass,
                                                     jlong handle,
                                                     jlong timestamp,
                                                     jdouble v0,
                                                     jdouble v1,
                                                     jdouble v2) {
  try {
    const bool profile =
        g_native_feed_stats_enabled.load(std::memory_order_relaxed);
    const std::uint64_t t0 = profile ? now_ns() : 0;
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "feedPipeline3I1: null pipeline handle");
      return nullptr;
    }

    std::vector<double> values_vec;
    values_vec.reserve(3);
    values_vec.push_back(static_cast<double>(v0));
    values_vec.push_back(static_cast<double>(v1));
    values_vec.push_back(static_cast<double>(v2));
    const std::uint64_t t_after_values = profile ? now_ns() : 0;

    const std::string port_string = "i1";
    const std::uint64_t t_after_port = profile ? now_ns() : 0;

    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values_vec});
    const std::uint64_t t_after_msg = profile ? now_ns() : 0;

    auto batch = program->receive(std::move(msg), port_string);
    const std::uint64_t t_after_receive = profile ? now_ns() : 0;
    if (is_batch_empty(batch)) {
      if (profile) {
        const std::uint64_t t_end = now_ns();
        g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
        g_native_feed_stats.input_values.fetch_add(
            3, std::memory_order_relaxed);
        g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                               std::memory_order_relaxed);
        g_native_feed_stats.values_copy_ns.fetch_add(
            t_after_values - t0, std::memory_order_relaxed);
        g_native_feed_stats.port_decode_ns.fetch_add(
            t_after_port - t_after_values, std::memory_order_relaxed);
        g_native_feed_stats.message_build_ns.fetch_add(
            t_after_msg - t_after_port, std::memory_order_relaxed);
        g_native_feed_stats.receive_ns.fetch_add(
            t_after_receive - t_after_msg, std::memory_order_relaxed);
      }
      return nullptr;
    }

    std::string json_out = batch_to_json(batch);
    const std::uint64_t t_after_json = profile ? now_ns() : 0;
    jstring out = std_to_jstring(env, json_out);
    if (profile) {
      const std::uint64_t t_end = now_ns();
      BatchStats batch_stats = count_batch_stats(batch);
      g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
      g_native_feed_stats.input_values.fetch_add(
          3, std::memory_order_relaxed);
      g_native_feed_stats.total_ns.fetch_add(t_end - t0,
                                             std::memory_order_relaxed);
      g_native_feed_stats.values_copy_ns.fetch_add(
          t_after_values - t0, std::memory_order_relaxed);
      g_native_feed_stats.port_decode_ns.fetch_add(
          t_after_port - t_after_values, std::memory_order_relaxed);
      g_native_feed_stats.message_build_ns.fetch_add(
          t_after_msg - t_after_port, std::memory_order_relaxed);
      g_native_feed_stats.receive_ns.fetch_add(
          t_after_receive - t_after_msg, std::memory_order_relaxed);
      g_native_feed_stats.json_serialize_ns.fetch_add(
          t_after_json - t_after_receive, std::memory_order_relaxed);
      g_native_feed_stats.jstring_ns.fetch_add(
          t_end - t_after_json, std::memory_order_relaxed);
      g_native_feed_stats.output_messages.fetch_add(
          batch_stats.messages, std::memory_order_relaxed);
      g_native_feed_stats.output_values.fetch_add(
          batch_stats.values, std::memory_order_relaxed);
    }
    return out;
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("feedPipeline3I1: ") + e.what());
    return nullptr;
  }
}

// Register the ordered set of output operator ids for a consolidated
// session pipeline. Must be called after createPipeline and before
// feedPipelineBufferSessionI1. The ordering is caller-defined; outputs
// emitted by the native side reference this registration by index, and
// the Java caller uses the same index to demux.
JNIEXPORT void JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_registerSessionOutputs(JNIEnv* env,
                                                            jclass,
                                                            jlong handle,
                                                            jobjectArray opIds) {
  try {
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "registerSessionOutputs: null handle");
      return;
    }
    if (!opIds) {
      throw_runtime_exception(env, "registerSessionOutputs: null opIds");
      return;
    }
    jsize n = env->GetArrayLength(opIds);
    std::vector<std::string> ids;
    ids.reserve(static_cast<size_t>(n));
    for (jsize i = 0; i < n; ++i) {
      auto js = static_cast<jstring>(env->GetObjectArrayElement(opIds, i));
      ids.push_back(jstring_to_std(env, js));
    }
    api::SessionOutputRegistry::instance().register_outputs(program,
                                                             std::move(ids));
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("registerSessionOutputs: ") + e.what());
  }
}

// Consolidated-session buffered feed. Writes outputs in a compact binary
// frame into a caller-supplied direct ByteBuffer. Frame layout (all
// little-endian / native order; caller must set ByteOrder.nativeOrder()):
//
//   int32 num_outputs
//   per output (variable):
//     int32 op_index         // index into the registered op-ids array
//     int32 num_values
//     int64 timestamp
//     float64 values[num_values]
//
// Return value:
//   >= 0: bytes written into the buffer
//     -1: buffer is not a direct buffer
//   < -1: required capacity in bytes (negated) — caller must grow and retry
JNIEXPORT jint JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_feedPipelineBufferSession(
    JNIEnv* env, jclass,
    jlong handle,
    jstring port,
    jlongArray timestamps,
    jobjectArray columns,
    jobject outBuffer) {
  try {
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "feedPipelineBufferSession: null handle");
      return 0;
    }
    const auto* session_map =
        api::SessionOutputRegistry::instance().lookup(program);
    if (!session_map) {
      throw_runtime_exception(env,
          "feedPipelineBufferSession: registerSessionOutputs not called");
      return 0;
    }
    if (!timestamps || !columns) {
      throw_runtime_exception(env,
          "feedPipelineBufferSession: null arrays");
      return 0;
    }
    void* buf_addr = env->GetDirectBufferAddress(outBuffer);
    jlong buf_capacity = env->GetDirectBufferCapacity(outBuffer);
    if (!buf_addr || buf_capacity < 0) {
      return -1;  // signal: not a direct buffer
    }

    std::string port_str = port ? jstring_to_std(env, port) : std::string{"i1"};

    const jsize n = env->GetArrayLength(timestamps);
    const jsize num_cols = env->GetArrayLength(columns);

    if (num_cols > 64) {
      throw_runtime_exception(env,
          "feedPipelineBufferSession: num_cols > 64");
      return 0;
    }

    std::vector<jdoubleArray> col_refs(num_cols);
    for (jsize c = 0; c < num_cols; ++c) {
      col_refs[c] = static_cast<jdoubleArray>(
          env->GetObjectArrayElement(columns, c));
      if (!col_refs[c] || env->GetArrayLength(col_refs[c]) != n) {
        throw_runtime_exception(env,
            "feedPipelineBufferSession: column array null or length mismatch");
        return 0;
      }
    }

    std::vector<jdouble*> col_elems(num_cols);
    for (jsize c = 0; c < num_cols; ++c) {
      col_elems[c] = static_cast<jdouble*>(
          env->GetPrimitiveArrayCritical(col_refs[c], nullptr));
    }

    jlong* ts_elems = static_cast<jlong*>(
        env->GetPrimitiveArrayCritical(timestamps, nullptr));

    static_assert(sizeof(jlong) == sizeof(rtbot::timestamp_t),
                  "jlong vs timestamp_t mismatch");
    auto* times = reinterpret_cast<const rtbot::timestamp_t*>(ts_elems);

    const bool was_jit = program->using_jit();
    rtbot::ProgramMsgBatch interp_batch;
    if (was_jit) {
      if (num_cols > 1) {
        double row[64];
        const std::size_t lane_width = program->jit_program()
            ? program->jit_program()->input_lane_width()
            : static_cast<std::size_t>(num_cols);
        for (jsize r = 0; r < n; ++r) {
          for (jsize c = 0; c < num_cols; ++c) row[c] = col_elems[c][r];
          program->send_vector(times[r], row, lane_width);
        }
      } else {
        for (jsize r = 0; r < n; ++r) {
          program->send(times[r], col_elems[0][r]);
        }
      }
    } else {
      std::vector<double> rows(static_cast<std::size_t>(n) *
                                static_cast<std::size_t>(num_cols));
      for (jsize i = 0; i < n; ++i) {
        for (jsize c = 0; c < num_cols; ++c) {
          rows[static_cast<std::size_t>(i) * num_cols + c] = col_elems[c][i];
        }
      }
      interp_batch = program->receive_buffer(
          port_str, rows.data(), static_cast<std::size_t>(n),
          static_cast<std::size_t>(num_cols), times);
    }

    env->ReleasePrimitiveArrayCritical(timestamps, ts_elems, JNI_ABORT);
    for (jsize c = 0; c < num_cols; ++c) {
      env->ReleasePrimitiveArrayCritical(col_refs[c], col_elems[c], JNI_ABORT);
    }

    // Single-pass speculative write into the direct buffer. Track overflow
    // separately so the caller can grow and retry. On the JIT path we drain
    // emit_buf_ destructively here; on overflow we already lost the data,
    // so the caller's contract becomes "grow on first call" — the retry
    // path re-drives the input rows into the JIT/interpreter via a second
    // call. To preserve the original API (caller-grow-and-retry without
    // re-supplying input), we use a non-destructive peek when overflow
    // would occur, which means we walk twice in the rare overflow case
    // and once in the steady state.
    char* const buf_begin = static_cast<char*>(buf_addr);
    char* const buf_end   = buf_begin + buf_capacity;
    char* p = buf_begin;

    if (static_cast<jlong>(sizeof(int32_t)) > buf_capacity) {
      // Buffer can't even hold the header. Switch to a destructive drain
      // followed by a size estimate via the interpreter side; for JIT the
      // best we can do without re-driving inputs is return a generous
      // estimate.
      if (was_jit) program->jit_program()->discard_emitted();
      return -static_cast<jint>(sizeof(int32_t) * 2);
    }
    p += sizeof(int32_t);  // reserve count slot

    int32_t kept = 0;
    bool overflowed = false;
    std::size_t overflow_required = sizeof(int32_t);

    auto emit_rec = [&](int64_t ts, int32_t opIdx, const double* vals,
                         std::size_t nvals) {
      const std::size_t need = sizeof(int32_t) + sizeof(int32_t) +
                                sizeof(int64_t) + nvals * sizeof(double);
      if (overflowed || (p + need) > buf_end) {
        if (!overflowed) {
          overflow_required = static_cast<std::size_t>(p - buf_begin);
          overflowed = true;
        }
        overflow_required += need;
        return;
      }
      const int32_t nv32 = static_cast<int32_t>(nvals);
      std::memcpy(p, &opIdx, sizeof(int32_t));     p += sizeof(int32_t);
      std::memcpy(p, &nv32,  sizeof(int32_t));     p += sizeof(int32_t);
      std::memcpy(p, &ts,    sizeof(int64_t));     p += sizeof(int64_t);
      if (nvals > 0) {
        std::memcpy(p, vals, nvals * sizeof(double));
        p += nvals * sizeof(double);
      }
      ++kept;
    };

    if (was_jit) {
      // Use a non-destructive peek so emit_buf_ survives an overflow and
      // the caller's grow-and-retry can re-drain it. In the steady state
      // (buffer large enough) this is a single walk of emit_buf_; on
      // success we explicitly discard.
      program->drain_records(
          [&](std::int64_t time, const std::string& op_id,
              const double* values, std::size_t n_values) {
            auto it = session_map->id_to_idx.find(op_id);
            if (it == session_map->id_to_idx.end()) return;
            emit_rec(static_cast<int64_t>(time), it->second, values, n_values);
          },
          /*consume=*/false);
    } else {
      for (const auto& [op_id, op_batch] : interp_batch) {
        auto it = session_map->id_to_idx.find(op_id);
        if (it == session_map->id_to_idx.end()) continue;
        const int32_t opIdx = it->second;
        for (const auto& [port_name, messages] : op_batch) {
          (void)port_name;
          for (const auto& message : messages) {
            if (auto* vm = dynamic_cast<const rtbot::Message<rtbot::VectorNumberData>*>(
                    message.get())) {
              const auto& vals = vm->data.values ? *vm->data.values
                                                  : std::vector<double>{};
              emit_rec(static_cast<int64_t>(vm->time), opIdx, vals.data(),
                       vals.size());
              continue;
            }
            if (auto* nm = dynamic_cast<const rtbot::Message<rtbot::NumberData>*>(
                    message.get())) {
              const double v = nm->data.value;
              emit_rec(static_cast<int64_t>(nm->time), opIdx, &v, 1);
            }
          }
        }
      }
    }

    if (overflowed) {
      // emit_buf_ is intact (non-destructive peek); caller grows and retries.
      return -static_cast<jint>(overflow_required);
    }

    if (was_jit) {
      // Steady-state success: drop the now-serialized records.
      program->jit_program()->discard_emitted();
    }
    std::memcpy(buf_begin, &kept, sizeof(int32_t));
    return static_cast<jint>(p - buf_begin);
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("feedPipelineBufferSession: ") + e.what());
    return 0;
  }
}

JNIEXPORT void JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_resetNativeFeedStats(JNIEnv* env,
                                                          jclass) {
  try {
    reset_native_feed_stats();
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("resetNativeFeedStats: ") + e.what());
  }
}

JNIEXPORT void JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_setNativeFeedStatsEnabled(JNIEnv* env,
                                                               jclass,
                                                               jboolean enabled) {
  try {
    g_native_feed_stats_enabled.store(enabled == JNI_TRUE,
                                      std::memory_order_relaxed);
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("setNativeFeedStatsEnabled: ") + e.what());
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_getNativeFeedStatsJson(JNIEnv* env,
                                                            jclass) {
  try {
    return std_to_jstring(env, native_feed_stats_to_json().dump());
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("getNativeFeedStatsJson: ") + e.what());
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_destroyPipeline(JNIEnv* env, jclass,
                                                     jlong handle) {
  try {
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (program) api::SessionOutputRegistry::instance().clear(program);
    delete program;
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("destroyPipeline: ") + e.what());
  }
}

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_serializePipeline(JNIEnv* env, jclass,
                                                       jlong handle) {
  try {
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env,
                              "serializePipeline: null pipeline handle");
      return nullptr;
    }
    return std_to_jstring(env, program->serialize_data());
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("serializePipeline: ") + e.what());
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_restorePipeline(JNIEnv* env, jclass,
                                                     jlong handle,
                                                     jstring stateJson) {
  try {
    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env,
                              "restorePipeline: null pipeline handle");
      return;
    }
    program->restore_data_from_json(jstring_to_std(env, stateJson));
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("restorePipeline: ") + e.what());
  }
}

}  // extern "C"
