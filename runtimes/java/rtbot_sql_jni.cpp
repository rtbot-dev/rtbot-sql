#include <jni.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <string>
#include <tuple>
#include <vector>

#include <nlohmann/json.hpp>

#include "rtbot/Message.h"
#include "rtbot/Program.h"
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
    errs.push_back(
        {{"message", e.message}, {"line", e.line}, {"column", e.column}});
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
// Pipeline output serialization
// (pattern from runtimes/python/sql_bindings.cpp decode_batch)
// ---------------------------------------------------------------------------

std::string batch_to_json(const rtbot::ProgramMsgBatch& batch) {
  struct OutputMsg {
    std::uint64_t timestamp;
    std::vector<double> values;
    std::string operator_id;
    std::string port;
  };

  std::vector<OutputMsg> out;

  for (const auto& [operator_id, operator_batch] : batch) {
    for (const auto& [port, messages] : operator_batch) {
      for (const auto& message : messages) {
        if (auto* vec =
                dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
                    message.get())) {
          OutputMsg msg{};
          msg.timestamp = static_cast<std::uint64_t>(vec->time);
          if (vec->data.values) {
            msg.values = *vec->data.values;
          } else {
            msg.values.clear();
          }
          msg.operator_id = operator_id;
          msg.port = port;
          out.push_back(msg);
          continue;
        }

        if (auto* num =
                dynamic_cast<rtbot::Message<rtbot::NumberData>*>(
                    message.get())) {
          OutputMsg msg{};
          msg.timestamp = static_cast<std::uint64_t>(num->time);
          msg.values = {num->data.value};
          msg.operator_id = operator_id;
          msg.port = port;
          out.push_back(msg);
        }
      }
    }
  }

  std::stable_sort(out.begin(), out.end(),
                   [](const OutputMsg& a, const OutputMsg& b) {
                     return std::tie(a.timestamp, a.operator_id, a.port) <
                            std::tie(b.timestamp, b.operator_id, b.port);
                   });

  json arr = json::array();
  for (const auto& m : out) {
    arr.push_back({{"timestamp", m.timestamp},
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

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_validateSql(JNIEnv* env, jclass,
                                                 jstring sql) {
  try {
    auto parse_result = parser::parse(jstring_to_std(env, sql));
    json j;
    j["valid"] = parse_result.ok();
    json errs = json::array();
    for (const auto& e : parse_result.errors) {
      errs.push_back(
          {{"message", e}, {"line", -1}, {"column", -1}});
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
    auto* program =
        new rtbot::Program(jstring_to_std(env, programJson));
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

JNIEXPORT jstring JNICALL
Java_dev_rtbot_sql_RtBotSqlCompiler_feedPipeline3BatchI1(JNIEnv* env, jclass,
                                                          jlong handle,
                                                          jlongArray timestamps,
                                                          jdoubleArray v0,
                                                          jdoubleArray v1,
                                                          jdoubleArray v2) {
  try {
    const bool profile =
        g_native_feed_stats_enabled.load(std::memory_order_relaxed);
    const std::uint64_t t0 = profile ? now_ns() : 0;

    auto* program = reinterpret_cast<rtbot::Program*>(handle);
    if (!program) {
      throw_runtime_exception(env, "feedPipeline3BatchI1: null pipeline handle");
      return nullptr;
    }
    if (!timestamps || !v0 || !v1 || !v2) {
      throw_runtime_exception(env, "feedPipeline3BatchI1: null input array");
      return nullptr;
    }

    const jsize n = env->GetArrayLength(timestamps);
    if (env->GetArrayLength(v0) != n
        || env->GetArrayLength(v1) != n
        || env->GetArrayLength(v2) != n) {
      throw_runtime_exception(
          env, "feedPipeline3BatchI1: array length mismatch");
      return nullptr;
    }

    jlong* ts_elems = env->GetLongArrayElements(timestamps, nullptr);
    jdouble* v0_elems = env->GetDoubleArrayElements(v0, nullptr);
    jdouble* v1_elems = env->GetDoubleArrayElements(v1, nullptr);
    jdouble* v2_elems = env->GetDoubleArrayElements(v2, nullptr);
    const std::uint64_t t_after_values = profile ? now_ns() : 0;

    std::map<std::string, std::vector<std::unique_ptr<rtbot::BaseMessage>>> buffer;
    auto& port_messages = buffer["i1"];
    port_messages.reserve(static_cast<std::size_t>(n));
    for (jsize i = 0; i < n; ++i) {
      std::vector<double> values_vec;
      values_vec.reserve(3);
      values_vec.push_back(static_cast<double>(v0_elems[i]));
      values_vec.push_back(static_cast<double>(v1_elems[i]));
      values_vec.push_back(static_cast<double>(v2_elems[i]));
      port_messages.push_back(rtbot::create_message<rtbot::VectorNumberData>(
          static_cast<rtbot::timestamp_t>(ts_elems[i]),
          rtbot::VectorNumberData{std::move(values_vec)}));
    }
    const std::uint64_t t_after_msg = profile ? now_ns() : 0;

    env->ReleaseLongArrayElements(timestamps, ts_elems, JNI_ABORT);
    env->ReleaseDoubleArrayElements(v0, v0_elems, JNI_ABORT);
    env->ReleaseDoubleArrayElements(v1, v1_elems, JNI_ABORT);
    env->ReleaseDoubleArrayElements(v2, v2_elems, JNI_ABORT);

    auto batch = program->receive_batch(buffer);
    const std::uint64_t t_after_receive = profile ? now_ns() : 0;
    if (is_batch_empty(batch)) {
      if (profile) {
        const std::uint64_t t_end = now_ns();
        g_native_feed_stats.calls.fetch_add(1, std::memory_order_relaxed);
        g_native_feed_stats.input_values.fetch_add(
            static_cast<std::uint64_t>(n) * 3ULL, std::memory_order_relaxed);
        g_native_feed_stats.total_ns.fetch_add(
            t_end - t0, std::memory_order_relaxed);
        g_native_feed_stats.values_copy_ns.fetch_add(
            t_after_values - t0, std::memory_order_relaxed);
        g_native_feed_stats.message_build_ns.fetch_add(
            t_after_msg - t_after_values, std::memory_order_relaxed);
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
          static_cast<std::uint64_t>(n) * 3ULL, std::memory_order_relaxed);
      g_native_feed_stats.total_ns.fetch_add(
          t_end - t0, std::memory_order_relaxed);
      g_native_feed_stats.values_copy_ns.fetch_add(
          t_after_values - t0, std::memory_order_relaxed);
      g_native_feed_stats.message_build_ns.fetch_add(
          t_after_msg - t_after_values, std::memory_order_relaxed);
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
        env, std::string("feedPipeline3BatchI1: ") + e.what());
    return nullptr;
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
