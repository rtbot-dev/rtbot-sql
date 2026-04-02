#include <jni.h>

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

#include <nlohmann/json.hpp>

#include "rtbot/Message.h"
#include "rtbot/Program.h"
#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/parser.h"

using json = nlohmann::json;
using namespace rtbot_sql;

namespace {

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
  return {j.at("name").get<std::string>(), j.at("index").get<int>()};
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
    cols.push_back({{"name", c.name}, {"index", c.index}});
  }
  schema["columns"] = cols;
  j["stream_schema"] = schema;

  // table_schema
  json tschema;
  tschema["name"] = r.table_schema.name;
  json tcols = json::array();
  for (const auto& c : r.table_schema.columns) {
    tcols.push_back({{"name", c.name}, {"index", c.index}});
  }
  tschema["columns"] = tcols;
  tschema["changelog_stream"] = r.table_schema.changelog_stream;
  tschema["key_columns"] = r.table_schema.key_columns;
  j["table_schema"] = tschema;

  // drop info
  j["drop_entity_name"] = r.drop_entity_name;
  j["drop_entity_type"] = entity_type_str(r.drop_entity_type);

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
          out.push_back({static_cast<std::uint64_t>(vec->time),
                         vec->data.values,
                         operator_id,
                         port});
          continue;
        }

        if (auto* num =
                dynamic_cast<rtbot::Message<rtbot::NumberData>*>(
                    message.get())) {
          out.push_back({
              static_cast<std::uint64_t>(num->time),
              {num->data.value},
              operator_id,
              port,
          });
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
Java_dev_rtbot_sql_RtBotSqlCompiler_compileSqlJson(JNIEnv* env, jclass,
                                                    jstring sql,
                                                    jstring catalogJson) {
  try {
    auto catalog = catalog_from_json(jstring_to_std(env, catalogJson));
    auto result = api::compile_sql(jstring_to_std(env, sql), catalog);
    return std_to_jstring(env, result_to_json(result).dump());
  } catch (const std::exception& e) {
    json err;
    err["errors"] = {{{"message", e.what()}, {"line", -1}, {"column", -1}}};
    return std_to_jstring(env, err.dump());
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

    std::string port_string = jstring_to_std(env, port);

    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values_vec});

    auto batch = program->receive(std::move(msg), port_string);
    return std_to_jstring(env, batch_to_json(batch));
  } catch (const std::exception& e) {
    throw_runtime_exception(
        env, std::string("feedPipeline: ") + e.what());
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
