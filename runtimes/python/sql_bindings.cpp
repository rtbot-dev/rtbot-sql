#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "rtbot/Message.h"
#include "rtbot/Program.h"
#include "rtbot_sql/api/batch_decoder.h"
#include "rtbot_sql/api/compiler.h"
#include "rtbot_sql/api/preprocessor.h"
#include "rtbot_sql/api/types.h"
#include "rtbot_sql/parser/parser.h"

namespace py = pybind11;

namespace {

using rtbot_sql::CatalogSnapshot;
using rtbot_sql::ColumnDef;
using rtbot_sql::CompilationError;
using rtbot_sql::CompilationResult;
using rtbot_sql::EntityType;
using rtbot_sql::SelectTier;
using rtbot_sql::StatementType;
using rtbot_sql::StreamSchema;
using rtbot_sql::TableSchema;
using rtbot_sql::ViewMeta;
using rtbot_sql::ViewType;
using rtbot_sql::ColumnType;

struct RuntimeOutputMessage {
  std::uint64_t timestamp = 0;
  std::vector<double> values;
  std::string operator_id;
  std::string port;
};

// Thin adapter around the shared rtbot_sql::api::decode_program_batch.
// The shared helper handles iteration + stable sort; this function only
// copies the decoded fields into the pybind-exposed POJO shape.
std::vector<RuntimeOutputMessage> decode_batch(
    const rtbot::ProgramMsgBatch& batch) {
  auto decoded = rtbot_sql::api::decode_program_batch(batch);
  std::vector<RuntimeOutputMessage> out;
  out.reserve(decoded.size());
  for (auto& m : decoded) {
    RuntimeOutputMessage msg;
    msg.timestamp = static_cast<std::uint64_t>(m.timestamp);
    msg.values = std::move(m.values);
    msg.operator_id = std::move(m.operator_id);
    msg.port = std::move(m.port);
    out.push_back(std::move(msg));
  }
  return out;
}

// Tier-3 SELECT one-shot pipeline: construct-feed-discard. The long-lived
// per-view program has been replaced by the consolidated NativeSessionPipeline
// below, so the only caller is LocalPipelineRunner.run_once.
class NativePipeline {
 public:
  explicit NativePipeline(const std::string& program_json)
      : program_(program_json) {}

  std::vector<RuntimeOutputMessage> feed(
      std::uint64_t timestamp,
      const std::vector<double>& values,
      const std::string& port = "i1") {
    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values});
    return decode_batch(program_.receive(std::move(msg), port));
  }

 private:
  rtbot::Program program_;
};

// Consolidated-session pipeline — wraps rtbot::Program with the shared
// SessionOutputRegistry so the Python runtime can feed once and receive
// outputs that carry the op_id string (RuntimeOutputMessage); the Python
// side already maps op_id -> view name via compile_session's
// view_terminals map, so no extra index bookkeeping is needed.
class NativeSessionPipeline {
 public:
  explicit NativeSessionPipeline(const std::string& program_json)
      : program_(program_json) {}

  ~NativeSessionPipeline() {
    rtbot_sql::api::SessionOutputRegistry::instance().clear(&program_);
  }

  // Register the output operator ids that this session exposes. Matches
  // the Java runtime's registerSessionOutputs — here we only need it so
  // that the decoded output stream includes the right operator ids.
  // (Python doesn't need the index-keyed binary path because pybind
  // already returns structured RuntimeOutputMessage POJOs directly, no
  // JSON round-trip.)
  void register_outputs(const std::vector<std::string>& op_ids) {
    rtbot_sql::api::SessionOutputRegistry::instance().register_outputs(
        &program_, op_ids);
  }

  std::vector<RuntimeOutputMessage> feed_buffer(
      py::array_t<int64_t, py::array::c_style> timestamps,
      py::array_t<double, py::array::c_style> values,
      const std::string& port = "i1") {
    auto ts_info = timestamps.request();
    auto vals_info = values.request();
    if (ts_info.ndim != 1 || vals_info.ndim != 2) {
      throw std::invalid_argument(
          "timestamps must be 1D, values must be 2D");
    }
    if (ts_info.shape[0] != vals_info.shape[0]) {
      throw std::invalid_argument(
          "timestamps and values must have the same number of rows");
    }
    const auto nrows = static_cast<std::size_t>(ts_info.shape[0]);
    const auto ncols = static_cast<std::size_t>(vals_info.shape[1]);

    static_assert(sizeof(std::int64_t) == sizeof(rtbot::timestamp_t),
                  "int64 vs timestamp_t mismatch");
    const auto* times = reinterpret_cast<const rtbot::timestamp_t*>(ts_info.ptr);
    const auto* rows = static_cast<const double*>(vals_info.ptr);

    auto batch = program_.receive_buffer(port, rows, nrows, ncols, times);
    return decode_batch(batch);
  }

  std::vector<RuntimeOutputMessage> feed(
      std::uint64_t timestamp,
      const std::vector<double>& values,
      const std::string& port = "i1") {
    auto msg = rtbot::create_message<rtbot::VectorNumberData>(
        static_cast<rtbot::timestamp_t>(timestamp),
        rtbot::VectorNumberData{values});
    return decode_batch(program_.receive(std::move(msg), port));
  }

 private:
  rtbot::Program program_;
};

}  // namespace

PYBIND11_MODULE(_rtbot_sql_native, m) {
  m.doc() = "Native rtbot-sql Python bindings";

  py::enum_<ViewType>(m, "ViewType")
      .value("SCALAR", ViewType::SCALAR)
      .value("KEYED", ViewType::KEYED)
      .value("TOPK", ViewType::TOPK);

  py::enum_<EntityType>(m, "EntityType")
      .value("STREAM", EntityType::STREAM)
      .value("VIEW", EntityType::VIEW)
      .value("MATERIALIZED_VIEW", EntityType::MATERIALIZED_VIEW)
      .value("TABLE", EntityType::TABLE);

  py::enum_<StatementType>(m, "StatementType")
      .value("CREATE_STREAM", StatementType::CREATE_STREAM)
      .value("CREATE_VIEW", StatementType::CREATE_VIEW)
      .value("CREATE_MATERIALIZED_VIEW",
             StatementType::CREATE_MATERIALIZED_VIEW)
      .value("CREATE_TABLE", StatementType::CREATE_TABLE)
      .value("INSERT", StatementType::INSERT)
      .value("SELECT", StatementType::SELECT)
      .value("SUBSCRIBE", StatementType::SUBSCRIBE)
      .value("DROP", StatementType::DROP)
      .value("DELETE", StatementType::DELETE);

  py::enum_<SelectTier>(m, "SelectTier")
      .value("TIER1_READ", SelectTier::TIER1_READ)
      .value("TIER2_SCAN", SelectTier::TIER2_SCAN)
      .value("TIER3_EPHEMERAL", SelectTier::TIER3_EPHEMERAL);

  py::enum_<ColumnType>(m, "ColumnType")
      .value("DOUBLE", ColumnType::DOUBLE)
      .value("TEXT", ColumnType::TEXT);

  py::class_<ColumnDef>(m, "ColumnDef")
      .def(py::init<>())
      .def(py::init<std::string, int>(), py::arg("name"), py::arg("index"))
      .def(py::init<std::string, int, ColumnType>(),
           py::arg("name"), py::arg("index"), py::arg("type"))
      .def_readwrite("name", &ColumnDef::name)
      .def_readwrite("index", &ColumnDef::index)
      .def_readwrite("type", &ColumnDef::type);

  py::class_<StreamSchema>(m, "StreamSchema")
      .def(py::init<>())
      .def_readwrite("name", &StreamSchema::name)
      .def_readwrite("columns", &StreamSchema::columns);

  py::class_<ViewMeta>(m, "ViewMeta")
      .def(py::init<>())
      .def_readwrite("name", &ViewMeta::name)
      .def_readwrite("entity_type", &ViewMeta::entity_type)
      .def_readwrite("view_type", &ViewMeta::view_type)
      .def_readwrite("field_map", &ViewMeta::field_map)
      .def_readwrite("source_streams", &ViewMeta::source_streams)
      .def_readwrite("program_json", &ViewMeta::program_json)
      .def_readwrite("output_stream", &ViewMeta::output_stream)
      .def_readwrite("per_key_prefix", &ViewMeta::per_key_prefix)
      .def_readwrite("known_keys", &ViewMeta::known_keys)
      .def_readwrite("key_index", &ViewMeta::key_index);

  py::class_<TableSchema>(m, "TableSchema")
      .def(py::init<>())
      .def_readwrite("name", &TableSchema::name)
      .def_readwrite("columns", &TableSchema::columns)
      .def_readwrite("changelog_stream", &TableSchema::changelog_stream);

  py::class_<CatalogSnapshot>(m, "CatalogSnapshot")
      .def(py::init<>())
      .def_readwrite("streams", &CatalogSnapshot::streams)
      .def_readwrite("views", &CatalogSnapshot::views)
      .def_readwrite("tables", &CatalogSnapshot::tables)
      .def_readwrite("dictionaries", &CatalogSnapshot::dictionaries);

  py::class_<CompilationError>(m, "CompilationError")
      .def(py::init<>())
      .def_readwrite("message", &CompilationError::message)
      .def_readwrite("line", &CompilationError::line)
      .def_readwrite("column", &CompilationError::column);

  py::class_<CompilationResult>(m, "CompilationResult")
      .def(py::init<>())
      .def_readwrite("statement_type", &CompilationResult::statement_type)
      .def_readwrite("program_json", &CompilationResult::program_json)
      .def_readwrite("field_map", &CompilationResult::field_map)
      .def_readwrite("source_streams", &CompilationResult::source_streams)
      .def_readwrite("view_type", &CompilationResult::view_type)
      .def_readwrite("key_index", &CompilationResult::key_index)
      .def_readwrite("select_tier", &CompilationResult::select_tier)
      .def_readwrite("insert_payload", &CompilationResult::insert_payload)
      .def_readwrite("stream_schema", &CompilationResult::stream_schema)
      .def_readwrite("entity_name", &CompilationResult::entity_name)
      .def_readwrite("drop_entity_name", &CompilationResult::drop_entity_name)
      .def_readwrite("drop_entity_type", &CompilationResult::drop_entity_type)
      .def_readwrite("errors", &CompilationResult::errors)
      .def_readwrite("dictionary_updates", &CompilationResult::dictionary_updates)
      .def("has_errors", &CompilationResult::has_errors);

  py::class_<RuntimeOutputMessage>(m, "RuntimeOutputMessage")
      .def(py::init<>())
      .def_readwrite("timestamp", &RuntimeOutputMessage::timestamp)
      .def_readwrite("values", &RuntimeOutputMessage::values)
      .def_readwrite("operator_id", &RuntimeOutputMessage::operator_id)
      .def_readwrite("port", &RuntimeOutputMessage::port);

  py::class_<NativePipeline>(m, "NativePipeline")
      .def(py::init<const std::string&>(), py::arg("program_json"))
      .def("feed", &NativePipeline::feed,
           py::arg("timestamp"),
           py::arg("values"),
           py::arg("port") = "i1");

  py::class_<NativeSessionPipeline>(m, "NativeSessionPipeline")
      .def(py::init<const std::string&>(), py::arg("program_json"))
      .def("register_outputs", &NativeSessionPipeline::register_outputs,
           py::arg("op_ids"))
      .def("feed", &NativeSessionPipeline::feed,
           py::arg("timestamp"),
           py::arg("values"),
           py::arg("port") = "i1")
      .def("feed_buffer", &NativeSessionPipeline::feed_buffer,
           py::arg("timestamps"),
           py::arg("values"),
           py::arg("port") = "i1");

  m.def(
      "compile_sql",
      [](const std::string& sql, const rtbot_sql::CatalogSnapshot& catalog,
         int64_t ts_units_per_second) {
        auto expanded = rtbot_sql::api::compile_sql_expanded(
            sql, catalog, ts_units_per_second);
        py::dict out;
        py::list results;
        for (const auto& r : expanded.results) {
          results.append(r);
        }
        out["results"] = results;
        out["new_ts_units_per_second"] = expanded.new_ts_units_per_second;
        return out;
      },
      "Preprocess and compile SQL in one step",
      py::arg("sql"), py::arg("catalog"), py::arg("ts_units_per_second"));

  m.def(
      "compile_session",
      [](const rtbot_sql::CatalogSnapshot& catalog) {
        auto r = rtbot_sql::api::compile_session_program(catalog);
        py::dict out;
        out["program_json"] = r.program_json;
        out["view_terminals"] = r.view_terminals;
        out["view_terminal_ports"] = r.view_terminal_ports;
        out["materialized_views"] = r.materialized_views;
        out["base_stream_inputs"] = r.base_stream_inputs;
        out["base_stream_ports"] = r.base_stream_ports;
        py::list errs;
        for (const auto& e : r.errors) {
          py::dict d;
          d["message"] = e.message;
          d["line"] = e.line;
          d["column"] = e.column;
          errs.append(d);
        }
        out["errors"] = errs;
        return out;
      },
      "Consolidate every view in the catalog into a single rtbot Program",
      py::arg("catalog"));

  m.def(
      "validate_sql",
      [](const std::string& sql) {
        py::dict out;
        auto parsed = rtbot_sql::parser::parse(sql);
        out["valid"] = parsed.ok();

        py::list errors;
        for (const auto& error : parsed.errors) {
          py::dict err;
          err["message"] = error;
          err["line"] = -1;
          err["column"] = -1;
          errors.append(err);
        }

        out["errors"] = errors;
        rtbot_sql::parser::free_result(parsed);
        return out;
      },
      "Parse-level SQL validation",
      py::arg("sql"));

  m.def(
      "preprocess_sql",
      [](const std::string& sql, int64_t ts_units_per_second) {
        auto result =
            rtbot_sql::api::preprocess_sql(sql, ts_units_per_second);
        py::dict out;
        py::list stmts;
        for (const auto& s : result.statements) {
          stmts.append(s);
        }
        out["statements"] = stmts;
        out["new_ts_units_per_second"] = result.new_ts_units_per_second;
        return out;
      },
      "Preprocess SQL — expand sugar syntax (ALIGNED STREAM, SET TIMESCALE)",
      py::arg("sql"), py::arg("ts_units_per_second"));
}
