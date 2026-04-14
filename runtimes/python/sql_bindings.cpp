#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "rtbot/Message.h"
#include "rtbot/Program.h"
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

struct InputVectorMessage {
  std::uint64_t timestamp = 0;
  std::vector<double> values;
  std::string port = "i1";
};

struct RuntimeOutputMessage {
  std::uint64_t timestamp = 0;
  std::vector<double> values;
  std::string operator_id;
  std::string port;
};

std::vector<RuntimeOutputMessage> decode_batch(
    const rtbot::ProgramMsgBatch& batch) {
  std::vector<RuntimeOutputMessage> out;

  for (const auto& [operator_id, operator_batch] : batch) {
    for (const auto& [port, messages] : operator_batch) {
      for (const auto& message : messages) {
        if (auto* vec =
                dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
                    message.get())) {
          RuntimeOutputMessage msg{};
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
          RuntimeOutputMessage msg{};
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
                   [](const RuntimeOutputMessage& a,
                      const RuntimeOutputMessage& b) {
                     return std::tie(a.timestamp, a.operator_id, a.port) <
                            std::tie(b.timestamp, b.operator_id, b.port);
                   });

  return out;
}

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

  std::vector<RuntimeOutputMessage> run(
      const std::vector<InputVectorMessage>& inputs) {
    std::vector<RuntimeOutputMessage> out;
    for (const auto& input : inputs) {
      auto chunk = feed(input.timestamp, input.values, input.port);
      out.insert(out.end(), chunk.begin(), chunk.end());
    }
    return out;
  }

  std::vector<RuntimeOutputMessage> feed_batch(
      py::array_t<int64_t, py::array::c_style> timestamps,
      py::array_t<double, py::array::c_style> values,
      const std::string& port = "i1") {
    auto ts = timestamps.template unchecked<1>();
    auto vals = values.template unchecked<2>();

    if (ts.shape(0) != vals.shape(0)) {
      throw std::invalid_argument(
          "timestamps and values must have the same number of rows");
    }

    std::vector<RuntimeOutputMessage> all_outputs;
    auto ncols = static_cast<size_t>(vals.shape(1));

    for (py::ssize_t i = 0; i < ts.shape(0); ++i) {
      std::vector<double> row(ncols);
      for (size_t j = 0; j < ncols; ++j) {
        row[j] = vals(i, static_cast<py::ssize_t>(j));
      }
      auto msg = rtbot::create_message<rtbot::VectorNumberData>(
          static_cast<rtbot::timestamp_t>(ts(i)),
          rtbot::VectorNumberData{std::move(row)});
      auto batch = program_.receive(std::move(msg), port);
      auto decoded = decode_batch(batch);
      all_outputs.insert(all_outputs.end(),
                         std::make_move_iterator(decoded.begin()),
                         std::make_move_iterator(decoded.end()));
    }
    return all_outputs;
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

  py::class_<InputVectorMessage>(m, "InputVectorMessage")
      .def(py::init<>())
      .def(py::init<std::uint64_t, std::vector<double>, std::string>(),
           py::arg("timestamp"),
           py::arg("values"),
           py::arg("port") = "i1")
      .def_readwrite("timestamp", &InputVectorMessage::timestamp)
      .def_readwrite("values", &InputVectorMessage::values)
      .def_readwrite("port", &InputVectorMessage::port);

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
           py::arg("port") = "i1")
      .def("feed_batch", &NativePipeline::feed_batch,
           py::arg("timestamps"),
           py::arg("values"),
           py::arg("port") = "i1")
      .def("run", &NativePipeline::run, py::arg("inputs"));

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
