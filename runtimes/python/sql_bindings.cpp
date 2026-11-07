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

struct Int64ColumnBuffer {
  std::vector<std::int64_t> data;
};

struct Float64MatrixBuffer {
  std::vector<double> data;
  std::size_t nrows = 0;
  std::size_t ncols = 0;
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
      py::buffer timestamps,
      py::buffer values,
      const std::string& port = "i1") {
    return decode_batch(run_receive_buffer(timestamps, values, port));
  }

  py::dict feed_buffer_columnar(
      py::buffer timestamps,
      py::buffer values,
      const std::string& port = "i1") {
    auto batch = run_receive_buffer(timestamps, values, port);
    auto decoded = rtbot_sql::api::decode_program_batch(batch);

    struct OpAcc {
      Int64ColumnBuffer ts;
      Float64MatrixBuffer vals;
    };
    std::map<std::string, OpAcc> by_op;
    for (const auto& m : decoded) {
      auto& acc = by_op[m.operator_id];
      acc.ts.data.push_back(static_cast<std::int64_t>(m.timestamp));
      if (acc.vals.ncols == 0 && !m.values.empty()) {
        acc.vals.ncols = m.values.size();
      }
      acc.vals.data.insert(acc.vals.data.end(), m.values.begin(), m.values.end());
      acc.vals.nrows += 1;
    }

    py::dict out;
    for (auto& [op_id, acc] : by_op) {
      out[py::str(op_id)] = py::make_tuple(
          py::cast(std::move(acc.ts)),
          py::cast(std::move(acc.vals)));
    }
    return out;
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
  rtbot::ProgramMsgBatch run_receive_buffer(
      py::buffer& timestamps,
      py::buffer& values,
      const std::string& port) {
    auto ts_info = timestamps.request();
    auto vals_info = values.request();
    if (ts_info.ndim != 1 || ts_info.itemsize != sizeof(std::int64_t)) {
      throw std::invalid_argument(
          "timestamps must be a 1D 64-bit integer buffer");
    }
    const std::string ts_fmt = ts_info.format;
    if (ts_fmt != "q" && ts_fmt != "l" && ts_fmt != "Q" && ts_fmt != "L" &&
        ts_fmt != "=q" && ts_fmt != "=l" && ts_fmt != "=Q" && ts_fmt != "=L") {
      throw std::invalid_argument(
          "timestamps buffer must have 64-bit integer dtype");
    }
    if (ts_info.shape[0] > 1 &&
        ts_info.strides[0] != static_cast<py::ssize_t>(sizeof(std::int64_t))) {
      throw std::invalid_argument("timestamps buffer must be C-contiguous");
    }
    if (vals_info.itemsize != sizeof(double) ||
        (vals_info.format != "d" && vals_info.format != "=d")) {
      throw std::invalid_argument("values must be a float64 buffer");
    }
    const auto nrows = static_cast<std::size_t>(ts_info.shape[0]);
    std::size_t ncols = 0;
    if (vals_info.ndim == 2) {
      if (static_cast<std::size_t>(vals_info.shape[0]) != nrows) {
        throw std::invalid_argument(
            "timestamps and values must have the same number of rows");
      }
      ncols = static_cast<std::size_t>(vals_info.shape[1]);
      if (nrows > 0 && ncols > 0) {
        if (vals_info.strides[1] != static_cast<py::ssize_t>(sizeof(double)) ||
            (nrows > 1 &&
             vals_info.strides[0] !=
                 static_cast<py::ssize_t>(ncols * sizeof(double)))) {
          throw std::invalid_argument("values 2D buffer must be C-contiguous");
        }
      }
    } else if (vals_info.ndim == 1) {
      const auto total = static_cast<std::size_t>(vals_info.shape[0]);
      if (nrows > 0 && total % nrows != 0) {
        throw std::invalid_argument(
            "1D values buffer length must be a multiple of timestamps length");
      }
      if (total > 1 &&
          vals_info.strides[0] != static_cast<py::ssize_t>(sizeof(double))) {
        throw std::invalid_argument("values 1D buffer must be C-contiguous");
      }
      ncols = nrows > 0 ? (total / nrows) : 0;
    } else {
      throw std::invalid_argument("values must be 1D or 2D");
    }

    static_assert(sizeof(std::int64_t) == sizeof(rtbot::timestamp_t),
                  "int64 vs timestamp_t mismatch");
    const auto* times = reinterpret_cast<const rtbot::timestamp_t*>(ts_info.ptr);
    const auto* rows = static_cast<const double*>(vals_info.ptr);
    return program_.receive_buffer(port, rows, nrows, ncols, times);
  }

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
      .def_readwrite("column", &CompilationError::column)
      .def_readwrite("end_line", &CompilationError::end_line)
      .def_readwrite("end_column", &CompilationError::end_column);

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

  py::class_<Int64ColumnBuffer>(m, "Int64ColumnBuffer", py::buffer_protocol())
      .def_buffer([](Int64ColumnBuffer& b) -> py::buffer_info {
        return py::buffer_info(
            b.data.data(),
            sizeof(std::int64_t),
            py::format_descriptor<std::int64_t>::format(),
            1,
            {static_cast<py::ssize_t>(b.data.size())},
            {static_cast<py::ssize_t>(sizeof(std::int64_t))});
      })
      .def("__len__", [](const Int64ColumnBuffer& b) { return b.data.size(); })
      .def("__getitem__", [](const Int64ColumnBuffer& b, py::ssize_t i) {
        if (i < 0) i += static_cast<py::ssize_t>(b.data.size());
        if (i < 0 || static_cast<std::size_t>(i) >= b.data.size()) {
          throw py::index_error();
        }
        return b.data[static_cast<std::size_t>(i)];
      })
      .def("tolist", [](const Int64ColumnBuffer& b) { return b.data; });

  py::class_<Float64MatrixBuffer>(m, "Float64MatrixBuffer", py::buffer_protocol())
      .def_buffer([](Float64MatrixBuffer& b) -> py::buffer_info {
        return py::buffer_info(
            b.data.data(),
            sizeof(double),
            py::format_descriptor<double>::format(),
            2,
            {static_cast<py::ssize_t>(b.nrows),
             static_cast<py::ssize_t>(b.ncols)},
            {static_cast<py::ssize_t>(b.ncols * sizeof(double)),
             static_cast<py::ssize_t>(sizeof(double))});
      })
      .def_property_readonly("shape", [](const Float64MatrixBuffer& b) {
        return py::make_tuple(b.nrows, b.ncols);
      })
      .def("__len__", [](const Float64MatrixBuffer& b) { return b.nrows; })
      .def("tolist", [](const Float64MatrixBuffer& b) {
        std::vector<std::vector<double>> rows(b.nrows);
        for (std::size_t r = 0; r < b.nrows; ++r) {
          rows[r].assign(b.data.begin() + r * b.ncols,
                         b.data.begin() + (r + 1) * b.ncols);
        }
        return rows;
      });

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
           py::arg("port") = "i1")
      .def("feed_buffer_columnar", &NativeSessionPipeline::feed_buffer_columnar,
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
          d["end_line"] = e.end_line;
          d["end_column"] = e.end_column;
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
          err["message"] = error.message;
          err["line"] = error.loc.line;
          err["column"] = error.loc.column;
          err["end_line"] = error.loc.end_line;
          err["end_column"] = error.loc.end_column;
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
