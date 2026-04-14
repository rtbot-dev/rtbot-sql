#include "rtbot_sql/api/compiler.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
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

constexpr const char* kPreset02Sql = R"sql(
CREATE MATERIALIZED VIEW kurtosis_trend AS
  SELECT device_id,
         channel_id,
         ex2 - POWER(mean_value, 2)               AS variance_value,
         ex4 - 4.0 * ex3 * mean_value
             + 6.0 * ex2 * POWER(mean_value, 2)
             - 3.0 * POWER(mean_value, 4)         AS m4,
         m4 / POWER(variance_value, 2)            AS kurtosis_value,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1
)sql";

constexpr const char* kPreset04Sql = R"sql(
CREATE MATERIALIZED VIEW crest_clearance_impulse AS
  SELECT device_id,
         channel_id,
         POWER(ex2, 0.5)                           AS rms_value,
         peak_value / POWER(ex2, 0.5)              AS crest_factor,
         peak_value / POWER(mean_sqrt_abs, 2)      AS clearance_factor,
         POWER(ex2, 0.5) / mean_abs                AS shape_factor,
         peak_value / mean_abs                     AS impulse_factor,
         peak_value * 2.0                          AS peak_to_peak,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1
)sql";

constexpr const char* kPreset05Sql = R"sql(
CREATE MATERIALIZED VIEW statistical_dashboard AS
  SELECT device_id,
         channel_id,
         mean_value,
         ex2 - POWER(mean_value, 2)               AS variance_value,
         POWER(variance_value, 0.5)               AS std_value,
         POWER(ex2, 0.5)                          AS rms_value,
         ex3 - 3.0 * ex2 * mean_value
             + 2.0 * POWER(mean_value, 3)         AS m3,
         m3 / POWER(variance_value, 1.5)          AS skewness_value,
         ex4 - 4.0 * ex3 * mean_value
             + 6.0 * ex2 * POWER(mean_value, 2)
             - 3.0 * POWER(mean_value, 4)         AS m4,
         m4 / POWER(variance_value, 2)            AS kurtosis_value,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1
)sql";

struct PresetDef {
  std::string id;
  std::string view_name;
  const char* sql;
};

const std::vector<PresetDef>& all_presets() {
  static const std::vector<PresetDef> kPresets = {
      {"01_rms", "rms_trend", kPreset01Sql},
      {"02_kurtosis", "kurtosis_trend", kPreset02Sql},
      {"04_crest", "crest_clearance_impulse", kPreset04Sql},
      {"05_dashboard", "statistical_dashboard", kPreset05Sql},
  };
  return kPresets;
}

struct Config {
  int bearings = 4;
  int samples_per_burst = 512;
  int bursts = 300;
  int progress_every = 10;
  bool pure_cpp = false;
  bool batch = false;
  std::vector<std::string> presets = {
      "01_rms",
      "02_kurtosis",
      "04_crest",
      "05_dashboard",
  };
};

struct PipelineInstance {
  std::string id;
  std::string view_name;
  rtbot::Program program;
  std::uint64_t forwarded_messages = 0;
  std::uint64_t output_messages = 0;

  PipelineInstance(std::string preset_id,
                   std::string output_view,
                   const std::string& program_json)
      : id(std::move(preset_id)),
        view_name(std::move(output_view)),
        program(program_json) {}
};

std::vector<std::string> split_csv(const std::string& value) {
  std::vector<std::string> out;
  std::string token;
  for (char c : value) {
    if (c == ',') {
      if (!token.empty()) {
        out.push_back(token);
        token.clear();
      }
      continue;
    }
    if (c != ' ' && c != '\t' && c != '\n') {
      token.push_back(c);
    }
  }
  if (!token.empty()) {
    out.push_back(token);
  }
  return out;
}

int parse_positive_int(const std::string& raw, const std::string& name) {
  char* end = nullptr;
  const long parsed = std::strtol(raw.c_str(), &end, 10);
  if (end == nullptr || *end != '\0' || parsed <= 0) {
    throw std::runtime_error("Invalid value for --" + name + ": " + raw);
  }
  return static_cast<int>(parsed);
}

void print_help(const char* argv0) {
  std::cout
      << "Usage: " << argv0 << " [options]\n\n"
      << "Runs a native C++ throughput benchmark for the IMS SQL pipeline.\n"
      << "It executes the same shared-view architecture used in the notebook:\n"
      << "vibration_raw -> vibration_moments -> materialized presets.\n\n"
      << "Options:\n"
      << "  --bearings N            Number of bearing keys (default: 4)\n"
      << "  --samples-per-burst N   Samples per burst before sentinel (default: 512)\n"
      << "  --bursts N              Number of bursts per bearing (default: 300)\n"
      << "  --progress-every N      Progress print interval in bursts (default: 10)\n"
      << "  --presets CSV           Active presets, comma-separated\n"
      << "                          Available: 01_rms,02_kurtosis,04_crest,05_dashboard\n"
      << "  --pure-cpp              Skip rtbot SQL and compute burst metrics in pure C++\n"
      << "                          (upper bound for this hardware; ignores --presets)\n"
      << "  --batch                 Feed the base program one burst per receive_batch\n"
      << "                          call (batched propagation; amortizes scheduling)\n"
      << "  --list-presets          Print available preset IDs and exit\n"
      << "  --help                  Show this message\n\n"
      << "Example:\n"
      << "  bazel run //tests/perf:ims_cpp_benchmark -- \\\n"
      << "    --presets=01_rms,02_kurtosis,04_crest,05_dashboard \\\n"
      << "    --bearings=4 --samples-per-burst=512 --bursts=300\n";
}

Config parse_args(int argc, char** argv) {
  Config cfg;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];

    auto read_next = [&](const std::string& flag) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("Missing value for --" + flag);
      }
      return argv[++i];
    };

    if (arg == "--help") {
      print_help(argv[0]);
      std::exit(0);
    }

    if (arg == "--list-presets") {
      for (const auto& preset : all_presets()) {
        std::cout << preset.id << " -> " << preset.view_name << '\n';
      }
      std::exit(0);
    }

    if (arg.rfind("--bearings=", 0) == 0) {
      cfg.bearings = parse_positive_int(arg.substr(11), "bearings");
      continue;
    }
    if (arg == "--bearings") {
      cfg.bearings = parse_positive_int(read_next("bearings"), "bearings");
      continue;
    }

    if (arg.rfind("--samples-per-burst=", 0) == 0) {
      cfg.samples_per_burst =
          parse_positive_int(arg.substr(20), "samples-per-burst");
      continue;
    }
    if (arg == "--samples-per-burst") {
      cfg.samples_per_burst =
          parse_positive_int(read_next("samples-per-burst"), "samples-per-burst");
      continue;
    }

    if (arg.rfind("--bursts=", 0) == 0) {
      cfg.bursts = parse_positive_int(arg.substr(9), "bursts");
      continue;
    }
    if (arg == "--bursts") {
      cfg.bursts = parse_positive_int(read_next("bursts"), "bursts");
      continue;
    }

    if (arg.rfind("--progress-every=", 0) == 0) {
      cfg.progress_every = parse_positive_int(arg.substr(17), "progress-every");
      continue;
    }
    if (arg == "--progress-every") {
      cfg.progress_every =
          parse_positive_int(read_next("progress-every"), "progress-every");
      continue;
    }

    if (arg.rfind("--presets=", 0) == 0) {
      cfg.presets = split_csv(arg.substr(10));
      continue;
    }
    if (arg == "--presets") {
      cfg.presets = split_csv(read_next("presets"));
      continue;
    }

    if (arg == "--pure-cpp") {
      cfg.pure_cpp = true;
      continue;
    }

    if (arg == "--batch") {
      cfg.batch = true;
      continue;
    }

    throw std::runtime_error("Unknown argument: " + arg);
  }

  if (cfg.presets.empty() && !cfg.pure_cpp) {
    throw std::runtime_error("At least one preset is required");
  }

  std::set<std::string> seen;
  std::vector<std::string> deduped;
  deduped.reserve(cfg.presets.size());
  for (const auto& preset : cfg.presets) {
    if (seen.insert(preset).second) {
      deduped.push_back(preset);
    }
  }
  cfg.presets = std::move(deduped);
  return cfg;
}

CompilationResult compile_or_die(const std::string& sql,
                                 const CatalogSnapshot& catalog,
                                 const std::string& label) {
  auto result = compile_sql(sql, catalog);
  if (!result.has_errors()) {
    return result;
  }

  std::cerr << "Compilation failed for " << label << ":\n";
  for (const auto& error : result.errors) {
    std::cerr << "  - " << error.message;
    if (error.line >= 0) {
      std::cerr << " (line " << error.line;
      if (error.column >= 0) {
        std::cerr << ", col " << error.column;
      }
      std::cerr << ")";
    }
    std::cerr << '\n';
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

double synthetic_amplitude(int sample_index) {
  return 0.5 + 0.3 * ((sample_index % 7) < 4 ? 1.0 : -1.0);
}

const PresetDef* find_preset(const std::string& preset_id) {
  const auto& presets = all_presets();
  const auto it = std::find_if(
      presets.begin(), presets.end(),
      [&](const PresetDef& def) { return def.id == preset_id; });
  return it == presets.end() ? nullptr : &(*it);
}

struct BurstMetrics {
  std::uint64_t sample_count;
  double mean_value;
  double variance_value;
  double std_value;
  double rms_value;
  double m3;
  double m4;
  double skewness_value;
  double kurtosis_value;
  double peak_value;
  double peak_to_peak;
  double crest_factor;
  double clearance_factor;
  double impulse_factor;
  double shape_factor;
};

inline double safe_div(double numerator, double denominator) {
  return denominator == 0.0 ? 0.0 : numerator / denominator;
}

BurstMetrics compute_burst_metrics(const double* samples, std::size_t n) {
  BurstMetrics m{};
  if (n == 0) {
    return m;
  }

  double sum = 0.0;
  double sum2 = 0.0;
  double sum3 = 0.0;
  double sum4 = 0.0;
  double sum_abs = 0.0;
  double sum_sqrt_abs = 0.0;
  double peak = 0.0;
  double min_v = samples[0];
  double max_v = samples[0];

  for (std::size_t i = 0; i < n; ++i) {
    const double x = samples[i];
    const double ax = std::fabs(x);
    const double x2 = x * x;
    sum += x;
    sum2 += x2;
    sum3 += x2 * x;
    sum4 += x2 * x2;
    sum_abs += ax;
    sum_sqrt_abs += std::sqrt(ax);
    if (ax > peak) peak = ax;
    if (x < min_v) min_v = x;
    if (x > max_v) max_v = x;
  }

  const double dn = static_cast<double>(n);
  const double mean = sum / dn;
  const double ex2 = sum2 / dn;
  const double ex3 = sum3 / dn;
  const double ex4 = sum4 / dn;
  const double mean_abs = sum_abs / dn;
  const double mean_sqrt_abs = sum_sqrt_abs / dn;

  const double variance = ex2 - mean * mean;
  const double std_value = std::sqrt(std::max(variance, 0.0));
  const double rms = std::sqrt(ex2);
  const double m3 = ex3 - 3.0 * ex2 * mean + 2.0 * mean * mean * mean;
  const double m4 = ex4 - 4.0 * ex3 * mean
                    + 6.0 * ex2 * mean * mean
                    - 3.0 * mean * mean * mean * mean;

  m.sample_count = static_cast<std::uint64_t>(n);
  m.mean_value = mean;
  m.variance_value = variance;
  m.std_value = std_value;
  m.rms_value = rms;
  m.m3 = m3;
  m.m4 = m4;
  m.skewness_value = safe_div(m3, std_value * std_value * std_value);
  m.kurtosis_value = safe_div(m4, variance * variance);
  m.peak_value = peak;
  m.peak_to_peak = max_v - min_v;
  m.crest_factor = safe_div(peak, rms);
  m.clearance_factor = safe_div(peak, mean_sqrt_abs * mean_sqrt_abs);
  m.impulse_factor = safe_div(peak, mean_abs);
  m.shape_factor = safe_div(rms, mean_abs);
  return m;
}

int run_pure_cpp(const Config& cfg) {
  std::cout << "\n================================================================\n";
  std::cout << "IMS pure C++ upper-bound benchmark (no rtbot SQL)\n";
  std::cout << "================================================================\n";
  std::cout << "Config:\n";
  std::cout << "  bearings            : " << cfg.bearings << "\n";
  std::cout << "  samples_per_burst   : " << cfg.samples_per_burst << "\n";
  std::cout << "  bursts              : " << cfg.bursts << "\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << "  " << std::setw(6) << "Burst" << "  " << std::setw(14) << "Samples"
            << "  " << std::setw(14) << "Samples/sec" << "  " << std::setw(12)
            << "Bursts/sec" << "\n";

  std::vector<double> buffer(static_cast<std::size_t>(cfg.samples_per_burst));
  for (int i = 0; i < cfg.samples_per_burst; ++i) {
    buffer[static_cast<std::size_t>(i)] = synthetic_amplitude(i);
  }

  std::uint64_t total_samples = 0;
  std::uint64_t total_bursts = 0;

  // Keep a sink to prevent the optimizer from throwing everything away.
  double sink = 0.0;

  const auto start = std::chrono::steady_clock::now();
  auto last_report = start;
  std::uint64_t last_report_samples = 0;

  for (int burst = 1; burst <= cfg.bursts; ++burst) {
    for (int bearing = 1; bearing <= cfg.bearings; ++bearing) {
      (void)bearing;
      const auto metrics = compute_burst_metrics(buffer.data(), buffer.size());
      sink += metrics.rms_value + metrics.kurtosis_value + metrics.skewness_value
              + metrics.crest_factor + metrics.clearance_factor
              + metrics.impulse_factor + metrics.shape_factor
              + metrics.peak_to_peak + metrics.m3 + metrics.m4;
      total_samples += metrics.sample_count;
      ++total_bursts;
    }

    if (burst % cfg.progress_every == 0 || burst == cfg.bursts) {
      const auto now = std::chrono::steady_clock::now();
      const std::chrono::duration<double> interval = now - last_report;
      const std::chrono::duration<double> elapsed = now - start;
      const auto interval_samples = total_samples - last_report_samples;
      const double interval_rate =
          interval_samples / std::max(interval.count(), 1e-9);
      const double burst_rate =
          total_bursts / std::max(elapsed.count(), 1e-9);

      std::cout << "  " << std::setw(6) << burst << "  " << std::setw(14)
                << total_samples << "  " << std::setw(14) << std::fixed
                << std::setprecision(0) << interval_rate << "  "
                << std::setw(12) << std::setprecision(1) << burst_rate << "\n";

      last_report = now;
      last_report_samples = total_samples;
    }
  }

  const auto end = std::chrono::steady_clock::now();
  const std::chrono::duration<double> elapsed = end - start;
  const double samples_per_sec = total_samples / std::max(elapsed.count(), 1e-9);
  const double bursts_per_sec = total_bursts / std::max(elapsed.count(), 1e-9);

  std::cout << "================================================================\n";
  std::cout << "Results (pure C++ upper bound)\n";
  std::cout << "----------------------------------------------------------------\n";
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "Elapsed wall time        : " << elapsed.count() << " s\n";
  std::cout << std::setprecision(0);
  std::cout << "Total bursts             : " << total_bursts << "\n";
  std::cout << "Total samples            : " << total_samples << "\n";
  std::cout << "Samples/sec              : " << samples_per_sec << "\n";
  std::cout << std::setprecision(1);
  std::cout << "Bursts/sec               : " << bursts_per_sec << "\n";
  std::cout << std::setprecision(6);
  std::cout << "(sink value, ignore)     : " << sink << "\n";
  std::cout << "================================================================\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Config cfg = parse_args(argc, argv);

    if (cfg.pure_cpp) {
      return run_pure_cpp(cfg);
    }

    CatalogSnapshot catalog;

    const auto stream = compile_or_die(kStreamSql, catalog, "vibration_raw stream");
    if (stream.statement_type != StatementType::CREATE_STREAM) {
      throw std::runtime_error("Expected CREATE_STREAM result for stream SQL");
    }
    register_stream(catalog, stream);

    const auto base_view =
        compile_or_die(kBaseViewSql, catalog, "vibration_moments view");
    if (base_view.statement_type != StatementType::CREATE_VIEW) {
      throw std::runtime_error("Expected CREATE_VIEW result for base view SQL");
    }
    register_view(catalog, base_view, EntityType::VIEW);

    std::vector<std::unique_ptr<PipelineInstance>> pipelines;
    pipelines.reserve(cfg.presets.size());

    for (const auto& preset_id : cfg.presets) {
      const auto* preset = find_preset(preset_id);
      if (!preset) {
        throw std::runtime_error("Unknown preset ID: " + preset_id);
      }

      const auto compiled = compile_or_die(
          preset->sql, catalog, "preset " + preset->id + " (" + preset->view_name + ")");

      if (compiled.statement_type != StatementType::CREATE_MATERIALIZED_VIEW) {
        throw std::runtime_error("Expected CREATE_MATERIALIZED_VIEW for " + preset->id);
      }

      register_view(catalog, compiled, EntityType::MATERIALIZED_VIEW);
      pipelines.push_back(std::make_unique<PipelineInstance>(
          preset->id, preset->view_name, compiled.program_json));
    }

    rtbot::Program base_program(base_view.program_json);
    std::uint64_t ingress_messages = 0;
    std::uint64_t base_outputs = 0;
    std::uint64_t materialized_outputs = 0;
    std::uint64_t program_receive_calls = 0;

    rtbot::timestamp_t timestamp = 1'000'000'000'000;
    const auto start = std::chrono::steady_clock::now();
    auto last_report = start;
    std::uint64_t last_report_ingress = 0;

    std::cout << "\n================================================================\n";
    std::cout << "IMS native C++ throughput benchmark\n";
    std::cout << "================================================================\n";
    std::cout << "Config:\n";
    std::cout << "  bearings            : " << cfg.bearings << "\n";
    std::cout << "  samples_per_burst   : " << cfg.samples_per_burst << "\n";
    std::cout << "  bursts              : " << cfg.bursts << "\n";
    std::cout << "  active presets      : ";
    for (std::size_t i = 0; i < cfg.presets.size(); ++i) {
      std::cout << cfg.presets[i];
      if (i + 1 < cfg.presets.size()) {
        std::cout << ",";
      }
    }
    std::cout << "\n----------------------------------------------------------------\n";
    std::cout << "  " << std::setw(6) << "Burst" << "  " << std::setw(12) << "Ingress"
              << "  " << std::setw(12) << "Msgs/sec" << "  " << std::setw(12)
              << "BaseOut" << "  " << std::setw(12) << "MatOut" << "\n";

    auto dispatch_base_batch = [&](const rtbot::ProgramMsgBatch& batch) {
      for (const auto& [base_op_id, op_batch] : batch) {
        (void)base_op_id;
        for (const auto& [base_port, messages] : op_batch) {
          (void)base_port;
          for (const auto& message : messages) {
            auto* vec_msg =
                dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(message.get());
            if (!vec_msg) {
              continue;
            }

            ++base_outputs;
            for (auto& pipeline : pipelines) {
              ++pipeline->forwarded_messages;
              ++program_receive_calls;
              auto output_batch = pipeline->program.receive(
                  make_vector_message(vec_msg->time, *vec_msg->data.values), "i1");
              const auto produced = count_total_messages(output_batch);
              pipeline->output_messages += produced;
              materialized_outputs += produced;
            }
          }
        }
      }
    };

    auto feed_raw = [&](double device_id, double channel_id, double amplitude,
                        rtbot::timestamp_t msg_ts) {
      ++program_receive_calls;
      ++ingress_messages;
      auto batch = base_program.receive(
          make_vector_message(msg_ts, {device_id, channel_id, amplitude}), "i1");
      dispatch_base_batch(batch);
    };

    // In batch mode we accumulate a full burst (all amplitude samples plus the
    // sentinel) into a per-port message buffer and feed it to the base program
    // as a single receive_batch call. Semantically equivalent to feeding
    // messages one at a time because sync_data_inputs is state-preserving and
    // monotone-per-port is preserved by appending a sorted vector.
    auto feed_burst_batched = [&](int bearing) {
      std::map<std::string, std::vector<std::unique_ptr<rtbot::BaseMessage>>> burst_buffer;
      auto& port = burst_buffer["i1"];
      port.reserve(static_cast<std::size_t>(cfg.samples_per_burst) + 1);
      for (int sample = 0; sample < cfg.samples_per_burst; ++sample) {
        port.push_back(make_vector_message(
            timestamp + sample,
            {static_cast<double>(bearing), 1.0, synthetic_amplitude(sample)}));
      }
      // Sentinel closes the current burst key (ABS(amplitude) > 0).
      port.push_back(make_vector_message(
          timestamp + cfg.samples_per_burst,
          {static_cast<double>(bearing), 1.0, 0.0}));

      ingress_messages += port.size();
      ++program_receive_calls;  // one batched call per burst
      auto batch = base_program.receive_batch(burst_buffer);
      dispatch_base_batch(batch);
    };

    for (int burst = 1; burst <= cfg.bursts; ++burst) {
      for (int bearing = 1; bearing <= cfg.bearings; ++bearing) {
        if (cfg.batch) {
          feed_burst_batched(bearing);
        } else {
          for (int sample = 0; sample < cfg.samples_per_burst; ++sample) {
            feed_raw(static_cast<double>(bearing), 1.0, synthetic_amplitude(sample),
                     timestamp + sample);
          }
          // Sentinel closes the current burst key (ABS(amplitude) > 0).
          feed_raw(static_cast<double>(bearing), 1.0, 0.0,
                   timestamp + cfg.samples_per_burst);
        }
        timestamp += cfg.samples_per_burst + 100;
      }

      if (burst % cfg.progress_every == 0 || burst == cfg.bursts) {
        const auto now = std::chrono::steady_clock::now();
        const std::chrono::duration<double> interval = now - last_report;
        const std::chrono::duration<double> elapsed = now - start;

        const auto interval_ingress = ingress_messages - last_report_ingress;
        const double interval_rate =
            interval_ingress / std::max(interval.count(), 1e-9);
        const double overall_rate =
            ingress_messages / std::max(elapsed.count(), 1e-9);
        const double eta_seconds =
            (cfg.bursts - burst) / std::max(static_cast<double>(burst), 1e-9) *
            elapsed.count();

        std::cout << "  " << std::setw(6) << burst << "  " << std::setw(12)
                  << ingress_messages << "  " << std::setw(12) << std::fixed
                  << std::setprecision(0) << interval_rate << "  "
                  << std::setw(12) << base_outputs << "  " << std::setw(12)
                  << materialized_outputs << "   ETA " << std::setprecision(1)
                  << eta_seconds << "s"
                  << "  overall " << std::setprecision(0) << overall_rate
                  << " msg/s\n";

        last_report = now;
        last_report_ingress = ingress_messages;
      }
    }

    const auto end = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = end - start;
    const double ingress_per_sec = ingress_messages / std::max(elapsed.count(), 1e-9);
    const double receive_calls_per_sec =
        program_receive_calls / std::max(elapsed.count(), 1e-9);

    std::cout << "================================================================\n";
    std::cout << "Results\n";
    std::cout << "----------------------------------------------------------------\n";
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Elapsed wall time        : " << elapsed.count() << " s\n";
    std::cout << std::setprecision(0);
    std::cout << "Ingress messages         : " << ingress_messages << "\n";
    std::cout << "Ingress throughput       : " << ingress_per_sec << " msg/s\n";
    std::cout << "Base view outputs        : " << base_outputs << "\n";
    std::cout << "Materialized outputs     : " << materialized_outputs << "\n";
    std::cout << "Program receive() calls  : " << program_receive_calls << "\n";
    std::cout << "receive() calls/sec      : " << receive_calls_per_sec << "\n";
    std::cout << "Per-preset stats:\n";
    for (const auto& pipeline : pipelines) {
      std::cout << "  - " << pipeline->id << " (" << pipeline->view_name
                << "): forwarded=" << pipeline->forwarded_messages
                << ", produced=" << pipeline->output_messages << "\n";
    }
    std::cout << "================================================================\n";

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "Benchmark error: " << ex.what() << "\n";
    return 1;
  }
}
