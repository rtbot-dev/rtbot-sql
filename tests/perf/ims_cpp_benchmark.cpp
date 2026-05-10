#include "rtbot_sql/api/compiler.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <iomanip>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "rtbot/Collector.h"
#include "rtbot/Message.h"
#include "rtbot/Program.h"
#include "rtbot/compiled/jit/JitCompiler.h"
#include "rtbot/fuse/BurstAggregate.h"
#include "rtbot/fuse/FusedOps.h"

namespace {

using rtbot_sql::CatalogSnapshot;
using rtbot_sql::CompilationResult;
using rtbot_sql::EntityType;
using rtbot_sql::StatementType;
using rtbot_sql::StreamSchema;
using rtbot_sql::ViewMeta;
using rtbot_sql::api::compile_sql;

using json = nlohmann::json;

// Recursively count operators in a program JSON, including nested prototypes.
// Returns a map of operator_type -> count.
std::map<std::string, int> count_operators_recursive(const json& operators_array) {
  std::map<std::string, int> counts;
  for (const auto& op : operators_array) {
    std::string op_type = op.value("type", "unknown");
    counts[op_type]++;
    // Recurse into KeyedPipeline prototypes
    if (op.contains("prototype") && op["prototype"].contains("operators")) {
      auto nested = count_operators_recursive(op["prototype"]["operators"]);
      for (const auto& [type, count] : nested) {
        counts[type] += count;
      }
    }
    // Recurse into Pipeline sub-graphs
    if (op_type == "Pipeline" && op.contains("operators")) {
      auto nested = count_operators_recursive(op["operators"]);
      for (const auto& [type, count] : nested) {
        counts[type] += count;
      }
    }
  }
  return counts;
}

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
  bool burst_native = false;
  bool burst_aggregate = false;
  bool burst_buffer = false;
  bool count_operators = false;
  bool parity_check = false;
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
      << "  --burst-native          Bypass the base program entirely; compute the\n"
      << "                          vibration_moments aggregate as a single-pass C++\n"
      << "                          kernel over the burst and feed the 10-field result\n"
      << "                          directly to downstream presets. Measures the\n"
      << "                          burst-aggregate ceiling via the rtbot downstream graph.\n"
      << "  --burst-aggregate       Replace the base program with a BurstAggregate operator\n"
      << "                          driven by fused bytecode. Same semantics as\n"
      << "                          --burst-native but exercises the real rtbot operator\n"
      << "                          path (Message input, bytecode per-sample loop,\n"
      << "                          emit-on-segment). Measures the productized path.\n"
      << "  --burst-buffer          Same as --burst-aggregate but feeds BurstAggregate\n"
      << "                          via the raw-buffer API (Operator::receive_data_buffer).\n"
      << "                          Zero Message allocations on the hot path — measures\n"
      << "                          the full ceiling through the productized operator.\n"
      << "  --count-operators       Compile each preset and print operator counts\n"
      << "                          (set RTBOT_DISABLE_FUSION=1 to compare without)\n"
      << "  --parity-check          Run the IMS workload twice (JIT then forced\n"
      << "                          interpreter) and bit-exact compare every emission\n"
      << "                          (timestamp, port_id, values) per preset.\n"
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

    if (arg == "--burst-native") {
      cfg.burst_native = true;
      continue;
    }

    if (arg == "--burst-aggregate") {
      cfg.burst_aggregate = true;
      continue;
    }

    if (arg == "--burst-buffer") {
      cfg.burst_buffer = true;
      continue;
    }

    if (arg == "--count-operators") {
      cfg.count_operators = true;
      continue;
    }

    if (arg == "--parity-check") {
      cfg.parity_check = true;
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

[[maybe_unused]] std::size_t count_total_messages(const rtbot::ProgramMsgBatch& batch) {
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

// Vectorized burst aggregator matching the vibration_moments view shape.
// Writes 10 doubles in the order the SQL view emits them, so the downstream
// preset pipelines consume the result unchanged. A single pass over the
// amplitude buffer produces every field.
inline void compute_vibration_moments_agg(const double* amplitude, std::size_t n,
                                            double device_id, double channel_id,
                                            double* out /* size 10 */) {
  double sum = 0.0, sum2 = 0.0, sum3 = 0.0, sum4 = 0.0;
  double sum_abs = 0.0, sum_sqrt_abs = 0.0;
  double peak = 0.0;
  for (std::size_t i = 0; i < n; ++i) {
    const double x = amplitude[i];
    const double ax = std::fabs(x);
    const double x2 = x * x;
    sum += x;
    sum2 += x2;
    sum3 += x2 * x;
    sum4 += x2 * x2;
    sum_abs += ax;
    sum_sqrt_abs += std::sqrt(ax);
    if (ax > peak) peak = ax;
  }
  const double dn = n == 0 ? 1.0 : static_cast<double>(n);
  out[0] = device_id;
  out[1] = channel_id;
  out[2] = sum / dn;           // mean_value = AVG(amplitude)
  out[3] = sum2 / dn;          // ex2 = AVG(amplitude^2)
  out[4] = sum3 / dn;          // ex3 = AVG(amplitude^3)
  out[5] = sum4 / dn;          // ex4 = AVG(amplitude^4)
  out[6] = peak;               // peak_value = MAX(ABS(amplitude))
  out[7] = sum_abs / dn;       // mean_abs = AVG(ABS(amplitude))
  out[8] = sum_sqrt_abs / dn;  // mean_sqrt_abs = AVG(SQRT(ABS(amplitude)))
  out[9] = static_cast<double>(n);  // sample_count = COUNT(*)
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

struct ParityEmission {
  std::int64_t t;
  std::int32_t port_id;
  std::vector<double> values;
};

inline std::uint64_t dbits(double v) {
  std::uint64_t b = 0;
  std::memcpy(&b, &v, sizeof(double));
  return b;
}

struct ParityRun {
  // Per preset id -> list of emissions captured from drain_into.
  std::map<std::string, std::vector<ParityEmission>> per_preset;
  // Base view emissions (one entry per produced vector message from the base
  // program drain).
  std::vector<ParityEmission> base_view;
};

ParityRun run_ims_capture(const Config& cfg, const std::string& base_view_json,
                          const std::vector<PresetDef>& active_presets,
                          const std::vector<std::string>& preset_jsons,
                          const std::string& mode_label) {
  ParityRun run;

  rtbot::Program base_program(base_view_json);
  std::cerr << "[parity:" << mode_label
            << "] base_view using_jit=" << base_program.using_jit() << "\n";

  std::vector<std::unique_ptr<PipelineInstance>> pipelines;
  pipelines.reserve(active_presets.size());
  for (std::size_t i = 0; i < active_presets.size(); ++i) {
    pipelines.push_back(std::make_unique<PipelineInstance>(
        active_presets[i].id, active_presets[i].view_name, preset_jsons[i]));
    std::cerr << "[parity:" << mode_label << "] preset " << active_presets[i].id
              << " using_jit=" << pipelines.back()->program.using_jit() << "\n";
  }

  rtbot::timestamp_t timestamp = 1'000'000'000'000;

  auto dispatch_base_emit = [&](std::int64_t t, const double* v, std::size_t n) {
    ParityEmission be;
    be.t = t;
    be.port_id = 0;
    be.values.assign(v, v + n);
    run.base_view.push_back(std::move(be));
    for (auto& pipeline : pipelines) {
      pipeline->program.send_vector(t, v, n);
      pipeline->program.drain_into(
          [&](std::int64_t et, const double* ev, std::size_t en) {
            ParityEmission e;
            e.t = et;
            e.port_id = 0;
            e.values.assign(ev, ev + en);
            run.per_preset[pipeline->id].push_back(std::move(e));
          });
    }
  };

  auto feed_raw = [&](double device_id, double channel_id, double amplitude,
                      rtbot::timestamp_t msg_ts) {
    const double row[3] = {device_id, channel_id, amplitude};
    base_program.send_vector(msg_ts, row, 3);
    base_program.drain_into(dispatch_base_emit);
  };

  for (int burst = 1; burst <= cfg.bursts; ++burst) {
    for (int bearing = 1; bearing <= cfg.bearings; ++bearing) {
      for (int sample = 0; sample < cfg.samples_per_burst; ++sample) {
        feed_raw(static_cast<double>(bearing), 1.0,
                 synthetic_amplitude(sample), timestamp + sample);
      }
      feed_raw(static_cast<double>(bearing), 1.0, 0.0,
               timestamp + cfg.samples_per_burst);
      timestamp += cfg.samples_per_burst + 100;
    }
  }
  return run;
}

bool diff_emissions(const std::string& label,
                    const std::vector<ParityEmission>& a,
                    const std::vector<ParityEmission>& b) {
  if (a.size() != b.size()) {
    std::cout << "  [" << label << "] FAIL: size mismatch (jit=" << a.size()
              << " interp=" << b.size() << ")\n";
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (a[i].t != b[i].t || a[i].port_id != b[i].port_id ||
        a[i].values.size() != b[i].values.size()) {
      std::cout << "  [" << label << "] FAIL: header mismatch at idx " << i
                << " jit(t=" << a[i].t << ", port=" << a[i].port_id
                << ", n=" << a[i].values.size() << ")"
                << " interp(t=" << b[i].t << ", port=" << b[i].port_id
                << ", n=" << b[i].values.size() << ")\n";
      return false;
    }
    for (std::size_t k = 0; k < a[i].values.size(); ++k) {
      const std::uint64_t ab = dbits(a[i].values[k]);
      const std::uint64_t bb = dbits(b[i].values[k]);
      if (ab != bb) {
        std::cout << "  [" << label << "] FAIL: value mismatch at idx " << i
                  << " value " << k << " t=" << a[i].t << " jit=" << std::hex
                  << ab << " interp=" << bb << std::dec << " ("
                  << std::setprecision(17) << a[i].values[k] << " vs "
                  << b[i].values[k] << std::setprecision(6) << ")\n";
        return false;
      }
    }
  }
  std::cout << "  [" << label << "] PASS: " << a.size()
            << " emissions match bit-exactly\n";
  return true;
}

int run_parity_check(const Config& cfg) {
  std::cout << "\n================================================================\n";
  std::cout << "IMS JIT vs interpreter bit-exact parity check\n";
  std::cout << "================================================================\n";
  std::cout << "Config:\n";
  std::cout << "  bearings            : " << cfg.bearings << "\n";
  std::cout << "  samples_per_burst   : " << cfg.samples_per_burst << "\n";
  std::cout << "  bursts              : " << cfg.bursts << "\n";
  std::cout << "  active presets      : ";
  for (std::size_t i = 0; i < cfg.presets.size(); ++i) {
    std::cout << cfg.presets[i];
    if (i + 1 < cfg.presets.size()) std::cout << ",";
  }
  std::cout << "\n----------------------------------------------------------------\n";

  CatalogSnapshot catalog;
  const auto stream = compile_or_die(kStreamSql, catalog, "vibration_raw stream");
  register_stream(catalog, stream);
  const auto base_view =
      compile_or_die(kBaseViewSql, catalog, "vibration_moments view");
  register_view(catalog, base_view, EntityType::VIEW);

  std::vector<PresetDef> active_presets;
  std::vector<std::string> preset_jsons;
  for (const auto& preset_id : cfg.presets) {
    const auto* preset = find_preset(preset_id);
    if (!preset) {
      throw std::runtime_error("Unknown preset ID: " + preset_id);
    }
    const auto compiled = compile_or_die(preset->sql, catalog,
                                         "preset " + preset->id);
    register_view(catalog, compiled, EntityType::MATERIALIZED_VIEW);
    active_presets.push_back(*preset);
    preset_jsons.push_back(compiled.program_json);
  }

  std::cout << "Pass 1: JIT enabled\n";
  rtbot::Program::set_force_interpreter_for_testing(false);
  ParityRun jit_run = run_ims_capture(cfg, base_view.program_json,
                                       active_presets, preset_jsons, "jit");

  std::cout << "Pass 2: forced interpreter\n";
  rtbot::Program::set_force_interpreter_for_testing(true);
  ParityRun interp_run = run_ims_capture(cfg, base_view.program_json,
                                          active_presets, preset_jsons, "interp");
  rtbot::Program::set_force_interpreter_for_testing(false);

  std::cout << "----------------------------------------------------------------\n";
  std::cout << "Diff results:\n";

  bool all_pass = true;
  all_pass &= diff_emissions("base_view (vibration_moments)",
                              jit_run.base_view, interp_run.base_view);
  for (const auto& preset : active_presets) {
    const auto& a = jit_run.per_preset[preset.id];
    const auto& b = interp_run.per_preset[preset.id];
    all_pass &= diff_emissions(preset.id + " (" + preset.view_name + ")", a, b);
  }

  std::cout << "================================================================\n";
  if (all_pass) {
    std::cout << "OVERALL: PASS — every JIT emission matches the interpreter "
                 "bit-exactly.\n";
  } else {
    std::cout << "OVERALL: FAIL — see divergence reports above.\n";
  }
  std::cout << "================================================================\n";
  return all_pass ? 0 : 1;
}

int run_count_operators(const Config& cfg) {
  const bool fusion_disabled = std::getenv("RTBOT_DISABLE_FUSION") != nullptr;

  std::cout << "\n================================================================\n";
  std::cout << "Operator count analysis"
            << (fusion_disabled ? " (FusedExpression DISABLED)" : " (FusedExpression ENABLED)")
            << "\n";
  std::cout << "================================================================\n";

  CatalogSnapshot catalog;

  const auto stream = compile_or_die(kStreamSql, catalog, "vibration_raw stream");
  register_stream(catalog, stream);

  const auto base_view =
      compile_or_die(kBaseViewSql, catalog, "vibration_moments view");
  register_view(catalog, base_view, EntityType::VIEW);

  // Count operators in the base view
  {
    auto j = json::parse(base_view.program_json);
    auto counts = count_operators_recursive(j["operators"]);
    int total = 0;
    for (const auto& [_, c] : counts) total += c;
    std::cout << "\nvibration_moments (base view): " << total << " operators\n";
    for (const auto& [type, count] : counts) {
      std::cout << "  " << std::setw(24) << std::left << type << " " << count << "\n";
    }
  }

  int grand_total = 0;

  for (const auto& preset_id : cfg.presets) {
    const auto* preset = find_preset(preset_id);
    if (!preset) {
      std::cerr << "Unknown preset: " << preset_id << "\n";
      continue;
    }

    const auto compiled = compile_or_die(
        preset->sql, catalog, "preset " + preset->id);
    register_view(catalog, compiled, EntityType::MATERIALIZED_VIEW);

    auto j = json::parse(compiled.program_json);
    auto counts = count_operators_recursive(j["operators"]);
    int total = 0;
    for (const auto& [_, c] : counts) total += c;

    std::cout << "\n" << preset->id << " (" << preset->view_name << "): "
              << total << " operators\n";
    for (const auto& [type, count] : counts) {
      std::cout << "  " << std::setw(24) << std::left << type << " " << count << "\n";
    }

    grand_total += total;
  }

  std::cout << "\n================================================================\n";
  std::cout << "Total operators across presets: " << grand_total << "\n";
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

    if (cfg.count_operators) {
      return run_count_operators(cfg);
    }

    if (cfg.parity_check) {
      return run_parity_check(cfg);
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
      // Dump the compiled JSON for inspection
      if (preset->id == "01_rms") {
        std::ofstream("/tmp/rtbot_sql_preset_01_rms.json") << compiled.program_json;
      }
      // Try to compile via the JIT directly to see the failure reason
      try {
        auto jit_p = rtbot::jit::JitCompiler{}.compile(compiled.program_json);
        std::cerr << "[probe] preset " << preset->id << " JIT compile OK\n";
      } catch (const std::exception& e) {
        std::cerr << "[probe] preset " << preset->id << " JIT compile FAILED: " << e.what() << "\n";
      }
      pipelines.push_back(std::make_unique<PipelineInstance>(
          preset->id, preset->view_name, compiled.program_json));
      std::cerr << "[probe] preset " << preset->id
                << " using_jit=" << pipelines.back()->program.using_jit() << "\n";
    }

    if (true) {
      std::ofstream("/tmp/rtbot_sql_base_view.json") << base_view.program_json;
    }
    try {
      auto jit_b = rtbot::jit::JitCompiler{}.compile(base_view.program_json);
      std::cerr << "[probe] base_view JIT compile OK\n";
    } catch (const std::exception& e) {
      std::cerr << "[probe] base_view JIT compile FAILED: " << e.what() << "\n";
    }
    rtbot::Program base_program(base_view.program_json);
    std::cerr << "[probe] base_view using_jit=" << base_program.using_jit() << "\n";
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
              pipeline->program.send_vector(vec_msg->time,
                                            vec_msg->data.values->data(),
                                            vec_msg->data.values->size());
              std::size_t produced = 0;
              pipeline->program.drain_into(
                  [&produced](std::int64_t, const double*, std::size_t) {
                    ++produced;
                  });
              pipeline->output_messages += produced;
              materialized_outputs += produced;
            }
          }
        }
      }
    };

    // Streaming dispatch: forward each base_program emission directly to the
    // preset pipelines via send_vector, avoiding the per-emission
    // Message<VectorNumberData> allocation that dispatch_base_batch incurs.
    auto dispatch_base_emit = [&](std::int64_t t, const double* v,
                                   std::size_t n) {
      ++base_outputs;
      for (auto& pipeline : pipelines) {
        ++pipeline->forwarded_messages;
        ++program_receive_calls;
        pipeline->program.send_vector(t, v, n);
        std::size_t produced = 0;
        pipeline->program.drain_into(
            [&produced](std::int64_t, const double*, std::size_t) {
              ++produced;
            });
        pipeline->output_messages += produced;
        materialized_outputs += produced;
      }
    };

    auto feed_raw = [&](double device_id, double channel_id, double amplitude,
                        rtbot::timestamp_t msg_ts) {
      ++program_receive_calls;
      ++ingress_messages;
      const double row[3] = {device_id, channel_id, amplitude};
      base_program.send_vector(msg_ts, row, 3);
      base_program.drain_into(dispatch_base_emit);
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

    // Shared amplitude buffer reused across bursts for the burst-native path.
    std::vector<double> amp_buf;
    if (cfg.burst_native) {
      amp_buf.resize(static_cast<std::size_t>(cfg.samples_per_burst));
      for (int i = 0; i < cfg.samples_per_burst; ++i) {
        amp_buf[static_cast<std::size_t>(i)] = synthetic_amplitude(i);
      }
    }

    // Pre-build the raw-buffer input for --burst-buffer: a flat row-major
    // buffer of [device_id, channel_id, amplitude] triples plus parallel
    // timestamps. Reused across bursts to avoid per-iteration allocation
    // noise in the measurement.
    std::vector<double> raw_rows;
    std::vector<rtbot::timestamp_t> raw_times;
    if (cfg.burst_buffer) {
      const std::size_t total_rows =
          static_cast<std::size_t>(cfg.samples_per_burst) + 1;
      raw_rows.resize(total_rows * 3);
      raw_times.resize(total_rows);
    }

    // Burst-aggregate path: a real BurstAggregate operator driven by fused
    // bytecode runs the aggregate + segment predicate directly in one pass
    // per burst. We wire a Collector to capture its output and forward it
    // to the preset pipelines — same downstream shape as the full SQL path.
    std::shared_ptr<rtbot::BurstAggregate> burst_op;
    std::shared_ptr<rtbot::Collector> burst_sink;
    if (cfg.burst_aggregate || cfg.burst_buffer) {
      using namespace rtbot::fused_op;
      // Aggregate bytecode matching vibration_moments' SELECT list.
      // State layout (derived by pack_bytecode + the shared-COUNT trick the
      // compiler uses): slot 2 is the shared count slot that later STATE_LOADs
      // read to avoid re-incrementing.
      //
      //   END 0: AVG(amp)                     → CUMSUM slot 0, COUNT slot 2
      //   END 1: AVG(amp^2)                   → CUMSUM slot 3, STATE_LOAD 2
      //   END 2: AVG(amp^3)                   → CUMSUM slot 5, STATE_LOAD 2
      //   END 3: AVG(amp^4)                   → CUMSUM slot 7, STATE_LOAD 2
      //   END 4: MAX(|amp|)                   → MAX_AGG slot 9
      //   END 5: AVG(|amp|)                   → CUMSUM slot 10, STATE_LOAD 2
      //   END 6: AVG(sqrt(|amp|))             → CUMSUM slot 12, STATE_LOAD 2
      //   END 7: COUNT(*)                     → STATE_LOAD 2
      std::vector<double> agg_bc = {
          // AVG(amp)
          INPUT, 2, CUMSUM, 0, COUNT, 2, DIV, END,
          // AVG(amp^2)
          INPUT, 2, INPUT, 2, MUL, CUMSUM, 3, STATE_LOAD, 2, DIV, END,
          // AVG(amp^3)
          INPUT, 2, INPUT, 2, INPUT, 2, MUL, MUL, CUMSUM, 5, STATE_LOAD, 2, DIV, END,
          // AVG(amp^4)
          INPUT, 2, INPUT, 2, INPUT, 2, INPUT, 2, MUL, MUL, MUL, CUMSUM, 7, STATE_LOAD, 2, DIV, END,
          // MAX(|amp|)
          INPUT, 2, ABS, MAX_AGG, 9, END,
          // AVG(|amp|)
          INPUT, 2, ABS, CUMSUM, 10, STATE_LOAD, 2, DIV, END,
          // AVG(sqrt(|amp|))
          INPUT, 2, ABS, SQRT, CUMSUM, 12, STATE_LOAD, 2, DIV, END,
          // COUNT(*)
          STATE_LOAD, 2, END,
      };
      // Segment predicate: ABS(amplitude) > 0.
      std::vector<double> seg_bc = {INPUT, 2, ABS, CONST, 0, GT, END};
      std::vector<double> seg_consts = {0.0};
      burst_op = rtbot::make_burst_aggregate(
          "burst_agg", std::move(agg_bc), {},
          std::move(seg_bc), std::move(seg_consts),
          /*key_columns=*/{0, 1},
          /*num_agg_outputs=*/8,
          /*num_input_cols=*/3);
      burst_sink = rtbot::make_vector_number_collector("burst_sink");
      burst_op->connect(burst_sink, 0, 0);
    }

    // Burst-native fast path: skip the base_program entirely. Compute the
    // vibration_moments aggregate in one vectorized pass over the amplitude
    // buffer, wrap the 10-field result in a VectorNumberData message, and
    // feed it directly to each downstream preset pipeline. Exercises the
    // burst-aggregate ceiling through rtbot's downstream graph.
    auto feed_burst_native = [&](int bearing, rtbot::timestamp_t boundary_ts) {
      double agg[10];
      compute_vibration_moments_agg(amp_buf.data(), amp_buf.size(),
                                     static_cast<double>(bearing), 1.0, agg);
      // One burst of "samples" ingressed — charge the amplitude samples + the
      // sentinel so the msg/s number is comparable with the other modes.
      ingress_messages += static_cast<std::uint64_t>(cfg.samples_per_burst) + 1;
      ++base_outputs;
      ++program_receive_calls;  // the single aggregate emission
      for (auto& pipeline : pipelines) {
        ++pipeline->forwarded_messages;
        pipeline->program.send_vector(boundary_ts, agg, 10);
        std::size_t produced = 0;
        pipeline->program.drain_into(
            [&produced](std::int64_t, const double*, std::size_t) {
              ++produced;
            });
        pipeline->output_messages += produced;
        materialized_outputs += produced;
      }
    };

    // Feeder for --burst-aggregate: constructs the burst + sentinel as a
    // vector of Message<VectorNumberData>, hands the batch to BurstAggregate,
    // then forwards its emitted aggregate messages to the preset pipelines.
    auto feed_burst_aggregate = [&](int bearing) {
      std::vector<std::unique_ptr<rtbot::BaseMessage>> batch;
      batch.reserve(static_cast<std::size_t>(cfg.samples_per_burst) + 1);
      for (int sample = 0; sample < cfg.samples_per_burst; ++sample) {
        batch.push_back(make_vector_message(
            timestamp + sample,
            {static_cast<double>(bearing), 1.0, synthetic_amplitude(sample)}));
      }
      batch.push_back(make_vector_message(
          timestamp + cfg.samples_per_burst,
          {static_cast<double>(bearing), 1.0, 0.0}));

      ingress_messages += batch.size();
      ++program_receive_calls;

      burst_op->receive_data_batch(batch, /*port_index=*/0, /*debug=*/false);
      burst_op->execute(false);

      auto& q = burst_sink->get_data_queue(0);
      while (!q.empty()) {
        auto* vm = dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(q.front().get());
        if (!vm) {
          q.pop_front();
          continue;
        }
        ++base_outputs;
        for (auto& pipeline : pipelines) {
          ++pipeline->forwarded_messages;
          pipeline->program.send_vector(vm->time, vm->data.values->data(),
                                         vm->data.values->size());
          std::size_t produced = 0;
          pipeline->program.drain_into(
              [&produced](std::int64_t, const double*, std::size_t) {
                ++produced;
              });
          pipeline->output_messages += produced;
          materialized_outputs += produced;
        }
        q.pop_front();
      }
    };

    // Feeder for --burst-buffer: fills the pre-allocated raw row buffer with
    // the burst + sentinel, then hands it directly to BurstAggregate via
    // receive_data_buffer. Zero Message allocations anywhere in the hot
    // path — the downstream forward to preset pipelines is the only Message
    // creation per burst (one aggregate message in, two to four preset
    // outputs out).
    auto feed_burst_buffer = [&](int bearing) {
      const std::size_t n = static_cast<std::size_t>(cfg.samples_per_burst);
      // Fill rows: [device_id, channel_id, amplitude].
      for (std::size_t s = 0; s < n; ++s) {
        raw_rows[s * 3 + 0] = static_cast<double>(bearing);
        raw_rows[s * 3 + 1] = 1.0;
        raw_rows[s * 3 + 2] = synthetic_amplitude(static_cast<int>(s));
        raw_times[s] = timestamp + static_cast<rtbot::timestamp_t>(s);
      }
      // Sentinel row closes the segment.
      raw_rows[n * 3 + 0] = static_cast<double>(bearing);
      raw_rows[n * 3 + 1] = 1.0;
      raw_rows[n * 3 + 2] = 0.0;
      raw_times[n] = timestamp + static_cast<rtbot::timestamp_t>(n);

      ingress_messages += n + 1;
      ++program_receive_calls;

      burst_op->receive_data_buffer(raw_rows.data(), n + 1, 3,
                                      raw_times.data(), /*port=*/0,
                                      /*debug=*/false);
      burst_op->execute(false);

      auto& q = burst_sink->get_data_queue(0);
      while (!q.empty()) {
        auto* vm = dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(q.front().get());
        if (!vm) {
          q.pop_front();
          continue;
        }
        ++base_outputs;
        for (auto& pipeline : pipelines) {
          ++pipeline->forwarded_messages;
          pipeline->program.send_vector(vm->time, vm->data.values->data(),
                                         vm->data.values->size());
          std::size_t produced = 0;
          pipeline->program.drain_into(
              [&produced](std::int64_t, const double*, std::size_t) {
                ++produced;
              });
          pipeline->output_messages += produced;
          materialized_outputs += produced;
        }
        q.pop_front();
      }
    };

    for (int burst = 1; burst <= cfg.bursts; ++burst) {
      for (int bearing = 1; bearing <= cfg.bearings; ++bearing) {
        if (cfg.burst_buffer) {
          feed_burst_buffer(bearing);
        } else if (cfg.burst_aggregate) {
          feed_burst_aggregate(bearing);
        } else if (cfg.burst_native) {
          feed_burst_native(bearing, timestamp + cfg.samples_per_burst);
        } else if (cfg.batch) {
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
