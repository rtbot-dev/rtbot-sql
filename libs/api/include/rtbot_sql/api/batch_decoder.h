#pragma once

// Shared runtime-binding helpers for decoding rtbot Program outputs.
// Each language runtime (Java JNI, Python pybind, WASM) used to reimplement
// this iteration (dynamic_cast over NumberData / VectorNumberData, stable
// sort) in its own bindings file. Centralising here keeps the decode
// semantics (message ordering, value extraction) consistent across
// runtimes and leaves each binding to own only its output-format code
// (JSON, POJO, binary ByteBuffer, etc).

#include <algorithm>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rtbot/Message.h"
#include "rtbot/Program.h"

namespace rtbot_sql::api {

struct DecodedMessage {
  rtbot::timestamp_t timestamp = 0;
  std::vector<double> values;
  std::string operator_id;
  std::string port;
};

// Iterate a ProgramMsgBatch, extracting {ts, values, op_id, port} for
// every Message<NumberData> and Message<VectorNumberData>, and
// stable-sort by (timestamp, operator_id, port). Caller-facing output
// formats (JSON, pybind POJO, direct ByteBuffer) consume the returned
// vector.
inline std::vector<DecodedMessage> decode_program_batch(
    const rtbot::ProgramMsgBatch& batch) {
  std::vector<DecodedMessage> out;
  for (const auto& [operator_id, operator_batch] : batch) {
    for (const auto& [port, messages] : operator_batch) {
      for (const auto& message : messages) {
        if (auto* vec =
                dynamic_cast<rtbot::Message<rtbot::VectorNumberData>*>(
                    message.get())) {
          DecodedMessage m;
          m.timestamp = vec->time;
          if (vec->data.values) m.values = *vec->data.values;
          m.operator_id = operator_id;
          m.port = port;
          out.push_back(std::move(m));
          continue;
        }
        if (auto* num = dynamic_cast<rtbot::Message<rtbot::NumberData>*>(
                message.get())) {
          DecodedMessage m;
          m.timestamp = num->time;
          m.values = {num->data.value};
          m.operator_id = operator_id;
          m.port = port;
          out.push_back(std::move(m));
        }
      }
    }
  }
  std::stable_sort(out.begin(), out.end(),
                   [](const DecodedMessage& a, const DecodedMessage& b) {
                     return std::tie(a.timestamp, a.operator_id, a.port) <
                            std::tie(b.timestamp, b.operator_id, b.port);
                   });
  return out;
}

// Per-Program* registry of consolidated-session output operator ids and
// their caller-assigned indices. Used by runtimes that expose a binary
// session-output format keyed by index rather than operator id string
// (Java JNI today; Python/browser may adopt later).
//
// Thread-safety: the registry methods are mutex-protected; however, the
// typical access pattern is set-once-at-deploy / read-many-during-feed
// on a single thread, matching the single-threaded-per-Program contract
// documented elsewhere in the runtime.
class SessionOutputRegistry {
 public:
  struct Entry {
    std::vector<std::string> op_ids;                  // index -> op id
    std::unordered_map<std::string, int> id_to_idx;   // op id -> index
  };

  void register_outputs(const rtbot::Program* program,
                         std::vector<std::string> op_ids);
  void clear(const rtbot::Program* program);

  // Returns nullptr if no registration exists. The returned pointer is
  // valid until the next mutating call (register_outputs / clear) for
  // the same program — callers must not hold it across such calls.
  const Entry* lookup(const rtbot::Program* program) const;

  // Process-wide shared instance used by the JNI layer.
  static SessionOutputRegistry& instance();

 private:
  mutable std::mutex mu_;
  std::unordered_map<const rtbot::Program*, Entry> map_;
};

}  // namespace rtbot_sql::api
