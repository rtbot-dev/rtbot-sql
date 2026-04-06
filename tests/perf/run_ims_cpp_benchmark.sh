#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

TARGET="//tests/perf:ims_cpp_benchmark"

if [[ $# -gt 0 ]]; then
  echo "[run] bazel run ${TARGET} -- $*"
  bazel run "${TARGET}" -- "$@"
  exit 0
fi

echo "[scenario] Tier-1 baseline presets (01_rms,02_kurtosis)"
bazel run "${TARGET}" -- \
  --presets=01_rms,02_kurtosis \
  --bearings=4 \
  --samples-per-burst=512 \
  --bursts=300 \
  --progress-every=20

echo
echo "[scenario] Full demo presets (01,02,04,05)"
bazel run "${TARGET}" -- \
  --presets=01_rms,02_kurtosis,04_crest,05_dashboard \
  --bearings=4 \
  --samples-per-burst=512 \
  --bursts=300 \
  --progress-every=20
