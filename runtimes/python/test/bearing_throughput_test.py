"""Python-side throughput benchmark mirroring Java's BearingLoadTest.

Measures msgs/sec of the rotating-machinery preset under
insert_dataframe()-driven ingestion. Two modes are exposed via the
RTBOT_BENCHMARK_SESSION env var (matching Java's -Drtbot.benchmark.session):

  - baseline (default): per-view cascade through LocalPipelineRunner.
  - consolidated session: single rtbot Program, one native feed per batch.

Usage:
  bazel test //runtimes/python:bearing_throughput_test --test_output=all
  bazel test //runtimes/python:bearing_throughput_test --test_output=all \
      --test_env=RTBOT_BENCHMARK_SESSION=1
"""

import gc
import os
import time
import unittest

import numpy as np

import rtbot_sql


SQL_STREAM = (
    "CREATE STREAM vibration_raw ("
    "device_id DOUBLE, channel_id DOUBLE, amplitude DOUBLE)"
)

SQL_BASE_VIEW = """
CREATE VIEW vibration_moments AS
  SELECT device_id,
         channel_id,
         AVG(amplitude)                  AS mean_value,
         AVG(POWER(amplitude, 2))        AS ex2,
         AVG(POWER(amplitude, 3))        AS ex3,
         AVG(POWER(amplitude, 4))        AS ex4,
         MAX(ABS(amplitude))             AS peak_value,
         AVG(ABS(amplitude))             AS mean_abs,
         AVG(POWER(ABS(amplitude), 0.5)) AS mean_sqrt_abs,
         COUNT(*)                         AS sample_count
  FROM vibration_raw
  GROUP BY device_id, channel_id, ABS(amplitude) > 0
"""

SQL_RMS = """
CREATE MATERIALIZED VIEW rms_trend AS
  SELECT device_id,
         channel_id,
         POWER(ex2, 0.5) AS rms
  FROM vibration_moments
  WHERE sample_count > 1
"""

SQL_KURTOSIS = """
CREATE MATERIALIZED VIEW kurtosis_trend AS
  SELECT device_id,
         channel_id,
         ex2 - mean_value * mean_value AS variance,
         POWER(ex2 - mean_value * mean_value, 0.5) AS std_dev,
         ex4 - 4.0 * ex3 * mean_value + 6.0 * ex2 * mean_value * mean_value - 3.0 * mean_value * mean_value * mean_value * mean_value AS m4,
         (ex4 - 4.0 * ex3 * mean_value + 6.0 * ex2 * mean_value * mean_value - 3.0 * mean_value * mean_value * mean_value * mean_value)
           / POWER(ex2 - mean_value * mean_value, 2) AS kurtosis
  FROM vibration_moments
  WHERE sample_count > 1
"""


def _generate_burst_amplitudes(num_samples: int) -> np.ndarray:
    amps = np.empty(num_samples, dtype=np.float64)
    for i in range(num_samples):
        amps[i] = 0.5 + 0.3 * (1.0 if (i % 7) < 4 else -1.0)
    return amps


def _bool_env(name: str, *, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _int_env(name: str, default: int) -> int:
    v = os.environ.get(name, "")
    try:
        return int(v) if v else default
    except ValueError:
        return default


class BearingThroughputTest(unittest.TestCase):
    """Match Java's BearingLoadTest: 4 bearings × 512 samples × 300 bursts."""

    NUM_BEARINGS = _int_env("RTBOT_BENCHMARK_BEARINGS", 4)
    SAMPLES_PER_BURST = _int_env("RTBOT_BENCHMARK_SAMPLES", 512)
    NUM_BURSTS = _int_env("RTBOT_BENCHMARK_BURSTS", 300)

    def test_bearing_preset_throughput(self):
        # Session mode is the default; RTBOT_BENCHMARK_SESSION=0 opts
        # out to measure the per-view-cascade fallback path.
        use_session = _bool_env("RTBOT_BENCHMARK_SESSION", default=True)
        sql = rtbot_sql.RtBotSql(use_consolidated_session=use_session)
        sql.execute(SQL_STREAM)
        sql.execute(SQL_BASE_VIEW)
        sql.execute(SQL_RMS)
        sql.execute(SQL_KURTOSIS)

        burst_size = self.SAMPLES_PER_BURST + 1
        amps_template = _generate_burst_amplitudes(self.SAMPLES_PER_BURST)

        gc.collect()
        wall_start = time.perf_counter()

        ts_cursor = 1_000_000_000_000
        total_messages = 0

        for _burst_idx in range(self.NUM_BURSTS):
            for bearing_id in range(1, self.NUM_BEARINGS + 1):
                ts = np.empty(burst_size, dtype=np.int64)
                # Row-major (n_rows, n_cols) matching the stream schema
                # order: device_id, channel_id, amplitude.
                vals = np.empty((burst_size, 3), dtype=np.float64)
                ts[: self.SAMPLES_PER_BURST] = np.arange(
                    ts_cursor, ts_cursor + self.SAMPLES_PER_BURST,
                    dtype=np.int64,
                )
                vals[: self.SAMPLES_PER_BURST, 0] = float(bearing_id)
                vals[: self.SAMPLES_PER_BURST, 1] = 1.0
                vals[: self.SAMPLES_PER_BURST, 2] = amps_template
                # Sentinel row (amplitude = 0 triggers GROUP BY emission).
                ts[self.SAMPLES_PER_BURST] = ts_cursor + self.SAMPLES_PER_BURST
                vals[self.SAMPLES_PER_BURST, 0] = float(bearing_id)
                vals[self.SAMPLES_PER_BURST, 1] = 1.0
                vals[self.SAMPLES_PER_BURST, 2] = 0.0

                sql.insert_buffer("vibration_raw", ts, vals, store_raw=False)
                total_messages += burst_size

                ts_cursor += self.SAMPLES_PER_BURST + 100

        wall_end = time.perf_counter()
        wall_s = wall_end - wall_start
        msgs_per_sec = total_messages / wall_s if wall_s > 0 else float("inf")

        expected = self.NUM_BURSTS * self.NUM_BEARINGS
        rms_rows = len(sql._store.read("rms_trend"))
        kurtosis_rows = len(sql._store.read("kurtosis_trend"))

        mode = "CONSOLIDATED SESSION" if use_session else "baseline (per-view)"
        print()
        print("=" * 64)
        print(f"BEARING PRESET THROUGHPUT (Python) — {mode}")
        print("=" * 64)
        print(
            f"Config: {self.NUM_BEARINGS} bearings × {self.SAMPLES_PER_BURST} "
            f"samples × {self.NUM_BURSTS} bursts"
        )
        print(f"Total messages: {total_messages:,}")
        print(f"Wall:           {wall_s:.3f} s")
        print(f"Messages/sec:   {msgs_per_sec:,.0f}")
        print(f"rms_trend:      {rms_rows} (expected {expected})")
        print(f"kurtosis_trend: {kurtosis_rows} (expected {expected})")
        print("=" * 64)

        self.assertEqual(rms_rows, expected)
        self.assertEqual(kurtosis_rows, expected)
        self.assertGreater(msgs_per_sec, 1000)


if __name__ == "__main__":
    unittest.main()
