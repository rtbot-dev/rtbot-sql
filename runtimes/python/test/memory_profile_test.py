"""Memory profiling test for the rotating-machinery preset architecture.

Validates that memory behavior is predictable during long-running ingestion:
  - C++ core: bounded (fixed topology, ring buffers, O(1) accumulators)
  - Python InMemoryStreamStore: grows linearly with stored output

This test quantifies the growth rate so we can make informed decisions about
retention policies for production deployments.

Key finding: The INSERT INTO path always stores raw rows in the stream store.
The insert_dataframe(store_raw=False) path skips raw storage. In production
(notebook / services), always use insert_dataframe with store_raw=False to
avoid O(N_raw) memory growth.

Usage:
  Bazel:     bazel test //runtimes/python:memory_profile_test
  Standalone: python runtimes/python/test/memory_profile_test.py
"""

import gc
import resource
import sys
import unittest

import rtbot_sql


# ─── SQL Definitions (mirror the preset architecture) ─────────────────────

SQL_STREAM = """
CREATE STREAM vibration_raw (
    device_id  DOUBLE,
    channel_id DOUBLE,
    amplitude  DOUBLE
)
"""

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

SQL_CREST = """
CREATE MATERIALIZED VIEW crest_clearance_impulse AS
  SELECT device_id,
         channel_id,
         peak_value / POWER(ex2, 0.5) AS crest_factor,
         peak_value / POWER(mean_sqrt_abs, 2) AS clearance_factor,
         peak_value / mean_abs AS impulse_factor,
         POWER(ex2, 0.5) / mean_abs AS shape_factor,
         2.0 * peak_value AS peak_to_peak
  FROM vibration_moments
  WHERE sample_count > 1
"""

SQL_DASHBOARD = """
CREATE MATERIALIZED VIEW statistical_dashboard AS
  SELECT device_id,
         channel_id,
         mean_value,
         ex2 - mean_value * mean_value AS variance,
         POWER(ex2 - mean_value * mean_value, 0.5) AS std_dev,
         POWER(ex2, 0.5) AS rms,
         (ex3 - 3.0 * mean_value * ex2 + 2.0 * mean_value * mean_value * mean_value)
           / POWER(ex2 - mean_value * mean_value, 1.5) AS skewness,
         (ex4 - 4.0 * ex3 * mean_value + 6.0 * ex2 * mean_value * mean_value - 3.0 * mean_value * mean_value * mean_value * mean_value)
           / POWER(ex2 - mean_value * mean_value, 2) AS kurtosis
  FROM vibration_moments
  WHERE sample_count > 1
"""


def _get_rss_mb() -> float:
  """Current process peak resident set size in MB (macOS/Linux).

  Note: ru_maxrss is the HIGH WATER MARK — it never decreases, even after
  gc.collect(). It shows the maximum RSS reached during the process lifetime.
  """
  usage = resource.getrusage(resource.RUSAGE_SELF)
  if sys.platform == "darwin":
    # macOS reports in bytes
    return usage.ru_maxrss / (1024 * 1024)
  else:
    # Linux reports in KB
    return usage.ru_maxrss / 1024


def _count_store_rows(sql: rtbot_sql.RtBotSql, names: list) -> dict:
  """Count rows per stream/view in the store."""
  return {name: len(sql._store.read(name)) for name in names}


def _create_preset_instance() -> rtbot_sql.RtBotSql:
  """Create and wire up the full preset architecture."""
  sql = rtbot_sql.RtBotSql()
  sql.execute(SQL_STREAM)
  sql.execute(SQL_BASE_VIEW)
  sql.execute(SQL_RMS)
  sql.execute(SQL_KURTOSIS)
  sql.execute(SQL_CREST)
  sql.execute(SQL_DASHBOARD)
  return sql


ALL_MV_NAMES = [
    "rms_trend", "kurtosis_trend",
    "crest_clearance_impulse", "statistical_dashboard",
]


def _generate_burst(
    device_id: float,
    channel_id: float,
    base_ts: int,
    samples_per_burst: int = 512,
) -> list:
  """Generate a synthetic vibration burst: N data samples + 1 sentinel.

  Returns list of (timestamp, [device_id, channel_id, amplitude]) tuples.
  Uses a simple deterministic pattern (no randomness needed for memory test).
  """
  rows = []
  for i in range(samples_per_burst):
    amp = 0.5 + 0.3 * (1.0 if (i % 7) < 4 else -1.0)
    rows.append((base_ts + i, [device_id, channel_id, amp]))
  # Sentinel row (amplitude = 0 triggers GROUP BY emission)
  rows.append((base_ts + samples_per_burst, [device_id, channel_id, 0.0]))
  return rows


def _ingest_bursts(sql, num_bursts, num_bearings, samples_per_burst, start_ts):
  """Ingest bursts via INSERT INTO. Returns the next timestamp."""
  ts = start_ts
  for _burst_idx in range(num_bursts):
    for bearing_id in range(1, num_bearings + 1):
      burst = _generate_burst(
          device_id=float(bearing_id),
          channel_id=1.0,
          base_ts=ts,
          samples_per_burst=samples_per_burst,
      )
      for timestamp, values in burst:
        sql.execute(
            f"INSERT INTO vibration_raw VALUES ({values[0]}, {values[1]}, {values[2]})"
        )
      ts += samples_per_burst + 100
  return ts


class MemoryProfileTest(unittest.TestCase):
  """Verify memory behavior during sustained ingestion."""

  NUM_BEARINGS = 4
  SAMPLES_PER_BURST = 512   # Smaller than real IMS (20480) for speed
  NUM_BURSTS = 100           # 100 bursts × 4 bearings = 400 GROUP BY emissions
  SAMPLE_INTERVAL = 25       # Measure every 25 bursts

  def test_store_growth_is_linear_with_materialized_output(self):
    """The InMemoryStreamStore should grow linearly with the number of
    materialized view output rows.

    Note: INSERT INTO always stores raw rows — this test measures total
    store growth (raw + MV). The insert_dataframe(store_raw=False) path
    avoids storing raw rows; that's tested separately in the notebook.
    """
    sql = _create_preset_instance()

    measurements = []  # (burst_index, mv_row_count, raw_row_count)

    ts = 1_000_000_000_000

    for burst_idx in range(self.NUM_BURSTS):
      for bearing_id in range(1, self.NUM_BEARINGS + 1):
        burst = _generate_burst(
            device_id=float(bearing_id),
            channel_id=1.0,
            base_ts=ts,
            samples_per_burst=self.SAMPLES_PER_BURST,
        )
        for timestamp, values in burst:
          sql.execute(
              f"INSERT INTO vibration_raw VALUES ({values[0]}, {values[1]}, {values[2]})"
          )
        ts += self.SAMPLES_PER_BURST + 100

      if (burst_idx + 1) % self.SAMPLE_INTERVAL == 0 or burst_idx == 0:
        mv_rows = sum(len(sql._store.read(n)) for n in ALL_MV_NAMES)
        raw_rows = len(sql._store.read("vibration_raw"))
        measurements.append((burst_idx + 1, mv_rows, raw_rows))

    gc.collect()
    rss_after = _get_rss_mb()

    # ── Report ───────────────────────────────────────────────────────────
    total_input = self.NUM_BURSTS * self.NUM_BEARINGS * (self.SAMPLES_PER_BURST + 1)
    total_mv = sum(len(sql._store.read(n)) for n in ALL_MV_NAMES)
    total_raw = len(sql._store.read("vibration_raw"))

    print("\n" + "=" * 72)
    print("MEMORY PROFILE: Rotating Machinery Preset Architecture")
    print("=" * 72)
    print(f"Configuration:")
    print(f"  Bearings:          {self.NUM_BEARINGS}")
    print(f"  Samples/burst:     {self.SAMPLES_PER_BURST}")
    print(f"  Bursts:            {self.NUM_BURSTS}")
    print(f"  Total input rows:  {total_input:,}")
    print(f"  Ingestion path:    INSERT INTO (stores raw + MV output)")
    print(f"  base VIEW stored:  False (vibration_moments is plain VIEW)")
    print()

    print("Store contents:")
    view_rows = len(sql._store.read("vibration_moments"))
    print(f"  {'vibration_raw (raw)':30s}: {total_raw:>8,} rows")
    print(f"  {'vibration_moments (VIEW)':30s}: {view_rows:>8,} rows (should be 0)")
    for name in ALL_MV_NAMES:
      count = len(sql._store.read(name))
      print(f"  {name:30s}: {count:>8,} rows")
    print(f"  {'TOTAL (raw + MV)':30s}: {total_raw + total_mv:>8,} rows")
    print()

    print("Growth profile:")
    print(f"  {'Burst':>6s}  {'MV Rows':>10s}  {'Raw Rows':>10s}  {'Ratio':>8s}")
    print(f"  {'─' * 6}  {'─' * 10}  {'─' * 10}  {'─' * 8}")
    for burst_idx, mv, raw in measurements:
      ratio = raw / mv if mv > 0 else 0
      print(f"  {burst_idx:6d}  {mv:10,}  {raw:10,}  {ratio:8.0f}:1")
    print()

    # Calculate MV growth rate
    if len(measurements) >= 2:
      _, first_mv, _ = measurements[0]
      _, last_mv, _ = measurements[-1]
      burst_delta = measurements[-1][0] - measurements[0][0]
      mv_delta = last_mv - first_mv
      if burst_delta > 0:
        mv_per_burst = mv_delta / burst_delta
        print(f"MV growth rate: {mv_per_burst:.1f} rows/burst cycle")
        print(f"  (Expected: {self.NUM_BEARINGS * 4} = "
              f"{self.NUM_BEARINGS} bearings × 4 MVs)")

    print(f"\nPeak RSS: {rss_after:.1f} MB")
    print()

    # Memory breakdown estimate
    # Each stored Message: ~300 bytes in Python (int + list of floats + dataclass overhead)
    bytes_per_msg = 300
    raw_est_mb = (total_raw * bytes_per_msg) / (1024 * 1024)
    mv_est_mb = (total_mv * bytes_per_msg) / (1024 * 1024)
    print(f"Estimated memory breakdown (~{bytes_per_msg} bytes/Message):")
    print(f"  Raw stream:  {raw_est_mb:7.1f} MB ({total_raw:,} rows)")
    print(f"  MV output:   {mv_est_mb:7.1f} MB ({total_mv:,} rows)")
    print(f"  Total store: {raw_est_mb + mv_est_mb:7.1f} MB")
    print()

    # Production projections
    ims_bursts = 2156
    ims_samples = 20480
    ims_bearings = 4
    ims_raw = ims_bursts * ims_bearings * (ims_samples + 1)
    ims_mv = ims_bursts * ims_bearings * 4
    print(f"IMS Dataset 1 projection (2156 bursts × 4 bearings × 20480 samples):")
    print(f"  insert_dataframe(store_raw=False):")
    print(f"    MV rows:   {ims_mv:>10,}  →  {ims_mv * bytes_per_msg / (1024**2):>7.1f} MB")
    print(f"  insert_dataframe(store_raw=True) or INSERT INTO:")
    print(f"    Raw rows:  {ims_raw:>10,}  →  {ims_raw * bytes_per_msg / (1024**2):>7.1f} MB")
    print(f"    MV rows:   {ims_mv:>10,}  →  {ims_mv * bytes_per_msg / (1024**2):>7.1f} MB")
    print(f"    TOTAL:     {ims_raw + ims_mv:>10,}  →  {(ims_raw + ims_mv) * bytes_per_msg / (1024**2):>7.1f} MB")
    print("=" * 72)

    # ── Assertions ────────────────────────────────────────────────────────

    # 1. Plain VIEW should NOT store anything
    self.assertEqual(view_rows, 0,
                     "vibration_moments (plain VIEW) should not store output")

    # 2. Each MV should have exactly NUM_BURSTS × NUM_BEARINGS rows
    expected_per_mv = self.NUM_BURSTS * self.NUM_BEARINGS
    for name in ALL_MV_NAMES:
      count = len(sql._store.read(name))
      self.assertEqual(count, expected_per_mv,
                       f"{name}: expected {expected_per_mv}, got {count}")

    # 3. Raw stream should have all input rows (INSERT INTO stores everything)
    self.assertEqual(total_raw, total_input)

    # 4. MV growth rate should be exactly NUM_BEARINGS × 4 per burst cycle
    if len(measurements) >= 2:
      self.assertAlmostEqual(
          mv_per_burst, self.NUM_BEARINGS * 4, delta=0.1,
          msg=f"MV growth should be {self.NUM_BEARINGS * 4}/burst",
      )

  def test_plain_view_never_stores(self):
    """vibration_moments is a plain VIEW — it should never accumulate rows
    regardless of how much data flows through it."""
    sql = _create_preset_instance()

    _ingest_bursts(sql, num_bursts=10, num_bearings=2,
                   samples_per_burst=64, start_ts=1_000_000)

    view_rows = len(sql._store.read("vibration_moments"))
    self.assertEqual(view_rows, 0,
                     "Plain VIEW should never store output")

    # But downstream MVs should have received data
    for name in ALL_MV_NAMES:
      count = len(sql._store.read(name))
      self.assertGreater(count, 0,
                         f"{name} should have data from VIEW propagation")

  def test_insert_into_always_stores_raw(self):
    """Document that INSERT INTO always stores raw rows in the stream store.
    The store_raw=False optimization only applies to insert_dataframe."""
    sql = _create_preset_instance()

    _ingest_bursts(sql, num_bursts=1, num_bearings=1,
                   samples_per_burst=100, start_ts=1_000_000)

    raw_count = len(sql._store.read("vibration_raw"))
    self.assertEqual(raw_count, 101,  # 100 data + 1 sentinel
                     "INSERT INTO should store all raw rows")


if __name__ == "__main__":
  unittest.main()
