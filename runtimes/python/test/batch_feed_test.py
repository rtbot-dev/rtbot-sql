"""Tests for insert_dataframe performance optimization (batch feed path).

Run from an environment with numpy + pandas:
  python -m pytest runtimes/python/test/batch_feed_test.py -v

Or directly:
  python runtimes/python/test/batch_feed_test.py
"""

import math
import time
import unittest

import numpy as np
import pandas as pd

import rtbot_sql
from rtbot_sql import compiler


def _columns_and_rows(result):
  if isinstance(result, dict):
    columns = list(result["columns"])
    rows = [list(row) for row in result["rows"]]
    if "timestamps" in result:
      time_column = result.get("time_column", "time")
      columns = [time_column] + columns
      rows = [[float(ts)] + row for ts, row in zip(result["timestamps"], rows)]
    return columns, rows

  if hasattr(result, "columns") and hasattr(result, "values"):
    return list(result.columns), result.values.tolist()

  raise TypeError(f"Unsupported result type: {type(result)!r}")


class FeedBatchCorrectnessTest(unittest.TestCase):
  """Prove feed_batch gives bit-identical results to row-by-row feed."""

  def test_feed_batch_matches_feed_loop_moving_average(self):
    """MOVING_AVERAGE via feed_batch == via feed loop."""
    # --- Row-by-row baseline (slow path) ---
    sql_slow = rtbot_sql.RtBotSql()
    sql_slow.execute("CREATE STREAM sensor (temperature DOUBLE)")
    sql_slow.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT temperature, MOVING_AVERAGE(temperature, 5) AS avg_temp "
        "FROM sensor"
    )
    readings = [{"time": i, "temperature": 20.0 + math.sin(i * 0.1)} for i in range(100)]
    sql_slow.insert_dataframe("sensor", readings)

    result_slow = sql_slow.execute("SELECT * FROM stats LIMIT 100")
    _, rows_slow = _columns_and_rows(result_slow)

    # --- Batch path (fast path via DataFrame) ---
    sql_fast = rtbot_sql.RtBotSql()
    sql_fast.execute("CREATE STREAM sensor (temperature DOUBLE)")
    sql_fast.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT temperature, MOVING_AVERAGE(temperature, 5) AS avg_temp "
        "FROM sensor"
    )
    df = pd.DataFrame(readings)
    sql_fast.insert_dataframe("sensor", df)

    result_fast = sql_fast.execute("SELECT * FROM stats LIMIT 100")
    _, rows_fast = _columns_and_rows(result_fast)

    # Must be identical
    self.assertEqual(len(rows_slow), len(rows_fast))
    for i, (slow, fast) in enumerate(zip(rows_slow, rows_fast)):
      self.assertEqual(len(slow), len(fast), f"Row {i} length mismatch")
      for j, (s, f) in enumerate(zip(slow, fast)):
        self.assertAlmostEqual(s, f, places=10,
                               msg=f"Row {i} col {j}: slow={s} fast={f}")

  def test_feed_batch_matches_feed_loop_group_by(self):
    """GROUP BY (KEYED view) via feed_batch == via feed loop."""
    readings = [
        {"time": 1, "device": 1.0, "value": 10.0},
        {"time": 2, "device": 2.0, "value": 20.0},
        {"time": 3, "device": 1.0, "value": 30.0},
        {"time": 4, "device": 2.0, "value": 40.0},
        {"time": 5, "device": 1.0, "value": 50.0},
    ]

    # Slow path (list-of-dicts)
    sql_slow = rtbot_sql.RtBotSql()
    sql_slow.execute("CREATE STREAM sensors (device DOUBLE, value DOUBLE)")
    sql_slow.execute(
        "CREATE MATERIALIZED VIEW agg AS "
        "SELECT device, SUM(value) AS total, COUNT(*) AS cnt "
        "FROM sensors GROUP BY device"
    )
    sql_slow.insert_dataframe("sensors", readings)
    result_slow = sql_slow.execute("SELECT * FROM agg")
    _, rows_slow = _columns_and_rows(result_slow)

    # Fast path (DataFrame)
    sql_fast = rtbot_sql.RtBotSql()
    sql_fast.execute("CREATE STREAM sensors (device DOUBLE, value DOUBLE)")
    sql_fast.execute(
        "CREATE MATERIALIZED VIEW agg AS "
        "SELECT device, SUM(value) AS total, COUNT(*) AS cnt "
        "FROM sensors GROUP BY device"
    )
    df = pd.DataFrame(readings)
    sql_fast.insert_dataframe("sensors", df)
    result_fast = sql_fast.execute("SELECT * FROM agg")
    _, rows_fast = _columns_and_rows(result_fast)

    # Sort by device for comparison
    rows_slow.sort(key=lambda r: r[1])
    rows_fast.sort(key=lambda r: r[1])

    self.assertEqual(len(rows_slow), len(rows_fast))
    for i, (slow, fast) in enumerate(zip(rows_slow, rows_fast)):
      for j, (s, f) in enumerate(zip(slow, fast)):
        self.assertAlmostEqual(s, f, places=10,
                               msg=f"Row {i} col {j}: slow={s} fast={f}")

  def test_feed_batch_matches_segment_group_by(self):
    """Segment GROUP BY (the IMS notebook pattern) via batch == via slow path."""
    # This is the critical test for the rotating-machinery use case.
    # Pattern: GROUP BY device_id, channel_id, ABS(amplitude) > 0
    readings = []
    # Burst 1: device 1
    for i in range(100):
      readings.append({"time": i + 1, "device_id": 1.0, "channel_id": 1.0, "amplitude": math.sin(i * 0.1) + 0.5})
    # Sentinel
    readings.append({"time": 101, "device_id": 1.0, "channel_id": 1.0, "amplitude": 0.0})
    # Burst 2: device 2
    for i in range(100):
      readings.append({"time": i + 200, "device_id": 2.0, "channel_id": 1.0, "amplitude": math.cos(i * 0.1) + 0.5})
    # Sentinel
    readings.append({"time": 301, "device_id": 2.0, "channel_id": 1.0, "amplitude": 0.0})

    sql_text = (
        "CREATE MATERIALIZED VIEW rms AS "
        "SELECT device_id, channel_id, "
        "COUNT(*) AS sample_count, "
        "POWER(SUM(amplitude * amplitude) / COUNT(*), 0.5) AS rms "
        "FROM vibration "
        "GROUP BY device_id, channel_id, ABS(amplitude) > 0"
    )

    # Slow path
    sql_slow = rtbot_sql.RtBotSql()
    sql_slow.execute("CREATE STREAM vibration (device_id DOUBLE, channel_id DOUBLE, amplitude DOUBLE)")
    sql_slow.execute(sql_text)
    sql_slow.insert_dataframe("vibration", readings)
    result_slow = sql_slow.execute("SELECT * FROM rms WHERE sample_count > 1")
    _, rows_slow = _columns_and_rows(result_slow)

    # Fast path
    sql_fast = rtbot_sql.RtBotSql()
    sql_fast.execute("CREATE STREAM vibration (device_id DOUBLE, channel_id DOUBLE, amplitude DOUBLE)")
    sql_fast.execute(sql_text)
    df = pd.DataFrame(readings)
    sql_fast.insert_dataframe("vibration", df)
    result_fast = sql_fast.execute("SELECT * FROM rms WHERE sample_count > 1")
    _, rows_fast = _columns_and_rows(result_fast)

    # Must match
    rows_slow.sort(key=lambda r: r[1])
    rows_fast.sort(key=lambda r: r[1])

    self.assertEqual(len(rows_slow), len(rows_fast),
                     f"Row count mismatch: slow={len(rows_slow)} fast={len(rows_fast)}")
    for i, (slow, fast) in enumerate(zip(rows_slow, rows_fast)):
      for j, (s, f) in enumerate(zip(slow, fast)):
        self.assertAlmostEqual(s, f, places=10,
                               msg=f"Row {i} col {j}: slow={s} fast={f}")

  def test_feed_batch_cascading_views(self):
    """Views depending on other views work correctly via batch path."""
    readings = []
    for i in range(200):
      readings.append({"time": i, "temperature": 20.0 + 2.0 * math.sin(i * 0.05)})

    sql_text_base = (
        "CREATE MATERIALIZED VIEW base_stats AS "
        "SELECT temperature, "
        "MOVING_AVERAGE(temperature, 10) AS avg_temp, "
        "MOVING_STD(temperature, 10) AS std_temp "
        "FROM sensor"
    )
    sql_text_derived = (
        "CREATE MATERIALIZED VIEW anomalies AS "
        "SELECT temperature, avg_temp, std_temp "
        "FROM base_stats"
    )

    # Slow path
    sql_slow = rtbot_sql.RtBotSql()
    sql_slow.execute("CREATE STREAM sensor (temperature DOUBLE)")
    sql_slow.execute(sql_text_base)
    sql_slow.execute(sql_text_derived)
    sql_slow.insert_dataframe("sensor", readings)
    result_slow = sql_slow.execute("SELECT * FROM anomalies LIMIT 200")
    _, rows_slow = _columns_and_rows(result_slow)

    # Fast path
    sql_fast = rtbot_sql.RtBotSql()
    sql_fast.execute("CREATE STREAM sensor (temperature DOUBLE)")
    sql_fast.execute(sql_text_base)
    sql_fast.execute(sql_text_derived)
    df = pd.DataFrame(readings)
    sql_fast.insert_dataframe("sensor", df)
    result_fast = sql_fast.execute("SELECT * FROM anomalies LIMIT 200")
    _, rows_fast = _columns_and_rows(result_fast)

    self.assertEqual(len(rows_slow), len(rows_fast))
    for i, (slow, fast) in enumerate(zip(rows_slow, rows_fast)):
      for j, (s, f) in enumerate(zip(slow, fast)):
        self.assertAlmostEqual(s, f, places=10,
                               msg=f"Row {i} col {j}: slow={s} fast={f}")

  def test_keyed_view_tracks_keys_correctly(self):
    """KEYED views track known keys via batch path."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM sensors (device DOUBLE, value DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW agg AS "
        "SELECT device, SUM(value) AS total, COUNT(*) AS cnt "
        "FROM sensors GROUP BY device"
    )

    df = pd.DataFrame({
        "time": [1, 2, 3, 4, 5],
        "device": [1.0, 2.0, 3.0, 1.0, 2.0],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    sql.insert_dataframe("sensors", df)

    result = sql.execute("SELECT * FROM agg")
    _, rows = _columns_and_rows(result)

    actual = {row[1]: row[1:] for row in rows}
    self.assertAlmostEqual(actual[1.0][1], 50.0)  # SUM(10+40)
    self.assertAlmostEqual(actual[1.0][2], 2.0)   # COUNT
    self.assertAlmostEqual(actual[2.0][1], 70.0)  # SUM(20+50)
    self.assertAlmostEqual(actual[2.0][2], 2.0)   # COUNT
    self.assertAlmostEqual(actual[3.0][1], 30.0)  # SUM(30)
    self.assertAlmostEqual(actual[3.0][2], 1.0)   # COUNT

  def test_store_raw_false_skips_raw_storage(self):
    """store_raw=False doesn't store raw stream but views still work."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM sensor (temperature DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT temperature, MOVING_AVERAGE(temperature, 5) AS avg_temp "
        "FROM sensor"
    )

    df = pd.DataFrame({
        "time": list(range(20)),
        "temperature": [20.0 + i * 0.5 for i in range(20)],
    })
    sql.insert_dataframe("sensor", df, store_raw=False)

    # Raw stream should have no data
    raw_messages = sql.get_store().read("sensor")
    self.assertEqual(len(raw_messages), 0)

    # View should still have data
    result = sql.execute("SELECT * FROM stats LIMIT 20")
    _, rows = _columns_and_rows(result)
    self.assertGreater(len(rows), 0)

  def test_column_map_works_with_fast_path(self):
    """column_map remapping works with DataFrame fast path."""
    sql = rtbot_sql.RtBotSql()
    sql.execute(
        "CREATE STREAM trades (price DOUBLE, qty DOUBLE, "
        "quote_qty DOUBLE, is_buyer_maker DOUBLE)"
    )

    df = pd.DataFrame({
        "timestamp": [1700000001000, 1700000001001],
        "price": [10.0, 11.0],
        "qty": [1.5, 2.5],
        "quoteQty": [15.0, 27.5],
        "isBuyerMaker_num": [1.0, 0.0],
    })
    sql.insert_dataframe(
        "trades",
        df,
        column_map={
            "quote_qty": "quoteQty",
            "is_buyer_maker": "isBuyerMaker_num",
        },
    )

    result = sql.execute("SELECT * FROM trades LIMIT 2")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(
        columns,
        ["time", "price", "qty", "quote_qty", "is_buyer_maker"],
    )
    self.assertEqual(
        [row[1:] for row in rows],
        [[10.0, 1.5, 15.0, 1.0], [11.0, 2.5, 27.5, 0.0]],
    )

  def test_multi_source_view_non_matching_timestamps(self):
    """Multi-source view with non-matching timestamps produces no output.

    RTBot operators synchronize on timestamps internally. The runtime simply
    routes each source's messages to the correct input port.
    """
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM btc (price DOUBLE)")
    sql.execute("CREATE STREAM eth (price DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW cross_stats AS "
        "SELECT b.price AS btc_price, e.price AS eth_price, b.price - e.price AS spread "
        "FROM btc b, eth e"
    )

    df_btc = pd.DataFrame({
        "time": [1700000001000, 1700000003000],
        "price": [100.0, 101.0],
    })
    df_eth = pd.DataFrame({
        "time": [1700000002000, 1700000004000],
        "price": [95.0, 95.0],
    })
    sql.insert_dataframe("btc", df_btc)
    sql.insert_dataframe("eth", df_eth)

    result = sql.execute("SELECT * FROM cross_stats LIMIT 10")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(rows, [])

  def test_multi_source_view_matching_timestamps(self):
    """Multi-source view with matching timestamps produces correct output."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM btc (price DOUBLE)")
    sql.execute("CREATE STREAM eth (price DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW cross_stats AS "
        "SELECT b.price AS btc_price, e.price AS eth_price, b.price - e.price AS spread "
        "FROM btc b, eth e"
    )

    df_btc = pd.DataFrame({
        "time": [1700000001000, 1700000003000],
        "price": [100.0, 101.0],
    })
    df_eth = pd.DataFrame({
        "time": [1700000001000, 1700000003000],
        "price": [95.0, 95.0],
    })
    sql.insert_dataframe("btc", df_btc)
    sql.insert_dataframe("eth", df_eth)

    result = sql.execute("SELECT * FROM cross_stats LIMIT 10")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(
        [row[1:] for row in rows],
        [[100.0, 95.0, 5.0], [101.0, 95.0, 6.0]],
    )


class FeedBatchPerformanceTest(unittest.TestCase):
  """Verify batch path performance characteristics.

  The batch path (DataFrame + numpy + C++ feed_batch) eliminates per-row
  Python→C++ crossing overhead. With complex operator graphs, the C++
  execution time per row dominates, so the speedup is modest (~1.1-1.5x).
  The primary benefit is avoiding Python dict iteration (iterrows was ~3K rps)
  and enabling store_raw=False to skip raw storage.

  These tests verify:
  1. Batch path is at least as fast as row-by-row (no regression).
  2. Throughput meets minimum for the IMS use case (> 5K rows/sec).
  3. store_raw=False is at least as fast as store_raw=True.
  """

  def test_batch_not_slower_than_row_by_row(self):
    """DataFrame batch path must not be slower than list-of-dicts."""
    n_rows = 20000
    readings_dicts = [
        {"time": i, "device": float(i % 4 + 1), "channel": 1.0,
         "amplitude": math.sin(i * 0.01) + 0.5}
        for i in range(n_rows)
    ]
    df = pd.DataFrame(readings_dicts)

    view_stmts = [
        ("CREATE MATERIALIZED VIEW rms AS "
         "SELECT device, channel, COUNT(*) AS n, "
         "SUM(amplitude * amplitude) / COUNT(*) AS mean_sq "
         "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"),
        ("CREATE MATERIALIZED VIEW kurt AS "
         "SELECT device, channel, COUNT(*) AS n "
         "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"),
        ("CREATE MATERIALIZED VIEW avg_amp AS "
         "SELECT device, channel, AVG(amplitude) AS avg_a, COUNT(*) AS n "
         "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"),
        ("CREATE MATERIALIZED VIEW peak AS "
         "SELECT device, channel, MAX(amplitude) AS peak_val, COUNT(*) AS n "
         "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"),
    ]

    def _setup_sql():
      s = rtbot_sql.RtBotSql()
      s.execute("CREATE STREAM vibration (device DOUBLE, channel DOUBLE, amplitude DOUBLE)")
      for v in view_stmts:
        s.execute(v)
      return s

    # Slow path (list-of-dicts → _insert_dataframe_slow)
    sql_slow = _setup_sql()
    result_slow = sql_slow.insert_dataframe("vibration", readings_dicts)
    slow_rps = result_slow.rows_per_sec

    # Fast path (DataFrame → _insert_dataframe_fast with C++ batch)
    sql_fast = _setup_sql()
    result_fast = sql_fast.insert_dataframe("vibration", df)
    fast_rps = result_fast.rows_per_sec

    speedup = fast_rps / slow_rps if slow_rps > 0 else float("inf")
    print(f"\n  Slow path (list-of-dicts, 4 views, {n_rows} rows): {slow_rps:,.0f} rows/sec")
    print(f"  Fast path (DataFrame batch, 4 views, {n_rows} rows): {fast_rps:,.0f} rows/sec")
    print(f"  Speedup: {speedup:.2f}x")

    # Batch path must not regress (allow 0.9x for measurement noise)
    self.assertGreater(speedup, 0.9,
                       f"Batch path ({fast_rps:,.0f} rps) should not be slower than "
                       f"row-by-row ({slow_rps:,.0f} rps). Got {speedup:.2f}x")

  def test_ims_throughput_minimum(self):
    """DataFrame batch path must exceed 5K rows/sec for IMS use case.

    IMS dataset: 2156 files × 20480 rows = ~44M rows.
    At 5K rows/sec → ~2.5 hours (acceptable with store_raw=False).
    At <5K → too slow for interactive validation.
    """
    n_rows = 20000
    df = pd.DataFrame({
        "time": list(range(n_rows)),
        "device": [float(i % 4 + 1) for i in range(n_rows)],
        "channel": [1.0] * n_rows,
        "amplitude": [math.sin(i * 0.01) + 0.5 for i in range(n_rows)],
    })

    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM vibration (device DOUBLE, channel DOUBLE, amplitude DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW rms AS "
        "SELECT device, channel, COUNT(*) AS n, "
        "SUM(amplitude * amplitude) / COUNT(*) AS mean_sq "
        "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"
    )
    sql.execute(
        "CREATE MATERIALIZED VIEW peak AS "
        "SELECT device, channel, MAX(amplitude) AS peak_val, COUNT(*) AS n "
        "FROM vibration GROUP BY device, channel, ABS(amplitude) > 0"
    )

    result = sql.insert_dataframe("vibration", df, store_raw=False)
    print(f"\n  IMS-like throughput (2 views, store_raw=False, {n_rows} rows): {result.rows_per_sec:,.0f} rows/sec")

    self.assertGreater(result.rows_per_sec, 5000,
                       f"IMS throughput ({result.rows_per_sec:,.0f} rps) must exceed 5K rows/sec "
                       f"for practical dataset validation")

  def test_store_raw_false_not_slower(self):
    """store_raw=False should not be slower than store_raw=True."""
    n_rows = 50000
    df = pd.DataFrame({
        "time": list(range(n_rows)),
        "col_a": [20.0 + math.sin(i * 0.01) for i in range(n_rows)],
        "col_b": [10.0 + math.cos(i * 0.01) for i in range(n_rows)],
        "col_c": [5.0 + math.sin(i * 0.02) for i in range(n_rows)],
    })

    view_stmts = [
        ("CREATE MATERIALIZED VIEW v1 AS "
         "SELECT col_a, MOVING_AVERAGE(col_a, 10) AS avg_a "
         "FROM sensor"),
        ("CREATE MATERIALIZED VIEW v2 AS "
         "SELECT col_b, MOVING_AVERAGE(col_b, 10) AS avg_b "
         "FROM sensor"),
    ]

    def _setup_sql():
      s = rtbot_sql.RtBotSql()
      s.execute("CREATE STREAM sensor (col_a DOUBLE, col_b DOUBLE, col_c DOUBLE)")
      for v in view_stmts:
        s.execute(v)
      return s

    # With raw storage
    sql_with = _setup_sql()
    result_with = sql_with.insert_dataframe("sensor", df, store_raw=True)

    # Without raw storage
    sql_without = _setup_sql()
    result_without = sql_without.insert_dataframe("sensor", df, store_raw=False)

    speedup = result_without.rows_per_sec / result_with.rows_per_sec if result_with.rows_per_sec > 0 else float("inf")
    print(f"\n  With raw storage ({n_rows} rows, 3 cols):    {result_with.rows_per_sec:,.0f} rows/sec")
    print(f"  Without raw storage ({n_rows} rows, 3 cols): {result_without.rows_per_sec:,.0f} rows/sec")
    print(f"  Speedup: {speedup:.2f}x")

    # store_raw=False must not be slower (allow 0.95x for noise)
    self.assertGreater(speedup, 0.95,
                       f"store_raw=False ({result_without.rows_per_sec:,.0f} rps) should not be slower than "
                       f"store_raw=True ({result_with.rows_per_sec:,.0f} rps). Got {speedup:.2f}x")


if __name__ == "__main__":
  unittest.main()
