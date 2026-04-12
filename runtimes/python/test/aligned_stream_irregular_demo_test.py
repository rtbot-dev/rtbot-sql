import os
import signal
import sys
import time
import unittest

import rtbot_sql

_BOLD = "\033[1m"
_DIM = "\033[2m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_RESET = "\033[0m"


def _fmt2(value: float) -> str:
  return f"{value:.2f}"


def _is_mismatch(received: float, expected: float, tolerance: float = 0.005) -> bool:
  return abs(received - expected) > tolerance


def _align_sql_output_bin_end(row_timestamp_us: int) -> int:
  return int(row_timestamp_us // 1_000_000)


_TEMPERATURE_POOL = [
    (0.08, 22.20),
    (0.37, 24.80),
    (0.74, 23.10),
    (0.91, 25.20),
]

_PRESSURE_POOL = [
    (0.16, 3.05),
    (0.49, 3.82),
    (0.83, 3.36),
    (0.95, 3.50),
]


def _bin_points(bin_index: int):
  # 1-4 points per stream, varying deterministically across bins.
  n_temp = (bin_index % 4) + 1
  n_pres = ((bin_index + 2) % 4) + 1
  temperature_points = [
      (offset, value + 0.52 * bin_index) for offset, value in _TEMPERATURE_POOL[:n_temp]
  ]
  pressure_points = [
      (offset, value + 0.10 * bin_index) for offset, value in _PRESSURE_POOL[:n_pres]
  ]
  return temperature_points, pressure_points


def _print_stream_pipelines(streams, bin_end):
  """Print aligned: name -> [(t, v), ...] -> (t_aggr, v_aggr)    (formula)"""
  parts = []
  for name, points in streams:
    tuples_str = "[" + ", ".join(f"({_fmt2(t)}, {_fmt2(v)})" for t, v in points) + "]"
    values = [v for _, v in points]
    avg = sum(values) / len(values)
    sum_str = " + ".join(_fmt2(v) for v in values)
    aggr_str = f"({_fmt2(float(bin_end))}, {_fmt2(avg)})"
    formula_str = f"({_fmt2(avg)} = ({sum_str}) / {len(values)})"
    parts.append((name, tuples_str, aggr_str, formula_str, avg))

  name_w = max(len(p[0]) for p in parts)
  tuples_w = max(len(p[1]) for p in parts)
  aggr_w = max(len(p[2]) for p in parts)

  avgs = []
  for name, tuples_str, aggr_str, formula_str, avg in parts:
    print(
        f"{_CYAN}{name.ljust(name_w)}{_RESET} "
        f"{_DIM}->{_RESET} "
        f"{tuples_str.ljust(tuples_w)} "
        f"{_DIM}->{_RESET} "
        f"{_GREEN}{aggr_str.ljust(aggr_w)}{_RESET}"
        f"    {_DIM}{formula_str}{_RESET}"
    )
    avgs.append(avg)
  return avgs


def _fmt_sql_val(received, expected):
  text = _fmt2(received)
  if _is_mismatch(received, expected):
    return f"{_RED}{text}{_RESET}"
  return f"{_GREEN}{text}{_RESET}"


def _fmt_delta(delta, received, expected):
  text = f"{delta:+.2f}"
  if _is_mismatch(received, expected):
    return f"{_RED}{text}{_RESET}"
  return f"{_DIM}{text}{_RESET}"


class AlignedStreamIrregularDemoTest(unittest.TestCase):
  def test_live_demo_until_killed(self):
    cadence_s = float(os.environ.get("RTBOT_ALIGNED_DEMO_CADENCE_S", "3"))
    max_bins_raw = os.environ.get("RTBOT_ALIGNED_DEMO_MAX_BINS", "")
    max_bins = int(max_bins_raw) if max_bins_raw.strip() else None

    rt = rtbot_sql.RtBotSql()
    rt.execute(
        "CREATE ALIGNED STREAM process (temperature_c DOUBLE, pressure_bar DOUBLE) BIN(1s)"
    )
    rt.execute(
        "CREATE MATERIALIZED VIEW pt_out AS "
        "SELECT "
        "temperature_c + 273.15 AS temperature_k, "
        "pressure_bar * 100000.0 AS pressure_pa, "
        "(pressure_bar * 100000.0) / (temperature_c + 273.15) AS ratio "
        "FROM process"
    )

    running = {"value": True}

    def _stop(_signum, _frame):
      running["value"] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"\n{_BOLD}Aligned stream irregular demo{_RESET}")
    print(f"cadence={_fmt2(cadence_s)}s per logical bin; press Ctrl+C to stop\n")
    sys.stdout.flush()

    # Per-bin buffered data: bin_end -> info dict
    pending_bins = {}
    next_bin_index = 0
    last_output_count = 0

    def _check_sql_output():
      """Check for new SQL output and print completed bin comparisons."""
      nonlocal last_output_count
      messages = rt._store.read("pt_out")
      new_messages = messages[last_output_count:]
      for row in new_messages:
        out_bin_end = _align_sql_output_bin_end(row.timestamp)
        if out_bin_end in pending_bins:
          _print_bin_comparison(
              out_bin_end, row.values[0], row.values[1], row.values[2])
      last_output_count = len(messages)

    def _print_bin_comparison(bin_end, rx_temp_k, rx_pres_pa, rx_ratio):
      """Print a complete bin: pipeline + Python + SQL + Delta."""
      info = pending_bins.pop(bin_end)
      bin_start = info["bin_start"]
      streams = info["streams"]
      exp_temp, exp_pres, exp_ratio = info["expected"]

      print()
      print(f"{_BOLD}=== Bin {bin_start} [{bin_start}s -> {bin_end}s] ==={_RESET}")
      print()
      _print_stream_pipelines(streams, bin_end)
      print()
      print(
          f"{_BOLD}Python:{_RESET} "
          f"temperature_k = {_GREEN}{_fmt2(exp_temp)}{_RESET}    "
          f"pressure_pa = {_GREEN}{_fmt2(exp_pres)}{_RESET}    "
          f"ratio = {_GREEN}{_fmt2(exp_ratio)}{_RESET}"
      )
      print(
          f"{_BOLD}SQL:{_RESET}    "
          f"temperature_k = {_fmt_sql_val(rx_temp_k, exp_temp)}    "
          f"pressure_pa = {_fmt_sql_val(rx_pres_pa, exp_pres)}    "
          f"ratio = {_fmt_sql_val(rx_ratio, exp_ratio)}"
      )
      d_temp = rx_temp_k - exp_temp
      d_pres = rx_pres_pa - exp_pres
      d_ratio = rx_ratio - exp_ratio
      has_mismatch = (
          _is_mismatch(rx_temp_k, exp_temp)
          or _is_mismatch(rx_pres_pa, exp_pres)
          or _is_mismatch(rx_ratio, exp_ratio)
      )
      mismatch_str = f"   {_BOLD}{_RED}MISMATCH{_RESET}" if has_mismatch else ""
      print(
          f"{_BOLD}Delta:{_RESET}  "
          f"temperature_k = {_fmt_delta(d_temp, rx_temp_k, exp_temp)}    "
          f"pressure_pa = {_fmt_delta(d_pres, rx_pres_pa, exp_pres)}    "
          f"ratio = {_fmt_delta(d_ratio, rx_ratio, exp_ratio)}"
          f"{mismatch_str}"
      )
      print()
      sys.stdout.flush()

    while running["value"]:
      if max_bins is not None and next_bin_index >= max_bins:
        break

      wall_start = time.time()
      bin_start = next_bin_index
      bin_end = next_bin_index + 1

      temp_points, pressure_points = _bin_points(next_bin_index)

      # Merge and sort all points by offset for interleaved insertion.
      merged = []
      for offset, value in temp_points:
        merged.append((offset, "temperature_c", value))
      for offset, value in pressure_points:
        merged.append((offset, "pressure_bar", value))
      merged.sort(key=lambda row: row[0])

      # Insert points one by one with real-time pacing.
      event_start = time.time()
      for offset_s, stream_name, value in merged:
        target_elapsed = offset_s * cadence_s
        now_elapsed = time.time() - event_start
        sleep_for = target_elapsed - now_elapsed
        if sleep_for > 0:
          time.sleep(sleep_for)
        ts_us = int((bin_start + offset_s) * 1_000_000)
        rt.insert_dataframe(stream_name, [{"time": ts_us, "value": value}])

        ts_str = _fmt2(bin_start + offset_s)
        print(
            f"  {_DIM}+{_RESET} {_CYAN}{stream_name:<15}{_RESET} "
            f"t={ts_str}s  v={_fmt2(value)}"
        )
        sys.stdout.flush()

        # Check if this insertion triggered any bin flush.
        _check_sql_output()

      # Compute Python expected values and buffer for comparison.
      avg_temp = sum(v for _, v in temp_points) / len(temp_points)
      avg_pressure = sum(v for _, v in pressure_points) / len(pressure_points)
      expected_temperature_k = avg_temp + 273.15
      expected_pressure_pa = avg_pressure * 100000.0
      expected_ratio = expected_pressure_pa / expected_temperature_k

      pending_bins[bin_end] = {
          "bin_start": bin_start,
          "streams": [
              ("temperature_c", list(temp_points)),
              ("pressure_bar", list(pressure_points)),
          ],
          "expected": (expected_temperature_k, expected_pressure_pa, expected_ratio),
      }

      next_bin_index += 1

      elapsed = time.time() - wall_start
      remaining = cadence_s - elapsed
      if remaining > 0:
        time.sleep(remaining)

    print(f"{_BOLD}stopped{_RESET}")
    sys.stdout.flush()


if __name__ == "__main__":
  unittest.main()
