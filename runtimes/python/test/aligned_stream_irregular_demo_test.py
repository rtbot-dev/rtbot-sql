import signal
import sys
import time
import unittest

import rtbot_sql


class AlignedStreamIrregularDemoTest(unittest.TestCase):
  def test_live_demo_until_killed(self):
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

    print("[demo] aligned stream irregular demo started")
    print("[demo] press Ctrl+C to stop")
    print("[demo] columns: bin_end_s expected.temperature_k expected.pressure_pa expected.ratio "
          "received.temperature_k received.pressure_pa received.ratio deltas")
    sys.stdout.flush()

    next_bin_index = 0
    last_output_count = 0

    while running["value"]:
      bin_start = next_bin_index
      bin_end = next_bin_index + 1

      temp_values = [
          22.0 + 0.7 * next_bin_index,
          24.5 + 0.3 * next_bin_index,
          23.2 + 0.5 * next_bin_index,
      ]
      pressure_values = [
          3.1 + 0.11 * next_bin_index,
          3.7 + 0.07 * next_bin_index,
          3.4 + 0.09 * next_bin_index,
      ]

      event_times = [
          int((bin_start + 0.10) * 1_000_000),
          int((bin_start + 0.32) * 1_000_000),
          int((bin_start + 0.61) * 1_000_000),
      ]

      for idx in range(3):
        rt.insert_dataframe(
            "temperature_c",
            [{"time": event_times[idx], "value": temp_values[idx]}],
        )
        rt.insert_dataframe(
            "pressure_bar",
            [{"time": event_times[idx], "value": pressure_values[idx]}],
        )
        time.sleep(0.10)

      trigger_ts = int((bin_end + 0.05) * 1_000_000)
      rt.insert_dataframe("temperature_c", [{"time": trigger_ts, "value": 20.0 + next_bin_index}])
      rt.insert_dataframe("pressure_bar", [{"time": trigger_ts, "value": 3.0 + next_bin_index * 0.1}])

      messages = rt._store.read("pt_out")
      if len(messages) > last_output_count:
        row = messages[-1]
        received_temperature_k = row.values[0]
        received_pressure_pa = row.values[1]
        received_ratio = row.values[2]
      else:
        received_temperature_k = float("nan")
        received_pressure_pa = float("nan")
        received_ratio = float("nan")

      avg_temp = sum(temp_values) / len(temp_values)
      avg_pressure = sum(pressure_values) / len(pressure_values)
      expected_temperature_k = avg_temp + 273.15
      expected_pressure_pa = avg_pressure * 100000.0
      expected_ratio = expected_pressure_pa / expected_temperature_k

      print(
          f"[demo] bin_end={bin_end:04d}s "
          f"expected=(Tk={expected_temperature_k:.6f},P={expected_pressure_pa:.6f},R={expected_ratio:.6f}) "
          f"received=(Tk={received_temperature_k:.6f},P={received_pressure_pa:.6f},R={received_ratio:.6f}) "
          f"delta=(Tk={received_temperature_k - expected_temperature_k:.6f},"
          f"P={received_pressure_pa - expected_pressure_pa:.6f},"
          f"R={received_ratio - expected_ratio:.6f})"
      )
      sys.stdout.flush()

      last_output_count = len(messages)
      next_bin_index += 1
      time.sleep(0.35)

    print("[demo] stopped")
    sys.stdout.flush()


if __name__ == "__main__":
  unittest.main()
