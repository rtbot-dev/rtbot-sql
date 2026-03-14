import math
import unittest

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


class CompilerBindingTest(unittest.TestCase):
  def test_compile_create_table(self):
    snapshot = compiler.native.CatalogSnapshot()

    result = compiler.native.compile_sql(
        "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION)",
        snapshot,
    )

    self.assertEqual(result.statement_type, compiler.native.StatementType.CREATE_STREAM)
    self.assertEqual(len(result.errors), 0)
    self.assertEqual(result.entity_name, "trades")


class RuntimeLifecycleTest(unittest.TestCase):
  def test_create_stream_alias(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute("CREATE STREAM ticks (value DOUBLE PRECISION)")
    sql.execute("INSERT INTO ticks VALUES (42)")

    result = sql.execute("SELECT * FROM ticks LIMIT 1")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "value"])
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][1:], [42.0])

  def test_create_insert_select_limit(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute(
        "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION, quantity DOUBLE PRECISION)"
    )
    sql.execute("INSERT INTO trades VALUES (1, 150.0, 200)")
    sql.execute("INSERT INTO trades VALUES (2, 80.0, 500)")
    sql.execute("INSERT INTO trades VALUES (3, 90.0, 300)")

    result = sql.execute("SELECT instrument_id, price FROM trades LIMIT 2")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "instrument_id", "price"])
    self.assertEqual([row[1:] for row in rows], [[2.0, 80.0], [3.0, 90.0]])

  def test_where_and_expression_projection(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute(
        "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION, quantity DOUBLE PRECISION)"
    )
    sql.execute("INSERT INTO trades VALUES (1, 150.0, 200)")
    sql.execute("INSERT INTO trades VALUES (2, 80.0, 500)")
    sql.execute("INSERT INTO trades VALUES (3, 120.0, 100)")

    result = sql.execute(
        "SELECT instrument_id, price * quantity AS trade_value "
        "FROM trades WHERE price > 100 LIMIT 10"
    )
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "instrument_id", "trade_value"])
    self.assertEqual([row[1:] for row in rows], [[1.0, 30000.0], [3.0, 12000.0]])

  def test_materialized_view_latest_per_key(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute(
        "CREATE TABLE trades (instrument_id DOUBLE PRECISION, quantity DOUBLE PRECISION)"
    )
    sql.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt "
        "FROM trades GROUP BY instrument_id"
    )

    sql.execute("INSERT INTO trades VALUES (1, 10)")
    sql.execute("INSERT INTO trades VALUES (2, 5)")
    sql.execute("INSERT INTO trades VALUES (1, 7)")

    result = sql.execute("SELECT * FROM stats")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "instrument_id", "total_qty", "cnt"])

    actual = {row[1]: row[1:] for row in rows}
    self.assertEqual(actual[1.0], [1.0, 17.0, 2.0])
    self.assertEqual(actual[2.0], [2.0, 5.0, 1.0])

  def test_insert_dataframe_with_column_map(self):
    try:
      import pandas as pd
    except ImportError:
      self.skipTest("pandas is required")

    sql = rtbot_sql.RtBotSql()
    sql.execute(
        "CREATE STREAM trades (price DOUBLE PRECISION, qty DOUBLE PRECISION, "
        "quote_qty DOUBLE PRECISION, is_buyer_maker DOUBLE PRECISION)"
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

  def test_multi_from_materialized_view(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute("CREATE STREAM btc (price DOUBLE PRECISION)")
    sql.execute("CREATE STREAM eth (price DOUBLE PRECISION)")
    sql.execute(
        "CREATE MATERIALIZED VIEW cross_stats AS "
        "SELECT b.price AS btc_price, e.price AS eth_price, b.price - e.price AS spread "
        "FROM btc b, eth e"
    )

    sql.insert_dataframe(
        "btc",
        [
            {"time": 1700000001000, "price": 100.0},
            {"time": 1700000003000, "price": 101.0},
        ],
    )
    sql.insert_dataframe(
        "eth",
        [
            {"time": 1700000002000, "price": 95.0},
            {"time": 1700000004000, "price": 95.0},
        ],
    )

    result = sql.execute("SELECT * FROM cross_stats LIMIT 10")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(
        [row[1:] for row in rows],
        [[100.0, 95.0, 5.0], [101.0, 95.0, 6.0]],
    )

  def test_multi_from_ephemeral_select_asof_correlation(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute("CREATE STREAM btc (price DOUBLE PRECISION)")
    sql.execute("CREATE STREAM eth (price DOUBLE PRECISION)")

    sql.insert_dataframe("btc", [{"time": 1000, "price": 100.0}])
    sql.insert_dataframe("eth", [{"time": 1200, "price": 95.0}])
    sql.insert_dataframe("btc", [{"time": 1300, "price": 102.0}])
    sql.insert_dataframe("eth", [{"time": 1700, "price": 97.0}])

    result = sql.execute(
        "SELECT b.price AS btc_price, e.price AS eth_price, "
        "b.price - e.price AS spread FROM btc b, eth e LIMIT 10"
    )
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(
        [row[1:] for row in rows],
        [[100.0, 95.0, 5.0], [102.0, 95.0, 7.0], [102.0, 97.0, 5.0]],
    )

  def test_filtered_select_on_materialized_view(self):
    sql = rtbot_sql.RtBotSql()
    sql.configure_time_format(formatter=lambda ts: ts)

    sql.execute("CREATE STREAM sensors (temperature DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT temperature, "
        "MOVING_AVERAGE(temperature, 50) AS avg_temp, "
        "MOVING_STD(temperature, 50) AS std_temp "
        "FROM sensors"
    )

    readings = []
    for i in range(200):
      temp = 20.0 + 2.0 * math.sin(i * 2 * math.pi / 60) + 0.3 * math.sin(i * 7.1)
      if i == 80:
        temp = 35.0
      elif i == 130:
        temp = 5.0
      elif i == 170:
        temp = 38.0
      readings.append({"time": i, "temperature": temp})

    sql.insert_dataframe("sensors", readings)

    result = sql.execute(
        "SELECT * FROM stats "
        "WHERE ABS(temperature - avg_temp) > 2 * std_temp"
    )
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "temperature", "avg_temp", "std_temp"])

    self.assertEqual([row[0] for row in rows], [80.0, 130.0, 170.0])

    self.assertEqual(len(rows), 3)
    for row in rows:
      self.assertEqual(len(row), 4, f"Expected 4 values per row, got {len(row)}: {row}")
      temperature = row[1]
      avg_temp = row[2]
      std_temp = row[3]
      self.assertGreater(
          abs(temperature - avg_temp), 2 * std_temp,
          f"Row {row} should satisfy the WHERE condition",
      )

  def test_configure_time_format_override(self):
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM ticks (value DOUBLE PRECISION)")
    sql.insert_dataframe("ticks", [{"time": 1700000000000, "value": 1.0}])

    sql.configure_time_format(unit="ms", column_name="ts", formatter=lambda ts: ts)
    result = sql.execute("SELECT * FROM ticks LIMIT 1")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["ts", "value"])
    self.assertEqual(rows[0][0], 1700000000000.0)


if __name__ == "__main__":
  unittest.main()
