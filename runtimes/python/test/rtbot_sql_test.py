import unittest

import rtbot_sql
from rtbot_sql import compiler


def _columns_and_rows(result):
  if isinstance(result, dict):
    return list(result["columns"]), [list(row) for row in result["rows"]]

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

    self.assertEqual(columns, ["value"])
    self.assertEqual(rows, [[42.0]])

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

    self.assertEqual(columns, ["instrument_id", "price"])
    self.assertEqual(rows, [[2.0, 80.0], [3.0, 90.0]])

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

    self.assertEqual(columns, ["instrument_id", "trade_value"])
    self.assertEqual(rows, [[1.0, 30000.0], [3.0, 12000.0]])

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

    self.assertEqual(columns, ["instrument_id", "total_qty", "cnt"])

    actual = {row[0]: row for row in rows}
    self.assertEqual(actual[1.0], [1.0, 17.0, 2.0])
    self.assertEqual(actual[2.0], [2.0, 5.0, 1.0])


if __name__ == "__main__":
  unittest.main()
