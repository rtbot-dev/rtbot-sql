import math
import unittest

import rtbot_sql
from rtbot_sql import compiler
from rtbot_sql.string_dictionary import StringDictionary

native = compiler.native


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

    compilation = compiler.native.compile_sql(
        "CREATE TABLE trades (instrument_id DOUBLE PRECISION, price DOUBLE PRECISION)",
        snapshot,
        1_000_000,
    )

    self.assertIn("results", compilation)
    result = compilation["results"][0]
    self.assertEqual(result.statement_type, compiler.native.StatementType.CREATE_STREAM)
    self.assertEqual(len(result.errors), 0)
    self.assertEqual(result.entity_name, "trades")


class ErrorLocationBindingTest(unittest.TestCase):
  """Verify the Python binding exposes line/column AND end_line/end_column.

  Without these tests, a regression in sql_bindings.cpp (e.g. removing
  d["end_line"] = e.end_line) would silently land — there's no other check
  that the language-level consumer actually receives the new fields.
  """

  def _compile_with_errors(self, sql: str):
    snapshot = compiler.native.CatalogSnapshot()
    compilation = compiler.native.compile_sql(sql, snapshot, 1_000_000)
    self.assertIn("results", compilation)
    self.assertGreater(len(compilation["results"]), 0)
    result = compilation["results"][0]
    self.assertGreater(len(result.errors), 0,
                       f"expected errors for SQL: {sql!r}")
    return result.errors

  def test_compilation_error_exposes_end_fields(self):
    # Semantic error: unknown source.
    errors = self._compile_with_errors("SELECT * FROM nonexistent_stream LIMIT 10")
    err = errors[0]
    # Both start and end positions must be exposed by the binding.
    self.assertGreaterEqual(err.line, 1)
    self.assertGreaterEqual(err.column, 1)
    self.assertGreaterEqual(err.end_line, 1)
    self.assertGreaterEqual(err.end_column, 1)
    # End must come after (or at) start.
    self.assertTrue(
        err.end_line > err.line
        or (err.end_line == err.line and err.end_column >= err.column))

  def test_syntax_error_exposes_end_fields(self):
    errors = self._compile_with_errors("SELEKT FROM x")
    err = errors[0]
    self.assertGreaterEqual(err.end_line, 1)
    self.assertGreaterEqual(err.end_column, 1)

  def test_converter_error_exposes_end_fields(self):
    # BETWEEN is rejected at AST conversion — the new ConverterError path
    # must surface its location through the binding.
    errors = self._compile_with_errors(
        "SELECT 1 FROM x WHERE x BETWEEN 1 AND 10 LIMIT 1")
    # Find the BETWEEN error.
    between_err = next(
        (e for e in errors if "BETWEEN" in e.message), None)
    self.assertIsNotNone(between_err, "expected BETWEEN error")
    self.assertGreaterEqual(between_err.line, 1)
    self.assertGreaterEqual(between_err.column, 1)
    self.assertGreaterEqual(between_err.end_line, 1)
    self.assertGreaterEqual(between_err.end_column, 1)

  def test_validate_sql_dict_includes_end_fields(self):
    # validate_sql is a separate entry point (parser-only, no compile).
    # Verify its dict output also carries end_line / end_column.
    out = compiler.native.validate_sql("SELEKT FROM x")
    self.assertFalse(out["valid"])
    self.assertGreater(len(out["errors"]), 0)
    err = out["errors"][0]
    self.assertIn("line", err)
    self.assertIn("column", err)
    self.assertIn("end_line", err)
    self.assertIn("end_column", err)
    self.assertGreaterEqual(err["end_line"], 1)
    self.assertGreaterEqual(err["end_column"], 1)


class RuntimeLifecycleTest(unittest.TestCase):
  def test_create_stream_alias(self):
    sql = rtbot_sql.RtBotSql()

    sql.execute("SELECT STREAM ticks (value DOUBLE PRECISION)")
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
        "SELECT STREAM trades (price DOUBLE PRECISION, qty DOUBLE PRECISION, "
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

    sql.execute("SELECT STREAM btc (price DOUBLE PRECISION)")
    sql.execute("SELECT STREAM eth (price DOUBLE PRECISION)")
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
            {"time": 1700000001000, "price": 95.0},
            {"time": 1700000003000, "price": 95.0},
        ],
    )

    result = sql.execute("SELECT * FROM cross_stats LIMIT 10")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(
        [row[1:] for row in rows],
        [[100.0, 95.0, 5.0], [101.0, 95.0, 6.0]],
    )

  def test_multi_from_non_matching_timestamps_produce_no_output(self):
    """Non-matching timestamps across streams produce no output.

    RTBot operators synchronize on timestamps. When stream A sends at t=1000
    and stream B at t=1200, no timestamp matches, so no output is produced.
    """
    sql = rtbot_sql.RtBotSql()

    sql.execute("SELECT STREAM btc (price DOUBLE PRECISION)")
    sql.execute("SELECT STREAM eth (price DOUBLE PRECISION)")

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
    self.assertEqual(rows, [])

  def test_multi_from_matching_timestamps_produce_output(self):
    """Matching timestamps across streams produce correct combined output."""
    sql = rtbot_sql.RtBotSql()

    sql.execute("SELECT STREAM btc (price DOUBLE PRECISION)")
    sql.execute("SELECT STREAM eth (price DOUBLE PRECISION)")

    sql.insert_dataframe("btc", [
        {"time": 1000, "price": 100.0},
        {"time": 2000, "price": 102.0},
    ])
    sql.insert_dataframe("eth", [
        {"time": 1000, "price": 95.0},
        {"time": 2000, "price": 97.0},
    ])

    result = sql.execute(
        "SELECT b.price AS btc_price, e.price AS eth_price, "
        "b.price - e.price AS spread FROM btc b, eth e LIMIT 10"
    )
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["time", "btc_price", "eth_price", "spread"])
    self.assertEqual(
        [row[1:] for row in rows],
        [[100.0, 95.0, 5.0], [102.0, 97.0, 5.0]],
    )

  def test_filtered_select_on_materialized_view(self):
    sql = rtbot_sql.RtBotSql()
    sql.configure_time_format(formatter=lambda ts: ts)

    sql.execute("SELECT STREAM sensors (temperature DOUBLE)")
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
    sql.execute("SELECT STREAM ticks (value DOUBLE PRECISION)")
    sql.insert_dataframe("ticks", [{"time": 1700000000000, "value": 1.0}])

    sql.configure_time_format(unit="ms", column_name="ts", formatter=lambda ts: ts)
    result = sql.execute("SELECT * FROM ticks LIMIT 1")
    columns, rows = _columns_and_rows(result)

    self.assertEqual(columns, ["ts", "value"])
    self.assertEqual(rows[0][0], 1700000000000.0)


class ViewStorageTest(unittest.TestCase):
  """Verify that non-materialized VIEWs do not store outputs in the stream
  store, while MATERIALIZED VIEWs do."""

  def test_view_does_not_store_output(self):
    """A CREATE VIEW should not accumulate rows in the stream store."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("SELECT STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")
    sql.execute(
        "CREATE VIEW trade_view AS "
        "SELECT instrument_id, price FROM trades LIMIT 1"
    )

    sql.execute("INSERT INTO trades VALUES (1, 100.0, 10)")
    sql.execute("INSERT INTO trades VALUES (2, 200.0, 20)")

    # The raw stream should be stored
    raw_msgs = sql._store.read("trades")
    self.assertEqual(len(raw_msgs), 2)

    # The VIEW output should NOT be stored
    view_msgs = sql._store.read("trade_view")
    self.assertEqual(len(view_msgs), 0,
                     "Plain VIEW should not store output in stream store")

  def test_materialized_view_stores_output(self):
    """A CREATE MATERIALIZED VIEW should accumulate rows in the stream store."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("SELECT STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")
    sql.execute(
        "CREATE MATERIALIZED VIEW trade_stats AS "
        "SELECT instrument_id, SUM(price) AS total_price, COUNT(*) AS cnt "
        "FROM trades GROUP BY instrument_id"
    )

    sql.execute("INSERT INTO trades VALUES (1, 100.0, 10)")
    sql.execute("INSERT INTO trades VALUES (2, 200.0, 20)")
    sql.execute("INSERT INTO trades VALUES (1, 150.0, 5)")

    # The MATERIALIZED VIEW output should be stored
    mv_msgs = sql._store.read("trade_stats")
    self.assertGreater(len(mv_msgs), 0,
                       "MATERIALIZED VIEW should store output in stream store")

  def test_view_chain_propagates_without_storing_intermediate(self):
    """A VIEW → MATERIALIZED VIEW chain should propagate data correctly
    while only storing the final materialized output, not the intermediate
    VIEW output."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("SELECT STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")

    # Intermediate VIEW: computes a derived column
    sql.execute(
        "CREATE VIEW enriched AS "
        "SELECT instrument_id, price * quantity AS notional "
        "FROM trades LIMIT 1"
    )

    # Final MATERIALIZED VIEW: aggregates from the VIEW
    sql.execute(
        "CREATE MATERIALIZED VIEW totals AS "
        "SELECT instrument_id, SUM(notional) AS total_notional, COUNT(*) AS cnt "
        "FROM enriched GROUP BY instrument_id"
    )

    sql.execute("INSERT INTO trades VALUES (1, 100.0, 10)")
    sql.execute("INSERT INTO trades VALUES (2, 200.0, 5)")
    sql.execute("INSERT INTO trades VALUES (1, 150.0, 3)")

    # Raw stream: stored
    self.assertEqual(len(sql._store.read("trades")), 3)

    # Intermediate VIEW: NOT stored
    self.assertEqual(len(sql._store.read("enriched")), 0,
                     "Intermediate VIEW should not store output")

    # Final MATERIALIZED VIEW: stored and correct
    mv_msgs = sql._store.read("totals")
    self.assertGreater(len(mv_msgs), 0,
                       "Downstream MATERIALIZED VIEW should store output")

    # Verify correctness via SELECT
    result = sql.execute("SELECT * FROM totals")
    columns, rows = _columns_and_rows(result)
    self.assertEqual(columns, ["time", "instrument_id", "total_notional", "cnt"])

    actual = {row[1]: row[1:] for row in rows}
    # instrument 1: (100*10) + (150*3) = 1450, count=2
    self.assertEqual(actual[1.0], [1.0, 1450.0, 2.0])
    # instrument 2: (200*5) = 1000, count=1
    self.assertEqual(actual[2.0], [2.0, 1000.0, 1.0])


class ColumnTypeBindingTest(unittest.TestCase):
  def test_column_type_enum_exists(self):
    """ColumnType enum is exposed via native bindings."""
    self.assertTrue(hasattr(native, 'ColumnType'))
    self.assertTrue(hasattr(native.ColumnType, 'DOUBLE'))
    self.assertTrue(hasattr(native.ColumnType, 'TEXT'))

  def test_column_def_has_type_field(self):
    """ColumnDef exposes the type field from C++."""
    col = native.ColumnDef("price", 0)
    # Default should be DOUBLE
    self.assertEqual(col.type, native.ColumnType.DOUBLE)


class DictionaryBindingTest(unittest.TestCase):
  def test_catalog_snapshot_has_dictionaries(self):
    """CatalogSnapshot exposes dictionaries field."""
    snap = native.CatalogSnapshot()
    self.assertTrue(hasattr(snap, 'dictionaries'))
    self.assertIsInstance(snap.dictionaries, dict)

  def test_compilation_result_has_dictionary_updates(self):
    """CompilationResult exposes dictionary_updates field."""
    result = native.CompilationResult()
    self.assertTrue(hasattr(result, 'dictionary_updates'))


class CatalogDictionaryTest(unittest.TestCase):
  def test_column_def_preserves_type(self):
    """Python ColumnDef stores TEXT type from C++ compilation."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    schema = rt._catalog.lookup_stream("sensors")
    self.assertEqual(schema.columns[0].col_type, "DOUBLE")
    self.assertEqual(schema.columns[1].col_type, "TEXT")
    self.assertEqual(schema.columns[2].col_type, "DOUBLE")

  def test_column_def_is_text(self):
    """ColumnDef.is_text helper method."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    schema = rt._catalog.lookup_stream("sensors")
    self.assertFalse(schema.columns[0].is_text)
    self.assertTrue(schema.columns[1].is_text)
    self.assertFalse(schema.columns[2].is_text)


class StringDictionaryTest(unittest.TestCase):
  def test_encode_assigns_sequential_ids(self):
    d = StringDictionary()
    self.assertEqual(d.encode("alpha"), 1.0)
    self.assertEqual(d.encode("beta"), 2.0)
    self.assertEqual(d.encode("alpha"), 1.0)  # idempotent

  def test_decode_roundtrip(self):
    d = StringDictionary()
    d.encode("hello")
    self.assertEqual(d.decode(1.0), "hello")
    self.assertIsNone(d.decode(99.0))

  def test_lookup(self):
    d = StringDictionary()
    d.encode("hello")
    self.assertEqual(d.lookup("hello"), 1.0)
    self.assertIsNone(d.lookup("unknown"))

  def test_put_mapping(self):
    d = StringDictionary()
    d.put_mapping(5.0, "hello")
    self.assertEqual(d.decode(5.0), "hello")
    self.assertEqual(d.lookup("hello"), 5.0)
    # Next encode should get 6.0
    self.assertEqual(d.encode("world"), 6.0)

  def test_put_mapping_idempotent_string(self):
    d = StringDictionary()
    d.encode("hello")  # ID 1.0
    d.put_mapping(99.0, "hello")  # no-op: string exists
    self.assertEqual(d.lookup("hello"), 1.0)
    self.assertIsNone(d.decode(99.0))

  def test_put_mapping_idempotent_id(self):
    d = StringDictionary()
    d.put_mapping(1.0, "hello")
    d.put_mapping(1.0, "world")  # no-op: ID exists
    self.assertEqual(d.decode(1.0), "hello")
    self.assertIsNone(d.lookup("world"))

  def test_size_and_empty(self):
    d = StringDictionary()
    self.assertTrue(d.is_empty)
    self.assertEqual(d.size, 0)
    d.encode("a")
    self.assertFalse(d.is_empty)
    self.assertEqual(d.size, 1)

  def test_all_entries(self):
    d = StringDictionary()
    d.encode("alpha")
    d.encode("beta")
    entries = d.all_entries()
    self.assertEqual(entries, {1.0: "alpha", 2.0: "beta"})

  def test_restore(self):
    d = StringDictionary()
    d.restore({"1": "alpha", "2": "beta"})
    self.assertEqual(d.decode(1.0), "alpha")
    self.assertEqual(d.decode(2.0), "beta")
    self.assertEqual(d.encode("gamma"), 3.0)


class CatalogDictionaryStorageTest(unittest.TestCase):
  def test_get_or_create_dictionary(self):
    from rtbot_sql.catalog import InMemoryCatalog
    cat = InMemoryCatalog()
    d1 = cat.get_or_create_dictionary("sensors.location")
    d2 = cat.get_or_create_dictionary("sensors.location")
    self.assertIs(d1, d2)

  def test_lookup_dictionary_returns_none_for_unknown(self):
    from rtbot_sql.catalog import InMemoryCatalog
    cat = InMemoryCatalog()
    self.assertIsNone(cat.lookup_dictionary("unknown"))

  def test_snapshot_includes_dictionaries(self):
    from rtbot_sql.catalog import InMemoryCatalog
    cat = InMemoryCatalog()
    d = cat.get_or_create_dictionary("sensors.location")
    d.encode("Bay A")
    snap = cat.snapshot()
    self.assertIn("sensors.location", snap.dictionaries)
    self.assertEqual(snap.dictionaries["sensors.location"][1.0], "Bay A")

  def test_drop_clears_related_dictionaries(self):
    from rtbot_sql.catalog import InMemoryCatalog
    cat = InMemoryCatalog()
    cat.register_stream("sensors", {"name": "sensors", "columns": [{"name": "loc", "index": 0}]})
    cat.get_or_create_dictionary("sensors.loc").encode("Bay A")
    cat.drop("sensors")
    self.assertIsNone(cat.lookup_dictionary("sensors.loc"))


class InsertMixedTest(unittest.TestCase):
  def test_insert_mixed_encodes_strings(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.insert_mixed("sensors", 1000, [1.0, "Bay A", 42.0])

    messages = rt._store.read("sensors")
    self.assertEqual(len(messages), 1)
    self.assertEqual(messages[0].values, [1.0, 1.0, 42.0])

  def test_insert_mixed_reuses_existing_ids(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.insert_mixed("sensors", 1000, [1.0, "Bay A", 42.0])
    rt.insert_mixed("sensors", 2000, [2.0, "Bay B", 99.0])
    rt.insert_mixed("sensors", 3000, [3.0, "Bay A", 55.0])

    messages = rt._store.read("sensors")
    self.assertEqual(len(messages), 3)
    self.assertEqual(messages[0].values[1], 1.0)  # Bay A = 1.0
    self.assertEqual(messages[1].values[1], 2.0)  # Bay B = 2.0
    self.assertEqual(messages[2].values[1], 1.0)  # Bay A = 1.0 (reused)

  def test_insert_mixed_string_into_double_column_raises(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, "Bay A", "not a number"])

  def test_insert_mixed_double_into_text_column_raises(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, 42.0, 42.0])

  def test_insert_mixed_unknown_stream_raises(self):
    rt = rtbot_sql.RtBotSql()
    with self.assertRaises(ValueError):
      rt.insert_mixed("nonexistent", 1000, [1.0])

  def test_insert_mixed_column_count_mismatch_raises(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, "Bay A"])


class DecodeRowTest(unittest.TestCase):
  def test_decode_row_converts_text_columns(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.insert_mixed("sensors", 1000, [1.0, "Bay A", 42.0])
    rt.insert_mixed("sensors", 2000, [2.0, "Bay B", 99.0])

    messages = rt._store.read("sensors")
    decoded = rt.decode_row("sensors", messages[0].values)
    self.assertEqual(decoded, [1.0, "Bay A", 42.0])

    decoded2 = rt.decode_row("sensors", messages[1].values)
    self.assertEqual(decoded2, [2.0, "Bay B", 99.0])

  def test_decode_row_unknown_stream_returns_raw(self):
    rt = rtbot_sql.RtBotSql()
    raw = [1.0, 2.0, 3.0]
    decoded = rt.decode_row("unknown", raw)
    self.assertEqual(decoded, [1.0, 2.0, 3.0])

  def test_decode_row_all_double_returns_doubles(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM prices (price DOUBLE PRECISION, volume DOUBLE PRECISION)")
    rt.execute("INSERT INTO prices VALUES (100.0, 200.0)")
    messages = rt._store.read("prices")
    decoded = rt.decode_row("prices", messages[0].values)
    self.assertEqual(decoded, [100.0, 200.0])


class SqlInsertDictionarySyncTest(unittest.TestCase):
  def test_sql_insert_syncs_dictionary(self):
    """SQL INSERT with string literal syncs dictionary back to catalog."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)")

    d = rt._catalog.lookup_dictionary("sensors.location")
    self.assertIsNotNone(d, "Dictionary should exist after SQL INSERT with string literal")
    self.assertEqual(d.decode(1.0), "Bay A")

  def test_consecutive_sql_inserts_maintain_dictionary(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)")
    rt.execute("INSERT INTO sensors VALUES (2, 'Bay B', 99.0)")
    rt.execute("INSERT INTO sensors VALUES (3, 'Bay A', 55.0)")

    messages = rt._store.read("sensors")
    self.assertEqual(len(messages), 3)
    # Bay A = 1.0, Bay B = 2.0, Bay A = 1.0 (reused)
    self.assertEqual(messages[0].values[1], 1.0)
    self.assertEqual(messages[1].values[1], 2.0)
    self.assertEqual(messages[2].values[1], 1.0)

    # decodeRow should work
    decoded = rt.decode_row("sensors", messages[1].values)
    self.assertEqual(decoded[1], "Bay B")

  def test_mixed_insert_paths_maintain_coherence(self):
    """insertMixed + SQL INSERT maintain dictionary coherence."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("SELECT STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.insert_mixed("sensors", 1000, [1.0, "Bay A", 42.0])
    rt.execute("INSERT INTO sensors VALUES (2, 'Bay B', 99.0)")
    rt.insert_mixed("sensors", 3000, [3.0, "Bay A", 55.0])

    messages = rt._store.read("sensors")
    self.assertEqual(len(messages), 3)
    self.assertEqual(messages[0].values[1], 1.0)  # Bay A
    self.assertEqual(messages[1].values[1], 2.0)  # Bay B
    self.assertEqual(messages[2].values[1], 1.0)  # Bay A

    self.assertEqual(rt.decode_row("sensors", messages[0].values)[1], "Bay A")
    self.assertEqual(rt.decode_row("sensors", messages[1].values)[1], "Bay B")


class AlignedStreamSugarTest(unittest.TestCase):
  def test_aligned_stream_sugar_syntax(self):
    """Full e2e test for CREATE ALIGNED STREAM ... BIN() sugar syntax.

    Pipeline: vibration → vibration_bin → vibration_rs → input
              bearing_temp → bearing_temp_bin → bearing_temp_rs → input

    The single sugar statement:
      CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)
    expands to 7 internal statements:
      SELECT STREAM vibration (value DOUBLE)
      SELECT STREAM bearing_temp (value DOUBLE)
      CREATE VIEW vibration_bin AS ... BIN_AVG(vibration, 1s)
      CREATE VIEW bearing_temp_bin AS ... BIN_AVG(bearing_temp, 1s)
      CREATE VIEW vibration_rs AS ... RESAMPLE_CONSTANT + TIMESHIFT
      CREATE VIEW bearing_temp_rs AS ... RESAMPLE_CONSTANT + TIMESHIFT
      CREATE MATERIALIZED VIEW input AS ... cross-join vibration_rs, bearing_temp_rs
    """
    rt = rtbot_sql.RtBotSql()

    # -- Execute the sugar: single call expands to 7 statements ---------
    rt.execute(
        "CREATE ALIGNED STREAM input (vibration DOUBLE, bearing_temp DOUBLE) BIN(1s)"
    )

    # -- Verify catalog state -------------------------------------------
    self.assertIsNotNone(rt._catalog.lookup_stream("vibration"))
    self.assertIsNotNone(rt._catalog.lookup_stream("bearing_temp"))
    self.assertIsNotNone(rt._catalog.lookup_view("vibration_bin"))
    self.assertIsNotNone(rt._catalog.lookup_view("bearing_temp_bin"))
    self.assertIsNotNone(rt._catalog.lookup_view("vibration_rs"))
    self.assertIsNotNone(rt._catalog.lookup_view("bearing_temp_rs"))
    self.assertIsNotNone(rt._catalog.lookup_view("input"))

    # -- Insert data (default _ts_units_per_second = 1_000_000 → µs) ---
    #
    # vibration stream inputs (bin 0, t < 1_000_000µs):
    #   (t=50000µs, [8.0]), (t=200000µs, [12.0]), (t=400000µs, [10.0]),
    #   (t=600000µs, [14.0]), (t=900000µs, [6.0])
    #   → AVG = (8+12+10+14+6)/5 = 10.0
    for ts, val in [
        (50000, 8.0), (200000, 12.0), (400000, 10.0),
        (600000, 14.0), (900000, 6.0),
    ]:
      rt.insert_mixed("vibration", ts, [val])

    # bearing_temp stream inputs (bin 0, t < 1_000_000µs):
    #   (t=100000µs, [68.0]), (t=300000µs, [72.0]), (t=500000µs, [74.0]),
    #   (t=700000µs, [78.0]), (t=850000µs, [76.0]), (t=950000µs, [82.0])
    #   → AVG = (68+72+74+78+76+82)/6 = 75.0
    for ts, val in [
        (100000, 68.0), (300000, 72.0), (500000, 74.0),
        (700000, 78.0), (850000, 76.0), (950000, 82.0),
    ]:
      rt.insert_mixed("bearing_temp", ts, [val])

    # vibration stream inputs (bin 1, 1_000_000 ≤ t < 2_000_000µs):
    #   (t=1050000µs, [20.0]), (t=1300000µs, [22.0]),
    #   (t=1500000µs, [28.0]), (t=1800000µs, [30.0])
    #   → AVG = (20+22+28+30)/4 = 25.0
    #
    # vibration_bin output (flushed when first bin-1 message arrives):
    #   (t=1050000µs, [10.0])  ← bin 0 avg flushed
    #
    # vibration_rs output (after RESAMPLE_CONSTANT + TIMESHIFT):
    #   (t=1000000µs, [10.0])  ← TIMESHIFT(-1000000) corrects resampler latency
    for ts, val in [
        (1050000, 20.0), (1300000, 22.0),
        (1500000, 28.0), (1800000, 30.0),
    ]:
      rt.insert_mixed("vibration", ts, [val])

    # bearing_temp stream inputs (bin 1, 1_000_000 ≤ t < 2_000_000µs):
    #   (t=1100000µs, [85.0]), (t=1400000µs, [90.0]), (t=1700000µs, [95.0])
    #   → AVG = (85+90+95)/3 = 90.0
    #
    # bearing_temp_bin output (flushed when first bin-1 message arrives):
    #   (t=1100000µs, [75.0])  ← bin 0 avg flushed
    #
    # bearing_temp_rs output (after RESAMPLE_CONSTANT + TIMESHIFT):
    #   (t=1000000µs, [75.0])  ← TIMESHIFT(-1000000) corrects resampler latency
    #
    # Combined "input" output (cross-join of vibration_rs and bearing_temp_rs):
    #   (t=1000000µs, [10.0, 75.0])  ← both streams aligned at bin-0 boundary
    for ts, val in [
        (1100000, 85.0), (1400000, 90.0), (1700000, 95.0),
    ]:
      rt.insert_mixed("bearing_temp", ts, [val])

    # vibration bin 2 (2_000_000 ≤ t < 3_000_000µs):
    #   (t=2100000µs, [30.0]), (t=2400000µs, [40.0]), (t=2700000µs, [35.0])
    #   → AVG = (30+40+35)/3 = 35.0
    #
    # vibration_bin output (flushed when first bin-2 message arrives):
    #   (t=2100000µs, [25.0])  ← bin 1 avg flushed
    #
    # vibration_rs output:
    #   (t=2000000µs, [25.0])  ← TIMESHIFT corrects to bin boundary
    for ts, val in [
        (2100000, 30.0), (2400000, 40.0), (2700000, 35.0),
    ]:
      rt.insert_mixed("vibration", ts, [val])

    # bearing_temp bin 2 (2_000_000 ≤ t < 3_000_000µs):
    #   (t=2200000µs, [96.0]), (t=2500000µs, [100.0]), (t=2800000µs, [104.0])
    #   → AVG = (96+100+104)/3 = 100.0
    #
    # bearing_temp_bin output (flushed when first bin-2 message arrives):
    #   (t=2200000µs, [90.0])  ← bin 1 avg flushed
    #
    # bearing_temp_rs output:
    #   (t=2000000µs, [90.0])  ← TIMESHIFT corrects to bin boundary
    #
    # Combined "input" output:
    #   (t=2000000µs, [25.0, 90.0])  ← both streams aligned at bin-1 boundary
    for ts, val in [
        (2200000, 96.0), (2500000, 100.0), (2800000, 104.0),
    ]:
      rt.insert_mixed("bearing_temp", ts, [val])

    # Bin 3 triggers flush of bin 2 for both streams:
    #   vibration: (t=3100000µs, [999.0]) → flushes bin-2 avg=35.0
    #   bearing_temp: (t=3200000µs, [999.0]) → flushes bin-2 avg=100.0
    rt.insert_mixed("vibration", 3100000, [999.0])
    rt.insert_mixed("bearing_temp", 3200000, [999.0])

    # -- Read from store and assert combined output ---------------------
    # Bin 3 trigger samples flushed both streams' bin-2 aggregates, so we
    # expect one combined output per closed bin (0, 1, 2).
    messages = rt._store.read("input")
    self.assertEqual(
        len(messages), 3,
        "Expected 3 combined outputs (bin 0 + bin 1 + bin 2)")

    # First message: bin 0 result at the 1-second boundary
    #   (t=1000000µs, [10.0, 75.0])
    self.assertEqual(messages[0].timestamp, 1000000)
    self.assertAlmostEqual(messages[0].values[0], 10.0, places=5)
    self.assertAlmostEqual(messages[0].values[1], 75.0, places=5)

    # Second message: bin 1 result at the 2-second boundary
    #   (t=2000000µs, [25.0, 90.0])
    self.assertEqual(messages[1].timestamp, 2000000)
    self.assertAlmostEqual(messages[1].values[0], 25.0, places=5)
    self.assertAlmostEqual(messages[1].values[1], 90.0, places=5)

    # Third message: bin 2 result at the 3-second boundary, flushed by the
    # bin-3 trigger samples fed above.
    #   (t=3000000µs, [35.0, 100.0])
    self.assertEqual(messages[2].timestamp, 3000000)
    self.assertAlmostEqual(messages[2].values[0], 35.0, places=5)
    self.assertAlmostEqual(messages[2].values[1], 100.0, places=5)

  def test_set_timescale_changes_units(self):
    """SET TIMESCALE ms changes bin width from µs to ms.

    With ms timescale, BIN(1s) creates bins 1000-wide (not 1_000_000).

    Pipeline: temp → temp_bin → temp_rs → sensor
    """
    rt = rtbot_sql.RtBotSql()

    # -- Change timescale to milliseconds -------------------------------
    rt.execute("SET TIMESCALE ms")

    # -- Create aligned stream with 1s bins (now 1000ms wide) ----------
    rt.execute(
        "CREATE ALIGNED STREAM sensor (temp DOUBLE) BIN(1s)"
    )

    # Verify catalog state
    self.assertIsNotNone(rt._catalog.lookup_stream("temp"))
    self.assertIsNotNone(rt._catalog.lookup_view("temp_bin"))
    self.assertIsNotNone(rt._catalog.lookup_view("temp_rs"))
    self.assertIsNotNone(rt._catalog.lookup_view("sensor"))

    # -- Insert data at ms-scale timestamps -----------------------------
    #
    # temp stream inputs (bin 0, t < 1000ms):
    #   (t=50ms, [20.0]), (t=300ms, [24.0]), (t=700ms, [26.0])
    #   → AVG = (20+24+26)/3 = 23.333...
    for ts, val in [(50, 20.0), (300, 24.0), (700, 26.0)]:
      rt.insert_mixed("temp", ts, [val])

    # temp stream inputs (bin 1, 1000 ≤ t < 2000ms):
    #   (t=1100ms, [30.0]), (t=1500ms, [36.0])
    #   → AVG = (30+36)/2 = 33.0
    #
    # temp_bin output (flushed when first bin-1 message arrives):
    #   (t=1100ms, [23.333...])  ← bin 0 avg flushed
    #
    # temp_rs output (after RESAMPLE_CONSTANT + TIMESHIFT):
    #   Resampler init — first input, no output yet
    for ts, val in [(1100, 30.0), (1500, 36.0)]:
      rt.insert_mixed("temp", ts, [val])

    # temp stream inputs (bin 2, 2000 ≤ t < 3000ms):
    #   (t=2100ms, [40.0]), (t=2500ms, [50.0])
    #   → AVG = (40+50)/2 = 45.0
    #
    # temp_bin output (flushed when first bin-2 message arrives):
    #   (t=2100ms, [33.0])  ← bin 1 avg flushed
    #
    # temp_rs output (resampler second input → produces first output):
    #   (t=1000ms, [23.333...])  ← bin 0 avg, TIMESHIFT(-1000) corrected
    for ts, val in [(2100, 40.0), (2500, 50.0)]:
      rt.insert_mixed("temp", ts, [val])

    # Bin 3 trigger → flushes bin 2:
    #   temp: (t=3100ms, [999.0]) → flushes bin-2 avg=45.0
    #
    # temp_rs output (resampler third input → produces second output):
    #   (t=2000ms, [33.0])  ← bin 1 avg, TIMESHIFT(-1000) corrected
    #
    # sensor output:
    #   (t=1000ms, [23.333...])  ← from bin 0
    #   (t=2000ms, [33.0])       ← from bin 1
    rt.insert_mixed("temp", 3100, [999.0])

    # -- Assert output at ms-scale timestamps ---------------------------
    messages = rt._store.read("sensor")
    self.assertEqual(
        len(messages), 3,
        "Expected 3 outputs (bin 0 + bin 1 + bin 2)")

    # First message: bin 0 at 1000ms boundary
    self.assertEqual(messages[0].timestamp, 1000)
    self.assertAlmostEqual(messages[0].values[0], 23.333333, places=3)

    # Second message: bin 1 at 2000ms boundary
    self.assertEqual(messages[1].timestamp, 2000)
    self.assertAlmostEqual(messages[1].values[0], 33.0, places=5)

    # Third message: bin 2 at 3000ms boundary, flushed by the bin-3
    # trigger sample at t=3100ms.
    self.assertEqual(messages[2].timestamp, 3000)
    self.assertAlmostEqual(messages[2].values[0], 45.0, places=5)


if __name__ == "__main__":
  unittest.main()
