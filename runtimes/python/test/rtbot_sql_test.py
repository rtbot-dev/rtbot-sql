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
    self.assertEqual(rows, [])

  def test_multi_from_matching_timestamps_produce_output(self):
    """Matching timestamps across streams produce correct combined output."""
    sql = rtbot_sql.RtBotSql()

    sql.execute("CREATE STREAM btc (price DOUBLE PRECISION)")
    sql.execute("CREATE STREAM eth (price DOUBLE PRECISION)")

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


class ViewStorageTest(unittest.TestCase):
  """Verify that non-materialized VIEWs do not store outputs in the stream
  store, while MATERIALIZED VIEWs do."""

  def test_view_does_not_store_output(self):
    """A CREATE VIEW should not accumulate rows in the stream store."""
    sql = rtbot_sql.RtBotSql()
    sql.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")
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
    sql.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")
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
    sql.execute("CREATE STREAM trades (instrument_id DOUBLE, price DOUBLE, quantity DOUBLE)")

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
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    schema = rt._catalog.lookup_stream("sensors")
    self.assertEqual(schema.columns[0].col_type, "DOUBLE")
    self.assertEqual(schema.columns[1].col_type, "TEXT")
    self.assertEqual(schema.columns[2].col_type, "DOUBLE")

  def test_column_def_is_text(self):
    """ColumnDef.is_text helper method."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
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
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.insert_mixed("sensors", 1000, [1.0, "Bay A", 42.0])

    messages = rt._store.read("sensors")
    self.assertEqual(len(messages), 1)
    self.assertEqual(messages[0].values, [1.0, 1.0, 42.0])

  def test_insert_mixed_reuses_existing_ids(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
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
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, "Bay A", "not a number"])

  def test_insert_mixed_double_into_text_column_raises(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, 42.0, 42.0])

  def test_insert_mixed_unknown_stream_raises(self):
    rt = rtbot_sql.RtBotSql()
    with self.assertRaises(ValueError):
      rt.insert_mixed("nonexistent", 1000, [1.0])

  def test_insert_mixed_column_count_mismatch_raises(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    with self.assertRaises(ValueError):
      rt.insert_mixed("sensors", 1000, [1.0, "Bay A"])


class DecodeRowTest(unittest.TestCase):
  def test_decode_row_converts_text_columns(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
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
    rt.execute("CREATE STREAM prices (price DOUBLE PRECISION, volume DOUBLE PRECISION)")
    rt.execute("INSERT INTO prices VALUES (100.0, 200.0)")
    messages = rt._store.read("prices")
    decoded = rt.decode_row("prices", messages[0].values)
    self.assertEqual(decoded, [100.0, 200.0])


class SqlInsertDictionarySyncTest(unittest.TestCase):
  def test_sql_insert_syncs_dictionary(self):
    """SQL INSERT with string literal syncs dictionary back to catalog."""
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
    rt.execute("INSERT INTO sensors VALUES (1, 'Bay A', 42.0)")

    d = rt._catalog.lookup_dictionary("sensors.location")
    self.assertIsNotNone(d, "Dictionary should exist after SQL INSERT with string literal")
    self.assertEqual(d.decode(1.0), "Bay A")

  def test_consecutive_sql_inserts_maintain_dictionary(self):
    rt = rtbot_sql.RtBotSql()
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
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
    rt.execute("CREATE STREAM sensors (device_id DOUBLE PRECISION, location TEXT, value DOUBLE PRECISION)")
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


if __name__ == "__main__":
  unittest.main()
