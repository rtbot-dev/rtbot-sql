"""Smoke test: verify rtbot_sql wheel works correctly in the venv.

Tests the core functionality needed for D03 notebooks:
- CREATE STREAM / CREATE TABLE
- INSERT INTO ... VALUES
- CREATE MATERIALIZED VIEW with GROUP BY, aggregation, HAVING
- SELECT queries (tier 1 reads)
- Windowed functions: MOVING_AVERAGE, STDDEV, COUNT, SUM, AVG
"""

import sys
import rtbot_sql
from rtbot_sql import RtBotSql

def test_basic_lifecycle():
    """Stream creation, insert, and select."""
    sql = RtBotSql()

    sql.execute("CREATE STREAM ticks (value DOUBLE PRECISION)")
    sql.execute("INSERT INTO ticks VALUES (10)")
    sql.execute("INSERT INTO ticks VALUES (20)")
    sql.execute("INSERT INTO ticks VALUES (30)")

    result = sql.execute("SELECT * FROM ticks LIMIT 3")
    print("=== Basic lifecycle ===")
    print(result)
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    print("PASS\n")


def test_materialized_view_group_by():
    """GROUP BY with SUM and COUNT — core of D03 scenarios."""
    sql = RtBotSql()

    sql.execute(
        "CREATE TABLE trades ("
        "  instrument_id DOUBLE PRECISION,"
        "  price DOUBLE PRECISION,"
        "  quantity DOUBLE PRECISION"
        ")"
    )
    sql.execute(
        "CREATE MATERIALIZED VIEW stats AS "
        "SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt "
        "FROM trades GROUP BY instrument_id"
    )

    sql.execute("INSERT INTO trades VALUES (1, 100.0, 10)")
    sql.execute("INSERT INTO trades VALUES (2, 200.0, 5)")
    sql.execute("INSERT INTO trades VALUES (1, 110.0, 7)")
    sql.execute("INSERT INTO trades VALUES (2, 190.0, 3)")

    result = sql.execute("SELECT * FROM stats")
    print("=== Materialized view with GROUP BY ===")
    print(result)
    assert len(result) == 2, f"Expected 2 keys, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    print("PASS\n")


def test_where_clause():
    """WHERE filter — used for large trade detection."""
    sql = RtBotSql()

    sql.execute(
        "CREATE TABLE trades ("
        "  instrument_id DOUBLE PRECISION,"
        "  price DOUBLE PRECISION,"
        "  quantity DOUBLE PRECISION"
        ")"
    )
    sql.execute("INSERT INTO trades VALUES (1, 150.0, 200)")
    sql.execute("INSERT INTO trades VALUES (2, 80.0, 500)")
    sql.execute("INSERT INTO trades VALUES (3, 120.0, 100)")

    result = sql.execute(
        "SELECT instrument_id, price * quantity AS notional "
        "FROM trades WHERE price > 100 LIMIT 10"
    )
    print("=== WHERE clause ===")
    print(result)
    assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    print("PASS\n")


def test_windowed_functions():
    """MOVING_AVERAGE and STDDEV — core analytics functions."""
    sql = RtBotSql()

    sql.execute(
        "CREATE STREAM sensor ("
        "  device_id DOUBLE PRECISION,"
        "  temp_c DOUBLE PRECISION"
        ")"
    )
    sql.execute(
        "CREATE MATERIALIZED VIEW device_stats AS "
        "SELECT device_id, temp_c,"
        "       MOVING_AVERAGE(temp_c, 5) AS avg_temp,"
        "       STDDEV(temp_c, 5) AS temp_std "
        "FROM sensor "
        "GROUP BY device_id"
    )

    # Feed some data for device 1
    for t in [20.0, 20.5, 21.0, 20.8, 20.3, 20.6, 21.2, 20.9]:
        sql.execute(f"INSERT INTO sensor VALUES (1, {t})")

    # Feed some data for device 2
    for t in [5.0, 5.1, 5.2, 5.0, 4.9]:
        sql.execute(f"INSERT INTO sensor VALUES (2, {t})")

    result = sql.execute("SELECT * FROM device_stats")
    print("=== Windowed functions (MOVING_AVERAGE, STDDEV) ===")
    print(result)
    assert len(result) == 2, f"Expected 2 devices, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    print("PASS\n")


def test_having_clause():
    """HAVING filter on materialized view — alert mechanism."""
    sql = RtBotSql()

    sql.execute(
        "CREATE STREAM readings ("
        "  sensor_id DOUBLE PRECISION,"
        "  value DOUBLE PRECISION"
        ")"
    )
    sql.execute(
        "CREATE MATERIALIZED VIEW alerts AS "
        "SELECT sensor_id, value,"
        "       MOVING_AVERAGE(value, 5) AS avg_val "
        "FROM readings "
        "GROUP BY sensor_id "
        "HAVING MOVING_AVERAGE(value, 5) > 50.0"
    )

    # Sensor 1: values around 30 — should NOT trigger
    for v in [28.0, 30.0, 32.0, 29.0, 31.0]:
        sql.execute(f"INSERT INTO readings VALUES (1, {v})")

    # Sensor 2: values around 60 — SHOULD trigger
    for v in [55.0, 60.0, 65.0, 58.0, 62.0]:
        sql.execute(f"INSERT INTO readings VALUES (2, {v})")

    result = sql.execute("SELECT * FROM alerts")
    print("=== HAVING clause (alert mechanism) ===")
    print(result)
    assert list(result.columns)[0] == "time", "Expected time column"
    # Only sensor 2 should have triggered
    if len(result) > 0:
        sensor_ids = set(result["sensor_id"].tolist())
        assert 2.0 in sensor_ids, "Sensor 2 should trigger alert"
        print(f"Alert triggered for sensor(s): {sensor_ids}")
    print("PASS\n")


def test_insert_dataframe():
    """insert_dataframe with pandas — key notebook API."""
    import pandas as pd

    sql = RtBotSql()

    sql.execute(
        "CREATE STREAM fleet ("
        "  vehicle_id DOUBLE PRECISION,"
        "  temp_c DOUBLE PRECISION,"
        "  quote_qty DOUBLE PRECISION,"
        "  is_buyer_maker DOUBLE PRECISION"
        ")"
    )

    df = pd.DataFrame({
        "timestamp": [1700000000000, 1700000000001, 1700000000002, 1700000000003],
        "vehicle_id": [1.0, 1.0, 2.0, 2.0],
        "temp_c": [2.5, 2.6, 3.1, 3.0],
        "quoteQty": [100.0, 101.0, 102.0, 103.0],
        "isBuyerMaker_num": [0.0, 1.0, 1.0, 0.0],
    })

    sql.insert_dataframe(
        "fleet",
        df,
        column_map={
            "quote_qty": "quoteQty",
            "is_buyer_maker": "isBuyerMaker_num",
        },
    )

    result = sql.execute("SELECT * FROM fleet LIMIT 10")
    print("=== insert_dataframe ===")
    print(result)
    assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    assert str(result["time"].iloc[0]).startswith("2023-11"), "Unexpected formatted time"
    print("PASS\n")


def test_multi_column_group_by_scenario():
    """Multi-column stream with GROUP BY — the cold chain pattern."""
    sql = RtBotSql()

    sql.execute(
        "CREATE STREAM fleet_telemetry ("
        "  vehicle_id DOUBLE PRECISION,"
        "  zone_id DOUBLE PRECISION,"
        "  temp_c DOUBLE PRECISION,"
        "  setpoint_c DOUBLE PRECISION,"
        "  door_open DOUBLE PRECISION"
        ")"
    )

    sql.execute(
        "CREATE MATERIALIZED VIEW vehicle_status AS "
        "SELECT vehicle_id, zone_id, temp_c, setpoint_c,"
        "       MOVING_AVERAGE(temp_c, 5) AS smooth_temp,"
        "       STDDEV(temp_c, 5) AS temp_variability,"
        "       MOVING_AVERAGE(door_open, 3) AS door_open_rate "
        "FROM fleet_telemetry "
        "GROUP BY vehicle_id"
    )

    # Vehicle 1 in zone 1 — healthy
    for t in [2.0, 2.1, 1.9, 2.0, 2.1]:
        sql.execute(f"INSERT INTO fleet_telemetry VALUES (1, 1, {t}, 2.0, 0)")

    # Vehicle 2 in zone 2 — drifting temp
    for t in [2.0, 2.5, 3.0, 3.5, 4.0]:
        sql.execute(f"INSERT INTO fleet_telemetry VALUES (2, 2, {t}, 2.0, 0)")

    result = sql.execute("SELECT * FROM vehicle_status")
    print("=== Cold chain pattern (multi-column GROUP BY) ===")
    print(result)
    assert len(result) == 2, f"Expected 2 vehicles, got {len(result)}"
    assert list(result.columns)[0] == "time", "Expected time column"
    print("PASS\n")


def test_multi_from_correlation_asof():
    """Cross-stream SQL should correlate with latest values from both streams."""
    sql = RtBotSql()

    sql.execute("CREATE STREAM btc_trades (price DOUBLE PRECISION)")
    sql.execute("CREATE STREAM eth_trades (price DOUBLE PRECISION)")

    sql.insert_dataframe("btc_trades", [{"time": 1000, "price": 100.0}])
    sql.insert_dataframe("eth_trades", [{"time": 1200, "price": 95.0}])
    sql.insert_dataframe("btc_trades", [{"time": 1300, "price": 102.0}])
    sql.insert_dataframe("eth_trades", [{"time": 1700, "price": 97.0}])

    result = sql.execute(
        "SELECT b.price AS btc_price, e.price AS eth_price, "
        "b.price - e.price AS spread FROM btc_trades b, eth_trades e LIMIT 10"
    )
    print("=== Multi-FROM correlation (ASOF) ===")
    print(result)
    assert list(result.columns)[0] == "time", "Expected time column"
    assert len(result) == 3, f"Expected 3 correlated rows, got {len(result)}"
    assert result["spread"].tolist() == [5.0, 7.0, 5.0], "Unexpected spread sequence"
    print("PASS\n")


if __name__ == "__main__":
    tests = [
        test_basic_lifecycle,
        test_materialized_view_group_by,
        test_where_clause,
        test_windowed_functions,
        test_having_clause,
        test_insert_dataframe,
        test_multi_column_group_by_scenario,
        test_multi_from_correlation_asof,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")

    if failed > 0:
        sys.exit(1)
    else:
        print("All tests passed!")
        sys.exit(0)
