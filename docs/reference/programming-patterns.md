---
title: Programming Patterns
sidebar_position: 9
---

# Programming Patterns

Reusable recipes for common real-time computation tasks.

## Streaming aggregation

Compute running totals per group:

```sql
CREATE MATERIALIZED VIEW market_stats AS
  SELECT instrument_id,
         SUM(quantity)      AS total_volume,
         COUNT(*)           AS trade_count,
         AVG(price)         AS avg_price
  FROM trades
  GROUP BY instrument_id
```

`SUM`, `COUNT`, and `AVG` without a window are cumulative — they aggregate all data since the stream started. Each instrument has independent state.

## Smoothing and trend detection

Apply progressive moving averages for noise reduction:

```sql
-- Momentum: difference between short and long MA
CREATE MATERIALIZED VIEW momentum AS
  SELECT price,
         MOVING_AVERAGE(price, 5)  AS ma_fast,
         MOVING_AVERAGE(price, 20) AS ma_slow,
         MOVING_AVERAGE(price, 5) - MOVING_AVERAGE(price, 20) AS momentum
  FROM trades;
```

When `ma_fast` crosses above `ma_slow`, momentum turns positive — a classic trend-following signal.

## Threshold alerts

Filter a computation's output to detect anomalies:

```sql
CREATE MATERIALIZED VIEW stats AS
  SELECT sensor_id, temperature,
         MOVING_AVERAGE(temperature, 100) AS baseline,
         MOVING_STD(temperature, 100) AS sd
  FROM sensors
  GROUP BY sensor_id;

CREATE MATERIALIZED VIEW alerts AS
  SELECT sensor_id, temperature, baseline, sd
  FROM stats
  WHERE ABS(temperature - baseline) > 2.5 * sd
```

The stats view computes a per-sensor baseline. The alerts view filters for readings that deviate by more than 2.5 standard deviations. This is a general-purpose anomaly detector that adapts to each sensor's normal behavior.

## Multi-signal stabilizer gate

Compute outputs only when multiple signals are simultaneously stable:

```sql
CREATE MATERIALIZED VIEW steady_state_metrics AS
  SELECT device_id,
         AVG(temperature) AS steady_temp,
         AVG(pressure) AS steady_pressure
  FROM sensors
  WHERE MOVING_STD(temperature, 20) < 0.1
    AND MOVING_STD(pressure, 20) < 0.05
  GROUP BY device_id
```

Unstable periods are dropped; only stable periods emit. Useful for calibration and quality control.

## View chaining for progressive refinement

Build computation layers, each adding sophistication:

```sql
-- Layer 1: raw signal conditioning
CREATE MATERIALIZED VIEW conditioned AS
  SELECT value,
         FIR(value, ARRAY[0.1, 0.2, 0.4, 0.2, 0.1]) AS filtered
  FROM raw_signal;

-- Layer 2: feature extraction
CREATE MATERIALIZED VIEW features AS
  SELECT filtered,
         MOVING_AVERAGE(filtered, 30) AS trend,
         MOVING_STD(filtered, 30) AS volatility
  FROM conditioned;

-- Layer 3: decision
CREATE MATERIALIZED VIEW decisions AS
  SELECT filtered, trend, volatility
  FROM features
  WHERE filtered > trend + 2 * volatility
```

Each layer has a clear responsibility: conditioning, feature extraction, decision making. Changes to one layer don't require modifying the others.

## Reference data enrichment

Use tables for configuration and lookups:

```sql
CREATE TABLE watchlist (
  account_id DOUBLE PRECISION PRIMARY KEY,
  risk_score DOUBLE PRECISION
);

CREATE MATERIALIZED VIEW watched_trades AS
  SELECT t.instrument_id, t.price, t.quantity, t.account_id, w.risk_score
  FROM trades t JOIN watchlist w ON t.account_id = w.account_id
```

Tables hold static or slowly-changing data. Table updates become effective immediately in downstream filtering.

## DSP pipelines

Signal processing chains for sensor and waveform data:

```sql
-- Step 1: bandpass filter to isolate frequency of interest
CREATE MATERIALIZED VIEW bandpass AS
  SELECT vibration,
         FIR(vibration, ARRAY[0.02, 0.05, 0.1, 0.15, 0.18, 0.15, 0.1, 0.05, 0.02]) AS bp_vibration
  FROM sensors;

-- Step 2: envelope detection via moving max
CREATE MATERIALIZED VIEW envelope AS
  SELECT bp_vibration,
         MOVING_MAX(ABS(bp_vibration), 20) AS envelope
  FROM bandpass;

-- Step 3: peak detection on the envelope
CREATE MATERIALIZED VIEW fault_events AS
  SELECT envelope,
         PEAK_DETECT(envelope, 50) AS fault_peak
  FROM envelope
```

This pattern isolates a frequency band, extracts the envelope, and detects fault events as peaks. Common in vibration-based condition monitoring.

## Live dashboards

Create materialized views designed to serve dashboard queries:

```sql
CREATE MATERIALIZED VIEW zone_summary AS
  SELECT zone_id,
         MOVING_AVERAGE(power_output, 60) AS avg_power,
         MOVING_MIN(power_output, 60) AS min_power,
         MOVING_MAX(power_output, 60) AS max_power,
         MOVING_STD(power_output, 60) AS power_variability
  FROM solar_panels
  GROUP BY zone_id;
```

Dashboard clients query `zone_summary` with `SELECT * FROM zone_summary`. Since the view is materialized, this is a TIER1_READ — a direct read of available output rows with no computation at query time.

## Multi-resolution analysis

Compute the same metric at different time scales:

```sql
CREATE MATERIALIZED VIEW multi_scale AS
  SELECT price,
         MOVING_AVERAGE(price, 10)  AS ma_10,
         MOVING_AVERAGE(price, 50)  AS ma_50,
         MOVING_AVERAGE(price, 200) AS ma_200,
         MOVING_STD(price, 10)  AS vol_10,
         MOVING_STD(price, 50)  AS vol_50,
         MOVING_STD(price, 200) AS vol_200
  FROM trades
```

Short windows capture fast-moving signals. Long windows capture slow trends. Comparing across scales reveals regime changes — for example, when short-term volatility exceeds long-term volatility, the market structure may be shifting.

## Composable view design guidelines

1. Break logic into small, named units
2. Materialize boundaries that need observability or reuse
3. Keep each stage semantically narrow (aggregation, smoothing, thresholding, enrichment)
4. Use `VIEW` for internal composition-only steps
5. Use `MATERIALIZED VIEW` for externally observed results
6. Verify each stage independently with `SELECT` before composing downstream
