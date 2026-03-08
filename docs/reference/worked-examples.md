---
title: Worked Examples
sidebar_position: 8
---

# Worked Examples

Complete, realistic examples showing RtBot SQL in production-like scenarios. Each example includes the full SQL, an explanation of what the pipeline does, and expected behavior.

## Bollinger Bands (financial)

Bollinger Bands are a volatility indicator: a moving average with upper and lower bands at 2 standard deviations. Prices outside the bands signal unusual activity.

```sql
CREATE STREAM trades (
  instrument_id DOUBLE PRECISION,
  price         DOUBLE PRECISION,
  quantity      DOUBLE PRECISION
);

CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20)                          AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * MOVING_STD(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id;
```

**What happens:** Each instrument gets independent 20-period bands. When a new trade arrives, the moving average and standard deviation update in constant time. The `GROUP BY instrument_id` ensures that instrument A's volatility doesn't affect instrument B's bands.

### Bollinger alerts with HAVING

```sql
CREATE MATERIALIZED VIEW bollinger_alerts AS
  SELECT instrument_id, price,
         MOVING_AVERAGE(price, 20) AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20)
```

Two-sided variant detecting both upper and lower band breaks:

```sql
CREATE MATERIALIZED VIEW price_alerts AS
  SELECT instrument_id, price
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20)
      OR price < MOVING_AVERAGE(price, 20) - 2 * MOVING_STD(price, 20)
```

**Use case:** Real-time price monitoring dashboards, automated trading signal generation, anomaly detection in financial markets.

## Volume spike detection (trade surveillance)

Detect unusually large trades by comparing each trade's notional value against a rolling baseline.

```sql
CREATE STREAM trades (
  instrument_id DOUBLE PRECISION,
  price         DOUBLE PRECISION,
  quantity      DOUBLE PRECISION,
  quote_qty     DOUBLE PRECISION
);

CREATE MATERIALIZED VIEW notional_stats AS
  SELECT instrument_id, price, quantity, quote_qty,
         MOVING_AVERAGE(quote_qty, 50) AS avg_notional,
         MOVING_STD(quote_qty, 50)         AS std_notional,
         MOVING_AVERAGE(quote_qty, 50) + 3 * MOVING_STD(quote_qty, 50) AS upper_band
  FROM trades
  GROUP BY instrument_id;

CREATE MATERIALIZED VIEW volume_alerts AS
  SELECT instrument_id, quote_qty, avg_notional, upper_band
  FROM notional_stats
  WHERE quote_qty > upper_band;
```

**What happens:** The first view computes per-instrument statistics on notional value (trade size in quote currency). The second view filters for trades exceeding 3 standard deviations above the 50-period mean — a volume spike. The view chain propagates automatically.

**Use case:** Trade surveillance, market manipulation detection, unusual activity alerting.

## Predictive maintenance (industrial IoT)

Monitor bearing vibration and temperature on manufacturing equipment. Use FIR filtering for vibration analysis and Bollinger Bands for thermal anomaly detection.

```sql
CREATE STREAM cnc_sensors (
  bearing_vibration DOUBLE PRECISION,
  spindle_speed     DOUBLE PRECISION,
  temperature       DOUBLE PRECISION,
  coolant_flow      DOUBLE PRECISION
);

-- Bandpass filter on vibration (11-tap FIR)
CREATE MATERIALIZED VIEW vibration_analysis AS
  SELECT bearing_vibration,
         FIR(bearing_vibration,
             ARRAY[0.01, 0.03, 0.07, 0.12, 0.17, 0.20, 0.17, 0.12, 0.07, 0.03, 0.01]) AS filtered_vibration,
         MOVING_AVERAGE(bearing_vibration, 100) AS baseline_vibration,
         MOVING_STD(bearing_vibration, 100) AS vibration_std
  FROM cnc_sensors;

-- Thermal anomaly detection
CREATE MATERIALIZED VIEW thermal_stats AS
  SELECT temperature,
         MOVING_AVERAGE(temperature, 200) AS temp_baseline,
         MOVING_STD(temperature, 200) AS temp_std,
         MOVING_AVERAGE(temperature, 200) + 2.5 * MOVING_STD(temperature, 200) AS temp_upper
  FROM cnc_sensors;

-- Maintenance alerts
CREATE MATERIALIZED VIEW maintenance_alerts AS
  SELECT temperature, temp_baseline, temp_upper
  FROM thermal_stats
  WHERE temperature > temp_upper;
```

**What happens:** The FIR filter isolates the frequency band associated with bearing defects. The thermal pipeline detects temperature excursions above 2.5 standard deviations. Maintenance alerts fire when temperature indicates abnormal operation.

**Use case:** Predictive maintenance for CNC machines, turbines, pumps, and rotating equipment.

## Multi-level view chain (progressive refinement)

Build increasingly refined statistics through a chain of views.

```sql
CREATE STREAM trades (
  instrument_id DOUBLE PRECISION,
  price         DOUBLE PRECISION,
  quantity      DOUBLE PRECISION
);

-- Level 1: raw per-instrument aggregates
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity) AS total_vol,
         COUNT(*)      AS trade_count
  FROM trades
  GROUP BY instrument_id;

-- Level 2: smooth the volume
CREATE MATERIALIZED VIEW vol_trends AS
  SELECT instrument_id,
         total_vol,
         MOVING_AVERAGE(total_vol, 10) AS smooth_vol
  FROM instrument_stats;

-- Level 3: further smoothing
CREATE MATERIALIZED VIEW vol_smooth AS
  SELECT instrument_id,
         smooth_vol,
         MOVING_AVERAGE(smooth_vol, 5) AS very_smooth_vol
  FROM vol_trends;
```

**What happens:** A single trade insertion cascades through three levels. Level 1 aggregates volume per instrument. Level 2 smooths it with a 10-period MA. Level 3 applies another 5-period MA for a very smooth trend line.

**Use case:** Multi-resolution analysis, trend extraction, noise reduction through progressive filtering.

## Cross-instrument correlation (multi-source)

Detect when historically correlated instruments diverge — a potential trading signal or risk indicator.

```sql
CREATE STREAM btc_trades (price DOUBLE PRECISION, quantity DOUBLE PRECISION);
CREATE STREAM eth_trades (price DOUBLE PRECISION, quantity DOUBLE PRECISION);

CREATE MATERIALIZED VIEW cross_stats AS
  SELECT b.price AS btc_price,
         e.price AS eth_price,
         MOVING_AVERAGE(b.price, 60) AS btc_mean,
         MOVING_AVERAGE(e.price, 60) AS eth_mean,
         MOVING_STD(b.price, 60) AS btc_std,
         MOVING_STD(e.price, 60) AS eth_std,
         MOVING_AVERAGE(b.price * e.price, 60) AS e_xy
  FROM btc_trades b, eth_trades e;

-- Pearson correlation: (E[XY] - E[X]*E[Y]) / (std_X * std_Y)
CREATE MATERIALIZED VIEW correlation AS
  SELECT btc_price, eth_price,
         (e_xy - btc_mean * eth_mean) / (btc_std * eth_std) AS corr_60
  FROM cross_stats;

-- Alert when correlation breaks down
CREATE MATERIALIZED VIEW correlation_alerts AS
  SELECT btc_price, eth_price, corr_60
  FROM correlation
  WHERE corr_60 < 0;
```

**What happens:** Two independent streams are joined by event time. The pipeline computes a rolling 60-period Pearson correlation coefficient. When BTC and ETH prices become negatively correlated, an alert fires.

**Use case:** Pairs trading, portfolio risk monitoring, cross-asset correlation surveillance.

## Fleet operations (logistics)

Monitor vehicle telemetry for fuel anomalies and route efficiency.

```sql
CREATE STREAM vehicle_telemetry (
  latitude   DOUBLE PRECISION,
  longitude  DOUBLE PRECISION,
  speed      DOUBLE PRECISION,
  fuel_level DOUBLE PRECISION,
  engine_rpm DOUBLE PRECISION
);

-- GPS smoothing (remove jitter)
CREATE MATERIALIZED VIEW smooth_position AS
  SELECT latitude, longitude, speed,
         MOVING_AVERAGE(latitude, 5)  AS smooth_lat,
         MOVING_AVERAGE(longitude, 5) AS smooth_lon,
         MOVING_AVERAGE(speed, 10)    AS smooth_speed
  FROM vehicle_telemetry;

-- Fuel anomaly detection
CREATE MATERIALIZED VIEW fuel_stats AS
  SELECT fuel_level,
         MOVING_AVERAGE(fuel_level, 20)  AS fuel_baseline,
         MOVING_STD(fuel_level, 20)          AS fuel_std
  FROM vehicle_telemetry;

CREATE MATERIALIZED VIEW fuel_alerts AS
  SELECT fuel_level, fuel_baseline, fuel_std
  FROM fuel_stats
  WHERE ABS(fuel_level - fuel_baseline) > 3 * fuel_std;
```

**What happens:** GPS coordinates are smoothed with a 5-point moving average to remove satellite jitter. Fuel level is monitored with Bollinger-style bands — sudden drops (theft, leaks) or jumps (refueling events) trigger alerts.

**Use case:** Fleet management, fuel theft detection, route optimization, driver behavior monitoring.
