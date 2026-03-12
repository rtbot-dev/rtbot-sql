---
title: Functions
sidebar_position: 4
---

# Functions

RtBot SQL provides streaming functions that compile to RtBot operators. Each function maintains internal state and updates incrementally with every new message.

## Cumulative aggregates

These functions aggregate all data seen since the start of the stream. They do not take a window parameter.

### SUM(expr)

Running sum of all values.

```sql
SELECT SUM(quantity) AS total_volume FROM trades
```

Compiles to: `CumulativeSum` operator.

### COUNT(*)

Running count of messages received.

```sql
SELECT COUNT(*) AS trade_count FROM trades
```

Compiles to: `Count` operator.

### AVG(expr)

Running average of all values. Computed as `SUM(expr) / COUNT(*)`.

```sql
SELECT AVG(price) AS mean_price FROM trades
```

Compiles to: `CumulativeSum` + `Count` + division.

## Windowed aggregates

These functions operate over a sliding window of the last N values. The window size is a required integer parameter.

**Warmup behavior:** All windowed functions begin producing output from the first message. During the first N-1 messages (the warmup period), they operate on whatever data is available rather than a full window. For most use cases this is fine, but in production alerting scenarios you may want to discard or gate early outputs to avoid false positives during startup.

### MOVING_AVERAGE(expr, N)

Average of the last N values. Also known as Simple Moving Average (SMA).

```sql
SELECT MOVING_AVERAGE(price, 20) AS sma_20 FROM trades
```

The first N-1 outputs use all available data. From the Nth message onward, the oldest value is dropped and the newest is added — constant-time update.

Compiles to: `MovingAverage(window_size=N)` operator.

### MOVING_SUM(expr, N)

Sum of the last N values.

```sql
SELECT MOVING_SUM(quantity, 50) AS volume_50 FROM trades
```

During the first N-1 messages, the sum is computed over all available data. From the Nth message onward, it maintains a constant-size sliding window.

Compiles to: `MovingSum(window_size=N)` operator.

### MOVING_COUNT(N)

Count of distinct keys seen in the last N messages. Useful for cardinality tracking.

```sql
SELECT MOVING_COUNT(100) AS active_instruments FROM trades
```

Compiles to: `Constant(1)` → `MovingSum(window_size=N)` operator chain.

### MOVING_STD(expr, N)

Standard deviation over the last N values. `STDDEV` is accepted as an alias but `MOVING_STD` is the preferred form.

```sql
SELECT MOVING_STD(price, 20) AS volatility FROM trades
```

During the first N-1 messages, the standard deviation is computed over all available values. This means early outputs may have higher variance due to the small sample size. From the Nth message onward, the window is full.

Compiles to: `StandardDeviation(window_size=N)` operator.

### MOVING_MIN(expr, N)

Minimum value in the last N values.

```sql
SELECT MOVING_MIN(price, 50) AS price_floor FROM trades
```

During the first N-1 messages, the minimum is computed over all available values.

Compiles to: `WindowMinMax(mode="min", window_size=N)` operator.

### MOVING_MAX(expr, N)

Maximum value in the last N values.

```sql
SELECT MOVING_MAX(price, 50) AS price_ceiling FROM trades
```

During the first N-1 messages, the maximum is computed over all available values.

Compiles to: `WindowMinMax(mode="max", window_size=N)` operator.

## DSP functions

Digital signal processing functions for sensor data, waveforms, and time-series conditioning.

### FIR(expr, ARRAY[coefficients])

Finite Impulse Response filter. Applies a weighted sum of the last N values, where N is the number of coefficients.

```sql
-- 9-tap low-pass filter
SELECT FIR(vibration, ARRAY[0.02, 0.05, 0.1, 0.15, 0.18, 0.15, 0.1, 0.05, 0.02]) AS filtered
FROM bearing_sensors
```

FIR filters are used for smoothing, noise removal, bandpass filtering, and signal conditioning. The coefficients determine the filter's frequency response.

During the first N-1 messages (where N is the number of coefficients), the filter operates with zero-padded history. Early outputs may not represent the intended frequency response.

Compiles to: `FiniteImpulseResponse(coefficients=[...])` operator.

### IIR(expr, ARRAY[ff_coefficients], ARRAY[fb_coefficients])

Infinite Impulse Response filter. Uses both feedforward and feedback coefficients.

```sql
-- Second-order low-pass filter
SELECT IIR(power_quality, ARRAY[0.0675, 0.1349, 0.0675], ARRAY[1.0, -1.1430, 0.4128]) AS filtered
FROM grid_sensors
```

IIR filters can achieve sharper frequency response than FIR filters with fewer coefficients, but they can be unstable if the feedback coefficients are not chosen carefully.

The filter starts with zero initial state for both feedforward and feedback history. The output stabilizes after a transient period that depends on the filter's pole locations.

Compiles to: `InfiniteImpulseResponse(ff_coefficients=[...], fb_coefficients=[...])` operator.

### RESAMPLE(expr, interval)

Resamples the signal to a constant rate. Outputs one value per `interval` time units using interpolation.

```sql
-- Resample to 1-second intervals (assuming millisecond timestamps)
SELECT RESAMPLE(temperature, 1000) AS temp_1hz FROM sensors
```

Useful when input data arrives at irregular intervals and you need a uniform sampling rate for downstream computation.

Compiles to: `ResamplerConstant(interval=N)` operator.

### PEAK_DETECT(expr, N)

Detects local peaks (maxima) in the signal over a window of N values. Emits the peak value when a peak is confirmed (i.e., when subsequent values have decreased).

```sql
SELECT PEAK_DETECT(pressure, 30) AS pressure_peak FROM pipeline_sensors
```

The detector requires at least N messages before it can confirm a peak. During the warmup period, no peaks are emitted.

Compiles to: `PeakDetector(window_size=N)` operator.

## Scalar math functions

Standard math functions that operate on individual values. These do not maintain state.

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(expr)` | Absolute value | `ABS(price - ma)` |
| `FLOOR(expr)` | Round down to integer | `FLOOR(temperature)` |
| `CEIL(expr)` | Round up to integer | `CEIL(price)` |
| `ROUND(expr)` | Round to nearest integer | `ROUND(ratio * 100)` |
| `LN(expr)` | Natural logarithm | `LN(price)` |
| `LOG10(expr)` | Base-10 logarithm | `LOG10(magnitude)` |
| `EXP(expr)` | Exponential (e^x) | `EXP(log_return)` |
| `POWER(expr, n)` | Raise to power | `POWER(deviation, 2)` |
| `SIN(expr)` | Sine | `SIN(angle)` |
| `COS(expr)` | Cosine | `COS(angle)` |
| `TAN(expr)` | Tangent | `TAN(angle)` |
| `SIGN(expr)` | Sign (-1, 0, or 1) | `SIGN(momentum)` |

## Expressions and operators

### Arithmetic

`+`, `-`, `*`, `/` — standard arithmetic operators.

The compiler optimizes automatically:
- When one side is constant, it uses a scalar operator (faster)
- When both sides are constant, it folds them at compile time

### Comparison

`>`, `<`, `>=`, `<=`, `=`, `!=` — standard comparison operators. Return 1.0 for true, 0.0 for false.

### Logical

`AND`, `OR`, `NOT` — boolean logic on comparison results.

### CASE expression

```sql
SELECT CASE
  WHEN price > 200 THEN 3
  WHEN price > 100 THEN 2
  ELSE 1
END AS price_tier
FROM trades
```

Compiled as mutually exclusive gates and a multiplexer.

## Tier-2 aggregate functions

When you SELECT from a materialized view (not a stream), you can use aggregate functions that operate across the available output rows. These are evaluated at query time, not incrementally.

```sql
-- Aggregate across all instruments
SELECT SUM(total_vol) AS market_volume,
       COUNT(*) AS active_instruments,
       AVG(avg_price) AS market_avg_price
FROM instrument_stats
```

Supported tier-2 aggregates: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`.

These are standard SQL aggregates — they scan the available output rows at query time.

## Combining functions

Functions can be combined in expressions:

```sql
SELECT MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band,
       MOVING_AVERAGE(price, 20) - 2 * MOVING_STD(price, 20) AS lower_band
FROM trades
```

The compiler shares computation when the same function appears multiple times with the same arguments. In the example above, `MOVING_AVERAGE(price, 20)` is computed once and reused.
