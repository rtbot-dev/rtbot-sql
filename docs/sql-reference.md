# RTBot SQL Language Reference

RTBot SQL compiles standard SQL into incremental operator graphs that process data one message at a time. Every query is *maintained*, not recomputed — results update as each message arrives, with O(1) cost per message for most operations.

All values are IEEE 754 double-precision floating-point. There are no string, boolean, or timestamp column types — timestamps are carried as event-time values within the data itself.

---

## Statements

### CREATE TABLE

Declares an input stream schema. Despite the SQL keyword `TABLE`, this defines a **stream** — an append-only sequence of messages. No data is stored until messages arrive.

**Syntax:**

```sql
CREATE TABLE stream_name (
  column1 DOUBLE PRECISION,
  column2 DOUBLE PRECISION,
  ...
)
```

**Description:**

Registers a named stream with an ordered list of columns. Each column maps to a positional index in the input vector (column1 = index 0, column2 = index 1, etc.). No timestamp column is needed — RTBot assigns event timestamps automatically from the message arrival sequence.

**Example:**

```sql
CREATE TABLE trades (
  instrument_id DOUBLE PRECISION,
  price DOUBLE PRECISION,
  quantity DOUBLE PRECISION,
  account_id DOUBLE PRECISION
)
```

**Notes:**
- Column type is always `DOUBLE PRECISION` (or `DOUBLE`). All data is IEEE 754 double.
- Column order determines vector indices: first column is index 0.
- The stream does not exist until this statement is executed. No storage is allocated.
- `CREATE TABLE` with `PRIMARY KEY` creates a **reference table** (lookup table), not a stream. See [CREATE TABLE with PRIMARY KEY](#create-table-with-primary-key).

---

### CREATE TABLE with PRIMARY KEY

Declares a mutable reference table for lookup/enrichment data (watchlists, configuration, zone maps).

**Syntax:**

```sql
CREATE TABLE table_name (
  key_column DOUBLE PRIMARY KEY,
  column2 DOUBLE,
  ...
)
```

**Description:**

Creates a keyed table backed by a changelog stream. Unlike streams, tables support mutable state — rows can be inserted, updated, and deleted. Tables are used with JOIN for enrichment patterns (e.g., filtering trades against a watchlist).

**Example:**

```sql
CREATE TABLE watchlist (
  account_id DOUBLE PRIMARY KEY,
  risk_score DOUBLE
)
```

**Incremental Behavior:**

Table state is maintained via a changelog stream (`rtbot:sql:table:<name>:changelog`). Each INSERT writes to the changelog; DELETE writes a NaN-tombstone row. JOINs against the table use the current state at the time of each incoming stream message.

**Notes:**
- Primary key determines the lookup key for JOINs.
- DELETE is expressed by writing a row with NaN values for non-key columns.

---

### CREATE MATERIALIZED VIEW

The core statement. Compiles a SQL query into an incremental operator graph and deploys it as a continuously-maintained pipeline.

**Syntax:**

```sql
CREATE MATERIALIZED VIEW view_name AS
  SELECT ...
  FROM source
  [WHERE condition]
  [GROUP BY column]
  [HAVING condition]
```

**Description:**

Compiles the SELECT query into an RTBot program (a DAG of operators) and deploys it. The view updates on every incoming message — not on a schedule, not on a trigger. It is always current. The result is readable via `SELECT * FROM view_name`.

**Example:**

```sql
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity)  AS total_vol,
         COUNT(*)       AS trade_count
  FROM trades
  GROUP BY instrument_id
```

**Incremental Behavior:**

The compiled operator graph processes each incoming message in O(1) time (for cumulative aggregates) or O(1) amortized (for windowed functions). State is maintained per key when GROUP BY is used. The output reflects the latest aggregated state for each key.

**Notes:**
- The source can be a stream, another materialized view, or a table.
- Views can chain: a materialized view can read from another materialized view, forming multi-level pipelines.
- Expression de-duplication ensures that identical sub-expressions compile to a single operator instance. For example, `MOVING_AVERAGE(price, 20)` appearing 3 times in a SELECT compiles to one `MovingAverage` operator.
- Field map: the output vector's column positions are determined by SELECT order. With GROUP BY, the key column is at index 0.

---

### CREATE VIEW

Declares a non-materialized (virtual) view. The query is stored but not deployed — it is evaluated on demand.

**Syntax:**

```sql
CREATE VIEW view_name AS
  SELECT ...
  FROM source
  [WHERE condition]
  [GROUP BY column]
  [HAVING condition]
```

**Description:**

Stores the compiled operator graph and field map in the catalog without deploying a pipeline. When another query references this view, the graph is inlined into the consuming pipeline (zero-hop composition). Useful as a reusable building block.

**Example:**

```sql
CREATE VIEW live_stats AS
  SELECT instrument_id,
         SUM(quantity) AS vol,
         COUNT(*)      AS cnt
  FROM trades
  GROUP BY instrument_id
```

**Notes:**
- No output streams are created. The view exists only in the catalog.
- Reading from a VIEW via SELECT triggers an ephemeral pipeline (Tier 3 execution).
- When a MATERIALIZED VIEW references a VIEW over the same source stream, the compiler inlines the VIEW's operator graph — no extra stream hop.

---

### INSERT INTO

Feeds data into a stream. Intended for development and testing; production ingestion uses Redis XADD directly.

**Syntax:**

```sql
INSERT INTO stream_name VALUES (value1, value2, ...)
```

**Description:**

Inserts a single row into the named stream. The number of values must match the stream's column count. All values are parsed as doubles.

**Example:**

```sql
INSERT INTO trades VALUES (1, 150.0, 200, 42)
```

**Notes:**
- Values map positionally to columns defined in CREATE TABLE.
- In production, data is ingested via `XADD stream_name * field1 value1 field2 value2 ...` directly to Redis Streams.

---

### SELECT

Point-in-time read of current state. Reads the maintained result — does not recompute.

**Syntax:**

```sql
SELECT column1, column2, ...
FROM source
[WHERE condition]
[GROUP BY column]
[HAVING condition]
[ORDER BY column [ASC|DESC]]
[LIMIT n]
```

**Description:**

Reads data from a stream, materialized view, or table. The execution strategy depends on the query complexity:

| Tier | Pattern | Execution |
|------|---------|-----------|
| **Tier 1: Direct Read** | `SELECT columns FROM view LIMIT n` | Reads stored output directly. O(1). |
| **Tier 2: Scan + Filter** | `SELECT columns FROM source WHERE condition LIMIT n` | Scans with stateless per-row filter. |
| **Tier 3: Ephemeral** | `SELECT columns FROM source GROUP BY ...` | Deploys temporary pipeline, replays data, collects result. |

**Example:**

```sql
-- Tier 1: read latest state per key
SELECT * FROM instrument_stats LIMIT 100

-- Tier 2: filtered scan
SELECT instrument_id, price FROM trades WHERE price > 100 LIMIT 10

-- Tier 3: ad-hoc aggregation
SELECT instrument_id, SUM(quantity) AS total_qty, COUNT(*) AS cnt
FROM trades GROUP BY instrument_id
```

**Cross-key aggregation:**

SELECT with aggregate functions over a keyed materialized view computes the aggregate across all keys:

```sql
SELECT SUM(total_volume) AS total, COUNT(*) AS cnt
FROM instrument_stats
```

This reads the current per-key rows and applies the aggregate in a single pass (Tier 2).

**Notes:**
- `SELECT * FROM stream` without LIMIT is an error for unbounded streams.
- ORDER BY requires LIMIT (compiles to a TopK operator).
- Aliases defined in SELECT can be referenced in WHERE and HAVING.

---

### DROP MATERIALIZED VIEW

Removes a materialized view and its associated pipeline and state.

**Syntax:**

```sql
DROP MATERIALIZED VIEW view_name
```

**Description:**

Stops the pipeline, removes output streams, and deletes the view from the catalog.

**Example:**

```sql
DROP MATERIALIZED VIEW bollinger
```

**Notes:**
- Fails with an error if other views depend on the dropped entity. Drop dependents first.
- `DROP VIEW` and `DROP TABLE` follow the same syntax for their respective entity types.

---

### DELETE FROM

Removes a row from a reference table by primary key.

**Syntax:**

```sql
DELETE FROM table_name WHERE key_column = value
```

**Description:**

Writes a tombstone (NaN values for non-key columns) to the table's changelog stream. Downstream JOINs will no longer match the deleted key.

**Example:**

```sql
DELETE FROM watchlist WHERE account_id = 42
```

---

### SUBSCRIBE

*Phase 2 — Upcoming.*

Live push of materialized view updates. Each time the view state changes, the new row is delivered to the subscriber.

**Expected syntax:**

```sql
SUBSCRIBE TO view_name
```

**Expected behavior:**

Returns a stream of update events. In Redis, implemented via `XREAD BLOCK` on the view's changelog stream. In browser runtime, delivered via WebSocket callbacks.

---

## Clauses

### WHERE

Row-level filter applied before aggregation. Evaluated incrementally per message.

**Syntax:**

```sql
SELECT ... FROM source WHERE condition
```

**Description:**

Filters rows based on a boolean condition. The condition is compiled to comparison operators + a Demultiplexer that gates the data flow. Only rows satisfying the condition pass through to downstream operators.

**Supported conditions:**

- Comparison: `column > value`, `column < value`, `column >= value`, `column <= value`, `column = value`, `column != value`
- Logical: `condition AND condition`, `condition OR condition`, `NOT condition`
- Expressions: `2 * price > 100`, `price - MOVING_AVERAGE(price, 20) > 5.0`
- Alias references: `SELECT 2*price AS dp FROM trades WHERE dp > 100 LIMIT 10`

**Example:**

```sql
SELECT instrument_id, price, quantity
FROM trades
WHERE price > 100
LIMIT 100
```

**Incremental Behavior:**

Each incoming message is evaluated against the predicate. If true, the message passes through. If false, it is discarded. O(1) per message for simple comparisons. For stream-vs-stream comparisons (e.g., `price > MOVING_AVERAGE(price, 20)`), uses synchronized comparison operators.

**Notes:**
- WHERE is evaluated before GROUP BY. Filtered rows never reach the aggregation operators.
- Aggregate functions in WHERE are an error. Use HAVING for post-aggregation filtering.

---

### GROUP BY

Creates independent sub-pipelines per key. Each unique key value gets its own isolated operator state.

**Syntax:**

```sql
SELECT key_column, aggregate_functions...
FROM source
GROUP BY key_column
```

**Description:**

Compiles to a `KeyedPipeline` operator that maintains a HashMap of key → sub-graph instances. Each sub-graph is a clone of the compiled prototype (the aggregate/window operators from the SELECT list). New keys are instantiated automatically on first message — no pre-registration, no capacity planning.

**Composite keys:**

```sql
SELECT instrument_id, exchange_id, SUM(quantity) AS total
FROM trades2
GROUP BY instrument_id, exchange_id
```

Multiple GROUP BY columns are hashed via a `Linear` operator to produce a single composite key.

**Example:**

```sql
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity)  AS total_vol,
         COUNT(*)       AS trade_count
  FROM trades
  GROUP BY instrument_id
```

**Incremental Behavior:**

When a message arrives with `instrument_id = 42`:
1. KeyedPipeline looks up key 42 in its HashMap.
2. If new: clones the prototype sub-graph and stores it.
3. Routes the message to key 42's sub-graph.
4. The sub-graph updates its aggregates (SUM, COUNT) in O(1).
5. Output: `[42, updated_total_vol, updated_trade_count]`.

Other keys are unaffected. State is fully isolated per key.

**Notes:**
- The GROUP BY key column is always at index 0 in the output vector.
- Non-aggregated columns (e.g., `price` in a GROUP BY query) pass through from the latest message for that key.
- Per-key state persists across messages. Aggregates are cumulative, not per-batch.

---

### HAVING

Post-aggregation filter. Only emits rows where the condition is true after aggregation.

**Syntax:**

```sql
SELECT key, aggregates...
FROM source
GROUP BY key
HAVING condition
```

**Description:**

Applied inside the per-key prototype sub-graph, after aggregate computation but before output. Compiled to comparison operators + Demultiplexer within the prototype. Shares operator instances with SELECT via expression de-duplication — `COUNT(*)` in both SELECT and HAVING compiles to a single `CountNumber` operator.

**Patterns:**

```sql
-- Threshold alert: emit only when count exceeds limit
HAVING COUNT(*) > 10

-- Bollinger band alert: emit when price breaks upper band
HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)

-- Two-sided alert (OR)
HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
    OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)

-- Deviation from baseline
HAVING fuel_level < MOVING_AVERAGE(fuel_level, 20) - 10.0

-- Alias reference
SELECT instrument_id, AVG(price) AS avg_p
FROM trades GROUP BY instrument_id
HAVING avg_p > 100
```

**Incremental Behavior:**

On every message, the HAVING condition is re-evaluated against the updated aggregate state. If true, the row passes through to output. If false, it is suppressed. This makes HAVING the primary mechanism for alert/anomaly detection patterns — the view only emits when a threshold is breached.

**Notes:**
- HAVING supports stream-vs-stream comparisons (e.g., `price > aggregate_expression`), compiled as `CompareSyncGT` + `Demultiplexer`.
- Aggregate aliases from SELECT can be used directly in HAVING.

---

### ORDER BY ... LIMIT

Ranks output by a column and returns the top-N results.

**Syntax:**

```sql
SELECT ... FROM source ORDER BY column [ASC|DESC] LIMIT n
```

**Description:**

Compiles to a `TopK` operator. ORDER BY without LIMIT is an error — RTBot does not sort unbounded streams.

**Example:**

```sql
SELECT instrument_id, quantity
FROM trades
ORDER BY quantity DESC
LIMIT 3
```

**Notes:**
- ASC (ascending) and DESC (descending) are both supported.
- LIMIT without ORDER BY caps the result set for scan-based reads.

---

### LIMIT

Caps the result set size for point-in-time queries.

**Syntax:**

```sql
SELECT ... FROM source [WHERE ...] LIMIT n
```

**Description:**

Limits the number of rows returned by a SELECT. For Tier 1 reads, limits the stream range query. For Tier 2 scans, stops scanning after N matching rows. Does not apply to materialized view output (materialized views maintain all keys).

---

## Aggregate Functions

### SUM

Running cumulative sum.

**Syntax:**

```sql
SUM(expr)
```

**Description:**

Computes the cumulative sum of an expression over all messages received for a given key. O(1) per message — adds the new value to the running total.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to sum |

**Incremental Behavior:**

Maintains a single accumulator. Each message adds the new value. This is a running total, not a batch recomputation. State: one double per key.

**Example:**

```sql
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity) AS total_vol
  FROM trades
  GROUP BY instrument_id

-- Input (for instrument_id = 1): [100, 200, 150]
-- Output total_vol after each:    [100, 300, 450]
```

**Operator:** `CumulativeSum`

---

### COUNT

Running count of messages.

**Syntax:**

```sql
COUNT(*)
```

**Description:**

Counts the total number of messages received for a given key. O(1) per message — increments a counter.

**Incremental Behavior:**

Maintains a single counter. Each message increments by 1. State: one double per key.

**Example:**

```sql
CREATE MATERIALIZED VIEW trade_counts AS
  SELECT instrument_id,
         COUNT(*) AS trade_count
  FROM trades
  GROUP BY instrument_id

-- Input (for instrument_id = 1): [msg1, msg2, msg3, msg4, msg5]
-- Output trade_count after each:  [1,    2,    3,    4,    5   ]
```

**Operator:** `CountNumber`

**Notes:**
- `COUNT(*)` takes no arguments. `COUNT(expr)` is not supported — use `COUNT(*)`.

---

### AVG

Running cumulative average.

**Syntax:**

```sql
AVG(expr)
```

**Description:**

Computes the cumulative average (running mean) of an expression. Implemented as `SUM(expr) / COUNT(*)` — two operators combined with division. O(1) per message.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to average |

**Incremental Behavior:**

Maintains a running sum and a running count. Each message updates both, then divides. State: two doubles per key.

**Example:**

```sql
CREATE MATERIALIZED VIEW avg_prices AS
  SELECT instrument_id,
         AVG(price) AS avg_price
  FROM trades
  GROUP BY instrument_id

-- Input prices (for instrument_id = 1): [100, 200, 150]
-- Output avg_price after each:           [100, 150, 150]
```

**Operators:** `CumulativeSum` + `CountNumber` + `Division`

---

### MIN / MAX

Minimum and maximum values. Available as cross-key aggregates in Tier 2 SELECT queries over keyed materialized views.

**Syntax:**

```sql
MIN(expr)
MAX(expr)
```

**Example:**

```sql
SELECT MIN(total_volume) AS lo, MAX(total_volume) AS hi
FROM instrument_stats
```

**Notes:**
- MIN/MAX are available for cross-key aggregation (reading current per-key values).
- For windowed min/max within a streaming pipeline, use MOVING_MIN / MOVING_MAX.

---

## Window Functions

### MOVING_AVERAGE

Sliding window arithmetic mean over the last N messages.

**Syntax:**

```sql
MOVING_AVERAGE(expr, window_size)
```

**Description:**

Computes the mean of the last `window_size` values of `expr`. Maintains a circular buffer internally. O(1) per message — subtracts the oldest value and adds the newest.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to average |
| window_size | positive integer constant | Number of messages in the window |

**Incremental Behavior:**

Maintains a circular buffer of the last N values and a running sum. On each message: subtract the value leaving the window, add the new value, divide by the current buffer size. During warmup (fewer than N messages received), the average is computed over the available values.

**Example:**

```sql
CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20) AS mid_band
  FROM trades
  GROUP BY instrument_id

-- Input prices:  [10, 20, 15, 25, 30]  (window_size = 3)
-- mid_band:      [10, 15, 15, 20, 23.3]
```

**Operator:** `MovingAverage`

**Notes:**
- Warmup: for the first N-1 messages, the average uses fewer than N values. There is no NaN output during warmup — you always get a valid result.
- State: circular buffer of N doubles per key.

---

### MOVING_SUM

Sliding window cumulative sum over the last N messages.

**Syntax:**

```sql
MOVING_SUM(expr, window_size)
```

**Description:**

Computes the sum of the last `window_size` values. Same sliding window semantics as MOVING_AVERAGE but returns the sum, not the mean.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to sum |
| window_size | positive integer constant | Number of messages in the window |

**Incremental Behavior:**

Maintains a circular buffer and a running sum. O(1) per message.

**Example:**

```sql
CREATE MATERIALIZED VIEW rolling_volume AS
  SELECT instrument_id,
         MOVING_SUM(quantity, 50) AS vol_50
  FROM trades
  GROUP BY instrument_id
```

**Operator:** `MovingSum`

---

### MOVING_COUNT

Count of messages in the last N messages per key. Useful for velocity detection.

**Syntax:**

```sql
MOVING_COUNT(window_size)
```

**Description:**

Counts the number of messages received in the last `window_size` messages. Implemented internally as `MOVING_SUM(1, window_size)` — a constant 1 is fed into a MovingSum operator.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| window_size | positive integer constant | Number of messages in the window |

**Incremental Behavior:**

O(1) per message. After warmup, the count equals `window_size` (every slot in the window has a message). Useful in HAVING clauses for rate-based alerting.

**Example:**

```sql
-- Alert when an account trades more than 15 times in 20 messages
CREATE MATERIALIZED VIEW velocity_alerts AS
  SELECT account_id, instrument_id, price, quantity
  FROM trades
  GROUP BY account_id
  HAVING MOVING_COUNT(20) > 15
```

**Operators:** `ConstantNumber(1)` + `MovingSum`

---

### STDDEV

Sliding window standard deviation over the last N messages.

**Syntax:**

```sql
STDDEV(expr, window_size)
```

**Aliases:** `MOVING_STD`

**Description:**

Computes the standard deviation of the last `window_size` values. O(1) per message using Welford's online algorithm. This is what makes Bollinger-style anomaly detection a one-liner in RTBot SQL.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to compute std dev over |
| window_size | positive integer constant | Number of messages in the window |

**Incremental Behavior:**

Maintains sufficient statistics (sum, sum of squares, count) in a circular buffer. O(1) per message — no recomputation over the window.

**Example:**

```sql
CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20)                          AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id
```

**De-duplication:** In this example, `MOVING_AVERAGE(price, 20)` appears 3 times and `STDDEV(price, 20)` appears 2 times, but the compiler de-duplicates them to one `MovingAverage` and one `StandardDeviation` operator. All references share the same output.

**Operator:** `StandardDeviation`

---

### MOVING_MIN

Sliding window minimum over the last N messages.

**Syntax:**

```sql
MOVING_MIN(expr, window_size)
```

**Description:**

Tracks the minimum value in the last `window_size` messages.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to track |
| window_size | positive integer constant | Number of messages in the window |

**Example:**

```sql
SELECT instrument_id,
       MOVING_MIN(price, 5) AS min_p,
       MOVING_MAX(price, 5) AS max_p
FROM trades
GROUP BY instrument_id
```

**Operator:** `WindowMinMax` (mode: min)

---

### MOVING_MAX

Sliding window maximum over the last N messages.

**Syntax:**

```sql
MOVING_MAX(expr, window_size)
```

**Description:**

Tracks the maximum value in the last `window_size` messages.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The value to track |
| window_size | positive integer constant | Number of messages in the window |

**Operator:** `WindowMinMax` (mode: max)

---

## Signal Processing Functions

### FIR

Finite Impulse Response filter. Applies a weighted sum over a window of input samples using fixed coefficients.

**Syntax:**

```sql
FIR(expr, ARRAY[c0, c1, c2, ...])
```

**Description:**

Convolves the input signal with the coefficient array. The output at each step is `c0*x[n] + c1*x[n-1] + c2*x[n-2] + ...`. This is the standard FIR filter from signal processing — the same coefficients from MATLAB or scipy work directly.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The input signal |
| ARRAY[...] | array literal of doubles | Filter coefficients (tap weights) |

**Incremental Behavior:**

Maintains a circular buffer of the last N input samples (where N = number of coefficients). Each new sample shifts the buffer and computes the weighted sum in O(N). State is continuous across messages — no batch boundary discontinuities.

**Example:**

```sql
-- Simple 3-tap low-pass filter
CREATE MATERIALIZED VIEW filtered_signal AS
  SELECT device_id,
         FIR(temperature, ARRAY[0.25, 0.5, 0.25]) AS smooth_temp
  FROM sensors
  GROUP BY device_id
```

**Operator:** `FiniteImpulseResponse`

**Notes:**
- Coefficient count determines the filter order.
- Coefficients must be constant (array literal). No dynamic coefficient computation.

---

### IIR

Infinite Impulse Response filter. Implements the standard difference equation with feedforward (numerator) and feedback (denominator) coefficients.

**Syntax:**

```sql
IIR(expr, ARRAY[a0, a1, ...], ARRAY[b0, b1, ...])
```

**Description:**

Implements the transfer function `H(z) = B(z) / A(z)` where A and B are the coefficient arrays. This is the standard IIR filter — Butterworth, Chebyshev, and other filter designs from MATLAB/scipy translate directly.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The input signal |
| ARRAY[a...] | array literal of doubles | Denominator (feedback) coefficients |
| ARRAY[b...] | array literal of doubles | Numerator (feedforward) coefficients |

**Incremental Behavior:**

Maintains circular buffers for both input and output history. Continuous state — no discontinuities between messages. IEEE 754 arithmetic ensures reproducible results.

**Example:**

```sql
-- First-order low-pass IIR filter
CREATE MATERIALIZED VIEW filtered AS
  SELECT device_id,
         IIR(temperature, ARRAY[1.0, -0.5], ARRAY[0.5]) AS filtered_temp
  FROM sensors
  GROUP BY device_id
```

**Operator:** `InfiniteImpulseResponse`

---

### RESAMPLE

Constant-rate resampling. Normalizes irregular arrival times to a fixed interval.

**Syntax:**

```sql
RESAMPLE(expr, interval)
```

**Description:**

Converts an irregularly-sampled signal to a constant-rate output. Useful for aligning signals before further processing (e.g., before FIR/IIR filters that assume constant sample rate).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The input signal |
| interval | positive integer constant | Resampling interval (in timestamp units) |

**Incremental Behavior:**

Buffers input samples and emits output at fixed intervals using interpolation. State: last input sample + timing state.

**Example:**

```sql
CREATE MATERIALIZED VIEW resampled AS
  SELECT device_id,
         RESAMPLE(temperature, 100) AS temp_100hz
  FROM sensors
  GROUP BY device_id
```

**Operator:** `ResamplerConstant`

---

### PEAK_DETECT

Local maximum detection over a sliding window.

**Syntax:**

```sql
PEAK_DETECT(expr, window_size)
```

**Description:**

Detects local peaks (maxima) in the input signal. A peak is identified when a value is the maximum within a window of `window_size` surrounding samples.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| expr | column or expression | The input signal |
| window_size | positive integer constant | Window size for peak detection |

**Example:**

```sql
CREATE MATERIALIZED VIEW peaks AS
  SELECT device_id,
         PEAK_DETECT(vibration, 10) AS peak
  FROM sensors
  GROUP BY device_id
```

**Operator:** `PeakDetector`

---

## Math Functions

Scalar math functions operate element-wise on each message. They are stateless — no incremental behavior section needed. All accept a single argument and return a single value.

| Function | Description | RTBot Operator |
|----------|-------------|----------------|
| `ABS(expr)` | Absolute value | `Abs` |
| `FLOOR(expr)` | Round down to nearest integer | `Floor` |
| `CEIL(expr)` / `CEILING(expr)` | Round up to nearest integer | `Ceil` |
| `ROUND(expr)` | Round to nearest integer | `Round` |
| `LN(expr)` / `LOG(expr)` | Natural logarithm | `Log` |
| `LOG10(expr)` | Base-10 logarithm | `Log10` |
| `EXP(expr)` | Exponential (e^x) | `Exp` |
| `SIN(expr)` | Sine (radians) | `Sin` |
| `COS(expr)` | Cosine (radians) | `Cos` |
| `TAN(expr)` | Tangent (radians) | `Tan` |
| `SIGN(expr)` | Sign function: returns 1, -1, or 0 | `Sign` |
| `POWER(expr, n)` | Raise to constant power | `Power` |

**Example:**

```sql
CREATE MATERIALIZED VIEW derived AS
  SELECT instrument_id,
         ABS(price - MOVING_AVERAGE(price, 20)) AS deviation,
         LN(price) AS log_price,
         POWER(quantity, 2) AS qty_squared
  FROM trades
  GROUP BY instrument_id
```

**Notes:**
- `POWER` takes two arguments; the exponent must be a constant.
- When applied to constant arguments, math functions are folded at compile time (e.g., `LN(2.718)` becomes `~1.0` in the compiled graph).

---

## Expressions and Operators

### Arithmetic Operators

| Operator | Description | Compilation |
|----------|-------------|-------------|
| `a + b` | Addition | `Add` (scalar) or `Addition` (sync) |
| `a - b` | Subtraction | `Add(-b)` (scalar) or `Subtraction` (sync) |
| `a * b` | Multiplication | `Scale` (scalar) or `Multiplication` (sync) |
| `a / b` | Division | `Scale(1/b)` (scalar) or `Division` (sync) |

**Optimization:** When one operand is a constant, the compiler uses lightweight scalar operators (`Add`, `Scale`) instead of synchronized two-input operators. This reduces state and latency.

**Constant folding:** When both operands are constants, the expression is evaluated at compile time. `2 * 3 + 1` compiles to the constant `7`.

### Comparison Operators

| Operator | Description | Scalar Op | Sync Op |
|----------|-------------|-----------|---------|
| `>` | Greater than | `CompareGT` | `CompareSyncGT` |
| `<` | Less than | `CompareLT` | `CompareSyncLT` |
| `>=` | Greater or equal | `CompareGTE` | `CompareSyncGTE` |
| `<=` | Less or equal | `CompareLTE` | `CompareSyncLTE` |
| `=` | Equal (with tolerance) | `CompareEQ` | `CompareSyncEQ` |
| `!=` | Not equal | `CompareNEQ` | `CompareSyncNEQ` |

Scalar comparison is used when one side is a constant (e.g., `price > 100`). Sync comparison is used when both sides are stream endpoints (e.g., `price > MOVING_AVERAGE(price, 20)`).

### Logical Operators

| Operator | Description | RTBot Operator |
|----------|-------------|----------------|
| `AND` | Logical conjunction | `LogicalAnd` |
| `OR` | Logical disjunction | `LogicalOr` |
| `NOT` | Logical negation | Optimized: inverts comparison (e.g., `NOT x > c` → `CompareLTE(c)`) |

---

## CASE Expression

Conditional branching within expressions.

**Syntax:**

```sql
CASE
  WHEN condition1 THEN result1
  WHEN condition2 THEN result2
  ...
  [ELSE default_result]
END
```

**Description:**

Evaluates WHEN conditions in order. Returns the result for the first true condition. If no condition matches and ELSE is present, returns the default. Compiled to mutually-exclusive boolean gates feeding a `Multiplexer` operator.

**Example:**

```sql
CREATE MATERIALIZED VIEW categorized AS
  SELECT instrument_id,
         CASE
           WHEN price > 200 THEN 3
           WHEN price > 100 THEN 2
           ELSE 1
         END AS tier
  FROM trades
  GROUP BY instrument_id
```

**Operators:** `LogicalNand` (NOT) + `LogicalAnd` (exclusivity) + `Multiplexer` (selection)

---

## JOIN

Stream-to-table join for enrichment patterns.

**Syntax:**

```sql
SELECT t.col1, t.col2, ...
FROM stream t JOIN table w ON t.key = w.key
```

**Description:**

For each incoming stream message, checks whether the join key exists in the reference table. If the key is present, the message passes through. Compiled to a `KeyedVariable` (existence check) + `Demultiplexer` (gate).

**Example:**

```sql
-- Only pass through trades from accounts in the watchlist
CREATE MATERIALIZED VIEW watched_trades AS
  SELECT t.instrument_id, t.price, t.account_id
  FROM trades t JOIN watchlist w ON t.account_id = w.account_id
```

**Incremental Behavior:**

The table state is maintained via its changelog stream. The `KeyedVariable` operator receives updates from the table on a separate input port. When a stream message arrives, the operator checks its internal HashMap for the join key.

**Notes:**
- The compiled program has a two-port input: port 1 for the stream, port 2 for the table changelog.
- Currently supports existence-based JOIN (filter pattern). Column projection from the table side is planned.

---

## Execution Model

### Incremental Semantics

Every RTBot SQL query is **maintained**, not recomputed. The compiled operator graph processes each incoming message and updates internal state. The output reflects the current aggregate state, not a batch recomputation over all historical data.

**What "maintained" means:**

- A materialized view with `SUM(quantity)` does not re-sum all historical quantities on each message. It adds the new value to the running total.
- A `MOVING_AVERAGE(price, 20)` does not scan the last 20 messages from storage. It maintains a circular buffer of 20 values and updates the average in O(1).
- A `GROUP BY instrument_id` does not re-process all instruments. Only the sub-graph for the incoming message's instrument is activated.

**Cost model:**

| Operation | Per-message cost | State per key |
|-----------|-----------------|---------------|
| SUM, COUNT, AVG | O(1) | 1-2 doubles |
| MOVING_AVERAGE, MOVING_SUM | O(1) | N doubles (circular buffer) |
| STDDEV | O(1) | N doubles + running stats |
| FIR | O(N) | N doubles (N = number of coefficients) |
| IIR | O(N+M) | N+M doubles (N = numerator, M = denominator order) |
| WHERE (scalar) | O(1) | None |
| WHERE (sync) | O(1) | Sync buffer |

---

### Per-Key Isolation

GROUP BY creates independent sub-pipelines per key via the `KeyedPipeline` operator.

**How it works:**

1. KeyedPipeline maintains a `HashMap<key_value, sub_graph>`.
2. On first message for a new key, it clones the prototype sub-graph (the compiled aggregates/windows).
3. Each message is routed to its key's sub-graph exclusively.
4. Sub-graphs maintain independent state — SUM for key 1 is separate from SUM for key 2.
5. The key value is prepended to the output vector.

**Properties:**
- New keys are instantiated automatically. No pre-registration. No capacity planning.
- Memory scales with number of active keys, not total message volume.
- Each key's state can be serialized independently via `collect()`/`restore()`.

---

### Expression De-duplication

The compiler caches compiled expression results by canonical form. When the same expression appears multiple times in a query, it compiles to a single operator, and all references share that operator's output.

**Example:**

```sql
SELECT instrument_id,
       MOVING_AVERAGE(price, 20) AS mid,
       MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper,
       MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower
FROM trades
GROUP BY instrument_id
```

Despite appearing 3 times, `MOVING_AVERAGE(price, 20)` compiles to **one** `MovingAverage` operator. `STDDEV(price, 20)` appears 2 times but compiles to **one** `StandardDeviation` operator. `VectorExtract` for `price` also compiles once.

**Why this matters:** Without de-duplication, this query would have 3 moving average operators and 2 standard deviation operators — each maintaining its own circular buffer. With de-duplication, the memory footprint and computation cost are minimized.

De-duplication also applies between SELECT and HAVING: `COUNT(*)` used in both compiles to a single `CountNumber` operator.

---

### Determinism Guarantees

RTBot SQL provides **execution determinism**: the same input data produces identical results on any deployment, at any time, without requiring the same persisted metadata state.

**Conditions for identical results:**

1. Same input stream data (same messages, same event timestamps)
2. Same program (same operator graph)
3. Same platform (same IEEE 754 floating-point behavior)

That's it. No need for the same deployment, the same cluster configuration, or the same wall-clock time.

**Why RTBot is deterministic:**

| Property | Guarantee |
|----------|-----------|
| Timestamps | In the data (event time in message payload). No system assigns timestamps. |
| Processing order | Single-threaded per key, strict event-time order. |
| Aggregation order | Fixed by event-time ordering within each key. IEEE 754 produces identical results for identical operation sequences. |
| Clock dependency | None. No `mz_now()`, no wall-clock references. |
| State | All internal to operators, captured by `collect()`/`restore()`. |
| Reference data | Events in a stream, state in snapshots. No external mutable state. |

**What this means in practice:**

- **Audit reproducibility:** Feed the same transaction data into a fresh deployment → identical alerts. No need to replicate persisted metadata or timing decisions.
- **Cross-environment consistency:** Development, staging, and production produce identical results from identical data.
- **Compliance:** Regulators can independently verify that the system produces the claimed outputs from the claimed inputs.

This is a strictly stronger guarantee than **recovery determinism** (same persisted state → same results), which is what distributed streaming systems typically offer. RTBot doesn't need persisted reclocking decisions because timestamps are in the data — there is nothing to "decide."

---

## View Chaining

Materialized views can read from other materialized views, creating multi-level computation pipelines.

**Example:**

```sql
-- Level 1: raw aggregation
CREATE MATERIALIZED VIEW v1 AS
  SELECT instrument_id, SUM(quantity) AS vol
  FROM trades
  GROUP BY instrument_id;

-- Level 2: smooth the aggregate
CREATE MATERIALIZED VIEW v2 AS
  SELECT instrument_id, MOVING_AVERAGE(vol, 10) AS smooth_vol
  FROM v1
  GROUP BY instrument_id;

-- Level 3: trend detection
CREATE MATERIALIZED VIEW v3 AS
  SELECT instrument_id, MOVING_AVERAGE(smooth_vol, 5) AS trend
  FROM v2
  GROUP BY instrument_id
```

Each level produces its own output stream. Downstream views consume the output of upstream views as their input. The entire chain updates incrementally on each incoming trade message.

---

## Error Handling

The compiler collects all detectable errors before returning — it does not fail on the first error. The compilation result includes an `errors` array.

### Compile-Time Errors

| Category | Example | Error message |
|----------|---------|---------------|
| Unknown entity | `SELECT * FROM nonexistent` | `Unknown stream or view: 'nonexistent'` |
| Column not found | `SELECT foo FROM trades` | `Column 'foo' not found in 'trades'. Available: instrument_id, price, quantity, account_id` |
| Missing GROUP BY | `SELECT id, SUM(x) FROM s` | `Column 'id' must appear in GROUP BY clause or be used in an aggregate function` |
| Invalid function args | `MOVING_AVERAGE(price)` | `MOVING_AVERAGE requires 2 arguments: (expr, window_size)` |
| Unsupported syntax | `SELECT DISTINCT ...` | `DISTINCT is not supported` |
| Unbounded scan | `SELECT * FROM trades` | Stream has unbounded entries. Add LIMIT or time bounds. |
| ORDER BY without LIMIT | `ORDER BY col DESC` | Error: ORDER BY requires LIMIT |
| Aggregate in WHERE | `WHERE AVG(price) > 100` | Aggregate function in WHERE; use HAVING instead |
| Parse failure | `SELEC FROM WHERE` | Syntax error from libpg_query parser |

### Runtime Errors

| Category | Example | Error message |
|----------|---------|---------------|
| Dependency conflict | `DROP TABLE trades` | `Cannot drop 'trades': referenced by materialized views: price_view` |
| Duplicate entity | `CREATE TABLE trades ...` | `Stream 'trades' already exists` |
| Insert target missing | INSERT into dropped stream | `Stream does not exist` |

---

## Worked Example: Bollinger Bands

The canonical RTBot SQL example — a complete Bollinger Bands computation in 7 lines.

```sql
CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20)                          AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id
```

**What the compiler produces:**

1. **Outer pipeline:** `Input → KeyedPipeline(key_index=0, prototype) → Output`
2. **Prototype sub-graph (per instrument):**
   - `VectorExtract(index=1)` → price (compiled once, shared)
   - `MovingAverage(20)` → mid_band (compiled once, referenced 3 times)
   - `StandardDeviation(20)` → stddev (compiled once, referenced 2 times)
   - `Scale(2.0)` → 2 * stddev
   - `Addition` → mid_band + 2*stddev (upper)
   - `Subtraction` → mid_band - 2*stddev (lower)
   - `VectorCompose` → [price, mid_band, upper_band, lower_band]
3. **Field map:** `{instrument_id: 0, price: 1, mid_band: 2, upper_band: 3, lower_band: 4}`

**De-duplication:** 6 references to 2 functions compile to 2 operators. Zero wasted computation.

**Output:** one row per instrument, always current. Query with `SELECT * FROM bollinger`.

---

## Worked Example: Bollinger Band Alerts with HAVING

Alert only when price breaks through the upper band.

```sql
CREATE MATERIALIZED VIEW bollinger_alerts AS
  SELECT instrument_id, price,
         MOVING_AVERAGE(price, 20) AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
```

The HAVING condition uses a `CompareSyncGT` operator because both sides are stream endpoints (price and the upper band computation). The view only emits rows when the price exceeds the upper band — a streaming anomaly detector in 7 lines.

Two-sided variant (alert on either band):

```sql
CREATE MATERIALIZED VIEW price_alerts AS
  SELECT instrument_id, price
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
      OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)
```
