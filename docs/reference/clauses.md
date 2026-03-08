---
title: Clauses
sidebar_position: 3
---

# Clauses

RtBot SQL supports the standard SQL clauses in SELECT statements. Each clause maps to specific behavior in the compiled operator graph.

## SELECT list

The SELECT list defines the output columns. It can contain column references, expressions, function calls, and aliases.

```sql
SELECT price,
       quantity * price AS notional,
       MOVING_AVERAGE(price, 20) AS ma_price
FROM trades
```

**Column references** map to specific positions in the source stream's schema. The compiler resolves column names against the catalog.

**Expressions** use standard arithmetic: `+`, `-`, `*`, `/`. These compile to arithmetic operators in the graph.

**Function calls** invoke streaming functions (see [Functions](/docs/reference/rtbot-sql/functions)). Each function compiles to one or more RtBot operators.

**Aliases** (`AS name`) define the output column names. These appear in the `field_map` of the compilation result and as column headers in query output.

### Star expansion

`SELECT *` expands to all columns of the source:

```sql
SELECT * FROM trades
-- equivalent to: SELECT instrument_id, price, quantity FROM trades
```

When selecting from a materialized view, `*` expands to the view's output columns, not the original stream columns.

## FROM

The FROM clause specifies the data source. It can be a stream, a materialized view, a plain view, or a table.

```sql
-- From a stream
SELECT price FROM trades

-- From a materialized view
SELECT ma_price FROM bollinger

-- From a table
SELECT max_price FROM thresholds
```

### Multi-source FROM

RtBot SQL supports reading from multiple sources for cross-stream joins:

```sql
SELECT b.price AS btc_price,
       e.price AS eth_price,
       MOVING_AVERAGE(b.price, 30) AS btc_ma,
       MOVING_AVERAGE(e.price, 30) AS eth_ma
FROM btc_trades b, eth_trades e
```

When multiple sources are specified, RtBot synchronizes them by timestamp. Messages from different streams are aligned by event time automatically — no watermark configuration or temporal join setup required. This is a key differentiator from other streaming systems.

## WHERE

The WHERE clause filters rows based on a predicate. Only rows satisfying the condition appear in the output.

```sql
SELECT instrument_id, price, ma_price
FROM bollinger
WHERE price > upper_band
```

WHERE supports standard comparison operators (`>`, `<`, `>=`, `<=`, `=`, `!=`), arithmetic expressions, and logical operators (`AND`, `OR`, `NOT`).

**Important:** WHERE can reference non-aggregate columns and expressions involving aggregate results. The filter is applied after all computations in the SELECT list are evaluated.

```sql
-- Filter on a computed value
SELECT price,
       MOVING_AVERAGE(price, 20) AS ma,
       MOVING_STD(price, 20) AS sd
FROM trades
WHERE ABS(price - MOVING_AVERAGE(price, 20)) > 2 * MOVING_STD(price, 20)
```

## GROUP BY

GROUP BY partitions the input by one or more key columns. Each unique key value gets an independent pipeline instance with its own state.

```sql
SELECT instrument_id,
       MOVING_AVERAGE(price, 20) AS ma
FROM trades
GROUP BY instrument_id
```

Under the hood, GROUP BY compiles to a `KeyedPipeline` operator. The key column is extracted from each incoming message and used to route it to the correct pipeline instance.

**Key properties:**
- Each key's state is fully isolated — one key's moving average doesn't affect another's
- New keys are created automatically when first seen
- The key column must appear in the SELECT list
- Multiple GROUP BY columns are supported

## HAVING

HAVING filters after aggregation, similar to WHERE but applied to aggregate results. It is typically used with GROUP BY.

```sql
SELECT instrument_id,
       COUNT(*) AS trade_count,
       SUM(quantity) AS total_volume
FROM trades
GROUP BY instrument_id
HAVING COUNT(*) > 100
```

HAVING can reference aggregate functions directly. Rows that don't satisfy the HAVING condition are suppressed from the output.

Common alerting pattern:

```sql
SELECT instrument_id, price,
       MOVING_AVERAGE(price, 20) AS mid_band,
       MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20) AS upper_band
FROM trades
GROUP BY instrument_id
HAVING price > MOVING_AVERAGE(price, 20) + 2 * MOVING_STD(price, 20)
```

## LIMIT

LIMIT caps the number of rows returned by a SELECT query.

```sql
SELECT * FROM bollinger LIMIT 50
```

LIMIT affects query output only. It does not affect the behavior of materialized views or their pipelines.

For materialized views with GROUP BY, LIMIT applies to the total output rows across all keys.

## ORDER BY

ORDER BY sorts results. It requires LIMIT to prevent unbounded sorting.

```sql
SELECT instrument_id, quantity
FROM trades
ORDER BY quantity DESC
LIMIT 10
```
