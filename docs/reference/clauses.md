---
title: Clauses
sidebar_position: 3
---

# Clauses

## WHERE

Row-level filter before aggregation.

```sql
SELECT instrument_id, price
FROM trades
WHERE price > 100
LIMIT 100
```

- Supports comparisons, boolean logic, arithmetic expressions, and aliases.
- Aggregate functions are not allowed in `WHERE`; use `HAVING`.

## GROUP BY

Creates per-key isolated sub-pipelines via `KeyedPipeline`.

```sql
SELECT instrument_id,
       SUM(quantity) AS total_vol,
       COUNT(*) AS trade_count
FROM trades
GROUP BY instrument_id
```

- Each key has isolated state.
- New keys are instantiated automatically.

## HAVING

Post-aggregation filter evaluated against updated aggregate state.

```sql
SELECT instrument_id, AVG(price) AS avg_p
FROM trades
GROUP BY instrument_id
HAVING avg_p > 100
```

Common alerting form:

```sql
HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
```

## ORDER BY ... LIMIT

Ranks and returns top-k rows.

```sql
SELECT instrument_id, quantity
FROM trades
ORDER BY quantity DESC
LIMIT 3
```

`ORDER BY` without `LIMIT` is invalid.

## LIMIT

Caps result size for point-in-time reads.

```sql
SELECT * FROM instrument_stats LIMIT 100
```
