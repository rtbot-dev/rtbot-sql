---
title: Statements
sidebar_position: 2
---

# Statements

## CREATE TABLE (stream)

Declares an input stream schema.

```sql
CREATE TABLE trades (
  instrument_id DOUBLE,
  price DOUBLE,
  quantity DOUBLE,
  account_id DOUBLE
)
```

## CREATE TABLE with PRIMARY KEY (reference table)

Declares mutable lookup state used in joins.

```sql
CREATE TABLE watchlist (
  account_id DOUBLE PRIMARY KEY,
  risk_score DOUBLE
)
```

## CREATE MATERIALIZED VIEW

Compiles and deploys a continuously maintained pipeline.

```sql
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity) AS total_vol,
         COUNT(*) AS trade_count
  FROM trades
  GROUP BY instrument_id
```

## CREATE VIEW

Stores a reusable query graph without deploying a persistent output pipeline.

```sql
CREATE VIEW live_stats AS
  SELECT instrument_id, SUM(quantity) AS vol
  FROM trades
  GROUP BY instrument_id
```

## INSERT INTO

Writes data into a stream (mostly for testing/dev).

```sql
INSERT INTO trades VALUES (1, 150.0, 200, 42)
```

## SELECT

Point-in-time read over streams, views, or tables.

```sql
SELECT instrument_id, price
FROM trades
WHERE price > 100
LIMIT 10
```

Notes:
- `SELECT * FROM stream` without `LIMIT` is invalid (unbounded).
- `ORDER BY` requires `LIMIT`.

## DROP MATERIALIZED VIEW

Removes deployed pipeline and catalog entry.

```sql
DROP MATERIALIZED VIEW bollinger
```

## DELETE FROM

Deletes a reference-table row by key.

```sql
DELETE FROM watchlist WHERE account_id = 42
```

## SUBSCRIBE

Planned live-update syntax:

```sql
SUBSCRIBE TO view_name
```
