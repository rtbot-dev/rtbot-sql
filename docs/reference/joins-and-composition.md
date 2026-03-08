---
title: Joins and Composition
sidebar_position: 5
---

# Joins and Composition

## JOIN

Current JOIN behavior is stream-to-table enrichment/filtering.

```sql
CREATE MATERIALIZED VIEW watched_trades AS
  SELECT t.instrument_id, t.price, t.account_id
  FROM trades t JOIN watchlist w ON t.account_id = w.account_id
```

Implementation model:
- `KeyedVariable` for keyed lookup against table state
- `Demultiplexer` to gate pass/fail rows

## VIEW vs MATERIALIZED VIEW

- `VIEW`: compositional building block, stored graph, no persistent output stream.
- `MATERIALIZED VIEW`: externally queryable state with deployed incremental pipeline.

Use `VIEW` when intermediate output does not need direct querying; use `MATERIALIZED VIEW` for externally observed results.

## View chaining

Materialized views can consume upstream materialized views.

```sql
CREATE MATERIALIZED VIEW v1 AS
  SELECT instrument_id, SUM(quantity) AS vol
  FROM trades
  GROUP BY instrument_id;

CREATE MATERIALIZED VIEW v2 AS
  SELECT instrument_id, MOVING_AVERAGE(vol, 10) AS smooth_vol
  FROM v1
  GROUP BY instrument_id;
```

Each stage is incrementally maintained and readable.

## Composable materialized-view patterns

Recommended approach for complex pipelines:

1. Break logic into small, named units.
2. Materialize boundaries that need observability or reuse.
3. Keep each stage semantically narrow (aggregation, smoothing, thresholding, enrichment).
4. Verify each stage independently with `SELECT` before composing downstream.
