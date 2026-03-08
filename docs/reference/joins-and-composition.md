---
title: Joins and Composition
sidebar_position: 5
---

# Joins and Composition

RtBot SQL provides three composition mechanisms: **stream-to-table JOIN**, **view chaining**, and **multi-source FROM**.

## Stream-to-table JOIN

JOIN enriches streaming data with reference table lookups:

```sql
CREATE TABLE watchlist (
  account_id DOUBLE PRECISION PRIMARY KEY,
  risk_score DOUBLE PRECISION
);

CREATE MATERIALIZED VIEW watched_trades AS
  SELECT t.instrument_id, t.price, t.account_id, w.risk_score
  FROM trades t JOIN watchlist w ON t.account_id = w.account_id
```

Under the hood, this compiles to a `KeyedVariable` for keyed lookup against table state and a `Demultiplexer` to gate pass/fail rows. Table updates take effect immediately in downstream filtering.

## View chaining

A materialized view can read from another materialized view's output. This creates a pipeline chain where data flows through multiple computation stages.

```sql
-- Level 1: per-instrument aggregation
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

When a trade is inserted, it propagates through all three levels automatically: `trades → instrument_stats → vol_trends → vol_smooth`.

### When to use view chaining

- **Progressive refinement** — compute raw stats, then smooth them, then detect anomalies
- **Separation of concerns** — each view handles one computation
- **Reusable intermediate results** — multiple downstream views can read from the same upstream view
- **Alerting pipelines** — chain a computation view with a filter view

```sql
-- Computation
CREATE MATERIALIZED VIEW price_bands AS
  SELECT instrument_id, price,
         MOVING_AVERAGE(price, 20) AS ma,
         MOVING_STD(price, 20) AS sd
  FROM trades
  GROUP BY instrument_id;

-- Alert (reads from computation)
CREATE MATERIALIZED VIEW breakout_alerts AS
  SELECT instrument_id, price, ma, sd
  FROM price_bands
  WHERE price > ma + 2 * sd;
```

## Multi-source FROM

When you need to combine data from multiple independent streams, use a multi-source FROM clause:

```sql
CREATE MATERIALIZED VIEW cross_stats AS
  SELECT b.price AS btc_price,
         e.price AS eth_price,
         MOVING_AVERAGE(b.price, 30) AS btc_ma,
         MOVING_AVERAGE(e.price, 30) AS eth_ma
  FROM btc_trades b, eth_trades e
```

### Automatic time alignment

RtBot synchronizes multi-source inputs by event time. When a message arrives on one source, RtBot waits until the other source has data at the same or later timestamp before processing. This ensures that computations always see temporally consistent data.

This is a major simplification compared to other streaming systems where you would need to configure:
- Watermarks
- Temporal join windows
- Buffer sizes
- Late arrival handling

In RtBot, time alignment is automatic and exact. No configuration required.

### Cross-stream correlation

Multi-source FROM is particularly useful for correlation analysis:

```sql
-- Compute per-stream statistics
CREATE MATERIALIZED VIEW cross_stats AS
  SELECT b.price AS btc_price,
         e.price AS eth_price,
         MOVING_AVERAGE(b.price, 60) AS btc_mean,
         MOVING_AVERAGE(e.price, 60) AS eth_mean,
         MOVING_STD(b.price, 60) AS btc_std,
         MOVING_STD(e.price, 60) AS eth_std
  FROM btc_trades b, eth_trades e;

-- Derive correlation from the stats
CREATE MATERIALIZED VIEW correlation AS
  SELECT btc_price, eth_price,
         MOVING_AVERAGE(btc_price * eth_price, 60) AS e_xy,
         btc_mean, eth_mean, btc_std, eth_std
  FROM cross_stats;
```

## VIEW vs MATERIALIZED VIEW for composition

Use `VIEW` (non-materialized) as an internal building block when the intermediate result does not need direct querying. Use `MATERIALIZED VIEW` when you need to query, subscribe to, or observe the stage's output independently.

## Choosing a composition pattern

| Pattern | Use case | Mechanism |
|---------|----------|-----------|
| Stream-to-table JOIN | Enriching stream data with reference lookups | JOIN clause with a table |
| View chaining | Progressive computation on the same data | One view reads from another view's output |
| Multi-source FROM | Combining independent data sources | Multiple streams in a single query, time-aligned |
| Combined | Cross-stream analysis with enrichment and refinement | All three patterns composed together |
