---
title: Programming Patterns
sidebar_position: 9
---

# Programming Patterns

## Pattern 1: Multi-signal stabilizer gate

Compute outputs only when multiple signals are simultaneously stable.

```sql
CREATE MATERIALIZED VIEW steady_state_metrics AS
SELECT device_id,
       AVG(temperature, 20) AS steady_temp,
       AVG(pressure, 20) AS steady_pressure,
       AVG(humidity, 20) AS steady_humidity
FROM sensors
WHERE STDDEV(temperature, 20) < 0.1
  AND STDDEV(pressure, 20) < 0.05
  AND STDDEV(humidity, 20) < 0.02
  AND STDDEV(flow_rate, 20) < 0.1
GROUP BY device_id
```

This creates a deterministic gate: unstable periods are dropped, stable periods emit.

## Pattern 2: Pairwise composition for separate streams

When signals arrive on separate streams, chain smaller views:

1. per-signal stats view
2. pairwise join view
3. composed equilibrium view

If timestamp grids differ, normalize sources first with `RESAMPLE` before joining.

## Pattern 3: Reference-data lookup

Use stream-to-table JOIN for watchlists/configuration:

```sql
CREATE TABLE watchlist (account_id DOUBLE PRIMARY KEY)

CREATE MATERIALIZED VIEW watched_trades AS
SELECT t.instrument_id, t.price, t.quantity, t.account_id
FROM trades t JOIN watchlist w ON t.account_id = w.account_id
```

Table updates become effective immediately in downstream filtering.

## Pattern 4: Composable view design

Prefer small composable stages:

- Stage A: derive signal metrics
- Stage B: smooth or normalize
- Stage C: apply thresholds/alerts

Guideline:
- Use `VIEW` for internal composition-only steps.
- Use `MATERIALIZED VIEW` when a stage needs independent querying/subscription.

This approach improves testability and operational clarity without sacrificing incremental behavior.
