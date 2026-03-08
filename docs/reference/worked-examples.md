---
title: Worked Examples
sidebar_position: 8
---

# Worked Examples

## Bollinger bands

```sql
CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20) AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id
```

Key properties:
- incremental updates per message
- per-key isolation by `instrument_id`
- expression de-duplication for reused `MOVING_AVERAGE` and `STDDEV`

## Bollinger alerts with HAVING

```sql
CREATE MATERIALIZED VIEW bollinger_alerts AS
  SELECT instrument_id, price,
         MOVING_AVERAGE(price, 20) AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
```

Two-sided variant:

```sql
CREATE MATERIALIZED VIEW price_alerts AS
  SELECT instrument_id, price
  FROM trades
  GROUP BY instrument_id
  HAVING price > MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20)
      OR price < MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20)
```
