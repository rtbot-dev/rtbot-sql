-- Per-instrument aggregates from raw trades stream
CREATE MATERIALIZED VIEW instrument_stats AS
  SELECT instrument_id,
         SUM(quantity)  AS total_vol,
         COUNT(*)       AS trade_count
  FROM trades
  GROUP BY instrument_id
