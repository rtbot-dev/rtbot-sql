-- Non-materialized view (ephemeral, re-evaluated on each query)
CREATE VIEW live_stats AS
  SELECT instrument_id,
         SUM(quantity) AS vol,
         COUNT(*)      AS cnt
  FROM trades
  GROUP BY instrument_id
