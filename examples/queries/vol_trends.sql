-- Moving average on upstream view (instrument_stats)
-- Requires: instrument_stats
CREATE MATERIALIZED VIEW vol_trends AS
  SELECT instrument_id,
         MOVING_AVERAGE(total_vol, 5) AS avg_vol
  FROM instrument_stats
  GROUP BY instrument_id
