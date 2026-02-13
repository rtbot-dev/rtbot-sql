-- Three-level view chain: trades -> v1 -> v2 -> v3

CREATE MATERIALIZED VIEW v1 AS
  SELECT instrument_id,
         SUM(quantity) AS vol
  FROM trades
  GROUP BY instrument_id;

-- Requires: v1
CREATE MATERIALIZED VIEW v2 AS
  SELECT instrument_id,
         MOVING_AVERAGE(vol, 10) AS smooth_vol
  FROM v1
  GROUP BY instrument_id;

-- Requires: v2
CREATE MATERIALIZED VIEW v3 AS
  SELECT instrument_id,
         MOVING_AVERAGE(smooth_vol, 5) AS trend
  FROM v2
  GROUP BY instrument_id
