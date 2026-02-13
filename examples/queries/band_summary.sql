-- Re-project from bollinger view (scalar projection, no GROUP BY)
-- Requires: bollinger
CREATE MATERIALIZED VIEW band_summary AS
  SELECT instrument_id, mid
  FROM bollinger
