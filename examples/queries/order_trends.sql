-- Two-level chain from table source: orders -> order_vol -> order_trends
-- Requires: orders table in catalog

CREATE MATERIALIZED VIEW order_vol AS
  SELECT instrument_id,
         SUM(qty) AS total_qty
  FROM orders
  GROUP BY instrument_id;

-- Requires: order_vol
CREATE MATERIALIZED VIEW order_trends AS
  SELECT instrument_id,
         MOVING_AVERAGE(total_qty, 10) AS avg_qty
  FROM order_vol
  GROUP BY instrument_id
