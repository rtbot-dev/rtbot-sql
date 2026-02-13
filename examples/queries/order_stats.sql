-- Aggregate from a table source (not a stream)
-- Tables are backed by KeyedVariable at runtime
CREATE MATERIALIZED VIEW order_stats AS
  SELECT instrument_id,
         SUM(qty)  AS total_qty,
         COUNT(*)  AS cnt
  FROM orders
  GROUP BY instrument_id
