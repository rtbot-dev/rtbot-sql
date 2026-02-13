CREATE MATERIALIZED VIEW bollinger AS
  SELECT instrument_id,
         price,
         MOVING_AVERAGE(price, 20)                          AS mid_band,
         MOVING_AVERAGE(price, 20) + 2 * STDDEV(price, 20) AS upper_band,
         MOVING_AVERAGE(price, 20) - 2 * STDDEV(price, 20) AS lower_band
  FROM trades
  GROUP BY instrument_id
