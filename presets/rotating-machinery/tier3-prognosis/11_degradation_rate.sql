-- =============================================================================
-- PRESET: degradation_rate
-- TIER:   3 — Fault Prognosis
-- PHASE:  Remaining Useful Life Estimation / Trend Velocity
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Section 3.3 uses Monotonicity (Spearman rank correlation with time)
-- as the key prognostic quality metric. A parameter with high
-- monotonicity shows a consistent upward trend suitable for
-- extrapolation to end of life.
--
-- The practical corollary: tracking the *rate* of change of the best
-- monotonic parameter (RMS) enables distinguishing between slow steady
-- wear (manageable) and accelerating failure propagation (urgent).
-- A sudden increase in degradation rate indicates the fault has
-- entered the propagation phase — characterised in the IMS dataset
-- by the sharp RMS rise in the final days of the experiment.
--
-- NOTEBOOK: tier3-prognosis/11_degradation_rate.ipynb
--   Validates the short/long crossover against the IMS dataset.
--   Shows the timing of rate and acceleration transitions relative
--   to the bearing end-of-life events documented by the IMS Center.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (SEGMENT): Per-burst RMS from the raw vibration signal.
-- Each burst produces exactly one clean RMS value.
--
-- Stage 2 (streaming): Short and long moving averages on the
-- burst-rate RMS. The crossover (short > long) indicates active
-- degradation; the acceleration (rate itself increasing) indicates
-- failure propagation.
--
-- BURST-UNIT WINDOW SIZES (at ~10 min burst intervals)
-- ----------------------------------------------------------------
--    6 bursts  ≈  1 hour  (short window)
--   72 bursts  ≈ 12 hours (long window)
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Per-burst RMS (event-scoped)
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW rms_burst AS
  SELECT device_id, channel_id,
         POWER(AVG(POWER(amplitude, 2)), 0.5) AS rms_value,
         COUNT(*) AS sample_count
  FROM vibration_raw
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 2: RMS at two time scales
-- ---------------------------------------------------------------------------
-- Short: 6 bursts ≈ 1 hour   → captures recent trends
-- Long:  72 bursts ≈ 12 hours → stable baseline
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW rms_dual_window AS
  SELECT device_id, channel_id,
         rms_value,
         MOVING_AVERAGE(rms_value, 6)  AS rms_short,
         MOVING_AVERAGE(rms_value, 72) AS rms_long
  FROM rms_burst;


-- ---------------------------------------------------------------------------
-- View 3: Degradation rate and acceleration
-- ---------------------------------------------------------------------------
-- Rate = short MA - long MA (positive = degrading)
-- Acceleration = rate of the rate (short - long on the rate itself)
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW degradation_rate AS
  SELECT device_id, channel_id,
         rms_value,
         rms_short,
         rms_long,

         -- ── Degradation rate = short MA − long MA ──────────────────
         rms_short - rms_long                      AS degradation_rate_value,

         -- ── Acceleration = rate of the rate ────────────────────────
         MOVING_AVERAGE(degradation_rate_value, 6)
           - MOVING_AVERAGE(degradation_rate_value, 72)
                                                   AS degradation_acceleration
  FROM rms_dual_window;


-- ---------------------------------------------------------------------------
-- View 4: Rapid degradation alert
-- ---------------------------------------------------------------------------
-- Fires when BOTH conditions hold:
--   1. Actively degrading (rate > 0: short MA above long MA)
--   2. Accelerating (acceleration > 0: rate itself increasing)
-- This combination identifies the failure propagation phase.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW rapid_degradation_alert AS
  SELECT device_id, channel_id,
         degradation_rate_value,
         degradation_acceleration
  FROM degradation_rate
  WHERE degradation_rate_value   > 0
    AND degradation_acceleration > 0;
