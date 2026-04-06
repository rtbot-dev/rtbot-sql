-- =============================================================================
-- PRESET: crest_clearance_impulse
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Advanced Statistical Parameters
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- The paper evaluates a second group of statistical parameters:
--
--   Crest Factor     = Peak / RMS
--   Clearance Factor = Peak / (mean(sqrt(|x|)))^2
--   Shape Factor     = RMS / mean(|x|)
--   Impulse Factor   = Peak / mean(|x|)
--   Peak-to-Peak     = 2 * Peak
--
-- While unhelpful on healthy bearings 1 and 2, on faulted bearings 3
-- and 4 Clearance Factor and Impulse Factor give "consistent
-- information" (Figures 10, 11). These parameters capture the
-- heavy-tailed shape change caused by fault-induced impulsive peaks.
--
-- NOTEBOOK: tier1-detection/04_crest_clearance_impulse.ipynb
--   Validates against IMS dataset. Compares Clearance and Impulse
--   Factor trends on healthy vs. faulted bearings.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Reads from the shared vibration_moments base VIEW, which performs
-- the single GROUP BY aggregation pass. This view derives all five
-- advanced parameters from the pre-computed summary statistics:
--
--   RMS    = sqrt(E[x²])           = POWER(ex2, 0.5)
--   Peak   = max(|x|)              = peak_value (from base)
--   Crest  = Peak / RMS
--   Clear  = Peak / (E[sqrt(|x|)])²
--   Shape  = RMS / E[|x|]
--   Impulse= Peak / E[|x|]
--   P2P    = 2 * Peak
--
-- The WHERE sample_count > 1 clause filters sentinel rows.
--
-- DEPENDS ON: 00_vibration_moments.sql (vibration_moments VIEW)
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
--
-- BURST-UNIT WINDOW SIZES (at ~10 min burst intervals)
-- ----------------------------------------------------------------
--   48 bursts  ≈  8 hours (baseline window)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Advanced statistical parameters (derived from shared moments)
-- ---------------------------------------------------------------------------
-- All five parameters computed from pre-aggregated values.
-- No intermediate materialized view needed — the base VIEW provides
-- peak_value, ex2, mean_abs, mean_sqrt_abs directly.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW crest_clearance_impulse AS
  SELECT device_id,
         channel_id,
         POWER(ex2, 0.5)                         AS rms_value,
         peak_value / POWER(ex2, 0.5)             AS crest_factor,
         peak_value / POWER(mean_sqrt_abs, 2)     AS clearance_factor,
         POWER(ex2, 0.5) / mean_abs               AS shape_factor,
         peak_value / mean_abs                     AS impulse_factor,
         peak_value * 2.0                          AS peak_to_peak,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1;


-- ---------------------------------------------------------------------------
-- View 2: Clearance Factor anomaly alert
-- ---------------------------------------------------------------------------
-- Baseline: 48 bursts ≈ 8 hours. Alert at 2x baseline.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW clearance_factor_alert AS
  SELECT device_id,
         channel_id,
         clearance_factor,
         MOVING_AVERAGE(clearance_factor, 48) AS baseline_cf
  FROM crest_clearance_impulse
  WHERE clearance_factor > MOVING_AVERAGE(clearance_factor, 48) * 2.0;
