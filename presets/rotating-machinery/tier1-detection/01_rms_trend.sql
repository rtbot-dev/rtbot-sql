-- =============================================================================
-- PRESET: rms_trend
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Early Warning
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- RMS (Root Mean Squared value) is defined as sqrt(sum(x^2)/N).
-- The paper demonstrates that RMS is one of the most informative
-- statistical parameters for fault detection: on bearings 3 and 4 of
-- the IMS dataset, RMS rises sharply from a baseline of ~0.12 to >0.5
-- at fault onset (Figures 6b, 7b), while remaining flat on healthy
-- bearings 1 and 2. RMS also scores highest on the prognostic metrics
-- of Correlation, Monotonicity and Robustness (Figure 49), making it
-- the single best all-round health indicator.
--
-- NOTEBOOK: tier1-detection/01_rms_trend.ipynb
--   Validates this query against the IMS dataset. Shows the RMS trend
--   plot over the 34-day experiment and the alert threshold calibration.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Reads from the shared vibration_moments base VIEW, which performs
-- the single GROUP BY aggregation pass. This view derives RMS from
-- the pre-computed E[x²] moment:
--
--   RMS = sqrt(E[x²]) = POWER(ex2, 0.5)
--
-- The WHERE sample_count > 1 clause filters sentinel rows (the
-- segment boundary markers with sample_count=1).
--
-- The alert view fires when the current RMS exceeds a configurable
-- multiple of the baseline RMS (default: 2.5x), consistent with the
-- paper's observation that fault onset corresponds to a 2–4x increase.
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
--  144 bursts  ≈  1 day
-- 1008 bursts  ≈  1 week
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Per-burst RMS (derived from shared moments)
-- ---------------------------------------------------------------------------
-- RMS = sqrt(E[x²]) = POWER(ex2, 0.5)
-- Sentinel rows (sample_count=1) are filtered out at this level.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW rms_trend AS
  SELECT device_id,
         channel_id,
         POWER(ex2, 0.5) AS rms_value,
         sample_count
  FROM vibration_moments
  WHERE sample_count > 1;


-- ---------------------------------------------------------------------------
-- View 2: RMS anomaly alert
-- ---------------------------------------------------------------------------
-- Fires when current burst RMS exceeds baseline * 2.5.
-- Baseline is the moving average over 48 bursts ≈ 8 hours.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW rms_alert AS
  SELECT device_id,
         channel_id,
         rms_value,
         MOVING_AVERAGE(rms_value, 48) AS baseline_rms
  FROM rms_trend
  WHERE rms_value > MOVING_AVERAGE(rms_value, 48) * 2.5;
