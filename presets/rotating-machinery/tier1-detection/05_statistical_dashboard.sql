-- =============================================================================
-- PRESET: statistical_dashboard
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Comprehensive Overview
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- The paper tracks all basic statistical parameters over the 34-day
-- IMS experiment (Figures 4–7):
--
--   Mean, Variance, Std Dev : stable on healthy bearings, slight rise
--                             at end of life
--   RMS                     : clear fault indicator on bearings 3 & 4
--   Skewness                : oscillates around 0, not a reliable detector
--   Kurtosis                : strongest leading indicator (rises 3→70+)
--
-- This dashboard computes all six simultaneously, providing the first
-- screening layer. Any anomaly here triggers deeper Tier 2 analysis.
--
-- NOTEBOOK: tier1-detection/05_statistical_dashboard.ipynb
--   End-to-end walkthrough of the IMS dataset using this view.
--   Reproduces Figures 4–7 from the paper in streaming form.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Reads from the shared vibration_moments base VIEW and derives all
-- six basic statistical parameters using alias chaining:
--
--   variance = ex2 − mean²
--   std      = sqrt(variance)
--   rms      = sqrt(E[x²])
--   m3       = E[x³] − 3·E[x²]·E[x] + 2·E[x]³
--   skewness = m3 / variance^1.5
--   m4       = E[x⁴] − 4·E[x³]·E[x] + 6·E[x²]·E[x]² − 3·E[x]⁴
--   kurtosis = m4 / variance²
--
-- This makes each formula self-documenting: the statistical
-- relationships are visible directly in the SQL.
--
-- The WHERE sample_count > 1 clause filters sentinel rows.
--
-- DEPENDS ON: 00_vibration_moments.sql (vibration_moments VIEW)
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

CREATE MATERIALIZED VIEW statistical_dashboard AS
  SELECT device_id,
         channel_id,

         -- ── Mean (passthrough from base) ────────────────────────────
         mean_value,

         -- ── Variance = E[x²] − E[x]² ──────────────────────────────
         ex2 - POWER(mean_value, 2)               AS variance_value,

         -- ── Standard Deviation = sqrt(variance) ────────────────────
         POWER(variance_value, 0.5)               AS std_value,

         -- ── RMS = sqrt(E[x²]) ─────────────────────────────────────
         POWER(ex2, 0.5)                          AS rms_value,

         -- ── Third central moment ───────────────────────────────────
         --    m3 = E[x³] − 3·E[x²]·E[x] + 2·E[x]³
         ex3 - 3.0 * ex2 * mean_value
             + 2.0 * POWER(mean_value, 3)         AS m3,

         -- ── Skewness = m3 / variance^1.5 ──────────────────────────
         m3 / POWER(variance_value, 1.5)          AS skewness_value,

         -- ── Fourth central moment ──────────────────────────────────
         --    m4 = E[x⁴] − 4·E[x³]·E[x] + 6·E[x²]·E[x]² − 3·E[x]⁴
         ex4 - 4.0 * ex3 * mean_value
             + 6.0 * ex2 * POWER(mean_value, 2)
             - 3.0 * POWER(mean_value, 4)         AS m4,

         -- ── Kurtosis = m4 / variance² ─────────────────────────────
         m4 / POWER(variance_value, 2)            AS kurtosis_value,

         sample_count

  FROM vibration_moments
  WHERE sample_count > 1;
