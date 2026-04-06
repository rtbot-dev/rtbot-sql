-- =============================================================================
-- PRESET: kurtosis_trend
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Impulsiveness Detection
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Kurtosis is defined as E[(x-mu)^4] / Var(x)^2.
-- A Gaussian distribution has kurtosis = 3. The paper shows kurtosis
-- is the single most sensitive early-warning indicator: on bearing 3
-- it rises from ~3 to over 70 (Figure 6d) and on bearing 4 it spikes
-- to over 80 (Figure 7d). It also serves as the quantitative accuracy
-- metric benchmarking all diagnostic techniques (Table 5).
--
-- Kurtosis detects the *impulsiveness* that characterises early-stage
-- spalling: rolling element impacts on a defect pit produce sharp
-- transient peaks. Crucially, kurtosis rises before RMS does, making
-- it a leading indicator.
--
-- NOTEBOOK: tier1-detection/02_kurtosis_trend.ipynb
--   Validates this query against the IMS dataset. Demonstrates the
--   early-warning lead time advantage of kurtosis over RMS.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Reads from the shared vibration_moments base VIEW, which performs
-- the single GROUP BY aggregation pass. This view derives kurtosis
-- from the pre-computed raw moments using alias chaining:
--
--   variance = ex2 − mean²
--   m4       = ex4 − 4·ex3·mean + 6·ex2·mean² − 3·mean⁴
--   kurtosis = m4 / variance²
--
-- The WHERE sample_count > 1 clause filters sentinel rows.
--
-- DEPENDS ON: 00_vibration_moments.sql (vibration_moments VIEW)
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Per-burst kurtosis (derived from shared moments)
-- ---------------------------------------------------------------------------
-- Alias chaining: mean_value/ex2/ex3/ex4 → variance → m4 → kurtosis
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW kurtosis_trend AS
  SELECT device_id,
         channel_id,

         -- ── Variance = E[x²] − E[x]² ──────────────────────────────
         ex2 - POWER(mean_value, 2)               AS variance_value,

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


-- ---------------------------------------------------------------------------
-- View 2: Kurtosis spike alert
-- ---------------------------------------------------------------------------
-- Threshold of 5.0: Gaussian = 3.0, early fault onset ≈ 4–6.
-- Paper shows bearing 3 reaching kurtosis > 70 at fault.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW kurtosis_alert AS
  SELECT device_id,
         channel_id,
         kurtosis_value
  FROM kurtosis_trend
  WHERE kurtosis_value > 5.0;
