-- =============================================================================
-- PRESET: vibration_moments (base view)
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Shared Aggregation Base
-- =============================================================================
--
-- PURPOSE
-- ----------------------------------------------------------------
-- This non-materialized VIEW performs the single GROUP BY aggregation
-- pass shared by all Tier 1 detection presets (01–05). Each raw
-- vibration burst is collapsed into statistical moments and summary
-- values that downstream MATERIALIZED VIEWs derive their indicators
-- from.
--
-- By centralising the aggregation here, we eliminate 4 redundant
-- GROUP BY passes (one per preset) and replace them with one pass
-- plus lightweight scalar derivations downstream.
--
-- ARCHITECTURE
-- ----------------------------------------------------------------
-- vibration_raw  ──►  vibration_moments (VIEW, not stored)
--                          ├──► rms_trend            (MATERIALIZED)
--                          ├──► kurtosis_trend       (MATERIALIZED)
--                          ├──► crest_clearance_...  (MATERIALIZED)
--                          └──► statistical_dashboard (MATERIALIZED)
--
-- As a plain VIEW, vibration_moments does NOT store its output.
-- It deploys an operator graph pipeline and propagates results to
-- all dependent materialized views, which do the storing.
--
-- COLUMNS PRODUCED
-- ----------------------------------------------------------------
--   device_id      — bearing identifier (passthrough)
--   channel_id     — sensor axis identifier (passthrough)
--   mean_value     — E[x]           (for variance, skewness, kurtosis)
--   ex2            — E[x²]          (for RMS, variance, kurtosis)
--   ex3            — E[x³]          (for skewness, kurtosis)
--   ex4            — E[x⁴]          (for kurtosis)
--   peak_value     — max(|x|)       (for crest, clearance, impulse)
--   mean_abs       — E[|x|]         (for shape, impulse factors)
--   mean_sqrt_abs  — E[sqrt(|x|)]   (for clearance factor)
--   sample_count   — N              (for sentinel filtering)
--
-- SEGMENT PATTERN
-- ----------------------------------------------------------------
-- The GROUP BY ABS(amplitude) > 0 pattern acts as a burst boundary.
-- While amplitude != 0, samples accumulate. When a sentinel row
-- (amplitude = 0) arrives, the boolean key changes, emitting the
-- accumulated aggregate. Per burst per bearing: exactly 2 emitted
-- rows (1 real data + 1 sentinel with sample_count=1).
--
-- Downstream views filter sentinels with WHERE sample_count > 1.
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

-- Stream declaration (provided by the deployment environment)
-- CREATE STREAM vibration_raw (
--     device_id  DOUBLE,
--     channel_id DOUBLE,
--     amplitude  DOUBLE
-- );

CREATE VIEW vibration_moments AS
  SELECT device_id,
         channel_id,

         -- ── First moment ────────────────────────────────────────────
         AVG(amplitude)                           AS mean_value,

         -- ── Raw moments (powers of x) ──────────────────────────────
         AVG(POWER(amplitude, 2))                 AS ex2,
         AVG(POWER(amplitude, 3))                 AS ex3,
         AVG(POWER(amplitude, 4))                 AS ex4,

         -- ── Peak and absolute-value statistics ─────────────────────
         MAX(ABS(amplitude))                      AS peak_value,
         AVG(ABS(amplitude))                      AS mean_abs,
         AVG(POWER(ABS(amplitude), 0.5))          AS mean_sqrt_abs,

         -- ── Sample count (for sentinel filtering) ──────────────────
         COUNT(*)                                  AS sample_count

  FROM vibration_raw
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;
