-- =============================================================================
-- PRESET: health_index
-- TIER:   3 — Fault Prognosis
-- PHASE:  Remaining Useful Life Estimation / Health Indicator Tracking
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Section 3.3 evaluates statistical parameters as health indicators
-- using three prognostic quality metrics:
--
--   Correlation  = Pearson(HI, time)         — linearity of trend
--   Monotonicity = Spearman rank(HI, time)   — consistent direction
--   Robustness   = mean(exp(-|HI - HI_smooth| / HI))  — noise resistance
--
-- Figure 49 shows that on the damaged bearings (3 and 4), the best
-- scoring parameters (highest average Corr + Mon + Rob) are:
--   1. RMS value            — top across all three metrics
--   2. Variance             — high Corr and Mon, moderate Rob
--   3. Standard Deviation   — nearly identical to Variance
--   4. Peak-to-Peak         — good Mon, moderate Corr
--
-- NOTEBOOK: tier3-prognosis/10_health_index.ipynb
--   Reproduces Figure 49. Validates the composite Health Index against
--   the IMS dataset, showing the 0-to-N trajectory from healthy to
--   end of life. Demonstrates how to calibrate the alert thresholds.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (SEGMENT): Collapses each burst into the four best
-- prognostic parameters — RMS, variance, standard deviation, and
-- peak-to-peak. All computed as exact per-burst values (no sliding
-- window bias). Peak is computed via MAX(ABS(amplitude)).
--
-- Stage 2 (streaming): Normalises each parameter against its
-- running baseline (48 bursts ≈ 8 hours) and combines into a
-- composite Health Index:
--
--   HI = (rms/rms_0 + var/var_0 + std/std_0 + p2p/p2p_0) / 4
--
-- Starts near 1.0 in healthy condition, rises monotonically with
-- degradation — exactly the property needed for RUL extrapolation.
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
-- View 1: Per-burst prognostic parameters (event-scoped)
-- ---------------------------------------------------------------------------
-- Uses alias chaining: ex2 → variance → std (fixes MOVING_STD bug)
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW prognostic_parameters AS
  SELECT device_id, channel_id,
         AVG(POWER(amplitude, 2))                              AS ex2,
         POWER(ex2, 0.5)                                       AS rms_value,
         ex2 - POWER(AVG(amplitude), 2)                        AS variance_value,
         POWER(variance_value, 0.5)                            AS std_value,
         MAX(ABS(amplitude)) * 2.0                             AS p2p_value,
         COUNT(*)                                              AS sample_count
  FROM vibration_raw
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 2: Composite Health Index
-- ---------------------------------------------------------------------------
-- Each parameter normalised against its 48-burst baseline, then averaged.
-- Baseline: 48 bursts ≈ 8 hours.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW health_index AS
  SELECT device_id, channel_id,
         rms_value,
         variance_value,
         std_value,
         p2p_value,

         -- ── Baselines (48-burst moving averages) ───────────────────
         MOVING_AVERAGE(rms_value,      48)        AS rms_base,
         MOVING_AVERAGE(variance_value, 48)        AS var_base,
         MOVING_AVERAGE(std_value,      48)        AS std_base,
         MOVING_AVERAGE(p2p_value,      48)        AS p2p_base,

         -- ── Normalised indicators ──────────────────────────────────
         rms_value      / rms_base                 AS rms_norm,
         variance_value / var_base                 AS var_norm,
         std_value      / std_base                 AS std_norm,
         p2p_value      / p2p_base                 AS p2p_norm,

         -- ── Composite Health Index ─────────────────────────────────
         (rms_norm + var_norm + std_norm + p2p_norm) / 4.0
                                                   AS health_index_value
  FROM prognostic_parameters;


-- ---------------------------------------------------------------------------
-- View 3: Health index alert — entering degradation phase
-- ---------------------------------------------------------------------------
-- HI > 1.5 means 50% above baseline on average across all four
-- parameters. This indicates the bearing has left the healthy regime.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW health_index_alert AS
  SELECT device_id, channel_id,
         health_index_value
  FROM health_index
  WHERE health_index_value > 1.5;
