-- =============================================================================
-- PRESET: detectivity
-- TIER:   1 — Fault Detection
-- PHASE:  Condition Monitoring / Hjorth's Parameters Composite
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Hjorth's parameters (Activity, Mobility, Complexity) adapted for
-- vibration condition monitoring. The paper finds that individually
-- they have ambiguous trends, but Detectivity — introduced by
-- Cocconcelli et al. (Mech. Syst. Signal Process. 2022, 164, 108247)
-- and defined as:
--
--   Detect = Act_dB - Mob_dB + Com_dB
--
-- where each term is 10*log10(parameter / parameter_ref), offers
-- "the clearest signature of fault" with a significant monotonic
-- increase at fault onset (Figures 14c, 15c).
--
-- DEFINITIONS
-- ----------------------------------------------------------------
--   Activity   = Var(x)
--   Mobility   = sqrt(Var(x') / Var(x))
--   Complexity = sqrt(Var(x'') / Var(x')) / Mobility(x)
--
-- where x' and x'' are the first and second time-derivatives,
-- approximated by FIR differencing filters.
--
-- NOTEBOOK: tier1-detection/03_detectivity.ipynb
--   Validates against IMS dataset. Shows Detectivity trend over time,
--   baseline calibration window selection, and comparison with
--   individual Hjorth parameters.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (streaming): FIR differencing filters compute first and
-- second derivatives of the raw signal. These run OUTSIDE SEGMENT
-- because FIR is a streaming operator — it must process each sample.
-- The cross-burst contamination from FIR is negligible: only 1-2
-- samples at burst boundaries are affected (filter length = 2-3 taps).
--
-- Stage 2 (SEGMENT): Collapses each burst into per-burst variances
-- of x, x', x'', then computes Hjorth parameters and Detectivity.
--
-- Stage 3 (streaming): Tracks Detectivity trend over bursts and
-- alerts when it exceeds a dB threshold above baseline.
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
-- View 1: First and second differences (streaming, outside SEGMENT)
-- ---------------------------------------------------------------------------
-- FIR is a streaming operator: it processes each sample independently.
-- The derivative approximations are computed on the raw 20 kHz stream.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW hjorth_derivatives AS
  SELECT device_id,
         channel_id,
         amplitude,
         FIR(amplitude, ARRAY[1.0, -1.0])       AS dx,
         FIR(amplitude, ARRAY[1.0, -2.0, 1.0])  AS ddx
  FROM vibration_raw;


-- ---------------------------------------------------------------------------
-- View 2: Per-burst Hjorth parameters (event-scoped)
-- ---------------------------------------------------------------------------
-- Activity   = Var(x) = E[x^2] - E[x]^2
-- Mobility   = sqrt(Var(dx) / Var(x))
-- Complexity = sqrt(Var(ddx) / Var(dx)) / Mobility
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW hjorth_burst AS
  SELECT device_id,
         channel_id,

         -- Activity = Variance of x
         AVG(POWER(amplitude, 2)) - POWER(AVG(amplitude), 2)
           AS activity,

         -- Mobility = sqrt(Var(dx) / Var(x))
         POWER(
           (AVG(POWER(dx, 2)) - POWER(AVG(dx), 2))
           / (AVG(POWER(amplitude, 2)) - POWER(AVG(amplitude), 2)),
           0.5
         ) AS mobility,

         -- Complexity = sqrt(Var(ddx) / Var(dx)) / Mobility
         POWER(
           (AVG(POWER(ddx, 2)) - POWER(AVG(ddx), 2))
           / (AVG(POWER(dx, 2)) - POWER(AVG(dx), 2)),
           0.5
         )
         / POWER(
           (AVG(POWER(dx, 2)) - POWER(AVG(dx), 2))
           / (AVG(POWER(amplitude, 2)) - POWER(AVG(amplitude), 2)),
           0.5
         ) AS complexity,

         COUNT(*) AS sample_count

  FROM hjorth_derivatives
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 3: Detectivity (dB-normalised composite)
-- ---------------------------------------------------------------------------
-- Reference values from the long-term moving average (baseline period).
-- 10*log10(a/b) = 10 * (LN(a) - LN(b)) / LN(10)
-- LN(10) ≈ 2.302585
-- Baseline: 48 bursts ≈ 8 hours.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW detectivity AS
  SELECT device_id,
         channel_id,
         activity,
         mobility,
         complexity,
         (  10.0 * (LN(activity)   - LN(MOVING_AVERAGE(activity,   48))) / 2.302585
          - 10.0 * (LN(mobility)   - LN(MOVING_AVERAGE(mobility,   48))) / 2.302585
          + 10.0 * (LN(complexity) - LN(MOVING_AVERAGE(complexity, 48))) / 2.302585
         ) AS detectivity_score
  FROM hjorth_burst;


-- ---------------------------------------------------------------------------
-- View 4: Detectivity alert
-- ---------------------------------------------------------------------------
-- Paper shows detectivity rising from ~0 to 5–15 dB at fault onset.
-- Threshold of 3.0 dB provides early warning.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW detectivity_alert AS
  SELECT device_id,
         channel_id,
         detectivity_score
  FROM detectivity
  WHERE detectivity_score > 3.0;
