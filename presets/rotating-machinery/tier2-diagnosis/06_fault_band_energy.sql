-- =============================================================================
-- PRESET: fault_band_energy
-- TIER:   2 — Fault Diagnosis
-- PHASE:  Fault Localisation / Characteristic Frequency Monitoring
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Each bearing sub-component produces a characteristic fault frequency:
--
--   BPFO = (n*fr/2)(1 - d/D*cos(phi))    outer race
--   BPFI = (n*fr/2)(1 + d/D*cos(phi))    inner race
--   BSF  = (fr*D/2d)(1-(d/D*cos(phi))^2) rolling element
--   FTF  = (fr/2)(1 - d/D*cos(phi))      cage
--
-- where n = number of rolling elements, fr = shaft rotation frequency,
-- d = ball diameter, D = pitch diameter, phi = contact angle.
--
-- IMS Rexnord ZA-2115 values (fr = 33.3 Hz at 2000 RPM):
--   BPFO = 236 Hz, BPFI = 297 Hz, BSF = 278 Hz, FTF = 15 Hz
--
-- The PSD analysis (Section 3.2.2, Figures 20–22) and STFT (Figures
-- 16–19) confirm harmonics of BPFI on bearing 3 and BPFO on bearing 4.
--
-- NOTEBOOK: tier2-diagnosis/06_fault_band_energy.ipynb
--   Shows how to design IIR coefficients for a specific bearing model
--   using scipy.signal. Validates band energy trends against the IMS
--   dataset and compares with the PSD figures from the paper.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (streaming): Four IIR band-pass filters isolate the
-- characteristic fault frequency bands. These run OUTSIDE SEGMENT
-- because IIR is a streaming operator. The cross-burst contamination
-- from IIR is negligible: the 10-minute gap between bursts far exceeds
-- any filter transient, so the filter state decays to zero between
-- bursts.
--
-- Stage 2 (SEGMENT): Collapses each burst into per-burst RMS energy
-- in each fault band. The SEGMENT condition uses the raw amplitude
-- (passed through from stage 1) to detect burst boundaries.
--
-- Stage 3 (streaming): Tracks band energy trends and alerts when any
-- band exceeds its baseline, identifying which component is degrading.
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
--
-- ⚠  PARAMETERISATION REQUIRED
-- ----------------------------------------------------------------
-- The IIR coefficients below target the IMS Rexnord ZA-2115 at
-- fs = 20480 Hz. For a different bearing or shaft speed, recompute
-- the coefficients from the bearing datasheet using the notebook.
--
-- BURST-UNIT WINDOW SIZES (at ~10 min burst intervals)
-- ----------------------------------------------------------------
--   48 bursts  ≈  8 hours (baseline window)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Band-pass filtering (streaming, outside SEGMENT)
-- ---------------------------------------------------------------------------
-- All four IIR filters in one view. The raw amplitude is passed through
-- for the SEGMENT condition in the next view.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fault_bands_filtered AS
  SELECT device_id, channel_id,
         amplitude,
         -- Outer race fault band (BPFO ≈ 236 Hz)
         IIR(amplitude,
             ARRAY[0.00723, 0.0, -0.00723],
             ARRAY[1.0, -1.98423, 0.98554]) AS bpfo_filtered,
         -- Inner race fault band (BPFI ≈ 297 Hz)
         IIR(amplitude,
             ARRAY[0.00908, 0.0, -0.00908],
             ARRAY[1.0, -1.97916, 0.98184]) AS bpfi_filtered,
         -- Ball spin frequency band (BSF ≈ 278 Hz)
         IIR(amplitude,
             ARRAY[0.00851, 0.0, -0.00851],
             ARRAY[1.0, -1.98069, 0.98299]) AS bsf_filtered,
         -- Cage frequency band (FTF ≈ 15 Hz)
         IIR(amplitude,
             ARRAY[0.00046, 0.0, -0.00046],
             ARRAY[1.0, -1.99954, 0.99908]) AS ftf_filtered
  FROM vibration_raw;


-- ---------------------------------------------------------------------------
-- View 2: Per-burst band energy (event-scoped)
-- ---------------------------------------------------------------------------
-- RMS of each filtered band within the burst. A rising trend in a
-- specific band directly identifies which bearing component is degrading.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fault_band_energy AS
  SELECT device_id, channel_id,
         POWER(AVG(POWER(bpfo_filtered, 2)), 0.5) AS bpfo_energy,
         POWER(AVG(POWER(bpfi_filtered, 2)), 0.5) AS bpfi_energy,
         POWER(AVG(POWER(bsf_filtered,  2)), 0.5) AS bsf_energy,
         POWER(AVG(POWER(ftf_filtered,  2)), 0.5) AS ftf_energy,
         COUNT(*) AS sample_count
  FROM fault_bands_filtered
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 3: Fault band alert
-- ---------------------------------------------------------------------------
-- Baseline: 48 bursts ≈ 8 hours. Alert at 3x baseline in any band.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW fault_band_alert AS
  SELECT device_id, channel_id,
         bpfo_energy, bpfi_energy, bsf_energy, ftf_energy
  FROM fault_band_energy
  WHERE bpfo_energy > MOVING_AVERAGE(bpfo_energy, 48) * 3.0
     OR bpfi_energy > MOVING_AVERAGE(bpfi_energy, 48) * 3.0
     OR bsf_energy  > MOVING_AVERAGE(bsf_energy,  48) * 3.0
     OR ftf_energy  > MOVING_AVERAGE(ftf_energy,  48) * 3.0;
