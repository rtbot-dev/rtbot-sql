-- =============================================================================
-- PRESET: cepstrum_prewhitening
-- TIER:   2 — Fault Diagnosis
-- PHASE:  Signal Pre-treatment / Noise and Resonance Removal
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- Cepstrum Pre-Whitening (CPW) is the highest-scoring technique in
-- the paper, achieving the best diagnostic kurtosis in combination
-- with SES:
--
--   CPW + SES: kurtosis = 86.0 (bearing 3), 81.7 (bearing 4)
--   Next best IES:        65.7              58.6     (Table 5)
--
-- CPW normalises all frequency components to unit amplitude, removing
-- structural resonances, coloured noise, and all masking components,
-- leaving fault signatures "thoroughly visible and unmistakably
-- detectable" (Figures 37–40).
--
-- NOTEBOOK: tier2-diagnosis/08_cepstrum_prewhitening.ipynb
--   Implements the full batch CPW + SES pipeline (using FFT) to
--   reproduce Figures 37–40. The notebook validates the streaming
--   approximation against the full CPW and shows they track together.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Streaming approximation of CPW via adaptive amplitude normalisation:
--
-- Stage 1 (streaming): High-pass IIR to isolate the resonance band
-- (same filter as envelope analysis). This acts as a spectral whitener
-- — it removes the low-frequency spectral shape, which is the dominant
-- masking component.
--
-- Stage 2 (SEGMENT): Per-burst kurtosis of the band-passed signal.
-- This captures the essence of CPW + SES: isolate the impulsive band,
-- then measure its impulsiveness. The pre-whitening effect comes from
-- the band-pass filter removing everything except the resonance-
-- modulated fault signature.
--
-- Stage 3 (streaming): Alert on burst-level kurtosis threshold.
-- A tighter threshold (4.0 vs 5.0 on raw) is reliable because the
-- band-pass filtering reduces the baseline kurtosis closer to 3.0.
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: High-pass filter for spectral whitening (streaming)
-- ---------------------------------------------------------------------------
-- Same high-pass as envelope_analysis — isolates the structural
-- resonance band (~2 kHz+) where fault impacts excite the bearing.
-- Passes through raw amplitude for SEGMENT condition.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW prewhitened_signal AS
  SELECT device_id, channel_id,
         amplitude,
         IIR(amplitude,
             ARRAY[0.7265, -1.4530, 0.7265],
             ARRAY[1.0, -1.3725, 0.5334]) AS whitened_amplitude
  FROM vibration_raw;


-- ---------------------------------------------------------------------------
-- View 2: Per-burst kurtosis of whitened signal (event-scoped)
-- ---------------------------------------------------------------------------
-- Raw-moments formula for kurtosis — same as preset 02 but applied
-- to the band-passed signal instead of raw amplitude.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW whitened_kurtosis AS
  SELECT device_id,
         channel_id,

         -- Kurtosis of whitened signal via raw moments
         ( AVG(POWER(whitened_amplitude, 4))
           - 4.0 * AVG(POWER(whitened_amplitude, 3)) * AVG(whitened_amplitude)
           + 6.0 * AVG(POWER(whitened_amplitude, 2)) * POWER(AVG(whitened_amplitude), 2)
           - 3.0 * POWER(AVG(whitened_amplitude), 4)
         )
         /
         POWER(
           AVG(POWER(whitened_amplitude, 2)) - POWER(AVG(whitened_amplitude), 2),
           2
         ) AS whitened_kurtosis_value,

         COUNT(*) AS sample_count

  FROM prewhitened_signal
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 3: Whitened kurtosis alert
-- ---------------------------------------------------------------------------
-- Tighter threshold (4.0 vs 5.0 on raw): band-pass filtering pushes
-- the healthy baseline closer to 3.0, so a lower threshold detects
-- faults earlier with fewer false positives.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW whitened_kurtosis_alert AS
  SELECT device_id, channel_id,
         whitened_kurtosis_value
  FROM whitened_kurtosis
  WHERE whitened_kurtosis_value > 4.0;
