-- =============================================================================
-- PRESET: envelope_analysis
-- TIER:   2 — Fault Diagnosis
-- PHASE:  Fault Localisation / Envelope Demodulation
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- The Squared Envelope Spectrum (SES) is "a benchmark among the
-- diagnostic techniques" (Section 3.2.3). The process: high-pass filter
-- the signal to isolate the structural resonance carrier band, square
-- it to emphasise modulation, extract the modulation envelope via
-- low-pass filtering, then analyse the envelope spectrum.
--
-- SES reveals fault frequencies as spectral peaks: BPFI harmonics
-- modulated by shaft frequency on bearing 3 (Figure 24), and BPFO
-- harmonics with BSF sidebands on bearing 4 (Figure 25).
--
-- NOTEBOOK: tier2-diagnosis/07_envelope_analysis.ipynb
--   Derives the streaming approximation from first principles. Compares
--   the time-domain envelope energy trend against the SES kurtosis
--   values from Table 5 of the paper. Shows how to interpret rising
--   envelope energy as a real-time SES proxy.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (streaming): Signal conditioning chain — high-pass IIR to
-- isolate resonance band, squaring for instantaneous power, low-pass
-- IIR to extract modulation envelope. These are streaming operators
-- that run OUTSIDE SEGMENT.
--
-- Stage 2 (SEGMENT): Collapses each burst into per-burst envelope
-- RMS — a single number representing the modulation energy in the
-- resonance band for that burst.
--
-- Stage 3 (streaming): Tracks envelope energy trend and alerts when
-- it exceeds baseline, indicating fault-induced modulation.
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
-- View 1: Envelope extraction chain (streaming, outside SEGMENT)
-- ---------------------------------------------------------------------------
-- Step 1: High-pass IIR to isolate structural resonance band (~2 kHz+)
-- Step 2: Square to get instantaneous power (squared envelope)
-- Step 3: Low-pass IIR to extract modulation at fault frequencies (<500 Hz)
-- The raw amplitude is passed through for SEGMENT condition.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW envelope_chain AS
  SELECT device_id, channel_id,
         amplitude,
         IIR(
           POWER(
             IIR(amplitude,
                 ARRAY[0.7265, -1.4530, 0.7265],
                 ARRAY[1.0, -1.3725, 0.5334]),
             2
           ),
           ARRAY[0.0009, 0.0019, 0.0009],
           ARRAY[1.0, -1.9112, 0.9150]
         ) AS envelope_value
  FROM vibration_raw;


-- ---------------------------------------------------------------------------
-- View 2: Per-burst envelope energy (event-scoped)
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW envelope_energy AS
  SELECT device_id, channel_id,
         POWER(AVG(POWER(envelope_value, 2)), 0.5) AS envelope_rms,
         COUNT(*) AS sample_count
  FROM envelope_chain
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;


-- ---------------------------------------------------------------------------
-- View 3: Envelope energy alert
-- ---------------------------------------------------------------------------
-- Baseline: 48 bursts ≈ 8 hours. Alert at 3x baseline.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW envelope_energy_alert AS
  SELECT device_id, channel_id,
         envelope_rms
  FROM envelope_energy
  WHERE envelope_rms > MOVING_AVERAGE(envelope_rms, 48) * 3.0;
