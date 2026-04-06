-- =============================================================================
-- PRESET: spectral_kurtosis_bands
-- TIER:   2 — Fault Diagnosis
-- PHASE:  Fault Localisation / Impulsive Band Detection (Kurtogram)
-- =============================================================================
--
-- SCIENTIFIC BASIS (Sacerdoti et al., Appl. Sci. 2023, 13, 5977)
-- ----------------------------------------------------------------
-- The Kurtogram (Section 3.2.7, Figure 32) evaluates kurtosis across
-- multiple frequency bands and window lengths to identify the most
-- impulsive band — the resonance excited by fault impacts.
--
-- IMS results:
--   Bearing 3: peak at ~4.997 kHz, bandwidth 0.426 kHz (Figure 32a)
--   Bearing 4: peak at ~2.974 kHz, bandwidth 0.002 kHz (Figure 32b)
--
-- The paper notes the Kurtogram-selected band does not always dominate
-- fault content, making multi-band monitoring useful for robustness.
--
-- NOTEBOOK: tier2-diagnosis/09_spectral_kurtosis_bands.ipynb
--   Reproduces the Kurtogram of Figure 32 in batch. Validates the
--   four-band streaming version against it and shows how to interpret
--   which band is most active in real time.
--
-- WHAT THIS PRESET DOES
-- ----------------------------------------------------------------
-- Stage 1 (streaming): Four IIR band-pass filters divide the spectrum
-- into frequency bands covering fault fundamentals through structural
-- resonances. These run OUTSIDE SEGMENT.
--
-- Stage 2 (SEGMENT): Per-burst kurtosis in each band using the
-- raw-moments formula. The band with the highest kurtosis identifies
-- the most impulsive frequency range — the one excited by fault impacts.
--
-- INPUT STREAM SCHEMA
-- ----------------------------------------------------------------
--   vibration_raw(device_id, channel_id, amplitude)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- View 1: Four-band filtering (streaming, outside SEGMENT)
-- ---------------------------------------------------------------------------
-- All four IIR band-pass filters in one view. Raw amplitude passed
-- through for SEGMENT condition.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW spectral_bands_filtered AS
  SELECT device_id, channel_id,
         amplitude,
         -- Band 1: 0–1.28 kHz (fault fundamentals)
         IIR(amplitude,
             ARRAY[0.0144, 0.0288, 0.0144],
             ARRAY[1.0, -1.6330, 0.6906]) AS band_low,
         -- Band 2: 1.28–3.5 kHz (fault harmonics)
         IIR(amplitude,
             ARRAY[0.0341, 0.0, -0.0341],
             ARRAY[1.0, -1.8740, 0.9319]) AS band_mid,
         -- Band 3: 3.5–6.0 kHz (typical bearing resonance zone)
         IIR(amplitude,
             ARRAY[0.0573, 0.0, -0.0573],
             ARRAY[1.0, -1.7157, 0.8854]) AS band_high,
         -- Band 4: 6.0–10.0 kHz (structural resonances)
         IIR(amplitude,
             ARRAY[0.0955, 0.0, -0.0955],
             ARRAY[1.0, -1.3090, 0.8090]) AS band_vhigh
  FROM vibration_raw;


-- ---------------------------------------------------------------------------
-- View 2: Per-burst kurtosis in each band (event-scoped)
-- ---------------------------------------------------------------------------
-- Raw-moments kurtosis formula applied to each band's filtered signal.
-- The band with highest kurtosis per burst indicates the most impulsive
-- frequency range.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW spectral_kurtosis_bands AS
  SELECT device_id, channel_id,

         -- Band 1: kurtosis (0–1.28 kHz)
         ( AVG(POWER(band_low, 4))
           - 4.0 * AVG(POWER(band_low, 3)) * AVG(band_low)
           + 6.0 * AVG(POWER(band_low, 2)) * POWER(AVG(band_low), 2)
           - 3.0 * POWER(AVG(band_low), 4)
         ) / POWER(
           AVG(POWER(band_low, 2)) - POWER(AVG(band_low), 2), 2
         ) AS kurt_band_low,

         -- Band 2: kurtosis (1.28–3.5 kHz)
         ( AVG(POWER(band_mid, 4))
           - 4.0 * AVG(POWER(band_mid, 3)) * AVG(band_mid)
           + 6.0 * AVG(POWER(band_mid, 2)) * POWER(AVG(band_mid), 2)
           - 3.0 * POWER(AVG(band_mid), 4)
         ) / POWER(
           AVG(POWER(band_mid, 2)) - POWER(AVG(band_mid), 2), 2
         ) AS kurt_band_mid,

         -- Band 3: kurtosis (3.5–6.0 kHz)
         ( AVG(POWER(band_high, 4))
           - 4.0 * AVG(POWER(band_high, 3)) * AVG(band_high)
           + 6.0 * AVG(POWER(band_high, 2)) * POWER(AVG(band_high), 2)
           - 3.0 * POWER(AVG(band_high), 4)
         ) / POWER(
           AVG(POWER(band_high, 2)) - POWER(AVG(band_high), 2), 2
         ) AS kurt_band_high,

         -- Band 4: kurtosis (6.0–10.0 kHz)
         ( AVG(POWER(band_vhigh, 4))
           - 4.0 * AVG(POWER(band_vhigh, 3)) * AVG(band_vhigh)
           + 6.0 * AVG(POWER(band_vhigh, 2)) * POWER(AVG(band_vhigh), 2)
           - 3.0 * POWER(AVG(band_vhigh), 4)
         ) / POWER(
           AVG(POWER(band_vhigh, 2)) - POWER(AVG(band_vhigh), 2), 2
         ) AS kurt_band_vhigh,

         COUNT(*) AS sample_count

  FROM spectral_bands_filtered
  GROUP BY device_id, channel_id, ABS(amplitude) > 0;
