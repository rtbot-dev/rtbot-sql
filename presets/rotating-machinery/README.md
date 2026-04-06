# Preset Library — Rotating Machinery

**Predictive maintenance of industrial rolling element bearings via vibration signal analysis.**

Scientific basis: Sacerdoti, D.; Strozzi, M.; Secchi, C. *A Comparison of Signal Analysis Techniques for the Diagnostics of the IMS Rolling Element Bearing Dataset.* Appl. Sci. 2023, 13, 5977. https://doi.org/10.3390/app13105977

Dataset: NASA IMS Rolling Element Bearing Dataset (University of Cincinnati Center for Intelligent Maintenance Systems).

---

## Directory layout

Each preset is a `.sql` file accompanied by an `.ipynb` notebook in the same subdirectory. The notebook shows the reasoning behind the SQL, validates it against the IMS dataset, and documents the parameter choices. The SQL file is the production artefact; the notebook is the scientific reference.

```
presets/rotating-machinery/
  README.md                                    ← this file
  tier1-detection/
    01_rms_trend.sql                           production preset
    01_rms_trend.ipynb                         validation notebook
    02_kurtosis_trend.sql
    02_kurtosis_trend.ipynb
    03_detectivity.sql
    03_detectivity.ipynb
    04_crest_clearance_impulse.sql
    04_crest_clearance_impulse.ipynb
    05_statistical_dashboard.sql
    05_statistical_dashboard.ipynb
  tier2-diagnosis/
    06_fault_band_energy.sql
    06_fault_band_energy.ipynb
    07_envelope_analysis.sql
    07_envelope_analysis.ipynb
    08_cepstrum_prewhitening.sql
    08_cepstrum_prewhitening.ipynb
    09_spectral_kurtosis_bands.sql
    09_spectral_kurtosis_bands.ipynb
  tier3-prognosis/
    10_health_index.sql
    10_health_index.ipynb
    11_degradation_rate.sql
    11_degradation_rate.ipynb
```

---

## Three-tier architecture

The library mirrors the three phases of condition monitoring established in the paper.

**Tier 1 — Fault Detection** (`tier1-detection/`): always-on statistical screening of the raw vibration signal. Answers: *is there a fault?* Computationally lightweight; suitable for continuous wide-area coverage across all monitored assets.

**Tier 2 — Fault Diagnosis** (`tier2-diagnosis/`): frequency-domain and DSP techniques that identify *which bearing component* is degrading. Activated after a Tier 1 alert. Requires knowledge of the bearing geometry (characteristic fault frequencies).

**Tier 3 — Fault Prognosis** (`tier3-prognosis/`): tracks the trajectory of degradation over time. Answers: *how fast is the bearing degrading?* Enables remaining useful life estimation.

---

## Presets

### Tier 1 — Fault Detection

| # | File | Key views | Paper evidence |
|---|------|-----------|----------------|
| 01 | `rms_trend` | `rms_trend`, `rms_alert` | RMS rises from ~0.12 to >0.5 at fault onset (Figs 6b, 7b). Top prognostic scores on Corr/Mon/Rob (Fig 49). |
| 02 | `kurtosis_trend` | `kurtosis_intermediates`, `kurtosis_trend`, `kurtosis_alert` | Kurtosis rises from ~3 (Gaussian) to >70 (Figs 6d, 7d). Leading indicator — rises before RMS. Accuracy benchmark across all techniques (Table 5). |
| 03 | `detectivity` | `hjorth_derivatives`, `hjorth_variances`, `hjorth_parameters`, `detectivity`, `detectivity_alert` | Detectivity: "the clearest signature of fault" as a single dB-normalised scalar (Figs 14c, 15c). Ref: Cocconcelli et al. 2022. |
| 04 | `crest_clearance_impulse` | `advanced_stat_intermediates`, `crest_clearance_impulse`, `clearance_factor_alert` | Clearance Factor and Impulse Factor give "consistent information" on faulted bearings (Figs 10, 11). |
| 05 | `statistical_dashboard` | `statistical_dashboard` | All six basic parameters (Mean, Variance, STD, RMS, Skewness, Kurtosis) in one view. Reproduces Figs 4–7. |

### Tier 2 — Fault Diagnosis

| # | File | Key views | Paper evidence |
|---|------|-----------|----------------|
| 06 | `fault_band_energy` | `bpfo_band`, `bpfi_band`, `bsf_band`, `ftf_band`, `fault_band_energy`, `fault_band_alert` | IIR bandpass filters around BPFO/BPFI/BSF/FTF. PSD (Figs 20–22) and STFT (Figs 16–19) confirm fault harmonics. ⚠ Requires bearing-specific coefficient parameterisation — see notebook. |
| 07 | `envelope_analysis` | `envelope_highpass`, `envelope_squared`, `envelope_demodulated`, `envelope_energy_trend`, `envelope_energy_alert` | SES is "a benchmark among diagnostic techniques" (Sec 3.2.3, Figs 24–25). This preset implements a streaming time-domain proxy. |
| 08 | `cepstrum_prewhitening` | `spectral_shape_estimate`, `prewhitened_signal`, `whitened_kurtosis`, `whitened_kurtosis_alert` | CPW highest-scoring technique: kurtosis 86.0 / 81.7 (Table 5). Removes all masking components leaving fault signatures "unmistakably detectable" (Figs 37–40). |
| 09 | `spectral_kurtosis_bands` | `band_low`, `band_mid`, `band_high`, `band_vhigh`, `spectral_kurtosis_bands` | Streaming Kurtogram across four frequency bands (Figs 32–36). Identifies the most impulsive resonance band in real time. |

### Tier 3 — Fault Prognosis

| # | File | Key views | Paper evidence |
|---|------|-----------|----------------|
| 10 | `health_index` | `prognostic_parameters`, `health_index`, `health_index_alert` | Composite of the four best prognostic parameters by Corr/Mon/Rob: RMS, Variance, STD, Peak-to-Peak (Fig 49). Score starts near 1.0 healthy, rises monotonically. |
| 11 | `degradation_rate` | `rms_dual_window`, `degradation_rate`, `rapid_degradation_alert` | Short/long moving average crossover. Detects active degradation (positive rate) and transition to rapid failure propagation (positive acceleration). |

---

## Input stream schema

All presets consume a single input stream:

```sql
CREATE STREAM vibration_raw (
    device_id  VARCHAR,   -- motor or asset identifier
    channel_id VARCHAR,   -- accelerometer axis (e.g. "x", "y")
    ts         DOUBLE,    -- timestamp (seconds or epoch ms)
    amplitude  DOUBLE     -- acceleration in m/s² or g
);
```

---

## Default parameterisation

Window sizes are calibrated for the IMS dataset at **fs = 20,480 Hz**. Scale linearly for other sampling rates.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `WINDOW_SIZE` | 20480 | 1 s of raw samples at 20480 Hz |
| `BASELINE_WIN` | 28800 | 8 h of per-second indicator values |
| `RMS alert mult` | 2.5× | Fault onset ≈ 2–4× RMS increase |
| `Kurtosis threshold` | 5.0 | Early fault (Gaussian baseline = 3.0) |
| `Detectivity threshold` | 3.0 dB | Paper shows 5–15 dB at fault onset |
| `Clearance Factor mult` | 2.0× | Consistent with Figs 10–11 |
| `Fault band alert mult` | 3.0× | Conservative; adjust per commissioning |
| `Health Index alert` | 1.5 | 50% above baseline = degradation onset |

---

## Dependency graph

```
vibration_raw
  │
  ├── Tier 1 (always-on)
  │     ├── 01_rms_trend         → rms_alert
  │     ├── 02_kurtosis_trend    → kurtosis_alert
  │     ├── 03_detectivity       → detectivity_alert
  │     ├── 04_crest_clearance   → clearance_factor_alert
  │     └── 05_statistical_dashboard
  │
  ├── Tier 2 (on Tier 1 alert)
  │     ├── 06_fault_band_energy → fault_band_alert
  │     ├── 07_envelope_analysis → envelope_energy_alert
  │     ├── 08_cepstrum_prewhitening → whitened_kurtosis_alert
  │     └── 09_spectral_kurtosis_bands
  │
  └── Tier 3 (continuous long-term)
        ├── 10_health_index      → health_index_alert
        └── 11_degradation_rate  → rapid_degradation_alert
```

---

## RtBot SQL operators used

`MOVING_AVERAGE`, `MOVING_SUM`, `STDDEV`, `POWER`, `ABS`, `LN`, `FIR`, `IIR`, `PEAK_DETECT`

---

## Adding a new domain

To add a preset collection for a different industrial domain (e.g. `cold-chain`, `hvac`, `network-security`), create a sibling directory under `presets/` following the same conventions:

- one `.sql` file per self-contained technique
- one `.ipynb` notebook co-located with each `.sql` file
- a `README.md` at the domain root
- tier subdirectories only if the domain naturally has detection / diagnosis / prognosis phases; otherwise use domain-specific subdirectory names
