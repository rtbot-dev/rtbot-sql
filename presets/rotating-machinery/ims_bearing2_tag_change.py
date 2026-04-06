#!/usr/bin/env python3
"""
IMS bearing-2 Python baseline for Ignition.

Purpose:
- Reference implementation to compare Python processing vs Coprocessor/RTBot.
- Intended for an Ignition Tag Change script attached to:
  [default]Edge/vibration/2/1/burst

Notes:
- Input is a CSV burst string (same format emitted by ims_publisher.py).
- The publisher appends a trailing 0.0 sentinel; this script strips it.
- Output tags are written under:
  [default]Edge/python/ims-bearing-2/...
"""

import math


INPUT_TAG = "[default]Edge/vibration/2/1/burst"
OUTPUT_BASE = "[default]Edge/python/ims-bearing-2"
DEVICE_ID = "2"
CHANNEL_ID = "1"


def _parse_csv_burst(payload):
    if payload is None:
        return []

    text = str(payload).strip()
    if not text:
        return []

    out = []
    for part in text.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(float(p))
        except Exception:
            # Skip malformed tokens; we still process the rest of the burst.
            continue

    # ims_publisher.py appends a trailing 0.0 sentinel.
    if out and out[-1] == 0.0:
        out = out[:-1]

    return out


def _mean(values):
    return sum(values) / float(len(values))


def _safe_div(numerator, denominator):
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _compute_metrics(samples):
    n = len(samples)
    if n == 0:
        return None

    mean_value = _mean(samples)
    centered = [x - mean_value for x in samples]

    m2 = _mean([x * x for x in centered])
    m3 = _mean([x * x * x for x in centered])
    m4 = _mean([x * x * x * x for x in centered])

    variance_value = m2
    std_value = math.sqrt(max(variance_value, 0.0))
    rms_value = math.sqrt(_mean([x * x for x in samples]))
    kurtosis_value = _safe_div(m4, variance_value * variance_value)
    skewness_value = _safe_div(m3, std_value * std_value * std_value)

    abs_values = [abs(x) for x in samples]
    peak_value = max(abs_values)
    min_value = min(samples)
    max_value = max(samples)
    peak_to_peak = max_value - min_value
    mean_abs = _mean(abs_values)
    mean_sqrt_abs = _mean([math.sqrt(a) for a in abs_values])

    crest_factor = _safe_div(peak_value, rms_value)
    clearance_factor = _safe_div(peak_value, mean_sqrt_abs * mean_sqrt_abs)
    impulse_factor = _safe_div(peak_value, mean_abs)
    shape_factor = _safe_div(rms_value, mean_abs)

    return {
        "sample_count": n,
        "device_id": DEVICE_ID,
        "channel_id": CHANNEL_ID,
        "mean_value": mean_value,
        "variance_value": variance_value,
        "std_value": std_value,
        "rms_value": rms_value,
        "m3": m3,
        "m4": m4,
        "skewness_value": skewness_value,
        "kurtosis_value": kurtosis_value,
        "peak_to_peak": peak_to_peak,
        "crest_factor": crest_factor,
        "clearance_factor": clearance_factor,
        "impulse_factor": impulse_factor,
        "shape_factor": shape_factor,
    }


def _write_metrics(metrics):
    paths = []
    values = []

    # rms_trend
    paths.extend([
        OUTPUT_BASE + "/rms_trend/sample_count",
        OUTPUT_BASE + "/rms_trend/rms_value",
        OUTPUT_BASE + "/rms_trend/device_id",
        OUTPUT_BASE + "/rms_trend/channel_id",
    ])
    values.extend([
        metrics["sample_count"],
        metrics["rms_value"],
        metrics["device_id"],
        metrics["channel_id"],
    ])

    # kurtosis_trend
    paths.extend([
        OUTPUT_BASE + "/kurtosis_trend/sample_count",
        OUTPUT_BASE + "/kurtosis_trend/kurtosis_value",
        OUTPUT_BASE + "/kurtosis_trend/m4",
        OUTPUT_BASE + "/kurtosis_trend/variance_value",
        OUTPUT_BASE + "/kurtosis_trend/device_id",
        OUTPUT_BASE + "/kurtosis_trend/channel_id",
    ])
    values.extend([
        metrics["sample_count"],
        metrics["kurtosis_value"],
        metrics["m4"],
        metrics["variance_value"],
        metrics["device_id"],
        metrics["channel_id"],
    ])

    # crest_clearance_impulse
    paths.extend([
        OUTPUT_BASE + "/crest_clearance_impulse/sample_count",
        OUTPUT_BASE + "/crest_clearance_impulse/peak_to_peak",
        OUTPUT_BASE + "/crest_clearance_impulse/impulse_factor",
        OUTPUT_BASE + "/crest_clearance_impulse/shape_factor",
        OUTPUT_BASE + "/crest_clearance_impulse/clearance_factor",
        OUTPUT_BASE + "/crest_clearance_impulse/crest_factor",
        OUTPUT_BASE + "/crest_clearance_impulse/rms_value",
        OUTPUT_BASE + "/crest_clearance_impulse/device_id",
        OUTPUT_BASE + "/crest_clearance_impulse/channel_id",
    ])
    values.extend([
        metrics["sample_count"],
        metrics["peak_to_peak"],
        metrics["impulse_factor"],
        metrics["shape_factor"],
        metrics["clearance_factor"],
        metrics["crest_factor"],
        metrics["rms_value"],
        metrics["device_id"],
        metrics["channel_id"],
    ])

    # statistical_dashboard
    paths.extend([
        OUTPUT_BASE + "/statistical_dashboard/sample_count",
        OUTPUT_BASE + "/statistical_dashboard/kurtosis_value",
        OUTPUT_BASE + "/statistical_dashboard/m4",
        OUTPUT_BASE + "/statistical_dashboard/skewness_value",
        OUTPUT_BASE + "/statistical_dashboard/m3",
        OUTPUT_BASE + "/statistical_dashboard/rms_value",
        OUTPUT_BASE + "/statistical_dashboard/std_value",
        OUTPUT_BASE + "/statistical_dashboard/variance_value",
        OUTPUT_BASE + "/statistical_dashboard/mean_value",
        OUTPUT_BASE + "/statistical_dashboard/device_id",
        OUTPUT_BASE + "/statistical_dashboard/channel_id",
    ])
    values.extend([
        metrics["sample_count"],
        metrics["kurtosis_value"],
        metrics["m4"],
        metrics["skewness_value"],
        metrics["m3"],
        metrics["rms_value"],
        metrics["std_value"],
        metrics["variance_value"],
        metrics["mean_value"],
        metrics["device_id"],
        metrics["channel_id"],
    ])

    system.tag.writeBlocking(paths, values)


def valueChanged(tagPath, previousValue, currentValue, initialChange, missedEvents):
    if initialChange:
        return

    # Optional guard if reused accidentally on another tag.
    if str(tagPath) != INPUT_TAG:
        return

    logger = system.util.getLogger("ims-bearing-2-python")

    samples = _parse_csv_burst(currentValue.value)
    if not samples:
        logger.warn("Skipped empty burst for %s" % str(tagPath))
        return

    metrics = _compute_metrics(samples)
    if metrics is None:
        logger.warn("Failed to compute metrics for %s" % str(tagPath))
        return

    _write_metrics(metrics)
