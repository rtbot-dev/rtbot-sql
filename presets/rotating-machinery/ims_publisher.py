#!/usr/bin/env python3
"""
IMS Bearing Dataset → Ignition OPC UA Publisher
================================================

Replays NASA IMS Bearings Dataset (Set No. 1) to Ignition memory tags via
OPC UA.  Each burst file (20,480 samples × 8 columns) is loaded, the x-axis
accelerometer columns are extracted, and each bearing's amplitude vector is
written as a single comma-separated CSV string to the corresponding Ignition
memory tag.

Data flow:
    Python → OPC UA → Ignition memory tag → TagChangeListener
    → ingestCsvBurstInternal → RTBot runtime

Usage:
    python ims_publisher.py                          # defaults
    python ims_publisher.py --bearings 3 4           # only bearings 3 & 4
    python ims_publisher.py --burst-step 10          # every 10th burst
    python ims_publisher.py --interval 1.0           # 1 s between bursts

Dependencies (all in requirements.txt + asyncua):
    pip install asyncua pooch numpy pandas
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pooch

# ---------------------------------------------------------------------------
# Lazy import: asyncua is only needed at runtime, not at import-time.  This
# lets us validate the module with a bare ``import ims_publisher`` even when
# asyncua is not installed (useful for CI / linting).
# ---------------------------------------------------------------------------
try:
    from asyncua import Client, ua
except ImportError:  # pragma: no cover
    Client = None  # type: ignore[assignment,misc]
    ua = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ims_publisher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IMS_URL = "https://data.nasa.gov/docs/legacy/IMS.zip"
SAMPLES_PER_BURST = 20_480

# Bearing ID → column index in the burst file (x-axis accelerometers)
BEARING_COLUMN: dict[int, int] = {
    1: 0,  # sensor_1
    2: 2,  # sensor_3
    3: 4,  # sensor_5
    4: 6,  # sensor_7
}

# OPC UA tag template  (bearing_id is substituted at runtime)
TAG_TEMPLATE = "ns=2;s=[default]Edge/vibration/{bearing}/1/burst"


# ===================================================================
# Dataset acquisition
# ===================================================================

def _find_extracted_dir(cache_dir: Path) -> Path | None:
    """Return the ``1st_test`` directory if it already exists."""
    for candidate in (
        cache_dir / "1st_test_extracted" / "1st_test",
        cache_dir / "1st_test",
    ):
        if candidate.is_dir():
            return candidate
    return None


def _extract_rar(rar_path: str, extract_dir: Path) -> None:
    """Extract the inner 1st_test.rar using bsdtar (macOS) or unrar."""
    marker = extract_dir / ".extraction_complete"
    if marker.exists():
        log.info("RAR already extracted (cached).")
        return

    extract_dir.mkdir(parents=True, exist_ok=True)
    log.info("Extracting %s with bsdtar ...", os.path.basename(rar_path))
    result = subprocess.run(
        ["bsdtar", "-xf", rar_path, "-C", str(extract_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"bsdtar extraction failed (code {result.returncode}):\n"
            f"{result.stderr}\n"
            "On macOS, bsdtar (built-in) handles RAR via libarchive.\n"
            "On Linux, install libarchive-tools or use: pip install rarfile"
        )
    marker.write_text("done")
    log.info("Extraction complete.")


def acquire_dataset() -> list[str]:
    """Download / locate the IMS dataset and return sorted burst file paths.

    Strategy:
      1. ``pooch`` — cached download from NASA.
      2. Kaggle CLI fallback.
      3. Print instructions and exit.
    """
    cache_dir = Path(pooch.os_cache("katenaria-ims"))

    # Fast path: already extracted?
    test_dir = _find_extracted_dir(cache_dir)
    if test_dir is not None:
        return _list_burst_files(test_dir)

    # ------------------------------------------------------------------
    # Strategy 1: pooch download + unzip + bsdtar
    # ------------------------------------------------------------------
    try:
        log.info("Downloading IMS dataset via pooch (first run only, ~1.6 GB) ...")
        unzipped = pooch.retrieve(
            url=IMS_URL,
            known_hash=None,
            processor=pooch.Unzip(),
            fname="IMS.zip",
        )
        rar_paths = [
            f for f in unzipped
            if f.endswith("1st_test.rar") and "__MACOSX" not in f
        ]
        if not rar_paths:
            raise FileNotFoundError("1st_test.rar not found in ZIP contents")

        extract_dir = Path(os.path.dirname(rar_paths[0])) / "1st_test_extracted"
        _extract_rar(rar_paths[0], extract_dir)
        test_dir = extract_dir / "1st_test"
        if test_dir.is_dir():
            return _list_burst_files(test_dir)
    except Exception as exc:
        log.warning("pooch download failed: %s", exc)

    # ------------------------------------------------------------------
    # Strategy 2: Kaggle CLI
    # ------------------------------------------------------------------
    try:
        log.info("Trying Kaggle CLI ...")
        kaggle_dir = cache_dir / "kaggle"
        kaggle_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                "kaggle", "datasets", "download",
                "-d", "vinayak123tyagi/bearing-dataset",
                "-p", str(kaggle_dir),
                "--unzip",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        test_dir = _find_extracted_dir(kaggle_dir)
        if test_dir is not None:
            return _list_burst_files(test_dir)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        log.warning("Kaggle CLI failed: %s", exc)

    # ------------------------------------------------------------------
    # Strategy 3: Instructions
    # ------------------------------------------------------------------
    print(
        "\n"
        "=== Could not download the IMS dataset automatically ===\n"
        "\n"
        "Option A — install pooch and retry:\n"
        "    pip install pooch\n"
        "\n"
        "Option B — download manually:\n"
        "    1. Go to https://data.nasa.gov/download/brfb-gzcv/application%2Fx-zip-compressed\n"
        "    2. Extract IMS.zip\n"
        "    3. Extract 1st_test.rar into a directory\n"
        f"    4. Place the '1st_test' folder at: {cache_dir / '1st_test'}\n"
        "\n"
        "Option C — use Kaggle:\n"
        "    pip install kaggle\n"
        "    kaggle datasets download -d vinayak123tyagi/bearing-dataset --unzip\n"
    )
    sys.exit(1)


def _list_burst_files(test_dir: Path) -> list[str]:
    """Return sorted list of burst file paths (exclude hidden files)."""
    files = sorted(
        str(test_dir / f)
        for f in os.listdir(test_dir)
        if not f.startswith(".") and (test_dir / f).is_file()
    )
    if not files:
        raise FileNotFoundError(f"No burst files found in {test_dir}")
    log.info(
        "Found %d burst files in %s", len(files), test_dir.name,
    )
    log.info("  First: %s", os.path.basename(files[0]))
    log.info("  Last:  %s", os.path.basename(files[-1]))
    return files


# ===================================================================
# Helpers
# ===================================================================

def filename_to_datetime(filepath: str) -> datetime:
    """Parse timestamp from IMS filename: ``YYYY.MM.DD.HH.MM.SS``."""
    parts = os.path.basename(filepath).split(".")
    return datetime(
        int(parts[0]), int(parts[1]), int(parts[2]),
        int(parts[3]), int(parts[4]), int(parts[5]),
    )


def build_csv(amplitudes: np.ndarray) -> str:
    """Convert amplitude array → CSV string with sentinel.

    1. Replace exact ``0.0`` with ``1e-15`` (avoids spurious segment breaks
       in the ``GROUP BY ABS(amplitude) > 0`` pattern).
    2. Format each value with ``:.10g`` for full double precision.
    3. Append a trailing ``0.0`` sentinel.  Since real zeros were replaced,
       the sentinel is unambiguous.
    """
    amps = amplitudes.copy()
    amps[amps == 0.0] = 1e-15
    # Use :.10g — keeps up to 10 significant digits, drops trailing zeros,
    # and avoids scientific notation for values in a reasonable range.
    parts = [f"{v:.10g}" for v in amps]
    parts.append("0.0")  # sentinel
    return ",".join(parts)


# ===================================================================
# OPC UA publishing
# ===================================================================

async def publish(
    server: str,
    user: str,
    password: str,
    bearings: list[int],
    interval: float,
    burst_step: int,
    max_bursts: int | None,
) -> None:
    """Connect to Ignition OPC UA and replay IMS burst files."""
    if Client is None:
        print(
            "ERROR: asyncua is not installed.\n"
            "       pip install asyncua",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Acquire dataset ───────────────────────────────────────────
    all_files = acquire_dataset()
    selected = all_files[::burst_step]
    if max_bursts is not None:
        selected = selected[:max_bursts]

    n_bursts = len(selected)
    n_bearings = len(bearings)
    log.info(
        "Will publish %d bursts × %d bearings = %d tag writes",
        n_bursts, n_bearings, n_bursts * n_bearings,
    )

    # ── Connect to OPC UA ─────────────────────────────────────────
    client = Client(url=server)
    client.set_user(user)
    client.set_password(password)
    # Use "None" security policy for local dev/demo.  asyncua's
    # set_security_string is a coroutine in some versions but we only
    # need to force the policy selection; the monkey-patch below
    # handles the actual password encryption bypass.
    try:
        coro = client.set_security_string("None")
        if coro is not None:
            await coro
    except Exception:
        pass  # acceptable – policy will be negotiated automatically

    # Monkey-patch: asyncua fails to encrypt the password when using
    # security policy "None" because ``peer_certificate`` is None.
    _original_encrypt = client._encrypt_password

    def _patched_encrypt(password: str | bytes, policy_uri: str):  # type: ignore[override]
        if not client.security_policy.peer_certificate:
            raw = password.encode("utf-8") if isinstance(password, str) else password
            return raw, ""
        return _original_encrypt(password, policy_uri)

    client._encrypt_password = _patched_encrypt  # type: ignore[assignment]

    log.info("Connecting to %s ...", server)
    async with client:
        log.info("Connected.")

        # Resolve tag nodes once
        tag_nodes: dict[int, object] = {}
        for b in bearings:
            node_id = TAG_TEMPLATE.format(bearing=b)
            tag_nodes[b] = client.get_node(node_id)
            log.info("  Bearing %d → %s", b, node_id)

        # ── Replay loop ──────────────────────────────────────────
        t_start = time.perf_counter()
        total_samples = 0

        for burst_idx, filepath in enumerate(selected):
            t_burst = time.perf_counter()
            ts = filename_to_datetime(filepath)

            try:
                raw = pd.read_csv(filepath, sep="\t", header=None).values
            except Exception as exc:
                log.error("Failed to read %s: %s — skipping", filepath.name, exc)
                continue
            n_samples = raw.shape[0]

            for b in bearings:
                col_idx = BEARING_COLUMN[b]
                amplitudes = raw[:, col_idx].astype(np.float64)
                csv_str = build_csv(amplitudes)

                dv = ua.DataValue(ua.Variant(csv_str, ua.VariantType.String))
                await tag_nodes[b].write_value(dv)

            total_samples += n_samples * n_bearings
            elapsed_burst = time.perf_counter() - t_burst
            elapsed_total = time.perf_counter() - t_start

            burst_num = burst_idx + 1
            rate = total_samples / elapsed_total if elapsed_total > 0 else 0
            log.info(
                "[%4d/%d] %s | %d samples/bearing | %.2fs | %.0f samp/s cumulative",
                burst_num,
                n_bursts,
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                n_samples,
                elapsed_burst,
                rate,
            )

            # Inter-burst delay (skip after last burst)
            if burst_idx < n_bursts - 1 and interval > 0:
                await asyncio.sleep(interval)

        # ── Summary ──────────────────────────────────────────────
        elapsed_total = time.perf_counter() - t_start
        log.info("─" * 60)
        log.info("Done.")
        log.info("  Total bursts:  %d", n_bursts)
        log.info("  Total samples: %s", f"{total_samples:,}")
        log.info("  Elapsed:       %.1fs (%.1f min)", elapsed_total, elapsed_total / 60)
        if elapsed_total > 0:
            log.info("  Average rate:  %s samples/sec", f"{total_samples / elapsed_total:,.0f}")


# ===================================================================
# CLI
# ===================================================================

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(
        description="Replay IMS Bearing Dataset to Ignition via OPC UA",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--server",
        default="opc.tcp://localhost:62541",
        help="OPC UA server endpoint",
    )
    p.add_argument("--user", default="opcuauser", help="OPC UA username")
    p.add_argument("--password", default="password", help="OPC UA password")
    p.add_argument(
        "--bearings",
        nargs="+",
        type=int,
        default=[1, 2, 3, 4],
        help="Bearing IDs to publish (1-4)",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between bursts",
    )
    p.add_argument(
        "--burst-step",
        type=int,
        default=1,
        help="Process every Nth burst",
    )
    p.add_argument(
        "--max-bursts",
        type=int,
        default=None,
        help="Limit number of bursts (None = all)",
    )
    args = p.parse_args(argv)

    # Validate arguments
    if args.burst_step < 1:
        p.error("--burst-step must be >= 1")
    for b in args.bearings:
        if b not in BEARING_COLUMN:
            p.error(f"Invalid bearing ID {b}. Must be one of {list(BEARING_COLUMN)}")

    return args


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    args = parse_args(argv)
    asyncio.run(
        publish(
            server=args.server,
            user=args.user,
            password=args.password,
            bearings=args.bearings,
            interval=args.interval,
            burst_step=args.burst_step,
            max_bursts=args.max_bursts,
        )
    )


if __name__ == "__main__":
    main()
