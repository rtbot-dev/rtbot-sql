#!/usr/bin/env bash
# Download Binance public trade data for the D03 notebook demos.
# Source: https://data.binance.vision — MIT license.
set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"
mkdir -p "$DATA_DIR"

DATE="2023-01-01"
SYMBOLS=(BTCUSDT ETHUSDT DOGEUSDT)

for SYM in "${SYMBOLS[@]}"; do
    CSV="$DATA_DIR/${SYM}-trades-${DATE}.csv"
    if [ -f "$CSV" ]; then
        echo "skip $SYM (already exists)"
        continue
    fi
    URL="https://data.binance.vision/data/spot/daily/trades/${SYM}/${SYM}-trades-${DATE}.zip"
    echo "downloading $SYM ..."
    curl -sL "$URL" -o "/tmp/${SYM}-trades-${DATE}.zip"
    unzip -o "/tmp/${SYM}-trades-${DATE}.zip" -d "$DATA_DIR"
    rm "/tmp/${SYM}-trades-${DATE}.zip"
    echo "  -> $(wc -l < "$CSV") trades"
done

echo "done"
