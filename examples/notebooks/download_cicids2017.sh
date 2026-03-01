#!/usr/bin/env bash
# Download CICIDS2017 dataset for Network Intrusion Detection notebook
# Source: Canadian Institute for Cybersecurity - UNB
# https://www.unb.ca/cic/datasets/ids-2017.html

set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"
mkdir -p "$DATA_DIR"

echo "=== CICIDS2017 Data Download ==="
echo ""
echo "This dataset is large (~1.5GB total). We'll download the Friday morning file"
echo "which contains DDoS, Brute Force, and other attack scenarios."
echo ""

# The dataset is hosted on UNB's servers
# Alternative mirrors: Kaggle, AWS Open Data

FRIDAY_FILE="Friday-WorkingHours-Morning.csv"

# Check if already exists
if [ -f "$DATA_DIR/$FRIDAY_FILE" ] && [ -s "$DATA_DIR/$FRIDAY_FILE" ]; then
    echo "Already have: $FRIDAY_FILE ($(wc -l < "$DATA_DIR/$FRIDAY_FILE") lines)"
    echo "Done!"
    exit 0
fi

echo "Downloading $FRIDAY_FILE..."
echo "(This may take several minutes due to size)"
echo ""

# Try direct download from UNB
URLS=(
    "https://www.unb.ca/cic/datasets/resources/network-intrusion-detection/Friday-WorkingHours-Morning.csv"
    "https://mirror.clarkson.edu/unb/Canada-Institute-Cybersecurity/datasets/CIC-IDS-2017/Machine-Learning-CSV/ML-Ready%20Data/Friday-WorkingHours-Morning.csv"
)

DOWNLOADED=0
for URL in "${URLS[@]}"; do
    echo "Trying: $URL"
    if curl -sLI "$URL" 2>/dev/null | grep -q "200\|OK"; then
        echo "  -> Downloading..."
        if curl -L --progress-bar "$URL" -o "$DATA_DIR/$FRIDAY_FILE"; then
            DOWNLOADED=1
            break
        fi
    fi
done

if [ "$DOWNLOADED" = "1" ] && [ -s "$DATA_DIR/$FRIDAY_FILE" ]; then
    echo ""
    echo "Success! Downloaded $(wc -l < "$DATA_DIR/$FRIDAY_FILE") lines"
else
    echo ""
    echo "ERROR: Could not download automatically."
    echo ""
    echo "Please download manually:"
    echo "1. Visit: https://www.unb.ca/cic/datasets/ids-2017.html"
    echo "2. Click 'Friday-WorkingHours-Morning.csv'"
    echo "3. Save to: $DATA_DIR/"
    echo ""
    echo "Alternative: Search for 'CICIDS2017' on Kaggle"
    exit 1
fi
