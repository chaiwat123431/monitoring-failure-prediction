#!/usr/bin/env bash
# Fetches the 2 NAB series this project ingests (PLANNING.md AD-10).
# data/ is gitignored — this script is how it gets populated locally.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="https://raw.githubusercontent.com/numenta/NAB/master/data/realAWSCloudwatch"
DEST="data/realAWSCloudwatch"
mkdir -p "$DEST"

for f in ec2_cpu_utilization_825cc2.csv rds_cpu_utilization_cc0c53.csv; do
  echo "Fetching $f..."
  curl -sf "$BASE/$f" -o "$DEST/$f"
  rows=$(($(wc -l < "$DEST/$f") - 1))
  echo "  -> $rows data rows"
done
