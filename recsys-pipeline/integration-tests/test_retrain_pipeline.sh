#!/usr/bin/env bash
# Integration test for run-retrain.sh
# Runs in dry-run mode (DRY_RUN=1) to verify the script's step sequencing without
# actually running Spark jobs or calling a live service.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== run-retrain.sh integration test (dry-run) ==="

DRY_RUN=1 bash "${REPO_ROOT}/run-retrain.sh" 2>&1 | tee /tmp/retrain-test-output.txt

if grep -q "DRY RUN: skip" /tmp/retrain-test-output.txt; then
  echo "PASS: dry-run mode activated"
else
  echo "FAIL: dry-run output not detected"
  exit 1
fi

for step in "Step 1" "Step 2" "Step 3" "Step 4" "Step 5"; do
  if grep -q "${step}" /tmp/retrain-test-output.txt; then
    echo "PASS: ${step} found in output"
  else
    echo "FAIL: ${step} not found in output"
    exit 1
  fi
done

echo "=== All integration tests passed ==="
