#!/usr/bin/env bash
# Integration tests for scripts/install-cron.sh
# Uses --dry-run so the real crontab is never modified.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_CRON="${SCRIPT_DIR}/../scripts/install-cron.sh"
PASS=0
FAIL=0

assert_contains() {
  local label="$1" pattern="$2" output="$3"
  if echo "${output}" | grep -q "${pattern}"; then
    echo "PASS: ${label}"
    PASS=$((PASS + 1))
  else
    echo "FAIL: ${label} — expected pattern '${pattern}' in:"
    echo "${output}" | sed 's/^/  /'
    FAIL=$((FAIL + 1))
  fi
}

assert_not_contains() {
  local label="$1" pattern="$2" output="$3"
  if ! echo "${output}" | grep -q "${pattern}"; then
    echo "PASS: ${label}"
    PASS=$((PASS + 1))
  else
    echo "FAIL: ${label} — unexpected pattern '${pattern}' found"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== install-cron.sh integration tests (dry-run only) ==="
echo ""

# Test 1: --dry-run install prints the cron entry without touching crontab
OUT=$(bash "${INSTALL_CRON}" --dry-run 2>&1)
assert_contains "dry-run shows DRY RUN prefix" "DRY RUN" "${OUT}"
assert_contains "dry-run shows run-retrain.sh" "run-retrain.sh" "${OUT}"
assert_contains "dry-run shows default 6-hour schedule" "0 \*/6 \* \* \*" "${OUT}"

# Test 2: --dry-run with custom schedule reflects the schedule
OUT=$(bash "${INSTALL_CRON}" --schedule "30 3 * * *" --dry-run 2>&1)
assert_contains "custom schedule in dry-run" "30 3 \* \* \*" "${OUT}"

# Test 3: --dry-run with custom log path reflects the path
OUT=$(bash "${INSTALL_CRON}" --log "/tmp/recsys-test.log" --dry-run 2>&1)
assert_contains "custom log path in dry-run" "/tmp/recsys-test.log" "${OUT}"

# Test 4: --uninstall with no existing entry exits cleanly
OUT=$(bash "${INSTALL_CRON}" --uninstall 2>&1) || true
assert_contains "uninstall with no entry is a no-op" "nothing to remove" "${OUT}"

# Test 5: marker comment is included in the generated entry
OUT=$(bash "${INSTALL_CRON}" --dry-run 2>&1)
assert_contains "marker comment present" "RECSYS_RETRAIN_JOB" "${OUT}"

echo ""
echo "=== Results: ${PASS} passed, ${FAIL} failed ==="
[[ "${FAIL}" == "0" ]]
