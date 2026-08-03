#!/usr/bin/env bash
# install-cron.sh — Install or remove the periodic retraining cron job.
#
# Usage:
#   ./install-cron.sh [--schedule "CRON_EXPR"] [--log PATH] [--uninstall] [--dry-run]
#
# Options:
#   --schedule EXPR   Cron schedule expression (default: "0 */6 * * *" = every 6 hours)
#   --log PATH        Log file for cron output (default: /var/log/recsys-retrain.log)
#   --uninstall       Remove the cron entry instead of installing it
#   --dry-run         Print what would change without modifying the crontab
#
# The script is idempotent: running it twice does not create duplicate entries.
# Identifies its entry by the RECSYS_RETRAIN_JOB marker comment.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RETRAIN_SCRIPT="${SCRIPT_DIR}/run-retrain.sh"
MARKER="# RECSYS_RETRAIN_JOB"
SCHEDULE="0 */6 * * *"
LOG_FILE="/var/log/recsys-retrain.log"
UNINSTALL=0
DRY_RUN=0

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --schedule)  SCHEDULE="$2"; shift 2 ;;
    --log)       LOG_FILE="$2"; shift 2 ;;
    --uninstall) UNINSTALL=1; shift ;;
    --dry-run)   DRY_RUN=1; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
current_crontab() {
  crontab -l 2>/dev/null || true
}

crontab_without_job() {
  current_crontab | grep -v "${MARKER}" | grep -v "run-retrain\.sh"
}

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ "${UNINSTALL}" == "0" && ! -f "${RETRAIN_SCRIPT}" ]]; then
  echo "ERROR: run-retrain.sh not found at ${RETRAIN_SCRIPT}" >&2
  echo "       Run this script from the recsys-pipeline/scripts/ directory." >&2
  exit 1
fi

# ── Uninstall ─────────────────────────────────────────────────────────────────
if [[ "${UNINSTALL}" == "1" ]]; then
  if ! current_crontab | grep -q "${MARKER}"; then
    echo "No recsys retrain cron job found — nothing to remove."
    exit 0
  fi
  echo "Removing recsys retrain cron job..."
  NEW_CRONTAB="$(crontab_without_job)"
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "DRY RUN: would write crontab:"
    echo "${NEW_CRONTAB}"
  else
    echo "${NEW_CRONTAB}" | crontab -
    echo "Done. Cron job removed."
  fi
  exit 0
fi

# ── Install ───────────────────────────────────────────────────────────────────
NEW_ENTRY="${SCHEDULE} cd $(dirname "${RETRAIN_SCRIPT}") && bash run-retrain.sh >> ${LOG_FILE} 2>&1 ${MARKER}"

if current_crontab | grep -q "${MARKER}"; then
  echo "Recsys retrain cron job already installed. Updating schedule..."
  EXISTING="$(current_crontab | grep "${MARKER}")"
  if [[ "${EXISTING}" == "${NEW_ENTRY}" ]]; then
    echo "Entry is already up to date:"
    echo "  ${EXISTING}"
    exit 0
  fi
  BASE="$(crontab_without_job)"
else
  echo "Installing recsys retrain cron job..."
  BASE="$(current_crontab)"
fi

NEW_CRONTAB="${BASE}
${NEW_ENTRY}"

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "DRY RUN: would add to crontab:"
  echo "  ${NEW_ENTRY}"
else
  echo "${NEW_CRONTAB}" | crontab -
  echo "Done. Cron job installed:"
  echo "  ${NEW_ENTRY}"
  echo ""
  echo "Tip: verify with 'crontab -l'"
fi
