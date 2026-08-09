#!/usr/bin/env bash
# Replay an explicit, bounded archive range only to recsys_events.backfill.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${SCRIPT_DIR}"

require_env() {
  local variable="$1"
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 1
  fi
}

require_env REPLAY_ARCHIVE_PATH
require_env REPLAY_START_DATE
require_env REPLAY_END_DATE
require_env REPLAY_MAX_ROWS
require_env REPLAY_RECORDS_PER_SECOND

args=(
  services/python-modeling/archive_replay.py
  --archive-path "${REPLAY_ARCHIVE_PATH}"
  --start-date "${REPLAY_START_DATE}"
  --end-date "${REPLAY_END_DATE}"
  --max-rows "${REPLAY_MAX_ROWS}"
  --records-per-second "${REPLAY_RECORDS_PER_SECOND}"
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
)

if [[ -n "${REPLAY_MANIFEST_DIR:-}" ]]; then
  args+=(--manifest-dir "${REPLAY_MANIFEST_DIR}")
fi
if [[ "${REPLAY_OVERRIDE_LIMIT:-0}" == "1" ]]; then
  args+=(--override-limit)
fi

exec python3 "${args[@]}"
