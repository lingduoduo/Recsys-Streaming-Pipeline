#!/usr/bin/env bash
# Re-drive recoverable dead-lettered events only to recsys_events.backfill.
#
# Deploy the catalog fix to BOTH the producer and the Spark job before running this.
# The eligibility gate proves a record decodes under the Python catalog; the pipeline
# decodes under the Scala one.
#
# Bounds are INGESTION dates (the dead-letter partition is kafka_timestamp), not the
# event-time bounds used by run-archive-replay.sh.
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

require_env REDRIVE_ARCHIVE_PATH
require_env REDRIVE_ARCHIVE_QUERY_NAMESPACE
require_env REDRIVE_OPERATION_ID
require_env REDRIVE_START_INGEST_DATE
require_env REDRIVE_END_INGEST_DATE
require_env REDRIVE_MAX_ROWS
require_env REDRIVE_RECORDS_PER_SECOND

args=(
  services/python-modeling/dead_letter_redrive.py
  --archive-path "${REDRIVE_ARCHIVE_PATH}"
  --archive-query-namespace "${REDRIVE_ARCHIVE_QUERY_NAMESPACE}"
  --operation-id "${REDRIVE_OPERATION_ID}"
  --start-ingest-date "${REDRIVE_START_INGEST_DATE}"
  --end-ingest-date "${REDRIVE_END_INGEST_DATE}"
  --max-rows "${REDRIVE_MAX_ROWS}"
  --records-per-second "${REDRIVE_RECORDS_PER_SECOND}"
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
)

if [[ -n "${REDRIVE_MANIFEST_DIR:-}" ]]; then
  args+=(--manifest-dir "${REDRIVE_MANIFEST_DIR}")
fi
if [[ "${REDRIVE_OVERRIDE_LIMIT:-0}" == "1" ]]; then
  args+=(--override-limit)
fi

exec python3 "${args[@]}"
