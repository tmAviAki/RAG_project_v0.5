#!/usr/bin/env bash
# Project:Confluence Evidence API  Component:scripts.ingest_progress  Version:v1.0.0
set -euo pipefail

# Defaults
SERVICE="api"                 # docker compose service name for the API container
WINDOW=60                     # seconds to measure rate over
SPACES="ALL"                  # ALL or comma-separated list e.g. OTT,ADO,MT
BATCH=200                     # page size for dry-run chunking
TARGET_TOTAL=""               # if set, skip dry-run and use this number

usage() {
  cat <<USAGE
Usage: $0 [--window N] [--spaces ALL|Space1,Space2] [--batch N] [--target N]

Options:
  --window N     Sampling window seconds for rate (default: ${WINDOW})
  --spaces LIST  Space filter for target estimation; "ALL" or comma-separated (default: ${SPACES})
  --batch N      Batch size for dry-run chunking (default: ${BATCH})
  --target N     Skip dry-run and use this target_total directly (default: unset)
  -h,--help      Show this help

Examples:
  $0 --spaces ALL --window 60
  $0 --spaces OTT,ADO,MT --batch 300
  $0 --target 120000 --window 30
USAGE
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --window) WINDOW="${2:-}"; shift 2 ;;
    --spaces) SPACES="${2:-}"; shift 2 ;;
    --batch)  BATCH="${2:-}"; shift 2 ;;
    --target) TARGET_TOTAL="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

echo "== ingest_progress =="
echo "service       : ${SERVICE}"
echo "window(s)     : ${WINDOW}"
echo "spaces        : ${SPACES}"
echo "batch         : ${BATCH}"
[[ -n "${TARGET_TOTAL}" ]] && echo "target_total  : ${TARGET_TOTAL} (provided)"

# Helper: read current vectors row count
current_vectors() {
  docker compose exec -T "${SERVICE}" python - <<'PY'
import numpy as np, os
p="/index/faiss/vectors.npy"
print(int(np.load(p).shape[0]) if os.path.exists(p) else 0)
PY
}

# Helper: dry-run chunk counting in SQLite (no API)
dry_run_target() {
  # Uses same chunker + skip-threshold as ingest (text<200 skipped)
  docker compose exec -T "${SERVICE}" sh -lc "python - <<'PY'
import sys; sys.path.append('/app')
from app.repository import connect, search_docs, fetch_docs
from app.chunker_rag import iter_chunks
from app.config import settings

spaces_arg = '${SPACES}'
spaces = None if spaces_arg == 'ALL' else set(x.strip() for x in spaces_arg.split(',') if x.strip())

conn = connect(settings.index_path)
cursor=0; batch=${BATCH}; total=0
while True:
    rows = search_docs(conn, q='', space=None, doctype=None, limit=batch, offset=cursor)
    if not rows: break
    if spaces is not None:
        rows = [r for r in rows if r['space'] in spaces]
    ids = [r['id'] for r in rows]
    docs = fetch_docs(conn, ids)
    for d in docs:
        t = d.get('text') or ''
        if len(t) < 200:  # skip tiny docs (same as ingest)
            continue
        total += sum(1 for _ in iter_chunks(d))
    cursor += batch
print(total)
PY"
}

# 1) Determine target_total
if [[ -z "${TARGET_TOTAL}" ]]; then
  echo "Estimating target_total via dry-run chunking (no API calls)..."
  TARGET_TOTAL="$(dry_run_target)"
fi

# 2) Current total vectors
CURRENT="$(current_vectors)"
echo "target_total  : ${TARGET_TOTAL}"
echo "current_total : ${CURRENT}"

# 3) Sample rate over WINDOW seconds
echo "Sampling rate over ${WINDOW}s..."
S1="${CURRENT}"
sleep "${WINDOW}"
S2="$(current_vectors)"
RATE=$(( S2 - S1 ))
# guard against negative or zero
if [[ "${RATE}" -lt 0 ]]; then RATE=0; fi

# 4) Compute remaining and ETA
REMAINING=$(( TARGET_TOTAL - S2 ))
if [[ "${REMAINING}" -lt 0 ]]; then REMAINING=0; fi
if [[ "${RATE}" -le 0 ]]; then
  ETA="inf"
else
  ETA=$(( REMAINING / RATE ))
fi

echo "chunks_per_min : ${RATE}"
echo "remaining      : ${REMAINING}"
echo "ETA_minutes    : ${ETA}"
exit 0
