#!/usr/bin/env bash
# Project: confAdogpt  Component: full_ingest.sh  Version: v1.0.0
set -euo pipefail

# -------------------------
# Defaults (can be overridden via env or flags)
# -------------------------
CONF_BATCH="${CONF_BATCH:-300}"
ADO_BATCH="${ADO_BATCH:-300}"
OCR_LOW_BATCH="${OCR_LOW_BATCH:-300}"
OCR_HIGH_BATCH="${OCR_HIGH_BATCH:-300}"

FLUSH_EVERY="${FLUSH_EVERY:-400}"

OCR_DPI_LOW="${OCR_DPI_LOW:-50}"
OCR_DPI_HIGH="${OCR_DPI_HIGH:-120}"
OCR_JOBS="${OCR_JOBS:-0}"

# Spaces: if empty, we will auto-derive Confluence spaces (ALL minus ADO)
CONF_SPACES="${CONF_SPACES:-}"   # e.g. "OTT,QA,STORAGE"
ADO_SPACES="${ADO_SPACES:-ADO}"  # typically "ADO"

# Include flags (1=do, 0=skip)
DO_INDEX_DOCS="${DO_INDEX_DOCS:-1}"
DO_CONF_ATTACH_NO_OCR="${DO_CONF_ATTACH_NO_OCR:-1}"
DO_ADO_ATTACH_NO_OCR="${DO_ADO_ATTACH_NO_OCR:-1}"
DO_OCR_LOW="${DO_OCR_LOW:-1}"
DO_OCR_HIGH="${DO_OCR_HIGH:-1}"

# Log files
LOG_DIR="${LOG_DIR:-/index}"
LOG_INDEX="${LOG_INDEX:-${LOG_DIR}/ingest_index.log}"
LOG_CONF_NO_OCR="${LOG_CONF_NO_OCR:-${LOG_DIR}/attachments_ingest_conf_no_ocr.log}"
LOG_ADO_NO_OCR="${LOG_ADO_NO_OCR:-${LOG_DIR}/attachments_ingest_ado_no_ocr.log}"
LOG_OCR_LOW="${LOG_OCR_LOW:-${LOG_DIR}/attachments_ingest_ocr_low.log}"
LOG_OCR_HIGH="${LOG_OCR_HIGH:-${LOG_DIR}/attachments_ingest_ocr_high.log}"

# -------------------------
# Helpers
# -------------------------
usage() {
  cat <<EOF
Usage: scripts/full_ingest.sh [options]

Phases (default include all, in this order):
  1) Confluence pages/spaces (index docs)
  2) Confluence attachments (no OCR)
  3) ADO attachments (no OCR)
  4) Attachments OCR low DPI (Confluence + ADO)
  5) Attachments OCR high DPI (Confluence + ADO)

Options / env:
  --skip-index-docs                  Skip phase 1
  --skip-conf-no-ocr                 Skip phase 2
  --skip-ado-no-ocr                  Skip phase 3
  --skip-ocr-low                     Skip phase 4
  --skip-ocr-high                    Skip phase 5

  --conf-spaces "S1,S2"              Explicit Confluence spaces (default: auto ALL minus ADO)
  --ado-spaces "ADO"                 ADO spaces (default: ADO)

  --conf-batch N                     Default: ${CONF_BATCH}
  --ado-batch N                      Default: ${ADO_BATCH}
  --ocr-low-batch N                  Default: ${OCR_LOW_BATCH}
  --ocr-high-batch N                 Default: ${OCR_HIGH_BATCH}
  --flush-every N                    Default: ${FLUSH_EVERY}

  --ocr-dpi-low N                    Default: ${OCR_DPI_LOW}
  --ocr-dpi-high N                   Default: ${OCR_DPI_HIGH}
  --ocr-jobs N                       Default: ${OCR_JOBS}

Examples:
  scripts/full_ingest.sh
  CONF_BATCH=200 FLUSH_EVERY=600 scripts/full_ingest.sh --skip-ocr-high
EOF
}

auto_conf_spaces() {
  # Derive ALL spaces minus ADO from the DB.
  docker compose exec -T api python - <<'PY'
import sqlite3, os
db="/index/docs.db"
con=sqlite3.connect(db); con.row_factory=sqlite3.Row
rows=con.execute("SELECT DISTINCT space FROM docs").fetchall()
spaces=sorted(set(r["space"] for r in rows if r["space"]!="ADO"))
print(",".join(spaces))
con.close()
PY
}

tail_log() {
  # Tail last 30 lines of a log if exists
  local f="$1"
  docker compose exec -T api sh -lc "[ -f \"$f\" ] && tail -n 30 \"$f\" || true"
}

# -------------------------
# Parse flags
# -------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --skip-index-docs) DO_INDEX_DOCS=0; shift ;;
    --skip-conf-no-ocr) DO_CONF_ATTACH_NO_OCR=0; shift ;;
    --skip-ado-no-ocr) DO_ADO_ATTACH_NO_OCR=0; shift ;;
    --skip-ocr-low) DO_OCR_LOW=0; shift ;;
    --skip-ocr-high) DO_OCR_HIGH=0; shift ;;
    --conf-spaces) CONF_SPACES="$2"; shift 2 ;;
    --ado-spaces) ADO_SPACES="$2"; shift 2 ;;
    --conf-batch) CONF_BATCH="$2"; shift 2 ;;
    --ado-batch) ADO_BATCH="$2"; shift 2 ;;
    --ocr-low-batch) OCR_LOW_BATCH="$2"; shift 2 ;;
    --ocr-high-batch) OCR_HIGH_BATCH="$2"; shift 2 ;;
    --flush-every) FLUSH_EVERY="$2"; shift 2 ;;
    --ocr-dpi-low) OCR_DPI_LOW="$2"; shift 2 ;;
    --ocr-dpi-high) OCR_DPI_HIGH="$2"; shift 2 ;;
    --ocr-jobs) OCR_JOBS="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

# Auto derive Confluence spaces (ALL minus ADO) if not provided
if [[ -z "${CONF_SPACES}" ]]; then
  echo "[INFO] Deriving Confluence spaces (ALL minus ADO)..."
  CONF_SPACES="$(auto_conf_spaces || true)"
  if [[ -z "${CONF_SPACES}" ]]; then
    echo "[WARN] Could not derive spaces; defaulting to ALL minus ADO not enforced. Using ALL (may include ADO)."
    CONF_SPACES="ALL"
  fi
fi

echo "[CFG] CONF_SPACES=${CONF_SPACES}"
echo "[CFG] ADO_SPACES=${ADO_SPACES}"
echo "[CFG] FLUSH_EVERY=${FLUSH_EVERY}"
echo "[CFG] CONF_BATCH=${CONF_BATCH} ADO_BATCH=${ADO_BATCH} OCR_LOW_BATCH=${OCR_LOW_BATCH} OCR_HIGH_BATCH=${OCR_HIGH_BATCH}"
echo "[CFG] OCR_DPI_LOW=${OCR_DPI_LOW} OCR_DPI_HIGH=${OCR_DPI_HIGH} OCR_JOBS=${OCR_JOBS}"
echo "[CFG] INCLUDE: index_docs=${DO_INDEX_DOCS} conf_no_ocr=${DO_CONF_ATTACH_NO_OCR} ado_no_ocr=${DO_ADO_ATTACH_NO_OCR} ocr_low=${DO_OCR_LOW} ocr_high=${DO_OCR_HIGH}"

# -------------------------
# Phase 1: Confluence pages/spaces
# -------------------------
if [[ "${DO_INDEX_DOCS}" == "1" ]]; then
  echo "[PHASE 1] Indexing Confluence spaces/pages (and ADO docs via indexer) ..."
  docker compose run --rm ingest 2>&1 | tee -a /tmp/ingest_index.stdout
  echo "[PHASE 1] DONE"
else
  echo "[PHASE 1] SKIPPED"
fi

# -------------------------
# Phase 2: Confluence attachments (NO OCR)
# -------------------------
if [[ "${DO_CONF_ATTACH_NO_OCR}" == "1" ]]; then
  echo "[PHASE 2] Confluence attachments (no OCR) ..."
  docker compose run --rm \
    -e PYTHONUNBUFFERED=1 \
    -e ATT_LOG_PATH="${LOG_CONF_NO_OCR}" \
    -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
    -e FLUSH_EVERY="${FLUSH_EVERY}" \
    api python -u -m app.attachments_ingest --spaces "${CONF_SPACES}" --batch "${CONF_BATCH}"
  echo "[PHASE 2] tail log:"
  tail_log "${LOG_CONF_NO_OCR}"
  echo "[PHASE 2] DONE"
else
  echo "[PHASE 2] SKIPPED"
fi

# -------------------------
# Phase 3: ADO attachments (NO OCR)
# -------------------------
if [[ "${DO_ADO_ATTACH_NO_OCR}" == "1" ]]; then
  echo "[PHASE 3] ADO attachments (no OCR) ..."
  docker compose run --rm \
    -e PYTHONUNBUFFERED=1 \
    -e ATT_LOG_PATH="${LOG_ADO_NO_OCR}" \
    -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
    -e FLUSH_EVERY="${FLUSH_EVERY}" \
    api python -u -m app.attachments_ingest --spaces "${ADO_SPACES}" --batch "${ADO_BATCH}"
  echo "[PHASE 3] tail log:"
  tail_log "${LOG_ADO_NO_OCR}"
  echo "[PHASE 3] DONE"
else
  echo "[PHASE 3] SKIPPED"
fi

# -------------------------
# Phase 4: OCR low DPI (Confluence + ADO)
# -------------------------
if [[ "${DO_OCR_LOW}" == "1" ]]; then
  echo "[PHASE 4] OCR low DPI (${OCR_DPI_LOW}) for ALL spaces (PDF-if-empty only) ..."
  docker compose run --rm \
    -e PYTHONUNBUFFERED=1 \
    -e ATT_LOG_PATH="${LOG_OCR_LOW}" \
    -e OCR_ENABLED=1 \
    -e OCR_PDF_IF_EMPTY=1 \
    -e OCR_IMAGES=0 \
    -e OCR_DPI="${OCR_DPI_LOW}" \
    -e OCR_JOBS="${OCR_JOBS}" \
    -e FLUSH_EVERY="${FLUSH_EVERY}" \
    api python -u -m app.attachments_ingest --spaces ALL --batch "${OCR_LOW_BATCH}"
  echo "[PHASE 4] tail log:"
  tail_log "${LOG_OCR_LOW}"
  echo "[PHASE 4] DONE"
else
  echo "[PHASE 4] SKIPPED"
endif

# -------------------------
# Phase 5: OCR higher DPI (Confluence + ADO)
# -------------------------
if [[ "${DO_OCR_HIGH}" == "1" ]]; then
  echo "[PHASE 5] OCR higher DPI (${OCR_DPI_HIGH}) for ALL spaces (PDF-if-empty + images) ..."
  docker compose run --rm \
    -e PYTHONUNBUFFERED=1 \
    -e ATT_LOG_PATH="${LOG_OCR_HIGH}" \
    -e OCR_ENABLED=1 \
    -e OCR_PDF_IF_EMPTY=1 \
    -e OCR_IMAGES=1 \
    -e OCR_DPI="${OCR_DPI_HIGH}" \
    -e OCR_JOBS="${OCR_JOBS}" \
    -e FLUSH_EVERY="${FLUSH_EVERY}" \
    api python -u -m app.attachments_ingest --spaces ALL --batch "${OCR_HIGH_BATCH}"
  echo "[PHASE 5] tail log:"
  tail_log "${LOG_OCR_HIGH}"
  echo "[PHASE 5] DONE"
else
  echo "[PHASE 5] SKIPPED"
fi

echo "[ALL PHASES] COMPLETE"
