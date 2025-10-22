#!/usr/bin/env bash
# Project: confAdogpt  Component: ingest_all  Version: v0.4.0
# One-command orchestrator for:
#   1) Confluence pages/spaces
#   2) Confluence attachments (no OCR)
#   3) ADO attachments (no OCR)
#   4) ALL attachments with low-DPI OCR (PDF if empty text; images off)
#   5) ALL attachments with higher-DPI OCR (PDF+images)
#
#   Safe guards:
#     - waits for container to finish
#     - checks atomic .tmp files are gone
#     - verifies FAISS alignment; auto-heals if needed
#     - per-phase logs into ./index
#
# Tunables via env (defaults shown):
#   BATCH=200
#   FLUSH_EVERY=150
#   OCR_DPI_LOW=50
#   OCR_DPI_HIGH=120
#   RUN_PAGES=1 RUN_CON_ATTACH=1 RUN_ADO_ATTACH=1 RUN_OCR_LOW=1 RUN_OCR_HIGH=1
#   COMPOSE_SERVICE=api
#   PY="python -u"
#   SPACES_CONFLUENCE=CONFLUENCE
#   SPACES_ADO=ADO
#   SPACES_ALL=ALL

set -euo pipefail

# -------------------- config --------------------
BATCH="${BATCH:-200}"
FLUSH_EVERY="${FLUSH_EVERY:-150}"
OCR_DPI_LOW="${OCR_DPI_LOW:-50}"
OCR_DPI_HIGH="${OCR_DPI_HIGH:-120}"

RUN_PAGES="${RUN_PAGES:-1}"
RUN_CON_ATTACH="${RUN_CON_ATTACH:-1}"
RUN_ADO_ATTACH="${RUN_ADO_ATTACH:-1}"
RUN_OCR_LOW="${RUN_OCR_LOW:-1}"
RUN_OCR_HIGH="${RUN_OCR_HIGH:-1}"

COMPOSE_SERVICE="${COMPOSE_SERVICE:-api}"
PY="${PY:-python -u}"

SPACES_CONFLUENCE="${SPACES_CONFLUENCE:-CONFLUENCE}"
SPACES_ADO="${SPACES_ADO:-ADO}"
SPACES_ALL="${SPACES_ALL:-ALL}"

VEC="./index/faiss/vectors.npy"
META="./index/faiss/meta.jsonl"

# -------------------- helpers --------------------
ts(){ date +"%Y-%m-%dT%H:%M:%S%z"; }

run_detached() {
  # $1: log path
  shift
  local log="$1"; shift
  echo "$(ts) [RUN] $*" | tee -a "$log"
  docker compose run -d --rm -e PYTHONUNBUFFERED=1 -e ATT_LOG_PATH="$log" $COMPOSE_SERVICE "$@"
}

wait_quiet() {
  # Wait until no attachments_ingest containers are running and no .tmp files exist.
  # Also wait for last "[RAG] flush done" after a "[RAG] flush start".
  # $1: log pattern glob (e.g., /index/attachments_ingest_*.log)
  local log_glob="$1"
  echo "$(ts) [WAIT] containers..." | tee -a ./index/ingest_all.log
  while docker ps --format '{{.Image}} {{.Command}}' | grep -q 'attachments_ingest' ; do
    sleep 2
  done
  echo "$(ts) [WAIT] atomic tmp files..." | tee -a ./index/ingest_all.log
  while docker compose exec -T "$COMPOSE_SERVICE" sh -lc 'ls ./index/faiss/vectors.npy.tmp ./index/faiss/meta.jsonl.tmp 2>/dev/null | wc -l' | grep -qv '^0$'; do
    sleep 1
  done

  echo "$(ts) [WAIT] flush done..." | tee -a ./index/ingest_all.log
  # give logs a moment to flush
  sleep 1
  # If a flush start is present without a following flush done, wait a bit
  local last_start last_done
  last_start=$(docker compose exec -T "$COMPOSE_SERVICE" sh -lc "grep -h '\[RAG\] flush start' $log_glob 2>/dev/null | tail -1 | wc -l" || true)
  last_done=$(docker compose exec -T "$COMPOSE_SERVICE" sh -lc "grep -h '\[RAG\] flush done' $log_glob 2>/dev/null | tail -1 | wc -l" || true)
  if [ "${last_start:-0}" = "1" ] && [ "${last_done:-0}" = "0" ]; then
    sleep 2
  fi
}

heal_alignment_if_needed() {
  # Checks vectors/meta sizes and heals if needed; returns 0 when aligned
  docker compose exec -T "$COMPOSE_SERVICE" $PY - <<'PY'
import os, json, numpy as np, sys, io
vecp="./index/faiss/vectors.npy"; metp="./index/faiss/meta.jsonl"
if not (os.path.exists(vecp) and os.path.exists(metp)):
    print("[HEAL] FAISS files missing; skipping.")
    sys.exit(0)

V = np.load(vecp)
nv = V.shape[0]
lines = []
with open(metp, "r", encoding="utf-8") as f:
    for ln in f:
        s = ln.strip()
        if not s: continue
        try: json.loads(s); lines.append(s)
        except json.JSONDecodeError: pass

nm = len(lines)
if nv == nm:
    print(f"[HEAL] aligned: vectors={nv} meta={nm}")
    sys.exit(0)

n = min(nv, nm)
print(f"[HEAL] fixing: vectors={nv} meta={nm} -> {n}")
# atomic rewrite
import tempfile, os
tv = vecp + ".tmp"; tm = metp + ".tmp"
with open(tv, "wb") as f: np.save(f, V[:n])
with open(tm, "w", encoding="utf-8") as f:
    f.write("\n".join(lines[:n]) + "\n")
os.replace(tv, vecp)
os.replace(tm, metp)
print("[HEAL] done.")
PY
}

show_alignment() {
  docker compose exec -T "$COMPOSE_SERVICE" $PY - <<'PY'
import numpy as np, os
v="./index/faiss/vectors.npy"; m="./index/faiss/meta.jsonl"
print("vectors:", int(np.load(v).shape[0]) if os.path.exists(v) else 0)
print("meta_lines:", sum(1 for _ in open(m,"r",encoding="utf-8")) if os.path.exists(m) else 0)
PY
}

# -------------------- phases --------------------
phase_pages() {
  local LOG="./index/ingest_confluence_pages.log"
  run_detached "./index/ingest_all.log" "$LOG" \
    $PY -m app.ingest_confluence --spaces ALL --batch "$BATCH"
  wait_quiet "$LOG"
  heal_alignment_if_needed
  show_alignment
}

phase_confluence_attachments_no_ocr() {
  local LOG="./index/ingest_confluence_attachments.log"
  run_detached "./index/ingest_all.log" "$LOG" \
    -e OCR_ENABLED=0 -e FLUSH_EVERY="$FLUSH_EVERY" \
    $PY -m app.attachments_ingest --spaces "$SPACES_CONFLUENCE" --batch "$BATCH"
  wait_quiet "$LOG"
  heal_alignment_if_needed
  show_alignment
}

phase_ado_attachments_no_ocr() {
  local LOG="./index/ingest_ado_attachments.log"
  run_detached "./index/ingest_all.log" "$LOG" \
    -e OCR_ENABLED=0 -e FLUSH_EVERY="$FLUSH_EVERY" \
    $PY -m app.attachments_ingest --spaces "$SPACES_ADO" --batch "$BATCH"
  wait_quiet "$LOG"
  heal_alignment_if_needed
  show_alignment
}

phase_all_lowdpi_ocr() {
  local LOG="./index/ingest_attachments_ocr_lowdpi.log"
  run_detached "./index/ingest_all.log" "$LOG" \
    -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=0 \
    -e OCR_DPI="$OCR_DPI_LOW" -e FLUSH_EVERY="$FLUSH_EVERY" \
    $PY -m app.attachments_ingest --spaces "$SPACES_ALL" --batch "$BATCH"
  wait_quiet "$LOG"
  heal_alignment_if_needed
  show_alignment
}

phase_all_highdpi_ocr() {
  local LOG="./index/ingest_attachments_ocr_highdpi.log"
  run_detached "./index/ingest_all.log" "$LOG" \
    -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=1 \
    -e OCR_DPI="$OCR_DPI_HIGH" -e FLUSH_EVERY="$FLUSH_EVERY" \
    $PY -m app.attachments_ingest --spaces "$SPACES_ALL" --batch "$BATCH"
  wait_quiet "$LOG"
  heal_alignment_if_needed
  show_alignment
}

# -------------------- main --------------------
echo "$(ts) [BEGIN] ingest_all (BATCH=$BATCH FLUSH_EVERY=$FLUSH_EVERY; DPI low=$OCR_DPI_LOW high=$OCR_DPI_HIGH)" | tee -a ./index/ingest_all.log

[ "$RUN_PAGES" = "1" ]         && phase_pages          || echo "$(ts) [SKIP] pages"
[ "$RUN_CON_ATTACH" = "1" ]    && phase_confluence_attachments_no_ocr || echo "$(ts) [SKIP] confluence attachments (no-ocr)"
[ "$RUN_ADO_ATTACH" = "1" ]    && phase_ado_attachments_no_ocr        || echo "$(ts) [SKIP] ado attachments (no-ocr)"
[ "$RUN_OCR_LOW" = "1" ]       && phase_all_lowdpi_ocr                || echo "$(ts) [SKIP] lowdpi ocr"
[ "$RUN_OCR_HIGH" = "1" ]      && phase_all_highdpi_ocr               || echo "$(ts) [SKIP] highdpi ocr"

echo "$(ts) [DONE] ingest_all" | tee -a ./index/ingest_all.log
