#!/usr/bin/env bash
# Project: confAdogpt  Component: pipeline_run  Version: v0.2.0
set -euo pipefail

COMPOSE="/usr/bin/docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml"
LOG_DIR="/var/tmp"
STAMP="$(date +%F-%H%M%S)"
LOG="${LOG_DIR}/confadogpt_pipeline_${STAMP}.log"
RUN_ID="run_${STAMP}"

# --- Tunables (edit if needed) ---
ATT_BATCH=300
RAG_BATCH=200
RFP_SPACE="VFRFP2025"     # example project space; set to empty "" to skip special round
DO_CODE=0                 # 1 to include code_ingest service run
OCR_ROUNDS=("0" "1")      # round1 no OCR, round2 OCR-if-empty (+images)
# -------------------------------

PARENT_PID="$$"

# --- Self detach (first pass) ---
if [[ "${1:-}" != "--child" ]]; then
  echo "[INFO] Launching pipeline in background (log: ${LOG})"
  # shellcheck disable=SC2086
  nohup bash -lc "'$0' --child" > "${LOG}" 2>&1 < /dev/null &
  disown
  echo "[INFO] Started. Tail logs: tail -f ${LOG}"
  exit 0
fi

echo "[PIPE][${RUN_ID}] started at $(date -u +%FT%TZ)"
echo "[PIPE] compose file: /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml"
echo "[PIPE] log file: ${LOG}"

die() { echo "[FATAL] $*" >&2; exit 99; }

run() {
  echo "[RUN] $*"
  eval "$@"
}

# ---------- Verification helpers ----------
verify_sqlite_invariants() {
  local space="${1:-}"
  echo "[CHK] SQLite invariants (space='${space:-ALL}')"
  ${COMPOSE} exec -T api python - <<'PY'
import sqlite3, json, sys, os
db="/index/docs.db"
con=sqlite3.connect(db); con.row_factory=sqlite3.Row
cur=con.cursor()
def one(q,args=()):
    return int(cur.execute(q,args).fetchone()[0])
res={}
res["docs"]=one("SELECT COUNT(*) FROM docs")
res["doc_texts"]=one("SELECT COUNT(*) FROM doc_texts")
res["attachments"]=one("SELECT COUNT(*) FROM attachments")
res["orph_texts"]=one("SELECT COUNT(*) FROM doc_texts WHERE id NOT IN (SELECT id FROM docs)")
res["orph_att"]=one("SELECT COUNT(*) FROM attachments WHERE content_id NOT IN (SELECT id FROM docs)")
space=os.environ.get("CHK_SPACE","")
if space:
    res["space_docs"]=one("SELECT COUNT(*) FROM docs WHERE space=?", (space,))
    res["space_att"]=one("""
        SELECT COUNT(*) FROM attachments a
        JOIN docs d ON d.id=a.content_id WHERE d.space=?""",(space,))
res["ok"] = (res["orph_texts"]==0 and res["orph_att"]==0)
print(json.dumps(res, indent=2))
con.close()
if not res["ok"]:
    sys.exit(2)
PY
}

verify_vector_alignment() {
  echo "[CHK] Vector/meta alignment"
  ${COMPOSE} exec -T api python - <<'PY'
import numpy as np, os, json, sys
v="/index/faiss/vectors.npy"; m="/index/faiss/meta.jsonl"
nv=int(np.load(v, mmap_mode="r").shape[0]) if os.path.exists(v) else 0
nm=sum(1 for _ in open(m,"r",encoding="utf-8")) if os.path.exists(m) else 0
aligned = (nv == nm)
print(json.dumps({"vectors":nv,"meta_lines":nm,"aligned":aligned}, indent=2))
sys.exit(0 if aligned else 3)
PY
}

semantic_smoke() {
  local q="${1:-ping}"
  echo "[CHK] Semantic smoke (q='${q}')"
  ${COMPOSE} exec -T api python - <<PY
from app.rag_store import NumpyStore, VSConfig
from app.embeddings import get_embedder
ok=True; err=None; have=False
try:
    emb = get_embedder().embed_texts(["${q}"])[0]
    hits = NumpyStore(VSConfig()).search(emb, k=1, filters={})
    have = bool(hits)
except Exception as e:
    ok=False; err=str(e)
import json, sys
print(json.dumps({"ok": ok, "have_hit": have, "q": "${q}", "err": err}, indent=2))
sys.exit(0 if ok else 4)
PY
}

# ---------- Steps ----------
step_index_all() {
  echo "[STEP] Index (Confluence+Attachments+ADO)"
  run ${COMPOSE} run --rm ingest
  verify_sqlite_invariants ""
}

step_attachments_round() {
  local spaces="$1" ; local ocr="$2" ; local dpi="$3" ; local round_name="$4"
  echo "[STEP] Attachments ingest '${round_name}' spaces='${spaces}' OCR=${ocr} DPI=${dpi}"
  if [[ "${ocr}" == "1" ]]; then
    run ${COMPOSE} run --rm \
      -e ATT_LOG_PATH="/index/attachments_ingest_${round_name}.log" \
      -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=1 -e OCR_DPI="${dpi}" \
      att_ingest python -u -m app.attachments_ingest --spaces "${spaces}" --batch ${ATT_BATCH}
  else
    run ${COMPOSE} run --rm \
      -e ATT_LOG_PATH="/index/attachments_ingest_${round_name}.log" \
      -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
      att_ingest python -u -m app.attachments_ingest --spaces "${spaces}" --batch ${ATT_BATCH}
  fi
  verify_sqlite_invariants ""
  verify_vector_alignment
}

step_rag_docs() {
  local spaces="$1"
  echo "[STEP] RAG ingest documents spaces='${spaces}'"
  run ${COMPOSE} run --rm api python -u -m app.rag_ingest --spaces "${spaces}" --batch ${RAG_BATCH}
  verify_vector_alignment
}

step_code_ingest() {
  echo "[STEP] Code ingest (service code_ingest)"
  run ${COMPOSE} run --rm code_ingest
  verify_vector_alignment
}

step_xref() {
  echo "[STEP] xref build"
  run ${COMPOSE} run --rm xref_build
}

# ---------- Orchestration ----------
{
  echo "[PIPE] 1) Index all"
  step_index_all

  echo "[PIPE] 2) Attachments rounds (ALL spaces)"
  round=1
  for o in "${OCR_ROUNDS[@]}"; do
    step_attachments_round "ALL" "${o}" "50" "round${round}_ALL"
    round=$((round+1))
  done

  if [[ -n "${RFP_SPACE}" ]]; then
    echo "[PIPE] 3) Focused attachments rounds (${RFP_SPACE})"
    step_attachments_round "${RFP_SPACE}" "0" "50" "round_focus_noocr_${RFP_SPACE}"
    step_attachments_round "${RFP_SPACE}" "1" "50" "round_focus_ocr_${RFP_SPACE}"
  fi

  echo "[PIPE] 4) RAG doc bodies (ALL)"
  step_rag_docs "ALL"

  if [[ "${DO_CODE}" == "1" ]]; then
    echo "[PIPE] 5) Code ingest"
    step_code_ingest
  fi

  echo "[PIPE] 6) xref build"
  step_xref

  echo "[PIPE] 7) Final checks"
  verify_sqlite_invariants ""
  verify_vector_alignment
  semantic_smoke "ping"

  echo "[PIPE] DONE OK at $(date -u +%FT%TZ)"
} || {
  echo "[PIPE] FAILED at $(date -u +%FT%TZ)"
  exit 99
}
