#!/usr/bin/env bash
# Project: confAdogpt  Component: pipeline_run_inc  Version: v0.3.0
# Incremental, verified, self-detaching ingest pipeline

set -euo pipefail

COMPOSE="/usr/bin/docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml"
ROOT="/mnt/disks/data/confAdogpt_action_server_docker_compose"
LOG_DIR="/var/tmp"
STAMP="$(date +%F-%H%M%S)"
LOG="${LOG_DIR}/confadogpt_pipeline_${STAMP}.log"
STATE="/mnt/disks/data/confAdogpt_action_server_docker_compose/index/pipeline_state.json"
LOCK="/mnt/disks/data/confAdogpt_action_server_docker_compose/index/pipeline.lock"

# ---- knobs (safe defaults; all incremental) ----
ATT_BATCH=300
RAG_BATCH=200
OCR_ROUNDS=("0")           # default: no OCR pass; add "1" for OCR-if-empty round
FOCUS_SPACES=()            # e.g. ("VFRFP2025") for focused attachment rounds
DO_CODE=0                  # toggle code_ingest
SMOKE_Q="ping"

# ---- self-detach ----
if [[ "${1:-}" != "--child" ]]; then
  echo "[INFO] Background pipeline → ${LOG}"
  nohup bash -lc "'$0' --child" > "${LOG}" 2>&1 < /dev/null &
  disown
  echo "[INFO] tail -f ${LOG}"
  exit 0
fi

banner() { echo -e "\n[PIPE][$STAMP] $*\n"; }
die() { echo "[FATAL] $*" >&2; mark_failed; rm -f "${LOCK}" || true; exit 99; }

ensure_dirs() { mkdir -p "${LOG_DIR}" "${ROOT}/index" || true; }

# ------------ state helpers ------------
jqget() { jq -r "$1" 2>/dev/null || true; }
mark_failed() {
  local now; now="$(date -u +%FT%TZ)"
  tmp="$(mktemp)"; cat >"$tmp" <<JSON
{
  "last_run": "${now}",
  "last_result": "FAILED"
}
JSON
  mv "$tmp" "$STATE"
}
mark_ok() {
  local now; now="$(date -u +%FT%TZ)"; shift || true
  # merge keys passed on stdin (jq) with last_run/result=OK
  local tmp; tmp="$(mktemp)"
  jq -n --arg now "$now" '
    .last_run=$now | .last_result="OK"
  ' > "$tmp"
  mv "$tmp" "$STATE"
}

# ------------ locking ------------
acquire_lock() {
  if [[ -e "$LOCK" ]]; then
    echo "[WARN] Lock exists ($LOCK). If no pipeline is running, remove it."
    exit 1
  fi
  echo "$STAMP $$" > "$LOCK"
}
release_lock() { rm -f "$LOCK" || true; }

# ------------ digests (incremental gating) ------------
digest_spaces() {
  # Mix page ndjson + page_storage mtimes/sizes (stable & cheap)
  find /mnt/disks/data/confgpt_action_server_docker_compose/export_data/spaces \
       -type f \( -name 'page.ndjson' -o -path '*/page_storage/*' \) \
       -printf '%P:%s:%T@\n' 2>/dev/null | sort | sha256sum | awk '{print $1}'
}
digest_attachments() {
  find /mnt/disks/data/confgpt_action_server_docker_compose/export_data/attachments \
       -type f -printf '%P:%s:%T@\n' 2>/dev/null | sort | sha256sum | awk '{print $1}'
}

# ------------ verifications ------------
verify_sqlite_invariants() {
  local scope_space="${1:-}"
  banner "Verify SQLite invariants (space=${scope_space:-ALL})"
  local code=0
  CHK_SPACE="$scope_space" ${COMPOSE} exec -T api python - <<'PY' || code=$?
import sqlite3, json, os, sys
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
sys.exit(0 if res["ok"] else 2)
PY
  [[ $code -eq 0 ]] || die "SQLite invariants failed"
}
verify_vector_alignment() {
  banner "Verify vector/meta alignment"
  local code=0
  ${COMPOSE} exec -T api python - <<'PY' || code=$?
import numpy as np, os, json, sys
v="/index/faiss/vectors.npy"; m="/index/faiss/meta.jsonl"
nv=int(np.load(v, mmap_mode="r").shape[0]) if os.path.exists(v) else 0
nm=sum(1 for _ in open(m,"r",encoding="utf-8")) if os.path.exists(m) else 0
aligned = (nv==nm)
print(json.dumps({"vectors":nv,"meta_lines":nm,"aligned":aligned}, indent=2))
sys.exit(0 if aligned else 3)
PY
  [[ $code -eq 0 ]] || die "Vectors/meta out of alignment"
}
semantic_smoke() {
  local q="${1:-ping}"
  banner "Semantic smoke (q=${q})"
  local code=0
  ${COMPOSE} exec -T api python - <<PY || code=$?
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
  [[ $code -eq 0 ]] || die "Semantic smoke failed"
}

# ------------ steps (incremental) ------------
step_index_all() {
  banner "STEP 1 — Ingest (Confluence + ADO)"
  local before="$(digest_spaces)"
  verify_sqlite_invariants ""  # pre-flight
  ${COMPOSE} run --rm ingest
  verify_sqlite_invariants ""  # post-flight
  local after="$(digest_spaces)"
  echo "[DBG] spaces digest before=${before} after=${after}"
  echo "{\"spaces_digest\":\"${after}\"}" > "${STATE}.tmp.spaces"
}

step_attachments_round() {
  local spaces="$1" ; local ocr="$2" ; local round_tag="$3"
  banner "STEP 2 — Attachments (spaces=${spaces} ocr=${ocr} tag=${round_tag})"
  local before="$(digest_attachments)"
  verify_sqlite_invariants ""    # pre-flight

  if [[ "${ocr}" == "1" ]]; then
    ${COMPOSE} run --rm \
      -e ATT_LOG_PATH="/index/attachments_ingest_${round_tag}.log" \
      -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=1 -e OCR_DPI=50 \
      att_ingest python -u -m app.attachments_ingest --spaces "${spaces}" --batch ${ATT_BATCH}
  else
    ${COMPOSE} run --rm \
      -e ATT_LOG_PATH="/index/attachments_ingest_${round_tag}.log" \
      -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
      att_ingest python -u -m app.attachments_ingest --spaces "${spaces}" --batch ${ATT_BATCH}
  fi

  verify_sqlite_invariants ""    # post-flight
  verify_vector_alignment

  local after="$(digest_attachments)"
  echo "[DBG] attachments digest before=${before} after=${after}"
  echo "{\"attachments_digest\":\"${after}\"}" > "${STATE}.tmp.att"
}

step_rag_docs() {
  local spaces="$1"
  banner "STEP 3 — RAG (doc bodies) spaces=${spaces}"
  verify_sqlite_invariants ""
  ${COMPOSE} run --rm api python -u -m app.rag_ingest --spaces "${spaces}" --batch ${RAG_BATCH}
  verify_vector_alignment
}

step_code_ingest() {
  banner "STEP 4 — Code ingest"
  verify_vector_alignment
  ${COMPOSE} run --rm code_ingest
  verify_vector_alignment
}

step_xref() {
  banner "STEP 5 — XRef build"
  ${COMPOSE} run --rm xref_build
}

# ------------ incremental gating wrapper ------------
should_run_index() {
  local prev="$(jq -r '.spaces_digest // ""' "$STATE" 2>/dev/null || true)"
  local now="$(digest_spaces)"
  [[ "$prev" != "$now" ]]
}
should_run_attachments() {
  local prev="$(jq -r '.attachments_digest // ""' "$STATE" 2>/dev/null || true)"
  local now="$(digest_attachments)"
  [[ "$prev" != "$now" ]]
}

# ------------ pipeline ------------
ensure_dirs
acquire_lock
trap 'release_lock' EXIT

echo "[PIPE] State file: $STATE"
if [[ -f "$STATE" ]]; then
  last=$(jq -r '.last_result // "UNKNOWN"' "$STATE" 2>/dev/null || echo "UNKNOWN")
  [[ "$last" == "OK" ]] || echo "[WARN] Previous run result: $last"
fi

# Pre-flight invariants before any phase
verify_sqlite_invariants ""

# 1) index if needed
if should_run_index; then
  step_index_all
else
  echo "[SKIP] Ingest (spaces unchanged)"
fi

# 2) attachments if needed (global)
if should_run_attachments; then
  for o in "${OCR_ROUNDS[@]}"; do
    step_attachments_round "ALL" "$o" "all_round_ocr${o}"
  done
else
  echo "[SKIP] Attachments (trees unchanged)"
fi

# 2b) focused spaces (always safe to run; will be a no-op for cached files)
for sp in "${FOCUS_SPACES[@]}"; do
  for o in "${OCR_ROUNDS[@]}"; do
    step_attachments_round "${sp}" "$o" "focus_${sp}_ocr${o}"
  done
done

# 3) RAG (doc bodies) — cheap & idempotent; run when spaces changed, else skip
if should_run_index; then
  step_rag_docs "ALL"
else
  echo "[SKIP] RAG (spaces unchanged)"
fi

# 4) code (optional)
if [[ "$DO_CODE" == "1" ]]; then
  step_code_ingest
fi

# 5) xref always safe after embeddings
step_xref

# final checks + smoke
verify_sqlite_invariants ""
verify_vector_alignment
semantic_smoke "${SMOKE_Q}"

# merge temp digests into state & mark OK
jq -s 'reduce .[] as $x ({}; . * $x)' "${STATE}.tmp.spaces" "${STATE}.tmp.att" 2>/dev/null \
  | jq '. |= .' > "${STATE}.tmp.merged" || true
if [[ -s "${STATE}.tmp.merged" ]]; then
  mv "${STATE}.tmp.merged" "$STATE"
else
  : > "$STATE"
fi
mark_ok

rm -f "${STATE}.tmp.spaces" "${STATE}.tmp.att" 2>/dev/null || true
echo "[PIPE] DONE OK $(date -u +%FT%TZ)"
