#!/usr/bin/env bash
# Project: confAdogpt  Component: load_vf_rfp  Version: v0.2.0
# Purpose: Stage VF RFP corpus into export_data and pre-unzip OOXML (xlsx/docx/pptx)
#          so XML payloads are directly ingestible; preserve a link to originals.

set -euo pipefail

# --------- Inputs (HOST paths) ----------
# Source tree containing the RFP documents (your exported directory)
SRC_ROOT="/mnt/disks/data/VFRFP"

# The *confgpt* export_data root actually mounted into the API container as /data (read-only)
EDATA_ROOT="/mnt/disks/data/confgpt_action_server_docker_compose/export_data"

# Your project (compose) root (for the "next steps" commands)
PROJECT_ROOT="/mnt/disks/data/confAdogpt_action_server_docker_compose"

# New logical space key to isolate this corpus
SPACE_KEY="RFP"

# A synthetic page to anchor all attachments (so attachments JOIN cleanly)
STUB_ID="99000001"
STUB_TITLE="VF PT RFP — Master Corpus"

# --------- Derived destinations ----------
SPACE_DIR="${EDATA_ROOT}/spaces/${SPACE_KEY}"
ATT_DST_DIR="${EDATA_ROOT}/attachments/${SPACE_KEY}/RFP_MASTER_${STUB_ID}"
PAGE_NDJSON="${SPACE_DIR}/page.ndjson"

# --------- Validate inputs ----------
if [[ ! -d "${SRC_ROOT}" ]]; then
  echo "[ERR] Missing source directory: ${SRC_ROOT}" >&2
  exit 2
fi
if [[ ! -d "${EDATA_ROOT}" ]]; then
  echo "[ERR] export_data root not found: ${EDATA_ROOT}" >&2
  exit 2
fi

# --------- Prepare export_data structure ----------
mkdir -p "${SPACE_DIR}" "${ATT_DST_DIR}"

# Minimal page.ndjson so indexer creates a doc row (id/title/type). No storage HTML is needed here.
cat > "${PAGE_NDJSON}" <<JSONL
{"id":"${STUB_ID}","title":"${STUB_TITLE}","type":"page"}
JSONL

echo "[OK] Wrote stub page: ${PAGE_NDJSON}"

# --------- Rsync the entire RFP tree under the stub's attachment dir ----------
# We preserve relative structure; subsequent tooling expects plain files under attachments/<SPACE>/.../<name>
echo "[COPY] Mirroring ${SRC_ROOT}/  →  ${ATT_DST_DIR}/"
rsync -a --delete --info=progress2 "${SRC_ROOT}/" "${ATT_DST_DIR}/"

# --------- OOXML pre-unzip (xlsx/docx/pptx) ----------
# For each OOXML file, make a "<file>.ooxml" sibling directory and unzip there.
# We drop large binaries (media/) to reduce noise; XML & rels stay as text inputs.
echo "[OOXML] Expanding .xlsx/.docx/.pptx into .ooxml folders (removing media/)"
shopt -s nullglob
ooxml_count=0
while IFS= read -r -d '' f; do
  out="${f}.ooxml"
  mkdir -p "${out}"
  # Unzip quietly; overwrite if re-run
  if unzip -qq -o -- "${f}" -d "${out}"; then
    # Remove binary media payloads; keep XML/text content
    rm -rf "${out}/word/media" "${out}/xl/media" "${out}/ppt/media" 2>/dev/null || true
    # Link back to original
    printf '{ "source": "%s" }\n' "$(realpath --relative-to="${ATT_DST_DIR}" "${f}")" > "${out}/_source.json"
    ((ooxml_count+=1))
  else
    echo "[WARN] unzip failed for: ${f}" >&2
    rm -rf "${out}" || true
  fi
done < <(find "${ATT_DST_DIR}" -type f \( -iname "*.xlsx" -o -iname "*.docx" -o -iname "*.pptx" \) -print0)
shopt -u nullglob

# --------- Summary ----------
files_total=$(find "${ATT_DST_DIR}" -type f | wc -l | tr -d '[:space:]')
xml_total=$(find "${ATT_DST_DIR}" -type f \( -iname "*.xml" -o -iname "*.rels" -o -iname "*.txt" -o -iname "*.m3u8" -o -iname "*.mpd" \) | wc -l | tr -d '[:space:]')
size_human=$(du -sh "${ATT_DST_DIR}" | awk '{print $1}')

echo
echo "[SUMMARY]"
echo "  Space:            ${SPACE_KEY}"
echo "  Stub page id:     ${STUB_ID}"
echo "  page.ndjson:      ${PAGE_NDJSON}"
echo "  Attach root:      ${ATT_DST_DIR}"
echo "  Total files:      ${files_total}"
echo "  OOXML expanded:   ${ooxml_count} (created *.ooxml directories)"
echo "  Text/XML files:   ${xml_total}"
echo "  Disk usage:       ${size_human}"
echo

# --------- Next steps: index + embed ----------
cat <<'CMD'

# 1) Build/refresh the SQLite index so the stub doc + attachments are registered:
docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml run --rm ingest

# 2) Embed attachments for this space (no OCR first pass; adjust as desired):
docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml run --rm \
  -e ATT_LOG_PATH=/index/attachments_ingest_rfp_round1.log \
  -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
  att_ingest python -u -m app.attachments_ingest --spaces RFP --batch 300

# (Optional) 3) Low-DPI OCR pass only for previously empty PDFs/images (if you want text from scanned docs):
docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml run --rm \
  -e ATT_LOG_PATH=/index/attachments_ingest_rfp_round2_ocr.log \
  -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=1 -e OCR_DPI=50 \
  att_ingest python -u -m app.attachments_ingest --spaces RFP --batch 200

# (Optional) 4) Verify vector/meta alignment:
docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml exec -T api \
  python - <<'PY'
import numpy as np, os, json
v="/index/faiss/vectors.npy"; m="/index/faiss/meta.jsonl"
nv=int(np.load(v, mmap_mode="r").shape[0]) if os.path.exists(v) else 0
nm=sum(1 for _ in open(m,"r",encoding="utf-8")) if os.path.exists(m) else 0
print({"vectors":nv,"meta_lines":nm,"aligned":nv==nm})
PY

# (Optional) 5) Sample RFP hits from the vector store:
docker compose -f /mnt/disks/data/confAdogpt_action_server_docker_compose/docker-compose.yml exec -T api \
  sh -lc 'jq -r "select(.space==\"RFP\") | .id + \"\\t\" + .title" /index/faiss/meta.jsonl | head -20'
CMD

echo "[DONE] Staging complete."
