#!/usr/bin/env bash
# Projects: confAdogpt  Component: load_project_vfrfp2025  Version: v0.1.0
set -euo pipefail

# ---- Inputs ----
SRC_ROOT="/mnt/disks/data/GPT-Data/Projects/VFRFP2025"
EDATA_ROOT="/mnt/disks/data/confgpt_action_server_docker_compose/export_data"
SPACE_KEY="VFRFP2025"
STUB_ID="99100001"
STUB_TITLE="VF RFP 2025 — Projects Corpus"

# ---- Validate inputs ----
if [[ ! -d "${SRC_ROOT}" ]]; then
  echo "[ERR] Missing project directory: ${SRC_ROOT}" >&2
  exit 2
fi

# ---- Prepare export_data structure ----
SPACE_DIR="${EDATA_ROOT}/spaces/${SPACE_KEY}"
ATT_DIR="${EDATA_ROOT}/attachments/${SPACE_KEY}/PROJECT_MASTER_${STUB_ID}"
mkdir -p "${SPACE_DIR}" "${ATT_DIR}"

# ---- Create stub page.ndjson ----
cat > "${SPACE_DIR}/page.ndjson" <<JSONL
{"id":"${STUB_ID}","title":"${STUB_TITLE}","type":"page"}
JSONL
echo "[OK] Wrote stub page: ${SPACE_DIR}/page.ndjson"

# ---- Mirror all project files ----
echo "[COPY] Mirroring ${SRC_ROOT}/ -> ${ATT_DIR}/"
rsync -a --delete --info=progress2 "${SRC_ROOT}/" "${ATT_DIR}/"

# ---- OOXML expand: unzip xlsx/docx/pptx for indexing ----
echo "[OOXML] Expanding .xlsx/.docx/.pptx into .ooxml folders"
find "${ATT_DIR}" -type f \( -iname '*.xlsx' -o -iname '*.docx' -o -iname '*.pptx' \) | while read -r f; do
  tgt="${f}.ooxml"
  rm -rf "$tgt"
  mkdir -p "$tgt"
  unzip -qq -n "$f" -d "$tgt" || true
  # drop heavy media/ to avoid bloat
  rm -rf "$tgt/media" "$tgt/word/media" "$tgt/ppt/media" "$tgt/xl/media"
done

# ---- Summary ----
echo "[SUMMARY]"
echo "  Space:        ${SPACE_KEY}"
echo "  Stub ID:      ${STUB_ID}"
echo "  page.ndjson:  ${SPACE_DIR}/page.ndjson"
echo "  Attach root:  ${ATT_DIR}"
echo "  Total files:  $(find "${ATT_DIR}" -type f | wc -l | tr -d ' ')"
echo "  OOXML expanded: $(find "${ATT_DIR}" -type d -name '*.ooxml' | wc -l)"
echo "  Disk usage:   $(du -sh "${ATT_DIR}" | awk '{print $1}')"

echo "[DONE] Projects VFRFP2025 staged for indexing. Next run ingest + att_ingest."
