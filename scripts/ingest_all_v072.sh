# scripts/ingest_all_v072.sh
#!/usr/bin/env bash
# Project: confAdogpt  Component: scripts  Version: v0.7.2
# One-shot supervised run: pages -> attachments (no OCR) -> images/PDF OCR (low DPI) -> code -> xref
set -euo pipefail

LOG_DIR="${LOG_DIR:-/index}"
OCR_DPI_LOW="${OCR_DPI_LOW:-50}"
OCR_DPI_HIGH="${OCR_DPI_HIGH:-150}"

echo "[ALL] step 1/5: Confluence/ADO pages are already in docs.db via indexer (run tools profile if needed)"
# docker compose run --rm ingest  # optional

echo "[ALL] step 2/5: attachments (no OCR), cached text respected"
docker compose run --rm \
  -e PYTHONUNBUFFERED=1 \
  -e ATT_LOG_PATH="$LOG_DIR/attachments_round1.log" \
  -e OCR_ENABLED=0 -e OCR_PDF_IF_EMPTY=0 -e OCR_IMAGES=0 \
  -e FLUSH_EVERY=250 \
  api python -u -m app.attachments_ingest --spaces ALL --batch 200

echo "[ALL] step 3/5: attachments OCR low DPI=${OCR_DPI_LOW}"
docker compose run --rm \
  -e PYTHONUNBUFFERED=1 \
  -e ATT_LOG_PATH="$LOG_DIR/attachments_round2_ocr${OCR_DPI_LOW}.log" \
  -e OCR_ENABLED=1 -e OCR_PDF_IF_EMPTY=1 -e OCR_IMAGES=1 \
  -e OCR_DPI="${OCR_DPI_LOW}" \
  -e FLUSH_EVERY=200 \
  api python -u -m app.attachments_ingest --spaces ALL --batch 200

echo "[ALL] step 4/5: source code ingestion (symbol-aware)"
bash scripts/code_ingest.sh

echo "[ALL] step 5/5: rebuild xref"
bash scripts/xref_rebuild.sh

echo "[ALL][DONE] confAdogpt v0.7.2 pipeline completed."
