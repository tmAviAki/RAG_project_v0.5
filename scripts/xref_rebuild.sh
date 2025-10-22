# scripts/xref_rebuild.sh
#!/usr/bin/env bash
# Project: confAdogpt  Component: scripts  Version: v0.7.2
set -euo pipefail
echo "[xref] rebuilding edges from current meta.jsonl"
docker compose exec -T api python -u -m app.xref_build
