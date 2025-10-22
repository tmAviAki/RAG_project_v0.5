# scripts/code_ingest.sh
#!/usr/bin/env bash
# Project: confAdogpt  Component: scripts  Version: v0.7.2
set -euo pipefail
CODE_ROOT="${CODE_ROOT:-/data/source-code}"
echo "[code_ingest] CODE_ROOT=$CODE_ROOT"
docker compose exec -T api python -u -m app.code_ingest --code-root "$CODE_ROOT"
