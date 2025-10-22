# Confluence Local Action API (Docker Compose)

This project exposes your **exported Confluence Data Center content** (pages, blogs, comments and attachments)
as a **local HTTP API** designed for Custom GPT Actions. It **does not** call Confluence;
it serves the content you already downloaded (15 GB in your case).

- **No auth** by default (sits on your LAN). Protect behind a reverse proxy if needed.
- **Chunk-size aware** responses: endpoints have `chunk_bytes` (defaults to 90 KB) so payloads stay under action-connector limits.
- **Two modes** for retrieval:
  - Standard JSON paging (recommended for ChatGPT Actions).
  - Optional NDJSON streaming for local tools.
- **Attachments** are served directly from your exported folder via `/attachments/...` URLs.

> Place your exported tree (the one with `attachments/` and `spaces/`) in `./export_data` before starting.

## Quick start

```bash
# 1) Put your exported data under ./export_data
#    Tree example:
#    export_data/
#      attachments/
#      spaces/
#      meta/probe.json

# 2) Build & start
docker compose up --build -d

# 3) (first run) Build the index (takes a few minutes; safe to rerun anytime)
docker compose run --rm ingest

# 4) Use the API
curl 'http://localhost:8000/v1/health'
curl 'http://localhost:8000/v1/stats'
curl 'http://localhost:8000/v1/search?q=NPVR&limit=10'
curl 'http://localhost:8000/v1/attachments/by-content/59705580'
# open in browser
open http://localhost:8000/docs
```

## Service layout

- **API**: FastAPI/uvicorn serving:
  - `GET /v1/health` – liveness.
  - `GET /v1/stats` – documents & attachments counts.
  - `GET /v1/spaces` – list spaces.
  - `GET /v1/search` – FTS search with **byte-capped** pages (`chunk_bytes` param; defaults to 90 KB). Returns `items[]` and `next` cursor.
  - `GET /v1/fetch` – fetch full content by IDs, chunked just like `/v1/search`.
  - `GET /v1/attachments/by-content/{content_id}` – list attachment files for a page/blog with public URLs.
  - `GET /attachments/...` – static files mapped to your `./export_data/attachments` folder.
  - `GET /v1/stream/search` – NDJSON streaming (human tools), not used by Actions.

- **Ingest** (one-off): builds a local **SQLite FTS5** index from your `spaces/*/*.ndjson` and, when present, storage HTML. Large responses are **never** sent; we always page results by **approximate bytes**.

> The code avoids assumptions about your exporter’s file names. It will index from `page.ndjson`/`blogpost.ndjson` if present; otherwise it tries to locate storage files by content ID found in file names. Attachments are mapped by inferring the content ID from folder names like `..._<contentId>` under `attachments/` (as in your tree).

## Configuration

Environment variables (see `.env.example`):

- `DATA_ROOT=/data` – where the export is mounted inside the container
- `INDEX_PATH=/index/docs.db` – SQLite DB path
- `CHUNK_SIZE_BYTES=90000` – byte target for paged responses (kept under action connector 100 KB)
- `AUTO_INGEST=0` – if `1`, the API will try to ingest on startup when no index exists
- `ALLOW_ORIGINS=*` – CORS

## Compose

- `api` listens on `0.0.0.0:8000` and mounts your export and an `index/` folder for the DB.
- `ingest` reuses the same image to populate the index (safe to run multiple times).

## Notes and guardrails

- The API only reads **your local export**. It makes **no Confluence** calls.
- Attachment discovery uses the folder suffix convention `_<digits>` → content ID (e.g., `_59705580`). If some attachments don’t follow this convention, you can still reach them via `/attachments/...` paths.
- Responses are paged by approximate **on-the-wire** size. For Actions, prefer `/v1/search` and `/v1/fetch` (JSON). The streaming endpoint is intended for local tooling only.

---

© 2025 • Local-only utility for your dataset. No external services are required.
