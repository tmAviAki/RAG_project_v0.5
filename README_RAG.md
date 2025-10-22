# RAG for Confluence + ADO

This adds a local vector store and endpoints for semantic search and answering with citations.

## Configure

- Default store: numpy-based cosine store under `/index/faiss/` (no FAISS dependency).
- Embeddings:
  - Default: offline FakeEmbedder (dim=64). For production, set `ALLOW_REMOTE_EMBEDDINGS=1` and `OPENAI_API_KEY`.

## Build embeddings index

```bash
# rebuild images
docker compose build --no-cache ingest api

# run RAG ingest (chunks, embed, upsert)
docker compose run --rm api python -m app.rag_ingest --since 1970-01-01T00:00:00Z --spaces ALL --batch 200

