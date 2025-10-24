# Release Notes v1.2.0

This release adds a local RAG layer (no external dependencies by default) and semantic endpoints
for vector search and answering with citations. Code ingestion endpoints are stubbed behind a feature flag.

- Vector store: numpy-based cosine index under /index/faiss/ (swappable later).
- Embeddings: offline FakeEmbedder by default; optional OpenAI backend if ALLOW_REMOTE_EMBEDDINGS=1.
- Endpoints: /v1/semantic/search and /v1/answer (byte-capped).
- Code: /v1/code/file works; symbol/deps endpoints return 501 until enabled.
### v0.6.1 (20251024)
P1 stabilization for Actions: 3.1.1 spec; remove HEAD; query analyzer; search knobs; release targets in Makefile.
### v0.7.0 (2025-10-24)
P2 Hybrid retrieval:
- Introduces Postgres+pgvector with HNSW index and BM25/tsvector clauses
- Adds hybrid search + code symbols + grep + graph neighbors endpoints
- Provides ingestion script and migration SQL; keeps existing SQLite path intact

