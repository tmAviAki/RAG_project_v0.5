# Release Notes v1.2.0

This release adds a local RAG layer (no external dependencies by default) and semantic endpoints
for vector search and answering with citations. Code ingestion endpoints are stubbed behind a feature flag.

- Vector store: numpy-based cosine index under /index/faiss/ (swappable later).
- Embeddings: offline FakeEmbedder by default; optional OpenAI backend if ALLOW_REMOTE_EMBEDDINGS=1.
- Endpoints: /v1/semantic/search and /v1/answer (byte-capped).
- Code: /v1/code/file works; symbol/deps endpoints return 501 until enabled.

