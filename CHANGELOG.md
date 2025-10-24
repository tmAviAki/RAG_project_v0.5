# Changelog

## v1.2.0
- Feature: RAG scaffolding (chunking, embeddings interface, local vector store).
- Endpoints: POST /v1/semantic/search, POST /v1/answer, code endpoints stubs.
- Tests: rag pipeline + semantic endpoints.
- Docs: README_RAG.md, README_CODE.md, OpenAPI addendum.
## v0.6.1 - 20251024
- Remove HEAD /v1/health (Actions compliance)
- OpenAPI 3.1.1 spec with single bearer scheme and schemas for all 200 responses
- Add identifier-aware query analyzer and lexical fallback groundwork
- Add search knobs: k (alias); chunk_bytes already supported

