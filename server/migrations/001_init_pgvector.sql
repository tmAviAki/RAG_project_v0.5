-- Project:RAG_project_v0.5 Component:migrations Version:v0.7.8
-- Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- Lexical docs (BM25/tsvector)
CREATE TABLE IF NOT EXISTS docs (
  id TEXT PRIMARY KEY,
  title TEXT,
  space TEXT,
  url TEXT,
  body TEXT,
  tsv tsvector
);
CREATE INDEX IF NOT EXISTS docs_tsv_idx ON docs USING GIN (tsv);

-- Dual-embedding storage:
--  - embedding_full: 3072-d (high-fidelity, not indexed)
--  - embedding_1536: 1536-d (ANN-indexed)
DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema='public' AND table_name='doc_embeddings'
  ) THEN
    CREATE TABLE doc_embeddings (
      id TEXT PRIMARY KEY REFERENCES docs(id) ON DELETE CASCADE,
      embedding_full vector(3072),
      embedding_1536 vector(1536)
    );
  ELSE
    -- Table exists. Ensure both columns are present and with correct types.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='doc_embeddings' AND column_name='embedding_full'
    ) THEN
      ALTER TABLE doc_embeddings ADD COLUMN embedding_full vector(3072);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='doc_embeddings' AND column_name='embedding_1536'
    ) THEN
      ALTER TABLE doc_embeddings ADD COLUMN embedding_1536 vector(1536);
    END IF;
  END IF;
END$$;

-- Drop any legacy indexes
DROP INDEX IF EXISTS doc_embeddings_hnsw;
DROP INDEX IF EXISTS doc_embeddings_ivfflat;

-- ANN index on 1536-d projection (L2). Tune lists as needed.
CREATE INDEX IF NOT EXISTS doc_embeddings_ivfflat
  ON doc_embeddings
  USING ivfflat (embedding_1536 vector_l2_ops)
  WITH (lists = 100);

-- Code symbol storage (lexical/code windows)
CREATE TABLE IF NOT EXISTS code_symbols (
  id BIGSERIAL PRIMARY KEY,
  path TEXT NOT NULL,
  lang TEXT,
  symbol TEXT,
  kind TEXT,
  start_line INTEGER,
  end_line INTEGER,
  content TEXT,
  tsv tsvector
);
CREATE INDEX IF NOT EXISTS code_symbols_tsv_idx ON code_symbols USING GIN (tsv);

-- Simple graph edges (code/doc links)
CREATE TABLE IF NOT EXISTS graph_edges (
  src TEXT,
  dst TEXT,
  kind TEXT,
  weight DOUBLE PRECISION
);
