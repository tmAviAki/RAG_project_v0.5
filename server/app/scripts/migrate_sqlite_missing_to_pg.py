#!/usr/bin/env python3
# Project:RAG_project_v0.5  Component:migrate_sqlite_missing_to_pg (legacy-PG schema)  Version:v0.2.0
# Purpose:
#   Migrate documents that exist in legacy SQLite (/index/docs.db) but are missing
#   from the legacy Postgres P2 tables (`docs`, `doc_embeddings` with id + embedding_full/embedding_1536).
#   - No reliance on SQLite d.url (v0.5 doesn't have it) → synthesize uri as /v1/fetch?ids=<id>
#   - Writes to legacy PG tables:
#       docs(id TEXT PK, title TEXT, space TEXT, url TEXT, body TEXT, tsv tsvector)
#       doc_embeddings(id TEXT PK, embedding_full vector(3072), embedding_1536 vector(1536))
#   - Embeds one vector per document (on the combined title+body text).
#   - Idempotent: ON CONFLICT(id) DO UPDATE.
#
# Usage (inside a container that has network to PG and api.openai.com):
#   export PGHOST=pg PGPORT=5432 PGUSER=rag PGPASSWORD=fabrix PGDATABASE=rag
#   export SQLITE_PATH=/index/docs.db
#   export OPENAI_API_KEY=<your_key>
#   python /tmp/migrate_sqlite_missing_to_pg.py
#
# Tunables:
#   EMBEDDING_MODEL (default text-embedding-3-large)
#   EMBED_DIM       (3072 or 1536; chooses doc_embeddings column)
#   BATCH_EMBED     (default 128)
#   MAX_BODY_CHARS  (default 12000)  # to keep payloads reasonable

import os
import sys
import sqlite3
import hashlib
from typing import List, Tuple

import httpx
import psycopg
from psycopg.rows import dict_row


# ------------------------
# Environment / Defaults
# ------------------------
SQLITE_PATH   = os.getenv("SQLITE_PATH", "/index/docs.db")
PG_DSN        = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
    os.getenv("PGUSER", "rag"),
    os.getenv("PGPASSWORD", "fabrix"),
    os.getenv("PGHOST", "pg"),
    os.getenv("PGPORT", "5432"),
    os.getenv("PGDATABASE", "rag"),
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
EMBED_MODEL    = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
EMBED_DIM      = int(os.getenv("EMBED_DIM", "3072"))  # 3072 or 1536
BATCH_EMBED    = int(os.getenv("BATCH_EMBED", "128"))
MAX_BODY_CHARS = int(os.getenv("MAX_BODY_CHARS", "12000"))


# ------------------------
# Helpers
# ------------------------
def ensure_legacy_pg_schema(cur) -> None:
    """
    Create legacy P2 tables if they don't exist.
    This matches the P2 migration DDL used in your repo:
      - docs(id, title, space, url, body, tsv)
      - doc_embeddings(id, embedding_full vector(3072), embedding_1536 vector(1536))
    """
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS docs (
            id   TEXT PRIMARY KEY,
            title TEXT,
            space TEXT,
            url   TEXT,
            body  TEXT,
            tsv   tsvector
        )
        """
    )
    # try create doc_embeddings with both columns present; IF NOT EXISTS guards duplicate creation
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS doc_embeddings (
                id TEXT PRIMARY KEY REFERENCES docs(id) ON DELETE CASCADE,
                embedding_full  vector(3072),
                embedding_1536  vector(1536)
            )
            """
        )
    except Exception:
        pass


def chunk_for_embedding(title: str, body: str) -> str:
    """
    Build a single text payload per document.
    We keep it bounded by MAX_BODY_CHARS to avoid huge payloads.
    """
    t = (title or "").strip()
    b = (body or "").strip()
    combined = (t + ("\n\n" if t and b else "") + b).strip()
    if not combined:
        combined = t or b or ""
    if len(combined) > MAX_BODY_CHARS:
        combined = combined[:MAX_BODY_CHARS]
    return combined


def embed_texts(texts: List[str]) -> List[List[float]]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required")

    # Split into batches to respect size limits
    out: List[List[float]] = []
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    url = "https://api.openai.com/v1/embeddings"

    for i in range(0, len(texts), BATCH_EMBED):
        block = texts[i : i + BATCH_EMBED]
        payload = {"model": EMBED_MODEL, "input": block}
        with httpx.Client(timeout=60.0) as client:
            r = client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            vecs = [d["embedding"] for d in data.get("data", [])]
            if len(vecs) != len(block):
                raise RuntimeError(
                    f"Embedding response length mismatch: got {len(vecs)} for {len(block)} inputs"
                )
            out.extend(vecs)
    return out


def compute_missing_ids(sqlite_path: str, pg_dsn: str) -> Tuple[List[str], sqlite3.Cursor, psycopg.Cursor]:
    con = sqlite3.connect(sqlite_path)
    con.row_factory = sqlite3.Row
    cur_sql = con.cursor()

    # All SQLite doc ids
    sqlite_ids = {str(r["id"]) for r in cur_sql.execute("SELECT id FROM docs")}

    pg = psycopg.connect(pg_dsn, autocommit=True, row_factory=dict_row)
    cur_pg = pg.cursor()
    ensure_legacy_pg_schema(cur_pg)

    # All PG doc ids (legacy P2 docs)
    cur_pg.execute("SELECT id FROM docs")
    pg_ids = {str(row["id"]) for row in cur_pg.fetchall()}

    missing = sorted(sqlite_ids - pg_ids)
    print(f"[INFO] Missing docs to migrate: {len(missing)}")
    return missing, cur_sql, cur_pg


def upsert_docs_and_embeddings(
    ids: List[str], cur_sql: sqlite3.Cursor, cur_pg: psycopg.Cursor
) -> None:
    """
    For a block of SQLite doc ids:
      - Fetch (id, space, type, title, text)
      - Insert/Update PG 'docs' with url, body, tsv
      - Embed combined text and upsert PG 'doc_embeddings'
        - write to embedding_full (3072) or embedding_1536 (1536) depending on EMBED_DIM
    """
    if not ids:
        return

    marks = ",".join("?" * len(ids))
    rows = cur_sql.execute(
        f"""
        SELECT d.id, d.space, d.type, d.title, COALESCE(dt.text, '') AS text
        FROM docs d
        LEFT JOIN doc_texts dt ON dt.id = d.id
        WHERE d.id IN ({marks})
        """,
        ids,
    ).fetchall()

    # Prepare payloads for embeddings and docs upsert
    payloads: List[str] = []
    pg_ids: List[str] = []
    bodies: List[str] = []
    titles: List[str] = []
    spaces: List[str] = []

    for r in rows:
        _id = str(r["id"])
        sp  = (r["space"] or "").strip()
        ti  = r["title"] or ""
        tx  = r["text"] or ""
        body = tx
        payload = chunk_for_embedding(ti, tx)

        pg_ids.append(_id)
        titles.append(ti)
        spaces.append(sp)
        bodies.append(body)
        payloads.append(payload)

    # Upsert docs first (id, title, space, url, body, tsv)
    for _id, ti, sp, bo in zip(pg_ids, titles, spaces, bodies):
        url = f"/v1/fetch?ids={_id}"
        cur_pg.execute(
            """
            INSERT INTO docs (id, title, space, url, body, tsv)
            VALUES (%s, %s, %s, %s, %s, to_tsvector('english', %s || ' ' || %s))
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                space = EXCLUDED.space,
                url   = EXCLUDED.url,
                body  = EXCLUDED.body,
                tsv   = EXCLUDED.tsv
            """,
            (_id, ti, sp, url, bo, ti, bo),
        )

    # Embed and upsert into doc_embeddings (legacy: by id)
    vectors = embed_texts(payloads)
    if EMBED_DIM == 3072:
        for _id, vec in zip(pg_ids, vectors):
            cur_pg.execute(
                """
                INSERT INTO doc_embeddings (id, embedding_full)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    embedding_full = EXCLUDED.embedding_full
                """,
                (_id, vec),
            )
    elif EMBED_DIM == 1536:
        for _id, vec in zip(pg_ids, vectors):
            cur_pg.execute(
                """
                INSERT INTO doc_embeddings (id, embedding_1536)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    embedding_1536 = EXCLUDED.embedding_1536
                """,
                (_id, vec),
            )
    else:
        raise RuntimeError("EMBED_DIM must be 1536 or 3072 for legacy doc_embeddings schema")


def main() -> None:
    if not os.path.exists(SQLITE_PATH):
        print(f"[FATAL] SQLite file not found: {SQLITE_PATH}", file=sys.stderr)
        sys.exit(2)
    if not OPENAI_API_KEY:
        print("[FATAL] OPENAI_API_KEY not set", file=sys.stderr)
        sys.exit(2)

    missing, cur_sql, cur_pg = compute_missing_ids(SQLITE_PATH, PG_DSN)
    if not missing:
        print("[OK] Nothing to migrate.")
        return

    total = len(missing)
    step  = 256
    moved = 0

    for i in range(0, total, step):
        block = missing[i : i + step]
        try:
            upsert_docs_and_embeddings(block, cur_sql, cur_pg)
            moved += len(block)
            print(f"[OK] Migrated {moved}/{total}")
        except Exception as e:
            print(f"[ERROR] failed at block {i}:{i+len(block)} → {e}", file=sys.stderr)
            # continue with next block
            continue

    print("[DONE] Migration complete.")


if __name__ == "__main__":
    main()
