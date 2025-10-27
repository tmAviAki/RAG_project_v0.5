#!/usr/bin/env python3
# File: /tmp/embed_pg_docs_missing_vectors.py
# Project:RAG_project_v0.5  Component:embed_pg_docs_missing_vectors  Version:v0.1.0
import os, sys, time, random, sqlite3, httpx, psycopg
from psycopg.rows import dict_row

SQLITE_PATH = os.getenv("SQLITE_PATH", "/index/docs.db")
PG_DSN = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
    os.getenv("PGUSER","rag"),
    os.getenv("PGPASSWORD","fabrix"),
    os.getenv("PGHOST","pg"),
    os.getenv("PGPORT","5432"),
    os.getenv("PGDATABASE","rag"),
)
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
EMBED_MODEL = os.getenv("EMBEDDING_MODEL","text-embedding-3-large")
EMBED_DIM = int(os.getenv("EMBED_DIM","3072"))         # 3072 or 1536
BATCH = int(os.getenv("BATCH_EMBED","128"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES","6"))
BACKOFF_MIN = float(os.getenv("BACKOFF_BASE_MS","500"))/1000.0
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX_MS","4000"))/1000.0
MAX_BODY_CHARS = int(os.getenv("MAX_BODY_CHARS","12000"))

def combined(title: str, body: str) -> str:
    t = (title or "").strip(); b = (body or "").strip()
    s = (t + ("\n\n" if t and b else "") + b).strip()
    return s[:MAX_BODY_CHARS] if len(s) > MAX_BODY_CHARS else s

def embed_batch(texts):
    if not OPENAI_API_KEY: raise RuntimeError("OPENAI_API_KEY required")
    url = "https://api.openai.com/v1/embeddings"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    attempt = 0
    while True:
        try:
            r = httpx.post(url, headers=headers, json={"model": EMBED_MODEL, "input": texts}, timeout=120.0)
            r.raise_for_status()
            data = r.json()
            vecs = [d["embedding"] for d in data.get("data", [])]
            if len(vecs) != len(texts):
                raise RuntimeError(f"embedding response {len(vecs)} != inputs {len(texts)}")
            return vecs
        except Exception as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                raise
            back = min(BACKOFF_MAX, BACKOFF_MIN*(2**(attempt-1))) + random.uniform(0,0.25)
            print(f"[WARN] embed retry {attempt}/{MAX_RETRIES}: {e} (sleep {back:.2f}s)")
            time.sleep(back)

def main():
    # connect PG (legacy P2 schema)
    pg = psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)
    pcur = pg.cursor()
    # determine PG docs and doc_embeddings coverage
    pcur.execute("SELECT id FROM docs"); ids_pg = {str(r["id"]) for r in pcur.fetchall()}
    try:
        pcur.execute("SELECT id FROM doc_embeddings"); ids_vec = {str(r["id"]) for r in pcur.fetchall()}
    except Exception:
        ids_vec = set()
    targets = sorted(ids_pg - ids_vec)
    print(f"[PLAN] docs without vectors: {len(targets)}")
    if not targets:
        print("[OK] nothing to do")
        return

    # open SQLite to read the authoritative text bodies
    con = sqlite3.connect(SQLITE_PATH); con.row_factory = sqlite3.Row
    cur = con.cursor()

    total = len(targets); done = 0
    for i in range(0, total, BATCH):
        block = targets[i:i+BATCH]
        marks = ",".join("?"*len(block))
        rows = cur.execute(f"""
            SELECT d.id, d.title, COALESCE(dt.text,'') AS text
            FROM docs d LEFT JOIN doc_texts dt ON dt.id=d.id
            WHERE d.id IN ({marks})
        """, block).fetchall()
        # preserve order
        texts = [combined(r["title"] or "", r["text"] or "") for r in rows]
        vecs  = embed_batch(texts)

        if EMBED_DIM == 3072:
            sql = "INSERT INTO doc_embeddings (id, embedding_full) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_full=EXCLUDED.embedding_full"
        else:
            sql = "INSERT INTO doc_embeddings (id, embedding_1536) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_1536=EXCLUDED.embedding_1536"

        for r, v in zip(rows, vecs):
            pcur.execute(sql, (str(r["id"]), v))

        done += len(rows)
        print(f"[OK] embedded {done}/{total}")

    pcur.close(); pg.close(); cur.close(); con.close()
    print("[DONE] PG docs vector backfill complete")

if __name__ == "__main__":
    main()
