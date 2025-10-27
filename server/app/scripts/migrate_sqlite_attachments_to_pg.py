#!/usr/bin/env python3
# Project:RAG_project_v0.5  Component:migrate_sqlite_attachments_to_pg_legacy_fast  Version:v0.4.1
# FAST + NUL-SAFE: backfill TEXT-LIKE attachments into legacy P2 schema:
#   docs(id TEXT PK, title TEXT, space TEXT, url TEXT, body TEXT, tsv tsvector)
#   doc_embeddings(id TEXT PK, embedding_full vector(3072), embedding_1536 vector(1536))
#
# Env (typical):
#   export PGHOST=pg PGPORT=5432 PGUSER=rag PGPASSWORD=fabrix PGDATABASE=rag
#   export SQLITE_PATH=/index/docs.db
#   export DATA_ROOT=/data
#   export ADO_ROOT=/ado
#   export OPENAI_API_KEY=***    EMBEDDING_MODEL=text-embedding-3-large  EMBED_DIM=3072
#   Optional: EMBED_CONCURRENCY=6 BATCH_EMBED=128 FILE_READERS=12 PG_BATCH=1000 FORCE=0 HTTP2=0
#
from __future__ import annotations
import os, sys, time, json, random, sqlite3, hashlib
from pathlib import Path
from typing import List, Tuple, Optional
import concurrent.futures as cf
import asyncio

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.errors import DataError

# ---------------- ENV ----------------
SQLITE_PATH   = os.getenv("SQLITE_PATH", "/index/docs.db")
DATA_ROOT     = os.getenv("DATA_ROOT", "/data")
ADO_ROOT      = os.getenv("ADO_ROOT", "/ado")
PG_DSN        = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
    os.getenv("PGUSER","rag"),
    os.getenv("PGPASSWORD","fabrix"),
    os.getenv("PGHOST","pg"),
    os.getenv("PGPORT","5432"),
    os.getenv("PGDATABASE","rag"),
)
OPENAI_API_KEY  = (os.getenv("OPENAI_API_KEY") or "").strip()
EMBED_MODEL     = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
EMBED_DIM       = int(os.getenv("EMBED_DIM", "3072"))  # 3072 or 1536
ATT_MAX_CHARS   = int(os.getenv("ATT_MAX_CHARS", "12000"))
ATT_MIN_TEXT_CHARS = int(os.getenv("ATT_MIN_TEXT_CHARS", "40"))
BATCH_EMBED     = int(os.getenv("BATCH_EMBED", "128"))
EMBED_CONCURRENCY = int(os.getenv("EMBED_CONCURRENCY", "6"))
FILE_READERS    = int(os.getenv("FILE_READERS", "12"))
PG_BATCH        = int(os.getenv("PG_BATCH", "1000"))
MAX_RETRIES     = int(os.getenv("MAX_RETRIES", "6"))
BACKOFF_MIN_S   = float(os.getenv("BACKOFF_BASE_MS", "500"))/1000.0
BACKOFF_MAX_S   = float(os.getenv("BACKOFF_MAX_MS", "4000"))/1000.0
HTTP2           = os.getenv("HTTP2", "0").strip().lower() in ("1","true","yes","on")
FORCE           = os.getenv("FORCE", "0").strip().lower() in ("1","true","yes","on")

TEXT_EXT = {
    ".txt",".log",".md",".markdown",".json",".yaml",".yml",".xml",
    ".cfg",".ini",".conf",".py",".c",".cpp",".h",".hpp",".sql",
    ".csv",".tsv",".html",".htm",".properties",".sh",".bash",".zsh",
}

# -------------- SCHEMA ---------------
def ensure_legacy_schema(cur) -> None:
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute("""
      CREATE TABLE IF NOT EXISTS docs (
        id    TEXT PRIMARY KEY,
        title TEXT,
        space TEXT,
        url   TEXT,
        body  TEXT,
        tsv   tsvector
      )
    """)
    try:
        cur.execute("""
          CREATE TABLE IF NOT EXISTS doc_embeddings (
            id TEXT PRIMARY KEY REFERENCES docs(id) ON DELETE CASCADE,
            embedding_full  vector(3072),
            embedding_1536  vector(1536)
          )
        """)
    except Exception:
        pass

# -------------- HELPERS --------------
def sanitize_text(s: str) -> str:
    """
    Remove NULs and control chars (except tab/newline), normalize newlines,
    trim length. This prevents 'PostgreSQL text fields cannot contain NUL'.
    """
    if not s:
        return ""
    # remove NULs fast
    if "\x00" in s:
        s = s.replace("\x00", "")
    # strip other control chars (< 0x20) except \n and \t
    s = "".join(ch if (ord(ch) >= 32 or ch in ("\n","\t")) else " " for ch in s)
    # normalize newlines and trim
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(s) > ATT_MAX_CHARS:
        s = s[:ATT_MAX_CHARS]
    return s

def att_space(rel: str) -> str:
    if rel.startswith("ADO/"):
        return "ADO"
    seg = (rel.split("/", 1)[0] or "").strip()
    return seg or "ATTACH"

def resolve_path(rel: str) -> Path:
    if rel.startswith("ADO/"):
        return Path(ADO_ROOT) / "attachments" / rel[len("ADO/"):]
    return Path(DATA_ROOT) / "attachments" / rel

def load_existing_vec_ids(cur) -> set[str]:
    try:
        cur.execute("SELECT id FROM doc_embeddings")
        return {str(r["id"]) for r in cur.fetchall()}
    except Exception:
        return set()

def combined_text(title: str, body: str) -> str:
    return sanitize_text(((title or "").strip() + ("\n\n" if title and body else "") + (body or "").strip()))

# ---------- PG UPSERT (batched, NUL-safe) ----------
def upsert_docs_batched(cur, rows: List[Tuple[str,str,str,str,str]]) -> None:
    # rows: (id, title, space, url, body)   -> body/title are already sanitized
    if not rows: return
    sql = """
      INSERT INTO docs (id, title, space, url, body, tsv)
      VALUES (%s,%s,%s,%s,%s, to_tsvector('english', %s || ' ' || %s))
      ON CONFLICT (id) DO UPDATE SET
        title=EXCLUDED.title, space=EXCLUDED.space, url=EXCLUDED.url, body=EXCLUDED.body, tsv=EXCLUDED.tsv
    """
    data = [(i, t, s, u, b, t, b) for (i,t,s,u,b) in rows]
    # extra guard: retry with harder sanitize if PG rejects a row
    try:
        cur.executemany(sql, data)
    except DataError as e:
        # perform per-row fallback
        for i, t, s, u, b, tt, bb in data:
            try:
                cur.execute(sql, (i, t, s, u, b, tt, bb))
            except DataError:
                t2 = sanitize_text(t)
                b2 = sanitize_text(b)
                cur.execute(sql, (i, t2, s, u, b2, t2, b2))

def upsert_vecs_batched(cur, ids: List[str], vecs: List[List[float]]) -> None:
    if not ids: return
    if EMBED_DIM == 3072:
        sql = "INSERT INTO doc_embeddings (id, embedding_full) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_full=EXCLUDED.embedding_full"
    else:
        sql = "INSERT INTO doc_embeddings (id, embedding_1536) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_1536=EXCLUDED.embedding_1536"
    cur.executemany(sql, list(zip(ids, vecs)))

# ---------- EMBEDDINGS (async) ----------
async def embed_block(client: httpx.AsyncClient, texts: List[str]) -> List[List[float]]:
    r = await client.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={"model": EMBED_MODEL, "input": texts}
    )
    if r.status_code in (400, 413):
        raise httpx.HTTPStatusError("payload too large or invalid", request=r.request, response=r)
    r.raise_for_status()
    data = r.json().get("data", [])
    vecs = [d["embedding"] for d in data]
    if len(vecs) != len(texts):
        raise RuntimeError(f"embedding count {len(vecs)} != inputs {len(texts)}")
    return vecs

async def embed_adaptive_async(texts: List[str], init_batch: int) -> List[List[float]]:
    out: List[List[float]] = []
    b = max(1, init_batch)
    i = 0
    async with httpx.AsyncClient(
        http2=HTTP2,
        timeout=httpx.Timeout(connect=20.0, read=120.0, write=120.0, pool=120.0),
        limits=httpx.Limits(max_connections=max(EMBED_CONCURRENCY*2, EMBED_CONCURRENCY+2),
                            max_keepalive_connections=EMBED_CONCURRENCY+2)
    ) as client:
        sem = asyncio.Semaphore(EMBED_CONCURRENCY)

        async def work(block: List[str]) -> List[List[float]]:
            attempt = 0
            sb = len(block)
            while True:
                try:
                    async with sem:
                        return await embed_block(client, block)
                except httpx.HTTPStatusError as e:
                    if e.response is not None and e.response.status_code in (400, 413):
                        if sb == 1:
                            # last resort: further trim and retry once
                            t = block[0][: int(ATT_MAX_CHARS*0.6)]
                            return await embed_block(client, [t])
                        half = max(1, sb // 2)
                        left  = await work(block[:half])
                        right = await work(block[half:])
                        return left + right
                    attempt += 1
                except Exception:
                    attempt += 1
                if attempt >= MAX_RETRIES:
                    raise
                back = min(BACKOFF_MAX_S, BACKOFF_MIN_S*(2**(attempt-1))) + random.uniform(0,0.25)
                print(f"[WARN] embed retry {attempt}/{MAX_RETRIES} (block={sb}) sleep {back:.2f}s")
                await asyncio.sleep(back)

        tasks: List[asyncio.Task] = []
        while i < len(texts):
            block = texts[i:i+b]
            tasks.append(asyncio.create_task(work(block)))
            i += b
            if b < init_batch:
                b += 1
        for t in tasks:
            out.extend(await t)
    return out

# -------------- FILE IO (threaded) --------------
def read_text_file(path: Path, limit: int) -> Optional[str]:
    try:
        txt = path.read_text("utf-8", errors="ignore")
        txt = sanitize_text(txt)
        if not txt or len(txt) < ATT_MIN_TEXT_CHARS:
            return None
        return txt
    except Exception:
        return None

# -------------- MAIN -----------------
def main():
    if not Path(SQLITE_PATH).exists():
        print(f"[FATAL] SQLite not found: {SQLITE_PATH}"); sys.exit(2)
    if not OPENAI_API_KEY:
        print("[FATAL] OPENAI_API_KEY required"); sys.exit(2)

    con = sqlite3.connect(SQLITE_PATH); con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute("SELECT content_id, name, relpath, COALESCE(size,0) AS size FROM attachments").fetchall()
    print(f"[INFO] attachments rows: {len(rows)}")

    pg = psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)
    pcur = pg.cursor()
    ensure_legacy_schema(pcur)

    existing = set()
    if not FORCE:
        existing = load_existing_vec_ids(pcur)

    # Build worklist
    work: List[Tuple[str, str, str, str]] = []
    for r in rows:
        rel = r["relpath"] or ""
        path = resolve_path(rel)
        if path.suffix.lower() not in TEXT_EXT:
            continue
        att_id = f"ATT:{r['content_id']}:{rel}"
        if not FORCE and att_id in existing:
            work.append((att_id, r["name"] or Path(rel).name, att_space(rel), rel))
            continue
        if not path.exists():
            continue
        work.append((att_id, r["name"] or Path(rel).name, att_space(rel), rel))

    total = len(work)
    print(f"[PLAN] candidates: {total} (skip existing vectors: {len(existing)}; FORCE={int(FORCE)})")
    if total == 0:
        print("[OK] nothing to do"); return

    # Concurrent file reads
    docs_batch: List[Tuple[str,str,str,str,str]] = []  # (id,title,space,url,body)
    pg_docs_done = 0
    def flush_docs():
        nonlocal docs_batch, pg_docs_done
        if not docs_batch: return
        for i in range(0, len(docs_batch), PG_BATCH):
            upsert_docs_batched(pcur, docs_batch[i:i+PG_BATCH])
        pg_docs_done += len(docs_batch)
        print(f"[DOCS] upserted {pg_docs_done}")
        docs_batch.clear()

    with cf.ThreadPoolExecutor(max_workers=FILE_READERS) as ex:
        futures: List[Tuple[cf.Future, Tuple[str,str,str,str]]] = []
        for item in work:
            att_id, title, space, rel = item
            path = resolve_path(rel)
            futures.append((ex.submit(read_text_file, path, ATT_MAX_CHARS), item))

        embed_ids: List[str] = []
        embed_texts: List[str] = []

        for fut, item in futures:
            txt = fut.result()
            att_id, title, space, rel = item
            title = sanitize_text(title)
            url   = f"/attachments/{rel}"
            body  = txt or title or ""
            docs_batch.append((att_id, title, space, url, body))
            if len(docs_batch) >= PG_BATCH:
                flush_docs()

            if not FORCE and att_id in existing:
                continue
            if txt:
                embed_ids.append(att_id)
                embed_texts.append(combined_text(title, txt))
        flush_docs()

    # Async embeddings
    if embed_ids:
        print(f"[EMB] start embedding {len(embed_ids)} attachments (batch={BATCH_EMBED}, conc={EMBED_CONCURRENCY})")
        vectors = asyncio.run(embed_adaptive_async(embed_texts, BATCH_EMBED))
        for i in range(0, len(embed_ids), PG_BATCH):
            upsert_vecs_batched(pcur, embed_ids[i:i+PG_BATCH], vectors[i:i+PG_BATCH])
        print(f"[EMB] done: {len(embed_ids)} vectors")
    else:
        print("[EMB] nothing to embed")

    print("[DONE] attachment legacy backfill complete.")

if __name__ == "__main__":
    t0 = time.time()
    try:
        main()
    finally:
        dt = time.time() - t0
        print(f"[TIME] {dt:.1f}s total")
