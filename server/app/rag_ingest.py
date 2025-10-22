# Project:Confluence Evidence API  Component:rag_ingest  Version:v0.3.0
from __future__ import annotations
import os, argparse, logging, signal
from typing import List, Dict, Any, Optional

from .repository import connect, search_docs, fetch_docs, count_stats
from .rag_store import NumpyStore, VSConfig
from .chunker_rag import iter_chunks
from .embeddings import get_embedder
from .config import settings

_STOP = False
def _sig_handler(signum, frame):
    global _STOP
    _STOP = True
    logging.info(f"[RAG] signal {signum} received, stopping gracefully...")

for _sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(_sig, _sig_handler)

def _setup_logging() -> None:
    log_path = os.getenv("RAG_LOG_PATH", "/index/rag_ingest.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode="a", encoding="utf-8")],
    )
    logging.info(f"[RAG] logfile={log_path}")

def load_docs(conn, space_filter: Optional[List[str]], batch: int, cursor: int = 0) -> List[Dict[str,Any]]:
    rows = search_docs(
        conn,
        q="",
        space=space_filter[0] if (space_filter and len(space_filter)==1) else None,
        doctype=None,
        limit=batch,
        offset=cursor,
    )
    ids = [r["id"] for r in rows]
    return fetch_docs(conn, ids)

def main():
    _setup_logging()
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="ISO8601 or epoch (placeholder)")
    ap.add_argument("--spaces", default="ALL")
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--embed-dim", type=int, default=int(os.getenv("EMBED_DIM","1536")))
    args = ap.parse_args()

    store = NumpyStore(VSConfig())  # auto-heal on load
    embedder = get_embedder(dim_hint=args.embed_dim)
    conn = connect(settings.index_path)

    stats = count_stats(conn)
    logging.info(f"[RAG] index={settings.index_path} docs={stats['docs']} attachments={stats['attachments']} "
                 f"spaces={args.spaces} batch={args.batch}")

    space_filter = None if args.spaces == "ALL" else args.spaces.split(",")
    cursor = 0
    total = 0

    while True:
        if _STOP:
            logging.info(f"[RAG] stop requested; exiting after cursor={cursor} total_chunks={total}")
            break

        logging.info(f"[RAG] cursor={cursor}")
        docs = load_docs(conn, space_filter, args.batch, cursor)
        if not docs:
            break

        chunks: List[Dict[str,Any]] = []
        for d in docs:
            t = d.get("text") or ""
            min_chars = int(os.getenv("CHUNK_MIN_CHARS", "200"))
            if len(t) < min_chars:
                continue
            for ch in iter_chunks(d):
                item = {
                    "id": d["id"], "space": d["space"], "type": d["type"], "title": d["title"],
                    "url": f"/v1/fetch?ids={d['id']}",
                    "chunk_ix": ch["chunk_ix"], "updated_at": d.get("updated") or "",
                    "text": ch["text"],
                }
                chunks.append(item)

        if not chunks:
            cursor += args.batch
            logging.info(f"[RAG] upserted 0 chunks (cursor={cursor})")
            continue

        texts = [c["text"] for c in chunks]
        embs = embedder.embed_texts(texts)
        for c, e in zip(chunks, embs):
            c["embedding"] = e

        n = store.upsert(chunks)  # atomic save
        total += n
        cursor += args.batch
        logging.info(f"[RAG] upserted {n} chunks (cursor={cursor})")

    logging.info(f"[RAG] done, total chunks: {total}")

if __name__ == "__main__":
    main()
