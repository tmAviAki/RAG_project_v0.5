# Project:Confluence Evidence API  Component:rag_ingest_batch  Version:v1.0.0
from __future__ import annotations
import os, argparse
from typing import List, Dict, Any, Optional
from .repository import connect, search_docs, fetch_docs
from .rag_store import NumpyStore, VSConfig
from .chunker_rag import iter_chunks
from .embed_cache import EmbedCache
from .embed_batch import run_batch, _hash_text

def load_docs(conn, space_filter: Optional[List[str]], batch: int, cursor: int = 0) -> List[Dict[str,Any]]:
    rows = search_docs(conn, q="", space=space_filter[0] if (space_filter and len(space_filter)==1) else None,
                       doctype=None, limit=batch, offset=cursor)
    ids = [r["id"] for r in rows]
    return fetch_docs(conn, ids)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None)
    ap.add_argument("--spaces", default="ALL")
    ap.add_argument("--batch", type=int, default=500)
    args = ap.parse_args()

    cfg = VSConfig()
    store = NumpyStore(cfg)
    cache = EmbedCache(os.getenv("EMBED_CACHE_PATH", "/index/embed_cache.sqlite"))

    from .config import settings
    conn = connect(settings.index_path)

    space_filter = None if args.spaces == "ALL" else args.spaces.split(",")
    cursor = 0
    total = 0
    while True:
        docs = load_docs(conn, space_filter, args.batch, cursor)
        if not docs:
            break

        chunks: List[Dict[str,Any]] = []
        texts: List[str] = []
        hashes: List[str] = []
        for d in docs:
            text = d.get("text") or ""
            if len(text) < 200:
                continue
            for ch in iter_chunks(d):
                ct = ch["text"]
                h = _hash_text(ct)
                chunks.append({
                    "id": d["id"], "space": d["space"], "type": d["type"], "title": d["title"],
                    "url": f"/v1/fetch?ids={d['id']}",
                    "chunk_ix": ch["chunk_ix"], "updated_at": d.get("updated") or "",
                    "text": ct
                })
                texts.append(ct)
                hashes.append(h)

        if not chunks:
            cursor += args.batch
            print(f"[RAG-BATCH] upserted 0 chunks (cursor={cursor})", flush=True)
            continue

        pairs = list(zip(hashes, texts))
        keys = [(h, os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")) for h,_ in pairs]
        cached = cache.get_many(keys)
        misses_texts = [t for h,t in pairs if h not in cached]
        if misses_texts:
            nins = run_batch(misses_texts, cache)
        embs_map = cache.get_many(keys)

        kept = 0
        for c, h in zip(chunks, hashes):
            vec = embs_map.get(h)
            if vec:
                c["embedding"] = vec
                kept += 1
        n = store.upsert([c for c in chunks if "embedding" in c])
        total += n
        cursor += args.batch
        print(f"[RAG-BATCH] upserted {n} chunks (cursor={cursor})", flush=True)

    print(f"[RAG-BATCH] done, total chunks: {total}", flush=True)

if __name__ == "__main__":
    main()

