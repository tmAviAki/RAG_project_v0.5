# Project:RAG_project_v0.5 Component:retrieval_hybrid Version:v0.7.9
from __future__ import annotations
import os, time, logging
from typing import List, Dict, Any
from .db.pg import get_conn
from .embeddings import get_embedder
from .reduction import get_reducer
from .otel import maybe_span

ALPHA = float(os.getenv("HYBRID_ALPHA", "0.5"))
FULL_DIM = int(os.getenv("EMBED_DIM", "3072"))

# Uniform logging (optional)
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_log = logging.getLogger("rag.hybrid")
if not _log.handlers:
    logging.basicConfig(level=getattr(logging, _LOG_LEVEL, logging.INFO),
                        format="%(asctime)s %(levelname)-7s rag.hybrid %(message)s")
_log.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

def _normalize(scores: List[float]) -> List[float]:
    if not scores: return []
    lo, hi = min(scores), max(scores)
    if hi <= lo: return [0.0 for _ in scores]
    return [(s - lo) / (hi - lo) for s in scores]

def search_hybrid(q: str, k: int = 20) -> Dict[str, Any]:
    t0 = time.time()
    _log.debug("hybrid start q=%r k=%d", q, k)
    lex_hits: List[Dict[str, Any]] = []
    vec_hits: List[Dict[str, Any]] = []

    with maybe_span("rag.lex"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title, space, url,
                           ts_rank_cd(tsv, plainto_tsquery('english', %s)) AS lscore
                    FROM docs
                    WHERE tsv @@ plainto_tsquery('english', %s)
                    ORDER BY lscore DESC
                    LIMIT %s
                """, (q, q, k))
                for row in cur.fetchall():
                    lex_hits.append({
                        "id": row[0], "title": row[1], "space": row[2], "url": row[3],
                        "lscore": float(row[4]) if row[4] is not None else None
                    })

    qv_full = None
    with maybe_span("rag.embed"):
        try:
            emb = get_embedder(dim_hint=FULL_DIM)
            qv_full = emb.embed_texts([q])[0]
        except Exception as e:
            _log.warning("hybrid embed error: %s", e)
            qv_full = None

    if qv_full is not None:
        with maybe_span("rag.ann"):
            reducer = get_reducer()               # 3072 -> 1536
            qv_1536 = reducer.reduce(qv_full)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT d.id, d.title, d.space, d.url,
                               1.0/(1.0 + (e.embedding_1536 <-> %s)) AS vscore
                        FROM doc_embeddings e
                        JOIN docs d ON d.id = e.id
                        ORDER BY e.embedding_1536 <-> %s
                        LIMIT %s
                    """, (qv_1536, qv_1536, k))
                    for row in cur.fetchall():
                        vec_hits.append({
                            "id": row[0], "title": row[1], "space": row[2], "url": row[3],
                            "vscore": float(row[4]) if row[4] is not None else None
                        })

    with maybe_span("rag.fuse"):
        by_id: Dict[str, Dict[str, Any]] = {}
        for h in lex_hits: by_id.setdefault(h["id"], {}).update(h)
        for h in vec_hits: by_id.setdefault(h["id"], {}).update(h)
        ls = _normalize([v.get("lscore", 0.0) or 0.0 for v in by_id.values()])
        vs = _normalize([v.get("vscore", 0.0) or 0.0 for v in by_id.values()])
        for (v, lnorm, vnorm) in zip(by_id.values(), ls, vs):
            v["score"] = ALPHA * lnorm + (1.0 - ALPHA) * vnorm
        items = sorted(by_id.values(), key=lambda x: x.get("score", 0.0), reverse=True)[:k]

    took_ms = int((time.time() - t0) * 1000)
    _log.debug("hybrid end took_ms=%d items=%d", took_ms, len(items))
    return {"items": items, "took_ms": took_ms}
