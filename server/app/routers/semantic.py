# Project: confAdogpt  Component: routers.semantic  Version: v0.2.0
from __future__ import annotations

import html
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..rag_store import NumpyStore, VSConfig
from ..embeddings import get_embedder

router = APIRouter(prefix="/v1", tags=["semantic"])

ALLOW_QUERY_FALLBACK = os.getenv("ALLOW_QUERY_FALLBACK", "0") == "1"
FORCE_ABSTAIN_DEFAULT = True

class SearchReq(BaseModel):
    q: str
    k: int = Field(8, ge=1, le=100)
    score_threshold: Optional[float] = Field(None, ge=0.0, le=1.0)
    sources: Optional[List[str]] = None

class SearchRespItem(BaseModel):
    id: str
    title: Optional[str] = None
    space: Optional[str] = None
    snippet: Optional[str] = None
    score: Optional[float] = None
    url: Optional[str] = None

@router.post("/semantic/search")
def semantic_search(req: SearchReq) -> Dict[str, Any]:
    cfg = VSConfig()
    store = NumpyStore(cfg)
    dim = store.effective_dim()
    emb = get_embedder(dim_hint=dim)
    try:
        qv = emb.embed_texts([req.q])[0]
    except Exception as e:
        if ALLOW_QUERY_FALLBACK:
            return {"items": [], "took_ms": 0}
        raise HTTPException(500, detail=str(e))
    t0 = time.time()
    filters: Optional[Dict[str, Any]] = None
    hits = store.search(qv, k=req.k, filters=filters)
    if req.score_threshold is not None:
        hits = [h for h in hits if float(h.get("score", 0.0)) >= req.score_threshold]
    took_ms = int((time.time() - t0) * 1000)
    items: List[Dict[str, Any]] = []
    for h in hits:
        item = {
            "id": h.get("id"),
            "title": h.get("title"),
            "space": h.get("space"),
            "snippet": html.escape(h.get("snippet", "")),
            "score": h.get("score"),
            "url": h.get("url"),
        }
        items.append(item)
    return {"items": items, "took_ms": took_ms}

class AnswerReq(BaseModel):
    question: str
    k: int = Field(12, ge=1, le=50)
    force_abstain_if_no_citations: bool = True

class AnswerResp(BaseModel):
    answer: str
    citations: List[Dict[str, Any]]

@router.post("/answer")
def answer(req: AnswerReq) -> AnswerResp:
    cfg = VSConfig()
    store = NumpyStore(cfg)
    dim = store.effective_dim()
    emb = get_embedder(dim_hint=dim)
    qv = emb.embed_texts([req.question])[0]
    hits = store.search(qv, k=req.k, filters=None)
    if not hits:
        if req.force_abstain_if_no_citations or FORCE_ABSTAIN_DEFAULT:
            raise HTTPException(422, detail="INSUFFICIENT CONTEXT")
        return AnswerResp(answer="No relevant evidence found.", citations=[])
    parts: List[str] = []
    cits: List[Dict[str, Any]] = []
    for i, h in enumerate(hits[: min(5, len(hits))], start=1):
        t = h.get("snippet") or h.get("title") or h.get("id")
        parts.append(f"[{i}] {t}")
        cits.append({
            "id": h.get("id"),
            "title": h.get("title"),
            "space": h.get("space"),
            "url": h.get("url"),
            "score": h.get("score"),
        })
    ans = ("This answer cites retrieved sources only. "
           "See numbered references below.\n\n" + "\n".join(parts))
    return AnswerResp(answer=ans, citations=cits)

class NDJSONAnswerReq(BaseModel):
    question: str
    k: int = Field(12, ge=1, le=50)
    force_abstain_if_no_citations: bool = True

@router.post("/stream/answer")
def stream_answer(req: NDJSONAnswerReq):
    cfg = VSConfig()
    store = NumpyStore(cfg)
    dim = store.effective_dim()
    emb = get_embedder(dim_hint=dim)
    qv = emb.embed_texts([req.question])[0]
    hits = store.search(qv, k=req.k, filters=None)
    if not hits:
        if req.force_abstain_if_no_citations or FORCE_ABSTAIN_DEFAULT:
            def gen_empty():
                yield '{"type":"final","answer":"INSUFFICIENT CONTEXT"}\n'
            return StreamingResponse(gen_empty(), media_type="application/x-ndjson")
    def gen():
        yield '{"type":"preamble","text":"composing answer from evidence"}\n'
        for i, h in enumerate(hits[: min(5, len(hits))], start=1):
            ev = {
                "type": "citation",
                "index": i,
                "id": h.get("id"),
                "title": h.get("title"),
                "url": h.get("url"),
                "score": h.get("score"),
            }
            import json
            yield json.dumps(ev, ensure_ascii=False) + "\n"
        final = {
            "type": "final",
            "answer": "Answer built from retrieved evidence. See citations.",
        }
        import json
        yield json.dumps(final, ensure_ascii=False) + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson")
