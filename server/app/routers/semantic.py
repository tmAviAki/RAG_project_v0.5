# File: server/app/routers/semantic.py
# Project: RAG_project_v0.5  Component: routers.semantic  Version: v0.2.1
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..rag_store import NumpyStore, VSConfig
from ..embeddings import get_embedder

router = APIRouter(prefix="/v1", tags=["semantic"])


class SearchReq(BaseModel):
    q: str = Field(..., description="query text")
    k: int = Field(8, ge=1, le=100)


class SearchItem(BaseModel):
    id: Optional[str] = None
    title: Optional[str] = None
    space: Optional[str] = None
    snippet: Optional[str] = None
    score: Optional[float] = None
    url: Optional[str] = None


class AnswerReq(BaseModel):
    question: str
    k: int = Field(12, ge=1, le=50)
    force_abstain_if_no_citations: bool = True


class AnswerResp(BaseModel):
    answer: str
    citations: List[SearchItem] = []


@router.post("/semantic/search")
def semantic_search(req: SearchReq) -> List[Dict[str, Any]]:
    cfg = VSConfig()
    store = NumpyStore(cfg)
    dim = store.effective_dim()
    emb = get_embedder(dim_hint=dim)
    t0 = time.time()
    try:
        qv = emb.embed_texts([req.q])[0]
    except Exception as e:
        if os.getenv("ALLOW_QUERY_FALLBACK", "0") == "1":
            return []
        raise HTTPException(status_code=503, detail=f"Embeddings unavailable: {e}")
    hits = store.search(qv, k=req.k, filters=None)  # type: ignore[arg-type]
    out: List[Dict[str, Any]] = []
    for h in hits or []:
        out.append(
            {
                "id": h.get("id"),
                "title": h.get("title"),
                "space": h.get("space"),
                "snippet": h.get("snippet"),
                "score": h.get("score"),
                "url": h.get("url"),
            }
        )
    _ = (time.time() - t0)  # timing available for future logging
    return out


@router.post("/answer")
def answer(req: AnswerReq) -> AnswerResp:
    cfg = VSConfig()
    store = NumpyStore(cfg)
    dim = store.effective_dim()
    emb = get_embedder(dim_hint=dim)
    try:
        qv = emb.embed_texts([req.question])[0]
    except Exception as e:
        if os.getenv("ALLOW_QUERY_FALLBACK", "0") == "1":
            return AnswerResp(
                answer="INSUFFICIENT CONTEXT — embeddings unavailable", citations=[]
            )
        raise HTTPException(status_code=503, detail=f"Embeddings unavailable: {e}")
    hits = store.search(qv, k=req.k, filters=None)  # type: ignore[arg-type]
    if not hits:
        if req.force_abstain_if_no_citations:
            return AnswerResp(answer="INSUFFICIENT CONTEXT", citations=[])
        return AnswerResp(answer="", citations=[])
    cits: List[SearchItem] = []
    for h in hits:
        cits.append(
            SearchItem(
                id=h.get("id"),
                title=h.get("title"),
                space=h.get("space"),
                snippet=h.get("snippet"),
                score=h.get("score"),
                url=h.get("url"),
            )
        )
    return AnswerResp(
        answer="Answer built from retrieved evidence. See citations.", citations=cits
    )


@router.post("/stream/answer")
def stream_answer(req: AnswerReq):
    def gen():
        preamble = {"type": "preamble"}
        yield json.dumps(preamble, ensure_ascii=False) + "\n"
        body = answer(req)
        for c in body.citations:
            ev = {
                "type": "citation",
                "id": c.id,
                "title": c.title,
                "url": c.url,
                "score": c.score,
            }
            yield json.dumps(ev, ensure_ascii=False) + "\n"
        final = {"type": "final", "answer": body.answer}
        yield json.dumps(final, ensure_ascii=False) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")

