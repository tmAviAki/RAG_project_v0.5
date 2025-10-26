# Project:RAG_project_v0.5 Component:routers.hybrid Version:v0.7.0
from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from ..retrieval_hybrid import search_hybrid

router = APIRouter(prefix="/v1", tags=["hybrid"])

@router.get("/search/hybrid")
def hybrid_search(q: str = Query(...), k: int = Query(20, ge=1, le=100)):
    try:
        return search_hybrid(q, k=k)
    except Exception as e:
        raise HTTPException(500, detail=str(e))

