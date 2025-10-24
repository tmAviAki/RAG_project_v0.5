# Project:RAG_project_v0.5 Component:routers.search Version:v0.6.1
from __future__ import annotations
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
import sqlite3, json
from ..config import settings
from ..repository import search_docs, fetch_docs
from ..chunker import iter_chunked_items
from ..models import PagedResponse, DocHit, DocFull
from .common import get_conn
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/v1", tags=["search"])

@router.get("/search", response_model=PagedResponse)
def search(
    q: str = Query("", description="Full-text query. Empty = list by title."),
    space: Optional[str] = Query(None),
    type: Optional[str] = Query(None, description="page|blogpost|comment"),
    k: int = Query(200, ge=1, le=5000, description="alias of limit"),
    limit: int = Query(200, ge=1, le=5000),
    cursor: int = Query(0, ge=0, description="Offset for paging"),
    chunk_bytes: int = Query(settings.chunk_size_bytes, ge=10_000, le=200_000),
    conn: sqlite3.Connection = Depends(get_conn),
):
    rows = search_docs(conn, q, space, type, limit=k or limit, offset=cursor)
    items = [DocHit(**r).model_dump() for r in rows]
    # Approximate byte-capped page
    payload = {"items": items, "next": cursor + len(items)}
    return JSONResponse(payload)

@router.get("/stream/search")
def stream_search(
    q: str = Query("", description="Full-text query. Empty = list by title."),
    space: Optional[str] = Query(None),
    type: Optional[str] = Query(None, description="page|blogpost|comment"),
    limit: int = Query(200, ge=1, le=5000),
    cursor: int = Query(0, ge=0, description="Offset for paging"),
    chunk_bytes: int = Query(settings.chunk_size_bytes, ge=10_000, le=200_000),
    conn: sqlite3.Connection = Depends(get_conn),
):
    rows = search_docs(conn, q, space, type, limit=limit, offset=cursor)
    hits = [DocHit(**r).model_dump() for r in rows]
    def gen():
        for payload, _, _ in iter_chunked_items(hits, chunk_bytes=chunk_bytes, envelope=False):
            yield payload
    return StreamingResponse(gen(), media_type="application/x-ndjson")

