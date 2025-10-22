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
    limit: int = Query(100, ge=1, le=1000),
    cursor: int = Query(0, ge=0, description="Offset for paging"),
    chunk_bytes: int = Query(settings.chunk_size_bytes, ge=10_000, le=150_000),
    conn: sqlite3.Connection = Depends(get_conn),
):
    rows = search_docs(conn, q, space, type, limit=limit, offset=cursor)
    # Assemble hits and chunk by bytes approximate
    hits = [DocHit(**r).model_dump() for r in rows]
    chunks = list(iter_chunked_items(hits, chunk_bytes=chunk_bytes, envelope=True))
    if not chunks:
        return {"items": [], "next": None, "approx_bytes": 0}
    # We return only the first chunk here (Action connectors expect a single JSON body).
    payload, n_items, approx = chunks[0]
    body = json.loads(payload.decode("utf-8"))
    next_cursor = cursor + n_items
    # If there are more results OR more chunks, expose next cursor
    body["next"] = next_cursor if (len(rows) > n_items) else None
    return JSONResponse(content=body)

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
