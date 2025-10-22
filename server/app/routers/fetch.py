from __future__ import annotations
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
import sqlite3, json
from ..config import settings
from ..repository import fetch_docs
from ..chunker import iter_chunked_items
from ..models import PagedResponse, DocFull
from .common import get_conn
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/v1", tags=["fetch"])

@router.get("/fetch", response_model=PagedResponse)
def fetch(
    ids: str = Query(..., description="Comma-separated content IDs"),
    chunk_bytes: int = Query(settings.chunk_size_bytes, ge=10_000, le=150_000),
    conn: sqlite3.Connection = Depends(get_conn),
):
    id_list = [x.strip() for x in ids.split(",") if x.strip()]
    if not id_list:
        raise HTTPException(400, detail="Provide at least one id")    
    rows = fetch_docs(conn, id_list)
    docs = [DocFull(**r).model_dump() for r in rows]
    chunks = list(iter_chunked_items(docs, chunk_bytes=chunk_bytes, envelope=True))
    if not chunks:
        return {"items": [], "next": None, "approx_bytes": 0}
    payload, n_items, approx = chunks[0]
    body = json.loads(payload.decode("utf-8"))
    # No 'next' here since fetch is by explicit IDs
    body["next"] = None
    return JSONResponse(content=body)

@router.get("/stream/fetch")
def stream_fetch(
    ids: str = Query(..., description="Comma-separated content IDs"),
    chunk_bytes: int = Query(settings.chunk_size_bytes, ge=10_000, le=200_000),
    conn: sqlite3.Connection = Depends(get_conn),
):
    id_list = [x.strip() for x in ids.split(",") if x.strip()]
    rows = fetch_docs(conn, id_list)
    docs = [DocFull(**r).model_dump() for r in rows]
    def gen():
        for payload, _, _ in iter_chunked_items(docs, chunk_bytes=chunk_bytes, envelope=False):
            yield payload
    return StreamingResponse(gen(), media_type="application/x-ndjson")
