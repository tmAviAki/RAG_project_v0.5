from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
import sqlite3
from ..config import settings
from ..repository import connect, count_stats, list_spaces, search_docs, fetch_docs, list_attachments
from ..chunker import iter_chunked_items
from ..models import PagedResponse, DocHit, DocFull
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi import Request

def get_conn() -> sqlite3.Connection:
    try:
        conn = connect(settings.index_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    return conn
