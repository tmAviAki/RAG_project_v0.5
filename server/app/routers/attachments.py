from __future__ import annotations
from fastapi import APIRouter, Depends, Query
import sqlite3
from ..repository import list_attachments
from ..config import settings
from .common import get_conn

router = APIRouter(prefix="/v1", tags=["attachments"])

@router.get("/attachments/by-content/{content_id}")
def attachments_list(content_id: str, conn: sqlite3.Connection = Depends(get_conn)) -> dict:
    items = list_attachments(conn, content_id)
    # Construct URLs relative to /attachments static mount
    for it in items:
        it["url"] = f"/attachments/{it['relpath']}"
    return {"content_id": content_id, "items": items}
