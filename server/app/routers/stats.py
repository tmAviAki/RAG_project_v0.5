from __future__ import annotations
from fastapi import APIRouter, Depends
import sqlite3
from ..repository import count_stats, list_spaces
from ..config import settings
from .common import get_conn

router = APIRouter(prefix="/v1", tags=["system"])

@router.get("/stats")
def stats(conn: sqlite3.Connection = Depends(get_conn)) -> dict:
    return count_stats(conn)

@router.get("/spaces")
def spaces(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict]:
    return list_spaces(conn)
