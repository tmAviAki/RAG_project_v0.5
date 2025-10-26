# Project:RAG_project_v0.5 Component:routers.code_symbols Version:v0.7.0
from __future__ import annotations
from typing import Dict, Any
from fastapi import APIRouter, Query
from ..db.pg import get_conn
from ..otel import maybe_span

router = APIRouter(prefix="/v1", tags=["code"])

@router.get("/code/symbols")
def code_symbols(q: str = Query(...), k: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    items = []
    with maybe_span("rag.lex"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT path, lang, symbol, kind, start_line, end_line,
                           substring(content for 400) AS snippet,
                           ts_rank_cd(tsv, plainto_tsquery('english', %s)) AS rank
                    FROM code_symbols
                    WHERE tsv @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT %s
                    """, (q, q, k),
                )
                for r in cur.fetchall():
                    items.append({
                        "path": r[0], "lang": r[1], "symbol": r[2], "kind": r[3],
                        "start_line": r[4], "end_line": r[5], "snippet": r[6],
                    })
    return {"items": items, "took_ms": 0}

@router.get("/code/grep")
def code_grep(q: str = Query(...), regex: int = Query(0), k: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    items = []
    sql = (
        "SELECT path, lang, symbol, kind, start_line, end_line, substring(content for 400) "
        "FROM code_symbols WHERE content ~ %s LIMIT %s"
        if regex else
        "SELECT path, lang, symbol, kind, start_line, end_line, substring(content for 400) "
        "FROM code_symbols WHERE content ILIKE '%'||%s||'%' LIMIT %s"
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (q, k))
            for r in cur.fetchall():
                items.append({
                    "path": r[0], "lang": r[1], "symbol": r[2], "kind": r[3],
                    "start_line": r[4], "end_line": r[5], "snippet": r[6],
                })
    return {"items": items, "took_ms": 0}

