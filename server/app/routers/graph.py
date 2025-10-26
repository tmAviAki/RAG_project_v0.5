# Project:RAG_project_v0.5 Component:routers.graph Version:v0.7.0
from __future__ import annotations
from typing import Dict, Any
from fastapi import APIRouter, Query
from ..db.pg import get_conn

router = APIRouter(prefix="/v1", tags=["graph"])

@router.get("/graph/neighbors")
def neighbors(id: str = Query(...), limit: int = Query(20, ge=1, le=200)) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT src, dst, kind, weight FROM graph_edges WHERE src = %s LIMIT %s",
                (id, limit),
            )
            nbrs = [{"src": s, "dst": d, "kind": k, "weight": w} for (s, d, k, w) in cur.fetchall()]
    return {"id": id, "neighbors": nbrs}

