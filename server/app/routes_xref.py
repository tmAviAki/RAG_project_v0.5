# server/app/routes_xref.py
# Project: confAdogpt  Component: routes_xref  Version: v0.7.2
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
import json
from pathlib import Path
import os

router = APIRouter(prefix="/v1", tags=["xref"])

XREF_PATH = Path(os.getenv("INDEX_ROOT", "/index")) / "xref.jsonl"

def _load_edges() -> List[Dict[str, str]]:
    if not XREF_PATH.exists():
        return []
    out: List[Dict[str, str]] = []
    with XREF_PATH.open("r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except json.JSONDecodeError:
                pass
    return out

@router.get("/xref")
def xref(id: str = Query(..., description="Node id (e.g., CODE:/path:10-200 or ATT:...)"),
         limit: int = Query(20, ge=1, le=100)) -> Dict[str, Any]:
    edges = _load_edges()
    nbrs = [e for e in edges if e.get("src") == id]
    nbrs = nbrs[:limit]
    return {"id": id, "neighbors": nbrs}
