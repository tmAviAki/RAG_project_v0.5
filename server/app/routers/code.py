# Project:Confluence Evidence API  Component:routers.code  Version:v0.4.0
from __future__ import annotations

import json
import os
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..rag_store import NumpyStore, VSConfig
from ..embeddings import get_embedder

ENABLE_TS = os.getenv("ENABLE_TREE_SITTER", "0") == "1"
CODE_ROOT = os.getenv("CODE_ROOT", "/code")
# Optional query-time fallback (only safe if the store was built with the same embedder)
ALLOW_QUERY_FALLBACK = os.getenv("ALLOW_QUERY_FALLBACK", "0") == "1"

router = APIRouter(prefix="/v1", tags=["code"])


class SymbolSearchReq(BaseModel):
    q: str = Field(..., min_length=1)
    lang: str = "cpp"
    k: int = Field(20, ge=1, le=100)


@router.post("/code/symbol-search")
def symbol_search(req: SymbolSearchReq) -> List[Dict[str, Any]]:
    if not ENABLE_TS:
        raise HTTPException(
            501,
            detail="INSUFFICIENT CONTEXT — PROVIDE Tree-sitter setup and set ENABLE_TREE_SITTER=1",
        )
    # Placeholder – real impl would use a symbol indexer (not shipped in this build)
    raise HTTPException(501, detail="Not implemented in this build")


def _safe_join(root: str, p: str) -> str:
    full = os.path.abspath(os.path.join(root, p.lstrip("/")))
    root_abs = os.path.abspath(root)
    if not (full == root_abs or full.startswith(root_abs + os.sep)):
        raise HTTPException(400, detail="path outside CODE_ROOT")
    return full


@router.get("/code/file")
def code_file(
    path: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    chunk_bytes: int = Query(90000, ge=10000, le=150000),
) -> Dict[str, Any]:
    safe = _safe_join(CODE_ROOT, path)
    if not os.path.isfile(safe):
        raise HTTPException(404, detail="file not found")
    with open(safe, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()
    if start is not None or end is not None:
        s = max(0, start or 0)
        e = end if (end is not None and end >= 0) else len(data)
        e = min(e, len(data))
        if e < s:
            s, e = e, e  # empty
        data = data[s:e]
    if len(data.encode("utf-8")) > chunk_bytes:
        data = data.encode("utf-8")[:chunk_bytes].decode("utf-8", "ignore")
    return {"path": path, "content": data}


@router.get("/code/deps")
def code_deps(path: str):
    raise HTTPException(501, detail="INSUFFICIENT CONTEXT — PROVIDE deps graph or set ENABLE_DEPS=1")


# -----------------------------
# Code semantic search (JSON)
# -----------------------------
class CodeSearchFilters(BaseModel):
    space: Optional[List[str]] = None
    lang: Optional[List[str]] = None
    path: Optional[str] = None  # substring filter


class CodeSearchReq(BaseModel):
    q: str = Field(..., min_length=1)
    k: int = Field(12, ge=1, le=100)
    filters: Optional[CodeSearchFilters] = None
    chunk_bytes: int = Field(90000, ge=10000, le=150000)


def _embed_query_or_503(q: str):
    try:
        return get_embedder().embed_texts([q])[0]
    except Exception as e:
        msg = str(e)
        if ALLOW_QUERY_FALLBACK:
            try:
                os.environ["ALLOW_REMOTE_EMBEDDINGS"] = "0"
                return get_embedder().embed_texts([q])[0]
            except Exception as e2:
                raise HTTPException(status_code=503, detail=f"Embeddings unavailable: {msg}; fallback failed: {e2}")
        raise HTTPException(
            status_code=503,
            detail=("Embeddings unavailable: " + msg + ". "
                    "Set ALLOW_QUERY_FALLBACK=1 only if your store was built with the same embedder.")
        )


def _filter_and_shape(hits: List[Dict[str, Any]],
                      k: int,
                      filters: Optional[CodeSearchFilters],
                      chunk_bytes: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    size = 0
    want_langs = set((filters.lang or [])) if filters else set()
    path_sub = (filters.path or "").strip() if (filters and filters.path) else ""

    for h in hits:
        if want_langs:
            hl = (h.get("lang") or "").lower()
            if hl not in {x.lower() for x in want_langs}:
                continue
        if path_sub:
            hp = (h.get("path") or "")
            if path_sub not in hp:
                continue

        item = {
            "id": h.get("id"),
            "title": h.get("title"),
            "path": h.get("path"),
            "lang": h.get("lang"),
            "symbol": h.get("symbol"),
            "line_start": h.get("line_start"),
            "line_end": h.get("line_end"),
            "snippet": (h.get("snippet") or ""),
            "score": h.get("score"),
            "url": h.get("url"),
        }
        js_len = len(str(item))
        if size + js_len > chunk_bytes:
            break
        out.append(item)
        size += js_len

        if len(out) >= k:
            break

    return out


@router.post("/code/search")
def code_search(req: CodeSearchReq) -> List[Dict[str, Any]]:
    store = NumpyStore(VSConfig())

    emb = _embed_query_or_503(req.q)

    base_filters: Dict[str, Any] = {"type": ["code"]}
    if req.filters and req.filters.space:
        base_filters["space"] = req.filters.space

    hits = store.search(emb, k=req.k, filters=base_filters)
    return _filter_and_shape(hits, req.k, req.filters, req.chunk_bytes)


# -----------------------------
# Code semantic search (NDJSON stream)
# -----------------------------
@router.get("/stream/code/search")
def stream_code_search(
    q: str = Query(..., description="Search query"),
    k: int = Query(12, ge=1, le=100),
    space: Optional[str] = Query(None, description="Optional space key"),
    lang: Optional[str] = Query(None, description="Optional comma-separated languages, e.g. 'python,cpp'"),
    path: Optional[str] = Query(None, description="Substring filter on code path"),
    chunk_bytes: int = Query(90000, ge=10000, le=200000),
):
    store = NumpyStore(VSConfig())
    emb = _embed_query_or_503(q)

    base_filters: Dict[str, Any] = {"type": ["code"]}
    if space:
        base_filters["space"] = [space]

    hits = store.search(emb, k=k, filters=base_filters)

    want_langs = {x.strip().lower() for x in (lang.split(",") if lang else []) if x.strip()}
    path_sub = (path or "").strip()

    def gen():
        sent = 0
        for h in hits:
            if want_langs:
                hl = (h.get("lang") or "").lower()
                if hl not in want_langs:
                    continue
            if path_sub:
                hp = (h.get("path") or "")
                if path_sub not in hp:
                    continue

            item = {
                "id": h.get("id"),
                "title": h.get("title"),
                "path": h.get("path"),
                "lang": h.get("lang"),
                "symbol": h.get("symbol"),
                "line_start": h.get("line_start"),
                "line_end": h.get("line_end"),
                "snippet": (h.get("snippet") or ""),
                "score": h.get("score"),
                "url": h.get("url"),
            }
            # NDJSON line
            yield (json.dumps(item, ensure_ascii=False) + "\n").encode("utf-8")
            sent += 1
            if sent >= k:
                break

    return StreamingResponse(gen(), media_type="application/x-ndjson")
