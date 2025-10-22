# Project:Confluence Evidence API  Component:chunker_rag  Version:v0.1.0
from __future__ import annotations
import math
from typing import Iterable, Dict

def approx_token_len(text: str) -> int:
    if not text:
        return 0
    # fallback heuristic: ~4 chars ≈ 1 token
    return math.ceil(len(text) / 4)

def iter_chunks(doc: Dict, target_tokens: int = 800, overlap_tokens: int = 100):
    text = doc.get("text","") or ""
    if not text:
        return
    # naive split by characters using token heuristic
    chars_per_tok = 4
    step = max(1, (target_tokens - overlap_tokens) * chars_per_tok)
    size = max(1, target_tokens * chars_per_tok)
    start = 0
    ix = 0
    while start < len(text):
        chunk_text = text[start:start+size]
        yield {
            "id": doc["id"],
            "chunk_ix": ix,
            "text": chunk_text,
            "space": doc.get("space",""),
            "type": doc.get("type",""),
            "title": doc.get("title",""),
        }
        ix += 1
        start += step

