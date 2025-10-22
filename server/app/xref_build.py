# Project: confAdogpt  Component: xref_build  Version: v0.8.0
"""
Build minimal cross-reference edges:
- code <-> doc edges by filename/symbol stem matches
- code <-> code edges by co-stem
Outputs NDJSON edges to /index/xref.jsonl
"""
from __future__ import annotations
import os, re, json, logging, time
from pathlib import Path
from typing import List, Dict, Any

from .rag_store import VSConfig

INDEX_ROOT = os.getenv("INDEX_ROOT", "/index")
XREF_PATH = Path(INDEX_ROOT) / "xref.jsonl"
META_PATH = Path(INDEX_ROOT) / "faiss" / "meta.jsonl"
LOG_PATH  = os.getenv("XREF_LOG_PATH", f"{INDEX_ROOT}/xref_build.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
FANOUT_MAX = int(os.getenv("XREF_FANOUT_MAX", "32"))
PROG_EVERY = int(os.getenv("XREF_PROGRESS_EVERY", "50000"))

log = logging.getLogger("xref_build")
if not log.handlers:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.DEBUG),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")],
    )

def _load_meta() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not META_PATH.exists():
        log.warning("meta.jsonl not found: %s", META_PATH)
        return out
    with META_PATH.open("r", encoding="utf-8") as f:
        for i, ln in enumerate(f, 1):
            s = ln.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except json.JSONDecodeError:
                continue
            if i % PROG_EVERY == 0:
                log.info("meta read: %d", i)
    log.info("meta loaded items=%d", len(out))
    return out

def _stem(name: str) -> str:
    base = name.rsplit("/", 1)[-1]
    base = base.split(".")[0]
    return base.lower()

def build():
    t0 = time.monotonic()
    meta = _load_meta()
    code_nodes = [m for m in meta if m.get("space") == "CODE" and m.get("type") == "code"]

    by_stem: Dict[str, List[str]] = {}
    for m in code_nodes:
        st = _stem(m.get("path",""))
        by_stem.setdefault(st, []).append(m["id"])

    edges: List[Dict[str, str]] = []
    pat_word = re.compile(r"[A-Za-z0-9_./-]+")
    for i, m in enumerate(meta, 1):
        mid = m.get("id")
        title = (m.get("title") or "")
        snippet = (m.get("snippet") or "")
        text = f"{title} {snippet}"[:2000]
        words = set(w.lower() for w in pat_word.findall(text))
        hits = []
        for w in words:
            if w in by_stem:
                hits.extend(by_stem[w])
        for nb in sorted(set(hits))[:FANOUT_MAX]:
            if nb == mid:
                continue
            edges.append({"src": mid, "dst": nb, "why": "stem-match"})
        if i % PROG_EVERY == 0:
            log.info("edges so far: %d (processed=%d)", len(edges), i)

    # co-stem edges inside code
    for st, ids in by_stem.items():
        up_to = min(FANOUT_MAX, len(ids))
        for i in range(up_to):
            for j in range(i+1, up_to):
                a, b = ids[i], ids[j]
                edges.append({"src": a, "dst": b, "why": "co-stem"})
                edges.append({"src": b, "dst": a, "why": "co-stem"})

    # persist
    tmp = XREF_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for e in edges:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    os.replace(tmp, XREF_PATH)
    log.info("[XREF][DONE] edges=%d path=%s dur_ms=%.1f", len(edges), str(XREF_PATH), (time.monotonic()-t0)*1000.0)

if __name__ == "__main__":
    build()
