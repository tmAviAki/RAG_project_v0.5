# Project:Confluence Evidence API  Component:embed_batch  Version:v1.0.0
from __future__ import annotations
import os, json, time, tempfile, uuid
from typing import List, Dict, Tuple
from .embed_cache import EmbedCache

BATCH_COMPLETION_WINDOW = os.getenv("EMBED_BATCH_COMPLETION_WINDOW", "24h")
BATCH_POLL_INTERVAL_SEC = int(os.getenv("EMBED_BATCH_POLL_INTERVAL_SEC", "5"))
BATCH_MAX_INPUTS = int(os.getenv("EMBED_BATCH_MAX_INPUTS", "50000"))
BATCH_INPUT_BYTES_MAX = int(os.getenv("EMBED_BATCH_INPUT_BYTES_MAX", str(200 * 1024 * 1024)))
MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
DIM = int(os.getenv("EMBED_DIM", "1536"))

def _hash_text(t: str) -> str:
    import hashlib
    return hashlib.sha256((t or "").encode("utf-8")).hexdigest()

def build_requests(texts: List[str]) -> Tuple[List[dict], List[str]]:
    reqs, hashes = [], []
    for i, t in enumerate(texts):
        h = _hash_text(t)
        hashes.append(h)
        reqs.append({
            "custom_id": h,
            "method": "POST",
            "url": "/v1/embeddings",
            "body": {"model": MODEL, "input": [t]}
        })
    return reqs, hashes

def write_jsonl(path: str, rows: List[dict]) -> int:
    total = 0
    with open(path, "wb") as f:
        for r in rows:
            line = (json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8")
            total += len(line)
            f.write(line)
    return total

def submit_batch(client, jsonl_path: str) -> str:
    up = client.files.create(file=open(jsonl_path, "rb"), purpose="batch")
    b = client.batches.create(
        input_file_id=up.id,
        endpoint="/v1/embeddings",
        completion_window=BATCH_COMPLETION_WINDOW,
    )
    return b.id

def wait_batch(client, batch_id: str) -> dict:
    while True:
        b = client.batches.retrieve(batch_id)
        if b.status in ("completed", "failed", "cancelled", "expired"):
            return b.to_dict()
        time.sleep(BATCH_POLL_INTERVAL_SEC)

def fetch_output_lines(client, output_file_id: str) -> List[dict]:
    fr = client.files.content(output_file_id)
    content = fr.text
    out = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except:
            continue
    return out

def parse_embeddings(lines: List[dict]) -> Dict[str, List[float]]:
    results: Dict[str, List[float]] = {}
    for row in lines:
        cid = row.get("custom_id")
        resp = row.get("response") or {}
        body = resp.get("body") or {}
        data = body.get("data") or []
        if cid and data and "embedding" in data[0]:
            results[cid] = data[0]["embedding"]
    return results

def run_batch(texts: List[str], cache: EmbedCache):
    from openai import OpenAI
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("INSUFFICIENT CONTEXT — PROVIDE OPENAI_API_KEY")
    client = OpenAI(api_key=key)

    reqs, hashes = build_requests(texts)
    with tempfile.TemporaryDirectory() as td:
        jl = os.path.join(td, f"emb-{uuid.uuid4().hex}.jsonl")
        size = write_jsonl(jl, reqs)
        if size > BATCH_INPUT_BYTES_MAX:
            raise RuntimeError("Batch input too large; split your inputs")
        bid = submit_batch(client, jl)
    info = wait_batch(client, bid)
    if info.get("status") != "completed":
        raise RuntimeError(f"Batch not completed: {info.get('status')}")
    out_id = info.get("output_file_id")
    if not out_id:
        raise RuntimeError("Missing output_file_id")
    lines = fetch_output_lines(client, out_id)
    emap = parse_embeddings(lines)
    tuples = []
    for h, t in zip(hashes, texts):
        vec = emap.get(h)
        if vec:
            tuples.append((h, MODEL, DIM, vec))
    if tuples:
        cache.put_many(tuples)
    return len(tuples)

