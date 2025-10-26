# Project:RAG_project_v0.5 Component:scripts.ingest_pgvector Version:v0.8.7
from __future__ import annotations
import os, sys, json, time, signal, hashlib, logging, traceback, math, threading
from pathlib import Path
from typing import List, Dict, Any, Iterable, Tuple, Optional

from ..db.pg import get_conn, ensure_extensions
from ..embeddings import get_embedder
from ..otel import maybe_span
from ..reduction import get_reducer

__version__ = "v0.8.7"

def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1","true","yes","on"}

DATA_ROOT     = Path(os.getenv("DATA_ROOT", "/data"))
INDEX_ROOT    = Path(os.getenv("INDEX_ROOT", "/index"))
SNAPSHOT_PATH = INDEX_ROOT / "ingest_pgvector.snapshot.json"

EMBED_DIM     = int(os.getenv("EMBED_DIM", "3072"))
ALLOW_REMOTE  = _env_bool("ALLOW_REMOTE_EMBEDDINGS", "0")
WATCH         = _env_bool("INGEST_WATCH", "1")
POLL_SEC      = int(os.getenv("INGEST_POLL_SEC", "300"))
BATCH_SIZE    = int(os.getenv("EMBED_BATCH", "64"))
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_TO     = int(os.getenv("OPENAI_TIMEOUT_SECS", "30"))
MODEL_HINT    = os.getenv("EMBEDDING_MODEL", os.getenv("MODEL", ""))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-7s ingest_pg %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ingest_pg")

_STOP = False
def _graceful(sig: int, _frame):
    global _STOP
    try: name = signal.Signals(sig).name
    except Exception: name = str(sig)
    log.info("Received %s — finishing current step then exiting", name)
    _STOP = True

signal.signal(signal.SIGINT, _graceful)
signal.signal(signal.SIGTERM, _graceful)

def _iter_ndjson(p: Path) -> Iterable[Dict[str, Any]]:
    if not p.exists(): return
    with p.open("r", encoding="utf-8") as fh:
        for ln in fh:
            s = ln.strip()
            if not s: continue
            try: yield json.loads(s)
            except json.JSONDecodeError: continue

def scan_pages(root: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    spaces = root / "spaces"
    rows: List[Dict[str, Any]] = []
    files: List[str] = []
    total_sz = 0; total_files = 0
    if not spaces.exists():
        return rows, {"files": [], "fingerprint": "", "total_files": 0, "total_size": 0}
    for sp in sorted(spaces.glob("*")):
        for name in ("page.ndjson","blogpost.ndjson"):
            p = sp / name
            if p.exists():
                files.append(str(p))
                st = p.stat()
                total_sz += st.st_size; total_files += 1
                rows.extend(_iter_ndjson(p))
    h = hashlib.sha256()
    for f in files:
        st = Path(f).stat()
        h.update(f.encode()); h.update(str(st.st_size).encode()); h.update(str(st.st_mtime_ns).encode())
    meta = {"files": files, "fingerprint": h.hexdigest(), "total_files": total_files, "total_size": total_sz}
    return rows, meta

def _read_snapshot(p: Path) -> Dict[str, Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except Exception:
        return {}

def _write_snapshot(p: Path, meta: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(p)

def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS docs (
                  id TEXT PRIMARY KEY,
                  title TEXT,
                  space TEXT,
                  url TEXT,
                  body TEXT,
                  tsv  tsvector
                )""")
            cur.execute("CREATE INDEX IF NOT EXISTS docs_tsv_idx ON docs USING GIN (tsv)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS doc_embeddings (
                  id              TEXT PRIMARY KEY REFERENCES docs(id) ON DELETE CASCADE,
                  embedding_full  vector(3072),
                  embedding_1536  vector(1536)
                )""")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS doc_embeddings_ivfflat
                ON doc_embeddings USING ivfflat (embedding_1536 vector_l2_ops)
                WITH (lists = 100)
            """)
    log.info("Schema ensured")

def upsert_docs(rows: List[Dict[str, Any]]) -> int:
    n = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                pid   = str(r.get("id"))
                title = r.get("title") or ""
                space = r.get("space") or r.get("key") or ""
                url   = r.get("url") or None
                body  = (r.get("body") or r.get("content") or "")[:200000]
                cur.execute("""
                    INSERT INTO docs (id, title, space, url, body, tsv)
                    VALUES (%s,%s,%s,%s,%s, to_tsvector('english', coalesce(%s,'') || ' ' || coalesce(%s,'')))
                    ON CONFLICT (id) DO UPDATE SET
                      title = EXCLUDED.title,
                      space = EXCLUDED.space,
                      url   = EXCLUDED.url,
                      body  = EXCLUDED.body,
                      tsv   = EXCLUDED.tsv
                """, (pid, title, space, url, body, title, body))
                n += 1
                if n % 1000 == 0: log.info("Docs upserted: %d", n)
    return n

def _call_with_timeout(fn, timeout_s: int) -> Optional[object]:
    res = {"obj": None, "err": None}
    def _runner():
        try: res["obj"] = fn()
        except Exception as e: res["err"] = e
    th = threading.Thread(target=_runner, daemon=True); th.start()
    th.join(timeout_s)
    if th.is_alive(): return None
    if res["err"] is not None: raise res["err"]
    return res["obj"]

def upsert_embeddings(rows: List[Dict[str, Any]], embed_dim: int) -> int:
    key = os.getenv("OPENAI_API_KEY", "")
    key_mask = f"{key[:7]}…" if key else "<missing>"
    log.info("Embedding stage: enabled=%s | ALLOW_REMOTE_EMBEDDINGS(raw)=%s | API_KEY=%s | model_hint=%s | dim=%s | batch=%s | timeout=%ss",
             ALLOW_REMOTE, os.getenv("ALLOW_REMOTE_EMBEDDINGS",""),
             ("SET:"+key_mask) if key else "MISSING",
             (os.getenv('EMBEDDING_MODEL') or os.getenv('MODEL') or "<unset>"),
             embed_dim, BATCH_SIZE, OPENAI_TO)
    if not ALLOW_REMOTE:
        log.info("Skipping embeddings: ALLOW_REMOTE_EMBEDDINGS is falsey"); return 0
    if not key:
        log.warning("Skipping embeddings: OPENAI_API_KEY not set"); return 0

    hard = max(5, OPENAI_TO + 5)
    log.info("Creating embedder (hard timeout %ss)…", hard)
    try:
        emb = _call_with_timeout(lambda: get_embedder(dim_hint=embed_dim), hard)
    except Exception as e:
        log.error("get_embedder() raised: %s", e); log.debug("Trace:\n%s", traceback.format_exc())
        return 0
    if emb is None:
        log.error("get_embedder() timed out after %ss — skipping embeddings this run", hard)
        return 0

    t0_red = time.monotonic()
    reducer = get_reducer()  # 3072 -> 1536
    log.info("Reducer ready in %.2fs (in=%d -> out=%d)", time.monotonic() - t0_red,
             getattr(reducer, "in_dim", 0), getattr(reducer, "out_dim", 0))

    total = 0
    batches = max(1, math.ceil(len(rows)/BATCH_SIZE))
    log.info("Embedding work plan: items=%d batch_size=%d batches=%d", len(rows), BATCH_SIZE, batches)

    if os.getenv("INGEST_EMBED_PREFLIGHT","0") == "1":
        try:
            _ = emb.embed_texts(["__ingest_pgvector_preflight__"]); log.debug("Preflight embedding OK")
        except Exception as e:
            log.error("Preflight embedding failed: %s", e); return 0

    with maybe_span("rag.embed"):
        for bi in range(batches):
            if _STOP: break
            lo = bi * BATCH_SIZE
            hi = min((bi+1)*BATCH_SIZE, len(rows))
            chunk = rows[lo:hi]
            ids   = [str(r.get("id")) for r in chunk]
            texts = [(r.get("title") or "") + "\n" + (r.get("body") or "") for r in chunk]

            log.info("Batch %d/%d — requesting embeddings for %d items (ids %s..%s)",
                     bi+1, batches, len(texts), ids[0] if ids else "-", ids[-1] if ids else "-")

            t0 = time.monotonic()
            try:
                vecs_full = emb.embed_texts(texts)
            except Exception as e:
                log.error("embed_texts() failed at batch %d: %s", bi+1, e)
                log.debug("Trace:\n%s", traceback.format_exc()); break
            t1 = time.monotonic()
            log.info("Batch %d/%d — embeddings OK in %.2fs", bi+1, batches, t1-t0)

            try:
                t2 = time.monotonic()
                vecs_1536 = [reducer.reduce(v) for v in vecs_full]
                t3 = time.monotonic()
                log.debug("Batch %d/%d — reduction OK in %.2fs", bi+1, batches, t3-t2)
            except Exception as e:
                log.error("reduction failed at batch %d: %s", bi+1, e)
                log.debug("Trace:\n%s", traceback.format_exc()); break

            with get_conn() as conn:
                with conn.cursor() as cur:
                    for pid, vf, v1536 in zip(ids, vecs_full, vecs_1536):
                        cur.execute("""
                            INSERT INTO doc_embeddings (id, embedding_full, embedding_1536)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                              embedding_full = EXCLUDED.embedding_full,
                              embedding_1536 = EXCLUDED.embedding_1536
                        """, (pid, vf, v1536))
                        total += 1

            log.info("Batch %d/%d — upserted %d items (cumulative %d)", bi+1, batches, len(ids), total)

    log.info("Embedding stage complete — total vectors stored: %d", total)
    return total

def run_once() -> Tuple[int, int]:
    ensure_extensions()
    ensure_schema()
    rows, meta = scan_pages(DATA_ROOT)
    log.info("Scanned corpus: files=%d size=%.2fMB rows=%d fingerprint=%s",
             meta.get("total_files", 0), meta.get("total_size", 0)/1e6, len(rows),
             (meta.get("fingerprint") or "")[:12])
    if not rows:
        if not (DATA_ROOT / "spaces").exists():
            log.warning("No content under %s", DATA_ROOT / "spaces")
        else:
            log.info("No rows found in NDJSON")
        _write_snapshot(SNAPSHOT_PATH, meta); return 0, 0

    prev = _read_snapshot(SNAPSHOT_PATH)
    if prev.get("fingerprint") == meta.get("fingerprint"):
        log.info("No changes since last run (fingerprint match)"); return 0, 0

    n_docs = upsert_docs(rows)
    n_vecs = upsert_embeddings(rows, EMBED_DIM)
    _write_snapshot(SNAPSHOT_PATH, meta)
    log.info("Run complete — docs=%d, vectors=%d", n_docs, n_vecs)
    return n_docs, n_vecs

def main():
    log.info("Start ingest_pgvector %s | DATA_ROOT=%s | INDEX_ROOT=%s | WATCH=%s | POLL=%ss | EMBED_DIM=%s | BATCH=%s | OPENAI_TIMEOUT_SECS=%s | MODEL_HINT=%s",
             __version__, str(DATA_ROOT), str(INDEX_ROOT), WATCH, POLL_SEC, EMBED_DIM, BATCH_SIZE, OPENAI_TO,
             (MODEL_HINT or "<unset>"))
    try:
        if not WATCH:
            run_once(); log.info("Exit (one-shot) OK"); return
        while True:
            docs_cnt, vecs_cnt = run_once()
            log.info("Sleeping %ss (CTRL+C to stop). Last run: docs=%d vectors=%d",
                     POLL_SEC, docs_cnt, vecs_cnt)
            for _ in range(POLL_SEC):
                if _STOP:
                    log.info("Stop requested; exiting."); return
                time.sleep(1)
    except Exception:
        log.error("Unhandled exception:\n%s", traceback.format_exc())
        raise

if __name__ == "__main__":
    os.environ["PYTHONUNBUFFERED"] = "1"
    main()
