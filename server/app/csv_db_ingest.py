# server/app/csv_db_ingest.py
# Project: confAdogpt  Component: csv_db_ingest  Version: v0.2.0
from __future__ import annotations
import os, sys, csv, time, json, hashlib, sqlite3, signal, argparse
from pathlib import Path
from typing import Dict, List, Iterable, Tuple, Optional

from .rag_store import NumpyStore, VSConfig
from .embeddings import get_embedder

LOG = None
_STOP = False

# --------------------------- Logging & OTEL ---------------------------
def _log_setup():
    import logging
    global LOG
    LOG = logging.getLogger("csv_ingest")
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s", "%Y-%m-%d %H:%M:%S")
    h.setFormatter(fmt)
    LOG.setLevel(os.getenv("LOG_LEVEL","INFO").upper())
    LOG.addHandler(h)

class _NoopSpan:
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

class _Tracer:
    def __init__(self):
        self._enabled = False
        self._span = _NoopSpan
        ep = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if ep:
            try:
                # soft import; if not present we silently no-op
                from opentelemetry import trace  # type: ignore
                from opentelemetry.sdk.resources import Resource  # type: ignore
                from opentelemetry.sdk.trace import TracerProvider  # type: ignore
                from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter  # type: ignore
                provider = TracerProvider(resource=Resource.create({"service.name": "csv_db_ingest"}))
                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=ep)))
                trace.set_tracer_provider(provider)
                self._tracer = trace.get_tracer("csv_db_ingest")
                self._enabled = True
                self._span = self._tracer.start_as_current_span
            except Exception:
                self._enabled = False
                self._span = _NoopSpan
        else:
            self._enabled = False
            self._span = _NoopSpan
    def span(self, name: str):
        return self._span(name)

_TRACER = _Tracer()

# --------------------------- Signals ---------------------------
for _sig in (signal.SIGINT, signal.SIGTERM):
    def _handler(signum, frame):
        global _STOP
        _STOP = True
        if LOG: LOG.warning("[CSV] signal %s received; will stop after current batch…", signum)
    signal.signal(_sig, _handler)

# --------------------------- Cache (skip unchanged CSVs) ---------------------------
def _cache_conn(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("""CREATE TABLE IF NOT EXISTS files(
        path TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        mtime INTEGER NOT NULL,
        rows INTEGER NOT NULL,
        last_run TEXT NOT NULL
    )""")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con

def _stat_key(p: Path) -> Tuple[int,int]:
    st = p.stat()
    return (int(st.st_size), int(st.st_mtime))

def _cache_hit(con: sqlite3.Connection, p: Path) -> Optional[Dict]:
    row = con.execute("SELECT size, mtime, rows, last_run FROM files WHERE path=?",(str(p),)).fetchone()
    if not row: return None
    return {"size": row[0], "mtime": row[1], "rows": row[2], "last_run": row[3]}

def _cache_put(con: sqlite3.Connection, p: Path, size: int, mtime: int, rows: int, run_id: str):
    con.execute("INSERT OR REPLACE INTO files(path,size,mtime,rows,last_run) VALUES (?,?,?,?,?)",
                (str(p), size, mtime, rows, run_id))

# --------------------------- Utilities ---------------------------
def _approx_tokens(s: str) -> int:
    return (len(s) + 3)//4 if s else 0

def _hash_row(customer: str, table: str, key: str, text: str) -> str:
    h = hashlib.sha256()
    h.update(customer.encode()); h.update(b"\x00")
    h.update(table.encode());    h.update(b"\x00")
    h.update(key.encode());      h.update(b"\x00")
    h.update((text or "").encode())
    return h.hexdigest()

def _row_to_text(row: Dict[str,str], max_cols: int, max_chars: int) -> str:
    items = []
    n = 0
    for k,v in row.items():
        if n>=max_cols: break
        ks = "" if k is None else str(k)
        vs = "" if v is None else str(v)
        items.append(f"{ks}={vs}")
        n+=1
    s = "; ".join(items)
    if len(s) > max_chars:
        s = s[:max_chars] + " …"
    return s

def _build_item(customer: str, table: str, key: str, text: str) -> Dict:
    """
    IMPORTANT: space is FIXED to "DB" (one global bucket).
    """
    return {
        "id": f"DB:{customer}:{table}:{key}",
        "space": "DB",
        "type": "db_row",
        "title": f"{customer}/{table}#{key}",
        "url": f"/v1/fetch?ids={customer}/{table}#{key}",
        "chunk_ix": 0,
        "updated_at": "",
        "snippet": (text or "")[:300],
        "customer": customer,
        "table": table,
        "row_key": key,
    }

# --------------------------- Dialect handling ---------------------------
def _open_csv(path: Path, sniff_bytes: int, dialect_name: Optional[str]) -> Tuple[csv.DictReader, any]:
    f = path.open("r", encoding="utf-8", errors="ignore", newline="")
    if dialect_name:
        try:
            reader = csv.DictReader(f, dialect=dialect_name)
            return reader, f
        except Exception:
            f.seek(0)
    # Sniff
    try:
        sample = f.read(sniff_bytes)
        f.seek(0)
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        reader = csv.DictReader(f, dialect=dialect)
        return reader, f
    except Exception:
        f.seek(0)
        reader = csv.DictReader(f)  # fall back to defaults
        return reader, f

def _iter_csv_rows(path: Path, max_rows: Optional[int], sniff_bytes: int, dialect_name: Optional[str]) -> Iterable[Dict[str,str]]:
    reader, f = _open_csv(path, sniff_bytes, dialect_name)
    n = 0
    try:
        for row in reader:
            yield { (k or "").strip(): ("" if v is None else str(v)) for k,v in (row or {}).items() }
            n += 1
            if max_rows is not None and n >= max_rows:
                break
    finally:
        try: f.close()
        except Exception: pass

def _pick_key(row: Dict[str,str], fallback_idx: int) -> str:
    for cand in ("id","ID","Id","pk","PK","key","KEY","Key"):
        if cand in row and row[cand] != "":
            return str(row[cand])
    return f"row{fallback_idx}"

# --------------------------- Pacing / Limits ---------------------------
class _Pacer:
    def __init__(self, tpm:int, rpm:int, headroom:float):
        self.tpm = max(0, tpm)
        self.rpm = max(0, rpm)
        self.head = max(0.1, min(headroom, 1.0))
        self._tok = 0
        self._req = 0
        self._win_start = time.monotonic()
    def account(self, tokens:int, requests:int) -> None:
        self._tok += max(0, tokens)
        self._req += max(0, requests)
    def maybe_sleep(self):
        if (self.tpm<=0 and self.rpm<=0): return
        elapsed = time.monotonic() - self._win_start
        if elapsed >= 60.0:
            self._tok = 0; self._req = 0; self._win_start = time.monotonic()
            return
        sleep_s = 0.0
        if self.tpm>0:
            limit_tok = self.tpm * self.head
            if self._tok > limit_tok:
                sleep_s = max(sleep_s, 60.0 - elapsed)
        if self.rpm>0:
            limit_req = self.rpm * self.head
            if self._req > limit_req:
                sleep_s = max(sleep_s, 60.0 - elapsed)
        if sleep_s>0:
            LOG.info("[CSV][PACE] sleeping %.2fs (rpm=%d/%d tpm=%d/%d)", sleep_s, self._req, self.rpm, self._tok, self.tpm)
            time.sleep(sleep_s)
            self._tok = 0; self._req = 0; self._win_start = time.monotonic()

# --------------------------- Main ingest ---------------------------
def ingest_csv_tree(
    csv_root: Path,
    customer: str,
    table_glob: str,
    max_rows_per_file: Optional[int],
    max_cols: int,
    row_text_chars: int,
    batch_embed: int,
    run_id: str,
    cache_path: Path,
    sniff_bytes: int,
    dialect_name: Optional[str],
    fail_fast: bool,
) -> Dict[str,int]:
    with _TRACER.span("csv_ingest.run"):
        LOG.info("[CSV] start csv_root=%s customer=%s table_glob=%s batch=%d", csv_root, customer, table_glob, batch_embed)
        store = NumpyStore(VSConfig())
        dim = store.effective_dim()
        embedder = get_embedder(dim_hint=dim)

        ccon = _cache_conn(str(cache_path))

        files = sorted(csv_root.rglob(table_glob))
        files = [p for p in files if p.is_file() and p.suffix.lower()==".csv"]
        LOG.info("[CSV] candidate files: %d", len(files))

        staged_meta: List[Dict] = []
        staged_text: List[str] = []

        total_rows = 0
        skipped_files = 0
        processed_files = 0
        embedded_chunks = 0
        embed_batches = 0
        cache_hits_files = 0

        pacer = _Pacer(
            tpm=int(os.getenv("EMBED_TPM_LIMIT","1000000")),
            rpm=int(os.getenv("EMBED_RPM_LIMIT","5000")),
            headroom=float(os.getenv("EMBED_HEADROOM","0.90"))
        )

        def _flush():
            nonlocal embedded_chunks, embed_batches
            if not staged_text: return
            with _TRACER.span("csv_ingest.embed_batch"):
                # pacing (approx token estimate)
                toks = sum(_approx_tokens(t) for t in staged_text)
                pacer.account(tokens=toks, requests=1)
                pacer.maybe_sleep()
                embs = embedder.embed_texts(staged_text)
                chunks = []
                for m, e in zip(staged_meta, embs):
                    ch = dict(m)
                    ch["embedding"] = e
                    chunks.append(ch)
                n = store.upsert(chunks)
                embedded_chunks += n
                embed_batches += 1
                staged_meta.clear(); staged_text.clear()

        for p in files:
            if _STOP: break
            size, mtime = _stat_key(p)
            hit = _cache_hit(ccon, p)
            if hit and hit["size"]==size and hit["mtime"]==mtime:
                skipped_files += 1
                cache_hits_files += 1
                continue

            processed_files += 1
            LOG.info("[CSV][SCAN] %s (size=%d mtime=%d)", p, size, mtime)

            row_idx = 0
            new_rows = 0
            try:
                for row in _iter_csv_rows(p, max_rows_per_file, sniff_bytes, dialect_name):
                    if _STOP: break
                    row_idx += 1
                    key = _pick_key(row, row_idx)
                    text = _row_to_text(row, max_cols=max_cols, max_chars=row_text_chars)
                    meta = _build_item(customer=customer, table=p.stem, key=key, text=text)
                    staged_meta.append(meta)
                    staged_text.append(text)
                    new_rows += 1
                    total_rows += 1

                    if len(staged_text) >= batch_embed:
                        _flush()
                _flush()
                _cache_put(ccon, p, size=size, mtime=mtime, rows=new_rows, run_id=run_id)
                ccon.commit()
                LOG.info("[CSV][OK] %s rows=%d batches=%d", p.name, new_rows, embed_batches)
            except Exception as e:
                LOG.error("[CSV][ERR] %s row=%d err=%s", p, row_idx, e)
                if fail_fast:
                    raise
                # else keep going with next file

        # Final alignment verification
        vec_path = Path(VSConfig().root) / VSConfig().store_dir / "vectors.npy"
        meta_path = Path(VSConfig().root) / VSConfig().store_dir / "meta.jsonl"
        try:
            import numpy as _np
            nv = int(_np.load(vec_path, mmap_mode="r").shape[0]) if vec_path.exists() else 0
        except Exception:
            nv = 0
        try:
            nm = sum(1 for _ in meta_path.open("r", encoding="utf-8")) if meta_path.exists() else 0
        except Exception:
            nm = 0
        aligned = (nv == nm)
        LOG.info("[CSV] end run_id=%s files: processed=%d skipped=%d rows=%d chunks_embedded=%d batches=%d cache_hits=%d aligned=%s nv=%d nm=%d",
                 run_id, processed_files, skipped_files, total_rows, embedded_chunks, embed_batches, cache_hits_files, aligned, nv, nm)

        return {
            "files_processed": processed_files,
            "files_skipped": skipped_files,
            "rows": total_rows,
            "chunks_embedded": embedded_chunks,
            "embed_batches": embed_batches,
            "cache_hits_files": cache_hits_files,
            "aligned": int(aligned),
            "nv": nv,
            "nm": nm,
        }

def main():
    _log_setup()
    ap = argparse.ArgumentParser(description="CSV → Vector store ingest (space='DB' unified)")
    ap.add_argument("--csv-root", default=os.getenv("CSV_ROOT","/csv"),
                    help="Root folder containing customer CSVs (e.g. /mnt/disks/data/CustomersDB/OBE/csv/)")
    ap.add_argument("--customer", required=True, help="Customer label (e.g. OBE). Stored in meta, space remains 'DB'.")
    ap.add_argument("--table-glob", default="*.csv", help="Glob under csv-root (default: *.csv)")
    ap.add_argument("--max-rows-per-file", type=int, default=None, help="Optional cap for huge files")
    ap.add_argument("--max-cols", type=int, default=int(os.getenv("CSV_MAX_COLS","80")))
    ap.add_argument("--row-text-chars", type=int, default=int(os.getenv("CSV_ROW_TEXT_CHARS","4000")))
    ap.add_argument("--batch-embed", type=int, default=int(os.getenv("CSV_BATCH_EMBED","256")))
    ap.add_argument("--cache-path", default=os.getenv("CSV_CACHE_PATH","/index/csv_ingest_cache.sqlite"))
    ap.add_argument("--run-id", default=time.strftime("run-%Y%m%d-%H%M%S"))
    ap.add_argument("--sniff-bytes", type=int, default=int(os.getenv("CSV_SNIFF_BYTES","200000")))
    ap.add_argument("--dialect", default=os.getenv("CSV_DIALECT",""), help="Optional csv dialect name")
    ap.add_argument("--fail-fast", action="store_true", default=(os.getenv("CSV_FAIL_FAST","0")=="1"))
    args = ap.parse_args()

    root = Path(args.csv_root).resolve()
    if not root.exists():
        LOG.error("[CSV] csv-root not found: %s", root); sys.exit(2)

    with _TRACER.span("csv_ingest.main"):
        stats = ingest_csv_tree(
            csv_root=root,
            customer=args.customer.strip(),
            table_glob=args.table_glob,
            max_rows_per_file=args.max_rows_per_file,
            max_cols=args.max_cols,
            row_text_chars=args.row_text_chars,
            batch_embed=args.batch_embed,
            run_id=args.run_id,
            cache_path=Path(args.cache_path),
            sniff_bytes=int(args.sniff_bytes),
            dialect_name=(args.dialect or None),
            fail_fast=bool(args.fail_fast),
        )
        # Persist a run report
        report_path = Path("/index")/f"csv_ingest_report_{args.customer}_{args.run_id}.json"
        try:
            report_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
        except Exception:
            pass
        print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()

