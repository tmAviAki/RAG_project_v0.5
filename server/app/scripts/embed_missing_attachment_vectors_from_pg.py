#!/usr/bin/env python3
# File: /tmp/embed_missing_attachment_vectors_from_pg.py
import os, psycopg, sqlite3, httpx, time, random
from psycopg.rows import dict_row
from pathlib import Path

PG_DSN = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
  os.getenv("PGUSER","rag"),
  os.getenv("PGPASSWORD","fabrix"),
  os.getenv("PGHOST","pg"),
  os.getenv("PGPORT","5432"),
  os.getenv("PGDATABASE","rag")
)
DATA_ROOT = os.getenv("DATA_ROOT","/data")
ADO_ROOT  = os.getenv("ADO_ROOT","/ado")
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
EMBED_MODEL = os.getenv("EMBEDDING_MODEL","text-embedding-3-large")
EMBED_DIM = int(os.getenv("EMBED_DIM","3072"))
BATCH = int(os.getenv("BATCH_EMBED","64"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES","6"))
BACKOFF_MIN = float(os.getenv("BACKOFF_BASE_MS","500"))/1000.0
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX_MS","4000"))/1000.0
ATT_MAX_CHARS = int(os.getenv("ATT_MAX_CHARS","12000"))
ATT_MIN_TEXT_CHARS = int(os.getenv("ATT_MIN_TEXT_CHARS","40"))

def sanitize_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\x00","")
    s = "".join(ch if (ord(ch)>=32 or ch in ("\n","\t")) else " " for ch in s)
    s = s.replace("\r\n","\n").replace("\r","\n").strip()
    return s[:ATT_MAX_CHARS] if len(s)>ATT_MAX_CHARS else s

def embed(texts):
    if not OPENAI_API_KEY: raise SystemExit("OPENAI_API_KEY required")
    url="https://api.openai.com/v1/embeddings"; hdr={"Authorization":f"Bearer {OPENAI_API_KEY}"}
    out=[]; i=0
    while i<len(texts):
        block=texts[i:i+BATCH]
        att=0
        while True:
            try:
                r=httpx.post(url, headers=hdr, json={"model":EMBED_MODEL,"input":block}, timeout=120.0)
                if r.status_code in (400,413):
                    if len(block)==1:
                        block=[block[0][: int(ATT_MAX_CHARS*0.6)]]
                        r=httpx.post(url, headers=hdr, json={"model":EMBED_MODEL,"input":block}, timeout=120.0)
                        r.raise_for_status()
                        break
                    # halve and loop
                    half=max(1,len(block)//2)
                    return embed(texts[:i+half]) + embed(texts[i+half:])
                r.raise_for_status(); break
            except Exception as e:
                att+=1
                if att>=MAX_RETRIES: raise
                back=min(BACKOFF_MAX, BACKOFF_MIN*(2**(att-1)))+random.uniform(0,0.25)
                time.sleep(back)
        data=r.json().get("data",[]); vecs=[d["embedding"] for d in data]
        if len(vecs)!=len(block): raise RuntimeError("embedding count mismatch")
        out.extend(vecs); i+=len(block)
    return out

def path_for_rel(rel: str) -> Path:
    if rel.startswith("ADO/"):
        return Path(ADO_ROOT)/"attachments"/rel[len("ADO/"):]
    return Path(DATA_ROOT)/"attachments"/rel

def main():
    pg=psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row); pc=pg.cursor()
    try:
        pc.execute("""
          SELECT d.id, d.title, d.body
          FROM docs d
          LEFT JOIN doc_embeddings e ON e.id=d.id
          WHERE d.id LIKE 'ATT:%' AND e.id IS NULL
        """)
        rows=pc.fetchall()
        if not rows:
            print("[OK] no missing attachment vectors"); return
        # Derive relpath from id: ATT:<cid>:<rel>
        ids, texts = [], []
        for r in rows:
            rid=str(r["id"])
            parts=rid.split(":",2)
            rel = parts[2] if len(parts)>=3 else ""
            p   = path_for_rel(rel)
            txt = None
            try:
                txt = p.read_text("utf-8", errors="ignore")
            except Exception:
                pass
            if not txt or len(txt)<ATT_MIN_TEXT_CHARS:
                # fallback to doc.body/title if file missing/empty
                txt = sanitize_text((r["title"] or "") + "\n\n" + (r["body"] or ""))
            else:
                txt = sanitize_text(txt)
            if not txt or len(txt)<ATT_MIN_TEXT_CHARS:
                continue
            ids.append(rid); texts.append(txt)

        if not ids:
            print("[OK] nothing usable to embed"); return

        vecs = embed(texts)
        if EMBED_DIM==3072:
            sql="INSERT INTO doc_embeddings (id, embedding_full) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_full=EXCLUDED.embedding_full"
        else:
            sql="INSERT INTO doc_embeddings (id, embedding_1536) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_1536=EXCLUDED.embedding_1536"

        pc.executemany(sql, list(zip(ids, vecs)))
        print(f"[DONE] embedded {len(ids)} attachment docs")
    finally:
        pc.close(); pg.close()

if __name__=="__main__":
    main()
