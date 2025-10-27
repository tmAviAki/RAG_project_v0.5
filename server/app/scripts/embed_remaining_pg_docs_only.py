#!/usr/bin/env python3
# File: /tmp/embed_remaining_pg_docs_only.py
import os, time, random, httpx, psycopg
from psycopg.rows import dict_row

PG_DSN = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
  os.getenv("PGUSER","rag"),
  os.getenv("PGPASSWORD","fabrix"),
  os.getenv("PGHOST","pg"),
  os.getenv("PGPORT","5432"),
  os.getenv("PGDATABASE","rag")
)
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()
EMBED_MODEL = os.getenv("EMBEDDING_MODEL","text-embedding-3-large")
EMBED_DIM = int(os.getenv("EMBED_DIM","3072"))
BATCH = int(os.getenv("BATCH_EMBED","64"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES","6"))
BACKOFF_MIN = float(os.getenv("BACKOFF_BASE_MS","500"))/1000.0
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX_MS","4000"))/1000.0
MAX_BODY_CHARS = int(os.getenv("ATT_MAX_CHARS","12000"))

def combined(t,b):
    t=(t or "").strip(); b=(b or "").strip()
    s=(t+("\n\n" if t and b else "")+b).strip()
    s=s.replace("\x00","")
    s="".join(ch if (ord(ch)>=32 or ch in ("\n","\t")) else " " for ch in s)
    return s[:MAX_BODY_CHARS] if len(s)>MAX_BODY_CHARS else s or t or b or "-"

def embed(texts):
    url="https://api.openai.com/v1/embeddings"
    hdr={"Authorization":f"Bearer {OPENAI_API_KEY}"}
    out=[]; i=0
    while i<len(texts):
        block=texts[i:i+BATCH]
        att=0
        while True:
            try:
                r=httpx.post(url, headers=hdr, json={"model":EMBED_MODEL, "input":block}, timeout=120.0)
                if r.status_code in (400,413):
                    if len(block)==1:
                        block=[block[0][: int(MAX_BODY_CHARS*0.6)]]
                        r=httpx.post(url, headers=hdr, json={"model":EMBED_MODEL, "input":block}, timeout=120.0)
                        r.raise_for_status()
                        break
                    half=max(1,len(block)//2)
                    return embed(texts[:i+half]) + embed(texts[i+half:])
                r.raise_for_status(); break
            except Exception as e:
                att+=1
                if att>=MAX_RETRIES: raise
                back=min(BACKOFF_MAX, BACKOFF_MIN*(2**(att-1)))+random.uniform(0,0.25)
                time.sleep(back)
        data=r.json().get("data",[])
        vecs=[d["embedding"] for d in data]
        if len(vecs)!=len(block): raise RuntimeError("count mismatch")
        out.extend(vecs); i+=len(block)
    return out

def main():
    if not OPENAI_API_KEY: raise SystemExit("OPENAI_API_KEY required")
    pg=psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row); pc=pg.cursor()
    pc.execute("""
      SELECT d.id, d.title, d.body
      FROM docs d LEFT JOIN doc_embeddings e ON e.id=d.id
      WHERE d.id LIKE 'ATT:%' AND e.id IS NULL
    """)
    rows=pc.fetchall()
    if not rows:
        print("[OK] nothing to embed"); return
    ids=[str(r["id"]) for r in rows]
    texts=[combined(r["title"], r["body"]) for r in rows]
    vecs=embed(texts)
    if EMBED_DIM==3072:
        sql="INSERT INTO doc_embeddings (id, embedding_full) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_full=EXCLUDED.embedding_full"
    else:
        sql="INSERT INTO doc_embeddings (id, embedding_1536) VALUES (%s,%s) ON CONFLICT (id) DO UPDATE SET embedding_1536=EXCLUDED.embedding_1536"
    pc.executemany(sql, list(zip(ids, vecs)))
    print(f"[DONE] embedded {len(ids)} attachment docs (PG body)")

if __name__=="__main__":
    main()
