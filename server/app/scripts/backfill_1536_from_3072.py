#!/usr/bin/env python3
# Project:RAG_project_v0.5  Component:backfill_1536_from_3072  Version:v0.2.0
import os, math, json, psycopg, numpy as np
from psycopg.rows import dict_row

PG_DSN = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
    os.getenv("PGUSER","rag"),
    os.getenv("PGPASSWORD","fabrix"),
    os.getenv("PGHOST","pg"),
    os.getenv("PGPORT","5432"),
    os.getenv("PGDATABASE","rag"),
)

IN_DIM  = 3072
OUT_DIM = 1536
BATCH   = int(os.getenv("RP_BATCH","2000"))
RP_SEED = int(os.getenv("RP_SEED","0"))

def rp_matrix(out_dim:int, in_dim:int, seed:int) -> np.ndarray:
    rng = np.random.default_rng(seed & 0xFFFFFFFF)
    M = rng.standard_normal((out_dim, in_dim), dtype=np.float32)
    M *= (1.0 / math.sqrt(out_dim))
    return M

def parse_vec_text(vtxt: str) -> np.ndarray:
    # pgvector text output looks like JSON array; parse safely
    # Example: "[0.1, -0.2, ...]"
    arr = json.loads(vtxt)
    v = np.asarray(arr, dtype=np.float32)
    if v.ndim != 1:
        raise ValueError("vector is not 1-D")
    return v

def main():
    rp = rp_matrix(OUT_DIM, IN_DIM, RP_SEED)

    pg = psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)
    cur = pg.cursor()

    cur.execute("""
      SELECT COUNT(*) AS n_missing
      FROM doc_embeddings
      WHERE embedding_full IS NOT NULL
        AND (embedding_1536 IS NULL)
    """)
    n_missing = int(cur.fetchone()["n_missing"])
    print(f"[PLAN] rows needing 1536 backfill: {n_missing}")
    if n_missing == 0:
        print("[OK] nothing to backfill"); return

    processed = 0
    while True:
        cur.execute("""
          SELECT id, embedding_full::text AS vtxt
          FROM doc_embeddings
          WHERE embedding_full IS NOT NULL
            AND (embedding_1536 IS NULL)
          LIMIT %s
        """, (BATCH,))
        rows = cur.fetchall()
        if not rows:
            break

        upd_payload = []
        for r in rows:
            try:
                v = parse_vec_text(r["vtxt"])
                if v.size != IN_DIM:
                    continue
                n = float(np.linalg.norm(v)) or 1.0
                v = (v / n).astype(np.float32, copy=False)
                y = rp @ v
                yn = float(np.linalg.norm(y)) or 1.0
                y = (y / yn).astype(np.float32, copy=False)
                upd_payload.append((y.tolist(), r["id"]))
            except Exception:
                continue

        if upd_payload:
            cur.executemany(
                "UPDATE doc_embeddings SET embedding_1536=%s WHERE id=%s",
                upd_payload
            )

        processed += len(rows)
        print(f"[OK] backfilled batch: {processed}/{n_missing}")

    cur.close(); pg.close()
    print("[DONE] 1536 backfill complete")

if __name__ == "__main__":
    main()
