#!/usr/bin/env python3
# Project:RAG_project_v0.5  Component:validate_legacy_vs_pg  Version:v0.1.0
"""
Compares coverage of Confluence & ADO between:
- Postgres legacy P2 tables: docs, doc_embeddings
- SQLite legacy index: /index/docs.db (docs, doc_texts, attachments, docs_fts)
- NumpyStore: /index/faiss/vectors.npy + meta.jsonl (optional)

Outputs:
- Counts of docs in PG vs SQLite by space (ADO/non-ADO)
- Embedding coverage in PG (how many docs also have an embedding row)
- NumpyStore-only vector coverage (if present)
- Attachment linkage present only in SQLite (count and sample IDs)
"""

from __future__ import annotations
import os, sys, sqlite3, json
from pathlib import Path
from typing import Dict, Set, Tuple
import psycopg
from psycopg.rows import dict_row

try:
    import numpy as np
except Exception:
    np = None  # optional

# ---- Config (env) ----
SQLITE_PATH = os.getenv("SQLITE_PATH", "/index/docs.db")
FAISS_DIR   = Path(os.getenv("FAISS_DIR", "/index/faiss"))
PG_DSN      = os.getenv("PG_DSN") or f"postgresql://{os.getenv('PGUSER','rag')}:{os.getenv('PGPASSWORD','fabrix')}@{os.getenv('PGHOST','pg')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','rag')}"

SHOW_SAMPLES = int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))

def load_sqlite() -> Tuple[Set[str], Set[str], Dict[str,int]]:
    if not Path(SQLITE_PATH).exists():
        print(f"[WARN] SQLite not found: {SQLITE_PATH}")
        return set(), set(), {}
    con = sqlite3.connect(SQLITE_PATH); con.row_factory = sqlite3.Row
    cur = con.cursor()

    # docs by ADO vs non-ADO
    s_ado = set(r["id"] for r in cur.execute("SELECT id FROM docs WHERE space='ADO'"))
    s_non = set(r["id"] for r in cur.execute("SELECT id FROM docs WHERE space!='ADO' OR space IS NULL"))
    # attachments linkage count per content id (SQLite only)
    attach_counts: Dict[str,int] = {}
    try:
        for r in cur.execute("SELECT content_id, COUNT(*) AS cnt FROM attachments GROUP BY content_id"):
            attach_counts[str(r["content_id"])] = int(r["cnt"] or 0)
    except sqlite3.OperationalError:
        # attachments table may not exist in very early builds
        pass
    con.close()
    return set(map(str, s_ado)), set(map(str, s_non)), attach_counts

def load_pg() -> Tuple[Set[str], Set[str], Set[str]]:
    pg = psycopg.connect(PG_DSN, row_factory=dict_row)
    cur = pg.cursor()
    # docs table (legacy P2)
    cur.execute("SELECT id, space FROM docs")
    ids_pg_ado, ids_pg_non = set(), set()
    for r in cur.fetchall():
        _id = str(r["id"])
        sp  = (r["space"] or "").strip()
        if sp == "ADO":
            ids_pg_ado.add(_id)
        else:
            ids_pg_non.add(_id)

    # doc_embeddings coverage (legacy P2)
    ids_emb: Set[str] = set()
    # embedding table stores doc id as primary key (legacy) OR separate rows
    # Most P2 schemas keep id as PK in doc_embeddings; if not, this select will adapt.
    # Try common layouts:
    have_id_pk = True
    try:
        cur.execute("SELECT id FROM doc_embeddings")
        rows = cur.fetchall()
        for r in rows:
            ids_emb.add(str(r["id"]))
    except Exception:
        have_id_pk = False
    if not have_id_pk:
        # Alternate layouts are rare; if you had (doc_id, embedding) we would need a join.
        # We surface as unknown in this case.
        print("[WARN] Could not read embeddings by id directly (non-standard doc_embeddings).")
    pg.close()
    return ids_pg_ado, ids_pg_non, ids_emb

def load_numpystore() -> Set[str]:
    if np is None:
        print("[INFO] numpy not installed; skipping NumpyStore coverage.")
        return set()
    vec_path = FAISS_DIR / "vectors.npy"
    meta_path = FAISS_DIR / "meta.jsonl"
    if not (vec_path.exists() and meta_path.exists()):
        print(f"[INFO] FAISS/NumpyStore not found under {FAISS_DIR}; skipping.")
        return set()
    try:
        vecs = np.load(vec_path, mmap_mode="r")  # shape [N, D]
        ids: Set[str] = set()
        with meta_path.open("r", encoding="utf-8") as f:
            for i, ln in enumerate(f):
                try:
                    obj = json.loads(ln)
                    _id = obj.get("id")
                    if _id is not None:
                        ids.add(str(_id))
                except Exception:
                    pass
        # sanity: ids size should match vec rows count
        if len(ids) != int(vecs.shape[0]):
            print(f"[WARN] NumpyStore meta rows ({len(ids)}) != vectors rows ({int(vecs.shape[0])})")
        return ids
    except Exception as e:
        print(f"[WARN] Failed to read NumpyStore: {e}")
        return set()

def sample(s: Set[str], k: int) -> str:
    return ", ".join(list(sorted(s))[:k])

def main():
    print("=== VALIDATION: RAG_project_v0.5 coverage (PG vs SQLite/Numpy) ===")
    # Load sources
    ids_sql_ado, ids_sql_non, attach_map = load_sqlite()
    ids_pg_ado,  ids_pg_non,  ids_pg_emb = load_pg()
    ids_np = load_numpystore()

    print("\n-- COUNTS -------------------------------")
    print(f"SQLite docs ADO       : {len(ids_sql_ado)}")
    print(f"SQLite docs Non-ADO   : {len(ids_sql_non)}")
    print(f"PG docs ADO           : {len(ids_pg_ado)}")
    print(f"PG docs Non-ADO       : {len(ids_pg_non)}")
    print(f"PG emb (doc_embeddings): {len(ids_pg_emb)} (rows by id)")
    print(f"NumpyStore vectors    : {len(ids_np)}")

    # Coverage deltas: who exists in SQLite but not in PG (missing ingest)
    miss_pg_ado = ids_sql_ado - ids_pg_ado
    miss_pg_non = ids_sql_non - ids_pg_non
    # Embedding coverage: docs in PG but not in doc_embeddings
    need_emb_ado = ids_pg_ado - ids_pg_emb if ids_pg_emb else set()
    need_emb_non = ids_pg_non - ids_pg_emb if ids_pg_emb else set()
    # Numpy-only docs (in Numpy meta but not in PG)
    numpy_only = ids_np - (ids_pg_ado | ids_pg_non) if ids_np else set()

    print("\n-- MISSING IN PG ------------------------")
    print(f"SQLite→PG ADO missing : {len(miss_pg_ado)}")
    if miss_pg_ado: print(f"  e.g. {sample(miss_pg_ado, SHOW_SAMPLES)}")
    print(f"SQLite→PG NonADO missing: {len(miss_pg_non)}")
    if miss_pg_non: print(f"  e.g. {sample(miss_pg_non, SHOW_SAMPLES)}")

    print("\n-- PG DOCS WITHOUT EMBEDDINGS ----------")
    if ids_pg_emb:
        print(f"PG ADO need embeddings   : {len(need_emb_ado)}")
        if need_emb_ado: print(f"  e.g. {sample(need_emb_ado, SHOW_SAMPLES)}")
        print(f"PG NonADO need embeddings: {len(need_emb_non)}")
        if need_emb_non: print(f"  e.g. {sample(need_emb_non, SHOW_SAMPLES)}")
    else:
        print("Could not determine embedding rows by id (non-standard doc_embeddings schema)")

    print("\n-- NUMPYSTORE-ONLY DOCS ----------------")
    print(f"in NumpyStore but not in PG: {len(numpy_only)}")
    if numpy_only: print(f"  e.g. {sample(numpy_only, SHOW_SAMPLES)}")

    print("\n-- ATTACHMENT LINKAGE (SQLite only) ----")
    total_att = sum(attach_map.values())
    print(f"SQLite attachments total  : {total_att}")
    # of these, how many are for content ids present in PG?
    pg_ids_all = (ids_pg_ado | ids_pg_non)
    att_on_pg_ids = sum(cnt for cid, cnt in attach_map.items() if cid in pg_ids_all)
    att_on_sql_only_ids = total_att - att_on_pg_ids
    print(f"Attachments whose content exists in PG docs   : {att_on_pg_ids}")
    print(f"Attachments on docs missing from PG (SQLite)  : {att_on_sql_only_ids}")
    if att_on_sql_only_ids > 0:
        # sample a few content ids missing in PG to make the gap obvious
        ex_ids = [cid for cid in attach_map if (cid not in pg_ids_all)]
        print(f"  example content_ids: {', '.join(ex_ids[:SHOW_SAMPLES])}")

    # Verdict
    print("\n=== VERDICT =============================")
    conditions = []
    if not miss_pg_ado and not miss_pg_non:
        conditions.append("ALL SQLite docs are present in Postgres.")
    else:
        conditions.append("Some SQLite docs are missing in Postgres.")

    if ids_pg_emb:
        if not need_emb_ado and not need_emb_non:
            conditions.append("All PG docs have embeddings in doc_embeddings.")
        else:
            conditions.append("Some PG docs do not have embeddings (doc_embeddings).")

    if ids_np is not None and len(ids_np) > 0:
        if len(numpy_only) == 0:
            conditions.append("No docs exist only in NumpyStore (FAISS).")
        else:
            conditions.append("Some docs exist only in NumpyStore, not in PG.")

    if att_on_sql_only_ids == 0:
        conditions.append("All attachment linkages from SQLite map to docs present in PG.")
    else:
        conditions.append("Some attachments link to docs absent from PG.")

    for c in conditions:
        print(f"- {c}")

    # Final recommendation
    if (not miss_pg_ado and not miss_pg_non and
        (not ids_pg_emb or (not need_emb_ado and not need_emb_non)) and
        (ids_np is None or len(numpy_only) == 0) and
        att_on_sql_only_ids == 0):
        print("\nRecommendation: Postgres is authoritative; proceed with new direct-to-PG ingestion.")
    else:
        print("\nRecommendation: Gaps detected. Migrate missing docs/embeddings (and attachments) into PG before deprecating SQLite/Numpy.")
        print("Use: scripts/migrate_sqlite_to_pg.py (provided earlier) or your tailored migrators.")
    print("========================================")
    
if __name__ == "__main__":
    main()
