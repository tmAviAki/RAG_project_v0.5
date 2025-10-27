#!/usr/bin/env python3
# Project:RAG_project_v0.5  Component:validate_legacy_vs_pg  Version:v0.1.1 (Py3.6 compatible)
"""
Compares coverage of Confluence & ADO between:
- Postgres legacy P2 tables: docs, doc_embeddings
- SQLite legacy index: /index/docs.db (docs, doc_texts, attachments)
- NumpyStore: /index/faiss/{vectors.npy, meta.jsonl} (optional)
Outputs counts + sample IDs; read-only.
"""

import os, sys, sqlite3, json
from pathlib import Path

# --- Try psycopg3 first, fallback to psycopg2 (Py3.6 often has psycopg2 installed) ---
_psycopg = None
try:
    import psycopg
    _psycopg = "psycopg3"
except Exception:
    try:
        import psycopg2 as psycopg
        from psycopg2.extras import RealDictCursor
        _psycopg = "psycopg2"
    except Exception:
        print("[FATAL] Need psycopg (v3) or psycopg2 installed. Try: pip install psycopg2-binary")
        sys.exit(2)

# numpy is optional
try:
    import numpy as np  # noqa
    HAVE_NUMPY = True
except Exception:
    HAVE_NUMPY = False

SQLITE_PATH = os.getenv("SQLITE_PATH", "/index/docs.db")
FAISS_DIR   = Path(os.getenv("FAISS_DIR", "/index/faiss"))
PG_DSN      = os.getenv("PG_DSN") or "postgresql://%s:%s@%s:%s/%s" % (
    os.getenv('PGUSER','rag'),
    os.getenv('PGPASSWORD','fabrix'),
    os.getenv('PGHOST','pg'),
    os.getenv('PGPORT','5432'),
    os.getenv('PGDATABASE','rag'),
)
SHOW_SAMPLES = int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))

def load_sqlite():
    """Return (ids_ado_sqlite, ids_non_sqlite, attach_count_map) as sets/dict of strings."""
    if not Path(SQLITE_PATH).exists():
        print("[WARN] SQLite not found: %s" % SQLITE_PATH)
        return set(), set(), {}
    con = sqlite3.connect(SQLITE_PATH); con.row_factory = sqlite3.Row
    cur = con.cursor()
    ado = set([str(r["id"]) for r in cur.execute("SELECT id FROM docs WHERE space='ADO'")])
    non = set([str(r["id"]) for r in cur.execute("SELECT id FROM docs WHERE space!='ADO' OR space IS NULL")])
    attach = {}
    try:
        for r in cur.execute("SELECT content_id, COUNT(*) AS cnt FROM attachments GROUP BY content_id"):
            cid = str(r["content_id"])
            attach[cid] = int(r["cnt"] or 0)
    except sqlite3.OperationalError:
        pass
    con.close()
    return ado, non, attach

def load_pg():
    """Return (ids_pg_ado, ids_pg_non, ids_pg_emb) as sets of strings."""
    ids_pg_ado, ids_pg_non, ids_pg_emb = set(), set(), set()
    if _psycopg == "psycopg3":
        cx = psycopg.connect(PG_DSN)  # dict_row not available on Py3.6
        cur = cx.cursor()
    else:
        cx = psycopg.connect(PG_DSN)
        cur = cx.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT id, space FROM docs")
    for row in cur.fetchall():
        # row can be tuple or dict depending on driver
        if isinstance(row, tuple):
            _id, sp = row[0], row[1]
        else:
            _id, sp = row["id"], row["space"]
        if (sp or "").strip() == "ADO":
            ids_pg_ado.add(str(_id))
        else:
            ids_pg_non.add(str(_id))

    # embeddings id coverage (common case: doc_embeddings(id ...))
    try:
        cur.execute("SELECT id FROM doc_embeddings")
        for row in cur.fetchall():
            if isinstance(row, tuple):
                ids_pg_emb.add(str(row[0]))
            else:
                ids_pg_emb.add(str(row["id"]))
    except Exception:
        print("[WARN] Could not read embeddings by id directly (non-standard doc_embeddings schema).")
        ids_pg_emb = set()

    cx.close()
    return ids_pg_ado, ids_pg_non, ids_pg_emb

def load_numpystore():
    """Return set of ids in /index/faiss/meta.jsonl (size should match vectors.npy rows)."""
    if not HAVE_NUMPY:
        print("[INFO] numpy not installed; skipping NumpyStore coverage.")
        return set()
    vec_path = FAISS_DIR / "vectors.npy"
    meta_path = FAISS_DIR / "meta.jsonl"
    if not (vec_path.exists() and meta_path.exists()):
        print("[INFO] FAISS/NumpyStore not found under %s; skipping." % FAISS_DIR)
        return set()
    try:
        vecs = __import__("numpy").load(str(vec_path), mmap_mode="r")
        ids = set()
        with meta_path.open("r", encoding="utf-8") as f:
            for ln in f:
                try:
                    obj = json.loads(ln)
                    _id = obj.get("id")
                    if _id is not None:
                        ids.add(str(_id))
                except Exception:
                    pass
        if len(ids) != int(vecs.shape[0]):
            print("[WARN] NumpyStore meta rows (%d) != vectors rows (%d)" % (len(ids), int(vecs.shape[0])))
        return ids
    except Exception as e:
        print("[WARN] Failed to read NumpyStore: %s" % e)
        return set()

def sample_ids(s, k):
    return ", ".join(list(sorted(s))[:k])

def main():
    print("=== VALIDATION: RAG_project_v0.5 coverage (PG vs SQLite/Numpy) ===")
    ids_sql_ado, ids_sql_non, attach_map = load_sqlite()
    ids_pg_ado,  ids_pg_non,  ids_pg_emb = load_pg()
    ids_np = load_numpystore()

    print("\n-- COUNTS -------------------------------")
    print("SQLite docs ADO        : %d" % len(ids_sql_ado))
    print("SQLite docs Non-ADO    : %d" % len(ids_sql_non))
    print("PG docs ADO            : %d" % len(ids_pg_ado))
    print("PG docs Non-ADO        : %d" % len(ids_pg_non))
    print("PG embeddings by id    : %d" % len(ids_pg_emb))
    print("NumpyStore vectors     : %d" % len(ids_np))

    miss_pg_ado = ids_sql_ado - ids_pg_ado
    miss_pg_non = ids_sql_non - ids_pg_non

    need_emb_ado = ids_pg_ado - ids_pg_emb if ids_pg_emb else set()
    need_emb_non = ids_pg_non - ids_pg_emb if ids_pg_emb else set()

    numpy_only = ids_np - (ids_pg_ado | ids_pg_non) if ids_np else set()

    print("\n-- MISSING IN PG ------------------------")
    print("SQLite→PG ADO missing     : %d" % len(miss_pg_ado))
    if miss_pg_ado:
        print("  e.g. %s" % sample_ids(miss_pg_ado, int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))))
    print("SQLite→PG NonADO missing  : %d" % len(miss_pg_non))
    if miss_pg_non:
        print("  e.g. %s" % sample_ids(miss_pg_non, int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))))

    print("\n-- PG DOCS WITHOUT EMBEDDINGS ----------")
    if ids_pg_emb:
        print("PG ADO need embeddings   : %d" % len(need_emb_ado))
        if need_emb_ado:
            print("  e.g. %s" % sample_ids(need_emb_ado, int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))))
        print("PG NonADO need embeddings: %d" % len(need_emb_non))
        if need_emb_non:
            print("  e.g. %s" % sample_ids(need_emb_non, int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))))
    else:
        print("Unknown legacy doc_embeddings schema; cannot list docs needing embeddings.")

    print("\n-- NUMPYSTORE-ONLY DOCS ----------------")
    print("in NumpyStore but not in PG: %d" % len(numpy_only))
    if numpy_only:
        print("  e.g. %s" % sample_ids(numpy_only, int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))))

    print("\n-- ATTACHMENT LINKAGE (SQLite only) ----")
    total_att = sum(attach_map.values())
    pg_ids_all = (ids_pg_ado | ids_pg_non)
    att_on_pg_ids = sum(cnt for cid, cnt in attach_map.items() if cid in pg_ids_all)
    att_on_sql_only_ids = total_att - att_on_pg_ids
    print("SQLite attachments total          : %d" % total_att)
    print("Attachments on docs present in PG : %d" % att_on_pg_ids)
    print("Attachments on docs missing in PG : %d" % att_on_sql_only_ids)
    if att_on_sql_only_ids > 0:
        ex_ids = [cid for cid in attach_map if cid not in pg_ids_all]
        print("  example content_ids: %s" % ", ".join(ex_ids[:int(os.getenv("VALIDATE_SHOW_SAMPLES","20"))]))

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
            conditions.append("Some PG docs lack embeddings.")
    if HAVE_NUMPY and ids_np:
        if not numpy_only:
            conditions.append("No docs exist only in NumpyStore.")
        else:
            conditions.append("Some docs exist only in NumpyStore, not in PG.")
    if att_on_sql_only_ids == 0:
        conditions.append("All SQLite attachment linkages map to docs present in PG.")
    else:
        conditions.append("Some attachments link to docs absent from PG.")

    for c in conditions:
        print("- %s" % c)

    if (not miss_pg_ado and not miss_pg_non and
        (not ids_pg_emb or (not need_emb_ado and not need_emb_non)) and
        (not HAVE_NUMPY or not ids_np or not numpy_only) and
        att_on_sql_only_ids == 0):
        print("\nRecommendation: Postgres is authoritative; proceed with new direct-to-PG ingestion.")
    else:
        print("\nRecommendation: Gaps detected. Migrate missing docs/embeddings before deprecating SQLite/Numpy.")
    print("========================================")

if __name__ == "__main__":
    main()
