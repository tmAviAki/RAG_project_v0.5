# Project:RAG_project_v0.5 Component:db.pg Version:v0.7.10
from __future__ import annotations
import os
from contextlib import contextmanager
import psycopg

def _build_dsn() -> str:
    if os.getenv("FORCE_PG_DSN", "0") == "1":
        dsn = os.getenv("PG_DSN")
        if dsn:
            return dsn
    host = os.getenv("PGHOST", os.getenv("POSTGRES_HOST", "pg"))
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "rag"))
    user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "rag"))
    pwd  = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "fabrix"))
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

@contextmanager
def get_conn():
    conn = psycopg.connect(_build_dsn(), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()

def ensure_extensions():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
