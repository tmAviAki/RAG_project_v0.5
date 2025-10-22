# Project:Confluence Evidence API  Component:repository  Version:v1.0.3
from __future__ import annotations
import os
import re
import sqlite3
from typing import List, Dict, Any, Optional

PRAGMAS = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("temp_store", "MEMORY"),
    ("cache_size", -200000),
    ("mmap_size", 268435456),
    ("locking_mode", "EXCLUSIVE"),
]

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS docs (
        id TEXT PRIMARY KEY,
        space TEXT NOT NULL,
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        storage_relpath TEXT,
        created TEXT,
        updated TEXT,
        version INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS doc_texts (
        id TEXT PRIMARY KEY,
        text TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS attachments (
        content_id TEXT NOT NULL,
        name TEXT NOT NULL,
        relpath TEXT NOT NULL,
        size INTEGER,
        sha256 TEXT,
        PRIMARY KEY (content_id, relpath)
    )""",
    """CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
        id, title, text, tokenize = 'unicode61'
    )"""
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_docs_space ON docs(space)",
    "CREATE INDEX IF NOT EXISTS idx_docs_type ON docs(type)",
    "CREATE INDEX IF NOT EXISTS idx_attach_cid ON attachments(content_id)",
]

def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    for k, v in PRAGMAS:
        conn.execute(f"PRAGMA {k}={v}")
    for stmt in SCHEMA:
        conn.execute(stmt)
    for stmt in INDEXES:
        conn.execute(stmt)
    return conn

def _normalize_fts_query(q: str) -> str:
    """
    Normalize a user string for FTS5 MATCH:
      - Map &/&& -> AND , |/|| -> OR
      - Preserve ( ) and AND/OR/NOT
      - Quote non-operator tokens so punctuation does not break parsing
    """
    if not q:
        return q
    s = q.replace("\u2013", "-").replace("\u2014", "-")
    s = f" {s} "
    s = s.replace("&&", " AND ").replace("&", " AND ")
    s = s.replace("||", " OR ").replace("|", " OR ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return s
    token_re = re.compile(r'"[^"]*"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^()\s]+', re.IGNORECASE)
    out: List[str] = []
    for t in token_re.findall(s):
        if t in ("(", ")"):
            out.append(t)
        elif re.fullmatch(r"(?i)AND|OR|NOT", t):
            out.append(t.upper())
        elif t.startswith('"') and t.endswith('"'):
            out.append(t)
        else:
            t_esc = t.replace('"', '""')
            out.append(f'"{t_esc}"')
    return " ".join(out)

def upsert_doc(conn: sqlite3.Connection, doc: Dict[str, Any], text: Optional[str]) -> None:
    conn.execute(
        """INSERT INTO docs (id, space, type, title, storage_relpath, created, updated, version)
           VALUES (:id, :space, :type, :title, :storage_relpath, :created, :updated, :version)
           ON CONFLICT(id) DO UPDATE SET
             space=excluded.space,
             type=excluded.type,
             title=excluded.title,
             storage_relpath=COALESCE(excluded.storage_relpath, docs.storage_relpath),
             created=COALESCE(excluded.created, docs.created),
             updated=COALESCE(excluded.updated, docs.updated),
             version=COALESCE(excluded.version, docs.version)""",
        doc,
    )
    if text is not None:
        conn.execute(
            "INSERT INTO doc_texts (id, text) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET text=excluded.text",
            (doc["id"], text),
        )
        # Keep only one FTS row per id to avoid skewing bm25 rankings
        conn.execute("DELETE FROM docs_fts WHERE id = ?", (doc["id"],))
        conn.execute(
            "INSERT INTO docs_fts (id, title, text) VALUES (?, ?, ?)",
            (doc["id"], doc["title"], text),
        )

def add_attachment(conn: sqlite3.Connection, content_id: str, name: str, relpath: str, size: int, sha256: str | None) -> None:
    conn.execute(
        """INSERT INTO attachments (content_id, name, relpath, size, sha256)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(content_id, relpath) DO UPDATE SET
             size=excluded.size,
             sha256=COALESCE(excluded.sha256, attachments.sha256)""",
        (content_id, name, relpath, size, sha256),
    )

def count_stats(conn: sqlite3.Connection) -> dict:
    c1 = conn.execute("SELECT COUNT(*) AS n FROM docs").fetchone()[0]
    c2 = conn.execute("SELECT COUNT(*) AS n FROM attachments").fetchone()[0]
    return {"docs": c1, "attachments": c2}

def list_spaces(conn: sqlite3.Connection) -> List[dict]:
    rows = conn.execute("SELECT space, COUNT(*) AS count FROM docs GROUP BY space ORDER BY space").fetchall()
    return [{"space": r["space"], "count": r["count"]} for r in rows]

def search_docs(conn: sqlite3.Connection, q: str, space: Optional[str], doctype: Optional[str], limit: int, offset: int) -> List[dict]:
    where: List[str] = []
    params: List[Any] = []

    if space:
        where.append("d.space = ?"); params.append(space)
    if doctype:
        where.append("d.type = ?"); params.append(doctype)

    if q.strip():
        safe_q = _normalize_fts_query(q.strip())
        where_sql = "WHERE " + " AND ".join(where + ["docs_fts MATCH ?"]) if where or safe_q else ""
        params2 = params + [safe_q, limit, offset]
        sql = f"""
            SELECT d.id, d.space, d.type, d.title,
                   (SELECT COUNT(*) FROM attachments a WHERE a.content_id = d.id) AS attachments_count,
                   snippet(docs_fts, 2, '<b>', '</b>', ' … ', 12) AS snippet
            FROM docs_fts
            JOIN docs d ON d.id = docs_fts.id
            {where_sql}
            ORDER BY bm25(docs_fts)
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(sql, params2).fetchall()
    else:
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        params2 = params + [limit, offset]
        sql = f"""
            SELECT d.id, d.space, d.type, d.title,
                   (SELECT COUNT(*) FROM attachments a WHERE a.content_id = d.id) AS attachments_count,
                   NULL AS snippet
            FROM docs d
            {where_sql}
            ORDER BY d.title COLLATE NOCASE
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(sql, params2).fetchall()

    return [dict(r) for r in rows]

def fetch_docs(conn: sqlite3.Connection, ids: List[str]) -> List[dict]:
    if not ids:
        return []
    marks = ",".join(["?"] * len(ids))
    sql = f"""
      SELECT d.id, d.space, d.type, d.title,
             dt.text AS text,
             (SELECT COUNT(*) FROM attachments a WHERE a.content_id = d.id) AS attachments_count
      FROM docs d LEFT JOIN doc_texts dt ON dt.id = d.id
      WHERE d.id IN ({marks})
      ORDER BY d.title COLLATE NOCASE
    """
    rows = [dict(r) for r in conn.execute(sql, ids).fetchall()]
    return rows

def list_attachments(conn: sqlite3.Connection, content_id: str) -> List[dict]:
    rows = conn.execute(
        "SELECT name, relpath, size, sha256 FROM attachments WHERE content_id=? ORDER BY name",
        (content_id,),
    ).fetchall()
    return [dict(r) for r in rows]

