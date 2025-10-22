# Project:Confluence Evidence API  Component:test_indexer_ado  Version:v1.0.0
from __future__ import annotations
import os, json, sqlite3, tempfile, shutil
from pathlib import Path
from app.repository import connect, fetch_docs, list_attachments
from app.indexer_ado import index_ado_cache

def mk_ado_fixture(tmp: Path):
    (tmp/"items").mkdir(parents=True, exist_ok=True)
    (tmp/"attachments").mkdir(parents=True, exist_ok=True)
    wi = {"id": 123, "System.Title": "Sample bug", "System.WorkItemType": "Bug", "System.Description": "desc"}
    (tmp/"items"/"123.json").write_text(json.dumps(wi), encoding="utf-8")
    (tmp/"attachments"/"f1.txt").write_text("hello", encoding="utf-8")
    idx = {"123": [{"name": "f1.txt", "path": "attachments/f1.txt", "size": 5}]}
    (tmp/"attachments_index.json").write_text(json.dumps(idx), encoding="utf-8")

def test_index_ado_cache_roundtrip(tmp_path: Path):
    mk_ado_fixture(tmp_path)
    db = tmp_path/"db.sqlite"
    conn = connect(str(db))
    n_docs, n_att = index_ado_cache(str(tmp_path), conn, space_key="ADO")
    assert n_docs == 1
    assert n_att == 1
    rows = fetch_docs(conn, ["ADO:123"])
    assert rows and rows[0]["title"] == "Sample bug"
    atts = list_attachments(conn, "ADO:123")
    assert atts and atts[0]["relpath"].startswith("ADO/")

