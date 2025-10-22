from __future__ import annotations
from app.rag_store import NumpyStore, VSConfig
from app.embeddings import FakeEmbedder
import shutil, os

def test_store_roundtrip(tmp_path):
    os.environ["INDEX_ROOT"] = str(tmp_path)
    store = NumpyStore(VSConfig(root=str(tmp_path), dim=64))
    emb = FakeEmbedder(64).embed_texts(["hello world","foo bar","hello foo"])
    chunks = []
    for i, (t,e) in enumerate(zip(["hello world","foo bar","hello foo"], emb)):
        chunks.append({"id": f"id{i}", "embedding": e, "space":"T", "type":"page", "title": f"t{i}", "url": "/x", "chunk_ix": 0, "updated_at": "", "text": t})
    n = store.upsert(chunks)
    assert n == 3
    q = FakeEmbedder(64).embed_texts(["hello"])[0]
    hits = store.search(q, k=2, filters={})
    assert len(hits) == 2
    assert all("score" in h for h in hits)

