from __future__ import annotations
import os
from fastapi.testclient import TestClient
from app.main import app
from app.rag_store import NumpyStore, VSConfig
from app.embeddings import FakeEmbedder

def setup_module(_m):
    # populate a tiny vector store
    root = "/tmp/index-tests"
    os.environ["INDEX_ROOT"] = root
    store = NumpyStore(VSConfig(root=root, dim=64))
    emb = FakeEmbedder(64).embed_texts(["alpha beta", "gamma delta"])
    items = [
        {"id":"X1","embedding":emb[0],"space":"TEST","type":"page","title":"A","url":"/v1/fetch?ids=X1","chunk_ix":0,"updated_at":"","text":"alpha beta"},
        {"id":"X2","embedding":emb[1],"space":"TEST","type":"page","title":"B","url":"/v1/fetch?ids=X2","chunk_ix":0,"updated_at":"","text":"gamma delta"},
    ]
    store.upsert(items)

def test_semantic_search():
    client = TestClient(app)
    r = client.post("/v1/semantic/search", json={"q":"alpha", "k":1})
    assert r.status_code == 200
    arr = r.json()
    assert isinstance(arr, list) and len(arr)==1

def test_answer():
    client = TestClient(app)
    r = client.post("/v1/answer", json={"q":"alpha", "top_k":1})
    assert r.status_code == 200
    body = r.json()
    assert "answer" in body and "citations" in body

