# Project: confAdogpt  Component: test_stream_answer_contract  Version: v0.1.0
from __future__ import annotations
from fastapi.testclient import TestClient
from server.app.main import app

def test_stream_answer_contract_smoke():
    client = TestClient(app)
    r = client.post("/v1/stream/answer", json={"question": "smoke", "k": 1})
    assert r.status_code == 200
    lines = [ln for ln in r.text.splitlines() if ln.strip()]
    assert any('"type":"preamble"' in ln or '"type": "preamble"' in ln for ln in lines)
    assert any('"type":"final"' in ln or '"type": "final"' in ln for ln in lines)
