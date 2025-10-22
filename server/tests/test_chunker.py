from app.chunker import iter_chunked_items

def test_chunker_basic():
    items = [ {"i": i, "v": "x" * 1000 } for i in range(100) ]
    chunks = list(iter_chunked_items(items, chunk_bytes=10_000, envelope=True))
    assert len(chunks) >= 5  # we expect multiple chunks
    # first chunk should not exceed ~10KB by construction
    payload, n, approx = chunks[0]
    assert approx <= 10_000
