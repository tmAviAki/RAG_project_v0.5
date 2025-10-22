# Project: confAdogpt  Component: embeddings  Version: v0.9.1
from __future__ import annotations

import os
import time
from typing import List, Optional, Tuple

import numpy as np
import httpx

try:
    from .embed_cache import EmbedCache
except Exception:
    EmbedCache = None  # cache optional

# Environment
ALLOW_REMOTE = os.getenv("ALLOW_REMOTE_EMBEDDINGS", "0") == "1"
MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
DIM_DEFAULT = int(os.getenv("EMBED_DIM", "1536"))
OPENAI_TIMEOUT_SECS = float(os.getenv("OPENAI_TIMEOUT_SECS", "30"))
MAX_RETRIES = int(os.getenv("EMBED_MAX_RETRIES", "4"))
BACKOFF_MIN = float(os.getenv("EMBED_BACKOFF_MIN_MS", "500")) / 1000.0
BACKOFF_MAX = float(os.getenv("EMBED_BACKOFF_MAX_MS", "4000")) / 1000.0

_singleton_fake = None
_singleton_remote = None
_cache = None

def _get_cache() -> Optional["EmbedCache"]:
    global _cache
    if _cache is None and EmbedCache is not None:
        try:
            _cache = EmbedCache()
        except Exception:
            _cache = None
    return _cache

def _hash_text(text: str) -> str:
    import hashlib
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

class FakeEmbedder:
    def __init__(self, dim: int):
        self.dim = int(dim)

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        arr = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, t in enumerate(texts):
            h = int(_hash_text(t)[:16], 16)
            rng = np.random.RandomState(h & 0x7FFFFFFF)
            v = rng.standard_normal(self.dim).astype(np.float32)
            n = np.linalg.norm(v)
            arr[i, :] = v / (n if n > 0 else 1.0)
        return [row.tolist() for row in arr]

class OpenAIEmbedder:
    def __init__(self, model: str, dim: int, timeout: float):
        self.model = model
        self.dim = int(dim)
        self._timeout = timeout
        self._client = None

    def _client_open(self):
        if self._client is None:
            try:
                from openai import OpenAI  # lazy import
            except Exception as e:
                raise RuntimeError(
                    "Remote embeddings requested but 'openai' is not installed"
                ) from e
            transport = httpx.HTTPTransport(retries=0)
            session = httpx.Client(timeout=self._timeout, transport=transport)
            self._client = OpenAI(http_client=session)
        return self._client

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        cli = self._client_open()
        cache = _get_cache()
        to_query: List[Tuple[int, str]] = []
        out: List[Optional[List[float]]] = [None] * len(texts)

        if cache is not None:
            keys = [_hash_text(t) for t in texts]
            hits = cache.get_many(keys, self.model, self.dim)
            for i, v in enumerate(hits):
                if v is None:
                    to_query.append((i, texts[i]))
                else:
                    out[i] = v
        else:
            for i, t in enumerate(texts):
                to_query.append((i, t))

        if to_query:
            ordered = [t for _, t in to_query]
            backoff = BACKOFF_MIN
            for attempt in range(MAX_RETRIES):
                try:
                    resp = cli.embeddings.create(model=self.model, input=ordered)
                    vecs = [d.embedding for d in resp.data]
                    if cache is not None:
                        cache.put_many([_hash_text(t) for t in ordered], self.model, self.dim, vecs)
                    for (i, _), v in zip(to_query, vecs):
                        if len(v) != self.dim:
                            raise RuntimeError("Embedding dimension mismatch")
                        out[i] = v
                    break
                except Exception:
                    time.sleep(min(backoff, BACKOFF_MAX))
                    backoff *= 2.0

        return [v if v is not None else [0.0] * self.dim for v in out]

def get_embedder(dim_hint: Optional[int] = None, force_local: bool = False):
    dim = int(dim_hint or DIM_DEFAULT or 1536)
    if not force_local and ALLOW_REMOTE:
        global _singleton_remote
        if _singleton_remote is None or _singleton_remote.dim != dim:
            _singleton_remote = OpenAIEmbedder(MODEL, dim, OPENAI_TIMEOUT_SECS)
        return _singleton_remote
    global _singleton_fake
    if _singleton_fake is None or _singleton_fake.dim != dim:
        _singleton_fake = FakeEmbedder(dim)
    return _singleton_fake

__all__ = ["FakeEmbedder", "OpenAIEmbedder", "get_embedder"]
