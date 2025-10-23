# File: server/app/embeddings.py
# Project: RAG_project_v0.5  Component: embeddings  Version: v0.9.2
from __future__ import annotations

import hashlib
import inspect
import math
import os
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
import numpy as np

try:
    from .embed_cache import EmbedCache  # optional, adapter handles variants
except Exception:
    EmbedCache = None  # type: ignore


MODEL = os.getenv("MODEL", "text-embedding-3-small")
DIM_DEFAULT = int(os.getenv("EMBED_DIM", "1536"))
ALLOW_REMOTE = os.getenv("ALLOW_REMOTE_EMBEDDINGS", "1") == "1"
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_TIMEOUT_SECS = float(os.getenv("OPENAI_TIMEOUT_SECS", "30"))
MAX_RETRIES = int(os.getenv("EMBED_MAX_RETRIES", "4"))
BACKOFF_MIN = float(os.getenv("EMBED_BACKOFF_MIN_MS", "500")) / 1000.0
BACKOFF_MAX = float(os.getenv("EMBED_BACKOFF_MAX_MS", "4000")) / 1000.0

_singleton_fake: Optional["FakeEmbedder"] = None
_singleton_remote: Optional["OpenAIEmbedder"] = None
_cache_singleton: Optional["EmbedCache"] = None


def _get_cache() -> Optional["EmbedCache"]:
    global _cache_singleton
    if _cache_singleton is not None:
        return _cache_singleton
    if EmbedCache is None:
        return None
    try:
        _cache_singleton = EmbedCache()  # type: ignore[call-arg]
        return _cache_singleton
    except Exception:
        return None


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _l2_normalize(vec: Sequence[float]) -> List[float]:
    s = math.sqrt(sum((x * x) for x in vec)) or 1.0
    return [float(x / s) for x in vec]


class _CacheAdapter:
    def __init__(self, cache: EmbedCache):
        self.cache = cache
        self.sig_get = None
        self.sig_put = None
        if hasattr(cache, "get_many"):
            self.sig_get = inspect.signature(cache.get_many)  # type: ignore
        if hasattr(cache, "put_many"):
            self.sig_put = inspect.signature(cache.put_many)  # type: ignore

    def get_many(self, pairs: Iterable[Tuple[str, str]]) -> Dict[str, List[float]]:
        if not self.sig_get:
            return {}
        params = list(self.sig_get.parameters.values())
        if len(params) == 2:
            return self.cache.get_many(list(pairs))  # type: ignore
        if len(params) == 4:
            hashes = [h for (h, _m) in pairs]
            model = next(iter(pairs))[1] if pairs else MODEL
            dim = DIM_DEFAULT
            return self.cache.get_many(hashes, model, dim)  # type: ignore
        return {}

    def put_many(
        self, tuples: Iterable[Tuple[str, str, int, List[float]]]
    ) -> None:
        if not self.sig_put:
            return
        params = list(self.sig_put.parameters.values())
        if len(params) == 2:
            self.cache.put_many(list(tuples))  # type: ignore
            return
        if len(params) == 5:
            hashes = [h for (h, _m, _d, _v) in tuples]
            model = next(iter(tuples))[1] if tuples else MODEL
            dim = next(iter(tuples))[2] if tuples else DIM_DEFAULT
            vecs = [v for (_h, _m, _d, v) in tuples]
            self.cache.put_many(hashes, model, dim, vecs)  # type: ignore


class FakeEmbedder:
    def __init__(self, dim: int) -> None:
        self.dim = int(dim)

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        out: List[List[float]] = []
        for t in texts:
            h = hashlib.blake2b(t.encode("utf-8"), digest_size=32).digest()
            rng = np.random.default_rng(int.from_bytes(h[:8], "big"))
            vec = rng.standard_normal(self.dim).astype(np.float32)
            out.append(_l2_normalize(vec.tolist()))
        return out


class OpenAIEmbedder:
    def __init__(self, model: str, dim: int, timeout_s: float) -> None:
        self.model = model
        self.dim = int(dim)
        self.timeout_s = float(timeout_s)

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=OPENAI_BASE_URL,
            timeout=self.timeout_s,
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        )

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        cache = _get_cache()
        adapter = _CacheAdapter(cache) if cache else None

        out: Dict[int, List[float]] = {}
        to_query: List[Tuple[int, str]] = []

        hashes = [_hash_text(t) for t in texts]
        if adapter:
            got = adapter.get_many([(h, self.model) for h in hashes])
            for i, h in enumerate(hashes):
                v = got.get(h)
                if v and len(v) == self.dim:
                    out[i] = v
                else:
                    to_query.append((i, texts[i]))
        else:
            to_query = list(enumerate(texts))

        if to_query:
            ordered = [t for (_i, t) in to_query]
            backoff = BACKOFF_MIN
            for attempt in range(MAX_RETRIES):
                try:
                    with self._client() as cli:
                        resp = cli.post(
                            "/embeddings",
                            json={"model": self.model, "input": ordered},
                        )
                    resp.raise_for_status()
                    data = resp.json()
                    vecs = [d["embedding"] for d in data.get("data", [])]
                    if adapter:
                        tuples = [
                            (_hash_text(t), self.model, self.dim, _l2_normalize(v))
                            for t, v in zip(ordered, vecs)
                        ]
                        adapter.put_many(tuples)
                    for (i, _), v in zip(to_query, vecs):
                        out[i] = _l2_normalize(v)
                    break
                except Exception:
                    if attempt + 1 >= MAX_RETRIES:
                        raise
                    time.sleep(min(backoff, BACKOFF_MAX))
                    backoff *= 2.0

        return [out[i] for i in range(len(texts))]


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

