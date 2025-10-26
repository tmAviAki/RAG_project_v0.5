# File: server/app/embeddings.py
# Project: RAG_project_v0.5  Component: embeddings  Version: v1.3.0
"""
Enhancements:
 - Uniform logging via LOG_LEVEL
 - Top-level embed_texts() convenience used by code_ingest.py
 - Backpressure (RPM/TPM with headroom) + simple EMA timing
 - Optional SQLite/LRU cache adapter (EmbedCache) compatibility
 - Dimension guard: enforces target dim; RP reduction when needed
 - Debug snapshot for /v1/debug/embedding-status
"""
from __future__ import annotations

import hashlib
import inspect
import logging
import math
import os
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
import numpy as np

try:
    from .embed_cache import EmbedCache  # optional
except Exception:  # pragma: no cover
    EmbedCache = None  # type: ignore

# ------------------------ Config / ENV ------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MODEL = os.getenv("MODEL", os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))
DIM_DEFAULT = int(os.getenv("EMBED_DIM", "1536"))
ALLOW_REMOTE = os.getenv("ALLOW_REMOTE_EMBEDDINGS", "1").strip().lower() in ("1", "true", "yes", "on")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_TIMEOUT_SECS = float(os.getenv("OPENAI_TIMEOUT_SECS", "30"))
MAX_RETRIES = int(os.getenv("EMBED_MAX_RETRIES", "4"))
BACKOFF_MIN = float(os.getenv("EMBED_BACKOFF_MIN_MS", "500")) / 1000.0
BACKOFF_MAX = float(os.getenv("EMBED_BACKOFF_MAX_MS", "4000")) / 1000.0

# Backpressure
WINDOW_SEC = float(os.getenv("EMBED_WINDOW_SEC", "60"))
CONCURRENCY = int(os.getenv("EMBED_CONCURRENCY", "4"))  # advertised to debug route
RPM_LIMIT = int(os.getenv("EMBED_RPM_LIMIT", "5000"))
TPM_LIMIT = int(os.getenv("EMBED_TPM_LIMIT", "1000000"))
HEADROOM = float(os.getenv("EMBED_HEADROOM", "0.90"))

log = logging.getLogger("embeddings")
if not log.handlers:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-7s embeddings %(message)s",
    )
log.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ------------------------ Helpers ------------------------
def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()

def _approx_tokens(s: str) -> int:
    return (len(s) + 3) // 4 if s else 0

def _l2_normalize(vec: Sequence[float]) -> List[float]:
    s = math.sqrt(sum((x * x) for x in vec)) or 1.0
    return [float(x / s) for x in vec]

# ------------------------ Cache adapter ------------------------
class _CacheAdapter:
    def __init__(self, cache: EmbedCache):
        self.cache = cache
        self.sig_get = getattr(cache, "get_many", None) and inspect.signature(cache.get_many)  # type: ignore
        self.sig_put = getattr(cache, "put_many", None) and inspect.signature(cache.put_many)  # type: ignore

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

    def put_many(self, tuples: Iterable[Tuple[str, str, int, List[float]]]) -> None:
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

def _get_cache() -> Optional["EmbedCache"]:
    if EmbedCache is None:
        return None
    try:
        return EmbedCache()  # type: ignore[call-arg]
    except Exception:
        return None

# ------------------------ Limiters ------------------------
class _Limiter:
    """Simple windowed limiter with headroom; tracks rpm/tpm and exposes a snapshot."""
    def __init__(self, window_sec: float, rpm: int, tpm: int, headroom: float):
        self.window_sec = max(1.0, float(window_sec))
        self.rpm = max(0, int(rpm))
        self.tpm = max(0, int(tpm))
        self.headroom = max(0.1, min(1.0, float(headroom)))
        self._start = time.monotonic()
        self._req = 0
        self._tok = 0
        self.cooldown_until: float = 0.0
    def _maybe_roll(self) -> None:
        if time.monotonic() - self._start >= self.window_sec:
            self._start = time.monotonic(); self._req = 0; self._tok = 0; self.cooldown_until = 0.0
    def account(self, tokens: int, requests: int) -> None:
        self._maybe_roll()
        self._tok += max(0, int(tokens)); self._req += max(0, int(requests))
    def maybe_sleep(self) -> float:
        self._maybe_roll()
        need_sleep = False
        if self.rpm > 0 and self._req > self.rpm * self.headroom: need_sleep = True
        if self.tpm > 0 and self._tok > self.tpm * self.headroom: need_sleep = True
        if not need_sleep: return 0.0
        end = self._start + self.window_sec
        now = time.monotonic()
        sleep_s = max(0.0, end - now)
        if sleep_s > 0:
            self.cooldown_until = end
            log.info("[EMB][PACE] sleeping %.2fs (rpm=%d/%d, tpm=%d/%d, head=%.2f)",
                     sleep_s, self._req, self.rpm, self._tok, self.tpm, self.headroom)
            time.sleep(sleep_s)
        self._maybe_roll()
        return sleep_s
    def snapshot(self) -> Dict[str, float | int]:
        self._maybe_roll()
        return {
            "rpm_used": self._req, "tpm_used": self._tok,
            "rpm_budget": self.rpm, "tpm_budget": self.tpm,
            "headroom": float(self.headroom), "cooldown_until": self.cooldown_until or 0.0,
        }

# ------------------------ Embedders ------------------------
class FakeEmbedder:
    def __init__(self, dim: int) -> None:
        self.dim = int(dim)
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        out: List[List[float]] = []
        for t in texts:
            h = hashlib.blake2b((t or "").encode("utf-8"), digest_size=32).digest()
            rng = np.random.default_rng(int.from_bytes(h[:8], "big"))
            vec = rng.standard_normal(self.dim).astype(np.float32).tolist()
            out.append(_l2_normalize(vec))
        return out
    def debug_snapshot(self) -> Dict[str, float | int | None]:
        return {"rpm_used":0,"tpm_used":0,"rpm_budget":0,"tpm_budget":0,"headroom":1.0,"cooldown_until":0.0,
                "inflight_without_ttfb":0,"ema_oneway_ms":0.0}

class OpenAIEmbedder:
    def __init__(self, model: str, dim: int, timeout_s: float) -> None:
        self.model = model; self.dim = int(dim); self.timeout_s = float(timeout_s)
        self._limiter = _Limiter(WINDOW_SEC, RPM_LIMIT, TPM_LIMIT, HEADROOM)
        self._ema_ms: float = 0.0
    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=OPENAI_BASE_URL, timeout=self.timeout_s,
                            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"})
    def _enforce_dim(self, v: Sequence[float]) -> List[float]:
        d = len(v)
        if d == self.dim:
            return _l2_normalize(v)
        if d == 3072 and self.dim == 1536:
            try:
                from .reduction import get_reducer
                return _l2_normalize(get_reducer().reduce(v))
            except Exception:  # pragma: no cover
                log.warning("Reducer not available; truncating 3072->1536")
                return _l2_normalize(list(v)[:1536])
        if d > self.dim:
            log.warning("Embedding dim %d > target %d — truncating", d, self.dim)
            return _l2_normalize(list(v)[: self.dim])
        if d < self.dim:
            log.warning("Embedding dim %d < target %d — zero-padding", d, self.dim)
            return _l2_normalize(list(v) + [0.0]*(self.dim - d))
        return _l2_normalize(v)
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
            toks = sum(_approx_tokens(t) for t in ordered)
            self._limiter.account(tokens=toks, requests=1)
            self._limiter.maybe_sleep()
            backoff = BACKOFF_MIN
            for attempt in range(MAX_RETRIES):
                t0 = time.monotonic()
                try:
                    with self._client() as cli:
                        resp = cli.post("/embeddings", json={"model": self.model, "input": ordered})
                    resp.raise_for_status()
                    data = resp.json()
                    vecs_raw = [d["embedding"] for d in data.get("data", [])]
                    vecs = [self._enforce_dim(v) for v in vecs_raw]
                    dt_ms = (time.monotonic() - t0) * 1000.0
                    self._ema_ms = (0.8*self._ema_ms + 0.2*dt_ms) if self._ema_ms>0 else dt_ms
                    if adapter:
                        tuples = [(_hash_text(t), self.model, self.dim, vec) for t, vec in zip(ordered, vecs)]
                        adapter.put_many(tuples)
                    for (i, _), v in zip(to_query, vecs):
                        out[i] = v
                    break
                except Exception as e:
                    if attempt + 1 >= MAX_RETRIES:
                        log.error("embed_texts failed after %d attempts: %s", attempt + 1, e); raise
                    sleep_s = min(backoff, BACKOFF_MAX)
                    log.warning("embed_texts attempt %d failed: %s; retrying in %.2fs", attempt + 1, e, sleep_s)
                    time.sleep(sleep_s); backoff *= 2.0
        return [out[i] for i in range(len(texts))]
    def debug_snapshot(self) -> Dict[str, float | int | None]:
        snap = self._limiter.snapshot()
        return {"rpm_used":snap["rpm_used"], "tpm_used":snap["tpm_used"],
                "rpm_budget":snap["rpm_budget"], "tpm_budget":snap["tpm_budget"],
                "headroom":snap["headroom"], "cooldown_until":snap["cooldown_until"],
                "inflight_without_ttfb":0, "ema_oneway_ms":round(self._ema_ms,2)}

# ------------------------ Public API ------------------------
_singleton_fake: Optional[FakeEmbedder] = None
_singleton_remote: Optional[OpenAIEmbedder] = None

def get_embedder(dim_hint: Optional[int] = None, force_local: bool = False):
    dim = int(dim_hint or DIM_DEFAULT or 1536)
    if not force_local and ALLOW_REMOTE and OPENAI_API_KEY:
        global _singleton_remote
        if _singleton_remote is None or _singleton_remote.dim != dim:
            _singleton_remote = OpenAIEmbedder(MODEL, dim, OPENAI_TIMEOUT_SECS)
        return _singleton_remote
    global _singleton_fake
    if _singleton_fake is None or _singleton_fake.dim != dim:
        _singleton_fake = FakeEmbedder(dim)
    return _singleton_fake

def embed_texts(texts: List[str], dim_hint: Optional[int] = None) -> List[List[float]]:
    """Convenience wrapper used by code_ingest, csv_db_ingest, etc."""
    emb = get_embedder(dim_hint=dim_hint)
    return emb.embed_texts(texts)

__all__ = ["FakeEmbedder", "OpenAIEmbedder", "get_embedder", "embed_texts", "WINDOW_SEC", "CONCURRENCY"]
