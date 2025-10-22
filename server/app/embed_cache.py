# Project:Confluence Evidence API  Component:embed_cache  Version:v1.0.0
from __future__ import annotations
import os, sqlite3, json, time, threading
from typing import Dict, Iterable, Tuple, List
from collections import OrderedDict

_DEFAULT_PATH = os.getenv("EMBED_CACHE_PATH", "/index/embed_cache.sqlite")
_MAX_ROWS = int(os.getenv("EMBED_CACHE_MAX_ROWS", "5000000"))  # hard cap
_HOT_LRU_SIZE = int(os.getenv("EMBED_CACHE_HOT_LRU_SIZE", "100000"))  # in-memory entries

_SCHEMA = """
CREATE TABLE IF NOT EXISTS embeds (
  hash TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  dim INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  vec TEXT NOT NULL   -- JSON array of floats
);
CREATE INDEX IF NOT EXISTS idx_embeds_model ON embeds(model);
"""

class _LRU:
    def __init__(self, cap: int):
        self.cap = max(0, cap)
        self._m = OrderedDict()
        self._lock = threading.Lock()
    def get(self, k: str):
        if self.cap == 0: return None
        with self._lock:
            v = self._m.get(k)
            if v is None: return None
            self._m.move_to_end(k)
            return v
    def put(self, k: str, v):
        if self.cap == 0: return
        with self._lock:
            if k in self._m:
                self._m.move_to_end(k)
                self._m[k] = v
            else:
                self._m[k] = v
                if len(self._m) > self.cap:
                    self._m.popitem(last=False)

class EmbedCache:
    """SQLite-backed embedding cache with a large hot LRU layer."""
    def __init__(self, path: str = _DEFAULT_PATH):
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.Lock()
        self._lru = _LRU(_HOT_LRU_SIZE)
        with self._conn() as c:
            for stmt in filter(None, _SCHEMA.split(";")):
                c.execute(stmt)

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA temp_store=MEMORY")
        return c

    def get_many(self, keys: Iterable[Tuple[str, str]]) -> Dict[str, List[float]]:
        """keys: iterable of (hash, model). returns {hash: vec} for hits."""
        now = int(time.time())
        hits: Dict[str, List[float]] = {}

        # LRU first
        to_fetch: List[Tuple[str, str]] = []
        for h, m in keys:
            v = self._lru.get(f"{m}:{h}")
            if v is not None:
                hits[h] = v
            else:
                to_fetch.append((h, m))
        if not to_fetch:
            return hits

        qmarks = ",".join(["?"] * len(to_fetch))
        params = [h for h, _ in to_fetch]
        models = {m for _, m in to_fetch}
        # Single model is common; if multiple, fetch all hashes and filter by model in Python.
        with self._lock, self._conn() as c:
            rows = c.execute(f"SELECT hash, model, vec FROM embeds WHERE hash IN ({qmarks})", params).fetchall()
        for h, m, vec_json in rows:
            if (h, m) in to_fetch or any(m == mm for _, mm in to_fetch):
                vec = json.loads(vec_json)
                hits[h] = vec
                self._lru.put(f"{m}:{h}", vec)

        return hits

    def put_many(self, tuples: Iterable[Tuple[str, str, int, List[float]]]) -> None:
        """tuples: (hash, model, dim, vec)"""
        now = int(time.time())
        rows = [(h, m, dim, now, json.dumps(vec)) for (h, m, dim, vec) in tuples]
        if not rows: return
        with self._lock, self._conn() as c:
            c.executemany(
                "INSERT OR REPLACE INTO embeds(hash, model, dim, ts, vec) VALUES(?,?,?,?,?)",
                rows,
            )
            # Optional: apply a very light cap if DB grows beyond MAX_ROWS
            n = c.execute("SELECT COUNT(*) FROM embeds").fetchone()[0]
            if n > _MAX_ROWS:
                # delete oldest ~5% rows
                to_delete = max(1, int(0.05 * n))
                c.execute(f"DELETE FROM embeds WHERE hash IN (SELECT hash FROM embeds ORDER BY ts ASC LIMIT {to_delete})")
        for h, m, dim, vec in tuples:
            self._lru.put(f"{m}:{h}", vec)

