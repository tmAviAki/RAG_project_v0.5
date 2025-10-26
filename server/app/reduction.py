# Project:RAG_project_v0.5 Component:reduction Version:v0.8.0
from __future__ import annotations
import os, math
from pathlib import Path
from typing import Iterable, List

import numpy as np

_DEF_IN = 3072
_DEF_OUT = 1536

_CACHE_DIR = Path(os.getenv("RP_CACHE_DIR", os.getenv("INDEX_ROOT", "/index")))
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _gaussian_matrix(rows: int, cols: int, seed: int) -> np.ndarray:
    key = f"rp_{rows}x{cols}_seed{seed}.npy"
    cache = _CACHE_DIR / key
    if cache.exists():
        try:
            M = np.load(cache)
            if M.shape == (rows, cols):
                return M.astype(np.float32, copy=False)
        except Exception:
            pass
    rng = np.random.default_rng(int(seed) & 0xFFFFFFFF)
    M = rng.standard_normal((rows, cols), dtype=np.float32)
    M *= (1.0 / math.sqrt(rows))
    try:
        np.save(cache, M.astype(np.float32, copy=False))
    except Exception:
        pass
    return M

class Reducer:
    def __init__(self, in_dim: int = _DEF_IN, out_dim: int = _DEF_OUT, seed: int | None = None):
        self.in_dim = int(in_dim)
        self.out_dim = int(out_dim)
        s = int(os.getenv("RP_SEED", "0")) if seed is None else int(seed)
        self.R = _gaussian_matrix(self.out_dim, self.in_dim, s)

    def reduce(self, vec: Iterable[float]) -> List[float]:
        x = np.asarray(list(vec), dtype=np.float32).ravel()
        if x.size == self.out_dim:  # already target dim: no-op
            return x.astype(np.float32, copy=False).tolist()
        if x.size != self.in_dim:
            raise ValueError(f"expected dim={self.in_dim} or {self.out_dim}, got {x.size}")
        y = self.R @ x
        return y.astype(np.float32, copy=False).tolist()

_singleton: Reducer | None = None

def get_reducer() -> Reducer:
    global _singleton
    if _singleton is None:
        out_dim = int(os.getenv("REDUCE_TO_DIM", str(_DEF_OUT)))
        _singleton = Reducer(_DEF_IN, out_dim)
    return _singleton
