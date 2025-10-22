# Project: confAdogpt  Component: rag_store  Version: v1.3.1
from __future__ import annotations
import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional

import numpy as np


@dataclass
class VSConfig:
    root: str = os.getenv("INDEX_ROOT", "/index")
    store_dir: str = "faiss"
    dim: int = int(os.getenv("EMBED_DIM", "1536"))


class NumpyStore:
    """
    Persistent vector store with:
      • Atomic saves for vectors/meta
      • Auto-heal on load
      • Memory-mapped vectors for read paths
      • Unit-length row storage => cosine == dot
    File layout:
      {root}/{store_dir}/vectors.npy
      {root}/{store_dir}/meta.jsonl
    """

    def __init__(self, cfg: VSConfig = VSConfig()):
        self.cfg = cfg
        self.dir = Path(cfg.root) / cfg.store_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.vec_path = self.dir / "vectors.npy"
        self.meta_path = self.dir / "meta.jsonl"
        self.vecs: np.ndarray = np.zeros((0, self.cfg.dim), dtype=np.float32)
        self.meta: List[Dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        if self.vec_path.exists():
            self.vecs = np.load(self.vec_path, mmap_mode="r")
        else:
            self.vecs = np.zeros((0, self.cfg.dim), dtype=np.float32)
        meta: List[Dict[str, Any]] = []
        if self.meta_path.exists():
            with self.meta_path.open("r", encoding="utf-8") as f:
                for ln in f:
                    s = ln.strip()
                    if not s:
                        continue
                    try:
                        meta.append(json.loads(s))
                    except json.JSONDecodeError:
                        continue
        self.meta = meta
        nv = int(self.vecs.shape[0])
        nm = len(self.meta)
        if nv != nm:
            n = min(nv, nm)
            if nv > n:
                self.vecs = self.vecs[:n, :]
            if nm > n:
                self.meta = self.meta[:n]
            self._save()

    def _save(self) -> None:
        tmp_vec = self.vec_path.with_name(self.vec_path.name + ".tmp")
        tmp_met = self.meta_path.with_name(self.meta_path.name + ".tmp")
        if isinstance(self.vecs, np.memmap):
            vec = np.array(self.vecs, dtype=np.float32, copy=False)
        else:
            vec = self.vecs.astype(np.float32, copy=False)
        with open(tmp_vec, "wb") as f:
            np.save(f, vec)
        with tmp_met.open("w", encoding="utf-8") as f:
            for m in self.meta:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")
        os.replace(tmp_vec, self.vec_path)
        os.replace(tmp_met, self.meta_path)
        self.vecs = np.load(self.vec_path, mmap_mode="r")

    @staticmethod
    def _dot_on_unit_rows(a: np.ndarray, B: np.ndarray) -> np.ndarray:
        if B.shape[0] == 0:
            return np.zeros((0,), dtype=np.float32)
        if a.dtype != np.float32:
            a = a.astype(np.float32, copy=False)
        an = np.linalg.norm(a).astype(np.float32)
        if an == 0.0:
            return np.zeros((B.shape[0],), dtype=np.float32)
        a = (a / max(an, 1e-8)).astype(np.float32, copy=False)
        return (B @ a).astype(np.float32, copy=False)

    def upsert(self, chunks: Iterable[Dict[str, Any]]) -> int:
        new_vecs: List[np.ndarray] = []
        new_meta: List[Dict[str, Any]] = []
        for ch in chunks:
            emb = ch.get("embedding")
            if not isinstance(emb, (list, tuple)) or len(emb) != self.cfg.dim:
                continue
            v = np.asarray(emb, dtype=np.float32)
            if v.ndim != 1:
                raise ValueError(f"embedding must be 1-D, got shape {v.shape}")
            if v.shape[0] != self.cfg.dim:
                raise ValueError(f"embedding dim {v.shape[0]} != store dim {self.cfg.dim}")
            n = float(np.linalg.norm(v))
            if n > 0.0:
                v = (v / n).astype(np.float32, copy=False)
            else:
                v = v.astype(np.float32, copy=False)
            m = {k: v2 for k, v2 in ch.items() if k not in ("embedding", "text")}
            txt = ch.get("text")
            if isinstance(txt, str) and txt:
                m["snippet"] = txt[:300]
            new_vecs.append(v)
            new_meta.append(m)
        if not new_vecs:
            return 0
        if self.vecs.shape[0] == 0:
            base = np.empty((0, self.cfg.dim), dtype=np.float32)
        else:
            base = np.array(self.vecs, dtype=np.float32, copy=False)
        self.vecs = np.vstack([base] + new_vecs).astype(np.float32, copy=False)
        self.meta.extend(new_meta)
        self._save()
        return len(new_meta)

    def upsert_batch(self, metas: List[Dict[str, Any]], embs: List[List[float]]) -> int:
        if not metas or not embs:
            return 0
        n = min(len(metas), len(embs))
        chunks = []
        for i in range(n):
            ch = dict(metas[i])
            ch["embedding"] = embs[i]
            chunks.append(ch)
        return self.upsert(chunks)

    def flush(self) -> None:
        return

    def search(self, q_emb: List[float], k: int = 8,
               filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        a = np.asarray(q_emb, dtype=np.float32)
        scores = self._dot_on_unit_rows(a, self.vecs)
        take = k * 5 if k > 0 else 0
        idx = np.argsort(-scores)[:take]
        out: List[Dict[str, Any]] = []
        filt = filters or {}
        for i in idx:
            m = self.meta[int(i)]
            if filt.get("space") and m.get("space") not in filt["space"]:
                continue
            if filt.get("type") and m.get("type") not in filt["type"]:
                continue
            item = dict(m)
            item["score"] = float(scores[int(i)])
            out.append(item)
            if len(out) >= k:
                break
        return out

    def delete_by_ids(self, ids: List[str]) -> int:
        if not ids:
            return 0
        keep = [i for i, m in enumerate(self.meta) if m.get("id") not in ids]
        removed = len(self.meta) - len(keep)
        if keep:
            vec = np.array(self.vecs[keep, :], dtype=np.float32, copy=False)
            self.vecs = vec
            self.meta = [self.meta[i] for i in keep]
        else:
            self.vecs = np.zeros((0, self.cfg.dim), dtype=np.float32)
            self.meta = []
        self._save()
        return removed

    def reindex_since(self, updated_after: Optional[str]) -> int:
        return 0

    def effective_dim(self) -> int:
        try:
            return int(self.vecs.shape[1])
        except Exception:
            return int(self.cfg.dim)

