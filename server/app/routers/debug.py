# Project:Confluence Evidence API  Component:routers.debug  Version:v1.2.0
from __future__ import annotations
import os
import time
from fastapi import APIRouter

router = APIRouter(prefix="/v1", tags=["debug"])

@router.get("/debug/embedding-status")
def embedding_status() -> dict:
    allow_remote = os.getenv("ALLOW_REMOTE_EMBEDDINGS", "0") == "1"
    if not allow_remote:
        return {
            "mode": "disabled",
            "model": None,
            "dim": None,
            "headroom": None,
            "rpm_used": None,
            "tpm_used": None,
            "rpm_budget": None,
            "tpm_budget": None,
            "cooldown_until": None,
            "cooldown_seconds_remaining": None,
            "concurrency": None,
            "inflight_without_ttfb": None,
            "ema_oneway_ms": None,
            "window_size_sec": 60,
        }

    try:
        from ..embeddings import get_embedder, WINDOW_SEC, CONCURRENCY
        model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        dim = int(os.getenv("EMBED_DIM", "1536"))

        snap = get_embedder().debug_snapshot()
        cooldown_until = snap.get("cooldown_until") or 0.0
        remaining = round(max(0.0, cooldown_until - time.monotonic()), 3) if cooldown_until else 0.0

        return {
            "mode": "remote",
            "model": model,
            "dim": dim,
            "headroom": round(snap["headroom"], 3),
            "rpm_used": snap["rpm_used"],
            "tpm_used": snap["tpm_used"],
            "rpm_budget": snap["rpm_budget"],
            "tpm_budget": snap["tpm_budget"],
            "cooldown_until": cooldown_until or None,
            "cooldown_seconds_remaining": remaining,
            "concurrency": CONCURRENCY,
            "inflight_without_ttfb": snap["inflight_without_ttfb"],
            "ema_oneway_ms": snap["ema_oneway_ms"],
            "window_size_sec": int(WINDOW_SEC),
        }
    except Exception as e:
        return {
            "mode": "remote",
            "error": str(e),
            "model": os.getenv("EMBEDDING_MODEL"),
            "dim": int(os.getenv("EMBED_DIM", "1536")),
            "headroom": None,
            "rpm_used": None,
            "tpm_used": None,
            "rpm_budget": None,
            "tpm_budget": None,
            "cooldown_until": None,
            "cooldown_seconds_remaining": None,
            "concurrency": None,
            "inflight_without_ttfb": None,
            "ema_oneway_ms": None,
            "window_size_sec": 60,
        }

