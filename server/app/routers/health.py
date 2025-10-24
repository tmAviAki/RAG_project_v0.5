# Project:RAG_project_v0.5 Component:routers.health Version:v0.6.1
from __future__ import annotations
from fastapi import APIRouter

router = APIRouter(prefix="/v1", tags=["system"])

@router.get("/health")
def health() -> dict:
    return {"ok": True}

