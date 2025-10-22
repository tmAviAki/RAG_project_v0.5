# Project:Confluence Evidence API  Component:routers.health  Version:v1.0.1  Date:2025-09-09
from __future__ import annotations
from fastapi import APIRouter, Response

router = APIRouter(prefix="/v1", tags=["system"])

@router.get("/health")
def health() -> dict:
    return {"ok": True}

@router.head("/health")
def health_head() -> Response:
    return Response(status_code=200)

