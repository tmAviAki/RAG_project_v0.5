# Project: confAdogpt  Component: security_api_key  Version: v0.1.0
from __future__ import annotations
import os
from fastapi import Header, HTTPException

API_KEY_ENV = "API_KEY"

def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    expected = os.getenv(API_KEY_ENV)
    if not expected:
        return
    if not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
