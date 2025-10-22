from __future__ import annotations
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class DocHit(BaseModel):
    id: str
    space: str
    type: str = Field(description="page|blogpost|comment (if present)")
    title: str
    snippet: Optional[str] = None
    attachments_count: int = 0

class DocFull(BaseModel):
    id: str
    space: str
    type: str
    title: str
    text: str
    attachments_count: int = 0

class PagedResponse(BaseModel):
    items: List[Dict[str, Any]]
    next: Optional[int] = None   # aligned with OpenAPI (cursor int)
    approx_bytes: int = 0

