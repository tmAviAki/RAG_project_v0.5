from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass
class Settings:
    data_root: str = os.getenv("DATA_ROOT", "/data")
    index_path: str = os.getenv("INDEX_PATH", "/index/docs.db")
    chunk_size_bytes: int = int(os.getenv("CHUNK_SIZE_BYTES", "90000"))  # stay under ~100KB limit
    auto_ingest: bool = os.getenv("AUTO_INGEST", "0") == "1"
    allow_origins: str = os.getenv("ALLOW_ORIGINS", "*")
    ado_root: str | None = os.getenv("ADO_ROOT", None)  # optional ADO cache root

settings = Settings()

