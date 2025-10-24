# Project:RAG_project_v0.5 Component:utils.query_analyzer Version:v0.6.1
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import List

IDENT_RE = re.compile(r'\b([A-Z0-9_]{3,})\b')
PATH_RE = re.compile(r'([A-Za-z0-9_\-\.]+(?:/[A-Za-z0-9_\-\.]+)+)')
CAMEL_RE = re.compile(r'\b([a-z]+[A-Za-z0-9]*[A-Z][A-Za-z0-9]*)\b')

@dataclass
class Analysis:
    tokens: List[str]
    has_identifier: bool
    has_path: bool
    has_camel: bool

def analyze(q: str) -> Analysis:
    ids = set(m.group(1) for m in IDENT_RE.finditer(q))
    paths = set(m.group(1) for m in PATH_RE.finditer(q))
    camels = set(m.group(1) for m in CAMEL_RE.finditer(q))
    toks = [*ids, *paths, *camels]
    return Analysis(tokens=toks, has_identifier=bool(ids), has_path=bool(paths), has_camel=bool(camels))

