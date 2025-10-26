# Project:RAG_project_v0.5 Component:otel Version:v0.7.0
from __future__ import annotations
from contextlib import contextmanager
try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer(__name__)
except Exception:
    _TRACER = None

@contextmanager
def maybe_span(name: str):
    if _TRACER is None:
        yield
        return
    with _TRACER.start_as_current_span(name):
        yield

