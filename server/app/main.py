# Project:Confluence Evidence API  Component:main  Version:v1.3.0
from __future__ import annotations
import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .config import settings
from . import indexer

# Routers
from .routers import health, stats, search, fetch, attachments, semantic, code
# New debug router (embedding/status introspection)
from .routers import debug as debug_router
# New xref router (prebuilt cross-reference neighborhood)
from .routes_xref import router as xref_router  # <-- added

app = FastAPI(title="Confluence Local Action API", version="1.3.0")

# Include routers
app.include_router(health.router)
app.include_router(stats.router)
app.include_router(search.router)
app.include_router(fetch.router)
app.include_router(attachments.router)
app.include_router(semantic.router)
app.include_router(code.router)
app.include_router(debug_router.router)
app.include_router(xref_router)  # <-- added

# Static attachments: map /attachments -> DATA_ROOT/attachments
attachments_dir = os.path.join(settings.data_root, "attachments")
if os.path.isdir(attachments_dir):
    app.mount("/attachments", StaticFiles(directory=attachments_dir), name="attachments")

# Optional ADO attachments mount at /attachments/ADO
ado_att_dir = os.path.join(settings.ado_root, "attachments") if settings.ado_root else None
if ado_att_dir and os.path.isdir(ado_att_dir):
    app.mount("/attachments/ADO", StaticFiles(directory=ado_att_dir), name="attachments-ado")

# CORS
origins = [o.strip() for o in settings.allow_origins.split(",")] if settings.allow_origins else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _maybe_init_otel() -> None:
    """
    Initialize OpenTelemetry only if OTEL_EXPORTER_OTLP_ENDPOINT is set and
    dependencies are present. Otherwise no-op.
    """
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({"service.name": "confluence-evidence-api"})
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
    except Exception as e:
        logging.getLogger(__name__).warning("OTel init skipped: %s", e)

@app.on_event("startup")
def startup_tasks():
    _maybe_init_otel()
    # If no index and AUTO_INGEST=1, build it once
    if settings.auto_ingest and not os.path.exists(settings.index_path):
        indexer.build_index(settings.data_root, settings.index_path)
