# Project:RAG_project_v0.5 Component:Dockerfile Version:v1.4.0
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LOG_LEVEL=DEBUG \
    OPENAI_TIMEOUT_SECS=30 \
    ALLOW_QUERY_FALLBACK=1 \
    UVICORN_WORKERS=1 \
    UVICORN_TIMEOUT_KEEP_ALIVE=10

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      curl ca-certificates \
      libxml2 libxslt1.1 \
      libmagic1 \
      poppler-utils \
      tesseract-ocr tesseract-ocr-eng \
      ocrmypdf qpdf ghostscript unpaper pngquant \
      jq \
      sqlite3 \
      tini \
      xxd \
      sudo \
      bash \
    && ln -sf /bin/bash /bin/sh \
    && rm -rf /var/lib/apt/lists*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
# P2 deps for Postgres + pgvector
RUN pip install --no-cache-dir 'psycopg[binary]>=3.1' 'pgvector>=0.2.4'

RUN pip install --no-cache-dir \
      pillow \
      pytesseract \
      python-docx \
      python-pptx \
      openpyxl \
      python-magic

COPY app ./app

# logging config (unchanged)
RUN cat > /app/logging.ini <<'EOF'
[loggers]
keys=root,uvicorn,uvicorn.error,uvicorn.access
[handlers]
keys=console
[formatters]
keys=standard
[logger_root]
level=DEBUG
handlers=console
[logger_uvicorn]
level=DEBUG
handlers=console
qualname=uvicorn
propagate=0
[logger_uvicorn.error]
level=DEBUG
handlers=console
qualname=uvicorn.error
propagate=0
[logger_uvicorn.access]
level=INFO
handlers=console
qualname=uvicorn.access
propagate=0
[handler_console]
class=StreamHandler
level=DEBUG
formatter=standard
args=(sys.stdout,)
[formatter_standard]
format=%(asctime)s %(levelname)-7s %(name)s %(message)s
datefmt=%Y-%m-%dT%H:%M:%S%z
EOF

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=10 \
  CMD curl -fsS http://localhost:8000/v1/health || exit 1

ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-config", "/app/logging.ini", "--proxy-headers", "--timeout-keep-alive", "10"]

