# Project:RAG_project_v0.5 Component:pg.Dockerfile Version:v0.1.0
FROM postgres:16.10

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      postgresql-16-pgvector \
 && rm -rf /var/lib/apt/lists/*
