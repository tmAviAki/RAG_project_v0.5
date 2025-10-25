# Project:RAG_project_v0.5 Component:Makefile Version:v0.8.0
.PHONY: build up down pg-build pg-up pg-down pg-reinit pg-migrate pg-ingest \
        api-up-pg api-up tools-ingest dist release-patch release-minor \
        release-major clean

COMPOSE := docker compose -f docker-compose.yml

build:
	$(COMPOSE) build --no-cache --pull

up:
	$(COMPOSE) up -d api

down:
	$(COMPOSE) down

# ---- Postgres (pgvector) lifecycle ----
pg-build:
	$(COMPOSE) --profile pg build pg

pg-up:
	$(COMPOSE) --profile pg up -d pg

pg-down:
	$(COMPOSE) --profile pg down

# Destructive: removes DB volume and re-initializes
pg-reinit:
	$(COMPOSE) --profile pg down -v
	$(COMPOSE) --profile pg up -d pg

# Run migrations from inside api (psql must be present; migrations copied in image)
pg-migrate:
	$(COMPOSE) --profile pg run --rm api \
	 bash -lc 'psql "host=$$PGHOST port=$$PGPORT dbname=$$PGDATABASE user=$$PGUSER password=$$PGPASSWORD" \
	           -f /app/migrations/001_init_pgvector.sql'

# Populate docs (+embeddings if ALLOW_REMOTE_EMBEDDINGS=1)
pg-ingest:
	$(COMPOSE) --profile pg --profile tools run --rm ingest_pg

# ---- API bring-up options ----
api-up:
	$(COMPOSE) up -d api

api-up-pg:
	$(COMPOSE) --profile pg up -d pg api

tools-ingest:
	$(COMPOSE) --profile tools run --rm ingest

# ---- Packaging ----
dist:
	mkdir -p dist
	tar -czf dist/rag_api_v0.8.0_p2.tar.gz server openapi docker-compose.yml \
	            docker/pg/Dockerfile migrations .env.example
	shasum -a 256 dist/rag_api_v0.8.0_p2.tar.gz > dist/rag_api_v0.8.0_p2.sha256

release-patch:
	git tag -a v0.8.0 -m "P2 hybrid (pgvector, unified compose)"
	git push --tags

release-minor:
	@echo "Bump minor then tag"

release-major:
	@echo "Bump major then tag"

clean:
	rm -rf dist
