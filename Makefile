# Project:RAG_project_v0.5 Component:Makefile Version:v0.7.0
.PHONY: build up down pg-up pg-migrate pg-ingest release-patch release-minor release-major dist clean

build:
	docker compose build --no-cache --pull

up:
	docker compose up -d api

down:
	docker compose down

pg-up:
	docker compose -f docker-compose.pgvector.yml up -d pg

pg-migrate:
	docker compose -f docker-compose.pgvector.yml run --rm api bash -lc 'psql $$PG_DSN -f migrations/001_init_pgvector.sql'

pg-ingest:
	docker compose -f docker-compose.pgvector.yml run --rm ingest_pg

dist:
	mkdir -p dist
	tar -czf dist/rag_api_v0.7.0_p2.tar.gz server openapi docker-compose.pgvector.yml migrations .env.example
	shasum -a 256 dist/rag_api_v0.7.0_p2.tar.gz > dist/rag_api_v0.7.0_p2.sha256

release-patch:
	git tag -a v0.7.0 -m "P2 hybrid"
	git push --tags

release-minor:
	@echo "Bump minor then tag"

release-major:
	@echo "Bump major then tag"

clean:
	rm -rf dist

