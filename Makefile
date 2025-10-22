# Makefile
# Project: confAdogpt  Component: Makefile  Version: v0.7.3
.PHONY: build up down code-ingest xref ingest-all test csv-ingest

build:
	docker compose build --no-cache --pull

up:
	docker compose up -d api

down:
	docker compose down

test:
	docker compose run --rm api pytest -q

csv-ingest:
	bash scripts/ingest_all.sh

code-ingest:
	bash scripts/code_ingest.sh

xref:
	bash scripts/xref_rebuild.sh

ingest-all:
	bash scripts/ingest_all_v072.sh
