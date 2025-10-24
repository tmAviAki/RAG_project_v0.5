# Project:RAG_project_v0.5 Component:Makefile Version:v0.6.1
.PHONY: release-patch release-minor release-major dist clean
VERSION?=$(shell git describe --tags --abbrev=0 2>/dev/null || echo v0.6.1)

dist:
	mkdir -p dist
	tar -czf dist/rag_api_$(VERSION).tar.gz server openapi docker-compose.yml
	shasum -a 256 dist/rag_api_$(VERSION).tar.gz > dist/rag_api_$(VERSION).sha256

release-patch:
	git tag -a $(VERSION) -m "patch"
	git push --tags

release-minor:
	@echo "Bump minor manually then tag"

release-major:
	@echo "Bump major manually then tag"

clean:
	rm -rf dist

