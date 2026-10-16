# Meridian developer tasks. Run `make help` for a list.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Development tools are pinned in tools/go.mod and run with `go tool`.
GOTOOL := go tool -modfile=tools/go.mod
# Keep local runs in the same FIPS 140-3 mode as CI, even if go.mod's default
# changes.
export GODEBUG := fips140=only
# Releases are built against the certified FIPS 140-3 module, so tests and
# builds use it too.
FIPS := GOFIPS140=certified

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint vulncheck test build ## Run all linters, tests and builds

.PHONY: tidy-check
tidy-check: ## Verify both Go modules are tidy
	go mod tidy -diff
	cd tools && go mod tidy -diff

.PHONY: lint
lint: ## Lint Go code
	$(GOTOOL) golangci-lint run ./...

.PHONY: fmt
fmt: ## Format Go code
	$(GOTOOL) golangci-lint fmt

.PHONY: vulncheck
vulncheck: ## Check dependencies and the standard library for known vulnerabilities
	$(GOTOOL) govulncheck ./...

.PHONY: test
test: ## Run tests with the race detector
	$(FIPS) go test -race -shuffle=on -cover ./...

.PHONY: build
build: ## Build static binaries
	$(FIPS) CGO_ENABLED=0 go build ./...
