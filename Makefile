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
# Every binary is a pure-Go static build, and cgo is not allowed. Only
# test-race turns it on, because the race detector needs it on Linux.
export CGO_ENABLED := 0

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint vulncheck cgo-check test test-race build ## Run all checks, tests and builds

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

.PHONY: cgo-check
cgo-check: ## Verify that no package or dependency uses cgo
	@for os in linux darwin; do \
		pkgs=$$(GOOS=$$os CGO_ENABLED=1 go list -deps -test -f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./...) || exit 1; \
		if [ -n "$$pkgs" ]; then echo "cgo is not allowed, but these packages use it on $$os:"; echo "$$pkgs"; exit 1; fi; \
	done

.PHONY: test
test: ## Run tests
	$(FIPS) go test -shuffle=on -cover ./...

.PHONY: test-race
test-race: ## Run tests with the race detector (needs cgo and a C compiler on Linux)
	$(FIPS) CGO_ENABLED=1 go test -race -shuffle=on ./...

.PHONY: build
build: ## Build static binaries
	$(FIPS) go build -trimpath ./...
