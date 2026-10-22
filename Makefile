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
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: check
check: tidy-check lint vulncheck cgo-check test test-race test-386 bench build ## Run all checks, tests and builds

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

# test-386 first builds and runs a small test binary for 32-bit x86, which
# x86-64 Linux runs natively unless its kernel was built or booted without
# 32-bit support, and macOS doesn't. Where that fails, it says why and skips
# the tests, except in CI, which must run them.
.PHONY: test-386
test-386: ## Run tests on 32-bit x86, where an int is 32 bits, if this machine runs its programs
	@probe=$$(mktemp -d) && trap 'rm -rf "$$probe"' EXIT && \
	if ! out=$$($(FIPS) GOARCH=386 go test -c -o "$$probe/probe.test" ./internal/jsonptr 2>&1 && \
		"$$probe/probe.test" -test.run='^$$' 2>&1); then \
		echo "test-386: this machine can't build or run 32-bit x86 programs:"; echo "$$out"; \
		if [ -n "$$CI" ]; then exit 1; fi; \
		echo "test-386: skipped"; exit 0; \
	fi && \
	echo '$(FIPS) GOARCH=386 go test -shuffle=on ./...' && \
	$(FIPS) GOARCH=386 go test -shuffle=on ./...

.PHONY: bench
bench: ## Run each benchmark once, to check that it still runs
	$(FIPS) go test -run='^$$' -bench=. -benchtime=1x ./...

.PHONY: test-race
test-race: ## Run tests with the race detector (needs cgo and a C compiler on Linux)
	$(FIPS) CGO_ENABLED=1 go test -race -shuffle=on ./...

.PHONY: build
build: ## Build static binaries
	$(FIPS) go build -trimpath ./...
