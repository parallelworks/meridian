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
check: tidy-check lint vulncheck cgo-check vet-cross test test-race test-386 bench build wasm ## Run all checks, tests and builds

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

# The systems besides Linux that the meridian command and its worker build
# for, with code of their own, which CI can't run: vet-cross compiles it,
# tests included, and cgo-check checks it.
CROSS_OS := darwin freebsd openbsd netbsd
# 32-bit FreeBSD passes procctl's 64-bit argument differently on each
# architecture, so each is vetted. Windows isn't a target, but is vetted
# too, so that what only builds on Unix says so.
CROSS_TARGETS := darwin/amd64 darwin/arm64 freebsd/amd64 freebsd/386 freebsd/arm openbsd/amd64 netbsd/amd64 windows/amd64

.PHONY: cgo-check
cgo-check: ## Verify that no package or dependency uses cgo
	@for os in linux $(CROSS_OS); do \
		pkgs=$$(GOOS=$$os CGO_ENABLED=1 go list -deps -test -f '{{if and .CgoFiles (not .Standard)}}{{.ImportPath}}{{end}}' ./...) || exit 1; \
		if [ -n "$$pkgs" ]; then echo "cgo is not allowed, but these packages use it on $$os:"; echo "$$pkgs"; exit 1; fi; \
	done

.PHONY: vet-cross
vet-cross: ## Compile and vet the code for macOS, the BSDs and Windows, tests included
	@for target in $(CROSS_TARGETS); do \
		echo "GOOS=$${target%/*} GOARCH=$${target#*/} go vet ./..."; \
		GOOS=$${target%/*} GOARCH=$${target#*/} go vet ./... || exit 1; \
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

# test-openssh runs the tests that check the ssh backend against OpenSSH's
# sshd, which SSHD names by its absolute path, as well as against the SSH
# server the tests run in process: each starts the sshd as this machine's
# user, on a port of the loopback interface, with a host key, a config and
# the keys it accepts made for that test alone (internal/sshdtest). Where
# there is no sshd, it says so and skips them, except in CI, which must run
# them.
SSHD ?= /usr/sbin/sshd
OPENSSH_TESTS := ^(TestServer|TestOpenSSH.*|TestSSH.*)$$
.PHONY: test-openssh
test-openssh: ## Run the ssh backend's tests against OpenSSH's sshd, which SSHD names
	@if [ ! -x "$(SSHD)" ]; then \
		echo "test-openssh: no sshd at $(SSHD)"; \
		if [ -n "$$CI" ]; then exit 1; fi; \
		echo "test-openssh: skipped"; exit 0; \
	fi && \
	echo "$(FIPS) MERIDIAN_TEST_SSHD=$(SSHD) go test -count=1 -run '$(OPENSSH_TESTS)' ./internal/sshdtest ./backend/ssh/... ./cmd/meridian" && \
	$(FIPS) MERIDIAN_TEST_SSHD=$(SSHD) go test -count=1 -run '$(OPENSSH_TESTS)' ./internal/sshdtest ./backend/ssh/... ./cmd/meridian

.PHONY: bench
bench: ## Run each benchmark once, to check that it still runs
	$(FIPS) go test -run='^$$' -bench=. -benchtime=1x ./...

# Under the race detector on a hosted runner, the coordinator package's tests
# take close to go test's default 10 minutes, so the limit is 15.
.PHONY: test-race
test-race: ## Run tests with the race detector (needs cgo and a C compiler on Linux)
	$(FIPS) CGO_ENABLED=1 go test -race -shuffle=on -timeout=15m ./...

.PHONY: build
build: ## Build static binaries
	$(FIPS) go build -trimpath ./...

# The packages an editor's WebAssembly runs: the phases, and what they
# import. None may need what WebAssembly lacks.
WASM_PKGS := ./format ./expr ./compile ./plan ./problems ./backend

.PHONY: wasm
wasm: ## Build the packages editors run as WebAssembly, for WASI and for browsers
	GOOS=wasip1 GOARCH=wasm go build $(WASM_PKGS)
	GOOS=js GOARCH=wasm go build $(WASM_PKGS)
